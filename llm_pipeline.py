import json
import os
import random
import re
import time
from collections import defaultdict, deque
from copy import deepcopy
from datetime import datetime
from typing import List, Optional
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from google import genai
from google.genai import types
from pydantic import BaseModel, Field

import prompts

load_dotenv()

client = genai.Client()

AGENT_2_BATCH_SIZE = 4
MAX_API_ATTEMPTS = 4
BASE_BACKOFF_SECONDS = 6
MAX_BACKOFF_SECONDS = 90
PACIFIC_TZ = ZoneInfo("America/Los_Angeles")

PRIMARY_MODEL = os.getenv("GEMINI_MODEL_PRIMARY", "gemini-2.5-flash")
FALLBACK_MODELS = [
    model.strip()
    for model in os.getenv("GEMINI_MODEL_FALLBACKS", "gemini-2.5-flash-lite").split(",")
    if model.strip()
]

# Based on Gemini API free-tier limits from the official docs.
FREE_TIER_LIMITS = {
    "gemini-2.5-flash": {"rpm": 10, "tpm": 250_000, "rpd": 250},
    "gemini-2.5-flash-lite": {"rpm": 15, "tpm": 250_000, "rpd": 1_000},
    "gemini-2.0-flash": {"rpm": 15, "tpm": 1_000_000, "rpd": 200},
    "gemini-2.0-flash-lite": {"rpm": 30, "tpm": 1_000_000, "rpd": 200},
}


class Agent1Rule(BaseModel):
    rule_id: str = Field(description="The rule identifier exactly as written (e.g. 'Rule 3.1')")
    title: str = Field(description="A short descriptive name (5-8 words)")
    description: Optional[str] = Field(None, description="What the rule detects in one sentence")
    severity: str = Field(description="One of CRITICAL, HIGH, MEDIUM, LOW")
    threshold: str = Field(description="The key numeric value or list")
    logic_type: str = Field(description="One of threshold, velocity, pattern, geographic, duplicate")
    sql_query: str = Field(description="The exact SQL query string")
    pandas_query: str = Field(description="The exact Pandas query string")
    explanation: str = Field(description="Explanation of what the query does")


class Agent2MappedRule(BaseModel):
    rule_id: str
    title: str
    severity: str
    sql_query: str
    pandas_query: str
    columns_remapped: List[str]
    values_remapped: List[str]
    status: str = Field(description="READY or SKIPPED")
    skip_reason: Optional[str] = Field(None, description="Reason if SKIPPED")


class Agent2Response(BaseModel):
    mapped_rules: List[Agent2MappedRule]


class LLMPipeline:
    """The 3-Agent orchestrator with structured outputs and rate-limit-aware retries."""

    def __init__(self):
        self._cache = {}
        self._request_windows = defaultdict(deque)
        self._token_windows = defaultdict(deque)
        self._daily_counts = defaultdict(int)
        self._daily_count_day = self._current_pacific_day()

    def _log(self, on_log, message: str):
        if on_log:
            on_log(message)

    def _approx_token_count(self, text: str) -> int:
        return max(1, len(text) // 4)

    def _cache_key(self, stage_name: str, payload: str) -> str:
        digest = hashlib_sha256(payload)
        return f"{stage_name}:{digest}"

    def _current_pacific_day(self) -> str:
        return datetime.now(PACIFIC_TZ).date().isoformat()

    def _reset_daily_counts_if_needed(self):
        current_day = self._current_pacific_day()
        if current_day != self._daily_count_day:
            self._daily_counts.clear()
            self._daily_count_day = current_day

    def _model_candidates(self) -> List[str]:
        models = []
        for model in [PRIMARY_MODEL, *FALLBACK_MODELS]:
            if model and model not in models:
                models.append(model)
        return models

    def configured_models(self) -> List[str]:
        return list(self._model_candidates())

    def _cleanup_windows(self, model: str, now: float):
        request_window = self._request_windows[model]
        while request_window and now - request_window[0] >= 60:
            request_window.popleft()

        token_window = self._token_windows[model]
        while token_window and now - token_window[0][0] >= 60:
            token_window.popleft()

    def _estimate_input_tokens(self, text: str) -> int:
        return max(1, len(text) // 4)

    def _record_request(self, model: str, input_tokens: int):
        now = time.time()
        self._cleanup_windows(model, now)
        self._request_windows[model].append(now)
        self._token_windows[model].append((now, input_tokens))
        self._reset_daily_counts_if_needed()
        self._daily_counts[model] += 1

    def _wait_for_free_tier_capacity(self, model: str, input_tokens: int, stage_name: str, on_log=None):
        limits = FREE_TIER_LIMITS.get(model)
        if not limits:
            return

        self._reset_daily_counts_if_needed()
        if self._daily_counts[model] >= limits["rpd"]:
            raise RuntimeError(
                f"{model} appears to have reached the free-tier daily request limit "
                f"({limits['rpd']} RPD, resets at midnight Pacific)."
            )

        while True:
            now = time.time()
            self._cleanup_windows(model, now)

            request_window = self._request_windows[model]
            token_window = self._token_windows[model]

            wait_for_rpm = 0.0
            wait_for_tpm = 0.0

            if len(request_window) >= limits["rpm"]:
                wait_for_rpm = max(0.0, 60 - (now - request_window[0]) + 0.25)

            used_tokens = sum(tokens for _, tokens in token_window)
            if used_tokens + input_tokens > limits["tpm"] and token_window:
                wait_for_tpm = max(0.0, 60 - (now - token_window[0][0]) + 0.25)

            wait_time = max(wait_for_rpm, wait_for_tpm)
            if wait_time <= 0:
                return

            reasons = []
            if wait_for_rpm > 0:
                reasons.append("RPM")
            if wait_for_tpm > 0:
                reasons.append("TPM")
            self._log(
                on_log,
                f"{stage_name}: free-tier guard waiting {wait_time:.1f}s for {model} to stay within {'/'.join(reasons)} limits.",
            )
            time.sleep(wait_time)

    def _json_schema_with_ordering(self, schema: dict) -> dict:
        """Adds propertyOrdering recursively for Gemini structured outputs, including 2.0 models."""
        ordered_schema = deepcopy(schema)

        def add_ordering(node):
            if not isinstance(node, dict):
                return

            properties = node.get("properties")
            if node.get("type") == "object" and isinstance(properties, dict):
                node.setdefault("propertyOrdering", list(properties.keys()))
                for child in properties.values():
                    add_ordering(child)

            items = node.get("items")
            if isinstance(items, dict):
                add_ordering(items)
            elif isinstance(items, list):
                for child in items:
                    add_ordering(child)

            for key in ("anyOf", "oneOf", "allOf", "prefixItems"):
                value = node.get(key)
                if isinstance(value, list):
                    for child in value:
                        add_ordering(child)

        add_ordering(ordered_schema)
        return ordered_schema

    def _build_generation_config(self, schema_hint: Optional[dict], temperature: float):
        config = {"temperature": temperature}
        if schema_hint is not None:
            config["response_mime_type"] = "application/json"
            config["response_json_schema"] = self._json_schema_with_ordering(schema_hint)
        return types.GenerateContentConfig(**config)

    def _extract_error_code(self, error: Exception) -> Optional[int]:
        for attr in ("code", "status_code", "http_status"):
            value = getattr(error, attr, None)
            if isinstance(value, int):
                return value
        match = re.search(r"\b(429|500|503|504)\b", str(error))
        return int(match.group(1)) if match else None

    def _is_model_unavailable(self, error: Exception) -> bool:
        message = str(error).lower()
        return (
            "not found" in message
            or "unsupported for generatecontent" in message
            or "unknown model" in message
        )

    def _is_daily_quota_exhausted(self, error: Exception) -> bool:
        message = str(error).lower()
        markers = (
            "requestsperday",
            "per day",
            "daily quota",
            "daily request limit",
            "rpd",
            "reset at midnight",
            "exceeded your current quota",
        )
        return self._extract_error_code(error) == 429 and any(marker in message for marker in markers)

    def _is_retriable_error(self, error: Exception) -> bool:
        code = self._extract_error_code(error)
        message = str(error).lower()
        transient_markers = (
            "resource_exhausted",
            "rate limit",
            "too many requests",
            "quota",
            "temporarily unavailable",
            "service unavailable",
            "deadline exceeded",
            "deadline",
            "timed out",
            "internal",
            "try again later",
            "retrydelay",
        )
        return code in {429, 500, 503, 504} or any(marker in message for marker in transient_markers)

    def _extract_retry_delay_seconds(self, error: Exception) -> Optional[float]:
        message = str(error)
        patterns = (
            r"retryDelay['\":\s]+(\d+(?:\.\d+)?)s",
            r"retry delay['\":\s]+(\d+(?:\.\d+)?)s",
        )
        for pattern in patterns:
            match = re.search(pattern, message, flags=re.IGNORECASE)
            if match:
                return float(match.group(1))
        return None

    def _retry_delay_seconds(self, error: Exception, attempt: int) -> float:
        hinted_delay = self._extract_retry_delay_seconds(error)
        if hinted_delay is not None:
            return min(MAX_BACKOFF_SECONDS, hinted_delay + 1)

        base_delay = BASE_BACKOFF_SECONDS * (2 ** (attempt - 1))
        jitter = random.uniform(0.0, 1.0)
        return min(MAX_BACKOFF_SECONDS, base_delay + jitter)

    def _call_with_retries(self, stage_name: str, prompt: str, schema_hint: Optional[dict], temperature: float, on_log=None):
        prompt_tokens = self._estimate_input_tokens(prompt)
        last_error = None
        models = self._model_candidates()

        for model_index, model in enumerate(models, start=1):
            for attempt in range(1, MAX_API_ATTEMPTS + 1):
                try:
                    self._wait_for_free_tier_capacity(model, prompt_tokens, stage_name, on_log=on_log)
                    self._record_request(model, prompt_tokens)
                    self._log(
                        on_log,
                        f"{stage_name}: calling {model} (attempt {attempt}/{MAX_API_ATTEMPTS}, model {model_index}/{len(models)}).",
                    )
                    return client.models.generate_content(
                        model=model,
                        contents=prompt,
                        config=self._build_generation_config(schema_hint, temperature),
                    )
                except Exception as error:
                    last_error = error
                    if self._is_model_unavailable(error):
                        self._log(on_log, f"{stage_name}: {model} is unavailable ({error}). Trying fallback model.")
                        break

                    if self._is_daily_quota_exhausted(error):
                        self._log(
                            on_log,
                            f"{stage_name}: {model} appears to be out of daily free-tier quota. Trying fallback model.",
                        )
                        break

                    if self._is_retriable_error(error) and attempt < MAX_API_ATTEMPTS:
                        delay = self._retry_delay_seconds(error, attempt)
                        self._log(
                            on_log,
                            f"{stage_name}: temporary API error on {model} ({error}). Waiting {delay:.1f}s before retry.",
                        )
                        time.sleep(delay)
                        continue

                    self._log(on_log, f"{stage_name}: {model} failed ({error}).")
                    break

        raise last_error if last_error else RuntimeError(f"{stage_name}: all Gemini models failed.")

    def _stream_text_with_retries(self, stage_name: str, prompt: str, schema_hint: Optional[dict], temperature: float, on_log=None):
        prompt_tokens = self._estimate_input_tokens(prompt)
        last_error = None
        models = self._model_candidates()

        for model_index, model in enumerate(models, start=1):
            for attempt in range(1, MAX_API_ATTEMPTS + 1):
                emitted_any = False
                try:
                    self._wait_for_free_tier_capacity(model, prompt_tokens, stage_name, on_log=on_log)
                    self._record_request(model, prompt_tokens)
                    self._log(
                        on_log,
                        f"{stage_name}: calling {model} (attempt {attempt}/{MAX_API_ATTEMPTS}, model {model_index}/{len(models)}).",
                    )
                    response_stream = client.models.generate_content_stream(
                        model=model,
                        contents=prompt,
                        config=self._build_generation_config(schema_hint, temperature),
                    )
                    for chunk in response_stream:
                        text = chunk.text or ""
                        if not text:
                            continue
                        emitted_any = True
                        yield text
                    return
                except Exception as error:
                    last_error = error
                    if emitted_any:
                        self._log(on_log, f"{stage_name}: stream failed after partial output on {model} ({error}).")
                        raise

                    if self._is_model_unavailable(error):
                        self._log(on_log, f"{stage_name}: {model} is unavailable ({error}). Trying fallback model.")
                        break

                    if self._is_daily_quota_exhausted(error):
                        self._log(
                            on_log,
                            f"{stage_name}: {model} appears to be out of daily free-tier quota. Trying fallback model.",
                        )
                        break

                    if self._is_retriable_error(error) and attempt < MAX_API_ATTEMPTS:
                        delay = self._retry_delay_seconds(error, attempt)
                        self._log(
                            on_log,
                            f"{stage_name}: temporary API error on {model} ({error}). Waiting {delay:.1f}s before retry.",
                        )
                        time.sleep(delay)
                        continue

                    self._log(on_log, f"{stage_name}: {model} failed ({error}).")
                    break

        raise last_error if last_error else RuntimeError(f"{stage_name}: all Gemini models failed.")

    def _strip_markdown_fences(self, text: str) -> str:
        cleaned = (text or "").strip()
        if "```json" in cleaned:
            cleaned = cleaned.split("```json", 1)[1].split("```", 1)[0].strip()
        elif "```" in cleaned:
            cleaned = cleaned.split("```", 1)[1].split("```", 1)[0].strip()
        return cleaned

    def _extract_json_block(self, text: str) -> str:
        """Extracts the first balanced JSON object/array from a noisy response."""
        cleaned = self._strip_markdown_fences(text)
        start_positions = [pos for pos in (cleaned.find("{"), cleaned.find("[")) if pos != -1]
        if not start_positions:
            return cleaned

        start = min(start_positions)
        opening = cleaned[start]
        closing = "}" if opening == "{" else "]"
        depth = 0
        in_string = False
        escape = False

        for index in range(start, len(cleaned)):
            char = cleaned[index]
            if in_string:
                if escape:
                    escape = False
                elif char == "\\":
                    escape = True
                elif char == '"':
                    in_string = False
                continue

            if char == '"':
                in_string = True
            elif char == opening:
                depth += 1
            elif char == closing:
                depth -= 1
                if depth == 0:
                    return cleaned[start:index + 1]

        return cleaned[start:]

    def _parse_json_text(self, raw_text: str):
        cleaned = self._strip_markdown_fences(raw_text)
        candidates = [cleaned]

        extracted = self._extract_json_block(cleaned)
        if extracted and extracted not in candidates:
            candidates.append(extracted)

        last_error = None
        for candidate in candidates:
            try:
                return json.loads(candidate)
            except json.JSONDecodeError as error:
                last_error = error

        if last_error:
            raise last_error
        raise json.JSONDecodeError("No JSON payload found.", cleaned, 0)

    def _parse_to_model(self, raw_text: str, pydantic_model):
        raw_data = self._parse_json_text(raw_text)
        if isinstance(raw_data, list):
            return [pydantic_model(**item) for item in raw_data]
        return pydantic_model(**raw_data)

    def _repair_json(self, raw_text: str, schema_hint: str, stage_name: str, on_log=None) -> str:
        repair_prompt = prompts.JSON_REPAIR_PROMPT.format(
            schema_hint=schema_hint,
            raw_text=raw_text[:20000],
        )
        self._log(on_log, f"{stage_name}: attempting JSON repair.")
        schema_dict = json.loads(schema_hint)
        response = self._call_with_retries(
            stage_name=f"{stage_name} repair",
            prompt=repair_prompt,
            schema_hint=schema_dict,
            temperature=0,
            on_log=on_log,
        )
        return response.text or ""

    def _parse_with_repair(self, raw_text: str, pydantic_model, schema_hint: str, stage_name: str, on_log=None):
        try:
            return self._parse_to_model(raw_text, pydantic_model)
        except Exception as error:
            self._log(on_log, f"{stage_name}: response validation failed ({error}).")
            repaired_text = self._repair_json(raw_text, schema_hint, stage_name, on_log=on_log)
            return self._parse_to_model(repaired_text, pydantic_model)

    def _serialize_agent_2_rules(self, rules: List[Agent1Rule]) -> str:
        serialized_rules = [
            {
                "rule_id": rule.rule_id,
                "title": rule.title,
                "description": rule.description,
                "severity": rule.severity,
                "threshold": rule.threshold,
                "logic_type": rule.logic_type,
                "sql_query": rule.sql_query,
                "pandas_query": rule.pandas_query,
                "explanation": rule.explanation,
            }
            for rule in rules
        ]
        return json.dumps(serialized_rules, separators=(",", ":"))

    def _chunk_rules(self, rules: List[Agent1Rule], chunk_size: int = AGENT_2_BATCH_SIZE):
        for index in range(0, len(rules), chunk_size):
            yield rules[index:index + chunk_size]

    def _generate_and_parse_json(self, prompt: str, pydantic_model, stage_name: str, on_log=None, cache_key: Optional[str] = None):
        if cache_key and cache_key in self._cache:
            self._log(on_log, f"{stage_name}: cache hit.")
            yield ("DONE", self._cache[cache_key])
            return

        base_schema_hint = pydantic_model.model_json_schema()
        if stage_name == "Agent 1":
            base_schema_hint = {"type": "array", "items": base_schema_hint}

        self._log(on_log, f"{stage_name}: sending prompt (~{self._approx_token_count(prompt)} tokens).")
        full_text = ""
        next_progress_mark = 600
        for text in self._stream_text_with_retries(
            stage_name=stage_name,
            prompt=prompt,
            schema_hint=base_schema_hint,
            temperature=0.1,
            on_log=on_log,
        ):
            full_text += text
            yield text
            if len(full_text) >= next_progress_mark:
                self._log(on_log, f"{stage_name}: received {len(full_text)} response characters.")
                next_progress_mark += 600

        try:
            schema_hint = json.dumps(base_schema_hint, separators=(",", ":"))
            parsed = self._parse_with_repair(
                full_text,
                pydantic_model,
                schema_hint=schema_hint,
                stage_name=stage_name,
                on_log=on_log,
            )
            if cache_key:
                self._cache[cache_key] = parsed
            self._log(on_log, f"{stage_name}: parsed response successfully.")
            yield ("DONE", parsed)
        except json.JSONDecodeError as error:
            self._log(on_log, f"{stage_name}: JSON decode failed.")
            yield ("ERROR", f"JSON Decode Error: {error}\n\nRAW OUTPUT:\n{full_text}")
        except Exception as error:
            self._log(on_log, f"{stage_name}: schema validation failed.")
            yield ("ERROR", f"JSON Parse/Validation failed: {error}\n\nRAW OUTPUT:\n{full_text}")

    def agent_1_extract_generic_rules(self, policy_text: str, on_log=None):
        """
        Agent 1 (Policy Interpreter) Generator:
        Yields string tokens of the raw JSON stream from the LLM.
        When finished, yields a tuple ("DONE", List[Agent1Rule]).
        """
        self._log(
            on_log,
            f"Agent 1: sending full policy context ({len(policy_text):,} characters, no pruning).",
        )
        prompt = prompts.AGENT_1_PROMPT.format(policy_text=policy_text)
        cache_key = self._cache_key("agent_1", policy_text)
        yield from self._generate_and_parse_json(
            prompt,
            Agent1Rule,
            stage_name="Agent 1",
            on_log=on_log,
            cache_key=cache_key,
        )

    def agent_2_map_all_rules(self, rules: List[Agent1Rule], dataset_columns: List[str], sample_data: str, on_log=None):
        """
        Agent 2 (Schema Mapper):
        Uses chunked calls for reliability while preserving the full per-rule context.
        """
        dataset_columns_json = json.dumps(dataset_columns, separators=(",", ":"))
        chunks = list(self._chunk_rules(rules))
        combined_rules = []

        self._log(
            on_log,
            f"Agent 2: split {len(rules)} rules into {len(chunks)} chunk(s) of up to {AGENT_2_BATCH_SIZE} rules.",
        )

        for chunk_index, chunk in enumerate(chunks, start=1):
            rules_json = self._serialize_agent_2_rules(chunk)
            prompt = prompts.AGENT_2_PROMPT.format(
                rules_json=rules_json,
                dataset_columns=dataset_columns_json,
                sample_data=sample_data,
            )
            cache_key = self._cache_key(
                f"agent_2_chunk_{chunk_index}",
                f"{rules_json}|{dataset_columns_json}|{sample_data}",
            )

            try:
                if cache_key in self._cache:
                    self._log(on_log, f"Agent 2 chunk {chunk_index}/{len(chunks)}: cache hit.")
                    parsed = self._cache[cache_key]
                else:
                    self._log(
                        on_log,
                        f"Agent 2 chunk {chunk_index}/{len(chunks)}: sending prompt (~{self._approx_token_count(prompt)} tokens).",
                    )
                    response = self._call_with_retries(
                        stage_name=f"Agent 2 chunk {chunk_index}/{len(chunks)}",
                        prompt=prompt,
                        schema_hint=Agent2Response.model_json_schema(),
                        temperature=0.1,
                        on_log=on_log,
                    )

                    schema_hint = json.dumps(Agent2Response.model_json_schema(), separators=(",", ":"))
                    parsed = self._parse_with_repair(
                        response.text or "",
                        Agent2Response,
                        schema_hint=schema_hint,
                        stage_name=f"Agent 2 chunk {chunk_index}/{len(chunks)}",
                        on_log=on_log,
                    )
                    self._cache[cache_key] = parsed

                combined_rules.extend(parsed.mapped_rules)
                self._log(
                    on_log,
                    f"Agent 2 chunk {chunk_index}/{len(chunks)}: mapped {len(parsed.mapped_rules)} rules.",
                )
            except Exception as error:
                self._log(on_log, f"Agent 2 chunk {chunk_index}/{len(chunks)} failed: {error}")
                return ("ERROR", f"Agent 2 mapping failed in chunk {chunk_index}/{len(chunks)}: {error}")

        result = Agent2Response(mapped_rules=combined_rules)
        self._log(on_log, f"Agent 2: mapped {len(result.mapped_rules)} rules successfully.")
        return ("DONE", result)

    def agent_2_map_schema_and_values(self, rules: List[Agent1Rule], dataset_columns: List[str], sample_data: str, on_log=None):
        """
        Agent 2 (Schema Mapper) Generator:
        Yields string tokens of the raw JSON stream from the LLM.
        When finished, yields a tuple ("DONE", Agent2Response).
        """
        rules_json = self._serialize_agent_2_rules(rules)
        dataset_columns_json = json.dumps(dataset_columns, separators=(",", ":"))

        prompt = prompts.AGENT_2_PROMPT.format(
            rules_json=rules_json,
            dataset_columns=dataset_columns_json,
            sample_data=sample_data,
        )
        cache_key = self._cache_key("agent_2_stream", f"{rules_json}|{dataset_columns_json}|{sample_data}")

        yield from self._generate_and_parse_json(
            prompt,
            Agent2Response,
            stage_name="Agent 2",
            on_log=on_log,
            cache_key=cache_key,
        )

    def agent_3_generate_executive_report(self, execution_metrics_json: str, on_log=None) -> str:
        """
        Agent 3 (Compliance Executor / Reporter):
        Takes the raw dictionary of violation counts and builds the final Markdown report.
        """
        prompt = prompts.AGENT_3_PROMPT.format(execution_metrics_json=execution_metrics_json)
        cache_key = self._cache_key("agent_3", execution_metrics_json)

        if cache_key in self._cache:
            self._log(on_log, "Agent 3: cache hit.")
            yield self._cache[cache_key]
            return

        self._log(on_log, f"Agent 3: sending prompt (~{self._approx_token_count(prompt)} tokens).")
        full_text = ""
        next_progress_mark = 800
        for text in self._stream_text_with_retries(
            stage_name="Agent 3",
            prompt=prompt,
            schema_hint=None,
            temperature=0.2,
            on_log=on_log,
        ):
            full_text += text
            yield text
            if len(full_text) >= next_progress_mark:
                self._log(on_log, f"Agent 3: generated {len(full_text)} report characters.")
                next_progress_mark += 800

        self._cache[cache_key] = full_text
        self._log(on_log, "Agent 3: report generation complete.")


def hashlib_sha256(payload: str) -> str:
    import hashlib

    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
