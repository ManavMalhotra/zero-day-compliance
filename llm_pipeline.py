import json
import hashlib
from google import genai
from google.genai import types
from pydantic import BaseModel, Field
from typing import List, Optional
from dotenv import load_dotenv

import prompts
from utils import optimize_policy_text

load_dotenv()

client = genai.Client()
MODEL_ID = 'gemini-3-flash-preview'
AGENT_2_BATCH_SIZE = 4

# --- Output Schemas ---

class Agent1Rule(BaseModel):
    rule_id: str = Field(description="The rule identifier exactly as written (e.g. 'Rule 3.1')")
    title: str = Field(description="A short descriptive name (5–8 words)")
    description: Optional[str] = Field(None, description="What the rule detects in one sentence")
    severity: str = Field(description="One of — CRITICAL, HIGH, MEDIUM, LOW")
    threshold: str = Field(description="The key numeric value or list")
    logic_type: str = Field(description="One of — threshold, velocity, pattern, geographic, duplicate")
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

# --- Pipeline Class ---

class LLMPipeline:
    """The 3-Agent Orchestrator"""

    def __init__(self):
        self._cache = {}

    def _log(self, on_log, message: str):
        if on_log:
            on_log(message)

    def _approx_token_count(self, text: str) -> int:
        return max(1, len(text) // 4)

    def _cache_key(self, stage_name: str, payload: str) -> str:
        digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
        return f"{stage_name}:{digest}"

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
            return [pydantic_model(**r) for r in raw_data]
        return pydantic_model(**raw_data)

    def _repair_json(self, raw_text: str, schema_hint: str, stage_name: str, on_log=None) -> str:
        repair_prompt = prompts.JSON_REPAIR_PROMPT.format(
            schema_hint=schema_hint,
            raw_text=raw_text[:20000],
        )
        self._log(on_log, f"{stage_name}: attempting JSON repair.")
        response = client.models.generate_content(
            model=MODEL_ID,
            contents=repair_prompt,
            config=types.GenerateContentConfig(temperature=0)
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
        minimized_rules = [
            {
                "rule_id": rule.rule_id,
                "title": rule.title,
                "severity": rule.severity,
                "sql_query": rule.sql_query,
                "pandas_query": rule.pandas_query,
            }
            for rule in rules
        ]
        return json.dumps(minimized_rules, separators=(",", ":"))

    def _chunk_rules(self, rules: List[Agent1Rule], chunk_size: int = AGENT_2_BATCH_SIZE):
        for index in range(0, len(rules), chunk_size):
            yield rules[index:index + chunk_size]

    def _generate_and_parse_json(self, prompt: str, pydantic_model, stage_name: str, on_log=None, cache_key: Optional[str] = None):
        """Helper to yield raw stream tokens, then clean and parse the final JSON."""
        if cache_key and cache_key in self._cache:
            self._log(on_log, f"{stage_name}: cache hit.")
            yield ("DONE", self._cache[cache_key])
            return

        self._log(on_log, f"{stage_name}: sending prompt (~{self._approx_token_count(prompt)} tokens).")
        response_stream = client.models.generate_content_stream(
            model=MODEL_ID,
            contents=prompt,
            config=types.GenerateContentConfig(temperature=0.1)
        )
        
        full_text = ""
        next_progress_mark = 600
        for chunk in response_stream:
            text = chunk.text or ""
            if not text:
                continue
            full_text += text
            yield text
            if len(full_text) >= next_progress_mark:
                self._log(on_log, f"{stage_name}: received {len(full_text)} response characters.")
                next_progress_mark += 600
            
        try:
            base_schema_hint = pydantic_model.model_json_schema()
            if stage_name == "Agent 1":
                base_schema_hint = {"type": "array", "items": base_schema_hint}

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
        except json.JSONDecodeError as de:
            self._log(on_log, f"{stage_name}: JSON decode failed.")
            yield ("ERROR", f"JSON Decode Error: {de}\n\nRAW OUTPUT:\n{full_text}")
        except Exception as e:
            self._log(on_log, f"{stage_name}: schema validation failed.")
            yield ("ERROR", f"JSON Parse/Validation failed: {e}\n\nRAW OUTPUT:\n{full_text}")

    def agent_1_extract_generic_rules(self, policy_text: str, on_log=None):
        """
        Agent 1 (Policy Interpreter) Generator:
        Yields string tokens of the raw JSON stream from the LLM. 
        When finished, yields a tuple ("DONE", List[Agent1Rule]).
        """
        optimized_policy_text = optimize_policy_text(policy_text)
        self._log(
            on_log,
            f"Agent 1: optimized policy from {len(policy_text):,} to {len(optimized_policy_text):,} characters.",
        )
        prompt = prompts.AGENT_1_PROMPT.format(policy_text=optimized_policy_text)
        cache_key = self._cache_key("agent_1", optimized_policy_text)
        yield from self._generate_and_parse_json(
            prompt,
            Agent1Rule,
            stage_name="Agent 1",
            on_log=on_log,
            cache_key=cache_key,
        )

    def agent_2_map_all_rules(self, rules: List[Agent1Rule], dataset_columns: List[str], sample_data: str, on_log=None):
        """
        Agent 2 (Schema Mapper) — Single batched LLM call for ALL rules.
        Returns Agent2Response directly (no streaming needed for speed).
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
                sample_data=sample_data
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
                    response = client.models.generate_content(
                        model=MODEL_ID,
                        contents=prompt,
                        config=types.GenerateContentConfig(temperature=0.1)
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
        # Convert Pydantic rules to compact dicts to reduce token usage.
        rules_json = self._serialize_agent_2_rules(rules)
        dataset_columns_json = json.dumps(dataset_columns, separators=(",", ":"))
        
        prompt = prompts.AGENT_2_PROMPT.format(
            rules_json=rules_json,
            dataset_columns=dataset_columns_json,
            sample_data=sample_data
        )
        cache_key = self._cache_key("agent_2_stream", f"{rules_json}|{dataset_columns_json}|{sample_data}")
        
        # We need Agent2Response which wraps the list of rules
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
        response_stream = client.models.generate_content_stream(
            model=MODEL_ID,
            contents=prompt,
            config=types.GenerateContentConfig(temperature=0.2)
        )
        full_text = ""
        next_progress_mark = 800
        for chunk in response_stream:
            text = chunk.text or ""
            if not text:
                continue
            full_text += text
            yield text
            if len(full_text) >= next_progress_mark:
                self._log(on_log, f"Agent 3: generated {len(full_text)} report characters.")
                next_progress_mark += 800

        self._cache[cache_key] = full_text
        self._log(on_log, "Agent 3: report generation complete.")
