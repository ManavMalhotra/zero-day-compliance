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
            
        # Clean markdown fences if present
        cleaned = full_text.strip()
        if "```json" in cleaned:
            cleaned = cleaned.split("```json")[1].split("```")[0].strip()
        elif "```" in cleaned:
            cleaned = cleaned.split("```")[1].split("```")[0].strip()
            
        try:
            raw_data = json.loads(cleaned)
            if isinstance(raw_data, list):
                parsed = [pydantic_model(**r) for r in raw_data]
            else:
                parsed = pydantic_model(**raw_data)
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
        rules_json = json.dumps([r.model_dump() for r in rules], separators=(",", ":"))
        
        prompt = prompts.AGENT_2_PROMPT.format(
            rules_json=rules_json,
            dataset_columns=dataset_columns,
            sample_data=sample_data
        )
        cache_key = self._cache_key("agent_2", f"{rules_json}|{dataset_columns}|{sample_data}")
        
        try:
            if cache_key in self._cache:
                self._log(on_log, "Agent 2: cache hit.")
                return ("DONE", self._cache[cache_key])

            self._log(on_log, f"Agent 2: sending prompt (~{self._approx_token_count(prompt)} tokens).")
            response = client.models.generate_content(
                model=MODEL_ID,
                contents=prompt,
                config=types.GenerateContentConfig(temperature=0.1)
            )
            
            cleaned = response.text.strip()
            if "```json" in cleaned:
                cleaned = cleaned.split("```json")[1].split("```")[0].strip()
            elif "```" in cleaned:
                cleaned = cleaned.split("```")[1].split("```")[0].strip()
                
            raw_data = json.loads(cleaned)
            
            if isinstance(raw_data, dict) and 'mapped_rules' in raw_data:
                raw_list = raw_data['mapped_rules']
            elif isinstance(raw_data, list):
                raw_list = raw_data
            else:
                return ("ERROR", f"Agent 2 unexpected format: {type(raw_data)}")

            parsed = Agent2Response(mapped_rules=[Agent2MappedRule(**r) for r in raw_list])
            self._cache[cache_key] = parsed
            self._log(on_log, f"Agent 2: mapped {len(parsed.mapped_rules)} rules successfully.")
            return ("DONE", parsed)
        except Exception as e:
            self._log(on_log, f"Agent 2 failed: {e}")
            return ("ERROR", f"Agent 2 mapping failed: {e}")

    def agent_2_map_schema_and_values(self, rules: List[Agent1Rule], dataset_columns: List[str], sample_data: str, on_log=None):
        """
        Agent 2 (Schema Mapper) Generator:
        Yields string tokens of the raw JSON stream from the LLM. 
        When finished, yields a tuple ("DONE", Agent2Response).
        """
        # Convert Pydantic rules to dicts to pass in prompt
        rules_json = json.dumps([r.model_dump() for r in rules], separators=(",", ":"))
        
        prompt = prompts.AGENT_2_PROMPT.format(
            rules_json=rules_json,
            dataset_columns=dataset_columns,
            sample_data=sample_data
        )
        cache_key = self._cache_key("agent_2_stream", f"{rules_json}|{dataset_columns}|{sample_data}")
        
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
