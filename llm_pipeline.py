import os
import json
from google import genai
from google.genai import types
from pydantic import BaseModel, Field
from typing import List, Optional
from dotenv import load_dotenv

import prompts

load_dotenv()

client = genai.Client()
MODEL_ID = 'gemini-3-pro-preview'

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
    
    def _generate_and_parse_json(self, prompt: str, pydantic_model):
        """Helper to yield raw stream tokens, then clean and parse the final JSON."""
        response_stream = client.models.generate_content_stream(
            model=MODEL_ID,
            contents=prompt,
            config=types.GenerateContentConfig(temperature=0.1)
        )
        
        full_text = ""
        for chunk in response_stream:
            text = chunk.text
            full_text += text
            yield text
            
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
            yield ("DONE", parsed)
        except json.JSONDecodeError as de:
            yield ("ERROR", f"JSON Decode Error: {de}\n\nRAW OUTPUT:\n{full_text}")
        except Exception as e:
            yield ("ERROR", f"JSON Parse/Validation failed: {e}\n\nRAW OUTPUT:\n{full_text}")

    def agent_1_extract_generic_rules(self, policy_text: str):
        """
        Agent 1 (Policy Interpreter) Generator:
        Yields string tokens of the raw JSON stream from the LLM. 
        When finished, yields a tuple ("DONE", List[Agent1Rule]).
        """
        prompt = prompts.AGENT_1_PROMPT.format(policy_text=policy_text)
        yield from self._generate_and_parse_json(prompt, Agent1Rule)

    async def agent_2_map_schema_and_values_async(self, rules: List[Agent1Rule], dataset_columns: List[str], sample_data: str):
        """Asynchronous version of Agent 2 designed to run concurrently."""
        import asyncio
        from google.genai import types
        
        async def map_single_rule(rule: Agent1Rule):
             prompt = prompts.AGENT_2_PROMPT.format(
                 rules_json=json.dumps([rule.model_dump()], indent=2),
                 dataset_columns=dataset_columns,
                 sample_data=sample_data
             )
             
             # Wrap synchronous genai generator in to_thread, ideally use async client if available
             def sync_call():
                 try:
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
                     
                     # Check if it returned a dict with 'mapped_rules' or a flat list
                     if isinstance(raw_data, dict) and 'mapped_rules' in raw_data:
                         raw_list = raw_data['mapped_rules']
                     elif isinstance(raw_data, list):
                         raw_list = raw_data
                     else:
                         return ("ERROR", f"Agent 2 output unexpected format: {type(raw_data)}")
                         
                     return ("DONE", Agent2Response(mapped_rules=[Agent2MappedRule(**r) for r in raw_list]))
                 except Exception as e:
                     return ("ERROR", f"Async mapping failed: {e}")
                     
             return await asyncio.to_thread(sync_call)
             
        # Execute all rules in the batch concurrently
        tasks = [map_single_rule(r) for r in rules]
        return await asyncio.gather(*tasks)

    def agent_2_map_schema_and_values(self, rules: List[Agent1Rule], dataset_columns: List[str], sample_data: str):
        """
        Agent 2 (Schema Mapper) Generator:
        Yields string tokens of the raw JSON stream from the LLM. 
        When finished, yields a tuple ("DONE", Agent2Response).
        """
        # Convert Pydantic rules to dicts to pass in prompt
        rules_json = json.dumps([r.model_dump() for r in rules], indent=2)
        
        prompt = prompts.AGENT_2_PROMPT.format(
            rules_json=rules_json,
            dataset_columns=dataset_columns,
            sample_data=sample_data
        )
        
        # We need Agent2Response which wraps the list of rules
        yield from self._generate_and_parse_json(prompt, Agent2Response)

    def agent_3_generate_executive_report(self, execution_metrics_json: str) -> str:
        """
        Agent 3 (Compliance Executor / Reporter):
        Takes the raw dictionary of violation counts and builds the final Markdown report.
        """
        prompt = prompts.AGENT_3_PROMPT.format(execution_metrics_json=execution_metrics_json)
        
        print("Agent 3: Generating Executive Report (Streaming)...")
        response_stream = client.models.generate_content_stream(
            model=MODEL_ID,
            contents=prompt,
            config=types.GenerateContentConfig(temperature=0.2)
        )
        for chunk in response_stream:
            yield chunk.text
