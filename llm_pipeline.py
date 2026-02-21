import os
from google import genai
from google.genai import types
from pydantic import BaseModel, Field
from typing import List
from dotenv import load_dotenv

load_dotenv()

# Setup Gemini Client
# Assumes GEMINI_API_KEY is in the environment
client = genai.Client()
MODEL_ID = 'gemini-2.5-flash'

class PolicyRule(BaseModel):
    rule_name: str = Field(description="A short, descriptive name for the compliance rule.")
    rule_description: str = Field(description="The exact plain-English logic extracted from the text.")
    sql_query: str = Field(description="The executable DuckDB SQL query to detect this violation.")

class PolicyExtraction(BaseModel):
    rules: List[PolicyRule] = Field(description="List of all hard rules extracted from the policy.")

class LLMPipeline:
    def __init__(self, schema_map):
        self.schema_map = schema_map

    def agent_1_extract_and_query(self, policy_text: str) -> PolicyExtraction:
        """
        Agent 1 (Policy Parser & Query Writer):
        Reads the policy document, extracts logical rules, and translates them into SQL.
        """
        prompt = f"""
        You are an elite Compliance Officer and a master DuckDB SQL Engineer.
        Your job is to read the provided Compliance Policy text and extract ANY "Hard Rules" (concrete mathematical constraints).
        For each rule you find, you must write an executable DuckDB SQL query that will find violations of that rule.
        
        CRITICAL RULES FOR SQL:
        1. You may ONLY use the tables and columns defined in the Schema Map below. Do not invent columns.
        2. Provide standard, valid SQL for DuckDB. 
        3. Do not include markdown formatting like ```sql in the output string, just the raw query.
        
        SCHEMA MAP:
        {self.schema_map}
        
        COMPLIANCE POLICY TEXT:
        {policy_text}
        """
        
        print("Agent 1: Analyzing policy and generating SQL...")
        response = client.models.generate_content(
            model=MODEL_ID,
            contents=prompt,
            config=types.GenerateContentConfig(
                response_mime_type="application/json",
                response_schema=PolicyExtraction,
                temperature=0.1
            ),
        )
        return response.parsed

    def agent_2_describe_violation(self, rule_description: str, flagged_record: dict) -> str:
        """
        Agent 2 (Scanner & Describer):
        Takes a flagged record and generates a descriptive explanation highlighting the broken policy.
        """
        prompt = f"""
        You are a Forensic Auditor. An automated SQL query just flagged a suspicious record in the system.
        
        THE BROKEN POLICY RULE:
        "{rule_description}"
        
        THE FLAGGED RECORD (JSON format):
        {flagged_record}
        
        TASK:
        Write a concise, 2-3 sentence audit explanation of WHY this record was flagged.
        Explicitly cite the policy rule. Be professional and objective.
        Do not output JSON, just output the plain text explanation.
        """
        
        print("Agent 2: Writing audit explanation for flagged record...")
        response = client.models.generate_content(
            model=MODEL_ID,
            contents=prompt,
            config=types.GenerateContentConfig(temperature=0.3)
        )
        return response.text
