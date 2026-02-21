import streamlit as st
import json
import datetime
import PyPDF2

from engine import DatabaseEngine
from llm_pipeline import LLMPipeline

# --- Setup State & Engine ---
if "engine" not in st.session_state:
    st.session_state.engine = DatabaseEngine()
if "pipeline" not in st.session_state:
    st.session_state.pipeline = LLMPipeline(st.session_state.engine.get_schema_map())
if "rules" not in st.session_state:
    st.session_state.rules = []
if "audit_log" not in st.session_state:
    st.session_state.audit_log = []

# --- Helper Functions ---
def extract_text_from_file(uploaded_file) -> str:
    if uploaded_file.name.endswith('.pdf'):
        pdf_reader = PyPDF2.PdfReader(uploaded_file)
        text = ""
        for page in pdf_reader.pages:
            text += page.extract_text() + "\n"
        return text
    else:
        # Fallback for plain text files (.txt)
        return uploaded_file.getvalue().decode("utf-8")

def parse_policy(text: str):
    with st.spinner("Agent 1 is analyzing the PDF and writing queries..."):
        try:
            extraction = st.session_state.pipeline.agent_1_extract_and_query(text)
            st.session_state.rules = [rule.model_dump() for rule in extraction.rules]
            st.success("Extraction complete!")
        except Exception as e:
            st.error(f"Failed to extract rules: {e}")

def run_query(rule_index, sql_query, rule_description):
    with st.spinner("Agent 2 is scanning the database..."):
        result = st.session_state.engine.execute_query(sql_query)
        if not result["success"]:
            st.error(f"SQL Error: {result['error']}")
            return
        
        if result["count"] == 0:
            st.info("No violations found for this rule.")
            return

        st.warning(f"Found {result['count']} suspicious records!")
        
        # Display the first flagged record for Agent 2 to explain
        first_record = result["data"][0]
        st.write("### Example Flagged Record:")
        st.json(first_record)
        
        with st.spinner("Agent 2 is writing the audit explanation..."):
            explanation = st.session_state.pipeline.agent_2_describe_violation(
                rule_description, first_record
            )
            
        st.write("### Auditor Explanation:")
        st.info(explanation)
        
        # Add to audit log
        log_entry = {
            "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "rule": rule_description,
            "approved_sql": sql_query,
            "flagged_count": result["count"],
            "explanation": explanation
        }
        st.session_state.audit_log.append(log_entry)

# --- UI Setup ---
st.set_page_config(page_title="AI Data Policy Agent", layout="wide")
st.title("🛡️ Data Policy Compliance Agent")
st.markdown("Automated PDF-to-SQL rule validation with Human-in-the-Loop approval.")

# Sidebar for Document Upload
st.sidebar.header("1. Upload Policy Document")
uploaded_file = st.sidebar.file_uploader("Upload AML Policy", type=["pdf", "txt"])

if uploaded_file is not None:
    if st.sidebar.button("Run Extractor Agent"):
        text = extract_text_from_file(uploaded_file)
        parse_policy(text)

# Main View: The HITL Interface
if st.session_state.rules:
    st.header("2. Review & Approve (Human-in-the-Loop)")
    st.markdown("Agent 1 has translated the PDF rules into DuckDB SQL. Review before execution.")
    
    for idx, rule in enumerate(st.session_state.rules):
        with st.expander(f"Rule: {rule['rule_name']}", expanded=True):
            st.write(f"**Policy Text:** {rule['rule_description']}")
            
            # Allow the analyst to edit the SQL
            edited_sql = st.text_area(
                "Proposed SQL (Edit if needed):", 
                value=rule['sql_query'], 
                key=f"sql_{idx}"
            )
            
            if st.button(f"Approve & Execute", key=f"run_{idx}"):
                run_query(idx, edited_sql, rule['rule_description'])

# Audit Log View
if st.session_state.audit_log:
    st.header("3. Audit Trail")
    for log in st.session_state.audit_log:
        st.json(log)
    
    # Download Button
    audit_json = json.dumps(st.session_state.audit_log, indent=2)
    st.download_button(
        label="Download Full Audit Report",
        data=audit_json,
        file_name="Audit_Report.json",
        mime="application/json"
    )
