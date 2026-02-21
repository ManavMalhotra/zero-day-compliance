import streamlit as st
import pandas as pd
import PyPDF2
from io import StringIO
import markdown
from xhtml2pdf import pisa
from io import BytesIO

from llm_pipeline import LLMPipeline
from executor import PandasExecutor
from utils import extract_text_from_file

# --- State Management ---
if "pipeline" not in st.session_state:
    st.session_state.pipeline = LLMPipeline()

if "agent_1_rules" not in st.session_state:
    st.session_state.agent_1_rules = []

if "agent_2_mapped_rules" not in st.session_state:
    st.session_state.agent_2_mapped_rules = []
    
if "final_report" not in st.session_state:
    st.session_state.final_report = ""

if "raw_df" not in st.session_state:
    st.session_state.raw_df = None

# --- UI Setup ---
st.set_page_config(page_title="AI Data Policy Agent", layout="wide")
st.title("🛡️ Data Policy Compliance Agent")
st.markdown("Automated PDF Policy -> Dynamic Pandas Mapping -> Executive Report")

import sys
class StreamlitConsoleRedirect:
    """Redirects print() statements to a Streamlit string buffer for live UI logs."""
    def __init__(self, st_placeholder):
        self.st_placeholder = st_placeholder
        self.log_text = ""
        self.terminal = sys.stdout

    def write(self, message):
        self.terminal.write(message) # Keep in actual terminal
        self.log_text += message
        # Keep last 3000 chars to prevent UI lag on massive JSONs
        display_text = self.log_text[-3000:]
        # Use Markdown because st.code updates can be slightly jarred dynamically
        self.st_placeholder.markdown(f"```json\n{display_text}\n```")

    def flush(self):
        self.terminal.flush()

import os

# --- Sidebar ---
st.sidebar.header("1. Upload Policy")
uploaded_policy = st.sidebar.file_uploader("Upload Policy (PDF/TXT)", type=["pdf", "txt"])

# --- Auto-Load Data ---
data_dir = os.path.join(os.path.dirname(__file__), "data")
available_csvs = [f for f in os.listdir(data_dir) if f.endswith(".csv")] if os.path.exists(data_dir) else []

if available_csvs:
    selected_csv = st.sidebar.selectbox("Select Repository Dataset", available_csvs)
    csv_path = os.path.join(data_dir, selected_csv)
    
    # Only read if it changed or not loaded yet
    if st.session_state.raw_df is None or "last_csv" not in st.session_state or st.session_state.last_csv != selected_csv:
        try:
            st.session_state.raw_df = pd.read_csv(csv_path)
            st.session_state.last_csv = selected_csv
        except Exception as e:
            st.sidebar.error(f"Failed to load CSV: {e}")

    if st.session_state.raw_df is not None:
        st.sidebar.success(f"Loaded: `{selected_csv}` ({len(st.session_state.raw_df)} rows)")
else:
    st.sidebar.error("No CSV files found in the `data/` repository directory.")

if uploaded_policy and st.session_state.raw_df is not None:
    if st.sidebar.button("Run Full Agent Pipeline", type="primary"):
        # Reset state on run
        st.session_state.agent_1_rules = []
        st.session_state.agent_2_mapped_rules = []
        st.session_state.final_report = ""
        
        policy_text = extract_text_from_file(uploaded_policy)
        executor = PandasExecutor(st.session_state.raw_df)
        schema_info = executor.get_schema_summary()

        # --- Live Backend Logging Window ---
        st.subheader("🖥️ Live Backend Logs")
        log_container = st.empty()
        # Override output removed (no more websocket flooding!)
        
        try:
            # [AGENT 1 EXECUTION]
            with st.status("🕵️‍♂️ Agent 1: Extracting Rules & Generating Queries...", expanded=True) as status1:
                try:
                    def agent1_streamer():
                        for chunk in st.session_state.pipeline.agent_1_extract_generic_rules(policy_text):
                            if isinstance(chunk, tuple):
                                st.session_state.agent1_result = chunk
                            else:
                                yield chunk
                                
                    with st.expander("Live JSON Parsing", expanded=True):
                        st.write_stream(agent1_streamer())
                        
                    status, payload = st.session_state.agent1_result
                    
                    if status == "ERROR":
                        st.error(payload)
                        st.stop()
                        
                    st.session_state.agent_1_rules = payload
                    st.write(f"✅ Extracted {len(st.session_state.agent_1_rules)} rules.")
                    status1.update(state="complete")
                except Exception as e:
                    status1.update(label=f"Agent 1 Error: {e}", state="error")
                    st.stop()

            # [AGENT 2 EXECUTION - SINGLE BATCHED CALL]
            with st.status("🗺️ Agent 2: Mapping Schema & Values...", expanded=True) as status2:
                try:
                     result = st.session_state.pipeline.agent_2_map_all_rules(
                         st.session_state.agent_1_rules,
                         schema_info['columns'],
                         schema_info['sample_csv']
                     )
                     
                     if result[0] == "ERROR":
                         st.error(result[1])
                         st.stop()
                     
                     st.session_state.agent_2_mapped_rules = [r.model_dump() for r in result[1].mapped_rules]
                     
                     for mapped in st.session_state.agent_2_mapped_rules:
                         if mapped['status'] == 'SKIPPED':
                             st.warning(f"⚠️ Skipped '{mapped['title']}': {mapped.get('skip_reason', 'Missing columns.')}")
                         else:
                             st.success(f"✅ Mapped '{mapped['title']}' columns: {mapped['columns_remapped']}")
                     
                     status2.update(state="complete")
                except Exception as e:
                    status2.update(label=f"Agent 2 Error: {e}", state="error")
                    st.stop()
                    
            # [AGENT 3 EXECUTION]
            with st.status("⚙️ Agent 3: Executing Mapped Queries & Generating Report...", expanded=True) as status3:
                try:
                    # Run the scripts locally to get raw metrics
                    st.write("Executing Pandas queries against DataFrame...")
                    raw_metrics_json = executor.run_all_rules_and_collect_metrics(st.session_state.agent_2_mapped_rules)
                    
                    # Pass to LLM to generate Markdown Report live
                    st.write("Generating Executive Report live...")
                    
                    def report_generator():
                        for chunk in st.session_state.pipeline.agent_3_generate_executive_report(raw_metrics_json):
                            print(chunk, end="", flush=True)
                            yield chunk
                            
                    with st.expander("Viewing Agent 3 Brain (Live Report Generation)"):
                        generated_report = st.write_stream(report_generator())
                        
                    st.session_state.final_report = generated_report.strip()
                    status3.update(state="complete")
                except Exception as e:
                    status3.update(label=f"Agent 3 Error: {e}", state="error")
                    st.stop()
                    
        finally:
            # Always restore standard terminal behavior to avoid breaking Streamlit server
            sys.stdout = sys.__stdout__

def convert_md_to_pdf(md_text):
    # Convert markdown to HTML (enabling tables)
    html = markdown.markdown(md_text, extensions=['tables'])
    # Add some basic styling for the PDF
    styled_html = f"<html><head><style>body {{ font-family: Helvetica, sans-serif; }} table {{ border-collapse: collapse; width: 100%; }} th, td {{ border: 1px solid black; padding: 8px; text-align: left; }}</style></head><body>{html}</body></html>"
    result = BytesIO()
    pisa.CreatePDF(BytesIO(styled_html.encode("utf-8")), dest=result)
    return result.getvalue()

# --- Main View ---

if st.session_state.final_report:
    tab1, tab2, tab3 = st.tabs(["📑 Executive Report (Agent 3)", "🗺️ Schema Mapping (Agent 2)", "🗄️ Raw Data"])
        
    with tab1:
        st.write("### AI Generated Executive Report")
        st.markdown(st.session_state.final_report, unsafe_allow_html=True)
        
        # Download Buttons
        st.divider()
        col1, col2 = st.columns(2)
        with col1:
            st.download_button(
                label="📥 Download Report (Markdown)",
                data=st.session_state.final_report,
                file_name="compliance_report.md",
                mime="text/markdown",
                use_container_width=True
            )
        with col2:
            try:
                pdf_bytes = convert_md_to_pdf(st.session_state.final_report)
                st.download_button(
                    label="📥 Download Report (PDF)",
                    data=pdf_bytes,
                    file_name="compliance_report.pdf",
                    mime="application/pdf",
                    use_container_width=True
                )
            except Exception as e:
                st.warning(f"PDF generation unavailable: {e}")
        
    with tab2:
        st.write("This tab shows exactly how Agent 2 translated Agent 1's generic rules into executable Pandas.")
        for rule in st.session_state.agent_2_mapped_rules:
            with st.expander(f"{rule['rule_id']}: {rule['title']} ({rule['status']})"):
                st.write("**Columns Remapped:**", rule['columns_remapped'])
                st.write("**Values Remapped:**", rule['values_remapped'])
                st.code(rule['sql_query'], language="sql")
                st.write("↓ Maps To ↓")
                st.code(rule['pandas_query'], language="python")
                
    with tab3:
        st.dataframe(st.session_state.raw_df.head(100))
        
# End of file
