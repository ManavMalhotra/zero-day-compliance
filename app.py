import streamlit as st
import pandas as pd
from datetime import datetime
from fpdf import FPDF

from llm_pipeline import LLMPipeline
from executor import PandasExecutor
from utils import build_schema_context, extract_text_from_file

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

if "run_logs" not in st.session_state:
    st.session_state.run_logs = []

# --- UI Setup ---
st.set_page_config(page_title="AI Data Policy Agent", layout="wide")
st.title("🛡️ Data Policy Compliance Agent")
st.markdown("Automated PDF Policy -> Dynamic Pandas Mapping -> Executive Report")

import os


def make_streamlit_logger(placeholder):
    """Creates a UI logger that updates the visible log pane immediately."""
    def append_log(message: str):
        timestamp = datetime.now().strftime("%H:%M:%S")
        entry = f"[{timestamp}] {message}"
        st.session_state.run_logs.append(entry)
        placeholder.code("\n".join(st.session_state.run_logs[-200:]), language="text")

    return append_log

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
        schema_context = build_schema_context(st.session_state.raw_df)

        # --- Live Backend Logging Window ---
        st.subheader("🖥️ Live Backend Logs")
        log_container = st.empty()
        st.session_state.run_logs = []
        append_log = make_streamlit_logger(log_container)
        append_log("Pipeline run started.")
        append_log(f"Loaded dataset with {len(st.session_state.raw_df):,} rows and {len(schema_info['columns']):,} columns.")
        append_log(
            "Gemini model chain: "
            + ", ".join(st.session_state.pipeline.configured_models())
            + " (with free-tier rate guards and fallback)."
        )
        
        try:
            # [AGENT 1 EXECUTION]
            with st.status("🕵️‍♂️ Agent 1: Extracting Rules...", expanded=True) as status1:
                try:
                    def agent1_streamer():
                        for chunk in st.session_state.pipeline.agent_1_extract_generic_rules(policy_text, on_log=append_log):
                            if isinstance(chunk, tuple):
                                st.session_state.agent1_result = chunk
                            else:
                                yield chunk
                                
                    append_log("Agent 1 started.")
                    with st.expander("Live JSON Parsing", expanded=True):
                        st.write_stream(agent1_streamer())
                        
                    status, payload = st.session_state.agent1_result
                    
                    if status == "ERROR":
                        append_log(f"Agent 1 error: {payload}")
                        st.error(payload)
                        st.stop()
                        
                    st.session_state.agent_1_rules = payload
                    append_log(f"Agent 1 completed. Extracted {len(st.session_state.agent_1_rules)} rules.")
                    st.write(f"✅ Extracted {len(st.session_state.agent_1_rules)} rules.")
                    status1.update(state="complete")
                except Exception as e:
                    append_log(f"Agent 1 error: {e}")
                    status1.update(label=f"Agent 1 Error: {e}", state="error")
                    st.stop()

            # [AGENT 2 EXECUTION - SINGLE BATCHED CALL]
            with st.status("🗺️ Agent 2: Mapping Schema & Values...", expanded=True) as status2:
                try:
                     append_log("Agent 2 started.")
                     result = st.session_state.pipeline.agent_2_map_all_rules(
                         st.session_state.agent_1_rules,
                         schema_info['columns'],
                         schema_context,
                         on_log=append_log,
                     )
                     
                     if result[0] == "ERROR":
                         append_log(f"Agent 2 error: {result[1]}")
                         st.error(result[1])
                         st.stop()
                     
                     st.session_state.agent_2_mapped_rules = [r.model_dump() for r in result[1].mapped_rules]
                     
                     for mapped in st.session_state.agent_2_mapped_rules:
                         if mapped['status'] == 'SKIPPED':
                             st.warning(f"⚠️ Skipped '{mapped['title']}': {mapped.get('skip_reason', 'Missing columns.')}")
                         else:
                             st.success(f"✅ Mapped '{mapped['title']}' columns: {mapped['columns_remapped']}")
                     
                     append_log(f"Agent 2 completed. Prepared {len(st.session_state.agent_2_mapped_rules)} mapped rules.")
                     status2.update(state="complete")
                except Exception as e:
                    append_log(f"Agent 2 error: {e}")
                    status2.update(label=f"Agent 2 Error: {e}", state="error")
                    st.stop()
                    
            # [AGENT 3 EXECUTION]
            with st.status("⚙️ Agent 3: Executing Mapped Queries & Generating Report...", expanded=True) as status3:
                try:
                    # Run the scripts locally to get raw metrics
                    append_log("Agent 3 execution started.")
                    st.write("Executing Pandas queries against DataFrame...")
                    raw_metrics_json = executor.run_all_rules_and_collect_metrics(
                        st.session_state.agent_2_mapped_rules,
                        on_log=append_log,
                    )
                    
                    # Pass to LLM to generate Markdown Report live
                    st.write("Generating Executive Report live...")
                    
                    def report_generator():
                        for chunk in st.session_state.pipeline.agent_3_generate_executive_report(
                            raw_metrics_json,
                            on_log=append_log,
                        ):
                            yield chunk
                            
                    with st.expander("Viewing Agent 3 Brain (Live Report Generation)"):
                        generated_report = st.write_stream(report_generator())
                        
                    st.session_state.final_report = generated_report.strip()
                    append_log("Agent 3 completed. Final report is ready.")
                    status3.update(state="complete")
                except Exception as e:
                    append_log(f"Agent 3 error: {e}")
                    status3.update(label=f"Agent 3 Error: {e}", state="error")
                    st.stop()
        finally:
            append_log("Pipeline run finished.")

def convert_md_to_pdf(md_text) -> bytes:
    """Convert markdown report to PDF and return raw bytes for Streamlit downloads."""
    def pdf_safe(text: str) -> str:
        """
        fpdf's core fonts only support latin-1, so replace unsupported characters
        with close ASCII equivalents instead of failing the whole export.
        """
        replacements = {
            "📑": "",
            "📊": "",
            "🚨": "[FLAGGED]",
            "✅": "[CLEAN]",
            "⚠️": "[SKIPPED]",
            "❌": "[ERROR]",
            "🚩": "",
            "📋": "",
            "→": "->",
            "↓": "->",
            "—": "-",
            "–": "-",
            "•": "-",
        }

        cleaned = text
        for source, target in replacements.items():
            cleaned = cleaned.replace(source, target)

        return cleaned.encode("latin-1", errors="replace").decode("latin-1")

    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()
    pdf.set_font('Helvetica', size=10)
    
    for line in md_text.split('\n'):
        stripped = pdf_safe(line.strip())
        # Handle headers
        if stripped.startswith('### '):
            pdf.set_font('Helvetica', 'B', 12)
            pdf.multi_cell(0, 7, stripped[4:])
            pdf.set_font('Helvetica', size=10)
        elif stripped.startswith('## '):
            pdf.set_font('Helvetica', 'B', 14)
            pdf.multi_cell(0, 8, stripped[3:])
            pdf.set_font('Helvetica', size=10)
        elif stripped.startswith('# '):
            pdf.set_font('Helvetica', 'B', 16)
            pdf.multi_cell(0, 10, stripped[2:])
            pdf.set_font('Helvetica', size=10)
        elif stripped.startswith('|'):
            # Table rows — render as fixed-width text
            pdf.set_font('Courier', size=8)
            pdf.multi_cell(0, 5, stripped)
            pdf.set_font('Helvetica', size=10)
        elif stripped.startswith('```'):
            continue  # Skip code fences
        elif stripped == '':
            pdf.ln(3)
        else:
            pdf.multi_cell(0, 6, stripped)

    # Make the return type explicit and stable across fpdf/pyfpdf variants.
    try:
        output = pdf.output(dest="S")
    except TypeError:
        output = pdf.output()

    if isinstance(output, bytes):
        return output
    if isinstance(output, bytearray):
        return bytes(output)
    if isinstance(output, str):
        return output.encode("latin-1")

    raise TypeError(f"Unexpected PDF output type: {type(output).__name__}")

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
