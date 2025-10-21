import os
import sys
import glob
import subprocess
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st
import traceback

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

PROFILES_DIR = os.path.join("outputs", "profiles")
CLEANED_DIR = os.path.join("outputs", "cleaned_data")
CHARTS_DIR = os.path.join("dashboard", "assets", "charts")


st.set_page_config(page_title="DataIQ Dashboard", layout="wide", initial_sidebar_state="expanded")
st.sidebar.title("DataIQ – Intelligent Data Quality & Insights Suite")
st.sidebar.markdown(
    """
    DataIQ is an intelligent data quality toolkit integrating Oracle ETL and AI-driven profiling. 
    It detects anomalies, cleans data, and visualizes trends for analysts and consultants.
    
    <a href="https://github.com/Abie2023/DataIQ" target="_blank">Learn More on GitHub</a>
    """,
    unsafe_allow_html=True
)
st.sidebar.markdown("<hr>", unsafe_allow_html=True)

# Initialize session state
if "data_source" not in st.session_state:
    st.session_state["data_source"] = "Oracle Database"
if "uploaded_data" not in st.session_state:
    st.session_state["uploaded_data"] = None
if "uploaded_filename" not in st.session_state:
    st.session_state["uploaded_filename"] = None
if "profiling_result" not in st.session_state:
    st.session_state["profiling_result"] = None
if "cleaned_data" not in st.session_state:
    st.session_state["cleaned_data"] = None
if "anomaly_result" not in st.session_state:
    st.session_state["anomaly_result"] = None
if "oracle_connected" not in st.session_state:
    st.session_state["oracle_connected"] = False


def display_csv_upload_and_actions():
    """Handles the CSV upload, processing, and action buttons in the main area."""
    st.subheader("📁 Upload CSV File & Actions")
    
    # Drag and Drop File Uploader in the main area
    uploaded_file = st.file_uploader(
        "Choose a CSV file",
        type=["csv"],
        help="Upload a CSV file for analysis (max 10,000 rows will be sampled if larger)",
        key="main_area_file_uploader"
    )
    
    if uploaded_file is not None:
        if st.session_state.get("uploaded_filename") != uploaded_file.name:
            # New file uploaded, clear previous results
            st.session_state["profiling_result"] = None
            st.session_state["cleaned_data"] = None
            st.session_state["anomaly_result"] = None
            
            try:
                # Read CSV
                df = pd.read_csv(uploaded_file)
                
                # Sample if too large
                original_rows = len(df)
                if original_rows > 10000:
                    df = df.sample(n=10000, random_state=42)
                    st.warning(f"Large file detected. Sampled 10,000 rows from {original_rows:,} total.")
                
                st.session_state["uploaded_data"] = df
                st.session_state["uploaded_filename"] = uploaded_file.name
                
            except Exception as e:
                st.error(f"❌ Error reading CSV: {e}")
                st.session_state["uploaded_data"] = None
    
    st.markdown("---")
    
    # Action buttons for CSV mode
    st.subheader("🚀 Actions")
    data_loaded = st.session_state["uploaded_data"] is not None
    
    col_prof, col_clean, col_detect = st.columns(3)
    run_prof_csv = col_prof.button("🔍 Run Profiling", disabled=not data_loaded, help="Generate a data quality profile for the dataset.")
    run_clean_csv = col_clean.button("🧹 Run Cleaning", disabled=not data_loaded, help="Clean duplicates, handle nulls, and normalize strings.")
    run_detect_csv = col_detect.button("🎯 Detect Anomalies", disabled=not data_loaded, help="Run AI-driven anomaly detection on the data.")
    
    
    # --- Action Logic ---
    if run_prof_csv and data_loaded:
        with st.spinner("Profiling data..."):
            try:
                from dataiq.data_profiler import DataProfiler
                profiler = DataProfiler()
                profile_result = profiler.profile(st.session_state["uploaded_data"], 
                                                     name=st.session_state["uploaded_filename"].replace(".csv", ""))
                score = profiler.generate_data_health_score(profile_result)
                st.session_state["profiling_result"] = {
                    "profile_df": profile_result["per_column"],  # DataFrame
                    "overall": profile_result["overall"],  # Dict
                    "score": score
                }
                st.success(f"✅ Profiling complete! Health Score: {score:.2f}")
                st.rerun() # Rerun to display results
            except Exception as e:
                st.error(f"❌ Profiling failed: {e}")
                st.text(traceback.format_exc())
    
    if run_clean_csv and data_loaded:
        with st.spinner("Cleaning data..."):
            try:
                from dataiq.data_cleaner import DataCleaner
                cleaner = DataCleaner()
                df = st.session_state["uploaded_data"]
                df1 = cleaner.clean_duplicates(df)
                df2 = cleaner.handle_nulls(df1, strategy="fill_mean")
                df3 = cleaner.normalize_strings(df2)
                st.session_state["cleaned_data"] = df3
                st.success(f"✅ Cleaning complete! {len(df3):,} rows cleaned.")
                st.rerun() # Rerun to display results
            except Exception as e:
                st.error(f"❌ Cleaning failed: {e}")
                st.text(traceback.format_exc())
    
    if run_detect_csv and data_loaded:
        with st.spinner("Detecting anomalies..."):
            try:
                from dataiq.anomaly_detector import AnomalyDetector
                detector = AnomalyDetector()
                # Use cleaned data if available, otherwise use uploaded data
                df = st.session_state["cleaned_data"] if st.session_state["cleaned_data"] is not None else st.session_state["uploaded_data"]
                result = detector.detect(df)
                st.session_state["anomaly_result"] = result
                st.success("✅ Anomaly detection complete!")
                st.rerun() # Rerun to display results
            except Exception as e:
                st.error(f"❌ Anomaly detection failed: {e}")
                st.text(traceback.format_exc())
    
    st.markdown("---")


# ==================== SIDEBAR ====================
st.sidebar.header("⚙️ Configuration")
data_source = st.sidebar.radio(
    "Select Data Source",
    ["CSV Upload", "Oracle Database"],
    key="data_source_radio",
    help="Choose between Oracle database connection or CSV file upload"
)

# Reset cache when switching data sources
if data_source != st.session_state["data_source"]:
    st.session_state["data_source"] = data_source
    st.session_state["uploaded_data"] = None
    st.session_state["profiling_result"] = None
    st.session_state["cleaned_data"] = None
    st.session_state["anomaly_result"] = None
    st.rerun()

st.sidebar.markdown("---")

# ==================== ORACLE MODE (Minimal Sidebar Logic) ====================
selected_table = "(no profiles yet)"
if data_source == "Oracle Database":
    st.sidebar.subheader("🗄️ Table Selection")
    
    # Check Oracle connectivity once and store the result
    oracle_connected = st.session_state["oracle_connected"]
    if not oracle_connected:
        try:
            from dataiq.oracle_connector import OracleConnector
            conn = OracleConnector()
            oracle_connected = conn.test_connection()
            conn.dispose()
            st.session_state["oracle_connected"] = oracle_connected
        except Exception:
             st.session_state["oracle_connected"] = False

    # Table selection based on existing profiles
    profile_files = sorted(glob.glob(os.path.join(PROFILES_DIR, "profile_*.csv")))
    tables = [os.path.splitext(os.path.basename(p))[0].replace("profile_", "") for p in profile_files]
    selected_table = st.sidebar.selectbox("Select table (based on saved profiles)", options=tables if tables else ["(no profiles yet)"])
    
# ==================== MAIN AREA ====================
st.header(f"{'📊' if data_source == 'CSV Upload' else '🗄️'} Data Source: {data_source}")
st.markdown("<hr>", unsafe_allow_html=True)


# --- CSV MODE MAIN DISPLAY ---
if data_source == "CSV Upload":
    display_csv_upload_and_actions() # <--- Function call for CSV upload and actions
    
    if st.session_state["uploaded_data"] is None:
        st.info("⬆️ **Upload a CSV file above to begin data quality analysis.**")
    else:
        df = st.session_state["uploaded_data"]
        
        ## Dataset Overview
        st.subheader(f"📄 Dataset: {st.session_state['uploaded_filename']}")
        
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Rows", f"{len(df):,}")
        col2.metric("Total Columns", f"{len(df.columns)}")
        col3.metric("Missing Values", f"{df.isnull().sum().sum():,}")
        col4.metric("Duplicate Rows", f"{df.duplicated().sum():,}")
        
        # Show sample data
        with st.expander("📋 View Sample Data (First 10 rows)"):
            st.dataframe(df.head(10), width=None)
        
        st.markdown("---")
        
        ## Profiling Results
        if st.session_state["profiling_result"] is not None:
            st.subheader("🔍 Profiling Results")
            result = st.session_state["profiling_result"]
            
            # Health Score and Overall Metrics
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("📈 Data Health Score", f"{result['score']:.2f} / 100")
            
            if "overall" in result:
                overall = result["overall"]
                col2.metric("Total Rows", f"{overall.get('rows', 0):,}")
                col3.metric("Total Nulls", f"{overall.get('total_nulls', 0):,}")
                col4.metric("Duplicate Rows", f"{overall.get('duplicate_rows', 0):,}")
            
            st.markdown("---")
            
            # Profile DataFrame
            profile_df = result['profile_df']
            with st.expander("📊 View Per-Column Profile", expanded=True):
                st.dataframe(profile_df, width=None, height=400)
            
            # Download profiling results
            csv = profile_df.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="💾 Download Profile as CSV",
                data=csv,
                file_name=f"profile_{st.session_state['uploaded_filename']}",
                mime="text/csv"
            )
        
        st.markdown("---")
        
        ## Cleaned Data
        if st.session_state["cleaned_data"] is not None:
            st.subheader("🧹 Cleaned Data")
            cleaned_df = st.session_state["cleaned_data"]
            
            col1, col2 = st.columns(2)
            col1.metric("Cleaned Rows", f"{len(cleaned_df):,}")
            col2.metric("Remaining Nulls", f"{cleaned_df.isnull().sum().sum():,}")
            
            with st.expander("📋 View Cleaned Data (First 10 rows)"):
                st.dataframe(cleaned_df.head(10), width=None)
            
            # Download cleaned data
            csv = cleaned_df.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="💾 Download Cleaned Data as CSV",
                data=csv,
                file_name=f"cleaned_{st.session_state['uploaded_filename']}",
                mime="text/csv"
            )
        
        st.markdown("---")
        
        ## Anomaly Detection Results
        if st.session_state["anomaly_result"] is not None:
            st.subheader("🎯 Anomaly Detection Results")
            anomaly_data = st.session_state["anomaly_result"]
            
            # Display anomaly chart if it exists
            chart_files = sorted(glob.glob(os.path.join(CHARTS_DIR, "anomalies_*.png")))
            if chart_files:
                st.image(chart_files[-1], caption="Anomalies Detected per Column", use_column_width=True)
            
            # Show anomaly counts
            if "anomalies_per_column" in anomaly_data:
                st.write("**Anomalies per Column:**")
                anomaly_counts = pd.DataFrame(list(anomaly_data["anomalies_per_column"].items()), 
                                                 columns=["Column", "Anomaly Count"])
                st.dataframe(anomaly_counts, width=None)

# --- ORACLE MODE MAIN DISPLAY ---
else:  # Oracle Database
    oracle_connected = st.session_state.get("oracle_connected", False)
    
    st.subheader("🚀 Actions and Status")
    
    # Display Oracle connectivity status
    if oracle_connected:
        st.success("✅ Oracle Database is Connected.")
    else:
        st.error("❌ Oracle Database is Disconnected or Unavailable. Check your connection or run a local profile using the command line.")
    
    st.markdown("---")
    
    # Move action buttons to main area for Oracle
    col1, col2 = st.columns(2)
    
    # Disable buttons if no table is selected or not connected
    is_disabled = not oracle_connected or selected_table == "(no profiles yet)"
    
    run_prof = col1.button("🔍 Run Profiling on Selected Table", disabled=is_disabled, help="Pulls data from Oracle, profiles it, and saves the profile locally.")
    run_clean = col2.button("🧹 Run Cleaning on Selected Table", disabled=is_disabled, help="Pulls data, cleans it using dataiq.data_cleaner, and saves the cleaned data locally.")
    
    # Handle button clicks
    if run_prof:
        with st.spinner(f"Running profiling for table **{selected_table}**..."):
            cmd = ["python", "main.py", "--mode", "profile", "--table", selected_table, "--limit", "1000"]
            try:
                subprocess.run(cmd, check=True)
                st.success("✅ Profiling complete")
                st.rerun()
            except subprocess.CalledProcessError as e:
                st.error(f"❌ Profiling failed: {e}")
            except Exception as e:
                st.error(f"❌ Profiling failed (Execution error): {e}")

    if run_clean:
        with st.spinner(f"Running cleaning for table **{selected_table}**..."):
            cmd = ["python", "main.py", "--mode", "clean", "--table", selected_table, "--limit", "1000"]
            try:
                subprocess.run(cmd, check=True)
                st.success("✅ Cleaning complete")
                st.rerun()
            except subprocess.CalledProcessError as e:
                st.error(f"❌ Cleaning failed: {e}")
            except Exception as e:
                st.error(f"❌ Cleaning failed (Execution error): {e}")

    st.markdown("---")
    
    # Show profiling data
    if profile_files and selected_table != "(no profiles yet)":
        profile_path = os.path.join(PROFILES_DIR, f"profile_{selected_table}.csv")
        if os.path.exists(profile_path):
            df_prof = pd.read_csv(profile_path)
            st.subheader(f"🔍 Profile: {selected_table}")
            
            # Key metrics
            col1, col2, col3, col4 = st.columns(4)
            
            try:
                # Safely extract metrics
                if 'metric' in df_prof.columns and 'value' in df_prof.columns:
                    metrics_dict = dict(zip(df_prof['metric'], df_prof['value']))
                    total_rows = metrics_dict.get('total_rows', 0)
                    null_pct = metrics_dict.get('null_percentage', 0)
                    dup_pct = metrics_dict.get('duplicate_percentage', 0)
                else:
                    # Try alternate column structure
                    total_rows = df_prof["total_rows"].iloc[0] if "total_rows" in df_prof.columns else len(df_prof)
                    null_count = df_prof["null_count"].sum() if "null_count" in df_prof.columns else 0
                    duplicate_rows = df_prof["duplicate_rows"].iloc[0] if "duplicate_rows" in df_prof.columns else 0
                    # Recalculate percentages safely
                    null_pct = (null_count / (total_rows * len(df_prof))) * 100 if total_rows > 0 and "null_count" in df_prof.columns and len(df_prof) > 0 else 0
                    dup_pct = (duplicate_rows / total_rows) * 100 if total_rows > 0 and "duplicate_rows" in df_prof.columns else 0
                
                col1.metric("Total Rows", f"{int(total_rows):,}")
                col2.metric("Null %", f"{float(null_pct):.2f}%")
                col3.metric("Duplicate %", f"{float(dup_pct):.2f}%")
                col4.metric("Columns Analyzed", len(df_prof))
            except Exception as e:
                col1.metric("Total Rows", "N/A")
                col2.metric("Null %", "N/A")
                col3.metric("Duplicate %", "N/A")
                col4.metric("Columns", len(df_prof))
                st.warning(f"Could not parse all metrics: {e}")
                
            # Show dataframe
            with st.expander("View Full Profile Data", expanded=True):
                st.dataframe(df_prof, width=None, height=400)
            
            # Download profile
            csv = df_prof.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="💾 Download Profile as CSV",
                data=csv,
                file_name=f"profile_{selected_table}.csv",
                mime="text/csv"
            )
        else:
            st.info(f"Profile file not found for **{selected_table}**. Click '🔍 Run Profiling on Selected Table' to generate it.")
    else:
        st.info("No profiles found. Select a table from the sidebar or click '🔍 Run Profiling' above to generate a new profile.")
    
    st.markdown("---")
    
    # Show cleaned data if available
    clean_files = sorted(glob.glob(os.path.join(CLEANED_DIR, "cleaned_*.csv")))
    if clean_files and selected_table != "(no profiles yet)":
        clean_path = os.path.join(CLEANED_DIR, f"cleaned_{selected_table}.csv")
        if os.path.exists(clean_path):
            st.subheader(f"🧹 Cleaned Data: {selected_table}")
            df_clean = pd.read_csv(clean_path)
            
            col1, col2 = st.columns(2)
            col1.metric("Cleaned Rows", f"{len(df_clean):,}")
            col2.metric("Remaining Nulls", f"{df_clean.isnull().sum().sum():,}")
            
            with st.expander("View Cleaned Data (First 10 rows)"):
                st.dataframe(df_clean.head(10), width=None)
            
            # Download cleaned data
            csv = df_clean.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="💾 Download Cleaned Data as CSV",
                data=csv,
                file_name=f"cleaned_{selected_table}.csv",
                mime="text/csv"
            )
            
            st.markdown("---")
        else:
            st.info(f"Cleaned data file not found for **{selected_table}**. Click '🧹 Run Cleaning on Selected Table' to generate it.")
    
    # Show anomaly detection charts
    st.subheader("🎯 Anomaly Detection Chart")
    chart_paths = sorted(glob.glob(os.path.join(CHARTS_DIR, "anomalies_*.png")))
    if chart_paths:
        st.image(chart_paths[-1], caption="Anomalies Detected per Column", use_column_width=True)
    else:
        st.info("No anomaly charts yet. Run detection via main.py --mode detect.")


st.markdown("<hr>", unsafe_allow_html=True)
st.markdown("*DataIQ v1.0.0 – Powered by Oracle, Python & AI*")