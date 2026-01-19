import streamlit as st
import requests
import json
import pandas as pd
from datetime import datetime

# Configuration
API_URL = "http://localhost:8000"

st.set_page_config(
    page_title="DataFactory 2.0 Dashboard",
    page_icon="🏭",
    layout="wide"
)

# Utils
def format_date(date_str):
    if not date_str:
        return "N/A"
    try:
        dt = datetime.fromisoformat(date_str)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except:
        return date_str

def api_request(method, endpoint, **kwargs):
    url = f"{API_URL}{endpoint}"
    try:
        if method == "GET":
            response = requests.get(url, **kwargs)
        elif method == "POST":
            response = requests.post(url, **kwargs)
        elif method == "PUT":
            response = requests.put(url, **kwargs)
        elif method == "DELETE":
            response = requests.delete(url, **kwargs)
        
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        st.error(f"API Error ({url}): {e}")
        if hasattr(e, 'response') and e.response is not None:
             try:
                 st.error(f"Details: {e.response.json().get('detail', 'Unknown error')}")
             except:
                 st.error(f"Response: {e.response.text}")
        return None

def upload_file(uploaded_file):
    """Upload file to API and return the saved path"""
    url = f"{API_URL}/upload/file"
    try:
        files = {'file': (uploaded_file.name, uploaded_file.getvalue())}
        response = requests.post(url, files=files)
        response.raise_for_status()
        return response.json().get('file_path')
    except Exception as e:
        st.error(f"File upload failed: {e}")
        return None

def render_config_inputs(ctype, mode="source"):
    """Render inputs for a connector and return the raw input dict"""
    inputs = {}
    key_prefix = f"{mode}_{ctype}"
    
    if ctype == "odbc":
        inputs['dsn'] = st.text_input("DSN", key=f"{key_prefix}_dsn")
        inputs['database'] = st.text_input("Database", key=f"{key_prefix}_db")
        inputs['schema'] = st.text_input("Schema", value="dbo", key=f"{key_prefix}_schema")
        
    elif ctype in ["postgresql", "mysql"]:
        col1, col2 = st.columns(2)
        with col1:
            inputs['host'] = st.text_input("Host", value="localhost", key=f"{key_prefix}_host")
            inputs['database'] = st.text_input("Database", key=f"{key_prefix}_db")
            inputs['user'] = st.text_input("User", key=f"{key_prefix}_user")
        with col2:
            inputs['port'] = st.number_input("Port", value=5432 if ctype == "postgresql" else 3306, key=f"{key_prefix}_port")
            inputs['password'] = st.text_input("Password", type="password", key=f"{key_prefix}_pass")
            if mode == "source" and ctype == "postgresql":
                inputs['schema'] = st.text_input("Schema", value="public", key=f"{key_prefix}_schema")

    elif mode == "source" and ctype in ["csv", "json", "excel"]:
        inputs['file'] = st.file_uploader(f"Upload {ctype.upper()}", type=[ctype if ctype != 'excel' else 'xlsx'], key=f"{key_prefix}_file")
        if ctype == "excel":
            inputs['sheet_name'] = st.text_input("Sheet Name (Optional)", key=f"{key_prefix}_sheet")
            
    elif ctype == "sqlite":
        # SQLite is usually a file path on the server/local, but could be uploaded if source
        inputs['database_path'] = st.text_input("Database Path", help="Absolute path on the server", key=f"{key_prefix}_dbpath")
        
    elif mode == "sink" and ctype in ["csv", "json", "parquet"]:
        inputs['directory'] = st.text_input("Output Directory", help="Absolute path on the server", key=f"{key_prefix}_dir")
        
    return inputs

# Sidebar Navigation
st.sidebar.title("DataFactory 2.0")
page = st.sidebar.radio("Navigation", ["Dashboard", "Job Management", "Create Job", "Global History", "Sink Viewer", "Scheduler", "Connectors Reference"])

# --- DASHBOARD ---
if page == "Dashboard":
    st.title("🏭 Dashboard")
    
    # Health Check
    health = api_request("GET", "/health")
    if health:
        col1, col2, col3 = st.columns(3)
        with col1:
             st.metric("System Status", health.get("status", "Unknown").upper(), delta="Healthy" if health.get("status") == "healthy" else "Error")
        with col2:
             st.metric("Scheduler Status", health.get("scheduler", "Unknown").upper())
        with col3:
             st.metric("Last Check", format_date(health.get("timestamp")))
    
    # Quick Stats
    jobs_data = api_request("GET", "/jobs")
    if jobs_data:
        jobs = jobs_data.get("jobs", [])
        total_jobs = len(jobs)
        enabled_jobs = sum(1 for j in jobs if j.get('schedule')) # Rough proxy for enabled if scheduled
        
        col1, col2 = st.columns(2)
        col1.info(f"Total Jobs: {total_jobs}")
        # col2.success(f"Scheduled Jobs: {enabled_jobs}")

# --- JOB MANAGEMENT ---
elif page == "Job Management":
    st.title("🛠 Job Management")
    
    # List Jobs
    jobs_data = api_request("GET", "/jobs")
    if jobs_data:
        jobs = jobs_data.get("jobs", [])
        
        if not jobs:
            st.info("No jobs found. Go to 'Create Job' to make one.")
        else:
            for job in jobs:
                with st.expander(f"Job: {job['job_name']} (ID: {job['job_id']})"):
                    col1, col2 = st.columns([3, 1])
                    
                    with col1:
                        st.write(f"**Source:** {job['source_type']}")
                        st.write(f"**Sink:** {job['sink_type']}")
                        st.write(f"**Schedule:** {job['schedule'] if job.get('schedule') else 'Manual'}")
                        if job.get('last_run'):
                           last_run = job['last_run']
                           status_color = "green" if last_run.get('status') == 'success' else "red"
                           st.markdown(f"**Last Run:** :{status_color}[{last_run.get('status').upper()}] at {format_date(last_run.get('start_time'))}")
                        else:
                            st.write("**Last Run:** Never")

                    with col2:
                        if st.button("Execute Now", key=f"exec_{job['job_id']}"):
                            res = api_request("POST", f"/jobs/{job['job_id']}/execute")
                            if res:
                                st.success("Job started successfully!")
                                st.json(res)
                        
                        if st.button("Delete", key=f"del_{job['job_id']}", type="primary"):
                            if api_request("DELETE", f"/jobs/{job['job_id']}"):
                                st.success("Job deleted!")
                                st.rerun()

                    # Tabs for Details and History
                    tab1, tab2 = st.tabs(["Configuration", "Execution History"])
                    
                    with tab1:
                        st.json({
                            "source_config": job.get('source_config'),
                            "sink_config": job.get('sink_config'),
                            "query": job.get('query')
                        })
                    
                    with tab2:
                        history_data = api_request("GET", f"/jobs/{job['job_id']}/history")
                        if history_data:
                            hist_df = pd.DataFrame(history_data.get("history", []))
                            if not hist_df.empty:
                                st.dataframe(
                                    hist_df[['history_id', 'status', 'started_at', 'completed_at', 'records_processed', 'error_message']],
                                    use_container_width=True
                                )
                                
                                # View Logs
                                selected_hist_id = st.selectbox("Select Execution ID to view logs", hist_df['history_id'].unique(), key=f"hist_sel_{job['job_id']}")
                                if st.button("View Logs", key=f"logs_btn_{job['job_id']}"):
                                     logs_data = api_request("GET", f"/logs/{selected_hist_id}")
                                     if logs_data:
                                         st.text_area("Logs", "\n".join([f"{l['timestamp']} [{l['level']}] {l['message']}" for l in logs_data.get("logs", [])]), height=200)
                            else:
                                st.info("No execution history.")


# --- CREATE JOB ---
elif page == "Create Job":
    st.title("➕ Create New Job")
    
    col1, col2 = st.columns(2)
    with col1:
        job_name = st.text_input("Job Name")
        source_type = st.selectbox("Source Type", ["odbc", "postgresql", "mysql", "csv", "json", "excel"])
    with col2:
        schedule = st.text_input("Cron Schedule (Optional)", placeholder="*/5 * * * *")
        sink_type = st.selectbox("Sink Type", ["sqlite", "postgresql", "mysql", "csv", "json", "parquet"])
        
    query = st.text_area("SQL Query (Optional)", placeholder="SELECT * FROM table")

    st.subheader("Configuration")
    col3, col4 = st.columns(2)
    
    with col3:
        st.markdown(f"**{source_type.upper()} Source Config**")
        source_inputs = render_config_inputs(source_type, "source")
        
    with col4:
        st.markdown(f"**{sink_type.upper()} Sink Config**")
        sink_inputs = render_config_inputs(sink_type, "sink")
        
    if st.button("Create Job", type="primary"):
        if not job_name:
            st.error("Job Name is required")
        else:
            # Process Source Config
            source_config = {}
            # Handle File Uploads
            if source_type in ["csv", "json", "excel"]:
                f = source_inputs.get('file')
                if not f:
                    st.error("Please upload a source file")
                    st.stop()
                
                path = upload_file(f)
                if not path:
                    st.stop()
                
                source_config['file_path'] = path
                if source_type == 'excel' and source_inputs.get('sheet_name'):
                    source_config['sheet_name'] = source_inputs['sheet_name']
            else:
                # Copy simple inputs
                source_config = {k: v for k, v in source_inputs.items() if v}

            # Process Sink Config
            sink_config = {k: v for k, v in sink_inputs.items() if v}

            payload = {
                "job_name": job_name,
                "source_type": source_type,
                "source_config": source_config,
                "sink_type": sink_type,
                "sink_config": sink_config,
                "query": query if query and query.strip() else None,
                "schedule": schedule if schedule and schedule.strip() else None
            }
            
            res = api_request("POST", "/jobs", json=payload)
            if res:
                st.success(f"Job '{job_name}' created successfully! ID: {res.get('job_id')}")

# --- GLOBAL HISTORY ---
elif page == "Global History":
    st.title("📜 Global Execution History")
    
    limit = st.slider("Rows to fetch", 10, 500, 100)
    history_data = api_request("GET", f"/history?limit={limit}")
    
    if history_data:
        hist_list = history_data.get("history", [])
        if hist_list:
            df = pd.DataFrame(hist_list)
            st.dataframe(
                df[['history_id', 'job_id', 'status', 'started_at', 'completed_at', 'records_processed', 'error_message']],
                use_container_width=True
            )
        else:
            st.info("No history records found.")

# --- SINK VIEWER ---
elif page == "Sink Viewer":
    st.title("🔎 Sink Data Viewer")
    st.markdown("Inspect data in the destination (Sink) of your jobs")
    
    jobs_data = api_request("GET", "/jobs")
    if jobs_data and jobs_data.get("jobs"):
        jobs = jobs_data.get("jobs", [])
        job_options = {f"{j['job_name']} ({j['sink_type']})": j['job_id'] for j in jobs}
        
        selected_job_name = st.selectbox("Select Job", list(job_options.keys()))
        selected_job_id = job_options[selected_job_name]
        
        if selected_job_id:
            try:
                # Fetch objects (tables/files)
                objects_res = api_request("GET", f"/jobs/{selected_job_id}/sink/objects")
                if objects_res and objects_res.get("objects"):
                    objects = objects_res.get("objects")
                    selected_object = st.selectbox("Select Table/File", objects)
                    
                    limit = st.slider("Preview Limit", 10, 500, 100)
                    
                    if st.button("Load Preview"):
                        with st.spinner("Loading data..."):
                            data_res = api_request("GET", f"/jobs/{selected_job_id}/sink/preview?object_name={selected_object}&limit={limit}")
                            if data_res and data_res.get("data"):
                                df = pd.DataFrame(data_res.get("data"))
                                st.dataframe(df, use_container_width=True)
                                st.caption(f"Showing {len(df)} rows")
                            else:
                                st.warning("No data found or empty table.")
                else:
                    st.warning("No tables or files found in this sink, or connection failed.")
            except Exception as e:
                st.error(f"Error connecting to sink: {e}")
    else:
        st.info("No jobs defined yet.")

# --- SCHEDULER ---
elif page == "Scheduler":
    st.title("⏰ Scheduler Status")
    
    col1, col2 = st.columns(2)
    with col1:
        if st.button("Pause Scheduler"):
            res = api_request("POST", "/scheduler/pause")
            if res: st.success("Scheduler paused")
            
    with col2:
        if st.button("Resume Scheduler"):
            res = api_request("POST", "/scheduler/resume")
            if res: st.success("Scheduler resumed")

    st.subheader("Scheduled Jobs")
    sched_jobs = api_request("GET", "/scheduler/jobs")
    
    if sched_jobs:
        jobs_list = sched_jobs.get("scheduled_jobs", [])
        if jobs_list:
            st.table(pd.DataFrame(jobs_list))
        else:
            st.info("No jobs currently scheduled.")

# --- CONNECTORS REFERENCE ---
elif page == "Connectors Reference":
    st.title("🔌 Connectors Reference")
    st.markdown("Use these examples to configure your jobs.")
    
    connectors = api_request("GET", "/connectors")
    
    if connectors:
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Source Connectors")
            for conn in connectors.get("source_connectors", []):
                with st.expander(f"{conn['type'].upper()} - {conn['description']}"):
                    st.code(json.dumps(conn['config_example'], indent=4), language="json")

        with col2:
            st.subheader("Sink Connectors")
            for conn in connectors.get("sink_connectors", []):
                with st.expander(f"{conn['type'].upper()} - {conn['description']}"):
                    st.code(json.dumps(conn['config_example'], indent=4), language="json")
