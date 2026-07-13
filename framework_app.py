import streamlit as st
import pandas as pd
import time

# Page configuration
st.set_page_config(page_title="Enterprise Framework Simulator", layout="wide")

st.title("✈️ Metadata-Driven Airport Logistics & Data Framework")
st.markdown("""
This application visualizes a **Single Metadata-Driven Architecture** using an airport baggage routing system[cite: 1]. 
Click **Trigger Live Baggage Ingestion Run** below to see exactly **which** framework runs, **where** it executes, and **how** it works at each stage[cite: 1].
""")
st.markdown("---")

# ==========================================
# SIDEBAR: STEP 2 - THE METADATA CONTROL LAYER
# ==========================================
st.sidebar.header("📁 Step 2: Metadata Repository")
st.sidebar.markdown("Modify the active control tables driving the backend framework.")

# Ingestion Config
st.sidebar.subheader("🔌 Connection Settings (Step 4)")
terminal_source = st.sidebar.selectbox("Active Intake Source", ["International Terminal Terminal-3", "Domestic Terminal Terminal-1"])

# Data Quality Config
st.sidebar.subheader("🧳 Security Validation Rules (Step 10)")
max_weight = st.sidebar.slider("Maximum Legal Bag Weight (kg)", min_value=20, max_value=50, value=32)
route_mode = st.sidebar.selectbox("On Rules Breach (Anomaly)", ["Quarantine & Flag for Inspection", "Fail Conveyor Belt Immediately"])

# ==========================================
# MAIN PAGE: LIVE CONFIGURATION
# ==========================================
st.subheader("📋 Active Control Table: `Dataset_Config` & `Data_Quality_Config`")
st.markdown("Before touching a single piece of data, the processing engine reads this central repository to build its logic dynamically[cite: 1].")

metadata_matrix = {
    "Ingestion Source": [terminal_source],
    "Target Lake Location": ["dbfs:/mnt/bronze/baggage_stream/"],
    "Max Weight Limit Check": [f"{max_weight} kg"],
    "Anomaly Policy": [route_mode],
    "Framework Status": ["ONLINE (Metadata-Driven)"]
}
st.table(pd.DataFrame(metadata_matrix))

st.info("""
💡 **METADATA CONTROL LAYER (Step 2)**[cite: 1]
* **WHICH FRAMEWORK:** Databricks Delta Tables + Unity Catalog Governance[cite: 1].
* **WHERE IT LIVES:** Centrally stored in Azure cloud storage, queried at runtime by processing workflows[cite: 1].
* **HOW IT WORKS:** The core code remains 100% generic[cite: 1]. It intercepts these parameters dynamically to drive ingestion rules, eliminating code redeployments when airport parameters change[cite: 1].
""")

st.markdown("---")

# Execution trigger
st.subheader("⚡ Databricks Workflow Orchestration Engine (Step 5)")
if st.button("Trigger Live Baggage Ingestion Run", type="primary"):
    
    # Status tracking placeholder
    status_bar = st.empty()
    
    # ------------------------------------------
    # 1. INGESTION FRAMEWORK
    # ------------------------------------------
    status_bar.info("📡 Steps 1, 3 & 4: Ingestion framework authenticating and establishing stream connection...")
    time.sleep(1.0)
    
    st.markdown("### 📡 1. Ingestion Framework Stage (Steps 1, 3 & 4)")
    st.caption("Four bags cross the scanner. Notice Bag #3 is overweight and Bag #4 has a sensor glitch (-5kg).")
    
    raw_bags = pd.DataFrame({
        "baggage_id": ["BAG-9921", "BAG-9922", "BAG-9923", "BAG-9924"],
        "passenger_name": ["Alice Green", "Bob Miller", "Charlie Smith", "Diana Prince"],
        "weight_kg": [18, 25, 65, -5],  
        "destination_flight": ["AI-101", "EK-203", "LH-430", "SQ-012"]
    })
    st.dataframe(raw_bags)
    
    with st.expander("📘 FRAMEWORK MAPPER: Ingestion Engine", expanded=True):
        st.markdown(f"""
        * **WHICH FRAMEWORK:** Parameterized Databricks Notebooks + Spark Structured Streaming + Azure Key Vault + Entra ID[cite: 1].
        * **WHERE IT LIVES / RUNS:** Deployed on Azure Databricks compute clusters sitting securely at the boundary between terminal hardware APIs and cloud storage[cite: 1].
        * **HOW IT WORKS:** Fetches secure secrets from Key Vault, completes an encrypted handshake with the source terminal stream ({terminal_source}), and loads the data raw into the **Bronze Layer**[cite: 1].
        """)
    st.markdown("---")
    
    # ------------------------------------------
    # 2. LANDING & IMMUTABILITY
    # ------------------------------------------
    status_bar.info("📥 Step 8: Writing raw stream to Immutable Bronze Delta Lake...")
    time.sleep(1.0)
    
    st.markdown("### 🟫 2. Landing Zone & Data Audit Stage (Step 8)")
    
    bronze_df = raw_bags.copy()
    bronze_df["_ingested_at_time"] = pd.Timestamp.now()
    bronze_df["_batch_ingest_id"] = "BATCH-RUN-2026-07-13-A"
    bronze_df["_source_terminal"] = terminal_source
    st.dataframe(bronze_df)
    
    with st.expander("📘 FRAMEWORK MAPPER: Landing Zone (Bronze Layer)", expanded=True):
        st.markdown("""
        * **WHICH FRAMEWORK:** Azure Data Lake Storage Gen2 (ADLS Gen2) + Delta Lake File Format[cite: 1].
        * **WHERE IT LIVES / RUNS:** Enterprise Distributed Cloud Object Storage[cite: 1].
        * **HOW IT WORKS:** Saves a **raw, completely untouched, immutable copy** of the original scanner signals[cite: 1]. Appends tracking tags (`_batch_ingest_id`) so engineers can replay historical events or audit sensor glitches without losing the original state[cite: 1].
        """)
    st.markdown("---")

    # ------------------------------------------
    # 3. QC & RECONCILIATION FRAMEWORK
    # ------------------------------------------
    status_bar.info("⚙️ Step 10: Running Quality Control (QC) & Validation Engine...")
    time.sleep(1.2)
    
    st.markdown("### 🥈 3. Quality Control (QC) & Reconciliation Stage (Step 10)")
    
    # Evaluate rules
    is_valid = (bronze_df["weight_kg"] > 0) & (bronze_df["weight_kg"] <= max_weight)
    silver_clean = bronze_df[is_valid].copy()
    quarantine_df = bronze_df[~is_valid].copy()
    
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("#### ✅ Clean Silver Table")
        if not silver_clean.empty:
            st.dataframe(silver_clean[["baggage_id", "passenger_name", "weight_kg", "destination_flight"]])
        else:
            st.warning("No baggage met the current security rules configuration.")
            
    with col2:
        st.markdown("#### ⚠️ Automated Quarantine Table")
        if not quarantine_df.empty:
            st.error(f"Isolated {len(quarantine_df)} bag(s) breaching active airport rules.")
            st.dataframe(quarantine_df[["baggage_id", "passenger_name", "weight_kg", "destination_flight"]])
        else:
            st.success("Perfect Run! 0 anomalies or rule breaches detected.")

    with st.expander("📘 FRAMEWORK MAPPER: Quality Control (QC) & Reconciliation", expanded=True):
        st.markdown(f"""
        * **WHICH FRAMEWORK:** Databricks Ingestion Engine / Delta Live Tables (DLT) Expectations[cite: 1].
        * **WHERE IT LIVES / RUNS:** Distributed in-memory Apache Spark execution tier inside Databricks clusters[cite: 1].
        * **HOW IT WORKS (QC):** Evaluates row-level records against the active rules dynamically pulled from the metadata repo[cite: 1]. Corrupted/dangerous elements are isolated into a **Quarantine Table** to prevent downstream data contamination[cite: 1].
        * **HOW IT WORKS (RECONCILIATION):** Performs record-count matching and source-to-target calculations to verify that `Input Count ({len(raw_bags)})` exactly equals `Clean Count ({len(silver_clean)}) + Quarantined Count ({len(quarantine_df)})`, ensuring zero data loss[cite: 1].
        """)
    st.markdown("---")

    # ------------------------------------------
    # 4. BUSINESS AGGREGATION (GOLD DATA PRODUCT)
    # ------------------------------------------
    status_bar.info("🏆 Step 11 & 12: Generating final operational business data product...")
    time.sleep(1.0)
    
    st.markdown("### 🪙 4. Flight Logistics Analytics Stage (Gold Layer)")
    
    if not silver_clean.empty:
        metric1, metric2, metric3 = st.columns(3)
        with metric1:
            st.metric(label="Total Bags Cleared for Flight Loading", value=int(len(silver_clean)))
        with metric2:
            st.metric(label="Total Valid Cargo Weight", value=f"{silver_clean['weight_kg'].sum()} kg")
        with metric3:
            st.metric(label="Average Safe Bag Weight", value=f"{silver_clean['weight_kg'].mean():.1f} kg")
    else:
        st.error("Dashboard Blank: No clean data available to calculate flight load metrics.")

    with st.expander("📘 FRAMEWORK MAPPER: Gold Business Aggregations", expanded=True):
        st.markdown("""
        * **WHICH FRAMEWORK:** Databricks SQL Warehouses + Delta Lake MERGE Operations + Unity Catalog[cite: 1].
        * **WHERE IT LIVES / RUNS:** High-performance analytical compute tier connected to management dashboards[cite: 1].
        * **HOW IT WORKS:** Aggregates and transforms clean, structured Silver records into production-ready analytical metrics, business definitions, and key performance indicators (KPIs) tailored for consumption[cite: 1].
        """)
    st.markdown("---")

    # ------------------------------------------
    # 5. ORCHESTRATION & AUDIT LOGGING
    # ------------------------------------------
    status_bar.success("🏁 Flight Ingestion Pipeline Completed Successfully!")
    
    st.subheader("📋 Step 14: Central Governance Operational Audit Log")
    log_entry = {
        "pipeline_job_id": "JOB-AIRPORT-DE-88371",
        "execution_timestamp": str(pd.Timestamp.now()),
        "terminal_monitored": terminal_source,
        "input_records_scanned": len(raw_bags),
        "successful_silver_writes": len(silver_clean),
        "quarantined_anomaly_count": len(quarantine_df),
        "pipeline_exit_code": "SUCCESS_WITH_ISOLATIONS"
    }
    st.json(log_entry)

    with st.expander("📘 FRAMEWORK MAPPER: Orchestration & Audit Logs", expanded=True):
        st.markdown("""
        * **WHICH FRAMEWORK:** Databricks Workflow System Tables + Azure Monitor & Log Analytics[cite: 1].
        * **WHERE IT LIVES / RUNS:** Central operational monitoring ledger table[cite: 1].
        * **HOW IT WORKS:** Automatically logs absolute runtime metrics (record volumes, execution status codes, and exceptions) into an immutable ledger table for performance alerting, error-handling recovery, and compliance tracing[cite: 1].
        """)