import streamlit as st
import pandas as pd
import time

# Page configuration
st.set_page_config(page_title="Enterprise Framework Simulator", layout="wide")

st.title("✈️ Metadata-Driven Airport Logistics & Data Framework")
st.markdown("""
This application visualizes a **Single Metadata-Driven Architecture** using an airport baggage routing system. 
Adjust the metadata configuration tables in the sidebar to see how the ingestion engine dynamically adapts its rules without rewriting any code.
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
st.markdown("Before touching a single piece of data, the processing engine reads this central repository to build its logic dynamically.")

metadata_matrix = {
    "Ingestion Source": [terminal_source],
    "Target Lake Location": ["dbfs:/mnt/bronze/baggage_stream/"],
    "Max Weight Limit Check": [f"{max_weight} kg"],
    "Anomaly Policy": [route_mode],
    "Framework Status": ["ONLINE (Metadata-Driven)"]
}
st.table(pd.DataFrame(metadata_matrix))

# Explaining Step 2 on the UI
st.info("""
💡 **HOW IT WORKS AT THE BLUEPRINT LAYER (Step 2):** 
The entire software script is completely generic. It doesn't know what a 'weight limit' is until it reads this metadata table at runtime[cite: 1]. If an airport policy changes tomorrow, you only update this configuration table—**zero code changes or pipeline redeployments required**[cite: 1].
""")

st.markdown("---")

# Execution trigger
st.subheader("⚡ Databricks Workflow Orchestration Engine (Step 5)")
if st.button("Trigger Live Baggage Ingestion Run", type="primary"):
    
    # Status tracking placeholder
    status_bar = st.empty()
    
    # ------------------------------------------
    # SOURCE SYSTEM / CONNECTORS
    # ------------------------------------------
    status_bar.info("📡 Steps 1, 3 & 4: Authenticating API protocols and reading scanner conveyor stream...")
    time.sleep(1.0)
    
    st.markdown("### 📡 1. Source System Output (Raw Scanner Events)")
    st.caption("Four bags have just crossed the intake laser scanner. Notice Bag #3 and Bag #4 have structural differences.")
    
    raw_bags = pd.DataFrame({
        "baggage_id": ["BAG-9921", "BAG-9922", "BAG-9923", "BAG-9924"],
        "passenger_name": ["Alice Green", "Bob Miller", "Charlie Smith", "Diana Prince"],
        "weight_kg": [18, 25, 65, -5],  # 65kg is dangerously overweight, -5kg is a sensor corruption glitch
        "destination_flight": ["AI-101", "EK-203", "LH-430", "SQ-012"]
    })
    st.dataframe(raw_bags)
    
    # Framework Box 1
    st.markdown("""
    > 🛠️ **WHICH FRAMEWORK:** Azure Key Vault + Microsoft Entra ID + Spark Structured Streaming Connectors[cite: 1].  
    > 📍 **WHERE IT RUNS:** Azure Databricks clusters securely processing live scanner inputs[cite: 1].  
    > ⚙️ **HOW IT WORKS:** Automatically queries Key Vault for secure credentials, initiates a encrypted TLS handshake with the terminal hardware, and pulls the raw baggage data stream into the system without impacting local terminal machinery[cite: 1].
    """)
    st.markdown("---")
    
    # ------------------------------------------
    # BRONZE LAYER
    # ------------------------------------------
    status_bar.info("📥 Step 8: Writing raw stream to Immutable Bronze Delta Lake...")
    time.sleep(1.0)
    
    st.markdown("### 🟫 2. Landing Zone (Bronze Layer)")
    st.caption("Data is safely written as a raw 'time-capsule'. System audit metadata is appended to protect historical records.")
    
    bronze_df = raw_bags.copy()
    bronze_df["_ingested_at_time"] = pd.Timestamp.now()
    bronze_df["_batch_ingest_id"] = "BATCH-RUN-2026-07-13-A"
    bronze_df["_source_terminal"] = terminal_source
    st.dataframe(bronze_df)
    
    # Framework Box 2
    st.markdown("""
    > 🛠️ **WHICH FRAMEWORK:** Azure Data Lake Storage Gen2 (ADLS Gen2) + Delta Lake File Format[cite: 1].  
    > 📍 **WHERE IT RUNS:** Enterprise Cloud Object Storage[cite: 1].  
    > ⚙️ **HOW IT WORKS:** Saves a **raw, completely untouched, immutable copy** of the original scanner signals[cite: 1]. By appending system audit tracking columns (`_batch_ingest_id`), data engineers can replay this exact moment years later to debug physical sensor glitches or fulfill strict compliance audits[cite: 1].
    """)
    st.markdown("---")

    # ------------------------------------------
    # SILVER LAYER & DATA QUALITY RULES
    # ------------------------------------------
    status_bar.info("⚙️ Step 10: Executing Data Quality Routing Engine...")
    time.sleep(1.2)
    
    st.markdown("### 🥈 3. The Smart Cleansing & Routing Engine (Silver Layer)")
    st.caption("The engine cross-references the Bronze data against the rules retrieved from Step 2's Metadata Repository[cite: 1].")
    
    # Evaluate rules: Weight must be positive and below the user-defined max_weight
    is_valid = (bronze_df["weight_kg"] > 0) & (bronze_df["weight_kg"] <= max_weight)
    silver_clean = bronze_df[is_valid].copy()
    quarantine_df = bronze_df[~is_valid].copy()
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### ✅ Clean & Verified Silver Table")
        st.caption("These bags successfully passed all safety rules and are ready for flight assignment.")
        if not silver_clean.empty:
            st.dataframe(silver_clean[["baggage_id", "passenger_name", "weight_kg", "destination_flight"]])
        else:
            st.warning("No baggage met the current security rules configuration.")
            
    with col2:
        st.markdown("#### ⚠️ Automated Quarantine Table")
        st.caption("Isolated bags that breached rules (e.g., overweight or corrupted negative weight sensor errors).")
        if not quarantine_df.empty:
            st.error(f"Security Alert: Isolated {len(quarantine_df)} bag(s) breaching active airport rules.")
            st.dataframe(quarantine_df[["baggage_id", "passenger_name", "weight_kg", "destination_flight"]])
        else:
            st.success("Perfect Run! 0 anomalies or rule breaches detected.")

    # Framework Box 3
    st.markdown("""
    > 🛠️ **WHICH FRAMEWORK:** Databricks Ingestion Engine powered by Delta Live Tables (DLT) Expectations[cite: 1].  
    > 📍 **WHERE IT RUNS:** In-memory distributed Spark computation layer inside Databricks[cite: 1].  
    > ⚙️ **HOW IT WORKS:** Checks the data line-by-line using validation masks[cite: 1]. Safe assets continue cleanly to the **Silver Layer**, while corrupted items or dangerous rule-breakers are instantly split into an isolated **Quarantine Table**[cite: 1]. This keeps the business running cleanly without crashing the core real-time application pipelines[cite: 1].
    """)
    st.markdown("---")

    # ------------------------------------------
    # GOLD LAYER
    # ------------------------------------------
    status_bar.info("🏆 Step 11 & 12: Generating final operational business data product...")
    time.sleep(1.0)
    
    st.markdown("### 🪙 4. Flight Logistics Analytics (Gold Layer)")
    st.caption("Flight dispatchers and airport managers use this aggregated view to finalize plane load balances.")
    
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

    # Framework Box 4
    st.markdown("""
    > 🛠️ **WHICH FRAMEWORK:** Databricks SQL Warehouses + Unity Catalog Permissions[cite: 1].  
    > 📍 **WHERE IT RUNS:** High-performance analytical compute tier connected to management dashboards[cite: 1].  
    > ⚙️ **HOW IT WORKS:** Flattens out complex schemas and aggregates the verified Silver records into clear, production-ready facts and KPIs (Key Performance Indicators)[cite: 1]. It serves as a secure, pre-calculated **Data Product** ready for end-user business consumption[cite: 1].
    """)
    st.markdown("---")

    # ------------------------------------------
    # AUDIT LOGGING
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

    # Framework Box 5
    st.markdown("""
    > 🛠️ **WHICH FRAMEWORK:** Databricks Workflow System Tables + Azure Log Analytics[cite: 1].  
    > 📍 **WHERE IT RUNS:** Central operational monitoring ledger[cite: 1].  
    > ⚙️ **HOW IT WORKS:** Automatically records absolute execution telemetry—exactly how many items were read, written, or failed[cite: 1]. This centralized logging ensures full operational oversight, alerts the operations team if errors spike, and provides an air-tight history for regulatory compliance[cite: 1].
    """)