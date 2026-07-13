import streamlit as st
import pandas as pd
import time

# Set page configuration
st.set_page_config(page_title="Metadata-Driven Framework Mapper", layout="wide")

st.title("🏥 Metadata-Driven Healthcare Data Platform & Framework Mapper")
st.markdown("""
This app simulates an enterprise data pipeline. Click **Simulate Ingestion Cycle** to watch the data move, 
and read the **Blueprint Mappers** under each section to see exactly **which** framework is running, **where** it lives, and **how** it works.
""")
st.markdown("---")

# ==========================================
# SIDEBAR: STEP 2 - METADATA REPOSITORY DESIGN
# ==========================================
st.sidebar.header("📁 Step 2: Metadata Repository")
st.sidebar.markdown("Define the dynamic boundaries for the ingestion engine below.")

# Ingestion Config
st.sidebar.subheader("🔌 Ingestion Settings (Step 4)")
stream_frequency = st.sidebar.selectbox("Ingestion Mode", ["Near Real-Time Streaming", "Hourly Batch"])

# Data Quality Config
st.sidebar.subheader("🩺 Clinical Validation Rules (Step 10)")
min_hr = st.sidebar.slider("Min Valid Heart Rate (BPM)", min_value=30, max_value=60, value=45)
max_hr = st.sidebar.slider("Max Valid Heart Rate (BPM)", min_value=120, max_value=200, value=160)

# ==========================================
# MAIN PAGE LAYOUT
# ==========================================

# Display active metadata repository settings
st.subheader("📋 Active Control Table: `Data_Quality_Config` & `Ingestion_Config`")
st.markdown("The processing framework reads this table *first* to dynamically set its rules before touching any data.")

metadata_data = {
    "Source System": ["ICU_Bed_Monitors"],
    "Dataset Name": ["patient_vitals"],
    "Ingestion Mode": [stream_frequency],
    "Rule: Min Heart Rate": [f"{min_hr} BPM"],
    "Rule: Max Heart Rate": [f"{max_hr} BPM"],
    "Target Bronze Path": ["dbfs:/mnt/bronze/vitals/"]
}
st.table(pd.DataFrame(metadata_data))

with st.expander("📘 ARCHITECTURE BLUEPRINT: Metadata Layer (Step 2)"):
    st.markdown("""
    * **WHICH Framework:** Databricks Delta Tables + Unity Catalog Governance[cite: 1].
    * **WHERE it runs:** Centrally stored in Azure storage, queried at runtime by Databricks processing clusters[cite: 1].
    * **HOW it works:** Pipeline code is completely generic[cite: 1]. It intercepts these parameters dynamically to drive ingestion logic, meaning you don't have to rewrite code when thresholds or new data sources change[cite: 1].
    """)

st.markdown("---")

# Execution trigger
st.subheader("⚡ Databricks Workflow Execution Engine")
if st.button("Simulate Ingestion Cycle", type="primary"):
    
    # Placeholder for status tracking
    status = st.empty()
    
    # ------------------------------------------
    # SOURCE SYSTEM / CONNECTORS
    # ------------------------------------------
    status.info("📡 Steps 3 & 4: Authenticating via Managed Identity & opening Kafka Stream...")
    time.sleep(0.8)
    
    st.markdown("### 📡 1. Source System Output (Raw IoT Stream)")
    raw_source = pd.DataFrame({
        "device_id": ["BED-01", "BED-02", "BED-03", "BED-04"],
        "patient_name": ["John Doe", "Jane Smith", "Robert Lee", "Baby Cyan"],
        "heart_rate_bpm": [72, 85, 240, 12],  # 240 is too high, 12 is too low
        "timestamp": ["2026-07-13 15:30:01", "2026-07-13 15:30:02", "2026-07-13 15:30:03", "2026-07-13 15:30:04"]
    })
    st.dataframe(raw_source)
    
    with st.expander("📘 ARCHITECTURE BLUEPRINT: Source & Connector Layer (Steps 3 & 4)"):
        st.markdown("""
        * **WHICH Framework:** Spark Structured Streaming + Azure Key Vault + Microsoft Entra ID[cite: 1].
        * **WHERE it runs:** Azure Databricks clusters securely connected to internal hospital event hubs[cite: 1].
        * **HOW it works:** Securely fetches credentials from Key Vault, establishes a handshake, and streams raw JSON/Avro telemetry directly into the Lakehouse environment without impacting operational hospital infrastructure[cite: 1].
        """)
    
    # ------------------------------------------
    # BRONZE LAYER
    # ------------------------------------------
    status.info("📥 Step 8: Writing raw stream to Immutable Bronze Delta Lake...")
    time.sleep(0.8)
    
    st.markdown("### 🟫 2. Landing Zone (Bronze Layer)")
    bronze_df = raw_source.copy()
    bronze_df["_ingested_at"] = pd.Timestamp.now()
    bronze_df["_datasource_authenticated_by"] = "KeyVault_Secret_EntraID"
    st.dataframe(bronze_df)
    
    with st.expander("📘 ARCHITECTURE BLUEPRINT: Landing Zone Layer (Step 8)"):
        st.markdown("""
        * **WHICH Framework:** Azure Data Lake Storage Gen2 (ADLS Gen2) + Delta Lake Storage Format[cite: 1].
        * **WHERE it runs:** Distributed cloud object storage system[cite: 1].
        * **HOW it works:** Acts as an **immutable, raw time capsule**[cite: 1]. It stores data exactly as it arrived alongside system audit markers (`_ingested_at`), ensuring data engineers can replay historical data or audit system issues at any time[cite: 1].
        """)
    
    # ------------------------------------------
    # SILVER LAYER & DATA QUALITY RULES
    # ------------------------------------------
    status.info("⚙️ Step 10: Processing Silver Layer - Routing records based on Metadata rules...")
    time.sleep(1)
    
    st.markdown("---")
    st.markdown("### 🥈 3. The Cleansing & Routing Engine (Silver Layer)")
    
    is_valid = (bronze_df["heart_rate_bpm"] >= min_hr) & (bronze_df["heart_rate_bpm"] <= max_hr)
    silver_clean = bronze_df[is_valid].copy()
    quarantine_df = bronze_df[~is_valid].copy()
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### ✅ Source-Aligned Silver (Clean Vitals)")
        if not silver_clean.empty:
            st.dataframe(silver_clean[["device_id", "patient_name", "heart_rate_bpm", "timestamp"]])
        else:
            st.warning("Zero records passed structural clinical rules!")
            
    with col2:
        st.markdown("#### ⚠️ Quarantine Isolation Table")
        if not quarantine_df.empty:
            st.error(f"Alert: {len(quarantine_df)} critical alert anomalies detected.")
            st.dataframe(quarantine_df[["device_id", "patient_name", "heart_rate_bpm", "timestamp"]])
        else:
            st.success("No anomalies found. All devices functioning within parameters.")

    with st.expander("📘 ARCHITECTURE BLUEPRINT: Data Quality & Silver Processing Layer (Step 10)"):
        st.markdown("""
        * **WHICH Framework:** Databricks Ingestion Engine / Delta Live Tables (DLT) Expectations[cite: 1].
        * **WHERE it runs:** In-memory Apache Spark execution engines inside Databricks[cite: 1].
        * **HOW it works:** Evaluates incoming streams against the active metadata guidelines[cite: 1]. Healthy records proceed to the **Silver Layer**, while corrupted/out-of-bounds metrics are systematically siphoned into a **Quarantine Table** to protect downstream systems without crashing the live ingestion pipeline[cite: 1].
        """)

    # ------------------------------------------
    # GOLD LAYER
    # ------------------------------------------
    status.info("🏆 Step 11 & 12: Updating Unity Catalog and calculating Gold Layer metrics...")
    time.sleep(0.8)
    
    st.markdown("---")
    st.markdown("### 🪙 4. Business Data Product (Gold Layer Dashboard)")
    
    if not silver_clean.empty:
        metric1, metric2, metric3 = st.columns(3)
        with metric1:
            st.metric(label="Total Active Monitored Patients", value=int(len(silver_clean)))
        with metric2:
            st.metric(label="Average Safe Heart Rate", value=f"{silver_clean['heart_rate_bpm'].mean():.1f} BPM")
        with metric3:
            st.metric(label="Peak Safe Heart Rate Detected", value=f"{silver_clean['heart_rate_bpm'].max()} BPM")
    else:
        st.error("Cannot display hospital dashboard: No valid clean data points survived validation.")

    with st.expander("📘 ARCHITECTURE BLUEPRINT: Gold Business Layer (Step 11 & 12)"):
        st.markdown("""
        * **WHICH Framework:** Databricks SQL Tables + Unity Catalog Metadata Engine[cite: 1].
        * **WHERE it runs:** High-performance Databricks SQL Warehouses integrated directly with BI presentation layers (like Power BI or Tableau)[cite: 1].
        * **HOW it works:** Flattens, contextualizes, and aggregates trusted Silver entries into structural metrics[cite: 1]. This ensures analytical dashboards view curated, clinically clean representations of operational facts[cite: 1].
        """)

    # ------------------------------------------
    # AUDIT LOGGING
    # ------------------------------------------
    status.success("🏁 Workflow Ingestion Run Complete!")
    
    st.markdown("---")
    st.subheader("📋 Step 14: Central Operational Audit Logs")
    log_entry = {
        "pipeline_run_id": "RUN-ID-ICU-99218",
        "timestamp": str(pd.Timestamp.now()),
        "ingestion_mode_read": stream_frequency,
        "total_records_processed": len(raw_source),
        "successful_clean_writes": len(silver_clean),
        "quarantined_anomaly_count": len(quarantine_df),
        "pipeline_exit_status": "SUCCESS"
    }
    st.json(log_entry)

    with st.expander("📘 ARCHITECTURE BLUEPRINT: Operational Log & Audit Layer (Step 13, 14 & 15)"):
        st.markdown("""
        * **WHICH Framework:** Databricks System Tables + Azure Log Analytics / Monitor[cite: 1].
        * **WHERE it runs:** Centralized governance tracking datastore[cite: 1].
        * **HOW it works:** Compiles structural job metrics (read records, write successes, quarantined anomalies, runtime durations, error codes) into an immutable ledger[cite: 1]. This satisfies rigorous regulatory compliance requirements and handles system health alerts[cite: 1].
        """)