import streamlit as st
import pandas as pd
import time

# Set page configuration
st.set_config = st.set_page_config(page_title="Metadata-Driven Healthcare Platform", layout="wide")

st.title("🏥 Metadata-Driven Healthcare Data Platform")
st.markdown("""
This simulator demonstrates how a **Single Metadata Framework** securely ingests patient vitals, 
validates them against medical thresholds, isolates anomalies, and delivers clean analytics for doctors.
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
st.markdown("The processing framework reads this table *first* to dynamically set its rules before touch any data.")

metadata_data = {
    "Source System": ["ICU_Bed_Monitors"],
    "Dataset Name": ["patient_vitals"],
    "Ingestion Mode": [stream_frequency],
    "Rule: Min Heart Rate": [f"{min_hr} BPM"],
    "Rule: Max Heart Rate": [f"{max_hr} BPM"],
    "Target Bronze Path": ["dbfs:/mnt/bronze/vitals/"]
}
st.table(pd.DataFrame(metadata_data))

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
    time.sleep(1)
    
    st.markdown("### 📡 1. Source System Output (Raw IoT Stream)")
    st.caption("Simulating raw event feeds from 4 hospital beds. Notice Bed #3 and Bed #4 have anomaly readings.")
    
    raw_source = pd.DataFrame({
        "device_id": ["BED-01", "BED-02", "BED-03", "BED-04"],
        "patient_name": ["John Doe", "Jane Smith", "Robert Lee", "Baby Cyan"],
        "heart_rate_bpm": [72, 85, 240, 12],  # 240 is too high, 12 is too low
        "timestamp": ["2026-07-13 15:30:01", "2026-07-13 15:30:02", "2026-07-13 15:30:03", "2026-07-13 15:30:04"]
    })
    st.dataframe(raw_source)
    
    # ------------------------------------------
    # BRONZE LAYER
    # ------------------------------------------
    status.info("📥 Step 8: Writing raw stream to Immutable Bronze Delta Lake...")
    time.sleep(1.2)
    
    st.markdown("### 🟫 2. Landing Zone (Bronze Layer)")
    st.caption("Data is safely stored exactly as it arrived. System audit columns are appended for compliance tracking.")
    
    bronze_df = raw_source.copy()
    bronze_df["_ingested_at"] = pd.Timestamp.now()
    bronze_df["_datasource_authenticated_by"] = "KeyVault_Secret_EntraID"
    st.dataframe(bronze_df)
    
    # ------------------------------------------
    # SILVER LAYER & DATA QUALITY RULES
    # ------------------------------------------
    status.info("⚙️ Step 10: Processing Silver Layer - Routing records based on Metadata rules...")
    time.sleep(1.5)
    
    st.markdown("---")
    st.markdown("### 🥈 3. The Cleansing & Routing Engine (Silver Layer)")
    
    # Evaluate data against metadata bounds
    is_valid = (bronze_df["heart_rate_bpm"] >= min_hr) & (bronze_df["heart_rate_bpm"] <= max_hr)
    silver_clean = bronze_df[is_valid].copy()
    quarantine_df = bronze_df[~is_valid].copy()
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### ✅ Source-Aligned Silver (Clean Vitals)")
        st.caption("These records passed all thresholds and are safe for hospital reporting.")
        if not silver_clean.empty:
            st.dataframe(silver_clean[["device_id", "patient_name", "heart_rate_bpm", "timestamp"]])
        else:
            st.warning("Zero records passed structural clinical rules!")
            
    with col2:
        st.markdown("#### ⚠️ Quarantine Isolation Table")
        st.caption("Corrupt data or critical anomalies are isolated automatically without breaking the engine pipeline.")
        if not quarantine_df.empty:
            st.error(f"Alert: {len(quarantine_df)} critical alert anomalies detected.")
            st.dataframe(quarantine_df[["device_id", "patient_name", "heart_rate_bpm", "timestamp"]])
        else:
            st.success("No anomalies found. All devices functioning within parameters.")

    # ------------------------------------------
    # GOLD LAYER
    # ------------------------------------------
    status.info("🏆 Step 11 & 12: Updating Unity Catalog and calculating Gold Layer metrics...")
    time.sleep(1.2)
    
    st.markdown("---")
    st.markdown("### 🪙 4. Business Data Product (Gold Layer Dashboard)")
    st.caption("Doctors look at this view. It presents calculated summary metrics based solely on trusted Silver data.")
    
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