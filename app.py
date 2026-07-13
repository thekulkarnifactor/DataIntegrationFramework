import streamlit as st
import pandas as pd
import time

# Set page configuration
st.set_page_config(page_title="Metadata-Driven Data Platform Demo", layout="wide")

st.title("⚙️ Single Metadata-Driven Enterprise Data Framework")
st.caption("Interactive Simulation: Watch how data flows based on central metadata configurations.")
st.markdown("---")

# ==========================================
# SIDEBAR: STEP 2 - METADATA REPOSITORY DESIGN
# ==========================================
st.sidebar.header("📁 Step 2: Metadata Repository")
st.sidebar.markdown("Modify the configurations below to see how the engine adapts dynamically.")

# Ingestion Config Simulation
st.sidebar.subheader("Ingestion Config")
load_type = st.sidebar.selectbox("Load Type", ["Incremental (CDC)", "Full Load"])
watermark_col = "updated_at" if load_type == "Incremental (CDC)" else "N/A"

# Data Quality Config Simulation
st.sidebar.subheader("Data Quality Rules")
min_price = st.sidebar.number_input("Min Allowed Price ($)", min_value=0, value=10)
fail_action = st.sidebar.selectbox("On Quality Failure", ["Quarantine Record", "Fail Pipeline"])

# ==========================================
# MAIN PAGE: LAYOUT
# ==========================================

# Step 1: Display the current active metadata driving the run
st.subheader("📋 Active Metadata Configuration (Read at Runtime by Engine)")
metadata_df = pd.DataFrame({
    "Parameter": ["Source System", "Dataset Name", "Load Type", "Watermark Column", "Min Price Rule", "On Failure"],
    "Value": ["ERP_SQL_Server", "orders", load_type, watermark_col, f"${min_price}", fail_action]
})
st.table(metadata_df)

st.markdown("---")

# Step 2: The Flow Controller
st.subheader("🚀 Run Pipeline Simulation")
st.write("Click the button below to trigger the Databricks Workflow Orchestration simulation.")

if st.button("Trigger Ingestion & Processing Pipeline", type="primary"):
    
    # Create placeholders for dynamic visual updates
    status_box = st.info("Step 5: Databricks Workflow initiated...")
    time.sleep(1)
    
    # ------------------------------------------
    # STEP 1: SOURCE DATA
    # ------------------------------------------
    status_box.info("Step 1 & 4: Connecting to ERP Source & extracting data...")
    time.sleep(1)
    
    st.markdown("### 🔌 Source Data Layer (Simulated ERP System)")
    source_data = pd.DataFrame({
        "order_id": [1001, 1002, 1003],
        "customer_id": [55, 62, 77],
        "price": [150, 5, 299],  # Note: Order 1002 breaks the "Min Price" rule if set > 5
        "updated_at": ["2026-07-13 10:00", "2026-07-13 10:05", "2026-07-13 10:10"]
    })
    st.dataframe(source_data)
    
    # ------------------------------------------
    # STEP 8: BRONZE LAYER
    # ------------------------------------------
    status_box.success("Step 8: Ingested into Landing Zone (Bronze Layer) as raw, immutable Delta format.")
    st.markdown("### 🟫 Landing Zone (Bronze Layer)")
    
    # Bronze adds operational metadata columns
    bronze_data = source_data.copy()
    bronze_data["_ingestion_timestamp"] = pd.Timestamp.now()
    bronze_data["_batch_id"] = "BATCH-2026-07-13-001"
    st.dataframe(bronze_data)
    time.sleep(1.5)
    
    # ------------------------------------------
    # STEP 10: DATA QUALITY & SILVER PROCESSING
    # ------------------------------------------
    status_box.info("Step 10: Executing Data Quality Engine checks based on metadata...")
    time.sleep(1.5)
    
    # Filter based on the sidebar's metadata rule
    valid_mask = bronze_data["price"] >= min_price
    silver_clean = bronze_data[valid_mask].copy()
    quarantine_box = bronze_data[~valid_mask].copy()
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### 🥈 Cleaned Layer (Source-Aligned Silver)")
        if not silver_clean.empty:
            # Dropping operational tags for clean presentation
            st.dataframe(silver_clean[["order_id", "customer_id", "price", "updated_at"]])
        else:
            st.warning("No records passed the quality rules!")
            
    with col2:
        st.markdown("### ⚠️ Quarantine Table (Failed Records)")
        if not quarantine_box.empty:
            st.error(f"Isolated {len(quarantine_box)} record(s) breaching Min Price rule (< ${min_price})")
            st.dataframe(quarantine_box)
        else:
            st.success("0 records quarantined. Perfect quality!")
            
    # ------------------------------------------
    # GOLD LAYER PROCESSING
    # ------------------------------------------
    time.sleep(1)
    status_box.info("Aggregating Unified Silver data into Gold Layer Business Data Products...")
    time.sleep(1)
    
    st.markdown("---")
    st.markdown("### 🪙 Business Data Product (Gold Layer)")
    
    if not silver_clean.empty:
        # Simulate business aggregation/KPI modeling typical of Gold layer
        col_kpi1, col_kpi2, col_kpi3 = st.columns(3)
        
        with col_kpi1:
            st.metric("Total Revenue Generated", f"${silver_clean['price'].sum()}")
        with col_kpi2:
            st.metric("Valid Order Count", len(silver_clean))
        with col_kpi3:
            st.metric("Average Order Value", f"${silver_clean['price'].mean():.2f}")
    else:
        st.write("No Gold KPI outputs due to missing clean data points.")
        
    status_box.success("🏁 Workflow Completed Successfully! Operational Logs stored in Audit Table.")
    
    # ------------------------------------------
    # STEP 13 & 14: LOGGING & AUDITING
    # ------------------------------------------
    with st.expander("🔍 View Step 14 Operational Audit Logs"):
        log_entry = {
            "workflow_name": ["Simulated_Ingestion_Engine"],
            "records_read": [len(source_data)],
            "records_written_silver": [len(silver_clean)],
            "records_quarantined": [len(quarantine_box)],
            "status": ["SUCCESS" if fail_action == "Quarantine Record" or len(quarantine_box)==0 else "FAILED"]
        }
        st.json(log_entry)