💡 What this Demo Illustrates live:Metadata Dynamism: Adjust the sliders/inputs in the sidebar. If you raise the "Min Allowed Price" above $\$5$, the app instantly routes order 1002 to the Quarantine Box instead of the Silver Layer during processing, showcasing Step 10 (Data Quality).  Traceability: It injects operational metadata fields like _batch_id and _ingestion_timestamp at the Bronze Layer, matching Step 8 exactly.  End-to-End Visibility: The top status bar changes state dynamically, demonstrating how Step 5 (Orchestration) steps through the platform steps sequentially.




This Streamlit application acts as an interactive simulation engine that brings the static 16-step architecture document to life. It shows exactly how a single, reusable framework can dynamically handle data ingestion and processing using only metadata configurations.

Here is exactly what happens under the hood when you interact with the application:

---

## 🧭 Phase 1: Reading the Metadata Control Layer

Before any data moves, the application simulates **Step 2 (Metadata Repository Design)**.

* **Dynamic Control Inputs:** The inputs in the sidebar mimic your metadata configuration tables (like `Ingestion_Config` and `Data_Quality_Config`).


* **Runtime Interception:** The script compiles these UI selections into a temporary DataFrame (`metadata_df`) displayed right at the top. This demonstrates how the core Databricks processing engine doesn't hardcode any logic; it instead reads this configuration matrix at runtime to determine its rules.



---

## ⚡ Phase 2: Orchestration & The Processing Flow

When you click **"Trigger Ingestion & Processing Pipeline"**, it activates an automated simulation loop controlled by **Step 5 (Databricks Workflow Orchestration)**. Using minor time delays (`time.sleep`), it walks you sequentially through the Lakehouse architecture:

### 1. Ingestion & Connectivity (Steps 1 & 4)

The app generates a raw dummy DataFrame representing new, live transactions sitting in an external enterprise ERP database (simulating a JDBC connector fetch). One of these records intentionally breaks common business rules to test the pipeline (e.g., an order with a price of only $\$5$).

### 2. The Landing Zone (Step 8: Bronze Layer)

The raw data is "saved" into the Bronze layer. Crucially, to mirror the document, the app appends **Operational Metadata** fields—a current timestamp (`_ingestion_timestamp`) and a batch tracking ID (`_batch_id`)—leaving the original data structurally pristine and untransformed.

### 3. Data Quality Engine & Quarantine (Step 10)

This is where the metadata-driven framework shines. The application evaluates the data against the "Min Allowed Price" parameter you set in the sidebar.

* Records that pass the rule are routed to the **Source-Aligned Silver Layer**.


* Records that fail are caught by the **Data Quality Engine** and instantly isolated into a **Quarantine Table** so production pipelines don't crash.



### 4. Analytical Aggregation (Gold Layer Processing)

The script takes only the *clean* records that survived the Silver layer validation and performs business calculations on them (aggregating total revenue, count, and averages). This produces a reporting-ready dashboard tier, mirroring the creation of a **Gold Layer Data Product**.

---

## 🔍 Phase 3: Logging & Auditing (Step 14)

At the very bottom of the run, the application expands to show a JSON payload representing **Step 14 (Logging & Audit Framework)**. It automatically calculates operational telemetry—counting exactly how many records were read, successfully processed, or quarantined—providing complete lineage and execution traceability for platform operators.