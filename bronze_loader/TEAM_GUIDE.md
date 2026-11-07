# CFA Data Lake: Bronze Loader & Control Plane — Team Operating Guide

Welcome to the **CFA Data Lake Bronze Loader**. This guide explains how the pipeline works, how to use the interactive Web UI Dashboard, how to execute CLI commands, and how to manage column mappings and security checks.

---

## 1. System Overview & Architecture

The Bronze Loader copies raw transactional and master data from heterogeneous enterprise sources into **PostgreSQL Bronze storage** (`bronze_nz` for North Zone and `bronze_ez` for East Zone) with zero data loss, guaranteed idempotency, automatic sensitive PII masking, and full row-level lineage:

```
  [Sources]                                          [PostgreSQL Data Lake]
  ┌───────────────────────────────┐                  ┌──────────────────────────────┐
  │ • Oracle Standby ADG:         │                  │ bronze_nz:                   │
  │   - North Zone (12 SAP CRM)   │                  │   12 SAP CRM Tables          │
  │   - East Zone (13 Siebel CRM) │                  │                              │
  │ • Parquet Files (.parquet)    │ ───────────────> │ bronze_ez:                   │
  │ • Delimited Files (.csv/.tsv) │  Bronze Loader   │   13 Siebel CRM Tables       │
  │ • JDBC External DBs           │                  │                              │
  └───────────────────────────────┘                  │ bronze_ctl:                  │
                                                     │   Control Plane & Catalog    │
                                                     └──────────────────────────────┘
```

### Every Ingested Record Carries 5 Universal Lineage Columns
1. `_load_id`: Pipeline run ID (`bronze_ctl.load_run.run_id`).
2. `_chunk_id`: Ingestion chunk ID (`bronze_ctl.load_chunk.chunk_id`).
3. `_loaded_at`: High-precision timestamp when the row was committed into PostgreSQL.
4. `_source`: Business source tag (e.g., `NZ_zone`, `EZ_zone`, `Teerva`, `portal`, `FMS`).
5. `_src_table`: Physical source table name or file name.

---

## 2. Quick Start (Running in 3 Steps)

### Step 1: Install Dependencies & Verify Environment
Ensure Python 3.11+ is installed, then install project dependencies:
```bash
pip install -r requirements.txt
```
Verify your `.env` file contains the target PostgreSQL connection string and Oracle credentials:
```ini
PG_DSN="host=127.0.0.1 port=5432 dbname=lake user=postgres password=..."
ORA_NZ_DSN="10.201.217.18:1521/oltpdr"
ORA_NZ_USER="DATALAKE_LOADING"
ORA_NZ_PASSWORD="..."
ORA_EZ_DSN="10.201.217.18:1521/oltpdr"
ORA_EZ_USER="DATALAKE_LOADING"
ORA_EZ_PASSWORD="..."
```

### Step 2: Start the Web UI Dashboard
Run the dashboard server (uses Python's standard library `http.server`, requiring no external Flask or npm packages):
```bash
python dashboard.py --port 8080
# or:
python bronze_sync.py ui --port 8080
```

### Step 3: Open in Browser
Navigate to:
```
http://localhost:8080
```

---

## 3. How to Use the Web Dashboard (Tab-by-Tab Guide)

### Tab 1: Live Ingestion Pipeline & Execution Monitor
* **KPI Metrics**: View total registered tables (25), active running chunks, clean rows loaded, quarantined records, skipped duplicates, and active alerts.
* **Table Progress Cards**: Track status, replication mode (`merge` or `append`), SLA lag hours, chunks completed, and progress bars.
* **Active Chunks In-Flight**: Real-time throughput (rows/sec), elapsed time, heartbeat monitoring, and stalled chunk detection (>10m).
* **"Ingest Source File / JDBC" Modal (Top Right)**:
  1. Click **"Ingest Source File / JDBC"**.
  2. Select source format: **Parquet**, **CSV/TSV**, or **JDBC/SQL**.
  3. Enter file path (e.g. `scratch/assets.parquet`, `scratch/subscribers.csv`) or JDBC URL (e.g. `jdbc:postgresql://user:pass@host:5432/db`).
  4. Enter target table name and select target zone (**NZ** or **EZ**).
  5. Select or click a quick source tag (**`NZ_zone`**, **`EZ_zone`**, **`Teerva`**, **`portal`**, **`FMS`**).
  6. Click **"Start Ingestion"** to execute. Live progress and row counts will update automatically.

---

### Tab 2: Single Oracle Schema & Source Tagging Catalog
* Used when multiple upstream source systems share a single Oracle database connection or schema.
* **Tagging Tables**:
  1. Find any table in the catalog list.
  2. Click **"Tag Source"**.
  3. Enter or select the source tag (e.g., `NZ_zone`, `EZ_zone`, `Teerva`, `portal`, `FMS`).
  4. Optionally add custom JSON metadata (e.g., `{"owner": "CRM Team", "domain": "Customer Master"}`).
  5. Click **"Save Source Tag"**. All future rows copied from this table will automatically receive this tag in `_source`.
* **Multi-Dimensional Filters**: Filter catalog by Source System and Ingest Mode (`[ORACLE]`, `[PARQUET]`, `[CSV]`, `[JDBC]`).

---

### Tab 3: Real-Time Bronze Data Explorer
* Browse real rows ingested into PostgreSQL Bronze tables (`bronze_nz` and `bronze_ez`).
* **How to Inspect Records**:
  1. Use the **"Select Table"** dropdown to pick any table (e.g., `bronze_nz.but000`, `bronze_ez.s_order`, `bronze_ez.s_srv_req`).
  2. Use the search box to filter rows by name, account number, or ID.
  3. Click **"Inspect Row"** on any row to open the **Record Dossier Drawer**.
  4. The drawer displays every column value, the complete JSON payload, and row-level lineage metadata (`_loaded_at`, `_load_id`, `_chunk_id`, `_source`, `_src_table`).

---

### Tab 4: Column Mappings & Checks Manager
* **25 Physical Tables Summary**:
  - Filter by **All (25)**, **North Zone SAP (12)**, or **East Zone Siebel (13)**.
  - Displays Mapped columns, Target CFA fields, Protected sensitive columns, and Unmapped column counts per table.
* **48 Protected Sensitive Columns Safeguard**:
  - Lists all 48 columns containing PII, passwords, voucher codes, tax IDs, bank accounts, IFSC, UMRN, card numbers, or OTPs.
  - Automatically masked as `[REDACTED_SENSITIVE]` whenever an error row is routed to quarantine.
* **Active Source-to-Target Column Mapping Rules**:
  - Filter rules by Zone (**`NZ`** or **`EZ`**), physical table, or search term.
  - Displays Source Column &rarr; Target Entity (`Customer_master_data`, `CRM_activity_table`, `Complaint_details`) &rarr; Target Field.
* **Adding or Editing a Column Mapping**:
  1. Click **"Add Column Mapping"** (or click **"Edit"** on an existing row).
  2. Select **Zone** (`NZ` or `EZ`).
  3. Select **Source Table**. Live columns from Oracle will populate into the column suggestions dropdown.
  4. Select or type **Source Column Name**.
  5. Select **Target CFA360 Entity** and enter **Target Field Name**.
  6. Choose **Mapping Type** (`DIRECT`, `DERIVED`, `PIPELINE`) and enter any transformation expressions.
  7. If the column contains sensitive PII or credentials, check **"Mark as Protected Sensitive Column"** and enter a reason.
  8. Click **"Save Column Mapping"**. Changes are immediately saved to database table `bronze_ctl.zone_column_mapping` and local JSON catalogs!

---

### Tab 5: Schema Drift & Data Quality Audit
* **Schema Evolution Log**:
  - Displays all detected additive columns from Oracle with source and Postgres types (`VARCHAR2` &rarr; `text`, `NUMBER` &rarr; `numeric`).
  - Shows change types (`column_added`, `type_changed`, `initial`).
* **Data Quality & Quarantine Console**:
  - Shows all records rejected due to NULL primary keys or missing required fields.
  - Click **"Inspect Payload"** to view rejected row data in JSON format with sensitive columns safely redacted.

---

### Tab 6: Architecture Blueprint & Technical Specifications
* Reference queries, PySpark transformation scripts, and data flow specifications for migrating Bronze data into Silver and Gold CFA360 layers.

---

## 4. How to Execute Ingestion via CLI

For automation scripts, cron jobs, or batch operations, use the CLI commands:

### Ingesting Files & External Databases
```bash
# 1. Ingest Parquet file with automatic Primary Key detection and MERGE:
python bronze_sync.py ingest --type parquet --file scratch/assets.parquet --table ASSET_PARQUET --source Teerva

# 2. Ingest Delimited CSV file:
python bronze_sync.py ingest --type csv --file scratch/subscribers.csv --table SUB_CSV --source portal --delimiter ","

# 3. Ingest generic JDBC SQL database table:
python bronze_sync.py ingest --type jdbc --url "jdbc:postgresql://user:pass@host:5432/billing_db" --table INVOICES --source FMS
```

### Replicating Oracle Tables
```bash
# 1. Run historic replication for all North Zone tables:
python bronze_sync.py historic --zone NZ --workers 4

# 2. Run historic replication for a specific East Zone table:
python bronze_sync.py historic --zone EZ --table S_ORDER --workers 4

# 3. Force re-run a historic chunk:
python bronze_sync.py historic --zone EZ --table S_ORDER --force

# 4. Run incremental CDC load (processes changed records since last watermark):
python bronze_sync.py incr --zone NZ
python bronze_sync.py incr --zone EZ
```

### Monitoring & Auditing Commands
```bash
# Check overall status and replication lag:
python bronze_sync.py status [--watch 10]

# Check system alerts:
python bronze_sync.py check

# Audit Data Quality quarantine counts:
python bronze_sync.py dq

# Verify 100% duplicate-free status across all bronze tables:
python bronze_sync.py verify

# View list of registered tables grouped by source system:
python bronze_sync.py sources

# Display full mapping audit summary across all zones:
python bronze_sync.py mapping --summary --zone ALL
```

---

## 5. Master Mapping Files & Reference Documents

| Artifact / File | Description | Location |
| :--- | :--- | :--- |
| **`zone_mappings_master.csv`** | Full tabular master mapping catalog (321 rows across NZ and EZ) | [`zone_mappings_master.csv`](file:///f:/Office_related/cfa%20datalake/bronze_loader/zone_mappings_master.csv) |
| **`zone_mappings_master.json`** | JSON master mapping catalog | [`zone_mappings_master.json`](file:///f:/Office_related/cfa%20datalake/bronze_loader/zone_mappings_master.json) |
| **`ez_mapping_data.json`** | East Zone Siebel CRM mapping specification (182 mappings, 39 sensitive columns) | [`ez_mapping_data.json`](file:///f:/Office_related/cfa%20datalake/bronze_loader/ez_mapping_data.json) |
| **`nz_mapping_data.json`** | North Zone SAP CRM mapping specification (139 mappings, 9 sensitive columns) | [`nz_mapping_data.json`](file:///f:/Office_related/cfa%20datalake/bronze_loader/nz_mapping_data.json) |
| **`config.toml`** | Table tuning, primary keys, watermarks, SLAs, and quarantine rules | [`config.toml`](file:///f:/Office_related/cfa%20datalake/bronze_loader/config.toml) |
| **`dashboard_and_pipeline_controls.md`** | Comprehensive architectural specification and database design | [dashboard_and_pipeline_controls.md](file:///C:/Users/acer/.gemini/antigravity/brain/85991701-937c-4c4d-b447-4f8dc897778e/dashboard_and_pipeline_controls.md) |

---

## 6. Standard Operating Procedures (SOP) & Troubleshooting

1. **How to add a new column mapping without touching code**:
   - Open Web UI &rarr; Tab 4 &rarr; Click **"Add Column Mapping"**.
   - Select Zone, Table, Column, CFA Target Entity, Target Field, and Save.
   - The mapping is immediately active in database table `bronze_ctl.zone_column_mapping`.
2. **How to protect a newly discovered sensitive PII column**:
   - Open Web UI &rarr; Tab 4 &rarr; Click **"Edit"** on the column.
   - Check **"Mark as Protected Sensitive Column"** and enter the reason (e.g., "Aadhaar number" or "API token").
   - Click Save. The column will be masked as `[REDACTED_SENSITIVE]` across all error logs and quarantine dumps.
3. **If a table replication is stalled (>10 minutes)**:
   - Check Tab 1 **Active Chunks Monitor** for stalled heartbeat warning.
   - Verify network connectivity to the Oracle standby ADG.
   - If needed, restart the worker process; idempotent chunk tracking will pick up from the last uncompleted chunk without duplicate rows.
