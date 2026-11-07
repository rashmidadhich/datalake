---
name: cfa-bronze-loader
description: Comprehensive operational runbook and developer guide for the BSNL CFA Data Lake Bronze Loader. Use this skill whenever running, maintaining, troubleshooting, or modifying the bronze ingestion pipeline, adding or updating column mappings across North Zone (SAP CRM) and East Zone (Siebel CRM), managing protected sensitive columns, performing multi-mode ingestions (Parquet, CSV/TSV, JDBC, Oracle DB), or operating the web control plane and REST API.
---

# BSNL CFA Data Lake: Bronze Loader Skill & Runbook

This skill provides an authoritative guide for operating, debugging, modifying, and extending the **CFA Bronze Data Lake Ingestion Pipeline** (`bronze_loader`).

---

## 1. System Architecture & Core Concepts

The Bronze Loader ingests raw transactional and master data from multiple enterprise sources into **PostgreSQL Bronze storage** with zero data loss, guaranteed idempotency, automatic sensitive PII masking, and row-level lineage:

```
  [Upstream Sources]                                 [PostgreSQL Target Data Lake]
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

### Active Production Zones
* **`NZ` (North Zone SAP CRM)**: Replicates 12 physical tables (`BUT000`, `IBIB`, `CRMD_ORDERADM_H`, `CRMD_ORDERADM_I`, `CRMD_ORDER_INDEX`, `CRM_JEST`, `TJ30T`, `ADRC`, `ADR2`, `ADR6`, `BUT021_FS`, `BUT050`) into `bronze_nz`.
* **`EZ` (East Zone Siebel CRM)**: Replicates 13 physical tables (`S_ORDER`, `S_ORDER_X`, `S_ORDER_TYPE`, `S_ORG_EXT`, `S_ORG_EXT_X`, `S_ORG_EXT_XM`, `S_ASSET`, `S_ASSET_X`, `S_ASSET_XA`, `S_ASSET_XM`, `S_ADDR_PER`, `S_CONTACT`, `S_SRV_REQ`) into `bronze_ez`.
* **Note on South Zone (`SZ`)**: South Zone data is not active/available; `bronze_sz` is purged and excluded from active configurations.

### 5 Universal Lineage Columns
Every record committed to PostgreSQL receives:
1. `_load_id` (`bigint`): Unique run ID (`bronze_ctl.load_run.run_id`).
2. `_chunk_id` (`bigint`): Ingestion chunk ID (`bronze_ctl.load_chunk.chunk_id`).
3. `_loaded_at` (`timestamptz`): Exact timestamp when row was written to PostgreSQL.
4. `_source` (`text`): Business source tag (e.g., `NZ_zone`, `EZ_zone`, `Teerva`, `portal`, `FMS`).
5. `_src_table` (`text`): Source physical table or file name.

---

## 2. Key Codebase Files & Sources of Truth

| File | Purpose | Key Responsibilities |
| :--- | :--- | :--- |
| **`bronze_sync.py`** | Core Engine & CLI | Connection pooling, chunked streaming, chunk planning, schema evolution, multi-mode ingestion, retry handling. |
| **`dashboard.py`** | Web UI Control Plane | Embedded zero-dependency HTTP server (`http.server`), REST APIs (`/api/status`, `/api/ingest`, `/api/mapping`, `/api/table/tag`). |
| **`mapping.py`** | Mapping & Security Engine | Database synchronization, PII redaction (`sanitize_raw_payload`), schema validation, dynamic JSON sync. |
| **`config.toml`** | Pipeline Configuration | Active zones (`NZ`, `EZ`), table modes (`merge`, `append`), primary keys, watermarks, SLAs. |
| **`bronze_ctl.sql`** | Control Plane DDL | PostgreSQL DDL for tracking tables (`load_run`, `load_chunk`, `load_quarantine`, `schema_history`, mapping tables). |
| **`zone_mappings_master.csv`** | Tabular Master Mapping | Complete 321-row source-to-CFA360 mapping specification across NZ and EZ. |
| **`zone_mappings_master.json`** | JSON Master Mapping | Unified JSON catalog of all 321 mapping specifications. |
| **`ez_mapping_data.json`** | East Zone Catalog | 182 mappings, 1,866 unmapped classifications, 39 sensitive columns, 97 target fields. |
| **`nz_mapping_data.json`** | North Zone Catalog | 139 mappings, 650 unmapped classifications, 9 sensitive columns, 71 target fields. |
| **`TEAM_GUIDE.md`** | Team Walkthrough | Operational instructions for developers, operators, and analysts. |

---

## 3. Web UI Dashboard Operations

Start the web dashboard on port 8080:
```bash
python dashboard.py --port 8080
# or:
python bronze_sync.py ui --port 8080
```
Open **`http://localhost:8080`** in a browser:
* **Tab 1: Live Ingestion Pipeline**: Real-time throughput (rows/sec), active chunk heartbeats, and one-click file/JDBC ingestion modal.
* **Tab 2: Source Tagging & Catalog**: Tag tables with business source identifiers (`NZ_zone`, `EZ_zone`, `Teerva`, `portal`, `FMS`) and JSON metadata.
* **Tab 3: Real-Time Bronze Data Explorer**: Live record browser across `bronze_nz` and `bronze_ez` with a slide-over **Record Dossier Drawer** showing raw JSON payloads and lineage columns.
* **Tab 4: Column Mappings & Checks Manager**:
  - Filter summary between **All (25)**, **North Zone SAP (12)**, and **East Zone Siebel (13)** tables.
  - Audit **48 Protected Sensitive Columns** (guarded against leaking into quarantine or logs).
  - Search, add, edit, or delete column mappings with instant database and JSON sync.
* **Tab 5: Schema Drift & DQ Audit**: Review schema history (`column_added`, `type_changed`) and inspect quarantined rows.
* **Tab 6: Architecture Blueprint**: Review Oracle parallel extraction queries and PySpark transformation scripts.

---

## 4. Managing Column Mappings & Security Checks

### Where Column Mappings Live
* **Database**: `bronze_ctl.zone_column_mapping` (table and column mappings to `Customer_master_data`, `CRM_activity_table`, `Complaint_details`).
* **Source Specs**: `nz_mapping_data.json`, `ez_mapping_data.json`, `zone_mappings_master.json`, `zone_mappings_master.csv`.

### How to Add or Update a Mapping
1. **Via Web UI**:
   - Tab 4 &rarr; Click **"Add Column Mapping"** (or click **"Edit"** on an existing row).
   - Select Zone (`NZ` or `EZ`), Table, Source Column, CFA Target Entity, Target Field, and Mapping Type.
   - If sensitive, check **"Mark as Protected Sensitive Column"** and enter reason.
   - Click **"Save Column Mapping"**.
2. **Via CLI**:
   ```bash
   python bronze_sync.py mapping --add --zone NZ --table BUT000 --col ZZNEW --entity Customer_master_data --field custom_attr --rule DIRECT
   ```
3. **Via Python / SQL**:
   Insert directly into `bronze_ctl.zone_column_mapping`:
   ```sql
   INSERT INTO bronze_ctl.zone_column_mapping (zone, src_table, src_column, target_entity, target_field, role, mapping_type, transform_rule, confidence)
   VALUES ('EZ', 'S_ORDER', 'X_NEW_FIELD', 'CRM_activity_table', 'new_field', 'Source value', 'DIRECT', 'DIRECT', 'HIGH')
   ON CONFLICT (zone, src_table, src_column, target_entity, target_field, role) DO UPDATE SET transform_rule = EXCLUDED.transform_rule;
   ```

### How to Protect Sensitive PII / Secrets
* Any column flagged `is_sensitive = true` in `bronze_ctl.zone_column_classification` is automatically redacted:
  - Masked as `[REDACTED_SENSITIVE]` in `mapping.py:sanitize_raw_payload()`.
  - Never leaks raw card numbers, CVVs, bank details, IFSC, UMRN, OTPs, or passwords into `bronze_ctl.load_quarantine` or application logs.
* To add a sensitive column flag via CLI:
  ```bash
  python bronze_sync.py mapping --add --zone EZ --table S_ORDER --col X_TOKEN --sensitive --reason "Authentication token"
  ```

---

## 5. Multi-Mode Ingestion Procedures

### Ingesting Parquet Files
```bash
python bronze_sync.py ingest --type parquet --file scratch/assets.parquet --table ASSET_PARQUET --source Teerva [--zone NZ] [--mode merge]
```
* Infers schema and types zero-copy via PyArrow.
* Auto-detects primary key candidates (e.g. `ASSET_ID`).
* Streams batches directly into PostgreSQL.

### Ingesting Delimited CSV/TSV Files
```bash
python bronze_sync.py ingest --type csv --file scratch/subscribers.csv --table SUB_CSV --source portal --delimiter "," [--zone NZ]
```
* Sniffs delimiter (`,`, `;`, `\t`, `|`).
* Dynamically infers BIGINT, INTEGER, DOUBLE, BOOLEAN, TIMESTAMP, and TEXT.
* Empty strings `""` are mapped cleanly to SQL `NULL`.

### Ingesting External JDBC SQL Databases
```bash
python bronze_sync.py ingest --type jdbc --url "jdbc:postgresql://user:pass@host:5432/billing_db" --table INVOICES --source FMS [--zone EZ]
```
* Introspects table columns and primary keys.
* Streams records via server-side cursors directly into Bronze PostgreSQL table.

---

## 6. Oracle Replication Procedures (Historic & Incremental)

### Historic Replication (Initial Backfill)
```bash
# Replicate all North Zone SAP tables:
python bronze_sync.py historic --zone NZ --workers 4

# Replicate specific East Zone table:
python bronze_sync.py historic --zone EZ --table S_ORDER --workers 4

# Force re-execution of already completed chunks:
python bronze_sync.py historic --zone EZ --table S_ORDER --force
```

### Incremental CDC Replication (Scheduled Hourly / Daily)
```bash
python bronze_sync.py incr --zone NZ
python bronze_sync.py incr --zone EZ
```
* Calculates incremental window: `[max_watermark - lookback_hours] .. [now - 5 minutes]`.
* Re-fetches only updated rows based on `watermark_col` (e.g. `LAST_UPD`, `UPTIM`, `CHDAT`).
* Idempotently UPSERTs records into Bronze via PostgreSQL `ON CONFLICT (...) DO UPDATE`.

---

## 7. Verification, Idempotency & Troubleshooting

### Pipeline Verification Commands
```bash
# Check overall status and lag:
python bronze_sync.py status [--watch 10]

# Check active alerts:
python bronze_sync.py check

# Check Data Quality quarantine counts:
python bronze_sync.py dq

# Verify zero duplicate rows across all tables:
python bronze_sync.py verify

# List all tables by business source tag:
python bronze_sync.py sources

# Display full mapping audit summary across all zones:
python bronze_sync.py mapping --summary --zone ALL
```

### Running Test Suites
```bash
# Run unit tests:
python -m unittest test_checks_unit.py test_multi_mode_ingest.py test_dashboard_api.py

# Run full end-to-end integration test:
python test_bronze_sync.py
```

### Important Runtime Guidelines for Agents & Developers
1. **Windows Sandbox Execution**: When running Python scripts or test commands on Windows, always use `BypassSandbox: true` due to filesystem permissions.
2. **Database Connections**: Always use `bronze_sync.connect()` to obtain a properly configured PostgreSQL connection.
3. **Workspace Mirroring**: Whenever modifying `config.toml`, `dashboard.py`, `mapping.py`, or documentation, synchronize changes to the mirror workspace at `f:\Office_related\cfa datalake\ingest_projects\bronze_loader\`.
4. **Preserve Documentation Integrity**: Do not remove existing docstrings or comments when refactoring pipeline code.
