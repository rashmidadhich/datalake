# Oracle -> Postgres bronze loader

1. `pip install -r requirements.txt` (Python 3.11+)
2. Configure credentials in `.env` (or copy `.env.example` to `.env`):
   - `PG_DSN` (PostgreSQL target connection string)
   - Per zone `ORA_SZ_DSN/USER/PASSWORD` (also `WZ`, `NZ`, `EZ` for other Oracle zones).
   - Use a read-only Oracle user, ideally on an Active Data Guard standby.
   - Credentials can also be supplied via shell environment or custom path via `--env-file <path>`.
3. Copy `config.example.toml` to `config.toml`; list zones/schemas and tune the big tables.
4. `python bronze_sync.py init` then `discover` (read the "NEEDS watermark" lines it prints).
5. Benchmark ONE big table first: `historic --zone SZ --table DATA_USAGE --workers 4`, watch `status --watch 10`.
6. Then `historic` for everything; when complete, schedule `incr` (hourly) and `check` (every 5-10 min).

## Data Integrity, Quality & Safety Controls

The system includes built-in enterprise pipeline checks:

- **Data Quality Check**:
  - Automatically validates primary key columns and configured required columns for NULL or empty strings.
  - Invalid records are routed to `bronze_ctl.load_quarantine` with row data (as JSONB) and clear `rejection_reason`.
  - Quarantined counts and metrics are tracked per chunk and exposed via `bronze_ctl.v_dq_summary`.
  - View with CLI: `python bronze_sync.py dq`.

- **Duplicate Checks**:
  - Detects intra-chunk and stream duplicate keys on tables with primary keys.
  - In `append` mode, prevents duplicate PK rows from being inserted into the bronze dataset.
  - In `merge` mode, counts stream duplicates and merges them cleanly via `ON CONFLICT DO UPDATE`.
  - Verify uniqueness across all bronze tables with: `python bronze_sync.py verify`.

- **Idempotency Checks**:
  - Prevents redundant chunk re-execution: if an identical chunk has already succeeded with identical boundaries, it is skipped idempotently.
  - Append-mode chunks execute transactional delete-before-copy (`DELETE ... WHERE range`), guaranteeing re-run safety.
  - Re-running the pipeline produces identical state without orphan or duplicate records.

- **Schema Checks & Database-Driven Mapping Management**:
  - Automatically identifies additive column additions from Oracle and applies `ALTER TABLE ... ADD COLUMN ...`.
  - Tracks all schema evolutions in `bronze_ctl.schema_history` (`initial`, `column_added`, `column_missing`, `type_changed`, `mapping_column_missing`).
  - **Dynamic Database Source of Truth**: All column mappings, unmapped column classifications, and target CFA schemas are stored and pulled directly from database tables:
    - `bronze_ctl.zone_column_mapping` (table & column mappings to CFA360 entities)
    - `bronze_ctl.zone_column_classification` (unmapped columns, classification, and sensitive PII flags)
    - `bronze_ctl.cfa_target_schema` (CFA360 canonical target entities and field types)
  - **Runtime SQL Control**: DBAs or engineers can add, modify, or delete column mappings and toggle sensitive flags directly via SQL `INSERT`, `UPDATE`, or `DELETE`. Changes take effect immediately without redeployment.
  - **Automatic Sensitive Column Redaction**: Safeguards sensitive columns (PAN, TAN, GSTIN, passwords, credentials, names, phones in `BUT000` and `CRMD_ORDERADM_H`). Any column flagged with `is_sensitive = true` in `bronze_ctl.zone_column_classification` is automatically redacted from raw payloads and quarantine records.
  - Flags unmapped custom Z fields (`UNKNOWN - ask NZ`) and brand new drift columns.

## Web UI Dashboard & Real-Time Monitoring Control Plane

Start the built-in interactive web control plane (standard library `http.server`, zero extra npm/flask dependencies):
```bash
python bronze_sync.py ui [--port 8080] [--host 0.0.0.0]
# or directly:
python dashboard.py [--port 8080]
```
Then open `http://localhost:8080` in your browser. The dashboard provides 100% real-time data monitoring:
- **KPI Summary**: Total replicated tables, active running chunks, clean rows loaded, quarantined rows, duplicates skipped, and active alerts.
- **Tables & Historic Progress**: Table-level replication status, mode, lag vs freshness SLA, chunk progress bars, and completion percentages.
- **Active Chunks Monitor**: Real-time in-flight chunk throughput (rows/sec), elapsed time, heartbeat monitoring, and stalled chunk detection (>10m).
- **Data Quality & Quarantine Console**: Live quarantined records from `bronze_ctl.load_quarantine`, with rejection reasons, timestamps, and row payload inspector with automatic sensitive column masking.
- **Real-Time Bronze Data Explorer**: Live dropdown selector to query real ingested rows from any Bronze table (`bronze_nz.but000`, `bronze_nz.ibib`, `bronze_nz.crmd_orderadm_h`, `bronze_sz.mst`, `bronze_sz.evt`, etc.) with search and slide-over **Record Dossier Drawer**.
- **Schema Mapping & Configuration Manager**:
  - Live table listing mappings from `bronze_ctl.zone_column_mapping`.
  - **"Configure Mapping" Modal**: Interactive UI to add new mappings or update transformation rules, target entities, confidence levels, and toggle sensitive column PII protection.
  - Instant REST API backend: `POST /api/mapping` and `POST /api/mapping/delete`.
- **Schema Evolution Log**: Additive column drift tracking, source/target data type mapping, and detection timestamps.
- **Live Event Stream & Runs**: Historical pipeline execution runs and filterable system event logs.

## Monitoring & CLI Commands

- `python bronze_sync.py tables [--zone NZ] [--schema SCHEMA] [--json]` (pulls and inspects live tables directly from Oracle DB connection)
- `python bronze_sync.py sources [--json]` (lists all tables categorized by marked source system: Teerva, portal, FMS, SZ_zone, NZ_zone)
- `python bronze_sync.py tag --table <name> --source <system> [--meta '{"owner":"..."}']` (tags table with source system and business metadata)
- `python bronze_sync.py ingest --type <parquet|csv|jdbc> --table <name> --source <system> [--file <path>] [--url <jdbc_url>]` (ingests Parquet, CSV, or JDBC source directly into Bronze table)
- `python bronze_sync.py discover [--live-oracle]` (auto-discovers schemas and tables directly from Oracle DB)
- `python bronze_sync.py ui [--port 8080]` (starts real-time web dashboard)
- `python bronze_sync.py mapping` (audits database-driven column mappings, sensitive columns, and target CFA fields)
- `python bronze_sync.py mapping --sync` (initializes database mapping tables from bundled catalog)
- `python bronze_sync.py mapping --add --table BUT000 --col ZZNEW --entity Customer_master_data --field custom_attr --rule DIRECT` (adds mapping)
- `python bronze_sync.py mapping --delete --table BUT000 --col ZZNEW` (removes mapping)
- `python bronze_sync.py status [--watch N]` (overall status and lag metrics)
- `python bronze_sync.py check` (exits code 2 when alerts exist in `v_alerts`)
- `python bronze_sync.py dq` (summary of rows fetched, loaded, quarantined, and rejection reasons)
- `python bronze_sync.py schema` (history of column additions, drift, and schema modifications)
- `python bronze_sync.py verify` (audits bronze tables for duplicates and verifies row counts)

## Multi-Mode Ingestion (Parquet Files, CSV/TSV, JDBC Connections, Oracle)

Bronze Loader supports ingesting data from multiple file formats and database connections into PostgreSQL Bronze tables with 100% data fidelity, row-level lineage (`_source`, `_src_table`, `_load_id`, `_chunk_id`, `_loaded_at`), schema evolution, data quality quarantine validation, duplicate checks, and Web UI controls:

- **Supported Ingest Modes**:
  - **Parquet (`parquet`)**: Direct chunked streaming via PyArrow. Supports single `.parquet` files, directory trees, or glob patterns (e.g. `/data/assets/*.parquet`). Auto-detects PyArrow data types, extracts bounds and min/max row counts, and detects primary key candidates.
  - **Delimited CSV / TSV (`csv`)**: Supports uncompressed and compressed delimited files (`.csv`, `.tsv`, `.csv.gz`). Configurable / auto-detected delimiters (`,`, `;`, `\t`, `|`), automatic data type inference (BIGINT, INTEGER, DOUBLE PRECISION, BOOLEAN, TIMESTAMP, TEXT), and automatic conversion of empty strings (`""`) to SQL `NULL` for PostgreSQL `COPY`.
  - **Generic JDBC / SQL (`jdbc`)**: Ingests directly from external SQL databases via standard JDBC URLs (`jdbc:postgresql://...`, `jdbc:sqlite:...`, `jdbc:oracle:...`, `jdbc:mysql:...`). Introspects table schemas, primary keys, and column types via DB-API drivers, streaming rows with server-side cursors.
  - **Oracle (`oracle`)**: High-throughput Oracle chunked extraction with ROWID/partition bounds, OCI arraysize tuning, and LOB streaming.

- **Universal Row-Level Lineage & Integrity**:
  Every record ingested—regardless of whether it originates from Parquet, CSV, JDBC, or Oracle—receives complete Bronze metadata columns:
  - `_source`: Business source system name (`Teerva`, `portal`, `FMS`, `SZ_zone`, `NZ_zone`, etc.)
  - `_src_table`: Physical source table or file name
  - `_load_id`: Unique ingestion pipeline run ID
  - `_chunk_id`: Ingestion chunk ID
  - `_loaded_at`: High-precision timestamp when the row landed in PostgreSQL

- **CLI Multi-Mode Ingestion**:
  ```bash
  # Ingest Parquet file with MERGE mode:
  python bronze_sync.py ingest --type parquet --file scratch/assets.parquet --table ASSET_PARQUET --source Teerva

  # Ingest Delimited CSV file:
  python bronze_sync.py ingest --type csv --file scratch/subscribers.csv --table SUB_CSV --source portal --delimiter ","

  # Ingest JDBC table from external database:
  python bronze_sync.py ingest --type jdbc --url "jdbc:sqlite:scratch/test_billing.sqlite" --table BILLING_INVOICES --source FMS

  # Ingest with explicit Primary Key and Full Refresh:
  python bronze_sync.py ingest --type parquet --file /data/users.parquet --table USERS --source portal --mode full_refresh --pk user_id
  ```

- **Web Dashboard Multi-Mode Ingestion UI & REST API**:
  - Click **"Ingest Source File / JDBC"** in the Web Dashboard header to open the interactive ingestion modal.
  - Switch seamlessly between **Parquet**, **CSV/TSV**, and **JDBC/SQL** with dynamic field toggles.
  - Real-time Ingestion Mode Badges (`[ORACLE]`, `[PARQUET]`, `[CSV]`, `[JDBC]`) across Table Status and Source Catalog views.
  - Programmatic REST API endpoint: `POST /api/ingest` accepting JSON payload with full feedback metrics (`rows_fetched`, `rows_loaded`, `rows_quarantined`, `duplicates_skipped`, `elapsed_s`).

## Single Oracle Schema Architecture & Dynamic Source Tagging

When all tables arrive from a single consolidated Oracle schema or connection, tables can be categorized and marked by business source system:
- **Supported Source Systems**: `SZ_zone`, `EZ_zone`, `NZ_zone`, `Teerva`, `portal`, `FMS`, or any user-defined source string.
- **Source Metadata**: Custom JSON metadata (e.g. `owner`, `domain`, `description`, `contact`) attached to each table configuration in `bronze_ctl.table_config.source_metadata`.
- **Source Lineage on Bronze Tables**: Every row copied into PostgreSQL Bronze includes system lineage columns:
  - `_source`: The tagged source system (`Teerva`, `portal`, `FMS`, `SZ_zone`, `NZ_zone`, etc.)
  - `_src_table`: Physical source table name in Oracle
  - `_load_id`: Ingestion pipeline run ID
  - `_chunk_id`: Ingestion chunk ID
  - `_loaded_at`: High-precision timestamp when record landed in PostgreSQL
- **Source Catalog View**: Query `bronze_ctl.v_source_catalog` to inspect all tables organized by source system with row counts and configurations.
- **Interactive Tagging in UI & REST API**: Click the "Tag" button on any table in the Web Dashboard or invoke `POST /api/table/tag` to update source systems and metadata dynamically without downtime.

## SQL Management Tables & Views

- Mapping & Security Tables:
  - `bronze_ctl.zone_column_mapping`
  - `bronze_ctl.zone_column_classification`
  - `bronze_ctl.cfa_target_schema`
- Pipeline State & Audit Tables:
  - `bronze_ctl.load_chunk`
  - `bronze_ctl.load_quarantine`
  - `bronze_ctl.schema_history`
  - `bronze_ctl.load_event`
  - `bronze_ctl.table_config` (includes `source_system` and `source_metadata`)
  - `bronze_ctl.table_state`
- Monitoring Views:
  - `bronze_ctl.v_source_catalog` (tables organized by source system with metadata)
  - `bronze_ctl.v_alerts`
  - `bronze_ctl.v_dq_summary`
  - `bronze_ctl.v_active_chunks`
  - `bronze_ctl.v_historic_progress`
  - `bronze_ctl.v_table_status` (includes `source_system` and `source_metadata`)
  - `bronze_ctl.v_sensitive_columns`
  - `bronze_ctl.v_nz_mapping_summary`
