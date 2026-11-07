-- Bronze control plane: config, state, runs, chunks, events, DQ quarantine, schema history + monitoring views. Safe to re-run.
CREATE SCHEMA IF NOT EXISTS bronze_ctl;

-- one row per Oracle table to replicate (filled by `bronze_sync.py discover`, tuned by config.toml)
CREATE TABLE IF NOT EXISTS bronze_ctl.table_config (
  zone text NOT NULL, src_schema text NOT NULL, src_table text NOT NULL,
  tgt_schema text NOT NULL, tgt_table text NOT NULL,
  mode text NOT NULL DEFAULT 'append' CHECK (mode IN ('append','merge','full_refresh')),
  pk_cols text[],                       -- needed for merge and deduplication
  watermark_col text,                   -- "changed since" column for incremental windows
  chunk_col text,                       -- date/timestamp column used to split the historic load
  chunk_days int NOT NULL DEFAULT 1,
  lookback_hours int NOT NULL DEFAULT 48,
  partitioned boolean NOT NULL DEFAULT false,   -- monthly range partitions on chunk_col (append mode)
  oracle_hint text,                     -- e.g. /*+ PARALLEL(t 4) */
  verify_counts boolean NOT NULL DEFAULT false, -- compare COUNT(*) in Oracle with rows loaded, per chunk
  freshness_sla_hours int NOT NULL DEFAULT 26,
  est_rows bigint, enabled boolean NOT NULL DEFAULT true,
  check_duplicates boolean NOT NULL DEFAULT true,
  check_idempotency boolean NOT NULL DEFAULT true,
  quarantine_enabled boolean NOT NULL DEFAULT true,
  dq_rules jsonb,                       -- custom rules: required_cols, range checks, regex
  PRIMARY KEY (zone, src_schema, src_table));

-- ensure newer columns exist on existing databases
ALTER TABLE bronze_ctl.table_config ADD COLUMN IF NOT EXISTS check_duplicates boolean NOT NULL DEFAULT true;
ALTER TABLE bronze_ctl.table_config ADD COLUMN IF NOT EXISTS check_idempotency boolean NOT NULL DEFAULT true;
ALTER TABLE bronze_ctl.table_config ADD COLUMN IF NOT EXISTS quarantine_enabled boolean NOT NULL DEFAULT true;
ALTER TABLE bronze_ctl.table_config ADD COLUMN IF NOT EXISTS dq_rules jsonb;
ALTER TABLE bronze_ctl.table_config ADD COLUMN IF NOT EXISTS source_system text;
ALTER TABLE bronze_ctl.table_config ADD COLUMN IF NOT EXISTS source_metadata jsonb;
ALTER TABLE bronze_ctl.table_config ADD COLUMN IF NOT EXISTS ingest_type text NOT NULL DEFAULT 'oracle';
ALTER TABLE bronze_ctl.table_config ADD COLUMN IF NOT EXISTS ingest_config jsonb;

-- source catalog view: organizes all tables by their marked business source (SZ_zone, EZ_zone, NZ_zone, Teerva, portal, FMS)
DROP VIEW IF EXISTS bronze_ctl.v_source_catalog CASCADE;
CREATE OR REPLACE VIEW bronze_ctl.v_source_catalog AS
SELECT COALESCE(source_system, zone) AS source_name,
       src_schema AS oracle_schema,
       src_table AS oracle_table,
       tgt_schema || '.' || tgt_table AS bronze_target_table,
       mode,
       pk_cols,
       watermark_col,
       est_rows,
       enabled,
       COALESCE(ingest_type, 'oracle') AS ingest_type,
       ingest_config,
       source_metadata
  FROM bronze_ctl.table_config
 ORDER BY source_name, src_table;

CREATE TABLE IF NOT EXISTS bronze_ctl.table_state (
  zone text NOT NULL, src_schema text NOT NULL, src_table text NOT NULL,
  historic_started_at timestamp, historic_done_at timestamptz,
  last_window_to timestamp,             -- Oracle-side time up to which incremental data is loaded
  last_success_at timestamptz,
  PRIMARY KEY (zone, src_schema, src_table));

CREATE TABLE IF NOT EXISTS bronze_ctl.load_run (
  run_id bigserial PRIMARY KEY, mode text, started_at timestamptz NOT NULL DEFAULT now(), ended_at timestamptz,
  status text NOT NULL DEFAULT 'running', host text, pid int, params jsonb);

CREATE TABLE IF NOT EXISTS bronze_ctl.load_chunk (
  chunk_id bigserial PRIMARY KEY, run_id bigint REFERENCES bronze_ctl.load_run,
  zone text NOT NULL, src_schema text NOT NULL, src_table text NOT NULL, tgt_schema text, tgt_table text,
  window_kind text NOT NULL CHECK (window_kind IN ('historic','incr')),
  chunk_kind text NOT NULL CHECK (chunk_kind IN ('all','range','null','window')),
  chunk_from timestamp, chunk_to timestamp,
  status text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','running','done','failed')),
  rows_fetched bigint NOT NULL DEFAULT 0, rows_loaded bigint, oracle_count bigint,
  rows_quarantined bigint NOT NULL DEFAULT 0,
  duplicates_skipped bigint NOT NULL DEFAULT 0,
  idempotent_skip boolean NOT NULL DEFAULT false,
  attempts int NOT NULL DEFAULT 0, started_at timestamptz, ended_at timestamptz, last_heartbeat timestamptz,
  error text);

-- ensure newer columns exist on existing databases
ALTER TABLE bronze_ctl.load_chunk ADD COLUMN IF NOT EXISTS rows_quarantined bigint NOT NULL DEFAULT 0;
ALTER TABLE bronze_ctl.load_chunk ADD COLUMN IF NOT EXISTS duplicates_skipped bigint NOT NULL DEFAULT 0;
ALTER TABLE bronze_ctl.load_chunk ADD COLUMN IF NOT EXISTS idempotent_skip boolean NOT NULL DEFAULT false;

CREATE INDEX IF NOT EXISTS ix_chunk_run ON bronze_ctl.load_chunk (run_id, status);
CREATE INDEX IF NOT EXISTS ix_chunk_tbl ON bronze_ctl.load_chunk (zone, src_schema, src_table, window_kind, status);
CREATE INDEX IF NOT EXISTS ix_chunk_running ON bronze_ctl.load_chunk (status) WHERE status = 'running';

-- quarantined rows that fail row-level data quality checks
CREATE TABLE IF NOT EXISTS bronze_ctl.load_quarantine (
  quarantine_id bigserial PRIMARY KEY,
  run_id bigint REFERENCES bronze_ctl.load_run,
  chunk_id bigint REFERENCES bronze_ctl.load_chunk,
  zone text NOT NULL,
  src_schema text NOT NULL,
  src_table text NOT NULL,
  rejection_reason text NOT NULL,
  row_data jsonb NOT NULL,
  quarantined_at timestamptz NOT NULL DEFAULT now());
CREATE INDEX IF NOT EXISTS ix_quarantine_tbl ON bronze_ctl.load_quarantine (zone, src_schema, src_table);
CREATE INDEX IF NOT EXISTS ix_quarantine_chunk ON bronze_ctl.load_quarantine (chunk_id);

-- schema history and drift audit
CREATE TABLE IF NOT EXISTS bronze_ctl.schema_history (
  history_id bigserial PRIMARY KEY,
  zone text NOT NULL,
  src_schema text NOT NULL,
  src_table text NOT NULL,
  column_name text NOT NULL,
  oracle_type text NOT NULL,
  pg_type text NOT NULL,
  change_type text NOT NULL,
  run_id bigint,
  chunk_id bigint,
  detected_at timestamptz NOT NULL DEFAULT now());
ALTER TABLE bronze_ctl.schema_history DROP CONSTRAINT IF EXISTS schema_history_change_type_check;
CREATE INDEX IF NOT EXISTS ix_schema_hist_tbl ON bronze_ctl.schema_history (zone, src_schema, src_table);

-- column mapping registry from zone source tables to target CFA360 entities
CREATE TABLE IF NOT EXISTS bronze_ctl.zone_column_mapping (
  mapping_id serial PRIMARY KEY,
  zone text NOT NULL DEFAULT 'NZ',
  src_table text NOT NULL,
  src_column text NOT NULL,
  target_entity text NOT NULL,
  target_field text NOT NULL,
  role text,
  mapping_type text,
  transform_rule text,
  confidence text,
  sample_value text,
  fill_pct numeric(5,2),
  notes text,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (zone, src_table, src_column, target_entity, target_field, role));
CREATE INDEX IF NOT EXISTS ix_col_map_lookup ON bronze_ctl.zone_column_mapping (zone, src_table, src_column);
CREATE INDEX IF NOT EXISTS ix_col_map_tgt ON bronze_ctl.zone_column_mapping (target_entity, target_field);

-- unmapped and classified columns (sensitive, skip, unknown)
CREATE TABLE IF NOT EXISTS bronze_ctl.zone_column_classification (
  class_id serial PRIMARY KEY,
  zone text NOT NULL DEFAULT 'NZ',
  src_table text NOT NULL,
  src_column text NOT NULL,
  classification text NOT NULL,
  is_sensitive boolean NOT NULL DEFAULT false,
  why_note text,
  sample_value text,
  fill_pct numeric(5,2),
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (zone, src_table, src_column));
CREATE INDEX IF NOT EXISTS ix_col_class_lookup ON bronze_ctl.zone_column_classification (zone, src_table, src_column);
CREATE INDEX IF NOT EXISTS ix_col_class_sensitive ON bronze_ctl.zone_column_classification (is_sensitive);

-- target CFA360 schema field specification
CREATE TABLE IF NOT EXISTS bronze_ctl.cfa_target_schema (
  target_id serial PRIMARY KEY,
  target_entity text NOT NULL,
  target_field text NOT NULL,
  data_type text NOT NULL,
  nullable boolean NOT NULL,
  status text,
  source_columns text,
  candidates text,
  join_keys text,
  confidence text,
  rule text,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (target_entity, target_field));

-- the upload log: start / progress / finish / retry / drift / error lines
CREATE TABLE IF NOT EXISTS bronze_ctl.load_event (
  event_id bigserial PRIMARY KEY, ts timestamptz NOT NULL DEFAULT now(), run_id bigint, chunk_id bigint,
  level text NOT NULL, event text NOT NULL, message text, details jsonb);
CREATE INDEX IF NOT EXISTS ix_event_ts ON bronze_ctl.load_event (ts DESC);
CREATE INDEX IF NOT EXISTS ix_event_chunk ON bronze_ctl.load_event (chunk_id);

-- ------------------------------------------------------------------ monitoring views
DROP VIEW IF EXISTS bronze_ctl.v_alerts CASCADE;
DROP VIEW IF EXISTS bronze_ctl.v_active_chunks CASCADE;
DROP VIEW IF EXISTS bronze_ctl.v_historic_progress CASCADE;
DROP VIEW IF EXISTS bronze_ctl.v_alerts CASCADE;
DROP VIEW IF EXISTS bronze_ctl.v_throughput_hourly CASCADE;
DROP VIEW IF EXISTS bronze_ctl.v_dq_summary CASCADE;
DROP VIEW IF EXISTS bronze_ctl.v_table_status CASCADE;
DROP VIEW IF EXISTS bronze_ctl.v_sensitive_columns CASCADE;
DROP VIEW IF EXISTS bronze_ctl.v_nz_mapping_summary CASCADE;

CREATE OR REPLACE VIEW bronze_ctl.v_table_status AS
SELECT c.zone, c.src_schema, c.src_table,
       COALESCE(c.source_system, c.zone) AS source_system,
       COALESCE(c.ingest_type, 'oracle') AS ingest_type,
       c.ingest_config,
       c.source_metadata,
       c.tgt_schema || '.' || c.tgt_table AS bronze_table, c.mode, c.enabled,
       s.historic_done_at, s.last_success_at,
       round((extract(epoch FROM now() - s.last_success_at) / 3600.0)::numeric, 1) AS lag_hours,
       c.freshness_sla_hours, c.est_rows,
       COALESCE(r.rows_loaded, 0) AS rows_loaded_all_runs, COALESCE(r.chunks_done, 0) AS chunks_done,
       COALESCE(r.chunks_failed, 0) AS chunks_failed,
       COALESCE(r.rows_quarantined, 0) AS rows_quarantined_all_runs,
       COALESCE(r.duplicates_skipped, 0) AS duplicates_skipped_all_runs
  FROM bronze_ctl.table_config c
  LEFT JOIN bronze_ctl.table_state s USING (zone, src_schema, src_table)
  LEFT JOIN LATERAL (
        SELECT sum(rows_loaded) FILTER (WHERE status = 'done') AS rows_loaded,
               count(*) FILTER (WHERE status = 'done') AS chunks_done,
               count(*) FILTER (WHERE status = 'failed') AS chunks_failed,
               sum(rows_quarantined) FILTER (WHERE status = 'done') AS rows_quarantined,
               sum(duplicates_skipped) FILTER (WHERE status = 'done') AS duplicates_skipped
          FROM bronze_ctl.load_chunk k
         WHERE k.zone = c.zone AND k.src_schema = c.src_schema AND k.src_table = c.src_table) r ON true;

CREATE OR REPLACE VIEW bronze_ctl.v_dq_summary AS
SELECT c.zone, c.src_schema, c.src_table,
       COALESCE(sum(k.rows_fetched), 0) AS rows_fetched_total,
       COALESCE(sum(k.rows_loaded), 0) AS rows_loaded_total,
       COALESCE(sum(k.rows_quarantined), 0) AS rows_quarantined_total,
       COALESCE(sum(k.duplicates_skipped), 0) AS duplicates_skipped_total,
       round(100.0 * COALESCE(sum(k.rows_quarantined), 0) / NULLIF(COALESCE(sum(k.rows_fetched), 0), 0), 2) AS quarantine_pct
  FROM bronze_ctl.table_config c
  LEFT JOIN bronze_ctl.load_chunk k USING (zone, src_schema, src_table)
 GROUP BY c.zone, c.src_schema, c.src_table;

CREATE OR REPLACE VIEW bronze_ctl.v_active_chunks AS
SELECT chunk_id, run_id, zone, src_table, window_kind, chunk_kind, chunk_from, chunk_to, attempts,
       rows_fetched, rows_quarantined, duplicates_skipped, now() - started_at AS elapsed,
       round((rows_fetched / GREATEST(extract(epoch FROM now() - started_at), 1))::numeric) AS rows_per_sec,
       now() - last_heartbeat AS since_heartbeat,
       (last_heartbeat < now() - interval '10 minutes') AS stalled
  FROM bronze_ctl.load_chunk WHERE status = 'running';

CREATE OR REPLACE VIEW bronze_ctl.v_historic_progress AS
SELECT zone, src_schema, src_table,
       count(DISTINCT (chunk_kind, chunk_from, chunk_to)) AS chunks_total,
       count(DISTINCT (chunk_kind, chunk_from, chunk_to)) FILTER (WHERE status = 'done') AS chunks_done,
       round(100.0 * count(DISTINCT (chunk_kind, chunk_from, chunk_to)) FILTER (WHERE status = 'done')
             / NULLIF(count(DISTINCT (chunk_kind, chunk_from, chunk_to)), 0), 1) AS pct_done,
       COALESCE(sum(rows_loaded) FILTER (WHERE status = 'done'), 0) AS rows_loaded,
       COALESCE(sum(rows_quarantined) FILTER (WHERE status = 'done'), 0) AS rows_quarantined,
       COALESCE(sum(duplicates_skipped) FILTER (WHERE status = 'done'), 0) AS duplicates_skipped,
       min(started_at) AS first_started, max(ended_at) AS last_ended
  FROM bronze_ctl.load_chunk WHERE window_kind = 'historic' GROUP BY zone, src_schema, src_table;

CREATE OR REPLACE VIEW bronze_ctl.v_throughput_hourly AS
SELECT date_trunc('hour', ended_at) AS hour, zone, count(*) AS chunks, sum(rows_loaded) AS rows_loaded,
       round((sum(rows_loaded) / GREATEST(sum(extract(epoch FROM ended_at - started_at)), 1))::numeric) AS rows_per_sec_per_worker
  FROM bronze_ctl.load_chunk WHERE status = 'done' AND ended_at IS NOT NULL GROUP BY 1, 2;

-- everything that needs a human: this is what `bronze_sync.py check` and your alerting should read
CREATE OR REPLACE VIEW bronze_ctl.v_alerts AS
WITH latest AS (
  SELECT DISTINCT ON (zone, src_schema, src_table, window_kind, chunk_kind, chunk_from, chunk_to) *
    FROM bronze_ctl.load_chunk
   ORDER BY zone, src_schema, src_table, window_kind, chunk_kind, chunk_from, chunk_to, chunk_id DESC)
SELECT 'critical'::text AS severity, zone, src_table, 'chunk failed (' || window_kind || ' ' || chunk_kind || ' '
       || COALESCE(chunk_from::text, '-') || '): ' || left(COALESCE(error, ''), 200) AS issue, ended_at AS since
  FROM latest l WHERE status = 'failed'
   -- historic / full chunks clear when the same chunk later succeeds; an incremental window stays open until the
   -- table's loaded-up-to time (last_window_to) moves past it, because retry windows have different boundaries
   AND (window_kind = 'historic' OR chunk_kind = 'all'
        OR chunk_to > COALESCE((SELECT s.last_window_to FROM bronze_ctl.table_state s WHERE s.zone = l.zone
                                  AND s.src_schema = l.src_schema AND s.src_table = l.src_table), '-infinity'))
UNION ALL
SELECT 'critical', zone, src_table, 'chunk running but no heartbeat for ' || (now() - last_heartbeat)::text, last_heartbeat
  FROM bronze_ctl.load_chunk WHERE status = 'running' AND last_heartbeat < now() - interval '10 minutes'
UNION ALL
SELECT 'warning', zone, src_table, 'stale: last success ' || lag_hours || ' h ago, SLA ' || freshness_sla_hours || ' h', last_success_at
  FROM bronze_ctl.v_table_status
 WHERE enabled AND historic_done_at IS NOT NULL AND lag_hours > freshness_sla_hours
UNION ALL
SELECT 'critical', zone, src_table, 'row count mismatch: fetched ' || rows_fetched || ', loaded ' || COALESCE(rows_loaded, 0)
       || ', quarantined ' || rows_quarantined || ', duplicates ' || duplicates_skipped
       || COALESCE(', oracle ' || oracle_count, ''), ended_at
  FROM latest WHERE status = 'done' AND ended_at > now() - interval '3 days'
   AND ((rows_fetched - COALESCE(rows_quarantined, 0) - COALESCE(duplicates_skipped, 0)) IS DISTINCT FROM rows_loaded
        OR (oracle_count IS NOT NULL AND oracle_count <> rows_fetched))
UNION ALL
SELECT 'warning', zone, src_table, 'missing mapping column required for CFA360: ' || column_name, detected_at
  FROM bronze_ctl.schema_history
 WHERE change_type = 'mapping_column_missing' AND detected_at > now() - interval '3 days';

-- view of all sensitive columns flagged across zones
CREATE OR REPLACE VIEW bronze_ctl.v_sensitive_columns AS
SELECT zone, src_table, src_column, why_note AS reason, sample_value, fill_pct
  FROM bronze_ctl.zone_column_classification
 WHERE is_sensitive = true
 ORDER BY zone, src_table, src_column;

-- view summarizing mapping coverage per source table across all zones
CREATE OR REPLACE VIEW bronze_ctl.v_nz_mapping_summary AS
WITH all_tbls AS (
    SELECT zone, src_table FROM bronze_ctl.zone_column_mapping
    UNION
    SELECT zone, src_table FROM bronze_ctl.zone_column_classification
)
SELECT t.zone, t.src_table,
       count(DISTINCT m.src_column) AS mapped_columns_count,
       count(DISTINCT m.target_field) AS target_fields_count,
       count(DISTINCT c.src_column) FILTER (WHERE c.is_sensitive = true) AS sensitive_columns_count,
       count(DISTINCT c.src_column) FILTER (WHERE c.classification ILIKE '%UNKNOWN%') AS unknown_columns_count,
       count(DISTINCT c.src_column) AS total_unmapped_count
  FROM all_tbls t
  LEFT JOIN bronze_ctl.zone_column_mapping m
    ON m.zone = t.zone AND m.src_table = t.src_table
  LEFT JOIN bronze_ctl.zone_column_classification c
    ON c.zone = t.zone AND c.src_table = t.src_table
 GROUP BY t.zone, t.src_table
 ORDER BY t.zone, t.src_table;
