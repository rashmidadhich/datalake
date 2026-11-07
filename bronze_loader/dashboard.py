#!/usr/bin/env python3
"""
Web UI Dashboard for Bronze Loader (Oracle -> Postgres Bronze Data Lake).
Provides a pure real-time operational interface:
- Real-Time Database Ingestion & Chunk Control Plane (PostgreSQL bronze_ctl)
- Real-Time Bronze Data Explorer (Browse actual rows from bronze_nz and bronze_ez tables)
- Real-Time Bronze Quarantine Console (Real rows from bronze_ctl.load_quarantine)
- Interactive Mapping Configuration Manager (Add, edit, and delete mappings & sensitive checks)
- Schema Evolution & Data Quality Audit (bronze_ctl.schema_history & v_dq_summary)
- Architecture Blueprint & Technical Specifications (Oracle Queries, PySpark scripts)

Run:
    python dashboard.py [--port 8080] [--host 0.0.0.0]
or:
    python bronze_sync.py ui [--port 8080]
"""
import argparse
import datetime as dt
import decimal
import json
import os
import sys
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlparse
from psycopg.types.json import Jsonb

import bronze_sync as bs


def json_serializer(obj):
    if isinstance(obj, (dt.datetime, dt.date)):
        return obj.isoformat()
    if isinstance(obj, (dt.timedelta,)):
        return str(obj)
    if isinstance(obj, (decimal.Decimal,)):
        return float(obj)
    raise TypeError(f"Type {type(obj)} not serializable")


def get_oracle_tables(zone="NZ"):
    """Connect directly to the Oracle DB connection and retrieve all accessible tables and schemas."""
    try:
        src = bs.SOURCE_FACTORY(zone, {})
        current = src.current_schema() if hasattr(src, "current_schema") else "UNKNOWN"
        schemas = src.schemas() if hasattr(src, "schemas") else []
        tables = src.all_accessible_tables() if hasattr(src, "all_accessible_tables") else []
        src.close()
        return {
            "zone": zone,
            "connected": True,
            "current_schema": current,
            "schemas": schemas,
            "tables": tables,
            "count": len(tables),
        }
    except Exception as e:
        return {
            "zone": zone,
            "connected": False,
            "error": str(e),
            "schemas": [],
            "tables": [],
            "count": 0,
        }


def get_oracle_columns(zone="NZ", schema=None, table=None):
    """Fetch live column definitions from Oracle for a given table."""
    if not table:
        return {"zone": zone, "table": table, "columns": []}
    try:
        src = bs.SOURCE_FACTORY(zone, {})
        s = schema or (src.current_schema() if hasattr(src, "current_schema") else None)
        cols = src.columns(s, table)
        pks = set(src.pk_cols(s, table)) if hasattr(src, "pk_cols") else set()
        src.close()
        return {
            "zone": zone,
            "schema": s,
            "table": table,
            "columns": [{"name": c["name"], "type": c["type"], "nullable": c["nullable"], "is_pk": c["name"] in pks} for c in cols],
            "count": len(cols)
        }
    except Exception as e:
        return {"zone": zone, "schema": schema, "table": table, "error": str(e), "columns": []}


def get_db_data():
    """Fetch complete real-time monitoring state from bronze_ctl views and tables."""
    ctl = bs.connect()
    try:
        # Table status
        tables = bs.rowdict(ctl.execute(
            "SELECT zone, src_schema, src_table, source_system, source_metadata, bronze_table, mode, enabled, "
            "historic_done_at, last_success_at, lag_hours, freshness_sla_hours, est_rows, "
            "rows_loaded_all_runs, chunks_done, chunks_failed, rows_quarantined_all_runs, "
            "duplicates_skipped_all_runs, ingest_type, ingest_config FROM bronze_ctl.v_table_status ORDER BY source_system, src_table"
        ))

        # Historic progress
        historic = bs.rowdict(ctl.execute(
            "SELECT zone, src_schema, src_table, chunks_total, chunks_done, pct_done, "
            "rows_loaded, rows_quarantined, duplicates_skipped, first_started, last_ended "
            "FROM bronze_ctl.v_historic_progress ORDER BY zone, src_table"
        ))

        # Active chunks
        active = bs.rowdict(ctl.execute(
            "SELECT chunk_id, run_id, zone, src_table, window_kind, chunk_kind, chunk_from, chunk_to, "
            "attempts, rows_fetched, rows_quarantined, duplicates_skipped, elapsed, rows_per_sec, "
            "since_heartbeat, stalled FROM bronze_ctl.v_active_chunks ORDER BY chunk_id DESC"
        ))

        # Alerts
        alerts = bs.rowdict(ctl.execute(
            "SELECT severity, zone, src_table, issue, since FROM bronze_ctl.v_alerts ORDER BY severity, zone, src_table"
        ))

        # DQ Summary
        dq_summary = bs.rowdict(ctl.execute(
            "SELECT zone, src_schema, src_table, rows_fetched_total, rows_loaded_total, "
            "rows_quarantined_total, duplicates_skipped_total, quarantine_pct "
            "FROM bronze_ctl.v_dq_summary ORDER BY zone, src_table"
        ))

        # Recent Quarantined Rows (all real rows)
        quarantine_rows = bs.rowdict(ctl.execute(
            "SELECT quarantine_id, run_id, chunk_id, zone, src_schema, src_table, "
            "rejection_reason, row_data, quarantined_at "
            "FROM bronze_ctl.load_quarantine ORDER BY quarantine_id DESC LIMIT 50"
        ))

        # Schema History
        schema_hist = bs.rowdict(ctl.execute(
            "SELECT history_id, zone, src_schema, src_table, column_name, oracle_type, pg_type, "
            "change_type, run_id, chunk_id, detected_at "
            "FROM bronze_ctl.schema_history ORDER BY history_id DESC LIMIT 50"
        ))

        # Recent Load Events
        events = bs.rowdict(ctl.execute(
            "SELECT event_id, ts, run_id, chunk_id, level, event, message, details "
            "FROM bronze_ctl.load_event ORDER BY event_id DESC LIMIT 50"
        ))

        # Recent Runs
        runs = bs.rowdict(ctl.execute(
            "SELECT run_id, mode, started_at, ended_at, status, host, pid "
            "FROM bronze_ctl.load_run ORDER BY run_id DESC LIMIT 10"
        ))

        # Zone Mapping Summary (NZ & EZ)
        try:
            nz_summary = bs.rowdict(ctl.execute(
                "SELECT zone, src_table, mapped_columns_count, target_fields_count, "
                "sensitive_columns_count, unknown_columns_count, total_unmapped_count "
                "FROM bronze_ctl.v_nz_mapping_summary ORDER BY zone, src_table"
            ))
        except Exception:
            nz_summary = []

        # Sensitive Columns (NZ & EZ)
        try:
            sensitive_cols = bs.rowdict(ctl.execute(
                "SELECT zone, src_table, src_column, reason, sample_value, fill_pct "
                "FROM bronze_ctl.v_sensitive_columns ORDER BY zone, src_table, src_column"
            ))
        except Exception:
            sensitive_cols = []

        # CFA Target Fields
        try:
            cfa_targets = bs.rowdict(ctl.execute(
                "SELECT target_entity, target_field, data_type, nullable, status, "
                "source_columns, confidence, rule FROM bronze_ctl.cfa_target_schema "
                "ORDER BY target_entity, target_field"
            ))
        except Exception:
            cfa_targets = []

        # Mapping Rules (NZ & EZ)
        try:
            mapping_rules = bs.rowdict(ctl.execute(
                "SELECT zone, src_table, src_column, target_entity, target_field, mapping_type, transform_rule, confidence "
                "FROM bronze_ctl.zone_column_mapping ORDER BY zone, src_table, src_column"
            ))
        except Exception:
            mapping_rules = []

        # Source Catalog (grouping tables by marked source: SZ_zone, EZ_zone, NZ_zone, Teerva, portal, FMS)
        try:
            source_catalog = bs.rowdict(ctl.execute(
                "SELECT source_name, oracle_schema, oracle_table, bronze_target_table, mode, "
                "pk_cols, watermark_col, est_rows, enabled, source_metadata, ingest_type, ingest_config "
                "FROM bronze_ctl.v_source_catalog ORDER BY source_name, oracle_table"
            ))
        except Exception:
            source_catalog = []

        # Real sample records from each bronze table
        sample_data = {}
        for t in tables:
            b_table = t["bronze_table"]
            try:
                rows = bs.rowdict(ctl.execute(f"SELECT * FROM {b_table} ORDER BY _loaded_at DESC LIMIT 25"))
                sample_data[b_table] = rows
            except Exception:
                sample_data[b_table] = []

        # Aggregated KPI Stats
        total_loaded = sum(t["rows_loaded_all_runs"] or 0 for t in tables)
        total_quarantined = sum(t["rows_quarantined_all_runs"] or 0 for t in tables)
        total_dups = sum(t["duplicates_skipped_all_runs"] or 0 for t in tables)
        total_chunks_done = sum(t["chunks_done"] or 0 for t in tables)
        total_chunks_failed = sum(t["chunks_failed"] or 0 for t in tables)

        return {
            "timestamp": dt.datetime.now().isoformat(),
            "kpi": {
                "tables_count": len(tables),
                "active_chunks_count": len(active),
                "alerts_count": len(alerts),
                "total_rows_loaded": total_loaded,
                "total_rows_quarantined": total_quarantined,
                "total_duplicates_skipped": total_dups,
                "chunks_done": total_chunks_done,
                "chunks_failed": total_chunks_failed,
            },
            "tables": tables,
            "source_catalog": source_catalog,
            "historic": historic,
            "active": active,
            "alerts": alerts,
            "dq_summary": dq_summary,
            "quarantine_rows": quarantine_rows,
            "schema_history": schema_hist,
            "events": events,
            "runs": runs,
            "nz_summary": nz_summary,
            "sensitive_cols": sensitive_cols,
            "cfa_targets": cfa_targets,
            "mapping_rules": mapping_rules,
            "sample_data": sample_data,
            "oracle_catalog": get_oracle_tables("NZ"),
        }
    finally:
        ctl.close()


HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>BSNL CFA Data Lake | Real-Time Control Plane & Mapping Manager</title>
  <!-- Tailwind CSS -->
  <script src="https://cdn.tailwindcss.com"></script>
  <!-- FontAwesome Icons -->
  <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
  <script>
    tailwind.config = {
      theme: {
        extend: {
          colors: {
            bsnl: {
              navy: '#003366',
              blue: '#0066b2',
              sky: '#0284c7',
              orange: '#f58220',
              gold: '#eab308',
              slate: '#0f172a'
            },
            alt: {
              container: '#e8f1f8',
              border: '#b0cde5',
              card: '#1f558f',
              cardhover: '#194575',
              sandbox: '#dbeafe',
              security: '#e2e8f0'
            }
          }
        }
      }
    }
  </script>
  <style>
    @import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;500;600&display=swap');
    body {
      font-family: 'Plus Jakarta Sans', sans-serif;
      background-color: #f8fafc;
      color: #1e293b;
    }
    code, pre, .font-mono {
      font-family: 'JetBrains Mono', monospace;
    }
    .flow-line {
      stroke-dasharray: 6, 6;
      animation: flowDash 1.2s linear infinite;
    }
    @keyframes flowDash {
      to {
        stroke-dashoffset: -24;
      }
    }
    @keyframes pulseGlow {
      0%, 100% { opacity: 0.9; transform: scale(1); }
      50% { opacity: 0.4; transform: scale(1.15); }
    }
    .pulse-glow {
      animation: pulseGlow 2s ease-in-out infinite;
    }
    ::-webkit-scrollbar {
      width: 6px;
      height: 6px;
    }
    ::-webkit-scrollbar-track {
      background: #f1f5f9;
    }
    ::-webkit-scrollbar-thumb {
      background: #cbd5e1;
      border-radius: 4px;
    }
    ::-webkit-scrollbar-thumb:hover {
      background: #94a3b8;
    }
  </style>
</head>
<body class="min-h-screen bg-slate-50 flex flex-col text-slate-800">

  <!-- ===================================================================
       HEADER & NAVIGATION BAR (PURE REAL-TIME)
  ==================================================================== -->
  <header class="bg-white border-b border-slate-200 sticky top-0 z-50 shadow-sm">
    <div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
      <div class="flex items-center justify-between h-16">
        <!-- Logo and Brand -->
        <div class="flex items-center space-x-3">
          <div class="w-10 h-10 rounded-lg bg-gradient-to-br from-blue-700 to-sky-500 flex items-center justify-center text-white font-black text-xl shadow-md shadow-blue-500/20">
            B
          </div>
          <div>
            <div class="flex items-center space-x-2">
              <span class="font-extrabold text-slate-900 text-lg tracking-tight">BSNL CFA Data Lake</span>
              <span class="bg-blue-100 text-blue-800 text-[11px] font-bold px-2 py-0.5 rounded-full border border-blue-200">Real-Time Ingestion</span>
            </div>
            <p class="text-xs text-slate-500 font-medium">South &amp; North Zonal DC Oracle Ingestion &bull; PySpark &amp; PostgreSQL Bronze Lakehouse</p>
          </div>
        </div>

        <!-- Global Live Status & Refresh -->
        <div class="flex items-center space-x-3">
          <div class="flex items-center space-x-2 bg-emerald-50 text-emerald-700 px-3 py-1.5 rounded-lg border border-emerald-200 text-xs font-semibold">
            <span class="w-2 h-2 rounded-full bg-emerald-500 pulse-glow"></span>
            <span id="global-status-text">Live Database: Connected</span>
          </div>
          <button onclick="fetchData()" class="px-3 py-1.5 bg-slate-100 hover:bg-slate-200 text-slate-700 rounded-lg text-xs font-semibold flex items-center space-x-1.5 border border-slate-300 transition-colors shadow-sm">
            <i class="fa-solid fa-arrows-rotate text-slate-500"></i>
            <span>Refresh Now</span>
          </button>
          <span id="auto-refresh-badge" class="hidden sm:inline-block text-[11px] font-mono text-slate-500 bg-slate-100 px-2 py-1 rounded border border-slate-200">Auto: 10s</span>
        </div>
      </div>

      <!-- Navigation Tabs -->
      <nav class="flex space-x-1 border-t border-slate-100 py-2 overflow-x-auto text-sm font-medium">
        <button id="tab-btn-controlplane" onclick="switchTab('controlplane')" class="tab-btn active px-3.5 py-2 rounded-lg font-semibold flex items-center space-x-2 text-white bg-blue-700 shadow-sm transition-all whitespace-nowrap">
          <i class="fa-solid fa-server"></i>
          <span>Ingestion &amp; Chunks Control Plane</span>
          <span id="badge-tables-count" class="bg-blue-500/30 text-white text-[10px] px-1.5 py-0.5 rounded-full ml-1">Live</span>
        </button>

        <button id="tab-btn-dataexplorer" onclick="switchTab('dataexplorer')" class="tab-btn px-3.5 py-2 rounded-lg font-medium flex items-center space-x-2 text-slate-600 hover:text-slate-900 hover:bg-slate-100 transition-all whitespace-nowrap">
          <i class="fa-solid fa-database"></i>
          <span>Real-Time Bronze Data Explorer</span>
          <span id="badge-records-count" class="bg-emerald-100 text-emerald-800 text-[10px] px-1.5 py-0.5 rounded-full font-bold ml-1">Live Rows</span>
        </button>

        <button id="tab-btn-quarantine" onclick="switchTab('quarantine')" class="tab-btn px-3.5 py-2 rounded-lg font-medium flex items-center space-x-2 text-slate-600 hover:text-slate-900 hover:bg-slate-100 transition-all whitespace-nowrap">
          <i class="fa-solid fa-shield-halved"></i>
          <span>Bronze Quarantine Console</span>
          <span id="badge-quarantine-count" class="bg-rose-100 text-rose-700 text-[10px] px-1.5 py-0.5 rounded-full font-bold ml-1">0</span>
        </button>

        <button id="tab-btn-mapping" onclick="switchTab('mapping')" class="tab-btn px-3.5 py-2 rounded-lg font-medium flex items-center space-x-2 text-slate-600 hover:text-slate-900 hover:bg-slate-100 transition-all whitespace-nowrap">
          <i class="fa-solid fa-table-columns"></i>
          <span>Column Mappings &amp; Checks Manager</span>
          <span id="badge-mapping-count" class="bg-purple-100 text-purple-800 text-[10px] px-1.5 py-0.5 rounded-full font-bold ml-1">139 Mappings</span>
        </button>

        <button id="tab-btn-schema" onclick="switchTab('schema')" class="tab-btn px-3.5 py-2 rounded-lg font-medium flex items-center space-x-2 text-slate-600 hover:text-slate-900 hover:bg-slate-100 transition-all whitespace-nowrap">
          <i class="fa-solid fa-code-branch"></i>
          <span>Schema Drift &amp; DQ Audit</span>
          <span class="bg-amber-100 text-amber-800 text-[10px] px-1.5 py-0.5 rounded-full font-bold ml-1">Audit Log</span>
        </button>

        <button id="tab-btn-blueprint" onclick="switchTab('blueprint')" class="tab-btn px-3.5 py-2 rounded-lg font-medium flex items-center space-x-2 text-slate-600 hover:text-slate-900 hover:bg-slate-100 transition-all whitespace-nowrap">
          <i class="fa-solid fa-diagram-project"></i>
          <span>Architecture &amp; DC Specs</span>
          <span class="bg-slate-200 text-slate-700 text-[10px] px-1.5 py-0.5 rounded-full font-bold ml-1">Reference</span>
        </button>
      </nav>
    </div>
  </header>

  <!-- Real-Time Toast Notification -->
  <div id="toast-notification" class="fixed top-20 right-8 z-50 hidden max-w-md bg-white border border-slate-200 rounded-xl shadow-xl p-4 transform transition-all duration-300">
    <div class="flex items-start space-x-3">
      <div id="toast-icon-container" class="w-7 h-7 rounded-lg bg-emerald-100 text-emerald-700 flex items-center justify-center text-sm font-bold flex-shrink-0">
        <i class="fa-solid fa-circle-check"></i>
      </div>
      <div class="flex-1">
        <h4 id="toast-title" class="text-xs font-bold text-slate-900">Success</h4>
        <p id="toast-message" class="text-xs text-slate-600 mt-0.5 font-sans">Mapping saved successfully.</p>
      </div>
      <button onclick="hideToast()" class="text-slate-400 hover:text-slate-600">
        <i class="fa-solid fa-xmark text-sm"></i>
      </button>
    </div>
  </div>

  <main class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8 flex-1">

    <!-- ===================================================================
         TAB 1: INGESTION & CHUNKS CONTROL PLANE (REAL-TIME POSTGRESQL BRONZE_CTL)
    ==================================================================== -->
    <div id="tab-controlplane" class="tab-content block space-y-6">
      <div class="bg-gradient-to-r from-slate-900 via-indigo-950 to-slate-900 rounded-xl p-6 text-white shadow-md border border-slate-800 flex flex-col md:flex-row md:items-center md:justify-between gap-4">
        <div>
          <div class="flex items-center space-x-2">
            <span class="bg-purple-500/20 text-purple-300 text-xs font-mono font-bold px-2 py-0.5 rounded border border-purple-400/30">POSTGRESQL BRONZE_CTL</span>
            <span class="text-xs text-slate-400 font-mono">localhost:5432 / datalake</span>
          </div>
          <h2 class="text-xl font-extrabold text-white mt-1">Live Database Ingestion &amp; Chunk Control Plane</h2>
          <p class="text-xs text-slate-300 mt-1 max-w-2xl">
            Real-time status of physical source-to-bronze replication jobs across South Zone (Clarity CRM) and North Zone (12 SAP CRM tables), with live worker heartbeat monitoring and SLA tracking.
          </p>
        </div>
        <div class="flex items-center space-x-3">
          <button onclick="openIngestModal()" class="px-3.5 py-2 bg-emerald-600 hover:bg-emerald-500 text-white rounded-lg text-xs font-semibold flex items-center space-x-1.5 transition-colors shadow">
            <i class="fa-solid fa-file-import"></i>
            <span>Ingest Source File / JDBC</span>
          </button>
          <button onclick="fetchData()" class="px-3.5 py-2 bg-indigo-600 hover:bg-indigo-500 text-white rounded-lg text-xs font-semibold flex items-center space-x-1.5 transition-colors shadow">
            <i class="fa-solid fa-arrows-rotate"></i>
            <span>Refresh State</span>
          </button>
        </div>
      </div>

      <!-- KPI Metrics Strip -->
      <div class="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-6 gap-3">
        <div class="bg-white rounded-xl border border-slate-200 shadow-sm p-4">
          <div class="flex items-center justify-between">
            <span class="text-xs font-semibold text-slate-500 uppercase tracking-wider">Configured Tables</span>
            <i class="fa-solid fa-table text-indigo-500 text-sm"></i>
          </div>
          <div id="kpi-tables" class="text-2xl font-black text-slate-900 mt-2 font-mono">0</div>
          <span class="text-[11px] text-slate-500">NZ &amp; EZ source tables</span>
        </div>

        <div class="bg-white rounded-xl border border-slate-200 shadow-sm p-4">
          <div class="flex items-center justify-between">
            <span class="text-xs font-semibold text-slate-500 uppercase tracking-wider">Active In-Flight</span>
            <i class="fa-solid fa-bolt text-amber-500 text-sm"></i>
          </div>
          <div id="kpi-active" class="text-2xl font-black text-amber-600 mt-2 font-mono">0</div>
          <span class="text-[11px] text-slate-500">Parallel chunk workers</span>
        </div>

        <div class="bg-white rounded-xl border border-slate-200 shadow-sm p-4">
          <div class="flex items-center justify-between">
            <span class="text-xs font-semibold text-slate-500 uppercase tracking-wider">Rows Loaded</span>
            <i class="fa-solid fa-database text-emerald-500 text-sm"></i>
          </div>
          <div id="kpi-rows" class="text-2xl font-black text-emerald-700 mt-2 font-mono">0</div>
          <span class="text-[11px] text-slate-500">Clean bronze rows</span>
        </div>

        <div class="bg-white rounded-xl border border-slate-200 shadow-sm p-4">
          <div class="flex items-center justify-between">
            <span class="text-xs font-semibold text-slate-500 uppercase tracking-wider">Dups Skipped</span>
            <i class="fa-solid fa-copy text-sky-500 text-sm"></i>
          </div>
          <div id="kpi-dups" class="text-2xl font-black text-sky-700 mt-2 font-mono">0</div>
          <span class="text-[11px] text-slate-500">Idempotency saved</span>
        </div>

        <div class="bg-white rounded-xl border border-slate-200 shadow-sm p-4">
          <div class="flex items-center justify-between">
            <span class="text-xs font-semibold text-slate-500 uppercase tracking-wider">Quarantined</span>
            <i class="fa-solid fa-triangle-exclamation text-rose-500 text-sm"></i>
          </div>
          <div id="kpi-quarantine" class="text-2xl font-black text-rose-700 mt-2 font-mono">0</div>
          <span class="text-[11px] text-slate-500">DQ violation rows</span>
        </div>

        <div class="bg-white rounded-xl border border-slate-200 shadow-sm p-4">
          <div class="flex items-center justify-between">
            <span class="text-xs font-semibold text-slate-500 uppercase tracking-wider">Active Alerts</span>
            <i class="fa-solid fa-bell text-rose-500 text-sm"></i>
          </div>
          <div id="kpi-alerts" class="text-2xl font-black text-slate-900 mt-2 font-mono">0</div>
          <span class="text-[11px] text-slate-500">SLAs, stalls, schemas</span>
        </div>
      </div>

      <!-- Critical Alerts Section -->
      <div id="alerts-card" class="hidden bg-rose-50 border border-rose-200 rounded-xl p-5 shadow-sm">
        <div class="flex items-center justify-between mb-3">
          <div class="flex items-center space-x-2">
            <i class="fa-solid fa-circle-radiation text-rose-600 text-base"></i>
            <h3 class="text-sm font-bold text-rose-900">Active Pipeline Alerts &amp; SLA Breaches</h3>
          </div>
          <span id="alerts-count-badge" class="bg-rose-200 text-rose-800 text-xs font-bold px-2 py-0.5 rounded-full">0 alerts</span>
        </div>
        <div id="alerts-list" class="space-y-2 max-h-56 overflow-y-auto"></div>
      </div>

      <!-- In-Flight Active Chunks & Worker Monitor -->
      <div class="bg-white rounded-xl border border-slate-200 shadow-sm p-5">
        <div class="flex items-center justify-between mb-4 pb-3 border-b border-slate-100">
          <div>
            <h3 class="text-sm font-bold text-slate-900 flex items-center space-x-2">
              <span class="w-2.5 h-2.5 rounded-full bg-amber-500 pulse-glow"></span>
              <span>In-Flight Active Chunks &amp; Worker Pool</span>
            </h3>
            <p class="text-xs text-slate-500 mt-0.5">Live heartbeat and stalled worker detection (alert triggered if heartbeat exceeds 120s)</p>
          </div>
          <span id="active-chunks-count-badge" class="bg-slate-100 text-slate-700 text-xs font-mono font-bold px-2.5 py-1 rounded">0 active</span>
        </div>
        <div class="overflow-x-auto">
          <table class="w-full text-left border-collapse text-xs">
            <thead>
              <tr class="bg-slate-50 text-slate-600 font-semibold border-b border-slate-200 uppercase tracking-wider text-[11px]">
                <th class="py-2.5 px-3">Chunk ID</th>
                <th class="py-2.5 px-3">Run ID</th>
                <th class="py-2.5 px-3">Zone / Table</th>
                <th class="py-2.5 px-3">Window</th>
                <th class="py-2.5 px-3">Range [From &rarr; To]</th>
                <th class="py-2.5 px-3 text-right">Fetched</th>
                <th class="py-2.5 px-3">Elapsed</th>
                <th class="py-2.5 px-3">Speed</th>
                <th class="py-2.5 px-3">Last Heartbeat</th>
                <th class="py-2.5 px-3">Status</th>
              </tr>
            </thead>
            <tbody id="active-chunks-body" class="divide-y divide-slate-100 font-mono">
              <tr><td colspan="10" class="py-6 text-center text-slate-400 font-sans">No chunks currently running</td></tr>
            </tbody>
          </table>
        </div>
      </div>

      <!-- Source System Catalog & Filter Bar -->
      <div class="bg-white rounded-xl border border-slate-200 shadow-sm p-5">
        <div class="flex flex-col md:flex-row md:items-center md:justify-between gap-4">
          <div>
            <div class="flex items-center space-x-2">
              <span class="bg-indigo-100 text-indigo-800 text-[10px] font-bold font-mono px-2 py-0.5 rounded border border-indigo-200">SINGLE ORACLE SCHEMA ARCHITECTURE</span>
              <span class="text-xs text-slate-500 font-mono">Dynamic Lineage Tagging</span>
            </div>
            <h3 class="text-sm font-bold text-slate-900 mt-1 flex items-center space-x-2">
              <i class="fa-solid fa-layer-group text-indigo-600"></i>
              <span>Source Systems &amp; Table Lineage Catalog</span>
            </h3>
            <p class="text-xs text-slate-500 mt-0.5 max-w-2xl">
              All tables from the single Oracle schema are tagged with their business source (<strong class="text-slate-800">Teerva</strong>, <strong class="text-slate-800">portal</strong>, <strong class="text-slate-800">FMS</strong>, <strong class="text-slate-800">SZ_zone</strong>, <strong class="text-slate-800">NZ_zone</strong>, <strong class="text-slate-800">EZ_zone</strong>). Ingested Bronze rows automatically receive <code class="font-mono bg-slate-100 px-1 py-0.5 rounded text-[11px] text-indigo-800">_source</code> and <code class="font-mono bg-slate-100 px-1 py-0.5 rounded text-[11px] text-indigo-800">_src_table</code> metadata columns.
            </p>
          </div>
          <div class="flex flex-wrap items-center gap-2">
            <div>
              <label class="text-[10px] uppercase font-bold text-slate-400 block mb-0.5">Filter by Source:</label>
              <select id="source-system-filter" onchange="filterTablesBySource()" class="px-2.5 py-1.5 text-xs rounded-lg border border-slate-300 focus:outline-none focus:ring-2 focus:ring-indigo-500 font-mono bg-white">
                <option value="">All Source Systems</option>
              </select>
            </div>
            <div>
              <label class="text-[10px] uppercase font-bold text-slate-400 block mb-0.5">Ingest Mode:</label>
              <select id="ingest-type-filter" onchange="filterTablesBySource()" class="px-2.5 py-1.5 text-xs rounded-lg border border-slate-300 focus:outline-none focus:ring-2 focus:ring-indigo-500 font-mono bg-white">
                <option value="">All Ingest Modes</option>
                <option value="oracle">Oracle DB</option>
                <option value="parquet">Parquet</option>
                <option value="csv">CSV / TSV</option>
                <option value="jdbc">JDBC / SQL</option>
              </select>
            </div>
            <div>
              <label class="text-[10px] uppercase font-bold text-slate-400 block mb-0.5">Search Table:</label>
              <input type="text" id="table-search-input" oninput="filterTablesBySource()" placeholder="Search table name..." class="px-2.5 py-1.5 text-xs rounded-lg border border-slate-300 focus:outline-none focus:ring-2 focus:ring-indigo-500 font-mono">
            </div>
          </div>
        </div>
        <!-- Dynamic Source Pills -->
        <div id="source-system-pills" class="flex flex-wrap items-center gap-2 mt-4 pt-3 border-t border-slate-100">
          <span class="text-[11px] text-slate-400 font-medium">Source Systems:</span>
          <!-- Dynamic pills rendered by JS -->
        </div>
      </div>

      <!-- Table Status & SLA Monitoring Table -->
      <div class="bg-white rounded-xl border border-slate-200 shadow-sm p-5">
        <div class="flex items-center justify-between mb-4 pb-3 border-b border-slate-100">
          <div>
            <h3 class="text-sm font-bold text-slate-900 flex items-center space-x-2">
              <i class="fa-solid fa-list-check text-indigo-600"></i>
              <span>Source Tables Replication Status &amp; Freshness SLA</span>
            </h3>
            <p class="text-xs text-slate-500 mt-0.5">Physical replication into PostgreSQL bronze schema (bronze_nz / bronze_ez)</p>
          </div>
          <span id="filtered-tables-count-badge" class="bg-slate-100 text-slate-700 text-xs font-mono font-bold px-2.5 py-1 rounded">0 tables</span>
        </div>
        <div class="overflow-x-auto">
          <table class="w-full text-left border-collapse text-xs">
            <thead>
              <tr class="bg-slate-50 text-slate-600 font-semibold border-b border-slate-200 uppercase tracking-wider text-[11px]">
                <th class="py-2.5 px-3">Zone</th>
                <th class="py-2.5 px-3">Source System</th>
                <th class="py-2.5 px-3">Ingest Type</th>
                <th class="py-2.5 px-3">Source Table</th>
                <th class="py-2.5 px-3">Bronze Table</th>
                <th class="py-2.5 px-3">Mode</th>
                <th class="py-2.5 px-3 text-right">Est. Rows</th>
                <th class="py-2.5 px-3 text-right">Loaded</th>
                <th class="py-2.5 px-3 text-right">Chunks (Done / Total)</th>
                <th class="py-2.5 px-3 text-right">Dups</th>
                <th class="py-2.5 px-3 text-right">Quarantined</th>
                <th class="py-2.5 px-3">Lag / SLA</th>
                <th class="py-2.5 px-3">Health</th>
                <th class="py-2.5 px-3 text-right">Action</th>
              </tr>
            </thead>
            <tbody id="tables-status-body" class="divide-y divide-slate-100 font-mono">
              <tr><td colspan="13" class="py-6 text-center text-slate-400 font-sans">Loading table status...</td></tr>
            </tbody>
          </table>
        </div>
      </div>

      <!-- Historic Progress Grid -->
      <div class="bg-white rounded-xl border border-slate-200 shadow-sm p-5">
        <div class="flex items-center justify-between mb-4 pb-3 border-b border-slate-100">
          <div>
            <h3 class="text-sm font-bold text-slate-900 flex items-center space-x-2">
              <i class="fa-solid fa-bars-progress text-blue-600"></i>
              <span>Historic Load Progress Tracker</span>
            </h3>
            <p class="text-xs text-slate-500 mt-0.5">Initial backfill chunk completion across all source tables</p>
          </div>
        </div>
        <div id="historic-progress-grid" class="grid grid-cols-1 md:grid-cols-2 gap-4">
          <div class="text-slate-400 text-xs py-4">Loading historic progress...</div>
        </div>
      </div>

      <!-- Recent Load Events Log -->
      <div class="bg-white rounded-xl border border-slate-200 shadow-sm p-5">
        <div class="flex items-center justify-between mb-3 pb-2 border-b border-slate-100">
          <div>
            <h4 class="text-sm font-bold text-slate-900 flex items-center space-x-2">
              <i class="fa-solid fa-clock-rotate-left text-slate-600"></i>
              <span>Real-Time Ingestion Event Log</span>
            </h4>
            <p class="text-xs text-slate-500">Live operational events from bronze_ctl.load_event</p>
          </div>
        </div>
        <div class="overflow-x-auto max-h-64 overflow-y-auto font-mono text-[11px]">
          <table class="w-full text-left border-collapse">
            <thead class="sticky top-0 bg-slate-50">
              <tr class="text-slate-600 uppercase text-[10px] border-b border-slate-200">
                <th class="py-1.5 px-2">Timestamp</th>
                <th class="py-1.5 px-2">Level</th>
                <th class="py-1.5 px-2">Event</th>
                <th class="py-1.5 px-2">Chunk / Run</th>
                <th class="py-1.5 px-2">Message</th>
              </tr>
            </thead>
            <tbody id="events-log-body" class="divide-y divide-slate-100">
              <tr><td colspan="5" class="py-4 text-center text-slate-400 font-sans">No events logged</td></tr>
            </tbody>
          </table>
        </div>
      </div>
    </div>

    <!-- ===================================================================
         TAB 2: REAL-TIME BRONZE DATA EXPLORER (LIVE ROWS BROWSER)
    ==================================================================== -->
    <div id="tab-dataexplorer" class="tab-content hidden space-y-6">
      <div class="bg-gradient-to-r from-emerald-900 via-teal-900 to-slate-900 rounded-xl p-6 text-white shadow-md border border-emerald-800 flex flex-col md:flex-row md:items-center md:justify-between gap-4">
        <div>
          <div class="flex items-center space-x-2">
            <span class="bg-emerald-500/20 text-emerald-300 text-xs font-mono font-bold px-2 py-0.5 rounded border border-emerald-400/30">POSTGRESQL REPLICATED BRONZE DATA</span>
            <span class="text-xs text-slate-300 font-mono">Real-Time Row Browser</span>
          </div>
          <h2 class="text-xl font-extrabold text-white mt-1">Real-Time Bronze Data Explorer</h2>
          <p class="text-xs text-slate-200 mt-1 max-w-2xl">
            Inspect physical rows stored in PostgreSQL bronze schemas (<code class="font-mono bg-emerald-950/60 px-1 py-0.5 rounded">bronze_nz</code> &amp; <code class="font-mono bg-emerald-950/60 px-1 py-0.5 rounded">bronze_ez</code>). Click any row to inspect complete column values, raw JSON payloads, and lineage metadata.
          </p>
        </div>
        <div class="flex items-center space-x-3">
          <label class="text-xs text-emerald-200 font-semibold">Select Table:</label>
          <select id="explorer-table-select" onchange="switchExplorerTable(this.value)" class="bg-slate-900 text-white text-xs font-mono font-semibold px-3 py-2 rounded-lg border border-emerald-500/40 focus:outline-none focus:ring-2 focus:ring-emerald-400">
            <!-- Dynamically populated -->
          </select>
        </div>
      </div>

      <!-- Table Browser Card -->
      <div class="bg-white rounded-xl border border-slate-200 shadow-sm p-5">
        <div class="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-3 mb-4 pb-3 border-b border-slate-100">
          <div>
            <h3 id="explorer-table-title" class="text-sm font-bold text-slate-900 flex items-center space-x-2">
              <i class="fa-solid fa-table-cells text-emerald-600"></i>
              <span>bronze_nz.but000</span>
            </h3>
            <p id="explorer-table-subtitle" class="text-xs text-slate-500 mt-0.5">Showing latest replicated rows with lineage columns (_loaded_at, _chunk_id, _run_id)</p>
          </div>
          <div class="flex items-center space-x-3">
            <div class="relative">
              <input type="text" id="explorer-search-input" oninput="filterExplorerTable()" placeholder="Search records..." class="pl-8 pr-3 py-1.5 text-xs rounded-lg border border-slate-300 focus:outline-none focus:ring-2 focus:ring-emerald-500 w-52 sm:w-64 font-mono">
              <i class="fa-solid fa-magnifying-glass absolute left-2.5 top-2.5 text-slate-400 text-xs"></i>
            </div>
            <span id="explorer-row-count-badge" class="bg-emerald-100 text-emerald-800 text-xs font-mono font-bold px-2 py-1 rounded">0 rows</span>
          </div>
        </div>

        <div class="overflow-x-auto max-h-[550px] overflow-y-auto">
          <table class="w-full text-left border-collapse text-xs">
            <thead class="sticky top-0 bg-slate-50 shadow-sm">
              <tr id="explorer-table-headers" class="text-slate-600 font-semibold border-b border-slate-200 uppercase text-[10px] tracking-wider font-mono">
                <!-- Dynamically populated -->
              </tr>
            </thead>
            <tbody id="explorer-table-body" class="divide-y divide-slate-100 font-mono text-[11px]">
              <!-- Dynamically populated -->
            </tbody>
          </table>
        </div>
      </div>
    </div>

    <!-- ===================================================================
         TAB 3: BRONZE QUARANTINE CONSOLE (100% REAL-TIME LOAD_QUARANTINE)
    ==================================================================== -->
    <div id="tab-quarantine" class="tab-content hidden space-y-6">
      <div class="bg-gradient-to-r from-rose-950 via-red-900 to-slate-900 rounded-xl p-6 text-white shadow-md border border-rose-800 flex flex-col md:flex-row md:items-center md:justify-between gap-4">
        <div>
          <div class="flex items-center space-x-2">
            <span class="bg-rose-500/20 text-rose-300 text-xs font-mono font-bold px-2 py-0.5 rounded border border-rose-400/30">POSTGRESQL LOAD_QUARANTINE</span>
            <span class="text-xs text-slate-300 font-mono">Real-Time Quarantine Records</span>
          </div>
          <h2 class="text-xl font-extrabold text-white mt-1">Bronze Data Quality &amp; Quarantine Console</h2>
          <p class="text-xs text-slate-200 mt-1 max-w-2xl">
            Physical rows rejected by Data Quality checks (null primary keys, invalid watermarks, schema mismatches). Sensitive columns are automatically sanitized (<code class="font-mono bg-rose-950/60 px-1 py-0.5 rounded text-rose-200">[REDACTED_SENSITIVE]</code>) before writing to quarantine.
          </p>
        </div>
      </div>

      <!-- Real Quarantine Breakdown Cards -->
      <div class="grid grid-cols-1 sm:grid-cols-3 gap-4">
        <div class="bg-white rounded-xl border border-rose-200 shadow-sm p-4">
          <div class="flex items-center justify-between">
            <span class="text-xs font-semibold text-rose-700 uppercase tracking-wider">Total Quarantined</span>
            <i class="fa-solid fa-triangle-exclamation text-rose-600 text-base"></i>
          </div>
          <div id="quarantine-total-count" class="text-2xl font-black text-rose-700 mt-2 font-mono">0</div>
          <span class="text-[11px] text-slate-500">Rows in bronze_ctl.load_quarantine</span>
        </div>

        <div class="bg-white rounded-xl border border-amber-200 shadow-sm p-4">
          <div class="flex items-center justify-between">
            <span class="text-xs font-semibold text-amber-700 uppercase tracking-wider">PK Null Violations</span>
            <i class="fa-solid fa-key text-amber-600 text-base"></i>
          </div>
          <div id="quarantine-pk-count" class="text-2xl font-black text-amber-700 mt-2 font-mono">0</div>
          <span class="text-[11px] text-slate-500">Primary key is null or blank</span>
        </div>

        <div class="bg-white rounded-xl border border-blue-200 shadow-sm p-4">
          <div class="flex items-center justify-between">
            <span class="text-xs font-semibold text-blue-700 uppercase tracking-wider">Sensitive Redactions</span>
            <i class="fa-solid fa-user-shield text-blue-600 text-base"></i>
          </div>
          <div id="quarantine-redacted-count" class="text-2xl font-black text-blue-700 mt-2 font-mono">100%</div>
          <span class="text-[11px] text-slate-500">Sensitive fields masked in payload</span>
        </div>
      </div>

      <!-- Quarantine Records Table -->
      <div class="bg-white rounded-xl border border-slate-200 shadow-sm p-5">
        <div class="flex items-center justify-between mb-4 pb-3 border-b border-slate-100">
          <div>
            <h3 class="text-sm font-bold text-slate-900 flex items-center space-x-2">
              <i class="fa-solid fa-ban text-rose-600"></i>
              <span>Rejection Audit Trail (bronze_ctl.load_quarantine)</span>
            </h3>
            <p class="text-xs text-slate-500 mt-0.5">Physical records quarantined during chunk ingestion runs</p>
          </div>
        </div>
        <div class="overflow-x-auto">
          <table class="w-full text-left border-collapse text-xs">
            <thead>
              <tr class="bg-slate-50 text-slate-600 font-semibold border-b border-slate-200 uppercase tracking-wider text-[11px]">
                <th class="py-2.5 px-3">Quarantine ID</th>
                <th class="py-2.5 px-3">Run / Chunk</th>
                <th class="py-2.5 px-3">Source Table</th>
                <th class="py-2.5 px-3">Rejection Reason</th>
                <th class="py-2.5 px-3">Quarantined At</th>
                <th class="py-2.5 px-3 text-right">Action</th>
              </tr>
            </thead>
            <tbody id="quarantine-table-body" class="divide-y divide-slate-100 font-mono">
              <tr><td colspan="6" class="py-6 text-center text-slate-400 font-sans">No quarantined rows</td></tr>
            </tbody>
          </table>
        </div>
      </div>
    </div>

    <!-- ===================================================================
         TAB 4: COLUMN MAPPINGS & CHECKS MANAGER (ADD / CHANGE INTERFACE)
    ==================================================================== -->
    <div id="tab-mapping" class="tab-content hidden space-y-6">
      <div class="bg-gradient-to-r from-blue-900 via-sky-900 to-blue-900 rounded-xl p-6 text-white shadow-md border border-blue-800 flex flex-col md:flex-row md:items-center md:justify-between gap-4">
        <div>
          <div class="flex items-center space-x-2">
            <span class="bg-sky-500/20 text-sky-300 text-xs font-mono font-bold px-2 py-0.5 rounded border border-sky-400/30">MAPPING CONFIGURATION &amp; MANAGEMENT</span>
            <span class="text-xs text-slate-300 font-mono">Live Ingestion &amp; Check Catalog</span>
          </div>
          <h2 class="text-xl font-extrabold text-white mt-1">Column Mappings &amp; Checks Configuration Manager</h2>
          <p class="text-xs text-slate-200 mt-1 max-w-2xl">
            Add, update, or remove source-to-target column mappings, transformation expressions, and automated security checks (such as sensitive PII / password redaction) with live synchronization into PostgreSQL <code class="font-mono bg-blue-950 px-1 py-0.5 rounded">bronze_ctl</code> and JSON specifications.
          </p>
        </div>
        <div class="flex items-center space-x-3">
          <button onclick="openAddMappingModal()" class="px-4 py-2.5 bg-emerald-600 hover:bg-emerald-500 text-white rounded-lg text-xs font-bold flex items-center space-x-2 transition-all shadow-md">
            <i class="fa-solid fa-plus"></i>
            <span>Add Column Mapping</span>
          </button>
        </div>
      </div>

      <!-- Architectural Explanation Cards: WHERE TO MAP & WHERE CHECKS LIVE -->
      <div class="grid grid-cols-1 md:grid-cols-3 gap-4">
        <div class="bg-white rounded-xl border border-blue-200 shadow-sm p-4">
          <div class="flex items-center space-x-2 mb-2">
            <span class="w-6 h-6 rounded-full bg-blue-100 text-blue-700 flex items-center justify-center font-bold text-xs">1</span>
            <h4 class="text-xs font-bold text-slate-900 uppercase tracking-wider">Where Columns Are Mapped</h4>
          </div>
          <p class="text-xs text-slate-600 mb-2">
            Column-to-entity mappings are persisted in:
          </p>
          <ul class="text-[11px] text-slate-700 space-y-1 font-mono">
            <li>&bull; <strong class="text-blue-700">bronze_ctl.zone_column_mapping</strong> (PostgreSQL DB)</li>
            <li>&bull; <strong class="text-blue-700">nz_mapping_data.json &amp; ez_mapping_data.json</strong> (source specs)</li>
            <li>&bull; <strong class="text-blue-700">zone_mappings_master.csv / .json</strong> (unified catalog)</li>
            <li>&bull; <strong class="text-blue-700">config.toml</strong> (tables, PKs, SLAs)</li>
          </ul>
        </div>

        <div class="bg-white rounded-xl border border-amber-200 shadow-sm p-4">
          <div class="flex items-center space-x-2 mb-2">
            <span class="w-6 h-6 rounded-full bg-amber-100 text-amber-700 flex items-center justify-center font-bold text-xs">2</span>
            <h4 class="text-xs font-bold text-slate-900 uppercase tracking-wider">Where Checks Are Defined</h4>
          </div>
          <p class="text-xs text-slate-600 mb-2">
            Validation rules are executed in Python:
          </p>
          <ul class="text-[11px] text-slate-700 space-y-1 font-mono">
            <li>&bull; <strong class="text-amber-800">bronze_sync.py:cmd_load_chunk()</strong> (PK &amp; Watermark null checks)</li>
            <li>&bull; <strong class="text-amber-800">mapping.py:sanitize_raw_payload()</strong> (sensitive data masking)</li>
            <li>&bull; <strong class="text-amber-800">mapping.py:validate_schema_mapping()</strong></li>
          </ul>
        </div>

        <div class="bg-white rounded-xl border border-emerald-200 shadow-sm p-4">
          <div class="flex items-center space-x-2 mb-2">
            <span class="w-6 h-6 rounded-full bg-emerald-100 text-emerald-700 flex items-center justify-center font-bold text-xs">3</span>
            <h4 class="text-xs font-bold text-slate-900 uppercase tracking-wider">How to Change in Real Time</h4>
          </div>
          <p class="text-xs text-slate-600 mb-2">
            Click <strong>"Add Column Mapping"</strong> above or <strong>"Edit"</strong> in any row below to update mappings and security flags. Changes are immediately active in the ingestion pipeline!
          </p>
        </div>
      </div>

      <!-- 25 Enterprise Physical Tables Grid Summary (NZ SAP + EZ Siebel) -->
      <div class="bg-white rounded-xl border border-slate-200 shadow-sm p-5">
        <div class="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-3 mb-4 pb-3 border-b border-slate-100">
          <div>
            <h3 class="text-sm font-bold text-slate-900 flex items-center space-x-2">
              <i class="fa-solid fa-cubes text-blue-600"></i>
              <span>25 Enterprise Source Physical Tables (NZ SAP CRM &amp; EZ Siebel CRM)</span>
            </h3>
            <p class="text-xs text-slate-500 mt-0.5">Physical tables replicated from North Zone and East Zone Oracle schemas into bronze lake</p>
          </div>
          <div class="flex items-center space-x-1.5 bg-slate-100 p-1 rounded-lg text-xs font-semibold">
            <button id="btn-tbl-filter-all" onclick="setTablesZoneFilter('ALL')" class="px-2.5 py-1 rounded-md bg-white text-slate-900 shadow-xs transition-all">All (25)</button>
            <button id="btn-tbl-filter-nz" onclick="setTablesZoneFilter('NZ')" class="px-2.5 py-1 rounded-md text-slate-600 hover:text-slate-900 transition-all">North Zone SAP (12)</button>
            <button id="btn-tbl-filter-ez" onclick="setTablesZoneFilter('EZ')" class="px-2.5 py-1 rounded-md text-slate-600 hover:text-slate-900 transition-all">East Zone Siebel (13)</button>
          </div>
        </div>
        <div id="nz-tables-grid" class="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-4 gap-3">
          <!-- Populated dynamically -->
        </div>
      </div>

      <!-- 48 Protected Sensitive Columns Card -->
      <div class="bg-amber-50/70 border border-amber-200 rounded-xl p-5 shadow-sm">
        <div class="flex items-center justify-between mb-3">
          <div class="flex items-center space-x-2">
            <i class="fa-solid fa-user-shield text-amber-700 text-base"></i>
            <h3 class="text-sm font-bold text-amber-950">48 Protected Sensitive Columns (Automatic [REDACTED_SENSITIVE] Guard)</h3>
          </div>
          <span class="bg-amber-200 text-amber-900 text-xs font-bold px-2 py-0.5 rounded-full font-mono">48 Columns Redacted (9 NZ + 39 EZ)</span>
        </div>
        <p class="text-xs text-amber-800 mb-3">
          These columns contain sensitive PII, passwords, voucher codes, tax identifiers, banking details (IFSC, UMRN), card/CVV data, or OTPs across North Zone (SAP CRM) and East Zone (Siebel CRM). The ingestion engine automatically masks these values before persisting raw error payloads into <code class="font-mono bg-amber-100 px-1 py-0.5 rounded">bronze_ctl.load_quarantine</code>.
        </p>
        <div class="overflow-x-auto max-h-72 overflow-y-auto">
          <table class="w-full text-left border-collapse text-xs">
            <thead class="sticky top-0 bg-amber-100 shadow-xs">
              <tr class="text-amber-900 font-semibold border-b border-amber-200 text-[11px]">
                <th class="py-2 px-3">Zone</th>
                <th class="py-2 px-3">Table</th>
                <th class="py-2 px-3">Sensitive Column</th>
                <th class="py-2 px-3">Classification Reason</th>
                <th class="py-2 px-3">Sample Value</th>
                <th class="py-2 px-3">Redaction Action</th>
              </tr>
            </thead>
            <tbody id="sensitive-columns-body" class="divide-y divide-amber-200/60 font-mono text-slate-800">
              <!-- Populated dynamically -->
            </tbody>
          </table>
        </div>
      </div>

      <!-- Live Oracle Database Tables Catalog -->
      <div class="bg-white rounded-xl border border-indigo-200 shadow-sm p-5">
        <div class="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-3 mb-4 pb-3 border-b border-indigo-100">
          <div>
            <div class="flex items-center space-x-2">
              <span id="oracle-status-pill" class="inline-flex items-center space-x-1.5 px-2 py-0.5 rounded-full text-[11px] font-bold bg-emerald-100 text-emerald-800 border border-emerald-300">
                <span class="w-2 h-2 rounded-full bg-emerald-500 animate-pulse"></span>
                <span>Oracle DB Connected</span>
              </span>
              <span id="oracle-host-info" class="text-xs text-slate-500 font-mono">10.201.217.18:1521/oltpdr</span>
            </div>
            <h3 class="text-sm font-bold text-slate-900 flex items-center space-x-2 mt-1">
              <i class="fa-solid fa-database text-indigo-600"></i>
              <span>Live Tables Pulled From Oracle DB Connection</span>
            </h3>
            <p id="oracle-catalog-subtitle" class="text-xs text-slate-500 mt-0.5">Directly introspected from Oracle data dictionary (all_tables &amp; all_tab_columns)</p>
          </div>
          <div class="flex flex-wrap items-center gap-2">
            <select id="oracle-zone-select" onchange="fetchLiveOracleTables(this.value)" class="px-2.5 py-1.5 text-xs rounded-lg border border-slate-300 focus:outline-none focus:ring-2 focus:ring-indigo-500 font-mono font-bold bg-white">
              <option value="NZ" selected>Zone NZ (North Zone SAP CRM)</option>
              <option value="EZ">Zone EZ (East Zone Siebel CRM)</option>
            </select>
            <select id="oracle-schema-filter" onchange="filterOracleCatalogTable()" class="px-2.5 py-1.5 text-xs rounded-lg border border-slate-300 focus:outline-none focus:ring-2 focus:ring-indigo-500 font-mono bg-white">
              <option value="">All Schemas</option>
            </select>
            <button onclick="fetchLiveOracleTables(document.getElementById('oracle-zone-select').value)" class="px-3 py-1.5 bg-indigo-50 hover:bg-indigo-100 text-indigo-700 border border-indigo-200 rounded-lg text-xs font-semibold flex items-center space-x-1.5 transition-colors">
              <i class="fa-solid fa-arrows-rotate"></i>
              <span>Refresh From Oracle</span>
            </button>
          </div>
        </div>

        <div class="overflow-x-auto">
          <table class="w-full text-left border-collapse text-xs">
            <thead>
              <tr class="bg-slate-50 text-slate-600 font-semibold border-b border-slate-200 uppercase text-[10px] tracking-wider font-mono">
                <th class="py-2.5 px-3">Zone</th>
                <th class="py-2.5 px-3">Source System</th>
                <th class="py-2.5 px-3">Oracle Schema</th>
                <th class="py-2.5 px-3">Source Table</th>
                <th class="py-2.5 px-3">Oracle Est. Rows</th>
                <th class="py-2.5 px-3">Bronze Status</th>
                <th class="py-2.5 px-3 text-right">Actions</th>
              </tr>
            </thead>
            <tbody id="oracle-tables-catalog-body" class="divide-y divide-slate-100 font-mono">
              <!-- Populated dynamically -->
            </tbody>
          </table>
        </div>
      </div>

      <!-- Active Mapping Rules Table with Edit and Delete Controls -->
      <div class="bg-white rounded-xl border border-slate-200 shadow-sm p-5">
        <div class="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-3 mb-4 pb-3 border-b border-slate-100">
          <div>
            <h3 class="text-sm font-bold text-slate-900 flex items-center space-x-2">
              <i class="fa-solid fa-arrows-split-up-and-left text-blue-600"></i>
              <span>Active Source-to-Target Column Mapping Rules</span>
            </h3>
            <p class="text-xs text-slate-500 mt-0.5">Live mapping specifications from bronze_ctl.zone_column_mapping</p>
          </div>
          <div class="flex flex-wrap items-center gap-2">
            <select id="mapping-zone-filter" onchange="filterMappingRules()" class="px-2.5 py-1.5 text-xs rounded-lg border border-slate-300 focus:outline-none focus:ring-2 focus:ring-blue-500 font-mono font-bold bg-white">
              <option value="">All Zones (NZ &amp; EZ)</option>
              <option value="NZ">North Zone (NZ - SAP CRM)</option>
              <option value="EZ">East Zone (EZ - Siebel CRM)</option>
            </select>
            <select id="mapping-table-filter" onchange="filterMappingRules()" class="px-2.5 py-1.5 text-xs rounded-lg border border-slate-300 focus:outline-none focus:ring-2 focus:ring-blue-500 font-mono bg-white">
              <option value="">All Tables (25)</option>
              <optgroup label="East Zone (EZ - Siebel CRM)">
                <option value="S_ORDER">S_ORDER</option>
                <option value="S_ORDER_X">S_ORDER_X</option>
                <option value="S_ORDER_TYPE">S_ORDER_TYPE</option>
                <option value="S_ORG_EXT">S_ORG_EXT</option>
                <option value="S_ORG_EXT_X">S_ORG_EXT_X</option>
                <option value="S_ORG_EXT_XM">S_ORG_EXT_XM</option>
                <option value="S_ASSET">S_ASSET</option>
                <option value="S_ASSET_X">S_ASSET_X</option>
                <option value="S_ASSET_XA">S_ASSET_XA</option>
                <option value="S_ASSET_XM">S_ASSET_XM</option>
                <option value="S_ADDR_PER">S_ADDR_PER</option>
                <option value="S_CONTACT">S_CONTACT</option>
                <option value="S_SRV_REQ">S_SRV_REQ</option>
              </optgroup>
              <optgroup label="North Zone (NZ - SAP CRM)">
                <option value="BUT000">BUT000</option>
                <option value="IBIB">IBIB</option>
                <option value="CRMD_ORDERADM_H">CRMD_ORDERADM_H</option>
                <option value="CRMD_ORDERADM_I">CRMD_ORDERADM_I</option>
                <option value="CRMD_ORDER_INDEX">CRMD_ORDER_INDEX</option>
                <option value="CRM_JEST">CRM_JEST</option>
                <option value="TJ30T">TJ30T</option>
                <option value="ADRC">ADRC</option>
                <option value="ADR2">ADR2</option>
                <option value="ADR6">ADR6</option>
                <option value="BUT021_FS">BUT021_FS</option>
                <option value="BUT050">BUT050</option>
              </optgroup>
            </select>
            <input type="text" id="mapping-search-input" oninput="filterMappingRules()" placeholder="Search column or entity..." class="px-3 py-1.5 text-xs rounded-lg border border-slate-300 focus:outline-none focus:ring-2 focus:ring-blue-500 w-48 font-mono">
            <button onclick="openAddMappingModal()" class="px-3 py-1.5 bg-blue-600 hover:bg-blue-700 text-white rounded-lg text-xs font-semibold flex items-center space-x-1.5 shadow-sm transition-all">
              <i class="fa-solid fa-plus"></i>
              <span>Add Mapping</span>
            </button>
          </div>
        </div>
        <div class="overflow-x-auto max-h-[500px] overflow-y-auto">
          <table class="w-full text-left border-collapse text-xs">
            <thead class="sticky top-0 bg-slate-50 shadow-sm">
              <tr class="text-slate-600 font-semibold border-b border-slate-200 uppercase text-[10px] tracking-wider font-mono">
                <th class="py-2 px-3">Zone</th>
                <th class="py-2 px-3">Source Table</th>
                <th class="py-2 px-3">Source Column</th>
                <th class="py-2 px-3">Target Entity</th>
                <th class="py-2 px-3">Target Field</th>
                <th class="py-2 px-3">Type</th>
                <th class="py-2 px-3">Transform Rule</th>
                <th class="py-2 px-3">Confidence</th>
                <th class="py-2 px-3">Sensitive Check</th>
                <th class="py-2 px-3 text-right">Actions</th>
              </tr>
            </thead>
            <tbody id="mapping-rules-body" class="divide-y divide-slate-100 font-mono text-[11px]">
              <!-- Populated dynamically -->
            </tbody>
          </table>
        </div>
      </div>

      <!-- CFA Target Entity Schema -->
      <div class="bg-white rounded-xl border border-slate-200 shadow-sm p-5">
        <div class="flex items-center justify-between mb-4 pb-3 border-b border-slate-100">
          <div>
            <h3 class="text-sm font-bold text-slate-900 flex items-center space-x-2">
              <i class="fa-solid fa-bullseye text-indigo-600"></i>
              <span>CFA360 Target Entity Schema Specifications</span>
            </h3>
            <p class="text-xs text-slate-500 mt-0.5">Target entity definitions fed by North Zone (SAP CRM) and East Zone (Siebel CRM) bronze tables</p>
          </div>
        </div>
        <div class="overflow-x-auto max-h-72 overflow-y-auto">
          <table class="w-full text-left border-collapse text-xs">
            <thead class="sticky top-0 bg-slate-50">
              <tr class="text-slate-600 font-semibold border-b border-slate-200 uppercase text-[11px]">
                <th class="py-2 px-3">Target Entity</th>
                <th class="py-2 px-3">Target Field</th>
                <th class="py-2 px-3">Type</th>
                <th class="py-2 px-3">Nullable</th>
                <th class="py-2 px-3">Source Columns (NZ SAP / EZ Siebel)</th>
                <th class="py-2 px-3">Confidence</th>
              </tr>
            </thead>
            <tbody id="cfa-targets-body" class="divide-y divide-slate-100 font-mono text-[11px]">
              <!-- Populated dynamically -->
            </tbody>
          </table>
        </div>
      </div>
    </div>

    <!-- ===================================================================
         TAB 5: SCHEMA DRIFT & DQ VERIFICATION AUDIT
    ==================================================================== -->
    <div id="tab-schema" class="tab-content hidden space-y-6">
      <div class="bg-gradient-to-r from-purple-950 via-slate-900 to-indigo-950 rounded-xl p-6 text-white shadow-md border border-purple-800 flex flex-col md:flex-row md:items-center md:justify-between gap-4">
        <div>
          <div class="flex items-center space-x-2">
            <span class="bg-purple-500/20 text-purple-300 text-xs font-mono font-bold px-2 py-0.5 rounded border border-purple-400/30">SCHEMA EVOLUTION AUDIT</span>
            <span class="text-xs text-slate-300 font-mono">bronze_ctl.schema_history &amp; v_dq_summary</span>
          </div>
          <h2 class="text-xl font-extrabold text-white mt-1">Schema Drift &amp; Data Quality Verification Audit</h2>
          <p class="text-xs text-slate-200 mt-1 max-w-2xl">
            Audit trail of every table mutation detected in Oracle, automated DDL alterations applied in PostgreSQL Bronze, and table-level Data Quality pass/quarantine statistics.
          </p>
        </div>
      </div>

      <div class="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <!-- Schema Evolution History -->
        <div class="bg-white rounded-xl border border-slate-200 shadow-sm p-5">
          <div class="flex items-center justify-between mb-3 pb-2 border-b border-slate-100">
            <div>
              <h4 class="text-sm font-bold text-slate-900 flex items-center space-x-2">
                <i class="fa-solid fa-code-branch text-purple-600"></i>
                <span>Schema Drift &amp; Evolution Audit Log</span>
              </h4>
              <p class="text-xs text-slate-500">Tracked column additions, type alterations, and mapping checks</p>
            </div>
          </div>
          <div class="overflow-x-auto max-h-96 overflow-y-auto">
            <table class="w-full text-left border-collapse text-xs">
              <thead class="sticky top-0 bg-slate-50">
                <tr class="text-slate-600 font-semibold border-b border-slate-200 uppercase text-[10px]">
                  <th class="py-2 px-2">Table</th>
                  <th class="py-2 px-2">Column</th>
                  <th class="py-2 px-2">Type (Ora &rarr; PG)</th>
                  <th class="py-2 px-2">Event</th>
                  <th class="py-2 px-2">Detected</th>
                </tr>
              </thead>
              <tbody id="schema-history-body" class="divide-y divide-slate-100 font-mono text-[11px]">
                <tr><td colspan="5" class="py-4 text-center text-slate-400 font-sans">No schema changes recorded</td></tr>
              </tbody>
            </table>
          </div>
        </div>

        <!-- Data Quality Rule Breakdown -->
        <div class="bg-white rounded-xl border border-slate-200 shadow-sm p-5">
          <div class="flex items-center justify-between mb-3 pb-2 border-b border-slate-100">
            <div>
              <h4 class="text-sm font-bold text-slate-900 flex items-center space-x-2">
                <i class="fa-solid fa-shield-virus text-emerald-600"></i>
                <span>Data Quality Verification Summary</span>
              </h4>
              <p class="text-xs text-slate-500">Fetched vs. Clean Loaded vs. Quarantined percentages</p>
            </div>
          </div>
          <div class="overflow-x-auto max-h-96 overflow-y-auto">
            <table class="w-full text-left border-collapse text-xs">
              <thead class="sticky top-0 bg-slate-50">
                <tr class="text-slate-600 font-semibold border-b border-slate-200 uppercase text-[10px]">
                  <th class="py-2 px-2">Table</th>
                  <th class="py-2 px-2 text-right">Fetched</th>
                  <th class="py-2 px-2 text-right">Loaded</th>
                  <th class="py-2 px-2 text-right">Dups</th>
                  <th class="py-2 px-2 text-right">Quarantine %</th>
                </tr>
              </thead>
              <tbody id="dq-summary-body" class="divide-y divide-slate-100 font-mono text-[11px]">
                <tr><td colspan="5" class="py-4 text-center text-slate-400 font-sans">No DQ summary data</td></tr>
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </div>

    
    <!-- ===================================================================
         TAB 6: ARCHITECTURE BLUEPRINT & TECHNICAL SPECIFICATIONS
    ==================================================================== -->
    <div id="tab-blueprint" class="tab-content hidden space-y-6">
      <div id="tab-blueprint-inner" class="space-y-6">
      
      <!-- Blueprint Executive Banner -->
      <div class="bg-white rounded-2xl p-6 border border-slate-200 shadow-sm flex flex-col md:flex-row items-start md:items-center justify-between gap-4">
        <div>
          <div class="flex items-center space-x-2">
            <span class="px-2.5 py-1 rounded bg-blue-100 text-blue-800 text-xs font-bold tracking-wide uppercase">Enterprise Production Architecture</span>
            <span class="text-xs text-slate-400 font-mono">BSNL-CFA-ARCH-v2.4</span>
          </div>
          <h1 class="text-2xl font-black text-slate-900 mt-1">DATA LAKE ARCHITECTURE</h1>
          <p class="text-sm text-slate-600 max-w-3xl mt-1">
            Seamless multi-datacenter data orchestration from 4 Zonal Data Centers into <strong class="text-blue-700">Dev Hyd Central Staging (South DC)</strong>, 
            running PySpark Bronze quality isolation, Silver SCD-2 lineage, and high-concurrency PostgreSQL serving for 50 Lakh subscribers.
          </p>
        </div>
        <div class="flex items-center space-x-3 shrink-0">
          <button onclick="switchTab('simulator')" class="px-4 py-2.5 bg-blue-700 hover:bg-blue-800 text-white rounded-xl text-sm font-semibold shadow-md shadow-blue-600/20 flex items-center space-x-2 transition-all">
            <i class="fa-solid fa-circle-play"></i>
            <span>Launch Pipeline Simulator</span>
          </button>
        </div>
      </div>

      <!-- THE ARCHITECTURE DIAGRAM CONTAINER (ALTEXSOFT STYLE) -->
      <div class="bg-white rounded-2xl p-6 border border-slate-200 shadow-sm overflow-x-auto">
        
        <!-- Live Flow Indicator Strip -->
        <div class="flex items-center justify-between mb-6 pb-4 border-b border-slate-100 text-xs text-slate-500">
          <div class="flex items-center space-x-4">
            <span class="flex items-center space-x-1.5"><span class="w-2.5 h-2.5 rounded-full bg-blue-600"></span><strong class="text-slate-800">4 Zonal DCs</strong>: South (Hyd), West (Pune), East (Kolkata), North (Chandigarh)</span>
            <span class="text-slate-300">|</span>
            <span class="flex items-center space-x-1.5"><span class="w-2.5 h-2.5 rounded-full bg-sky-500"></span><strong class="text-slate-800">Project Hub</strong>: Dev Hyd (South DC)</span>
            <span class="text-slate-300">|</span>
            <span class="flex items-center space-x-1.5"><span class="w-2.5 h-2.5 rounded-full bg-emerald-500"></span><strong class="text-slate-800">Ingestion Cadence</strong>: Hourly Delta (10k) + Nightly Full Dump (50L)</span>
          </div>
          <div class="flex items-center space-x-2 text-blue-600 font-semibold cursor-pointer" onclick="openArchModal('overview')">
            <i class="fa-solid fa-circle-info"></i>
            <span>Click any module for live technical specs</span>
          </div>
        </div>

        <!-- SVG Visual Conduits & Diagram Grid -->
        <div class="relative min-w-[1080px]">

          <!-- 4 PRIMARY COLUMNS GRID -->
          <div class="grid grid-cols-12 gap-5 items-stretch">
            
            <!-- COLUMN 1: DATA SOURCES (Col span 3) -->
            <div class="col-span-3 bg-[#e8f1f8] border-2 border-[#b0cde5] rounded-2xl p-4 flex flex-col justify-between shadow-sm">
              <div>
                <div class="flex items-center justify-between mb-3">
                  <h3 class="text-base font-extrabold text-[#1a4a84] tracking-wide">Data sources</h3>
                  <span class="text-[10px] bg-blue-200/70 text-blue-900 font-bold px-2 py-0.5 rounded">4 Zonal DCs</span>
                </div>
                
                <div class="space-y-3">
                  <!-- Structured Card -->
                  <div onclick="openArchModal('sources_structured')" class="bg-[#1f558f] hover:bg-[#194575] text-white rounded-xl p-3.5 shadow transition-all cursor-pointer group">
                    <div class="flex items-center justify-between mb-1.5">
                      <span class="font-bold text-sm tracking-wide">Structured</span>
                      <i class="fa-solid fa-table text-blue-200 group-hover:scale-110 transition-transform"></i>
                    </div>
                    <ul class="text-xs text-blue-100 space-y-1 list-disc list-inside">
                      <li><strong>4 DCs Clarity/CRM</strong> (SZ, WZ, EZ, NZ)</li>
                      <li><strong>UNMS</strong> (OSS Provisioning Engine)</li>
                      <li><strong>FMS Portal</strong> (TIP / Partner Masters)</li>
                      <li><strong>PMS & Billing</strong> (Sanchar Soft)</li>
                      <li><strong>IFTV & OTT</strong> Platforms</li>
                    </ul>
                  </div>

                  <!-- Semi-structured Card -->
                  <div onclick="openArchModal('sources_semi')" class="bg-[#1f558f] hover:bg-[#194575] text-white rounded-xl p-3.5 shadow transition-all cursor-pointer group">
                    <div class="flex items-center justify-between mb-1.5">
                      <span class="font-bold text-sm tracking-wide">Semi-structured</span>
                      <i class="fa-solid fa-code text-blue-200 group-hover:scale-110 transition-transform"></i>
                    </div>
                    <ul class="text-xs text-blue-100 space-y-1 list-disc list-inside">
                      <li>Payment Gateway Webhooks</li>
                      <li>Franchisee REST APIs (FMS JSON)</li>
                      <li>OLT Diagnostic XML feeds</li>
                      <li>Reconciliation CSVs</li>
                    </ul>
                  </div>

                  <!-- Unstructured Card -->
                  <div onclick="openArchModal('sources_unstructured')" class="bg-[#1f558f] hover:bg-[#194575] text-white rounded-xl p-3.5 shadow transition-all cursor-pointer group">
                    <div class="flex items-center justify-between mb-1.5">
                      <span class="font-bold text-sm tracking-wide">Unstructured</span>
                      <i class="fa-solid fa-file-lines text-blue-200 group-hover:scale-110 transition-transform"></i>
                    </div>
                    <ul class="text-xs text-blue-100 space-y-1 list-disc list-inside">
                      <li>Digital CAF scanned forms</li>
                      <li>Customer Call Center voice logs</li>
                      <li>OLT Syslogs & SNMP traps</li>
                      <li>Social care feedback tickets</li>
                    </ul>
                  </div>
                </div>
              </div>
              <div class="mt-3 pt-2 border-t border-blue-200/60 text-[11px] text-blue-900 font-medium text-center">
                <i class="fa-solid fa-server mr-1"></i> South, West, East, North Zonal DBs
              </div>
            </div>

            <!-- COLUMN 2: DATA INGESTION (Col span 2) -->
            <div class="col-span-2 bg-[#e8f1f8] border-2 border-[#b0cde5] rounded-2xl p-4 flex flex-col justify-between shadow-sm">
              <div>
                <div class="flex items-center justify-between mb-3">
                  <h3 class="text-base font-extrabold text-[#1a4a84] tracking-wide">Data ingestion</h3>
                  <span class="text-[10px] bg-blue-200/70 text-blue-900 font-bold px-2 py-0.5 rounded">Push-Based</span>
                </div>

                <div class="space-y-4">
                  <!-- Batch / Scheduled -->
                  <div onclick="openArchModal('ingestion_batch')" class="bg-[#1f558f] hover:bg-[#194575] text-white rounded-xl p-3.5 shadow transition-all cursor-pointer group">
                    <div class="flex items-center justify-between mb-1.5">
                      <span class="font-bold text-sm">Batch / scheduled</span>
                      <i class="fa-solid fa-clock text-blue-200 group-hover:rotate-45 transition-transform"></i>
                    </div>
                    <div class="text-xs text-blue-100 space-y-1">
                      <div class="p-1.5 bg-blue-900/40 rounded border border-blue-400/30">
                        <strong class="text-white block">Hourly Incremental:</strong>
                        <span>~10k rows/hr delta push</span>
                      </div>
                      <div class="p-1.5 bg-blue-900/40 rounded border border-blue-400/30 mt-1">
                        <strong class="text-white block">Nightly Full Dump:</strong>
                        <span>02:00 AM full 50L snapshot</span>
                      </div>
                    </div>
                  </div>

                  <!-- Central Connector Indicator -->
                  <div class="py-1 px-2 text-center text-xs font-bold text-blue-900 flex items-center justify-center space-x-1.5">
                    <i class="fa-solid fa-arrow-right text-blue-600"></i>
                    <span>Extract & Load</span>
                    <i class="fa-solid fa-arrow-right text-blue-600"></i>
                  </div>

                  <!-- Real-time Streaming -->
                  <div onclick="openArchModal('ingestion_stream')" class="bg-[#1f558f] hover:bg-[#194575] text-white rounded-xl p-3.5 shadow transition-all cursor-pointer group">
                    <div class="flex items-center justify-between mb-1.5">
                      <span class="font-bold text-sm">Real-time streaming</span>
                      <i class="fa-solid fa-bolt text-amber-300 group-hover:scale-125 transition-transform"></i>
                    </div>
                    <ul class="text-xs text-blue-100 space-y-1 list-disc list-inside">
                      <li>Kafka CDC Stream</li>
                      <li>Immediate Order State Changes</li>
                      <li>Instant UNMS provisioning trigger</li>
                    </ul>
                  </div>
                </div>
              </div>

              <div class="mt-3 pt-2 border-t border-blue-200/60 text-[11px] text-blue-900 font-medium text-center">
                <i class="fa-solid fa-network-wired mr-1"></i> MPLS Secure DB Push
              </div>
            </div>

            <!-- COLUMN 3: DATA STORAGE AND PROCESSING (Col span 4) -->
            <div class="col-span-4 bg-[#e8f1f8] border-2 border-[#b0cde5] rounded-2xl p-4 flex flex-col justify-between shadow-sm">
              <div>
                <div class="flex items-center justify-between mb-3">
                  <h3 class="text-base font-extrabold text-[#1a4a84] tracking-wide">Data storage and processing</h3>
                  <span class="text-[10px] bg-emerald-100 text-emerald-800 font-bold px-2 py-0.5 rounded border border-emerald-200">Dev Hyd Staging</span>
                </div>

                <!-- Sub-grid: Raw/Landing + Transform -->
                <div class="grid grid-cols-2 gap-3 mb-3">
                  
                  <!-- Raw / Landing Zone -->
                  <div onclick="openArchModal('storage_raw')" class="bg-[#1f558f] hover:bg-[#194575] text-white rounded-xl p-3 shadow cursor-pointer group">
                    <div class="flex items-center justify-between mb-2">
                      <span class="font-bold text-xs">Raw / landing</span>
                      <div class="flex space-x-1 text-blue-200">
                        <i class="fa-solid fa-plane-arrival"></i>
                        <i class="fa-solid fa-database"></i>
                      </div>
                    </div>
                    <div class="bg-blue-900/50 rounded-lg p-2 text-center border border-blue-400/30">
                      <span class="text-xs font-bold text-white block">Raw data store</span>
                      <span class="text-[10px] text-blue-200">Central Oracle Ingestion<br>+ Parquet Bronze Lake</span>
                    </div>
                    <div class="mt-2 text-[10px] text-blue-100 space-y-0.5">
                      <div>• LAND_SZ_ORDERS</div>
                      <div>• LAND_WZ_ORDERS</div>
                      <div>• Snappy Parquet (8x comp)</div>
                    </div>
                  </div>

                  <!-- Transform Engine -->
                  <div onclick="openArchModal('storage_transform')" class="bg-[#1f558f] hover:bg-[#194575] text-white rounded-xl p-3 shadow cursor-pointer group">
                    <div class="flex items-center justify-between mb-2">
                      <span class="font-bold text-xs">Transform</span>
                      <i class="fa-solid fa-gears text-blue-200 group-hover:rotate-90 transition-transform"></i>
                    </div>
                    <div class="space-y-1.5">
                      <div class="bg-white text-slate-800 rounded-lg p-1.5 text-center shadow-xs">
                        <span class="text-[11px] font-bold block">Batch processing</span>
                        <span class="text-[9px] text-slate-500 font-medium">PySpark Quality & Quarantine</span>
                      </div>
                      <div class="bg-white text-slate-800 rounded-lg p-1.5 text-center shadow-xs">
                        <span class="text-[11px] font-bold block">Micro-batch / Real-time</span>
                        <span class="text-[9px] text-slate-500 font-medium">Silver SCD Type 2 Lineage</span>
                      </div>
                    </div>
                  </div>

                </div>

                <!-- Processed Data (ODS Serving) -->
                <div onclick="openArchModal('storage_processed')" class="bg-[#1f558f] hover:bg-[#194575] text-white rounded-xl p-3 shadow cursor-pointer group mb-3">
                  <div class="flex items-center justify-between mb-1.5">
                    <div class="flex items-center space-x-2">
                      <i class="fa-solid fa-database text-blue-200"></i>
                      <span class="font-bold text-sm">Processed data (PostgreSQL ODS)</span>
                    </div>
                    <span class="bg-emerald-500 text-white text-[10px] px-2 py-0.5 rounded font-bold">Sub-10ms Serving</span>
                  </div>
                  <p class="text-xs text-blue-100">
                    High-speed indexed PostgreSQL Customer 360 & Partner 360 Master Tables (50 Lakh records, in-memory cache, zero duplicate history).
                  </p>
                </div>

                <!-- Analytical Sandboxes -->
                <div class="bg-[#dbeafe] border border-[#a2c4e6] rounded-xl p-3">
                  <span class="text-xs font-extrabold text-[#1a4a84] block mb-2">Analytical sandboxes</span>
                  <div class="grid grid-cols-3 gap-2">
                    <div onclick="openArchModal('sandbox_discovery')" class="bg-white text-slate-800 rounded-lg p-2 text-center shadow-xs cursor-pointer hover:bg-blue-50 transition-colors">
                      <i class="fa-solid fa-magnifying-glass-chart text-blue-600 text-sm mb-1"></i>
                      <span class="text-[10px] font-bold block">Data discovery</span>
                    </div>
                    <div onclick="openArchModal('sandbox_exploratory')" class="bg-white text-slate-800 rounded-lg p-2 text-center shadow-xs cursor-pointer hover:bg-blue-50 transition-colors">
                      <i class="fa-solid fa-chart-line text-blue-600 text-sm mb-1"></i>
                      <span class="text-[10px] font-bold block">Exploratory analysis</span>
                    </div>
                    <div onclick="openArchModal('sandbox_predictive')" class="bg-white text-slate-800 rounded-lg p-2 text-center shadow-xs cursor-pointer hover:bg-blue-50 transition-colors">
                      <i class="fa-solid fa-brain text-blue-600 text-sm mb-1"></i>
                      <span class="text-[10px] font-bold block">Predictive modeling</span>
                    </div>
                  </div>
                </div>

              </div>

              <div class="mt-3 pt-2 border-t border-blue-200/60 text-[11px] text-blue-900 font-medium text-center">
                <i class="fa-solid fa-shield-virus mr-1"></i> Automated Quarantine Split & Self-Healing
              </div>
            </div>

            <!-- COLUMN 4: DATA CONSUMPTION (Col span 3) -->
            <div class="col-span-3 bg-[#e8f1f8] border-2 border-[#b0cde5] rounded-2xl p-4 flex flex-col justify-between shadow-sm">
              <div>
                <div class="flex items-center justify-between mb-3">
                  <h3 class="text-base font-extrabold text-[#1a4a84] tracking-wide">Data consumption</h3>
                  <span class="text-[10px] bg-blue-200/70 text-blue-900 font-bold px-2 py-0.5 rounded">Omnichannel</span>
                </div>

                <div class="space-y-2.5">
                  
                  <!-- BI analytics -->
                  <div onclick="openArchModal('consume_bi')" class="bg-white hover:bg-slate-50 text-slate-800 border border-slate-200 rounded-xl p-2.5 shadow-xs flex items-center space-x-3 cursor-pointer group transition-colors">
                    <div class="w-8 h-8 rounded-lg bg-blue-100 text-blue-700 flex items-center justify-center shrink-0 group-hover:scale-110 transition-transform">
                      <i class="fa-solid fa-chart-pie text-sm"></i>
                    </div>
                    <div>
                      <span class="text-xs font-bold text-slate-900 block">BI analytics</span>
                      <span class="text-[10px] text-slate-500">CGM / GM Executive Dashboards & Circle KPIs</span>
                    </div>
                  </div>

                  <!-- Reporting -->
                  <div onclick="openArchModal('consume_reporting')" class="bg-white hover:bg-slate-50 text-slate-800 border border-slate-200 rounded-xl p-2.5 shadow-xs flex items-center space-x-3 cursor-pointer group transition-colors">
                    <div class="w-8 h-8 rounded-lg bg-blue-100 text-blue-700 flex items-center justify-center shrink-0 group-hover:scale-110 transition-transform">
                      <i class="fa-solid fa-file-contract text-sm"></i>
                    </div>
                    <div>
                      <span class="text-xs font-bold text-slate-900 block">Reporting</span>
                      <span class="text-[10px] text-slate-500">Daily Revenue Assurance & TRAI Compliance</span>
                    </div>
                  </div>

                  <!-- Data warehouse / ODS -->
                  <div onclick="openArchModal('consume_dwh')" class="bg-white hover:bg-slate-50 text-slate-800 border border-slate-200 rounded-xl p-2.5 shadow-xs flex items-center space-x-3 cursor-pointer group transition-colors">
                    <div class="w-8 h-8 rounded-lg bg-blue-100 text-blue-700 flex items-center justify-center shrink-0 group-hover:scale-110 transition-transform">
                      <i class="fa-solid fa-cubes text-sm"></i>
                    </div>
                    <div>
                      <span class="text-xs font-bold text-slate-900 block">Data warehouse / ODS</span>
                      <span class="text-[10px] text-slate-500">PostgreSQL Serving for Downstream Systems</span>
                    </div>
                  </div>

                  <!-- Real-time alerting -->
                  <div onclick="openArchModal('consume_alerts')" class="bg-white hover:bg-slate-50 text-slate-800 border border-slate-200 rounded-xl p-2.5 shadow-xs flex items-center space-x-3 cursor-pointer group transition-colors">
                    <div class="w-8 h-8 rounded-lg bg-amber-100 text-amber-700 flex items-center justify-center shrink-0 group-hover:scale-110 transition-transform">
                      <i class="fa-solid fa-triangle-exclamation text-sm"></i>
                    </div>
                    <div>
                      <span class="text-xs font-bold text-slate-900 block">Real-time alerting</span>
                      <span class="text-[10px] text-slate-500">Quarantine Spike & Provisioning SLA Breach</span>
                    </div>
                  </div>

                  <!-- Search / Querying -->
                  <div onclick="openArchModal('consume_api')" class="bg-white hover:bg-slate-50 text-slate-800 border border-slate-200 rounded-xl p-2.5 shadow-xs flex items-center space-x-3 cursor-pointer group transition-colors">
                    <div class="w-8 h-8 rounded-lg bg-emerald-100 text-emerald-700 flex items-center justify-center shrink-0 group-hover:scale-110 transition-transform">
                      <i class="fa-solid fa-bolt text-sm"></i>
                    </div>
                    <div>
                      <span class="text-xs font-bold text-slate-900 block">Search / Querying (APIs)</span>
                      <span class="text-[10px] text-slate-500">BSNL SelfCare App, IVRS, Call Center, OBD</span>
                    </div>
                  </div>

                </div>
              </div>

              <div class="mt-3 pt-2 border-t border-blue-200/60 text-[11px] text-blue-900 font-medium text-center">
                <i class="fa-solid fa-mobile-screen mr-1"></i> Sub-10ms Omnichannel Endpoints
              </div>
            </div>

          </div>

          <!-- SPANNING BOTTOM BAR: DATA SECURITY, GOVERNANCE, AND MONITORING -->
          <div onclick="openArchModal('security_gov')" class="mt-4 bg-[#e2e8f0] border-2 border-[#cbd5e1] rounded-xl p-3.5 flex items-center justify-between cursor-pointer hover:bg-slate-200 transition-colors shadow-xs">
            <div class="flex items-center space-x-3">
              <div class="w-7 h-7 rounded bg-blue-800 text-white flex items-center justify-center text-xs font-bold">
                <i class="fa-solid fa-shield-halved"></i>
              </div>
              <div>
                <span class="text-sm font-extrabold text-slate-800 block">Data security, governance, and monitoring</span>
                <span class="text-xs text-slate-600">Row-level RBAC · Immutable SCD-2 Bi-Temporal Audit Lineage · Automatic Quarantine Remediation · Zero Data Inflation Idempotency</span>
              </div>
            </div>
            <div class="flex items-center space-x-2 text-xs font-semibold text-blue-700">
              <span>View Governance Guardrails</span>
              <i class="fa-solid fa-chevron-right text-[10px]"></i>
            </div>
          </div>

        </div>

      </div>

      <!-- KEY ARCHITECTURE PILLARS (3 CARDS) -->
      <div class="grid grid-cols-1 md:grid-cols-3 gap-5">
        
        <div class="bg-white rounded-xl p-5 border border-slate-200 shadow-sm">
          <div class="flex items-center space-x-2 text-blue-700 font-bold text-sm mb-2">
            <i class="fa-solid fa-rotate text-blue-600"></i>
            <span>Dual Ingestion Strategy</span>
          </div>
          <p class="text-xs text-slate-600 leading-relaxed">
            <strong>Hourly Incremental (~10k rows)</strong> pushes fast modifications for live service provisioning. 
            <strong>Nightly Full Dump (50L at 02:00 AM)</strong> catches backdated entries, stale orders, and offline DBA edits, ensuring 100% self-healing with zero data loss.
          </p>
        </div>

        <div class="bg-white rounded-xl p-5 border border-slate-200 shadow-sm">
          <div class="flex items-center space-x-2 text-emerald-700 font-bold text-sm mb-2">
            <i class="fa-solid fa-fingerprint text-emerald-600"></i>
            <span>Idempotency & Quality Shield</span>
          </div>
          <p class="text-xs text-slate-600 leading-relaxed">
            Every incoming batch is cryptographically hashed. Re-runs are recognized instantly, preventing row multiplication. 
            Bronze rules trap missing keys and corrupted plans in Quarantine, keeping PostgreSQL pristine.
          </p>
        </div>

        <div class="bg-white rounded-xl p-5 border border-slate-200 shadow-sm">
          <div class="flex items-center space-x-2 text-purple-700 font-bold text-sm mb-2">
            <i class="fa-solid fa-clock-rotate-left text-purple-600"></i>
            <span>SCD Type 2 Bi-Temporal History</span>
          </div>
          <p class="text-xs text-slate-600 leading-relaxed">
            Customer tariff shifts, shifting of premises, and TIP changes never overwrite old records. Old versions are closed with effective timestamps; new versions open with <code class="bg-purple-50 text-purple-700 px-1 py-0.5 rounded text-[10px]">IS_CURRENT = TRUE</code>.
          </p>
        </div>

      </div>

    </div>

    <!-- ========================================================================= -->
    <!-- TAB 2: PIPELINE SIMULATOR (INTERACTIVE ENGINE WITH IDEMPOTENCY)           -->
    <!-- ========================================================================= -->
      
      <div class="pt-6 border-t border-slate-200">
        <h3 class="text-lg font-bold text-slate-900 mb-4 flex items-center space-x-2">
          <i class="fa-solid fa-code text-indigo-600"></i>
          <span>Production Oracle Extraction Queries & Lakehouse Tiering Specifications</span>
        </h3>
        <div id="tab-techspecs-inner" class="space-y-6">
      
      <!-- Tech Specs Header -->
      <div class="bg-white rounded-2xl p-6 border border-slate-200 shadow-sm">
        <div class="flex items-center space-x-2 text-blue-700 text-xs font-bold uppercase tracking-wide">
          <i class="fa-solid fa-terminal"></i>
          <span>Architecture Deep-Dive & SDE Standards</span>
        </div>
        <h2 class="text-2xl font-black text-slate-900 mt-1">Technical Implementation Specifications</h2>
        <p class="text-xs text-slate-500 mt-1">
          Exact DC extraction queries, Parquet vs. Direct Database architectural comparison, Dev Hyd topology, and stubbed PySpark production code.
        </p>
      </div>

      <!-- SECTION 1: DC EXTRACTION QUERIES -->
      <div class="bg-white rounded-2xl p-6 border border-slate-200 shadow-sm space-y-4">
        <div class="flex items-center justify-between">
          <h3 class="text-base font-extrabold text-slate-900 flex items-center space-x-2">
            <span class="w-6 h-6 rounded bg-blue-100 text-blue-700 flex items-center justify-center text-xs font-bold">1</span>
            <span>Data Center Extraction Queries (Oracle 19c Source DBs)</span>
          </h3>
          <span class="text-xs text-slate-400">Deployed at South (Hyd), West (Pune), East (Kolkata), North (Chandigarh)</span>
        </div>

        <div class="grid grid-cols-1 lg:grid-cols-2 gap-4">
          <!-- Hourly Incremental Query -->
          <div class="border border-slate-200 rounded-xl overflow-hidden shadow-xs">
            <div class="bg-slate-100 px-4 py-2 border-b border-slate-200 flex items-center justify-between text-xs">
              <span class="font-bold text-slate-800"><i class="fa-solid fa-bolt text-blue-600 mr-1.5"></i> Hourly Incremental Push Query</span>
              <span class="text-[10px] bg-blue-100 text-blue-800 px-2 py-0.5 rounded font-mono">Runs Hourly (65-min window)</span>
            </div>
            <pre class="bg-slate-900 text-slate-200 p-4 text-xs font-mono overflow-x-auto leading-relaxed">
<span class="text-slate-500">-- Pushes ~10k changed rows per hour into Dev Hyd Central Staging</span>
<span class="text-purple-400">SELECT</span> 
    ORDER_ID, 
    CA_NUMBER, 
    FTTH_NUMBER, 
    CUSTOMER_NAME, 
    CIRCLE_CODE, 
    SSA_CODE, 
    PLAN_ID, 
    PLAN_NAME, 
    ORDER_TYPE, 
    ORDER_STATUS, 
    FMS_FRANCHISEE_ID, 
    UNMS_PROV_STATUS, 
    CREATED_DATE, 
    LAST_MODIFIED_DATE,
    <span class="text-emerald-300">'HOURLY_DELTA'</span> <span class="text-purple-400">AS</span> INGESTION_SOURCE,
    <span class="text-purple-400">SYSDATE</span> <span class="text-purple-400">AS</span> PUSH_TIMESTAMP
<span class="text-purple-400">FROM</span> CRM_CFA_ORDERS_LOCAL
<span class="text-purple-400">WHERE</span> LAST_MODIFIED_DATE &gt;= <span class="text-purple-400">SYSDATE</span> - <span class="text-purple-400">INTERVAL</span> <span class="text-amber-300">'65'</span> <span class="text-purple-400">MINUTE</span>;
            </pre>
            <div class="p-3 bg-slate-50 text-[11px] text-slate-600 border-t border-slate-200">
              <strong>Key Safety Feature:</strong> The 65-minute window provides a 5-minute safety overlap to guarantee that network glitches never cause lost rows.
            </div>
          </div>

          <!-- Daily Full Dump Query -->
          <div class="border border-slate-200 rounded-xl overflow-hidden shadow-xs">
            <div class="bg-slate-100 px-4 py-2 border-b border-slate-200 flex items-center justify-between text-xs">
              <span class="font-bold text-slate-800"><i class="fa-solid fa-database text-indigo-600 mr-1.5"></i> Daily Full Dump Snapshot Query</span>
              <span class="text-[10px] bg-indigo-100 text-indigo-800 px-2 py-0.5 rounded font-mono">Runs Nightly at 02:00 AM</span>
            </div>
            <pre class="bg-slate-900 text-slate-200 p-4 text-xs font-mono overflow-x-auto leading-relaxed">
<span class="text-slate-500">-- Pulls 100% of 50 Lakh active customer rows for full reconciliation</span>
<span class="text-purple-400">SELECT</span> <span class="text-cyan-400">/*+ PARALLEL(8) FULL(o) */</span>
    o.ORDER_ID,
    o.CA_NUMBER,
    o.FTTH_NUMBER,
    o.CUSTOMER_NAME,
    o.CIRCLE_CODE,
    o.SSA_CODE,
    o.PLAN_ID,
    o.PLAN_NAME,
    o.ORDER_STATUS,
    o.FMS_FRANCHISEE_ID,
    o.UNMS_PROV_STATUS,
    o.CREATED_DATE,
    o.LAST_MODIFIED_DATE,
    <span class="text-emerald-300">'DAILY_FULL_DUMP'</span> <span class="text-purple-400">AS</span> INGESTION_SOURCE,
    <span class="text-purple-400">SYSDATE</span> <span class="text-purple-400">AS</span> RECON_TIMESTAMP
<span class="text-purple-400">FROM</span> CRM_CFA_ORDERS_MASTER_ALL o;
            </pre>
            <div class="p-3 bg-slate-50 text-[11px] text-slate-600 border-t border-slate-200">
              <strong>Healing Purpose:</strong> Catches backdated customer CAF entries, offline DBA modifications, and stalled orders that missed the hourly window.
            </div>
          </div>
        </div>
      </div>

      <!-- SECTION 2: STORAGE COMPARISON (WITH PARQUET VS WITHOUT PARQUET) -->
      <div class="bg-white rounded-2xl p-6 border border-slate-200 shadow-sm space-y-4">
        <div class="flex items-center justify-between">
          <h3 class="text-base font-extrabold text-slate-900 flex items-center space-x-2">
            <span class="w-6 h-6 rounded bg-blue-100 text-blue-700 flex items-center justify-center text-xs font-bold">2</span>
            <span>Storage Architectural Analysis: With Parquet vs. Without Parquet (Direct DB)</span>
          </h3>
          <span class="text-xs bg-emerald-100 text-emerald-800 font-bold px-2 py-0.5 rounded border border-emerald-200">Recommended: Snappy Parquet Bronze</span>
        </div>

        <div class="overflow-x-auto">
          <table class="w-full text-left text-xs border border-slate-200 rounded-xl overflow-hidden">
            <thead class="bg-slate-100 text-slate-800 font-bold border-b border-slate-200 text-[11px]">
              <tr>
                <th class="py-3 px-4">Evaluation Dimension</th>
                <th class="py-3 px-4 bg-blue-50/70 text-blue-900">Method A: With Parquet Format (Snappy Compressed)</th>
                <th class="py-3 px-4 bg-slate-50 text-slate-900">Method B: Without Parquet (Direct Database Tables)</th>
                <th class="py-3 px-4">BSNL CFA Verdict</th>
              </tr>
            </thead>
            <tbody class="divide-y divide-slate-200 text-slate-600">
              <tr>
                <td class="py-3 px-4 font-semibold text-slate-900">Storage Footprint (50L Rows)</td>
                <td class="py-3 px-4 bg-blue-50/30 text-blue-900 font-medium"><i class="fa-solid fa-check text-emerald-600 mr-1"></i> ~280 MB to ~350 MB (75% to 80% compression)</td>
                <td class="py-3 px-4">~1.5 GB to ~2.2 GB raw Oracle/PostgreSQL tablespace</td>
                <td class="py-3 px-4 text-emerald-700 font-bold">Parquet saves 80% SAN storage</td>
              </tr>
              <tr>
                <td class="py-3 px-4 font-semibold text-slate-900">Query & Scan Performance</td>
                <td class="py-3 px-4 bg-blue-50/30 text-blue-900 font-medium"><i class="fa-solid fa-check text-emerald-600 mr-1"></i> Columnar pruning reads only queried attributes; partition skips non-target dates</td>
                <td class="py-3 px-4">Full table scan or heavy index bloat on raw tables</td>
                <td class="py-3 px-4 text-emerald-700 font-bold">10x faster aggregation scans</td>
              </tr>
              <tr>
                <td class="py-3 px-4 font-semibold text-slate-900">Immutable Replayability</td>
                <td class="py-3 px-4 bg-blue-50/30 text-blue-900 font-medium"><i class="fa-solid fa-check text-emerald-600 mr-1"></i> 100% immutable file snapshots allow re-running Silver pipeline for any historical date</td>
                <td class="py-3 px-4">Requires complex Oracle Flashback or database backup restore</td>
                <td class="py-3 px-4 text-emerald-700 font-bold">Mandatory for TRAI/CAG audit</td>
              </tr>
              <tr>
                <td class="py-3 px-4 font-semibold text-slate-900">Database Connection Overhead</td>
                <td class="py-3 px-4 bg-blue-50/30 text-blue-900 font-medium"><i class="fa-solid fa-check text-emerald-600 mr-1"></i> Zero active connections during PySpark transformations; files read from local mount</td>
                <td class="py-3 px-4">High connection pool and lock contention on Oracle staging tables</td>
                <td class="py-3 px-4 text-emerald-700 font-bold">Zero lock risk for DC DBAs</td>
              </tr>
              <tr>
                <td class="py-3 px-4 font-semibold text-slate-900">Pure Python / Lightweight Fallback</td>
                <td class="py-3 px-4 bg-blue-50/30 text-blue-900 font-medium">Readable natively by Python via DuckDB / Polars with zero JVM required</td>
                <td class="py-3 px-4">Requires psycopg2 / cx_Oracle cursors with row-by-row memory buffers</td>
                <td class="py-3 px-4 text-blue-700 font-bold">DuckDB/Polars handles in 4 sec</td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>

      <!-- SECTION 3: READABLE STUBBED CODE -->
      <div class="bg-white rounded-2xl p-6 border border-slate-200 shadow-sm space-y-4">
        <div class="flex items-center justify-between">
          <h3 class="text-base font-extrabold text-slate-900 flex items-center space-x-2">
            <span class="w-6 h-6 rounded bg-blue-100 text-blue-700 flex items-center justify-center text-xs font-bold">3</span>
            <span>Stubbed Production Logic: Bronze Quality, Silver SCD-2 & PostgreSQL Serving</span>
          </h3>
          <span class="text-xs text-slate-400">Written in plain, human-readable Python & PySpark</span>
        </div>

        <div class="grid grid-cols-1 lg:grid-cols-2 gap-4">
          
          <!-- Code Block 1: Bronze Quality & Quarantine Split -->
          <div class="border border-slate-200 rounded-xl overflow-hidden shadow-xs">
            <div class="bg-slate-100 px-4 py-2 border-b border-slate-200 flex items-center justify-between text-xs">
              <span class="font-bold text-slate-800 font-mono">01_bronze_quality_filter.py</span>
              <span class="text-[10px] bg-slate-200 text-slate-700 px-2 py-0.5 rounded font-bold">PySpark Bronze</span>
            </div>
            <pre class="bg-slate-900 text-slate-200 p-4 text-xs font-mono overflow-x-auto leading-relaxed">
<span class="text-purple-400">from</span> pyspark.sql <span class="text-purple-400">import</span> functions <span class="text-purple-400">as</span> F

<span class="text-slate-500"># 1. Read landing data from Dev Hyd Staging</span>
df_raw = spark.read.table(<span class="text-amber-300">"dev_hyd.land_sz_orders"</span>)

<span class="text-slate-500"># 2. Rule 1: Business Key Validation</span>
cond_valid_key = (
    F.col(<span class="text-amber-300">"CA_NUMBER"</span>).isNotNull() & 
    (F.trim(F.col(<span class="text-amber-300">"CA_NUMBER"</span>)) != <span class="text-amber-300">""</span>) &
    F.col(<span class="text-amber-300">"FTTH_NUMBER"</span>).isNotNull()
)

<span class="text-slate-500"># 3. Rule 2: Plan Catalog Validation</span>
valid_plans = spark.read.table(<span class="text-amber-300">"dev_hyd.plan_catalog_master"</span>).select(<span class="text-amber-300">"PLAN_ID"</span>)
cond_valid_plan = F.col(<span class="text-amber-300">"PLAN_ID"</span>).isin([r.PLAN_ID <span class="text-purple-400">for</span> r <span class="text-purple-400">in</span> valid_plans.collect()])

<span class="text-slate-500"># 4. Split clean vs quarantine without killing job</span>
df_clean = df_raw.filter(cond_valid_key & cond_valid_plan)
df_quarantine = df_raw.filter(~(cond_valid_key & cond_valid_plan))     .withColumn(<span class="text-amber-300">"REJECT_REASON"</span>, 
        F.when(~cond_valid_key, <span class="text-amber-300">"NULL_OR_BLANK_BUSINESS_KEY"</span>)
         .otherwise(<span class="text-amber-300">"INVALID_OR_MISSING_PLAN_ID"</span>))

<span class="text-slate-500"># 5. Route to destinations</span>
df_clean.write.mode(<span class="text-amber-300">"append"</span>).parquet(<span class="text-amber-300">"lake/bronze/clean_orders"</span>)
df_quarantine.write.mode(<span class="text-amber-300">"append"</span>).parquet(<span class="text-amber-300">"lake/bronze/quarantine"</span>)
            </pre>
          </div>

          <!-- Code Block 2: Silver SCD Type 2 Merge -->
          <div class="border border-slate-200 rounded-xl overflow-hidden shadow-xs">
            <div class="bg-slate-100 px-4 py-2 border-b border-slate-200 flex items-center justify-between text-xs">
              <span class="font-bold text-slate-800 font-mono">02_silver_scd2_merge.py</span>
              <span class="text-[10px] bg-slate-200 text-slate-700 px-2 py-0.5 rounded font-bold">PySpark Silver</span>
            </div>
            <pre class="bg-slate-900 text-slate-200 p-4 text-xs font-mono overflow-x-auto leading-relaxed">
<span class="text-purple-400">from</span> delta.tables <span class="text-purple-400">import</span> DeltaTable

target_table = DeltaTable.forName(spark, <span class="text-amber-300">"silver.cfa_customer_360_history"</span>)
incoming_clean = spark.read.parquet(<span class="text-amber-300">"lake/bronze/clean_orders"</span>)

<span class="text-slate-500"># Detect Tariff Shifts or Address Movements</span>
<span class="text-slate-500"># If (CA_NUMBER, FTTH_NUMBER) exists and attributes changed:</span>
<span class="text-slate-500"># Step A: Close previous active record</span>
target_table.alias(<span class="text-amber-300">"target"</span>).merge(
    source=incoming_clean.alias(<span class="text-amber-300">"src"</span>),
    condition=<span class="text-amber-300">"target.CA_NUMBER = src.CA_NUMBER AND target.IS_CURRENT = true"</span>
).whenMatchedUpdate(
    condition=<span class="text-amber-300">"target.PLAN_ID != src.PLAN_ID"</span>,
    set={
        <span class="text-amber-300">"IS_CURRENT"</span>: <span class="text-amber-300">"false"</span>,
        <span class="text-amber-300">"EFFECTIVE_TO"</span>: <span class="text-amber-300">"src.LAST_MODIFIED_DATE"</span>
    }
).execute()

<span class="text-slate-500"># Step B: Insert new version as current</span>
incoming_clean.withColumn(<span class="text-amber-300">"IS_CURRENT"</span>, F.lit(<span class="text-purple-400">True</span>))               .withColumn(<span class="text-amber-300">"EFFECTIVE_FROM"</span>, F.col(<span class="text-amber-300">"LAST_MODIFIED_DATE"</span>))               .write.mode(<span class="text-amber-300">"append"</span>).saveAsTable(<span class="text-amber-300">"silver.cfa_customer_360_history"</span>)
            </pre>
          </div>

          <!-- Code Block 3: PostgreSQL ODS Serving Upsert -->
          <div class="border border-slate-200 rounded-xl overflow-hidden shadow-xs">
            <div class="bg-slate-100 px-4 py-2 border-b border-slate-200 flex items-center justify-between text-xs">
              <span class="font-bold text-slate-800 font-mono">03_postgres_ods_upsert.sql</span>
              <span class="text-[10px] bg-slate-200 text-slate-700 px-2 py-0.5 rounded font-bold">PostgreSQL 16 ODS</span>
            </div>
            <pre class="bg-slate-900 text-slate-200 p-4 text-xs font-mono overflow-x-auto leading-relaxed">
<span class="text-slate-500">-- High-speed atomic upsert into active Customer 360 table</span>
<span class="text-purple-400">INSERT INTO</span> cfa_customer_360_master (
    customer_key, ca_number, ftth_number, customer_name,
    category, circle_code, plan_id, plan_name,
    partner_id, unms_speed_mbps, sync_source, last_updated
) <span class="text-purple-400">VALUES</span> (
    <span class="text-amber-300">'SZ-KA-BLR-001'</span>, <span class="text-amber-300">'1029384756'</span>, <span class="text-amber-300">'080-22994411'</span>, <span class="text-amber-300">'Rajesh Kumar'</span>,
    <span class="text-amber-300">'Retail FTTH'</span>, <span class="text-amber-300">'KA'</span>, <span class="text-amber-300">'FIBRE_ULTRA_OTT'</span>, <span class="text-amber-300">'Fibre Ultra 300M'</span>,
    <span class="text-amber-300">'TIP-BLR-882'</span>, <span class="text-cyan-400">300</span>, <span class="text-amber-300">'Hourly Delta'</span>, <span class="text-purple-400">NOW</span>()
)
<span class="text-purple-400">ON CONFLICT</span> (customer_key) 
<span class="text-purple-400">DO UPDATE SET</span>
    plan_id = EXCLUDED.plan_id,
    plan_name = EXCLUDED.plan_name,
    unms_speed_mbps = EXCLUDED.unms_speed_mbps,
    sync_source = EXCLUDED.sync_source,
    last_updated = EXCLUDED.last_updated;
            </pre>
          </div>

          <!-- Code Block 4: Pure Python Lightweight Fallback -->
          <div class="border border-slate-200 rounded-xl overflow-hidden shadow-xs">
            <div class="bg-slate-100 px-4 py-2 border-b border-slate-200 flex items-center justify-between text-xs">
              <span class="font-bold text-slate-800 font-mono">04_pure_python_duckdb_fallback.py</span>
              <span class="text-[10px] bg-slate-200 text-slate-700 px-2 py-0.5 rounded font-bold">Python Polars / DuckDB</span>
            </div>
            <pre class="bg-slate-900 text-slate-200 p-4 text-xs font-mono overflow-x-auto leading-relaxed">
<span class="text-purple-400">import</span> polars <span class="text-purple-400">as</span> pl

<span class="text-slate-500"># For lightweight edge environments without a Spark cluster:</span>
df = pl.read_parquet(<span class="text-amber-300">"dev_hyd/landing/cfa_orders.parquet"</span>)

<span class="text-slate-500"># Vectorized Quality Gate in 3 lines:</span>
clean_df = df.filter(
    pl.col(<span class="text-amber-300">"CA_NUMBER"</span>).is_not_null() & 
    (pl.col(<span class="text-amber-300">"PLAN_ID"</span>).str.len_chars() &gt; <span class="text-cyan-400">0</span>)
)

<span class="text-slate-500"># Fast batch write direct to PostgreSQL:</span>
clean_df.write_database(
    table_name=<span class="text-amber-300">"cfa_customer_360_master"</span>,
    connection=<span class="text-amber-300">"postgresql://user:pass@10.20.30.40:5432/cfa_lake"</span>,
    if_table_exists=<span class="text-amber-300">"append"</span>
)
            </pre>
          </div>

        </div>
      </div>

    </div>

  </main>

  <!-- ========================================================================= -->
  <!-- MODAL: ARCHITECTURE MODULE TECHNICAL INSPECTOR                            -->
  <!-- ========================================================================= -->
      </div>
    </div>


  </main>

  <!-- ===================================================================
       MODAL: ADD / EDIT COLUMN MAPPING & CHECKS CONFIGURATION
  ==================================================================== -->
  <div id="mapping-modal" class="fixed inset-0 bg-slate-900/50 backdrop-blur-sm z-50 hidden flex items-center justify-center p-4">
    <div class="bg-white rounded-2xl max-w-xl w-full shadow-2xl border border-slate-200 overflow-hidden transform transition-all">
      <div class="px-6 py-4 bg-slate-50 border-b border-slate-100 flex items-center justify-between">
        <div class="flex items-center space-x-3">
          <div class="w-8 h-8 rounded-lg bg-blue-100 text-blue-700 flex items-center justify-center text-sm font-bold">
            <i class="fa-solid fa-table-columns"></i>
          </div>
          <h3 id="mapping-modal-title" class="font-bold text-slate-900 text-base">Add New Column Mapping</h3>
        </div>
        <button onclick="closeMappingModal()" class="text-slate-400 hover:text-slate-600 p-1.5 rounded-lg hover:bg-slate-100 transition-colors">
          <i class="fa-solid fa-xmark text-lg"></i>
        </button>
      </div>

      <form id="mapping-form" onsubmit="event.preventDefault(); submitMappingForm();" class="p-6 space-y-4 text-xs">
        <div class="grid grid-cols-2 gap-3">
          <div>
            <label class="block font-semibold text-slate-700 mb-1">Zone <span class="text-rose-500">*</span>:</label>
            <select id="map-zone" onchange="onZoneSelectChange(this.value)" class="w-full px-3 py-2 border border-slate-300 rounded-lg font-mono font-bold focus:outline-none focus:ring-2 focus:ring-blue-500 bg-white">
              <option value="NZ">North Zone (NZ - SAP CRM)</option>
              <option value="EZ">East Zone (EZ - Siebel CRM)</option>
            </select>
          </div>
          <div>
            <label class="block font-semibold text-slate-700 mb-1">Source Table <span class="text-rose-500">*</span>:</label>
            <select id="map-table" onchange="onSourceTableChange(this.value)" class="w-full px-3 py-2 border border-slate-300 rounded-lg font-mono focus:outline-none focus:ring-2 focus:ring-blue-500 bg-white" required>
              <optgroup label="Live Oracle Tables (from DB Connection)">
                <option value="S_ASSET" data-schema="DATALAKE_LOADING">DATALAKE_LOADING.S_ASSET (338 cols)</option>
                <option value="S_ORDER" data-schema="DATALAKE_LOADING">DATALAKE_LOADING.S_ORDER</option>
                <option value="S_CONTACT" data-schema="DATALAKE_LOADING">DATALAKE_LOADING.S_CONTACT</option>
                <option value="S_ORG_EXT" data-schema="DATALAKE_LOADING">DATALAKE_LOADING.S_ORG_EXT</option>
                <option value="S_SRV_REQ" data-schema="DATALAKE_LOADING">DATALAKE_LOADING.S_SRV_REQ</option>
                <option value="S_ADDR_PER" data-schema="DATALAKE_LOADING">DATALAKE_LOADING.S_ADDR_PER</option>
                <option value="TOAD_PLAN_TABLE" data-schema="CFADMIN">CFADMIN.TOAD_PLAN_TABLE</option>
              </optgroup>
              <optgroup label="East Zone Siebel CRM Tables (13 Tables)">
                <option value="S_ORDER">S_ORDER (Orders)</option>
                <option value="S_ORDER_X">S_ORDER_X (Order Extension)</option>
                <option value="S_ORDER_TYPE">S_ORDER_TYPE (Order Types)</option>
                <option value="S_ORG_EXT">S_ORG_EXT (Accounts)</option>
                <option value="S_ORG_EXT_X">S_ORG_EXT_X (Account Extension)</option>
                <option value="S_ORG_EXT_XM">S_ORG_EXT_XM (Account Multi-Value)</option>
                <option value="S_ASSET">S_ASSET (Installed Services)</option>
                <option value="S_ASSET_X">S_ASSET_X (Asset Extension)</option>
                <option value="S_ASSET_XA">S_ASSET_XA (Asset Attributes)</option>
                <option value="S_ASSET_XM">S_ASSET_XM (Asset Multi-Value)</option>
                <option value="S_ADDR_PER">S_ADDR_PER (Addresses)</option>
                <option value="S_CONTACT">S_CONTACT (Contacts / Persons)</option>
                <option value="S_SRV_REQ">S_SRV_REQ (Complaints / SR)</option>
              </optgroup>
              <optgroup label="North Zone SAP Master &amp; Transaction Tables (12 Tables)">
                <option value="BUT000">BUT000 (BP Master)</option>
                <option value="IBIB">IBIB (Installed Base / Services)</option>
                <option value="CRMD_ORDERADM_H">CRMD_ORDERADM_H (Order Header)</option>
                <option value="CRMD_ORDERADM_I">CRMD_ORDERADM_I (Order Item)</option>
                <option value="CRMD_ORDER_INDEX">CRMD_ORDER_INDEX (Order Index)</option>
                <option value="CRM_JEST">CRM_JEST (Status Object)</option>
                <option value="TJ30T">TJ30T (Status Texts)</option>
                <option value="ADRC">ADRC (Addresses)</option>
                <option value="ADR2">ADR2 (Telephone numbers)</option>
                <option value="ADR6">ADR6 (E-mail addresses)</option>
                <option value="BUT021_FS">BUT021_FS (Address usage)</option>
                <option value="BUT050">BUT050 (BP Relationships)</option>
              </optgroup>
            </select>
          </div>
        </div>

        <div>
          <label class="block font-semibold text-slate-700 mb-1">Source Column Name <span class="text-rose-500">*</span>:</label>
          <input type="text" id="map-col" list="oracle-cols-datalist" oninput="this.value = this.value.toUpperCase()" placeholder="e.g. ROW_ID, ASSET_NUM, or type custom" class="w-full px-3 py-2 border border-slate-300 rounded-lg font-mono focus:outline-none focus:ring-2 focus:ring-blue-500" required>
          <datalist id="oracle-cols-datalist"></datalist>
          <p id="col-oracle-hint" class="text-[10px] text-slate-400 mt-1">Live columns pulled directly from Oracle DB connection appear as suggestions</p>
        </div>

        <div class="grid grid-cols-2 gap-3">
          <div>
            <label class="block font-semibold text-slate-700 mb-1">Target CFA360 Entity <span class="text-rose-500">*</span>:</label>
            <select id="map-entity" class="w-full px-3 py-2 border border-slate-300 rounded-lg font-mono focus:outline-none focus:ring-2 focus:ring-blue-500 bg-white" required>
              <option value="Customer_master_data">Customer_master_data</option>
              <option value="CRM_activity_table">CRM_activity_table</option>
              <option value="Complaint_details">Complaint_details</option>
              <option value="Order_Header">Order_Header</option>
              <option value="Order_Item">Order_Item</option>
              <option value="Address">Address</option>
              <option value="Contact_Details">Contact_Details</option>
            </select>
          </div>
          <div>
            <label class="block font-semibold text-slate-700 mb-1">Target Field Name <span class="text-rose-500">*</span>:</label>
            <input type="text" id="map-field" oninput="this.value = this.value.toUpperCase()" placeholder="e.g. CUSTOMER_ATTRIBUTE" class="w-full px-3 py-2 border border-slate-300 rounded-lg font-mono focus:outline-none focus:ring-2 focus:ring-blue-500" required>
          </div>
        </div>

        <div class="grid grid-cols-3 gap-3">
          <div>
            <label class="block font-semibold text-slate-700 mb-1">Mapping Type:</label>
            <select id="map-type" class="w-full px-3 py-2 border border-slate-300 rounded-lg font-mono focus:outline-none focus:ring-2 focus:ring-blue-500 bg-white">
              <option value="DIRECT">DIRECT</option>
              <option value="DERIVED">DERIVED</option>
              <option value="PIPELINE">PIPELINE</option>
            </select>
          </div>
          <div>
            <label class="block font-semibold text-slate-700 mb-1">Transformation Rule:</label>
            <input type="text" id="map-rule" value="DIRECT" placeholder="e.g. DIRECT or TRIM(x)" class="w-full px-3 py-2 border border-slate-300 rounded-lg font-mono focus:outline-none focus:ring-2 focus:ring-blue-500">
          </div>
          <div>
            <label class="block font-semibold text-slate-700 mb-1">Confidence:</label>
            <select id="map-conf" class="w-full px-3 py-2 border border-slate-300 rounded-lg font-mono focus:outline-none focus:ring-2 focus:ring-blue-500 bg-white">
              <option value="HIGH">HIGH</option>
              <option value="MEDIUM">MEDIUM</option>
              <option value="LOW">LOW</option>
            </select>
          </div>
        </div>

        <!-- Sensitive Column Safeguard & Check -->
        <div class="bg-amber-50 border border-amber-200 rounded-lg p-3 space-y-2">
          <label class="flex items-center space-x-2 cursor-pointer">
            <input type="checkbox" id="map-sensitive" onchange="toggleSensitiveReasonInput()" class="rounded text-amber-600 focus:ring-amber-500 h-4 w-4">
            <span class="font-bold text-amber-900 text-xs">Flag as Protected Sensitive Column (Enforce Automatic [REDACTED_SENSITIVE] Guard)</span>
          </label>
          <div id="map-sensitive-reason-container" class="hidden">
            <label class="block text-[11px] font-semibold text-amber-800 mb-1">Sensitive Classification Reason:</label>
            <input type="text" id="map-reason" placeholder="e.g. Holds PAN, GSTIN, Password, or PII" class="w-full px-3 py-1.5 border border-amber-300 rounded bg-white text-xs font-sans focus:outline-none focus:ring-2 focus:ring-amber-500">
          </div>
        </div>

        <div>
          <label class="block font-semibold text-slate-700 mb-1">Notes / Documentation (Optional):</label>
          <input type="text" id="map-notes" placeholder="e.g. Business partner custom tax code field" class="w-full px-3 py-2 border border-slate-300 rounded-lg font-sans focus:outline-none focus:ring-2 focus:ring-blue-500">
        </div>

        <div id="map-modal-error" class="hidden bg-rose-50 text-rose-800 border border-rose-200 rounded p-2 text-xs font-medium"></div>

        <div class="pt-3 border-t border-slate-100 flex items-center justify-end space-x-3">
          <button type="button" onclick="closeMappingModal()" class="px-4 py-2 bg-slate-100 hover:bg-slate-200 text-slate-700 font-semibold rounded-lg text-xs transition-colors">
            Cancel
          </button>
          <button type="submit" id="btn-save-mapping" class="px-4 py-2 bg-blue-600 hover:bg-blue-700 text-white font-bold rounded-lg text-xs flex items-center space-x-1.5 shadow transition-colors">
            <i class="fa-solid fa-floppy-disk"></i>
            <span>Save Mapping Configuration</span>
          </button>
        </div>
      </form>
    </div>
  </div>

  <!-- ===================================================================
       MODAL: TAG SOURCE SYSTEM & METADATA (SINGLE ORACLE SCHEMA SUPPORT)
  ==================================================================== -->
  <div id="tag-source-modal" class="fixed inset-0 bg-slate-900/50 backdrop-blur-sm z-50 hidden flex items-center justify-center p-4">
    <div class="bg-white rounded-2xl max-w-lg w-full shadow-2xl border border-slate-200 overflow-hidden transform transition-all">
      <div class="px-6 py-4 bg-slate-50 border-b border-slate-100 flex items-center justify-between">
        <div class="flex items-center space-x-3">
          <div class="w-8 h-8 rounded-lg bg-indigo-100 text-indigo-700 flex items-center justify-center text-sm font-bold">
            <i class="fa-solid fa-tags"></i>
          </div>
          <div>
            <h3 id="tag-source-modal-title" class="font-bold text-slate-900 text-base">Tag Source System &amp; Lineage Metadata</h3>
            <p class="text-[11px] text-slate-500">Assign source system &amp; business lineage to Oracle table</p>
          </div>
        </div>
        <button onclick="closeTagSourceModal()" class="text-slate-400 hover:text-slate-600 p-1.5 rounded-lg hover:bg-slate-100 transition-colors">
          <i class="fa-solid fa-xmark text-lg"></i>
        </button>
      </div>

      <form id="tag-source-form" onsubmit="event.preventDefault(); submitTagSourceForm();" class="p-6 space-y-4 text-xs">
        <div class="grid grid-cols-2 gap-3">
          <div>
            <label class="block font-semibold text-slate-700 mb-1">Target Table:</label>
            <input type="text" id="tag-source-table" class="w-full px-3 py-2 border border-slate-300 rounded-lg bg-slate-100 text-slate-800 font-mono font-bold" readonly>
          </div>
          <div>
            <label class="block font-semibold text-slate-700 mb-1">Zone / Schema:</label>
            <input type="text" id="tag-source-zone" class="w-full px-3 py-2 border border-slate-300 rounded-lg bg-slate-100 text-slate-600 font-mono" readonly>
          </div>
        </div>

        <div>
          <label class="block font-semibold text-slate-700 mb-1">Source System Name <span class="text-rose-500">*</span>:</label>
          <div class="space-y-1.5">
            <input type="text" id="tag-source-system" list="source-systems-datalist" placeholder="Select or type: Teerva, portal, FMS, SZ_zone, NZ_zone, EZ_zone..." class="w-full px-3 py-2 border border-slate-300 rounded-lg font-mono focus:outline-none focus:ring-2 focus:ring-indigo-500" required>
            <datalist id="source-systems-datalist">
              <option value="Teerva">Teerva (Asset / Services)</option>
              <option value="portal">portal (Selfcare / Web)</option>
              <option value="FMS">FMS (Fault Management System)</option>
              <option value="SZ_zone">SZ_zone (South Zone Clarity)</option>
              <option value="NZ_zone">NZ_zone (North Zone SAP CRM)</option>
              <option value="EZ_zone">EZ_zone (East Zone)</option>
              <option value="WZ_zone">WZ_zone (West Zone)</option>
            </datalist>
            <div class="flex flex-wrap items-center gap-1.5 mt-1.5">
              <span class="text-[10px] text-slate-400">Quick tags:</span>
              <button type="button" onclick="setQuickSourceTag('NZ_zone')" class="px-2 py-0.5 bg-indigo-50 hover:bg-indigo-100 text-indigo-700 rounded border border-indigo-200 text-[10px] font-mono font-semibold">NZ_zone</button>
              <button type="button" onclick="setQuickSourceTag('EZ_zone')" class="px-2 py-0.5 bg-purple-50 hover:bg-purple-100 text-purple-700 rounded border border-purple-200 text-[10px] font-mono font-semibold">EZ_zone</button>
              <button type="button" onclick="setQuickSourceTag('Teerva')" class="px-2 py-0.5 bg-emerald-50 hover:bg-emerald-100 text-emerald-700 rounded border border-emerald-200 text-[10px] font-mono font-semibold">Teerva</button>
              <button type="button" onclick="setQuickSourceTag('portal')" class="px-2 py-0.5 bg-blue-50 hover:bg-blue-100 text-blue-700 rounded border border-blue-200 text-[10px] font-mono font-semibold">portal</button>
              <button type="button" onclick="setQuickSourceTag('FMS')" class="px-2 py-0.5 bg-slate-50 hover:bg-slate-100 text-slate-700 rounded border border-slate-200 text-[10px] font-mono font-semibold">FMS</button>
            </div>
          </div>
          <p class="text-[10px] text-slate-500 mt-1">Every row copied into PostgreSQL Bronze will carry this tag in the <code class="font-mono bg-slate-100 px-1 py-0.5 rounded text-indigo-700 font-bold">_source</code> column.</p>
        </div>

        <div>
          <label class="block font-semibold text-slate-700 mb-1">Source Metadata (JSON):</label>
          <textarea id="tag-source-metadata" rows="3" placeholder='{"owner": "CRM Operations", "domain": "Customer Master", "environment": "Production", "sla_hours": 24}' class="w-full px-3 py-2 border border-slate-300 rounded-lg font-mono text-[11px] focus:outline-none focus:ring-2 focus:ring-indigo-500"></textarea>
          <p class="text-[10px] text-slate-400 mt-0.5">Persisted in <code class="font-mono">bronze_ctl.table_config.source_metadata</code></p>
        </div>

        <div class="pt-3 border-t border-slate-100 flex items-center justify-end space-x-3">
          <button type="button" onclick="closeTagSourceModal()" class="px-4 py-2 bg-slate-100 hover:bg-slate-200 text-slate-700 font-semibold rounded-lg text-xs transition-colors">Cancel</button>
          <button type="submit" id="btn-save-tag-source" class="px-4 py-2 bg-indigo-600 hover:bg-indigo-700 text-white font-bold rounded-lg text-xs flex items-center space-x-1.5 shadow transition-colors">
            <i class="fa-solid fa-floppy-disk"></i>
            <span>Save Source Tag</span>
          </button>
        </div>
      </form>
    </div>
  </div>

  <!-- ===================================================================
       MODAL: MULTI-MODE INGESTION (PARQUET, CSV, JDBC)
  ==================================================================== -->
  <div id="ingest-modal" class="fixed inset-0 bg-slate-900/50 backdrop-blur-sm z-50 hidden flex items-center justify-center p-4">
    <div class="bg-white rounded-2xl max-w-xl w-full shadow-2xl border border-slate-200 overflow-hidden transform transition-all">
      <div class="px-6 py-4 bg-slate-50 border-b border-slate-100 flex items-center justify-between">
        <div class="flex items-center space-x-3">
          <div class="w-8 h-8 rounded-lg bg-emerald-100 text-emerald-700 flex items-center justify-center text-sm font-bold">
            <i class="fa-solid fa-file-import"></i>
          </div>
          <div>
            <h3 class="font-bold text-slate-900 text-base">Multi-Mode Data Ingestion Engine</h3>
            <p class="text-[11px] text-slate-500">Ingest Parquet (.parquet), CSV (.csv/.tsv), or generic JDBC SQL source</p>
          </div>
        </div>
        <button onclick="closeIngestModal()" class="text-slate-400 hover:text-slate-600 p-1.5 rounded-lg hover:bg-slate-100 transition-colors">
          <i class="fa-solid fa-xmark text-lg"></i>
        </button>
      </div>

      <form id="ingest-form" onsubmit="event.preventDefault(); submitIngestForm();" class="p-6 space-y-4 text-xs max-h-[85vh] overflow-y-auto">
        <!-- Ingestion Mode Selector Cards -->
        <div>
          <label class="block font-semibold text-slate-700 mb-1.5">Select Ingestion Source Type <span class="text-rose-500">*</span>:</label>
          <div class="grid grid-cols-3 gap-2">
            <label class="cursor-pointer border-2 rounded-xl p-3 flex flex-col items-center space-y-1 hover:bg-slate-50 transition-all text-center has-[:checked]:border-emerald-600 has-[:checked]:bg-emerald-50/50">
              <input type="radio" name="ingest_type_radio" value="parquet" checked onchange="onIngestTypeChange('parquet')" class="sr-only">
              <i class="fa-solid fa-file-lines text-emerald-600 text-lg"></i>
              <span class="font-bold text-slate-900">Parquet</span>
              <span class="text-[10px] text-slate-500 font-mono">.parquet / glob</span>
            </label>
            <label class="cursor-pointer border-2 rounded-xl p-3 flex flex-col items-center space-y-1 hover:bg-slate-50 transition-all text-center has-[:checked]:border-sky-600 has-[:checked]:bg-sky-50/50">
              <input type="radio" name="ingest_type_radio" value="csv" onchange="onIngestTypeChange('csv')" class="sr-only">
              <i class="fa-solid fa-file-csv text-sky-600 text-lg"></i>
              <span class="font-bold text-slate-900">CSV / TSV</span>
              <span class="text-[10px] text-slate-500 font-mono">delimited text</span>
            </label>
            <label class="cursor-pointer border-2 rounded-xl p-3 flex flex-col items-center space-y-1 hover:bg-slate-50 transition-all text-center has-[:checked]:border-purple-600 has-[:checked]:bg-purple-50/50">
              <input type="radio" name="ingest_type_radio" value="jdbc" onchange="onIngestTypeChange('jdbc')" class="sr-only">
              <i class="fa-solid fa-network-wired text-purple-600 text-lg"></i>
              <span class="font-bold text-slate-900">JDBC / SQL</span>
              <span class="text-[10px] text-slate-500 font-mono">Postgres, MySQL, etc.</span>
            </label>
          </div>
        </div>

        <!-- Dynamic Source Location Input -->
        <div id="ingest-file-group">
          <label id="ingest-file-label" class="block font-semibold text-slate-700 mb-1">File Path or Glob Pattern <span class="text-rose-500">*</span>:</label>
          <input type="text" id="ingest-file-path" placeholder="e.g. scratch/assets.parquet or /data/lake/*.parquet" class="w-full px-3 py-2 border border-slate-300 rounded-lg font-mono focus:outline-none focus:ring-2 focus:ring-emerald-500 text-xs">
          <div class="flex items-center space-x-2 mt-1">
            <span class="text-[10px] text-slate-400">Quick samples:</span>
            <button type="button" onclick="setQuickIngestFile('scratch/assets.parquet', 'ASSET_PARQUET', 'Teerva', 'parquet')" class="px-2 py-0.5 bg-emerald-50 hover:bg-emerald-100 text-emerald-700 rounded border border-emerald-200 text-[10px] font-mono">scratch/assets.parquet</button>
            <button type="button" onclick="setQuickIngestFile('scratch/subscribers.csv', 'SUB_CSV', 'portal', 'csv')" class="px-2 py-0.5 bg-sky-50 hover:bg-sky-100 text-sky-700 rounded border border-sky-200 text-[10px] font-mono">scratch/subscribers.csv</button>
          </div>
        </div>

        <div id="ingest-jdbc-group" class="hidden space-y-2">
          <div>
            <label class="block font-semibold text-slate-700 mb-1">JDBC Connection URL <span class="text-rose-500">*</span>:</label>
            <input type="text" id="ingest-jdbc-url" placeholder="jdbc:postgresql://user:pass@host:5432/db or jdbc:sqlite:path" class="w-full px-3 py-2 border border-slate-300 rounded-lg font-mono focus:outline-none focus:ring-2 focus:ring-purple-500 text-xs">
            <div class="flex items-center space-x-2 mt-1">
              <span class="text-[10px] text-slate-400">Sample:</span>
              <button type="button" onclick="setQuickIngestJDBC('jdbc:sqlite:scratch/test_billing.sqlite', 'BILLING_INVOICES', 'FMS')" class="px-2 py-0.5 bg-purple-50 hover:bg-purple-100 text-purple-700 rounded border border-purple-200 text-[10px] font-mono">scratch/test_billing.sqlite</button>
            </div>
          </div>
          <div class="grid grid-cols-2 gap-3">
            <div>
              <label class="block font-semibold text-slate-700 mb-1">Source Schema (optional):</label>
              <input type="text" id="ingest-source-schema" placeholder="PUBLIC" class="w-full px-3 py-2 border border-slate-300 rounded-lg font-mono focus:outline-none focus:ring-2 focus:ring-purple-500 text-xs">
            </div>
          </div>
        </div>

        <!-- Delimiter for CSV -->
        <div id="ingest-delimiter-group" class="hidden">
          <label class="block font-semibold text-slate-700 mb-1">CSV Delimiter:</label>
          <div class="flex items-center space-x-2">
            <input type="text" id="ingest-csv-delimiter" value="," class="w-24 px-3 py-1.5 border border-slate-300 rounded-lg font-mono text-center focus:outline-none focus:ring-2 focus:ring-sky-500 text-xs">
            <span class="text-[11px] text-slate-500">Standard: <code class="font-mono">,</code> (comma), <code class="font-mono">\t</code> (tab), <code class="font-mono">|</code> (pipe), <code class="font-mono">;</code> (semicolon)</span>
          </div>
        </div>

        <!-- Target Bronze Table, Zone, Source Tag -->
        <div class="grid grid-cols-1 sm:grid-cols-2 gap-3">
          <div>
            <label class="block font-semibold text-slate-700 mb-1">Target Bronze Table <span class="text-rose-500">*</span>:</label>
            <input type="text" id="ingest-target-table" placeholder="e.g. ASSET_PARQUET" class="w-full px-3 py-2 border border-slate-300 rounded-lg font-mono font-bold focus:outline-none focus:ring-2 focus:ring-emerald-500 text-xs" required>
            <span class="text-[10px] text-slate-400">PostgreSQL table: bronze_[zone].[table]</span>
          </div>
          <div>
            <label class="block font-semibold text-slate-700 mb-1">Target Zone:</label>
            <select id="ingest-zone" class="w-full px-3 py-2 border border-slate-300 rounded-lg font-mono focus:outline-none focus:ring-2 focus:ring-emerald-500 text-xs bg-white">
              <option value="NZ" selected>NZ (North Zone SAP CRM)</option>
              <option value="EZ">EZ (East Zone Siebel CRM)</option>
            </select>
          </div>
        </div>

        <!-- Source System Tag -->
        <div>
          <label class="block font-semibold text-slate-700 mb-1">Source System Tag <span class="text-rose-500">*</span>:</label>
          <input type="text" id="ingest-source-tag" list="source-systems-datalist" placeholder="e.g. Teerva, portal, FMS, NZ_zone, EZ_zone..." class="w-full px-3 py-2 border border-slate-300 rounded-lg font-mono focus:outline-none focus:ring-2 focus:ring-emerald-500 text-xs" required>
          <div class="flex flex-wrap items-center gap-1.5 mt-1.5">
            <span class="text-[10px] text-slate-400">Quick tags:</span>
            <button type="button" onclick="setQuickIngestTag('NZ_zone')" class="px-2 py-0.5 bg-indigo-50 hover:bg-indigo-100 text-indigo-700 rounded border border-indigo-200 text-[10px] font-mono font-semibold">NZ_zone</button>
            <button type="button" onclick="setQuickIngestTag('EZ_zone')" class="px-2 py-0.5 bg-purple-50 hover:bg-purple-100 text-purple-700 rounded border border-purple-200 text-[10px] font-mono font-semibold">EZ_zone</button>
            <button type="button" onclick="setQuickIngestTag('Teerva')" class="px-2 py-0.5 bg-emerald-50 hover:bg-emerald-100 text-emerald-700 rounded border border-emerald-200 text-[10px] font-mono font-semibold">Teerva</button>
            <button type="button" onclick="setQuickIngestTag('portal')" class="px-2 py-0.5 bg-blue-50 hover:bg-blue-100 text-blue-700 rounded border border-blue-200 text-[10px] font-mono font-semibold">portal</button>
            <button type="button" onclick="setQuickIngestTag('FMS')" class="px-2 py-0.5 bg-slate-50 hover:bg-slate-100 text-slate-700 rounded border border-slate-200 text-[10px] font-mono font-semibold">FMS</button>
          </div>
        </div>

        <!-- Ingestion Mode & Primary Key -->
        <div class="grid grid-cols-1 sm:grid-cols-2 gap-3">
          <div>
            <label class="block font-semibold text-slate-700 mb-1">Ingestion Mode:</label>
            <select id="ingest-mode" class="w-full px-3 py-2 border border-slate-300 rounded-lg font-mono focus:outline-none focus:ring-2 focus:ring-emerald-500 text-xs bg-white">
              <option value="merge" selected>MERGE (UPSERT by Primary Key)</option>
              <option value="append">APPEND (Append-only / Audit)</option>
              <option value="full_refresh">FULL REFRESH (Truncate &amp; Load)</option>
            </select>
          </div>
          <div>
            <label class="block font-semibold text-slate-700 mb-1">Primary Key Columns:</label>
            <input type="text" id="ingest-pk-cols" placeholder="e.g. ASSET_ID (auto-detected if blank)" class="w-full px-3 py-2 border border-slate-300 rounded-lg font-mono focus:outline-none focus:ring-2 focus:ring-emerald-500 text-xs">
          </div>
        </div>

        <!-- Status / Result Alert Container -->
        <div id="ingest-status-alert" class="hidden p-3 rounded-lg text-xs font-mono"></div>

        <div class="pt-3 border-t border-slate-100 flex items-center justify-end space-x-3">
          <button type="button" onclick="closeIngestModal()" class="px-4 py-2 bg-slate-100 hover:bg-slate-200 text-slate-700 font-semibold rounded-lg text-xs transition-colors">Cancel</button>
          <button type="submit" id="btn-run-ingest" class="px-5 py-2.5 bg-emerald-600 hover:bg-emerald-700 text-white font-bold rounded-lg text-xs flex items-center space-x-1.5 shadow transition-colors">
            <i class="fa-solid fa-play"></i>
            <span>Run Multi-Mode Ingest</span>
          </button>
        </div>
      </form>
    </div>
  </div>

  <!-- Real-Time Row Record Dossier Drawer -->
  <div id="dossier-drawer" class="fixed inset-y-0 right-0 max-w-lg w-full bg-white shadow-2xl border-l border-slate-200 z-50 transform translate-x-full transition-transform duration-300 ease-in-out flex flex-col">
    <div class="p-5 border-b border-slate-100 bg-slate-50 flex items-center justify-between">
      <div class="flex items-center space-x-2.5">
        <div class="w-8 h-8 rounded-lg bg-emerald-600 text-white flex items-center justify-center font-bold text-xs">
          <i class="fa-solid fa-file-lines"></i>
        </div>
        <div>
          <h3 class="text-sm font-bold text-slate-900">Live Bronze Record Dossier</h3>
          <p id="dossier-table-name" class="text-[11px] text-slate-500 font-mono">bronze_nz.but000</p>
        </div>
      </div>
      <button onclick="closeDossier()" class="text-slate-400 hover:text-slate-600 p-1 rounded-lg">
        <i class="fa-solid fa-xmark text-lg"></i>
      </button>
    </div>

    <div class="p-5 overflow-y-auto flex-1 space-y-5 text-xs">
      <div>
        <h4 class="font-bold text-slate-800 uppercase tracking-wider text-[10px] text-slate-400 mb-2">Ingestion Lineage Metadata</h4>
        <div id="dossier-lineage" class="bg-slate-50 border border-slate-200 rounded-lg p-3 font-mono text-[11px] space-y-1">
          <!-- Populated dynamically -->
        </div>
      </div>

      <div>
        <h4 class="font-bold text-slate-800 uppercase tracking-wider text-[10px] text-slate-400 mb-2">Column Attributes</h4>
        <div class="border border-slate-200 rounded-lg overflow-hidden">
          <table class="w-full text-left border-collapse text-[11px]">
            <tbody id="dossier-attributes-body" class="divide-y divide-slate-100 font-mono">
              <!-- Populated dynamically -->
            </tbody>
          </table>
        </div>
      </div>
    </div>

    <div class="p-4 bg-slate-50 border-t border-slate-200 flex justify-end">
      <button onclick="closeDossier()" class="px-4 py-2 bg-slate-800 text-white text-xs font-semibold rounded-lg hover:bg-slate-700">
        Done
      </button>
    </div>
  </div>

  <!-- Architectural & Payload Inspector Modal -->
  <div id="arch-modal" class="fixed inset-0 bg-slate-900/50 backdrop-blur-sm z-50 hidden flex items-center justify-center p-4">
    <div class="bg-white rounded-2xl max-w-2xl w-full shadow-2xl border border-slate-200 overflow-hidden transform transition-all">
      <div class="px-6 py-4 bg-slate-50 border-b border-slate-100 flex items-center justify-between">
        <div class="flex items-center space-x-3">
          <div id="modal-icon-container" class="w-8 h-8 rounded-lg bg-blue-100 text-blue-700 flex items-center justify-center text-sm font-bold">
            <i class="fa-solid fa-circle-info"></i>
          </div>
          <h3 id="modal-title" class="font-bold text-slate-900 text-base">Module Inspection</h3>
        </div>
        <button onclick="closeArchModal()" class="text-slate-400 hover:text-slate-600 p-1.5 rounded-lg hover:bg-slate-100 transition-colors">
          <i class="fa-solid fa-xmark text-lg"></i>
        </button>
      </div>
      <div id="modal-content" class="p-6 text-sm text-slate-600 space-y-4 max-h-[75vh] overflow-y-auto">
        <!-- Content injected dynamically -->
      </div>
      <div class="px-6 py-3.5 bg-slate-50 border-t border-slate-100 flex justify-end">
        <button onclick="closeArchModal()" class="px-4 py-2 bg-slate-200 hover:bg-slate-300 text-slate-700 font-semibold rounded-lg text-xs transition-colors">
          Close
        </button>
      </div>
    </div>
  </div>

  <!-- ===================================================================
       CLIENT-SIDE CONTROLLER & REAL-TIME CONNECTOR
  ==================================================================== -->
  <script>
    const modalDict = {
      sources_structured: {
        title: "Structured Data Sources (4 Zonal DCs)",
        icon: "fa-solid fa-table",
        html: `<p><strong>Source Systems:</strong> Clarity CRM, Billing (Sanchar Soft), UNMS OSS, FMS TIP Portal, PMS, IFTV & OTT platforms.</p>
               <p><strong>Zonal Data Centers:</strong></p>
               <ul class="list-disc list-inside space-y-1">
                 <li><strong>South DC:</strong> Hyderabad (Hosts AP, Telangana, Karnataka, Tamil Nadu, Kerala)</li>
                 <li><strong>West DC:</strong> Pune (Maharashtra, Gujarat, MP, Chhattisgarh)</li>
                 <li><strong>East DC:</strong> Kolkata (West Bengal, Bihar, Odisha, Assam, NE)</li>
                 <li><strong>North DC:</strong> Chandigarh (Punjab, Haryana, UP, Rajasthan, J&K)</li>
               </ul>
               <p class="mt-2 text-slate-500">Full table schemas (50-70 columns) are replicated so future analytical needs never require DC script modifications.</p>`
      },
      sources_semi: {
        title: "Semi-Structured Feeds",
        icon: "fa-solid fa-code",
        html: `<p>Includes real-time webhook payloads from Payment Gateways (SBI, BillDesk), partner commission calculation receipts from FMS, and diagnostic JSONs from ONT/OLT diagnostic APIs.</p>`
      },
      sources_unstructured: {
        title: "Unstructured Metadata",
        icon: "fa-solid fa-file-lines",
        html: `<p>Scanned Customer Application Forms (CAF PDFs), customer care call center transcripts, and OLT system logs used for network telemetry and satisfaction scoring.</p>`
      },
      ingestion_batch: {
        title: "Dual Ingestion: Hourly Incremental + Nightly Dump",
        icon: "fa-solid fa-clock",
        html: `<p><strong>1. Hourly Incremental Push:</strong> Pulls records where <code>LAST_MODIFIED_DATE &gt;= SYSDATE - 65 MINUTE</code>. Moves ~10k customer changes per hour with low latency.</p>
               <p><strong>2. Nightly Full Dump (02:00 AM):</strong> Full <code>SELECT *</code> snapshot of all 50 Lakh records. Automatically heals orders delayed by inventory holds, offline DBA edits, or network hiccups.</p>`
      },
      ingestion_stream: {
        title: "Real-Time Event Streaming",
        icon: "fa-solid fa-bolt",
        html: `<p>Kafka CDC stream triggered on instant payment confirmation or immediate customer line disconnects to reflect sub-second state changes.</p>`
      },
      storage_raw: {
        title: "Dev Hyd Central Staging (Raw / Landing)",
        icon: "fa-solid fa-plane-arrival",
        html: `<p><strong>Location:</strong> South Data Center, Hyderabad (Dev Hyd).</p>
               <p><strong>Staging Tables:</strong> <code>LAND_SZ_ORDERS</code>, <code>LAND_WZ_ORDERS</code>, <code>LAND_EZ_ORDERS</code>, <code>LAND_NZ_ORDERS</code>.</p>
               <p>Incoming data is stored in Snappy-compressed Parquet files partitioned by <code>ingest_date=YYYY-MM-DD/zone=SZ</code>, yielding 80% compression.</p>`
      },
      storage_transform: {
        title: "PySpark Bronze & Silver Transform Engine",
        icon: "fa-solid fa-gears",
        html: `<p><strong>Bronze Guardrails:</strong> Evaluates 6 data quality checks. Clean records move forward; malformed rows split into Quarantine without crashing the pipeline.</p>
               <p><strong>Silver SCD Type 2 Engine:</strong> Compares incoming attributes against active customer master. If tariff or address changed, it expires version 1 and appends version 2 with valid timestamps.</p>`
      },
      storage_processed: {
        title: "Processed Data: PostgreSQL Customer 360 ODS",
        icon: "fa-solid fa-database",
        html: `<p>Production indexed Operational Data Store serving 50 Lakh customer entities. Master table: <code>cfa_customer_360_master</code>.</p>
               <p>Equipped with compound primary keys <code>(ftth_number, ca_number)</code> delivering sub-10ms queries for omnichannel APIs.</p>`
      },
      consume_api: {
        title: "Omnichannel API Gateway",
        icon: "fa-solid fa-bolt",
        html: `<p>Single high-concurrency API layer powering the BSNL SelfCare mobile application, IVRS helpline, Outbound Dialer (OBD), and Call Center CRM portals.</p>`
      },
      security_gov: {
        title: "Enterprise Governance, Security & Lineage",
        icon: "fa-solid fa-shield-halved",
        html: `<p><strong>RBAC:</strong> Field-level masking for customer PII according to TRAI guidelines.</p>
               <p><strong>Idempotency:</strong> SHA-256 batch signatures guarantee that re-running a batch never inflates counts or creates duplicate customer versions.</p>
               <p><strong>Lineage:</strong> Every record tracks <code>_ingest_batch_id</code>, <code>_source_dc</code>, and bi-temporal timestamps.</p>`
      }
    };

    let liveData = window.INITIAL_DATA || null;
    let currentExplorerTable = 'bronze_nz.but000';
    let isEditing = false;

    function fmtNum(n) {
      if (n === null || n === undefined) return '0';
      return Number(n).toLocaleString('en-IN');
    }

    function fmtDate(s) {
      if (!s) return '-';
      try {
        const d = new Date(s);
        return d.toLocaleDateString('en-IN', { month: 'short', day: '2-digit', hour: '2-digit', minute: '2-digit' });
      } catch (e) {
        return s;
      }
    }

    function showToast(title, message, isError = false) {
      const toast = document.getElementById('toast-notification');
      document.getElementById('toast-title').textContent = title;
      document.getElementById('toast-message').textContent = message;
      const icon = document.getElementById('toast-icon-container');
      if (isError) {
        icon.className = 'w-7 h-7 rounded-lg bg-rose-100 text-rose-700 flex items-center justify-center text-sm font-bold flex-shrink-0';
        icon.innerHTML = '<i class="fa-solid fa-triangle-exclamation"></i>';
      } else {
        icon.className = 'w-7 h-7 rounded-lg bg-emerald-100 text-emerald-700 flex items-center justify-center text-sm font-bold flex-shrink-0';
        icon.innerHTML = '<i class="fa-solid fa-circle-check"></i>';
      }
      toast.classList.remove('hidden');
      setTimeout(() => {
        hideToast();
      }, 5000);
    }

    function hideToast() {
      document.getElementById('toast-notification').classList.add('hidden');
    }

    function switchTab(tabId) {
      document.querySelectorAll('.tab-content').forEach(el => el.classList.add('hidden'));
      document.querySelectorAll('.tab-btn').forEach(btn => {
        btn.classList.remove('active', 'bg-blue-700', 'text-white', 'shadow-sm');
        btn.classList.add('text-slate-600', 'hover:text-slate-900', 'hover:bg-slate-100');
      });

      const target = document.getElementById(`tab-${tabId}`);
      const targetBtn = document.getElementById(`tab-btn-${tabId}`);

      if (target) target.classList.remove('hidden');
      if (targetBtn) {
        targetBtn.classList.add('active', 'bg-blue-700', 'text-white', 'shadow-sm');
        targetBtn.classList.remove('text-slate-600', 'hover:text-slate-900', 'hover:bg-slate-100');
      }
    }

    function openArchModal(key) {
      const data = modalDict[key] || {
        title: "Architecture Specification",
        icon: "fa-solid fa-circle-info",
        html: `<p>Detailed production specifications for ${key}.</p>`
      };
      document.getElementById('modal-title').textContent = data.title;
      document.getElementById('modal-icon-container').innerHTML = `<i class="${data.icon}"></i>`;
      document.getElementById('modal-content').innerHTML = data.html;
      document.getElementById('arch-modal').classList.remove('hidden');
    }

    function closeArchModal() {
      document.getElementById('arch-modal').classList.add('hidden');
    }

    function closeDossier() {
      document.getElementById('dossier-drawer').classList.add('translate-x-full');
    }

    // ===================================================================
    // MAPPING MODAL & CRUD FUNCTIONS
    // ===================================================================
    function openAddMappingModal() {
      isEditing = false;
      document.getElementById('mapping-modal-title').textContent = 'Add New Column Mapping';
      document.getElementById('map-zone').value = 'NZ';
      document.getElementById('map-table').value = 'BUT000';
      document.getElementById('map-col').value = '';
      document.getElementById('map-col').readOnly = false;
      document.getElementById('map-entity').value = 'Customer_master_data';
      document.getElementById('map-field').value = '';
      document.getElementById('map-type').value = 'DIRECT';
      document.getElementById('map-rule').value = 'DIRECT';
      document.getElementById('map-conf').value = 'HIGH';
      document.getElementById('map-sensitive').checked = false;
      document.getElementById('map-reason').value = '';
      document.getElementById('map-sensitive-reason-container').classList.add('hidden');
      document.getElementById('map-notes').value = '';
      document.getElementById('map-modal-error').classList.add('hidden');
      document.getElementById('mapping-modal').classList.remove('hidden');
      onSourceTableChange(document.getElementById('map-table').value);
    }

    function onZoneSelectChange(zone) {
      const select = document.getElementById('map-table');
      if (zone === 'EZ') {
        select.value = 'S_ORDER';
      } else {
        select.value = 'BUT000';
      }
      onSourceTableChange(select.value);
    }

    function editMapping(idx) {
      if (!liveData || !liveData.mapping_rules) return;
      const r = liveData.mapping_rules[idx];
      if (!r) return;

      isEditing = true;
      document.getElementById('mapping-modal-title').textContent = `Edit Mapping: [${r.zone || 'NZ'}] ${r.src_table}.${r.src_column}`;
      document.getElementById('map-zone').value = r.zone || 'NZ';
      document.getElementById('map-table').value = r.src_table;
      document.getElementById('map-col').value = r.src_column;
      document.getElementById('map-col').readOnly = true;
      document.getElementById('map-entity').value = r.target_entity;
      document.getElementById('map-field').value = r.target_field;
      document.getElementById('map-type').value = r.mapping_type || 'DIRECT';
      document.getElementById('map-rule').value = r.transform_rule || 'DIRECT';
      document.getElementById('map-conf').value = r.confidence || 'HIGH';

      // Check if sensitive
      const isSens = (liveData.sensitive_cols || []).some(s => (s.zone === r.zone || !s.zone || !r.zone) && s.src_table === r.src_table && s.src_column === r.src_column);
      document.getElementById('map-sensitive').checked = isSens;
      if (isSens) {
        const sc = (liveData.sensitive_cols || []).find(s => (s.zone === r.zone || !s.zone || !r.zone) && s.src_table === r.src_table && s.src_column === r.src_column);
        document.getElementById('map-reason').value = sc ? sc.reason : '';
        document.getElementById('map-sensitive-reason-container').classList.remove('hidden');
      } else {
        document.getElementById('map-reason').value = '';
        document.getElementById('map-sensitive-reason-container').classList.add('hidden');
      }

      document.getElementById('map-notes').value = r.notes || '';
      document.getElementById('map-modal-error').classList.add('hidden');
      document.getElementById('mapping-modal').classList.remove('hidden');
      onSourceTableChange(document.getElementById('map-table').value);
    }

    function closeMappingModal() {
      document.getElementById('mapping-modal').classList.add('hidden');
    }

    function toggleSensitiveReasonInput() {
      const chk = document.getElementById('map-sensitive').checked;
      const cont = document.getElementById('map-sensitive-reason-container');
      if (chk) {
        cont.classList.remove('hidden');
      } else {
        cont.classList.add('hidden');
      }
    }

    async function submitMappingForm() {
      const errBox = document.getElementById('map-modal-error');
      errBox.classList.add('hidden');

      const payload = {
        zone: document.getElementById('map-zone').value || 'NZ',
        src_table: document.getElementById('map-table').value,
        src_column: document.getElementById('map-col').value.trim(),
        target_entity: document.getElementById('map-entity').value,
        target_field: document.getElementById('map-field').value.trim(),
        mapping_type: document.getElementById('map-type').value,
        transform_rule: document.getElementById('map-rule').value.trim() || 'DIRECT',
        confidence: document.getElementById('map-conf').value,
        is_sensitive: document.getElementById('map-sensitive').checked,
        sensitive_reason: document.getElementById('map-reason').value.trim(),
        notes: document.getElementById('map-notes').value.trim()
      };

      if (!payload.src_column || !payload.target_field) {
        errBox.textContent = 'Please provide both Source Column and Target Field.';
        errBox.classList.remove('hidden');
        return;
      }

      const btn = document.getElementById('btn-save-mapping');
      btn.disabled = true;
      btn.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i> <span>Saving...</span>';

      try {
        const res = await fetch('/api/mapping', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload)
        });

        const result = await res.json();
        if (res.ok && result.ok) {
          closeMappingModal();
          showToast('Success', result.message || 'Mapping saved successfully.');
          await fetchData();
        } else {
          errBox.textContent = result.error || 'Failed to save mapping.';
          errBox.classList.remove('hidden');
        }
      } catch (err) {
        errBox.textContent = 'Network or server error: ' + err.message;
        errBox.classList.remove('hidden');
      } finally {
        btn.disabled = false;
        btn.innerHTML = '<i class="fa-solid fa-floppy-disk"></i> <span>Save Mapping Configuration</span>';
      }
    }

    async function confirmDeleteMapping(idx) {
      if (!liveData || !liveData.mapping_rules) return;
      const r = liveData.mapping_rules[idx];
      if (!r) return;

      if (!confirm(`Are you sure you want to delete mapping for ${r.src_table}.${r.src_column} -> ${r.target_entity}.${r.target_field}?`)) {
        return;
      }

      try {
        const res = await fetch('/api/mapping/delete', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            zone: r.zone || 'NZ',
            src_table: r.src_table,
            src_column: r.src_column,
            target_entity: r.target_entity,
            target_field: r.target_field
          })
        });

        const result = await res.json();
        if (res.ok && result.ok) {
          showToast('Deleted', result.message || 'Mapping deleted successfully.');
          await fetchData();
        } else {
          alert(result.error || 'Could not delete mapping.');
        }
      } catch (err) {
        alert('Error deleting mapping: ' + err.message);
      }
    }

    // ===================================================================
    // SOURCE SYSTEM TAGGING & FILTERING HELPERS (SINGLE ORACLE SCHEMA)
    // ===================================================================
    function getSourceBadge(srcName) {
      const norm = (srcName || '').toLowerCase();
      let cls = 'bg-slate-100 text-slate-800 border-slate-300';
      if (norm.includes('teerva')) cls = 'bg-emerald-100 text-emerald-800 border-emerald-300';
      else if (norm.includes('portal')) cls = 'bg-blue-100 text-blue-800 border-blue-300';
      else if (norm.includes('fms')) cls = 'bg-purple-100 text-purple-800 border-purple-300';
      else if (norm.includes('sz')) cls = 'bg-amber-100 text-amber-800 border-amber-300';
      else if (norm.includes('nz')) cls = 'bg-indigo-100 text-indigo-800 border-indigo-300';
      else if (norm.includes('ez')) cls = 'bg-cyan-100 text-cyan-800 border-cyan-300';
      else if (norm.includes('wz')) cls = 'bg-orange-100 text-orange-800 border-orange-300';
      return `<span class="inline-flex items-center space-x-1 px-2 py-0.5 rounded text-[10px] font-bold border ${cls}">
        <i class="fa-solid fa-tag text-[9px] opacity-70"></i>
        <span>${srcName || 'UNKNOWN'}</span>
      </span>`;
    }

    function getIngestBadge(itype) {
      const it = (itype || 'oracle').toLowerCase();
      if (it === 'parquet') {
        return `<span class="inline-flex items-center space-x-1 px-1.5 py-0.5 rounded text-[10px] font-bold bg-emerald-50 text-emerald-800 border border-emerald-300">
          <i class="fa-solid fa-file-lines text-[9px]"></i><span>PARQUET</span>
        </span>`;
      } else if (it === 'csv') {
        return `<span class="inline-flex items-center space-x-1 px-1.5 py-0.5 rounded text-[10px] font-bold bg-sky-50 text-sky-800 border border-sky-300">
          <i class="fa-solid fa-file-csv text-[9px]"></i><span>CSV</span>
        </span>`;
      } else if (it === 'jdbc') {
        return `<span class="inline-flex items-center space-x-1 px-1.5 py-0.5 rounded text-[10px] font-bold bg-purple-50 text-purple-800 border border-purple-300">
          <i class="fa-solid fa-network-wired text-[9px]"></i><span>JDBC</span>
        </span>`;
      }
      return `<span class="inline-flex items-center space-x-1 px-1.5 py-0.5 rounded text-[10px] font-bold bg-amber-50 text-amber-800 border border-amber-300">
        <i class="fa-solid fa-database text-[9px]"></i><span>ORACLE</span>
      </span>`;
    }

    function openTagSourceModal(tableName, currentSource, zone, metadata) {
      document.getElementById('tag-source-table').value = tableName;
      document.getElementById('tag-source-zone').value = zone || 'NZ';
      document.getElementById('tag-source-system').value = currentSource || '';
      if (metadata && typeof metadata === 'object' && Object.keys(metadata).length > 0) {
        document.getElementById('tag-source-metadata').value = JSON.stringify(metadata, null, 2);
      } else {
        document.getElementById('tag-source-metadata').value = '';
      }
      document.getElementById('tag-source-modal').classList.remove('hidden');
    }

    function closeTagSourceModal() {
      document.getElementById('tag-source-modal').classList.add('hidden');
    }

    function setQuickSourceTag(tagName) {
      document.getElementById('tag-source-system').value = tagName;
    }

    async function submitTagSourceForm() {
      const table = document.getElementById('tag-source-table').value;
      const source = document.getElementById('tag-source-system').value.trim();
      const zone = document.getElementById('tag-source-zone').value;
      const metaRaw = document.getElementById('tag-source-metadata').value.trim();
      let meta = null;
      if (metaRaw) {
        try {
          meta = JSON.parse(metaRaw);
        } catch (e) {
          alert("Invalid JSON format in Source Metadata: " + e.message);
          return;
        }
      }
      const btn = document.getElementById('btn-save-tag-source');
      btn.disabled = true;
      btn.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i> Saving...';
      try {
        const res = await fetch('/api/table/tag', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ table, source, zone, meta })
        });
        const result = await res.json();
        if (result.ok) {
          showToast("Source Tag Saved", `Marked '${table}' with source '${source}'`);
          closeTagSourceModal();
          await fetchData();
        } else {
          alert("Error: " + (result.error || "Failed to update source tag"));
        }
      } catch (err) {
        alert("Network error: " + err.message);
      } finally {
        btn.disabled = false;
        btn.innerHTML = '<i class="fa-solid fa-floppy-disk"></i> <span>Save Source Tag</span>';
      }
    }

    // ===================================================================
    // MULTI-MODE INGESTION CONTROLS (PARQUET, CSV, JDBC)
    // ===================================================================
    function openIngestModal() {
      document.getElementById('ingest-modal').classList.remove('hidden');
      const alertBox = document.getElementById('ingest-status-alert');
      if (alertBox) alertBox.classList.add('hidden');
    }

    function closeIngestModal() {
      document.getElementById('ingest-modal').classList.add('hidden');
    }

    function onIngestTypeChange(type) {
      const fileGroup = document.getElementById('ingest-file-group');
      const jdbcGroup = document.getElementById('ingest-jdbc-group');
      const delimGroup = document.getElementById('ingest-delimiter-group');
      if (type === 'jdbc') {
        fileGroup.classList.add('hidden');
        jdbcGroup.classList.remove('hidden');
        delimGroup.classList.add('hidden');
      } else if (type === 'csv') {
        fileGroup.classList.remove('hidden');
        jdbcGroup.classList.add('hidden');
        delimGroup.classList.remove('hidden');
        document.getElementById('ingest-file-label').textContent = 'CSV File Path or Glob (*):';
      } else {
        fileGroup.classList.remove('hidden');
        jdbcGroup.classList.add('hidden');
        delimGroup.classList.add('hidden');
        document.getElementById('ingest-file-label').textContent = 'Parquet File Path or Glob (*):';
      }
    }

    function setQuickIngestTag(tag) {
      document.getElementById('ingest-source-tag').value = tag;
    }

    function setQuickIngestFile(filePath, tableName, sourceTag, itype) {
      const radio = document.querySelector(`input[name="ingest_type_radio"][value="${itype}"]`);
      if (radio) {
        radio.checked = true;
        onIngestTypeChange(itype);
      }
      document.getElementById('ingest-file-path').value = filePath;
      document.getElementById('ingest-target-table').value = tableName;
      document.getElementById('ingest-source-tag').value = sourceTag;
    }

    function setQuickIngestJDBC(url, tableName, sourceTag) {
      const radio = document.querySelector('input[name="ingest_type_radio"][value="jdbc"]');
      if (radio) {
        radio.checked = true;
        onIngestTypeChange('jdbc');
      }
      document.getElementById('ingest-jdbc-url').value = url;
      document.getElementById('ingest-target-table').value = tableName;
      document.getElementById('ingest-source-tag').value = sourceTag;
    }

    async function submitIngestForm() {
      const typeRadio = document.querySelector('input[name="ingest_type_radio"]:checked');
      const itype = typeRadio ? typeRadio.value : 'parquet';
      const filePath = document.getElementById('ingest-file-path').value.trim();
      const jdbcUrl = document.getElementById('ingest-jdbc-url').value.trim();
      const tableName = document.getElementById('ingest-target-table').value.trim();
      const zone = document.getElementById('ingest-zone').value;
      const sourceTag = document.getElementById('ingest-source-tag').value.trim();
      const mode = document.getElementById('ingest-mode').value;
      const pk = document.getElementById('ingest-pk-cols').value.trim();
      const delimiter = document.getElementById('ingest-csv-delimiter').value || ',';
      const schema = document.getElementById('ingest-source-schema').value.trim();

      const alertBox = document.getElementById('ingest-status-alert');
      alertBox.className = 'p-3 rounded-lg text-xs font-mono bg-blue-50 text-blue-800 border border-blue-200 block';
      alertBox.innerHTML = '<i class="fa-solid fa-spinner fa-spin mr-1"></i> Ingestion in progress... processing data...';

      const btn = document.getElementById('btn-run-ingest');
      btn.disabled = true;

      try {
        const res = await fetch('/api/ingest', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            type: itype,
            file: filePath,
            url: jdbcUrl,
            table: tableName,
            zone: zone,
            source: sourceTag,
            mode: mode,
            pk: pk,
            delimiter: delimiter,
            schema: schema
          })
        });
        const result = await res.json();
        if (res.ok && result.ok) {
          alertBox.className = 'p-3 rounded-lg text-xs font-mono bg-emerald-50 text-emerald-800 border border-emerald-200 block';
          alertBox.innerHTML = `
            <div class="font-bold flex items-center space-x-1 mb-1">
              <i class="fa-solid fa-circle-check text-emerald-600"></i>
              <span>INGESTION COMPLETED SUCCESSFULLY</span>
            </div>
            <div>Target: <strong>${result.target_table}</strong> (${result.ingest_type.toUpperCase()})</div>
            <div>Source System: <strong>${result.source_system}</strong> | Mode: <strong>${result.mode.toUpperCase()}</strong></div>
            <div>Rows: Fetched: <strong>${result.rows_fetched}</strong>, Loaded: <strong>${result.rows_loaded}</strong>, Quarantined: <strong>${result.rows_quarantined}</strong>, Dups: <strong>${result.duplicates_skipped}</strong></div>
            <div class="text-[10px] text-slate-500 mt-1">Elapsed: ${result.elapsed_s}s</div>
          `;
          showToast("Ingestion Complete", `${result.ingest_type.toUpperCase()} -> ${result.target_table} (${result.rows_loaded} loaded)`);
          await fetchData();
        } else {
          alertBox.className = 'p-3 rounded-lg text-xs font-mono bg-rose-50 text-rose-800 border border-rose-200 block';
          alertBox.innerHTML = `<div class="font-bold mb-1"><i class="fa-solid fa-triangle-exclamation mr-1"></i> Ingestion Failed</div><div>${result.error || 'Unknown error occurred'}</div>`;
        }
      } catch (err) {
        alertBox.className = 'p-3 rounded-lg text-xs font-mono bg-rose-50 text-rose-800 border border-rose-200 block';
        alertBox.innerHTML = `<div class="font-bold mb-1"><i class="fa-solid fa-triangle-exclamation mr-1"></i> Network/Server Error</div><div>${err.message}</div>`;
      } finally {
        btn.disabled = false;
      }
    }

    function filterTablesBySource(selectedSource, selectedType) {
      if (!liveData || !liveData.tables) return;
      const filterSelect = document.getElementById('source-system-filter');
      if (selectedSource !== undefined && filterSelect) {
        filterSelect.value = selectedSource;
      }
      const typeSelect = document.getElementById('ingest-type-filter');
      if (selectedType !== undefined && typeSelect) {
        typeSelect.value = selectedType;
      }
      const sourceVal = filterSelect ? filterSelect.value : '';
      const typeVal = typeSelect ? typeSelect.value.toLowerCase() : '';
      const searchVal = document.getElementById('table-search-input') ? document.getElementById('table-search-input').value.trim().toUpperCase() : '';

      const filtered = liveData.tables.filter(t => {
        const sName = t.source_system || t.zone || '';
        const itype = (t.ingest_type || 'oracle').toLowerCase();
        const matchSource = !sourceVal || sName.toLowerCase() === sourceVal.toLowerCase();
        const matchType = !typeVal || itype === typeVal;
        const matchSearch = !searchVal || t.src_table.toUpperCase().includes(searchVal) || (t.bronze_table && t.bronze_table.toUpperCase().includes(searchVal));
        return matchSource && matchType && matchSearch;
      });

      const badge = document.getElementById('filtered-tables-count-badge');
      if (badge) badge.textContent = `${filtered.length} / ${liveData.tables.length} tables`;

      renderTableRows(filtered);
    }

    function renderTableRows(tables) {
      const tablesBody = document.getElementById('tables-status-body');
      if (!tables || tables.length === 0) {
        tablesBody.innerHTML = '<tr><td colspan="14" class="py-6 text-center text-slate-400 font-sans">No tables match the selected filter</td></tr>';
        return;
      }
      tablesBody.innerHTML = tables.map(t => {
        let modeBadge = '<span class="bg-sky-100 text-sky-700 font-bold px-1.5 py-0.5 rounded text-[10px]">APPEND</span>';
        if (t.mode === 'merge') {
          modeBadge = '<span class="bg-purple-100 text-purple-700 font-bold px-1.5 py-0.5 rounded text-[10px]">MERGE</span>';
        } else if (t.mode === 'full_refresh') {
          modeBadge = '<span class="bg-amber-100 text-amber-700 font-bold px-1.5 py-0.5 rounded text-[10px]">FULL REFRESH</span>';
        }
        const isHealthy = !t.lag_hours || (t.freshness_sla_hours && t.lag_hours <= t.freshness_sla_hours);
        const srcName = t.source_system || t.zone || 'UNKNOWN';
        const metaStr = JSON.stringify(t.source_metadata || {}).replace(/"/g, '&quot;');
        return `
          <tr class="hover:bg-slate-50 transition-colors">
            <td class="py-2 px-3 font-bold text-slate-800">${t.zone}</td>
            <td class="py-2 px-3">${getSourceBadge(srcName)}</td>
            <td class="py-2 px-3">${getIngestBadge(t.ingest_type)}</td>
            <td class="py-2 px-3 font-semibold text-slate-900">${t.src_table}</td>
            <td class="py-2 px-3 text-slate-500 font-mono text-[11px]">${t.bronze_table}</td>
            <td class="py-2 px-3">${modeBadge}</td>
            <td class="py-2 px-3 text-right text-slate-600">${fmtNum(t.est_rows)}</td>
            <td class="py-2 px-3 text-right text-emerald-700 font-bold">${fmtNum(t.rows_loaded_all_runs)}</td>
            <td class="py-2 px-3 text-right text-slate-700">${fmtNum(t.chunks_done || 0)} / ${fmtNum((t.chunks_done || 0) + (t.chunks_failed || 0))}</td>
            <td class="py-2 px-3 text-right text-sky-700">${fmtNum(t.duplicates_skipped_all_runs)}</td>
            <td class="py-2 px-3 text-right text-rose-700">${fmtNum(t.rows_quarantined_all_runs)}</td>
            <td class="py-2 px-3 text-slate-600 text-[11px]">${t.lag_hours !== null ? t.lag_hours.toFixed(1) + 'h' : '-'} / ${t.freshness_sla_hours || '-'}h</td>
            <td class="py-2 px-3">
              ${isHealthy ? 
                '<span class="text-emerald-600 font-bold flex items-center space-x-1"><i class="fa-solid fa-circle-check"></i> <span>OK</span></span>' :
                '<span class="text-rose-600 font-bold flex items-center space-x-1"><i class="fa-solid fa-triangle-exclamation"></i> <span>LAG</span></span>'}
            </td>
            <td class="py-2 px-3 text-right">
              <button onclick="openTagSourceModal('${t.src_table}', '${srcName}', '${t.zone}', ${metaStr})" class="px-2 py-1 bg-indigo-50 hover:bg-indigo-100 text-indigo-700 border border-indigo-200 rounded text-[10px] font-bold inline-flex items-center space-x-1 transition-colors">
                <i class="fa-solid fa-tag text-[9px]"></i>
                <span>Tag</span>
              </button>
            </td>
          </tr>
        `;
      }).join('');
    }

    // ===================================================================
    // RENDER FUNCTIONS
    // ===================================================================
    function renderControlPlane(data) {
      if (!data) return;
      if (data.kpi) {
        document.getElementById('kpi-tables').textContent = data.kpi.tables_count || 0;
        document.getElementById('kpi-active').textContent = data.kpi.active_chunks_count || 0;
        document.getElementById('kpi-rows').textContent = fmtNum(data.kpi.total_rows_loaded);
        document.getElementById('kpi-dups').textContent = fmtNum(data.kpi.total_duplicates_skipped);
        document.getElementById('kpi-quarantine').textContent = fmtNum(data.kpi.total_rows_quarantined);
        document.getElementById('kpi-alerts').textContent = data.kpi.alerts_count || 0;
        
        const badgeTables = document.getElementById('badge-tables-count');
        if (badgeTables) {
          badgeTables.textContent = `${data.kpi.tables_count || 0} Tables`;
        }
      }

      const alertsCard = document.getElementById('alerts-card');
      const alertsList = document.getElementById('alerts-list');
      const alertsCountBadge = document.getElementById('alerts-count-badge');
      if (data.alerts && data.alerts.length > 0) {
        alertsCard.classList.remove('hidden');
        alertsCountBadge.textContent = `${data.alerts.length} alerts`;
        alertsList.innerHTML = data.alerts.map(a => `
          <div class="flex items-start space-x-2 bg-white/80 border border-rose-200 rounded p-2 text-xs font-mono">
            <span class="px-1.5 py-0.5 rounded text-[10px] font-bold ${a.severity === 'critical' ? 'bg-rose-700 text-white' : 'bg-amber-500 text-white'}">${a.severity.toUpperCase()}</span>
            <span class="font-bold text-slate-800">[${a.zone}.${a.src_table}]</span>
            <span class="flex-1 text-slate-700 font-sans">${a.issue}</span>
            <span class="text-slate-400 text-[10px]">${fmtDate(a.since)}</span>
          </div>
        `).join('');
      } else {
        alertsCard.classList.add('hidden');
      }

      const activeBody = document.getElementById('active-chunks-body');
      const activeCountBadge = document.getElementById('active-chunks-count-badge');
      if (activeCountBadge) activeCountBadge.textContent = `${data.active ? data.active.length : 0} active`;
      if (!data.active || data.active.length === 0) {
        activeBody.innerHTML = '<tr><td colspan="10" class="py-6 text-center text-slate-400 font-sans">No chunks currently running</td></tr>';
      } else {
        activeBody.innerHTML = data.active.map(c => `
          <tr class="hover:bg-amber-50/40">
            <td class="py-2 px-3 font-bold text-slate-900">${c.chunk_id}</td>
            <td class="py-2 px-3 text-slate-500">${c.run_id}</td>
            <td class="py-2 px-3 font-semibold text-blue-700">${c.zone}.${c.src_table}</td>
            <td class="py-2 px-3 text-slate-600">${c.chunk_kind || c.window_kind || 'historic'}</td>
            <td class="py-2 px-3 text-slate-600 text-[11px]">${c.chunk_from || '-'} &rarr; ${c.chunk_to || '-'}</td>
            <td class="py-2 px-3 text-right text-emerald-700 font-bold">${fmtNum(c.rows_fetched)}</td>
            <td class="py-2 px-3 text-slate-600">${c.elapsed || '-'}</td>
            <td class="py-2 px-3 text-slate-600">${c.rows_per_sec ? Math.round(c.rows_per_sec) + ' r/s' : '-'}</td>
            <td class="py-2 px-3 text-slate-500 text-[11px]">${c.since_heartbeat || '-'}</td>
            <td class="py-2 px-3">
              ${c.stalled ? 
                '<span class="bg-rose-100 text-rose-700 text-[10px] font-bold px-2 py-0.5 rounded-full border border-rose-200">STALLED</span>' : 
                '<span class="bg-emerald-100 text-emerald-700 text-[10px] font-bold px-2 py-0.5 rounded-full border border-emerald-200">ACTIVE</span>'}
            </td>
          </tr>
        `).join('');
      }

      // Populate source filter and dynamic pills
      if (data.tables) {
        const sourceCounts = {};
        const typeCounts = {};
        data.tables.forEach(t => {
          const s = t.source_system || t.zone || 'UNKNOWN';
          sourceCounts[s] = (sourceCounts[s] || 0) + 1;
          const ty = (t.ingest_type || 'oracle').toLowerCase();
          typeCounts[ty] = (typeCounts[ty] || 0) + 1;
        });

        const filterSelect = document.getElementById('source-system-filter');
        if (filterSelect) {
          const curVal = filterSelect.value;
          filterSelect.innerHTML = '<option value="">All Source Systems (' + data.tables.length + ')</option>' +
            Object.keys(sourceCounts).sort().map(s => `
              <option value="${s}" ${s === curVal ? 'selected' : ''}>${s} (${sourceCounts[s]} tables)</option>
            `).join('');
        }

        const pillsContainer = document.getElementById('source-system-pills');
        if (pillsContainer) {
          pillsContainer.innerHTML = `
            <div class="flex flex-wrap items-center gap-1.5 w-full">
              <span class="text-[11px] text-slate-400 font-medium mr-1">Source Systems:</span>
              <button onclick="filterTablesBySource('', undefined)" class="px-2.5 py-0.5 rounded-full text-xs font-semibold bg-slate-100 hover:bg-slate-200 text-slate-700 border border-slate-300 transition-colors">
                All (${data.tables.length})
              </button>
              ${Object.keys(sourceCounts).sort().map(s => {
                const badge = getSourceBadge(s);
                return `
                  <button onclick="filterTablesBySource('${s}', undefined)" class="hover:opacity-80 transition-opacity">
                    ${badge.replace('</span>', ` <span class="bg-white/80 px-1 py-0.2 rounded-full text-[9px] font-black ml-1">${sourceCounts[s]}</span></span>`)}
                  </button>
                `;
              }).join('')}
            </div>
            <div class="flex flex-wrap items-center gap-1.5 w-full mt-2 pt-2 border-t border-slate-100">
              <span class="text-[11px] text-slate-400 font-medium mr-1">Ingest Modes:</span>
              <button onclick="filterTablesBySource(undefined, '')" class="px-2.5 py-0.5 rounded-full text-xs font-semibold bg-slate-100 hover:bg-slate-200 text-slate-700 border border-slate-300 transition-colors">
                All (${data.tables.length})
              </button>
              ${Object.keys(typeCounts).sort().map(ty => {
                const badge = getIngestBadge(ty);
                return `
                  <button onclick="filterTablesBySource(undefined, '${ty}')" class="hover:opacity-80 transition-opacity">
                    ${badge.replace('</span>', ` <span class="bg-white/80 px-1 py-0.2 rounded-full text-[9px] font-black ml-1">${typeCounts[ty]}</span></span>`)}
                  </button>
                `;
              }).join('')}
            </div>
          `;
        }

        filterTablesBySource();
      }

      const historicGrid = document.getElementById('historic-progress-grid');
      if (data.historic && data.historic.length > 0) {
        historicGrid.innerHTML = data.historic.map(h => {
          const pct = Math.round(h.pct_done || 0);
          const isDone = pct >= 100;
          return `
            <div class="bg-slate-50 border border-slate-200 rounded-lg p-3">
              <div class="flex items-center justify-between mb-1 text-xs">
                <span class="font-mono font-bold text-slate-900">${h.zone}.${h.src_table}</span>
                <span class="font-mono font-semibold ${isDone ? 'text-emerald-700' : 'text-blue-700'}">${pct}% (${h.chunks_done}/${h.chunks_total} chunks)</span>
              </div>
              <div class="w-full bg-slate-200 rounded-full h-2 overflow-hidden mb-2">
                <div class="h-2 rounded-full ${isDone ? 'bg-emerald-500' : 'bg-blue-600'}" style="width: ${pct}%"></div>
              </div>
              <div class="flex justify-between text-[11px] text-slate-500 font-mono">
                <span>Loaded: ${fmtNum(h.rows_loaded)} rows</span>
                <span>Dups: ${fmtNum(h.duplicates_skipped)}</span>
              </div>
            </div>
          `;
        }).join('');
      }

      const eventsBody = document.getElementById('events-log-body');
      if (data.events && data.events.length > 0) {
        eventsBody.innerHTML = data.events.map(ev => `
          <tr class="hover:bg-slate-50">
            <td class="py-1.5 px-2 text-slate-400 text-[10px] whitespace-nowrap">${fmtDate(ev.ts)}</td>
            <td class="py-1.5 px-2"><span class="px-1.5 py-0.5 rounded text-[9px] font-bold ${ev.level === 'ERROR' ? 'bg-rose-100 text-rose-800' : ev.level === 'WARNING' ? 'bg-amber-100 text-amber-800' : 'bg-slate-100 text-slate-700'}">${ev.level}</span></td>
            <td class="py-1.5 px-2 font-bold text-slate-800">${ev.event}</td>
            <td class="py-1.5 px-2 text-slate-500">${ev.chunk_id ? 'chunk #' + ev.chunk_id : '-'}</td>
            <td class="py-1.5 px-2 text-slate-700 font-sans">${ev.message || '-'}</td>
          </tr>
        `).join('');
      }
    }

    function renderQuarantineConsole(data) {
      if (!data) return;
      const count = (data.quarantine_rows && data.quarantine_rows.length) || (data.kpi && data.kpi.total_rows_quarantined) || 0;
      document.getElementById('badge-quarantine-count').textContent = count;
      document.getElementById('quarantine-total-count').textContent = count;

      let pkNulls = 0;
      if (data.quarantine_rows) {
        pkNulls = data.quarantine_rows.filter(r => (r.rejection_reason || '').toLowerCase().includes('pk') || (r.rejection_reason || '').toLowerCase().includes('null')).length;
      }
      document.getElementById('quarantine-pk-count').textContent = pkNulls;

      const qBody = document.getElementById('quarantine-table-body');
      if (!data.quarantine_rows || data.quarantine_rows.length === 0) {
        qBody.innerHTML = '<tr><td colspan="6" class="py-6 text-center text-slate-400 font-sans">No quarantined rows recorded</td></tr>';
      } else {
        qBody.innerHTML = data.quarantine_rows.map(q => `
          <tr class="hover:bg-rose-50/40">
            <td class="py-2 px-3 font-bold text-rose-900">${q.quarantine_id}</td>
            <td class="py-2 px-3 text-slate-500 text-[11px]">Run #${q.run_id || '-'} / Chunk #${q.chunk_id || '-'}</td>
            <td class="py-2 px-3 font-semibold text-slate-900">${q.zone || ''}.${q.src_table || ''}</td>
            <td class="py-2 px-3 text-rose-700 font-semibold font-sans">${q.rejection_reason}</td>
            <td class="py-2 px-3 text-slate-500 text-[11px]">${fmtDate(q.quarantined_at)}</td>
            <td class="py-2 px-3 text-right">
              <button onclick="inspectQuarantinePayload(${q.quarantine_id})" class="px-2.5 py-1 bg-rose-100 hover:bg-rose-200 text-rose-800 rounded font-semibold text-[11px] transition-colors">
                <i class="fa-solid fa-code"></i> Inspect Payload
              </button>
            </td>
          </tr>
        `).join('');
      }
    }

    function inspectQuarantinePayload(qid) {
      if (!liveData || !liveData.quarantine_rows) return;
      const row = liveData.quarantine_rows.find(r => r.quarantine_id === qid);
      if (!row) return;

      const payloadStr = typeof row.row_data === 'string' ? row.row_data : JSON.stringify(row.row_data, null, 2);
      const title = `Quarantine Row #${qid} - [${row.zone || ''}.${row.src_table || ''}]`;
      const html = `
        <div class="space-y-3">
          <div class="bg-rose-50 border border-rose-200 rounded-lg p-3 text-xs">
            <span class="font-bold text-rose-800 block">Rejection Reason:</span>
            <span class="text-rose-900 font-mono">${row.rejection_reason}</span>
          </div>
          <div>
            <div class="flex items-center justify-between mb-1">
              <span class="text-xs font-semibold text-slate-500 uppercase tracking-wider">Raw Payload (With Sensitive Columns Redacted):</span>
              <span class="text-[10px] bg-blue-100 text-blue-800 px-1.5 py-0.5 rounded font-mono font-bold">100% PII Masked</span>
            </div>
            <pre class="bg-slate-900 text-emerald-400 p-4 rounded-xl text-xs font-mono overflow-x-auto max-h-72 border border-slate-800">${payloadStr}</pre>
          </div>
        </div>
      `;

      document.getElementById('modal-title').textContent = title;
      document.getElementById('modal-icon-container').innerHTML = '<i class="fa-solid fa-ban text-rose-600"></i>';
      document.getElementById('modal-content').innerHTML = html;
      document.getElementById('arch-modal').classList.remove('hidden');
    }

    let activeTablesZoneFilter = 'ALL';

    function setTablesZoneFilter(z) {
      activeTablesZoneFilter = z;
      const bAll = document.getElementById('btn-tbl-filter-all');
      const bNZ = document.getElementById('btn-tbl-filter-nz');
      const bEZ = document.getElementById('btn-tbl-filter-ez');
      [bAll, bNZ, bEZ].forEach(b => {
        if (b) {
          b.className = 'px-2.5 py-1 rounded-md text-slate-600 hover:text-slate-900 transition-all';
        }
      });
      if (z === 'ALL' && bAll) bAll.className = 'px-2.5 py-1 rounded-md bg-white text-slate-900 shadow-xs transition-all';
      if (z === 'NZ' && bNZ) bNZ.className = 'px-2.5 py-1 rounded-md bg-white text-blue-800 font-bold shadow-xs transition-all';
      if (z === 'EZ' && bEZ) bEZ.className = 'px-2.5 py-1 rounded-md bg-white text-purple-800 font-bold shadow-xs transition-all';
      if (liveData) renderMappingGuide(liveData);
    }

    function renderMappingGuide(data) {
      if (!data) return;
      const badgeCount = document.getElementById('badge-mapping-count');
      if (badgeCount && data.mapping_rules) {
        badgeCount.textContent = `${data.mapping_rules.length} Mappings`;
      }

      // 1. 25 Physical Tables Grid (NZ SAP & EZ Siebel)
      const nzGrid = document.getElementById('nz-tables-grid');
      if (data.nz_summary && data.nz_summary.length > 0) {
        const filteredSummary = data.nz_summary.filter(nz => {
          if (activeTablesZoneFilter === 'ALL') return true;
          return nz.zone === activeTablesZoneFilter;
        });
        nzGrid.innerHTML = filteredSummary.map(nz => {
          const isEZ = nz.zone === 'EZ';
          const zBadge = isEZ ? 'bg-purple-100 text-purple-800' : 'bg-blue-100 text-blue-800';
          const zLabel = isEZ ? 'Siebel EZ' : 'SAP NZ';
          const borderHover = isEZ ? 'hover:border-purple-400' : 'hover:border-blue-400';
          return `
            <div class="bg-slate-50 border border-slate-200 rounded-lg p-3 ${borderHover} transition-colors">
              <div class="flex items-center justify-between mb-1">
                <span class="font-mono font-bold text-slate-900 text-sm">${nz.src_table}</span>
                <span class="${zBadge} text-[10px] font-bold px-1.5 py-0.5 rounded">${zLabel}</span>
              </div>
              <div class="text-[11px] text-slate-600 space-y-0.5 mt-2 font-mono">
                <div class="flex justify-between"><span>Mapped:</span><span class="font-bold text-emerald-700">${nz.mapped_columns_count}</span></div>
                <div class="flex justify-between"><span>Targets:</span><span class="font-semibold text-slate-800">${nz.target_fields_count}</span></div>
                <div class="flex justify-between"><span>Sensitive:</span><span class="font-bold ${nz.sensitive_columns_count > 0 ? 'text-amber-700' : 'text-slate-400'}">${nz.sensitive_columns_count}</span></div>
                <div class="flex justify-between"><span>Unmapped:</span><span class="text-slate-500">${nz.total_unmapped_count}</span></div>
              </div>
            </div>
          `;
        }).join('');
      }

      // 2. Sensitive columns (48 columns: 9 NZ + 39 EZ)
      const sensBody = document.getElementById('sensitive-columns-body');
      if (data.sensitive_cols && data.sensitive_cols.length > 0) {
        sensBody.innerHTML = data.sensitive_cols.map(sc => {
          const isEZ = sc.zone === 'EZ';
          const zBadge = isEZ ? 
            '<span class="bg-purple-100 text-purple-800 text-[10px] font-bold px-1.5 py-0.5 rounded">EZ</span>' : 
            '<span class="bg-blue-100 text-blue-800 text-[10px] font-bold px-1.5 py-0.5 rounded">NZ</span>';
          return `
            <tr class="hover:bg-amber-100/40">
              <td class="py-2 px-3">${zBadge}</td>
              <td class="py-2 px-3 font-bold text-slate-900">${sc.src_table}</td>
              <td class="py-2 px-3 font-bold text-amber-900 bg-amber-100/60 rounded">${sc.src_column}</td>
              <td class="py-2 px-3 text-slate-700 font-sans">${sc.reason}</td>
              <td class="py-2 px-3 text-slate-500 text-[11px]">${sc.sample_value || '-'}</td>
              <td class="py-2 px-3"><span class="bg-rose-100 text-rose-800 text-[10px] font-bold px-2 py-0.5 rounded border border-rose-200">[REDACTED_SENSITIVE]</span></td>
            </tr>
          `;
        }).join('');
      }

      // 3. Mapping Rules Table
      renderMappingRulesTable(data.mapping_rules || []);

      // 4. CFA Targets
      const cfaBody = document.getElementById('cfa-targets-body');
      if (data.cfa_targets && data.cfa_targets.length > 0) {
        cfaBody.innerHTML = data.cfa_targets.map(ct => `
          <tr class="hover:bg-slate-50">
            <td class="py-1.5 px-3 font-bold text-indigo-900">${ct.target_entity}</td>
            <td class="py-1.5 px-3 font-bold text-slate-900">${ct.target_field}</td>
            <td class="py-1.5 px-3 text-slate-600">${ct.data_type || '-'}</td>
            <td class="py-1.5 px-3 text-slate-500">${ct.nullable ? 'YES' : 'NO'}</td>
            <td class="py-1.5 px-3 text-blue-700">${ct.source_columns || '-'}</td>
            <td class="py-1.5 px-3"><span class="bg-emerald-100 text-emerald-800 text-[10px] font-bold px-1.5 py-0.5 rounded">${ct.confidence || 'HIGH'}</span></td>
          </tr>
        `).join('');
      }

      // 5. Live Oracle Tables Catalog
      if (data.oracle_catalog) {
        renderOracleTablesCatalog(data.oracle_catalog);
        populateSourceTableDropdown(data.oracle_catalog);
      }
    }

    let currentOracleZone = 'NZ';
    let currentOracleCatalog = null;

    async function fetchLiveOracleTables(zone) {
      currentOracleZone = zone || 'NZ';
      try {
        const res = await fetch(`/api/oracle/tables?zone=${currentOracleZone}`);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();
        currentOracleCatalog = data;
        renderOracleTablesCatalog(data);
        populateSourceTableDropdown(data);
      } catch (e) {
        console.error('Failed to fetch Oracle tables:', e);
      }
    }

    function renderOracleTablesCatalog(cat) {
      if (!cat) return;
      currentOracleCatalog = cat;
      const statusPill = document.getElementById('oracle-status-pill');
      if (statusPill) {
        if (cat.connected) {
          statusPill.className = 'inline-flex items-center space-x-1.5 px-2 py-0.5 rounded-full text-[11px] font-bold bg-emerald-100 text-emerald-800 border border-emerald-300';
          statusPill.innerHTML = '<span class="w-2 h-2 rounded-full bg-emerald-500 animate-pulse"></span><span>Oracle DB Connected</span>';
        } else {
          statusPill.className = 'inline-flex items-center space-x-1.5 px-2 py-0.5 rounded-full text-[11px] font-bold bg-rose-100 text-rose-800 border border-rose-300';
          statusPill.innerHTML = '<span class="w-2 h-2 rounded-full bg-rose-500"></span><span>Oracle Offline</span>';
        }
      }

      const sub = document.getElementById('oracle-catalog-subtitle');
      if (sub && cat.tables) {
        sub.innerHTML = `Current Schema: <strong class="text-slate-800 font-mono">${cat.current_schema || 'N/A'}</strong> | Accessible Schemas: <strong class="text-indigo-700 font-mono">${(cat.schemas || []).join(', ') || 'N/A'}</strong> | Found <strong class="text-slate-900">${cat.tables.length} tables</strong> in Oracle DB connection`;
      }

      // Populate schema dropdown
      const schemaSelect = document.getElementById('oracle-schema-filter');
      if (schemaSelect && cat.schemas) {
        const curVal = schemaSelect.value;
        schemaSelect.innerHTML = '<option value="">All Schemas</option>' + cat.schemas.map(s => `
          <option value="${s}" ${s === curVal ? 'selected' : ''}>${s}</option>
        `).join('');
      }

      filterOracleCatalogTable();
    }

    function filterOracleCatalogTable() {
      if (!currentOracleCatalog || !currentOracleCatalog.tables) return;
      const tbody = document.getElementById('oracle-tables-catalog-body');
      if (!tbody) return;

      const schemaFilter = document.getElementById('oracle-schema-filter') ? document.getElementById('oracle-schema-filter').value : '';
      let tables = currentOracleCatalog.tables;
      if (schemaFilter) {
        tables = tables.filter(t => t.owner === schemaFilter);
      }

      if (tables.length === 0) {
        tbody.innerHTML = `
          <tr>
            <td colspan="7" class="py-8 text-center text-slate-400 font-sans text-xs">
              No tables found in Oracle connection for the selected filter.
            </td>
          </tr>
        `;
        return;
      }

      // Check registered tables in liveData
      const registeredMap = new Map();
      if (liveData && liveData.tables) {
        liveData.tables.forEach(t => {
          registeredMap.set(t.src_table.toUpperCase(), t);
          registeredMap.set(`${t.zone}.${t.src_schema}.${t.src_table}`.toUpperCase(), t);
        });
      }

      tbody.innerHTML = tables.map(t => {
        const key = `${currentOracleZone}.${t.owner}.${t.table_name}`.toUpperCase();
        const regTable = registeredMap.get(key) || registeredMap.get(t.table_name.toUpperCase());
        const isReg = !!regTable;
        const regBadge = isReg
          ? '<span class="bg-emerald-100 text-emerald-800 px-2 py-0.5 rounded text-[10px] font-bold border border-emerald-300">REGISTERED</span>'
          : '<span class="bg-slate-100 text-slate-600 px-2 py-0.5 rounded text-[10px] font-medium border border-slate-200">AVAILABLE</span>';

        const srcName = regTable ? (regTable.source_system || regTable.zone) : (t.owner.includes('CRM') ? 'NZ_zone' : 'SZ_zone');
        const metaStr = regTable ? JSON.stringify(regTable.source_metadata || {}).replace(/"/g, '&quot;') : '{}';

        return `
          <tr class="hover:bg-slate-50 transition-colors">
            <td class="py-2.5 px-3 font-bold text-slate-900">${currentOracleZone}</td>
            <td class="py-2.5 px-3">${getSourceBadge(srcName)}</td>
            <td class="py-2.5 px-3 font-bold text-indigo-700">${t.owner}</td>
            <td class="py-2.5 px-3 font-extrabold text-slate-900">${t.table_name}</td>
            <td class="py-2.5 px-3 text-slate-600">${t.num_rows !== null ? Number(t.num_rows).toLocaleString() : '0'}</td>
            <td class="py-2.5 px-3">${regBadge}</td>
            <td class="py-2.5 px-3 text-right space-x-1.5 whitespace-nowrap">
              <button onclick="openTagSourceModal('${t.table_name}', '${srcName}', '${currentOracleZone}', ${metaStr})" class="px-2.5 py-1 bg-slate-100 hover:bg-slate-200 text-slate-700 border border-slate-300 rounded text-[11px] font-bold inline-flex items-center space-x-1 transition-colors">
                <i class="fa-solid fa-tag text-[10px]"></i>
                <span>Tag Source</span>
              </button>
              <button onclick="openAddMappingModalWithTable('${t.table_name}', '${t.owner}')" class="px-2.5 py-1 bg-indigo-50 hover:bg-indigo-100 text-indigo-700 border border-indigo-200 rounded text-[11px] font-bold inline-flex items-center space-x-1 transition-colors">
                <i class="fa-solid fa-plus text-[10px]"></i>
                <span>Map Columns</span>
              </button>
            </td>
          </tr>
        `;
      }).join('');
    }

    function populateSourceTableDropdown(cat) {
      const select = document.getElementById('map-table');
      if (!select) return;

      const curVal = select.value;
      const oracleTables = (cat && cat.tables) ? cat.tables : [];
      let optHtml = '';

      if (oracleTables.length > 0) {
        optHtml += '<optgroup label="Live Oracle Tables (from DB Connection)">';
        oracleTables.forEach(t => {
          const sel = (t.table_name === curVal) ? 'selected' : '';
          optHtml += `<option value="${t.table_name}" data-schema="${t.owner}" ${sel}>${t.owner}.${t.table_name} (${t.num_rows !== null ? t.num_rows : 0} rows)</option>`;
        });
        optHtml += '</optgroup>';
      }

      optHtml += `
        <optgroup label="East Zone Siebel CRM Tables (13 Tables)">
          <option value="S_ORDER">S_ORDER (Orders)</option>
          <option value="S_ORDER_X">S_ORDER_X (Order Extension)</option>
          <option value="S_ORDER_TYPE">S_ORDER_TYPE (Order Types)</option>
          <option value="S_ORG_EXT">S_ORG_EXT (Accounts)</option>
          <option value="S_ORG_EXT_X">S_ORG_EXT_X (Account Extension)</option>
          <option value="S_ORG_EXT_XM">S_ORG_EXT_XM (Account Multi-Value)</option>
          <option value="S_ASSET">S_ASSET (Installed Services)</option>
          <option value="S_ASSET_X">S_ASSET_X (Asset Extension)</option>
          <option value="S_ASSET_XA">S_ASSET_XA (Asset Attributes)</option>
          <option value="S_ASSET_XM">S_ASSET_XM (Asset Multi-Value)</option>
          <option value="S_ADDR_PER">S_ADDR_PER (Addresses)</option>
          <option value="S_CONTACT">S_CONTACT (Contacts / Persons)</option>
          <option value="S_SRV_REQ">S_SRV_REQ (Complaints / SR)</option>
        </optgroup>
        <optgroup label="North Zone SAP Master & Transaction Tables (12 Tables)">
          <option value="BUT000">BUT000 (BP Master)</option>
          <option value="IBIB">IBIB (Installed Base / Services)</option>
          <option value="CRMD_ORDERADM_H">CRMD_ORDERADM_H (Order Header)</option>
          <option value="CRMD_ORDERADM_I">CRMD_ORDERADM_I (Order Item)</option>
          <option value="CRMD_ORDER_INDEX">CRMD_ORDER_INDEX (Order Index)</option>
          <option value="CRM_JEST">CRM_JEST (Status Object)</option>
          <option value="TJ30T">TJ30T (Status Texts)</option>
          <option value="ADRC">ADRC (Addresses)</option>
          <option value="ADR2">ADR2 (Telephone numbers)</option>
          <option value="ADR6">ADR6 (E-mail addresses)</option>
          <option value="BUT021_FS">BUT021_FS (Address usage)</option>
          <option value="BUT050">BUT050 (BP Relationships)</option>
        </optgroup>
      `;
      select.innerHTML = optHtml;
      if (curVal) {
        select.value = curVal;
      }
    }

    async function onSourceTableChange(tableName) {
      const select = document.getElementById('map-table');
      const opt = select.options[select.selectedIndex];
      const schema = opt ? opt.getAttribute('data-schema') : '';
      const zone = document.getElementById('map-zone').value || 'NZ';

      const datalist = document.getElementById('oracle-cols-datalist');
      const hint = document.getElementById('col-oracle-hint');

      try {
        const url = `/api/oracle/columns?zone=${zone}&table=${tableName}${schema ? '&schema=' + schema : ''}`;
        const res = await fetch(url);
        if (res.ok) {
          const data = await res.json();
          if (data.columns && data.columns.length > 0) {
            datalist.innerHTML = data.columns.map(c => `
              <option value="${c.name}">${c.name} (${c.type}${c.is_pk ? ' - PK' : ''})</option>
            `).join('');
            if (hint) {
              hint.innerHTML = `<span class="text-emerald-600 font-semibold"><i class="fa-solid fa-check mr-1"></i>Loaded ${data.columns.length} columns directly from Oracle (${tableName})</span>`;
            }
            return;
          }
        }
      } catch (e) {
        console.warn('Could not fetch columns from Oracle:', e);
      }
      if (hint) {
        hint.innerHTML = '<span class="text-slate-400">Type or select a source column name</span>';
      }
    }

    function openAddMappingModalWithTable(tableName, schema) {
      openAddMappingModal();
      const select = document.getElementById('map-table');
      if (select) {
        select.value = tableName;
        onSourceTableChange(tableName);
      }
    }

    function renderMappingRulesTable(rules) {
      const tbody = document.getElementById('mapping-rules-body');
      if (!rules || rules.length === 0) {
        tbody.innerHTML = '<tr><td colspan="10" class="py-6 text-center text-slate-400 font-sans">No mapping rules configured</td></tr>';
        return;
      }
      tbody.innerHTML = rules.map((r, idx) => {
        const isEZ = r.zone === 'EZ';
        const zBadge = isEZ ? 
          '<span class="bg-purple-100 text-purple-800 text-[10px] font-bold px-1.5 py-0.5 rounded">EZ</span>' : 
          '<span class="bg-blue-100 text-blue-800 text-[10px] font-bold px-1.5 py-0.5 rounded">NZ</span>';
        const isSens = (liveData.sensitive_cols || []).some(s => (s.zone === r.zone || !s.zone || !r.zone) && s.src_table === r.src_table && s.src_column === r.src_column);
        return `
          <tr class="hover:bg-slate-50">
            <td class="py-2 px-3">${zBadge}</td>
            <td class="py-2 px-3 font-bold text-slate-900">${r.src_table}</td>
            <td class="py-2 px-3 font-bold text-blue-700">${r.src_column}</td>
            <td class="py-2 px-3 text-indigo-900 font-semibold">${r.target_entity}</td>
            <td class="py-2 px-3 text-slate-800">${r.target_field}</td>
            <td class="py-2 px-3"><span class="bg-slate-100 text-slate-700 text-[10px] font-bold px-1.5 py-0.5 rounded">${r.mapping_type || 'DIRECT'}</span></td>
            <td class="py-2 px-3 text-slate-500 text-[11px] font-sans">${r.transform_rule || '-'}</td>
            <td class="py-2 px-3"><span class="bg-emerald-100 text-emerald-800 text-[10px] font-bold px-1.5 py-0.5 rounded">${r.confidence || 'HIGH'}</span></td>
            <td class="py-2 px-3">
              ${isSens ? 
                '<span class="bg-rose-100 text-rose-800 text-[10px] font-bold px-1.5 py-0.5 rounded border border-rose-200">[REDACTED_SENSITIVE]</span>' : 
                '<span class="text-slate-400 text-[10px]">Normal</span>'}
            </td>
            <td class="py-2 px-3 text-right whitespace-nowrap space-x-1">
              <button onclick="editMapping(${idx})" class="px-2 py-1 bg-blue-50 hover:bg-blue-100 text-blue-700 rounded font-semibold text-[10px] transition-colors">
                <i class="fa-solid fa-pen-to-square"></i> Edit
              </button>
              <button onclick="confirmDeleteMapping(${idx})" class="px-2 py-1 bg-rose-50 hover:bg-rose-100 text-rose-700 rounded font-semibold text-[10px] transition-colors">
                <i class="fa-solid fa-trash"></i>
              </button>
            </td>
          </tr>
        `;
      }).join('');
    }

    function filterMappingRules() {
      if (!liveData || !liveData.mapping_rules) return;
      const q = (document.getElementById('mapping-search-input')?.value || '').toLowerCase();
      const tblFilter = document.getElementById('mapping-table-filter')?.value || '';
      const zoneFilter = document.getElementById('mapping-zone-filter')?.value || '';

      const filtered = liveData.mapping_rules.filter(r => {
        const matchesZone = !zoneFilter || (r.zone || 'NZ') === zoneFilter;
        const matchesTbl = !tblFilter || r.src_table === tblFilter;
        const matchesQ = !q || (
          (r.src_table || '').toLowerCase().includes(q) ||
          (r.src_column || '').toLowerCase().includes(q) ||
          (r.target_entity || '').toLowerCase().includes(q) ||
          (r.target_field || '').toLowerCase().includes(q) ||
          (r.transform_rule || '').toLowerCase().includes(q)
        );
        return matchesZone && matchesTbl && matchesQ;
      });
      renderMappingRulesTable(filtered);
    }
    }

    function renderSchemaAndDQ(data) {
      if (!data) return;
      const schemaBody = document.getElementById('schema-history-body');
      if (data.schema_history && data.schema_history.length > 0) {
        schemaBody.innerHTML = data.schema_history.map(s => `
          <tr class="hover:bg-slate-50">
            <td class="py-1.5 px-2 font-bold text-slate-800">${s.zone}.${s.src_table}</td>
            <td class="py-1.5 px-2 text-slate-700">${s.column_name}</td>
            <td class="py-1.5 px-2 text-slate-500">${s.oracle_type || '-'} &rarr; ${s.pg_type || '-'}</td>
            <td class="py-1.5 px-2"><span class="bg-slate-100 text-slate-700 px-1 py-0.5 rounded text-[10px] font-semibold">${s.change_type}</span></td>
            <td class="py-1.5 px-2 text-slate-400 text-[10px]">${fmtDate(s.detected_at)}</td>
          </tr>
        `).join('');
      }

      const dqBody = document.getElementById('dq-summary-body');
      if (data.dq_summary && data.dq_summary.length > 0) {
        dqBody.innerHTML = data.dq_summary.map(dq => `
          <tr class="hover:bg-slate-50">
            <td class="py-1.5 px-2 font-bold text-slate-800">${dq.zone}.${dq.src_table}</td>
            <td class="py-1.5 px-2 text-right text-slate-600">${fmtNum(dq.rows_fetched_total)}</td>
            <td class="py-1.5 px-2 text-right text-emerald-700 font-bold">${fmtNum(dq.rows_loaded_total)}</td>
            <td class="py-1.5 px-2 text-right text-sky-700">${fmtNum(dq.duplicates_skipped_total)}</td>
            <td class="py-1.5 px-2 text-right font-bold ${Number(dq.quarantine_pct) > 0 ? 'text-rose-600' : 'text-slate-400'}">${Number(dq.quarantine_pct).toFixed(2)}%</td>
          </tr>
        `).join('');
      }
    }

    // ===================================================================
    // REAL-TIME BRONZE DATA EXPLORER
    // ===================================================================
    function setupExplorerTableDropdown(data) {
      const select = document.getElementById('explorer-table-select');
      if (!select) return;
      if (!data || !data.sample_data) return;

      const tables = Object.keys(data.sample_data);
      if (tables.length === 0) return;

      if (!tables.includes(currentExplorerTable)) {
        currentExplorerTable = tables[0];
      }

      select.innerHTML = tables.map(t => `
        <option value="${t}" ${t === currentExplorerTable ? 'selected' : ''}>${t} (${data.sample_data[t].length} rows)</option>
      `).join('');

      renderDataExplorerTable();
    }

    function switchExplorerTable(tableName) {
      currentExplorerTable = tableName;
      renderDataExplorerTable();
    }

    function renderDataExplorerTable() {
      if (!liveData || !liveData.sample_data) return;
      const rows = liveData.sample_data[currentExplorerTable] || [];

      document.getElementById('explorer-table-title').innerHTML = `
        <i class="fa-solid fa-table-cells text-emerald-600"></i>
        <span>${currentExplorerTable}</span>
      `;
      document.getElementById('explorer-row-count-badge').textContent = `${rows.length} rows`;

      const thead = document.getElementById('explorer-table-headers');
      const tbody = document.getElementById('explorer-table-body');

      if (rows.length === 0) {
        thead.innerHTML = '<th class="py-2 px-3">Status</th>';
        tbody.innerHTML = '<tr><td class="py-6 text-center text-slate-400 font-sans">No records replicated yet for this table</td></tr>';
        return;
      }

      const cols = Object.keys(rows[0]);
      const dataCols = cols.filter(c => !c.startsWith('_'));
      const lineCols = cols.filter(c => c.startsWith('_'));
      const sortedCols = [...dataCols, ...lineCols];

      thead.innerHTML = `
        <th class="py-2 px-3 text-slate-400">#</th>
        ${sortedCols.map(c => `<th class="py-2 px-3 whitespace-nowrap ${c.startsWith('_') ? 'text-slate-400' : 'text-slate-700'}">${c}</th>`).join('')}
        <th class="py-2 px-3 text-right">Action</th>
      `;

      const q = (document.getElementById('explorer-search-input')?.value || '').toLowerCase();
      const filteredRows = q ? rows.filter(r => Object.values(r).some(v => String(v).toLowerCase().includes(q))) : rows;

      tbody.innerHTML = filteredRows.map((r, idx) => `
        <tr class="hover:bg-emerald-50/40 cursor-pointer" onclick="openRowDossier(${idx})">
          <td class="py-2 px-3 text-slate-400 font-bold">${idx + 1}</td>
          ${sortedCols.map(c => `
            <td class="py-2 px-3 whitespace-nowrap ${c.startsWith('_') ? 'text-slate-400 text-[10px]' : 'text-slate-800 font-medium'}">
              ${r[c] !== null && r[c] !== undefined ? String(r[c]) : '<span class="text-slate-300">null</span>'}
            </td>
          `).join('')}
          <td class="py-2 px-3 text-right whitespace-nowrap">
            <button onclick="event.stopPropagation(); openRowDossier(${idx})" class="px-2 py-0.5 bg-emerald-100 hover:bg-emerald-200 text-emerald-800 rounded font-semibold text-[10px]">
              Inspect
            </button>
          </td>
        </tr>
      `).join('');
    }

    function filterExplorerTable() {
      renderDataExplorerTable();
    }

    function openRowDossier(rowIdx) {
      if (!liveData || !liveData.sample_data) return;
      const rows = liveData.sample_data[currentExplorerTable] || [];
      const row = rows[rowIdx];
      if (!row) return;

      document.getElementById('dossier-table-name').textContent = currentExplorerTable;

      const lineage = document.getElementById('dossier-lineage');
      lineage.innerHTML = `
        <div><span class="text-slate-400">_source:</span> <span class="text-indigo-700 font-bold">${row._source || '-'}</span></div>
        <div><span class="text-slate-400">_src_table:</span> <span class="text-slate-800 font-bold">${row._src_table || '-'}</span></div>
        <div><span class="text-slate-400">_loaded_at:</span> <span class="text-emerald-700 font-bold">${row._loaded_at || '-'}</span></div>
        <div><span class="text-slate-400">_chunk_id:</span> <span class="text-slate-700">${row._chunk_id || '-'}</span></div>
        <div><span class="text-slate-400">_load_id:</span> <span class="text-slate-700">${row._load_id || '-'}</span></div>
        <div><span class="text-slate-400">_op:</span> <span class="text-purple-700 font-bold">${row._op || 'I'}</span></div>
      `;

      const attrBody = document.getElementById('dossier-attributes-body');
      attrBody.innerHTML = Object.entries(row).map(([k, v]) => `
        <tr class="hover:bg-slate-50">
          <td class="py-1.5 px-3 font-semibold text-slate-600 w-1/3">${k}</td>
          <td class="py-1.5 px-3 text-slate-900 font-mono break-all">${v !== null && v !== undefined ? String(v) : '<span class="text-slate-300">null</span>'}</td>
        </tr>
      `).join('');

      document.getElementById('dossier-drawer').classList.remove('translate-x-full');
    }

    async function fetchData() {
      try {
        const res = await fetch('/api/status');
        if (res.ok) {
          const data = await res.json();
          liveData = data;
          renderControlPlane(data);
          renderQuarantineConsole(data);
          renderMappingGuide(data);
          renderSchemaAndDQ(data);
          setupExplorerTableDropdown(data);
          document.getElementById('global-status-text').textContent = 'Live Database: Connected';
        }
      } catch (err) {
        if (liveData) {
          renderControlPlane(liveData);
          renderQuarantineConsole(liveData);
          renderMappingGuide(liveData);
          renderSchemaAndDQ(liveData);
          setupExplorerTableDropdown(liveData);
        }
        document.getElementById('global-status-text').textContent = 'Live Mode: Active';
      }
    }

    window.addEventListener('DOMContentLoaded', () => {
      if (liveData) {
        renderControlPlane(liveData);
        renderQuarantineConsole(liveData);
        renderMappingGuide(liveData);
        renderSchemaAndDQ(liveData);
        setupExplorerTableDropdown(liveData);
      }
      fetchData();
      setInterval(fetchData, 10000);
    });
  </script>
</body>
</html>
"""


class DashboardRequestHandler(BaseHTTPRequestHandler):

    def log_message(self, format, *args):
        # Silence default access logs to stdout for quiet operation
        pass

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path

        if path in ("/", "/index.html"):
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            try:
                data = get_db_data()
                body_json = json.dumps(data, default=json_serializer)
                content = HTML_TEMPLATE.replace(
                    "let liveData = window.INITIAL_DATA || null;",
                    f"window.INITIAL_DATA = {body_json};\n    let liveData = window.INITIAL_DATA;"
                )
            except Exception:
                content = HTML_TEMPLATE
            self.wfile.write(content.encode("utf-8"))
            return

        if path == "/api/status":
            try:
                data = get_db_data()
                body = json.dumps(data, default=json_serializer).encode("utf-8")
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", "application/json")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(body)
            except Exception as e:
                err_data = {"error": str(e)}
                body = json.dumps(err_data).encode("utf-8")
                self.send_response(HTTPStatus.INTERNAL_SERVER_ERROR)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(body)
            return

        if path == "/api/oracle/tables":
            try:
                qs = parse_qs(parsed.query)
                zone = qs.get("zone", ["NZ"])[0]
                data = get_oracle_tables(zone)
                body = json.dumps(data, default=json_serializer).encode("utf-8")
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", "application/json")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(body)
            except Exception as e:
                err_data = {"error": str(e), "tables": [], "connected": False}
                body = json.dumps(err_data).encode("utf-8")
                self.send_response(HTTPStatus.INTERNAL_SERVER_ERROR)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(body)
            return

        if path == "/api/oracle/columns":
            try:
                qs = parse_qs(parsed.query)
                zone = qs.get("zone", ["NZ"])[0]
                schema = qs.get("schema", [None])[0]
                table = qs.get("table", [None])[0]
                data = get_oracle_columns(zone, schema, table)
                body = json.dumps(data, default=json_serializer).encode("utf-8")
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", "application/json")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(body)
            except Exception as e:
                err_data = {"error": str(e), "columns": []}
                body = json.dumps(err_data).encode("utf-8")
                self.send_response(HTTPStatus.INTERNAL_SERVER_ERROR)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(body)
            return

        self.send_response(HTTPStatus.NOT_FOUND)
        self.end_headers()

    def do_POST(self):
        parsed = urlparse(self.path)
        path = parsed.path

        content_length = int(self.headers.get("Content-Length", 0))
        body_raw = self.rfile.read(content_length)

        try:
            payload = json.loads(body_raw.decode("utf-8")) if body_raw else {}
        except Exception as ex:
            self.send_response(HTTPStatus.BAD_REQUEST)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"error": f"Invalid JSON payload: {ex}"}).encode())
            return

        if path == "/api/mapping":
            import mapping
            try:
                res = mapping.add_or_update_mapping(
                    zone=payload.get("zone", "NZ"),
                    src_table=payload.get("src_table", ""),
                    src_column=payload.get("src_column", ""),
                    target_entity=payload.get("target_entity", ""),
                    target_field=payload.get("target_field", ""),
                    role=payload.get("role", "DIRECT"),
                    mapping_type=payload.get("mapping_type", "DIRECT"),
                    transform_rule=payload.get("transform_rule", "DIRECT"),
                    confidence=payload.get("confidence", "HIGH"),
                    is_sensitive=bool(payload.get("is_sensitive", False)),
                    sensitive_reason=payload.get("sensitive_reason", ""),
                    notes=payload.get("notes", "")
                )
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", "application/json")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(json.dumps(res).encode())
            except Exception as ex:
                self.send_response(HTTPStatus.INTERNAL_SERVER_ERROR)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"ok": False, "error": str(ex)}).encode())
            return

        if path == "/api/mapping/delete":
            import mapping
            try:
                res = mapping.delete_mapping(
                    zone=payload.get("zone", "NZ"),
                    src_table=payload.get("src_table", ""),
                    src_column=payload.get("src_column", ""),
                    target_entity=payload.get("target_entity", ""),
                    target_field=payload.get("target_field", "")
                )
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", "application/json")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(json.dumps(res).encode())
            except Exception as ex:
                self.send_response(HTTPStatus.INTERNAL_SERVER_ERROR)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"ok": False, "error": str(ex)}).encode())
            return

        if path == "/api/table/tag":
            src_table = payload.get("src_table") or payload.get("table", "")
            source_system = payload.get("source_system") or payload.get("source", "")
            source_metadata = payload.get("source_metadata") or payload.get("meta")
            zone = payload.get("zone")
            if not src_table or not source_system:
                self.send_response(HTTPStatus.BAD_REQUEST)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"ok": False, "error": "table and source are required"}).encode())
                return
            ctl = bs.connect()
            try:
                if isinstance(source_metadata, str) and source_metadata.strip():
                    try:
                        source_metadata = json.loads(source_metadata)
                    except Exception:
                        source_metadata = {"note": source_metadata}
                meta_jsonb = Jsonb(source_metadata) if source_metadata is not None else None

                query = "UPDATE bronze_ctl.table_config SET source_system = %s, source_metadata = COALESCE(%s, source_metadata) WHERE UPPER(src_table) = UPPER(%s)"
                params = [source_system, meta_jsonb, src_table.strip()]
                if zone:
                    query += " AND zone = %s"
                    params.append(zone)
                query += " RETURNING zone, src_schema, src_table, source_system, source_metadata;"
                updated = ctl.execute(query, tuple(params)).fetchall()
                ctl.commit()
                if updated:
                    res = {
                        "ok": True,
                        "message": f"Successfully marked table '{src_table}' with source '{source_system}'",
                        "updated": [
                            {"zone": u[0], "schema": u[1], "table": u[2], "source": u[3], "metadata": u[4]}
                            for u in updated
                        ]
                    }
                else:
                    res = {"ok": False, "error": f"Table '{src_table}' not found in bronze_ctl.table_config"}
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", "application/json")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(json.dumps(res, default=str).encode())
            except Exception as ex:
                self.send_response(HTTPStatus.INTERNAL_SERVER_ERROR)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"ok": False, "error": str(ex)}).encode())
            finally:
                ctl.close()
            return

        if path == "/api/ingest":
            try:
                itype = payload.get("type", "parquet")
                file_path = payload.get("file")
                jdbc_url = payload.get("url")
                src_table = payload.get("table")
                src_schema = payload.get("schema")
                zone = payload.get("zone", "NZ")
                source_system = payload.get("source")
                mode = payload.get("mode", "merge")
                pk = payload.get("pk")
                watermark = payload.get("watermark")
                delimiter = payload.get("delimiter", ",")
                meta = payload.get("meta")
                arraysize = int(payload.get("arraysize", 50000))

                class IngestArgs:
                    pass

                args = IngestArgs()
                args.type = itype
                args.file = file_path
                args.url = jdbc_url
                args.table = src_table
                args.schema = src_schema
                args.zone = zone
                args.source = source_system
                args.mode = mode
                args.pk = pk
                args.watermark = watermark
                args.delimiter = delimiter
                args.meta = meta
                args.arraysize = arraysize
                args.config = "config.toml"
                args.env_file = None
                args.return_dict = True

                result = bs.cmd_ingest(args)
                if isinstance(result, dict) and result.get("ok"):
                    self.send_response(HTTPStatus.OK)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Access-Control-Allow-Origin", "*")
                    self.end_headers()
                    self.wfile.write(json.dumps(result, default=str).encode())
                else:
                    self.send_response(HTTPStatus.BAD_REQUEST)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Access-Control-Allow-Origin", "*")
                    self.end_headers()
                    self.wfile.write(json.dumps({"ok": False, "error": f"Ingestion failed or returned non-zero ({result})"}).encode())
            except Exception as ex:
                self.send_response(HTTPStatus.INTERNAL_SERVER_ERROR)
                self.send_header("Content-Type", "application/json")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(json.dumps({"ok": False, "error": str(ex)}).encode())
            return

        self.send_response(HTTPStatus.NOT_FOUND)
        self.end_headers()


def run_server(host="0.0.0.0", port=8080):
    server_address = (host, port)
    httpd = HTTPServer(server_address, DashboardRequestHandler)
    url = f"http://{'localhost' if host in ('0.0.0.0', '127.0.0.1') else host}:{port}"
    print(f"\n==================================================================")
    print(f" BSNL CFA Data Lake Real-Time Dashboard running at:")
    print(f"   --> {url}")
    print(f"==================================================================")
    print(f"Press Ctrl+C to stop the dashboard server.\n")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nStopping dashboard server...")
    finally:
        httpd.server_close()


def main():
    parser = argparse.ArgumentParser(description="Bronze Loader Web Dashboard")
    parser.add_argument("--host", default="0.0.0.0", help="Host to bind (default: 0.0.0.0)")
    parser.add_argument("--port", type=int, default=8080, help="Port to listen on (default: 8080)")
    parser.add_argument("--env-file", default=None, help="Path to .env credentials file")
    args = parser.parse_args()

    if args.env_file:
        bs.load_env(args.env_file, override=True)
    else:
        bs.load_env()

    run_server(args.host, args.port)


if __name__ == "__main__":
    main()
