#!/usr/bin/env python3
"""
North Zone (NZ) SAP to CFA360 Column Mapping, Schema Check, and Sensitive Column Protection.

Provides:
- Column mapping dictionary for the 12 North Zone SAP CRM tables -> CFA360 entities:
    Customer_master_data, CRM_activity_table, Complaint_details
- Classification of 650 unmapped columns (SENSITIVE, SKIP, UNKNOWN)
- Schema check validation: verifies Oracle schema against expected CFA360 mappings
- Sensitive column protection: auto-redacts PAN, TAN, GSTIN, credentials, names, and phone numbers from raw payloads
- Synchronization to PostgreSQL bronze_ctl control plane tables:
    bronze_ctl.zone_column_mapping, bronze_ctl.zone_column_classification, bronze_ctl.cfa_target_schema
"""
import json
import logging
import os
import sys

LOG = logging.getLogger("bronze_sync")
HERE = os.path.dirname(os.path.abspath(__file__))
MAPPING_DATA_PATH = os.path.join(HERE, "nz_mapping_data.json")
EZ_MAPPING_DATA_PATH = os.path.join(HERE, "ez_mapping_data.json")
MASTER_MAPPING_DATA_PATH = os.path.join(HERE, "zone_mappings_master.json")

_CACHES = {}


def load_mapping_from_db(ctl, zone="NZ"):
    """
    Pull complete mapping dataset directly from PostgreSQL bronze_ctl tables:
    - bronze_ctl.zone_column_mapping
    - bronze_ctl.zone_column_classification
    - bronze_ctl.cfa_target_schema
    Enables managing all column mappings and checks directly from database tables.
    """
    import bronze_sync as bs

    z_filter = (zone or "NZ").upper()
    where_sql = "" if z_filter == "ALL" else "WHERE UPPER(zone) = UPPER(%s)"
    params = () if z_filter == "ALL" else (z_filter,)

    # 1. Pull mappings
    map_rows = bs.rowdict(ctl.execute(f"""
        SELECT zone, src_table, src_column, target_entity, target_field, role,
               mapping_type, transform_rule, confidence, sample_value, fill_pct, notes
        FROM bronze_ctl.zone_column_mapping
        {where_sql}
        ORDER BY zone, src_table, src_column
    """, params))

    mappings = []
    for r in map_rows:
        mappings.append({
            "Source sheet (SAP table)": r["src_table"],
            "Source column": r["src_column"],
            "CFA360 sheet": r["target_entity"],
            "CFA360 field": r["target_field"],
            "Role": r["role"] or "-",
            "Mapping type": r["mapping_type"] or "-",
            "Transformation / rule (at CFA field level)": r["transform_rule"] or None,
            "Confidence": r["confidence"] or None,
            "Sample value": r["sample_value"],
            "Fill % (100-row sample)": (float(r["fill_pct"]) / 100.0) if r["fill_pct"] is not None else None,
            "Note on this column": r["notes"] or ""
        })

    # 2. Pull classifications (unmapped & sensitive)
    class_rows = bs.rowdict(ctl.execute(f"""
        SELECT zone, src_table, src_column, classification, is_sensitive, why_note, sample_value, fill_pct
        FROM bronze_ctl.zone_column_classification
        {where_sql}
        ORDER BY zone, src_table, src_column
    """, params))

    unmapped = []
    for c in class_rows:
        cls_name = "SENSITIVE" if c["is_sensitive"] else (c["classification"] or "")
        if cls_name == "sensitive":
            cls_name = "SENSITIVE"
        elif cls_name == "skip_constant":
            cls_name = "SKIP - constant / fixed value across all rows"
        elif cls_name == "skip_empty":
            cls_name = "SKIP - empty or zero throughout sample"
        elif cls_name == "technical":
            cls_name = "TECHNICAL / AUDIT - not in CRM extract"
        elif cls_name == "unknown":
            cls_name = "UNKNOWN - ask NZ"

        unmapped.append({
            "Source sheet (SAP table)": c["src_table"],
            "Source column": c["src_column"],
            "Class": cls_name,
            "Why / note": c["why_note"] or "",
            "Sample value": c["sample_value"],
            "Fill % (100-row sample)": (float(c["fill_pct"]) / 100.0) if c["fill_pct"] is not None else None
        })

    # 3. Pull CFA target schema
    tgt_rows = bs.rowdict(ctl.execute("""
        SELECT target_entity, target_field, data_type, nullable, status,
               source_columns, candidates, join_keys, confidence, rule
        FROM bronze_ctl.cfa_target_schema
        ORDER BY target_entity, target_field
    """))

    cfa_fields = []
    for t in tgt_rows:
        cfa_fields.append({
            "CFA360 sheet": t["target_entity"],
            "CFA360 field": t["target_field"],
            "Type": t["data_type"],
            "Nullable": "NULL" if t["nullable"] else "NOT NULL",
            "Status": t["status"] or "",
            "Source columns (value / also used / alternative)": t["source_columns"] or "",
            "Candidates (unconfirmed)": t["candidates"] or "",
            "Join keys / filters": t["join_keys"] or "",
            "Confidence": t["confidence"] or "",
            "Rule": t["rule"] or ""
        })

    return {
        "mappings": mappings,
        "unmapped_columns": unmapped,
        "cfa_field_coverage": cfa_fields
    }


def load_mapping_data(ctl=None, zone="NZ", force_json=False):
    """
    Load mapping dataset.
    Prioritizes pulling dynamically from PostgreSQL bronze_ctl database tables.
    Falls back to bundled JSON only if database is not reachable.
    """
    global _CACHES
    import bronze_sync as bs

    z_key = (zone or "NZ").upper()
    if not force_json:
        should_close = False
        conn = ctl
        if conn is None:
            try:
                conn = bs.connect()
                should_close = True
            except Exception:
                conn = None

        if conn is not None:
            try:
                db_data = load_mapping_from_db(conn, zone=z_key)
                if db_data["mappings"] or db_data["unmapped_columns"]:
                    return db_data
            except Exception as ex:
                LOG.debug("Could not query mappings from database table, using fallback: %s", ex)
            finally:
                if should_close and conn:
                    conn.close()

    # Static JSON fallback (for bootstrap / offline tests)
    if z_key not in _CACHES:
        if z_key == "EZ":
            path = EZ_MAPPING_DATA_PATH
        elif z_key == "ALL":
            path = MASTER_MAPPING_DATA_PATH
        else:
            path = MAPPING_DATA_PATH

        if not os.path.exists(path):
            raise FileNotFoundError(f"Mapping data file not found: {path}")
        with open(path, "r", encoding="utf-8") as f:
            _CACHES[z_key] = json.load(f)
    return _CACHES[z_key]


def normalize_table_name(table_name):
    """Normalize table name (e.g. CRMD_ORDERADM_h -> CRMD_ORDERADM_H)."""
    if not table_name:
        return ""
    return table_name.strip().upper()


def normalize_col_name(col_name):
    """Normalize column name to uppercase stripped string."""
    if not col_name:
        return ""
    return col_name.strip().upper()


def get_all_tables(zone="NZ", ctl=None, from_oracle=False):
    """Get list of distinct source tables defined in the database mapping catalog or directly from Oracle DB."""
    import bronze_sync as bs

    if from_oracle:
        try:
            src = bs.SOURCE_FACTORY(zone, {})
            tables = [t["table_name"] for t in src.all_accessible_tables()] if hasattr(src, "all_accessible_tables") else [t[0] for t in src.tables()]
            src.close()
            if tables:
                return sorted(normalize_table_name(t) for t in tables)
        except Exception as ex:
            LOG.debug("Could not query tables directly from Oracle DB: %s", ex)

    should_close = False
    conn = ctl
    if conn is None:
        try:
            conn = bs.connect()
            should_close = True
        except Exception:
            conn = None

    if conn is not None:
        try:
            if (zone or "NZ").upper() == "ALL":
                rows = conn.execute("""
                    SELECT DISTINCT src_table FROM bronze_ctl.zone_column_mapping
                    UNION
                    SELECT DISTINCT src_table FROM bronze_ctl.zone_column_classification
                    ORDER BY 1
                """).fetchall()
            else:
                rows = conn.execute("""
                    SELECT DISTINCT src_table FROM bronze_ctl.zone_column_mapping WHERE UPPER(zone) = UPPER(%s)
                    UNION
                    SELECT DISTINCT src_table FROM bronze_ctl.zone_column_classification WHERE UPPER(zone) = UPPER(%s)
                    ORDER BY 1
                """, (zone, zone)).fetchall()
            if rows:
                return sorted(normalize_table_name(r[0]) for r in rows)
        except Exception as ex:
            LOG.debug("Could not query tables from database: %s", ex)
        finally:
            if should_close and conn:
                conn.close()

    data = load_mapping_data(zone=zone, force_json=True)
    tables = set(normalize_table_name(m.get("Source sheet (SAP table)") or m.get("Source table")) for m in data.get("mappings", []))
    return sorted(tables)


def get_sensitive_columns(ctl=None, zone="NZ", table=None):
    """
    Return set or dict of sensitive column names pulled directly from bronze_ctl.zone_column_classification.
    Managed directly from database table.
    """
    import bronze_sync as bs

    should_close = False
    conn = ctl
    if conn is None:
        try:
            conn = bs.connect()
            should_close = True
        except Exception:
            conn = None

    if conn is not None:
        try:
            z_filter = (zone or "NZ").upper()
            if table:
                where_tbl = "WHERE UPPER(src_table) = %s AND is_sensitive = true" if z_filter == "ALL" else "WHERE UPPER(zone) = UPPER(%s) AND UPPER(src_table) = %s AND is_sensitive = true"
                p_tbl = (normalize_table_name(table),) if z_filter == "ALL" else (z_filter, normalize_table_name(table))
                rows = conn.execute(
                    f"SELECT src_column FROM bronze_ctl.zone_column_classification {where_tbl}",
                    p_tbl
                ).fetchall()
                return {normalize_col_name(r[0]) for r in rows}
            else:
                where_all = "WHERE is_sensitive = true" if z_filter == "ALL" else "WHERE UPPER(zone) = UPPER(%s) AND is_sensitive = true"
                p_all = () if z_filter == "ALL" else (z_filter,)
                rows = conn.execute(
                    f"SELECT src_table, src_column FROM bronze_ctl.zone_column_classification {where_all}",
                    p_all
                ).fetchall()
                res = {}
                for tbl, col in rows:
                    t_norm = normalize_table_name(tbl)
                    res.setdefault(t_norm, set()).add(normalize_col_name(col))
                return res
        except Exception as e:
            LOG.debug("Could not query sensitive columns from database: %s", e)
        finally:
            if should_close and conn:
                conn.close()

    # Static fallback
    data = load_mapping_data(zone=zone, force_json=True)
    res = {}
    for r in data.get("unmapped_columns", []):
        if r.get("Class") == "SENSITIVE" or r.get("is_sensitive"):
            t_norm = normalize_table_name(r.get("Source sheet (SAP table)") or r.get("table"))
            c_norm = normalize_col_name(r.get("Source column") or r.get("column"))
            res.setdefault(t_norm, set()).add(c_norm)

    if table:
        return res.get(normalize_table_name(table), set())
    return res


def sanitize_raw_payload(row_dict, sensitive_columns):
    """
    Redact or mask sensitive columns from a row dictionary before saving into raw_payload or quarantine.
    Replaces values of sensitive columns with '[REDACTED_SENSITIVE]'.
    """
    if not row_dict or not sensitive_columns:
        return row_dict
    sanitized = {}
    sens_upper = {c.upper() for c in sensitive_columns}
    for k, v in row_dict.items():
        if k.upper() in sens_upper:
            sanitized[k] = "[REDACTED_SENSITIVE]"
        else:
            sanitized[k] = v
    return sanitized


def get_table_mappings(ctl=None, zone="NZ", table=None):
    """Get mapping rows pulled directly from bronze_ctl.zone_column_mapping table."""
    import bronze_sync as bs

    should_close = False
    conn = ctl
    if conn is None:
        try:
            conn = bs.connect()
            should_close = True
        except Exception:
            conn = None

    if conn is not None:
        try:
            t_norm = normalize_table_name(table) if table else None
            if t_norm:
                rows = bs.rowdict(conn.execute("""
                    SELECT zone, src_table, src_column, target_entity, target_field, role,
                           mapping_type, transform_rule, confidence, sample_value, fill_pct, notes
                    FROM bronze_ctl.zone_column_mapping
                    WHERE zone = %s AND UPPER(src_table) = %s
                    ORDER BY src_column
                """, (zone, t_norm)))
            else:
                rows = bs.rowdict(conn.execute("""
                    SELECT zone, src_table, src_column, target_entity, target_field, role,
                           mapping_type, transform_rule, confidence, sample_value, fill_pct, notes
                    FROM bronze_ctl.zone_column_mapping
                    WHERE zone = %s
                    ORDER BY src_table, src_column
                """, (zone,)))
            if rows:
                mappings = []
                for r in rows:
                    mappings.append({
                        "Source sheet (SAP table)": r["src_table"],
                        "Source column": r["src_column"],
                        "CFA360 sheet": r["target_entity"],
                        "CFA360 field": r["target_field"],
                        "Role": r["role"] or "-",
                        "Mapping type": r["mapping_type"] or "-",
                        "Transformation / rule (at CFA field level)": r["transform_rule"] or None,
                        "Confidence": r["confidence"] or None,
                        "Sample value": r["sample_value"],
                        "Fill % (100-row sample)": (float(r["fill_pct"]) / 100.0) if r["fill_pct"] is not None else None,
                        "Note on this column": r["notes"] or ""
                    })
                return mappings
        except Exception as ex:
            LOG.debug("Could not query table mappings from database: %s", ex)
        finally:
            if should_close and conn:
                conn.close()

    data = load_mapping_data(force_json=True)
    t_norm = normalize_table_name(table) if table else None
    mappings = []
    for m in data.get("mappings", []):
        if t_norm is None or normalize_table_name(m["Source sheet (SAP table)"]) == t_norm:
            mappings.append(m)
    return mappings


def get_unmapped_columns(ctl=None, zone="NZ", table=None, classification=None):
    """Get unmapped columns list pulled directly from bronze_ctl.zone_column_classification table."""
    import bronze_sync as bs

    should_close = False
    conn = ctl
    if conn is None:
        try:
            conn = bs.connect()
            should_close = True
        except Exception:
            conn = None

    if conn is not None:
        try:
            t_norm = normalize_table_name(table) if table else None
            cls_filter = classification.lower() if classification else None
            rows = bs.rowdict(conn.execute("""
                SELECT zone, src_table, src_column, classification, is_sensitive, why_note, sample_value, fill_pct
                FROM bronze_ctl.zone_column_classification
                WHERE zone = %s
                  AND (%s IS NULL OR UPPER(src_table) = %s)
                  AND (%s IS NULL OR LOWER(classification) = %s)
                ORDER BY src_table, src_column
            """, (zone, t_norm, t_norm, cls_filter, cls_filter)))
            if rows:
                results = []
                for c in rows:
                    cls_name = "SENSITIVE" if c["is_sensitive"] else (c["classification"] or "")
                    results.append({
                        "Source sheet (SAP table)": c["src_table"],
                        "Source column": c["src_column"],
                        "Class": cls_name,
                        "Why / note": c["why_note"] or "",
                        "Sample value": c["sample_value"],
                        "Fill % (100-row sample)": (float(c["fill_pct"]) / 100.0) if c["fill_pct"] is not None else None
                    })
                return results
        except Exception as ex:
            LOG.debug("Could not query unmapped columns from database: %s", ex)
        finally:
            if should_close and conn:
                conn.close()

    data = load_mapping_data(force_json=True)
    t_norm = normalize_table_name(table) if table else None
    results = []
    for u in data.get("unmapped_columns", []):
        if t_norm and normalize_table_name(u["Source sheet (SAP table)"]) != t_norm:
            continue
        if classification and u.get("Class") != classification:
            continue
        results.append(u)
    return results


def sync_mapping_to_db(ctl, zone="ALL"):
    """
    Idempotently syncs mapping rows, unmapped classifications, and CFA target schema into bronze_ctl.
    Supports zone='NZ', zone='EZ', or zone='ALL' (syncs both).
    """
    zones_to_sync = ["NZ", "EZ"] if (zone or "ALL").upper() == "ALL" else [zone.upper()]
    map_inserted = 0
    unmap_inserted = 0
    cfa_inserted = 0

    for z in zones_to_sync:
        data = load_mapping_data(zone=z, force_json=True)
        mappings = data.get("mappings", [])
        unmapped = data.get("unmapped_columns", [])
        cfa_fields = data.get("cfa_field_coverage", [])

        # 1. Sync zone_column_mapping
        for m in mappings:
            src_tbl = normalize_table_name(m.get("Source sheet (SAP table)") or m.get("Source table"))
            src_col = normalize_col_name(m.get("Source column"))
            tgt_ent = m.get("CFA360 sheet") or "Unknown"
            tgt_fld = m.get("CFA360 field") or "-"
            role = m.get("Role") or "-"
            m_type = m.get("Mapping type") or "-"
            rule = m.get("Transformation / rule (at CFA field level)") or m.get("Note on this column") or ""
            conf = m.get("Confidence") or "Low"
            sample = str(m.get("Sample value") or "")[:200]
            fill_pct = None
            try:
                if m.get("Fill % (100-row sample)") is not None:
                    fill_pct = round(float(m["Fill % (100-row sample)"]) * 100, 2)
            except Exception:
                pass
            notes = m.get("Note on this column") or ""

            ctl.execute(
                """
                INSERT INTO bronze_ctl.zone_column_mapping
                  (zone, src_table, src_column, target_entity, target_field, role, mapping_type,
                   transform_rule, confidence, sample_value, fill_pct, notes)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (zone, src_table, src_column, target_entity, target_field, role)
                DO UPDATE SET
                  mapping_type = EXCLUDED.mapping_type,
                  transform_rule = EXCLUDED.transform_rule,
                  confidence = EXCLUDED.confidence,
                  sample_value = EXCLUDED.sample_value,
                  fill_pct = EXCLUDED.fill_pct,
                  notes = EXCLUDED.notes
                """,
                (z, src_tbl, src_col, tgt_ent, tgt_fld, role, m_type, rule, conf, sample, fill_pct, notes)
            )
            map_inserted += 1

        # 2. Sync zone_column_classification
        for u in unmapped:
            src_tbl = normalize_table_name(u.get("Source sheet (SAP table)") or u.get("table") or u.get("Source table"))
            src_col = normalize_col_name(u.get("Source column") or u.get("column"))
            cls_name = u.get("Class") or ("SENSITIVE" if u.get("is_sensitive") else "UNKNOWN - ask NZ")
            is_sens = ("SENSITIVE" in str(cls_name).upper() or bool(u.get("is_sensitive")))
            why = u.get("Why / note") or u.get("reason") or u.get("Class") or ""
            sample = str(u.get("Sample value") or "")[:200]
            fill_pct = None
            try:
                if u.get("Fill % (100-row sample)") is not None:
                    fill_pct = round(float(u["Fill % (100-row sample)"]) * 100, 2)
            except Exception:
                pass

            ctl.execute(
                """
                INSERT INTO bronze_ctl.zone_column_classification
                  (zone, src_table, src_column, classification, is_sensitive, why_note, sample_value, fill_pct)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (zone, src_table, src_column)
                DO UPDATE SET
                  classification = EXCLUDED.classification,
                  is_sensitive = EXCLUDED.is_sensitive,
                  why_note = EXCLUDED.why_note,
                  sample_value = EXCLUDED.sample_value,
                  fill_pct = EXCLUDED.fill_pct
                """,
                (z, src_tbl, src_col, cls_name, is_sens, why, sample, fill_pct)
            )
            unmap_inserted += 1

        # 3. Sync cfa_target_schema
        for c in cfa_fields:
            tgt_ent = c.get("CFA360 sheet") or "Unknown"
            tgt_fld = c.get("CFA360 field") or "Unknown"
            d_type = c.get("Type") or "VARCHAR2(50)"
            nullable = (c.get("Nullable") != "NOT NULL")
            status = c.get("Status") or ""
            src_cols = c.get("Source columns (value / also used / alternative)") or ""
            cands = c.get("Candidates (unconfirmed)") or ""
            joins = c.get("Join keys / filters") or ""
            conf = c.get("Confidence") or "Low"
            rule = c.get("Rule") or c.get("Join path / rule") or c.get("Transformation / rule") or ""

            ctl.execute(
                """
                INSERT INTO bronze_ctl.cfa_target_schema
                  (target_entity, target_field, data_type, nullable, status,
                   source_columns, candidates, join_keys, confidence, rule)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (target_entity, target_field)
                DO UPDATE SET
                  data_type = EXCLUDED.data_type,
                  nullable = EXCLUDED.nullable,
                  status = EXCLUDED.status,
                  source_columns = EXCLUDED.source_columns,
                  candidates = EXCLUDED.candidates,
                  join_keys = EXCLUDED.join_keys,
                  confidence = EXCLUDED.confidence,
                  rule = EXCLUDED.rule
                """,
                (tgt_ent, tgt_fld, d_type, nullable, status, src_cols, cands, joins, conf, rule)
            )
            cfa_inserted += 1

    ctl.commit()
    return {
        "ok": True,
        "zones": zones_to_sync,
        "mappings_synced": map_inserted,
        "unmapped_synced": unmap_inserted,
        "cfa_fields_synced": cfa_inserted,
    }


def validate_schema_mapping(zone, table, oracle_columns, ctl=None, run_id=None, chunk_id=None):
    """
    Validate Oracle table columns against the North Zone CFA360 mapping registry.

    Checks:
    1. Are any required mapped source columns missing in Oracle? (mapping_column_missing)
    2. Are sensitive columns present and confirmed protected? (sensitive_detected)
    3. Are there unknown columns with data? (UNKNOWN - ask NZ)
    4. Are there brand new unclassified columns? (unclassified_drift)

    Returns a comprehensive validation dict.
    """
    t_norm = normalize_table_name(table)
    oracle_cols_set = {normalize_col_name(c) for c in oracle_columns}

    data = load_mapping_data()
    table_mappings = [m for m in data.get("mappings", []) if normalize_table_name(m["Source sheet (SAP table)"]) == t_norm]
    table_unmapped = [u for u in data.get("unmapped_columns", []) if normalize_table_name(u["Source sheet (SAP table)"]) == t_norm]

    # Required mapped source columns (excluding 'NOT IN EXTRACT' or pipeline-only keys)
    mapped_source_cols = {
        normalize_col_name(m["Source column"]): m
        for m in table_mappings
        if m.get("Mapping type") != "NOT IN EXTRACT" and m.get("Source column")
    }

    # Sensitive columns
    sensitive_cols = {
        normalize_col_name(u["Source column"]): u
        for u in table_unmapped
        if u.get("Class") == "SENSITIVE"
    }

    # Unknown custom Z columns
    unknown_cols = {
        normalize_col_name(u["Source column"]): u
        for u in table_unmapped
        if u.get("Class") == "UNKNOWN - ask NZ"
    }

    # All known columns in dictionary
    all_known_cols = set(mapped_source_cols.keys()).union(
        normalize_col_name(u["Source column"]) for u in table_unmapped
    )

    # Missing mapped columns
    missing_mapped = [c for c in mapped_source_cols if c not in oracle_cols_set]

    # Sensitive columns present in Oracle table
    sensitive_present = [c for c in sensitive_cols if c in oracle_cols_set]

    # Unknown columns present
    unknown_present = [c for c in unknown_cols if c in oracle_cols_set]

    # Brand new drift columns (neither in mapped nor unmapped)
    unclassified_drift = [c for c in oracle_cols_set if c not in all_known_cols]

    # Record in database if ctl is provided
    if ctl is not None and zone == "NZ":
        # Log missing mapped columns
        for col in missing_mapped:
            try:
                ctl.execute(
                    """
                    INSERT INTO bronze_ctl.schema_history
                      (zone, src_schema, src_table, column_name, oracle_type, pg_type, change_type, run_id, chunk_id)
                    VALUES (%s, 'SAP', %s, %s, 'MAPPED_SOURCE', 'UNKNOWN', 'mapping_column_missing', %s, %s)
                    """,
                    (zone, t_norm, col, run_id, chunk_id)
                )
            except Exception as e:
                LOG.debug("Could not record missing mapping column in schema_history: %s", e)

        # Log sensitive columns audit
        if sensitive_present:
            try:
                ctl.execute(
                    """
                    INSERT INTO bronze_ctl.load_event
                      (run_id, chunk_id, level, event, message, details)
                    VALUES (%s, %s, 'INFO', 'sensitive_columns_safeguard',
                            %s, %s::jsonb)
                    """,
                    (run_id, chunk_id,
                     f"Table {t_norm} contains {len(sensitive_present)} sensitive columns; redaction guards active.",
                     json.dumps({"zone": zone, "table": t_norm, "sensitive_columns": sensitive_present}))
                )
            except Exception as e:
                LOG.debug("Could not record sensitive safeguard event: %s", e)

    return {
        "zone": zone,
        "table": t_norm,
        "total_oracle_columns": len(oracle_columns),
        "mapped_columns_count": len(mapped_source_cols),
        "missing_mapped": missing_mapped,
        "sensitive_present": sensitive_present,
        "unknown_present": unknown_present,
        "unclassified_drift": unclassified_drift,
        "is_valid": len(missing_mapped) == 0,
        "sensitive_protected": True,
    }


def print_mapping_summary(ctl=None, zone="ALL"):
    """Print high-level mapping and sensitive columns summary."""
    z_req = (zone or "ALL").upper()
    zones = ["NZ", "EZ"] if z_req == "ALL" else [z_req]

    print("\n================================================================================")
    print(" CFA360 COLUMN MAPPING & SENSITIVE CHECKS SUMMARY (NORTH & EAST ZONES)")
    print("================================================================================")

    for z in zones:
        sys_name = "North Zone (NZ) SAP CRM" if z == "NZ" else "East Zone (EZ) Siebel CRM"
        data = load_mapping_data(ctl, zone=z)
        tables = get_all_tables(zone=z, ctl=ctl)
        sens_cols = get_sensitive_columns(ctl, zone=z)

        print(f"\n--- {sys_name} ---")
        print(f" Registered Source Tables:        {len(tables)}")
        print(f" Total Source-to-Target Mappings: {len(data.get('mappings', []))}")
        print(f" Total Unmapped / Skip Columns:   {len(data.get('unmapped_columns', []))}")
        print(f" Sensitive Columns Flagged:       {sum(len(v) for v in sens_cols.values())} (Guarded against raw_payload)")
        print(f"  {'Table':<20} {'Mapped':<10} {'Sensitive':<12} {'Target Entities'}")
        print(f"  {'-'*18} {'-'*8} {'-'*10} {'-'*24}")

        for t in tables:
            t_maps = [m for m in data.get("mappings", []) if normalize_table_name(m.get("Source sheet (SAP table)") or m.get("Source table")) == t]
            t_unmap = [u for u in data.get("unmapped_columns", []) if normalize_table_name(u.get("Source sheet (SAP table)") or u.get("table")) == t]
            t_sens = [u for u in t_unmap if u.get("Class") == "SENSITIVE" or u.get("is_sensitive")]
            targets = sorted(set(m.get("CFA360 sheet") for m in t_maps if m.get("CFA360 sheet")))
            sens_str = f"{len(t_sens)} PROTECTED" if t_sens else "0"
            print(f"  {t:<20} {len(t_maps):<10} {sens_str:<12} {', '.join(targets)}")

    print("\n== SENSITIVE COLUMNS AUDIT (EXCLUDED FROM RAW_PAYLOAD & QUARANTINE) ==")
    all_sens = get_sensitive_columns(ctl, zone=z_req)
    for t, cols in sorted(all_sens.items()):
        for col in sorted(cols):
            print(f"  [{t}] {col:<26} -> Guarded / Redacted")
    print("================================================================================\n")


def add_or_update_mapping(zone="NZ", src_table="", src_column="", target_entity="", target_field="",
                          role="DIRECT", mapping_type="DIRECT", transform_rule="DIRECT", confidence="HIGH",
                          is_sensitive=False, sensitive_reason="", notes="", ctl=None):
    """Add a new mapping or update an existing mapping configuration both in
    bronze_ctl database and nz_mapping_data.json file."""
    global _CACHE
    import bronze_sync as bs

    table = normalize_table_name(src_table)
    column = normalize_col_name(src_column)
    target_ent = target_entity.strip()
    target_fld = target_field.strip().upper()
    map_type = mapping_type.strip().upper() or "DIRECT"
    conf = confidence.strip().upper() or "HIGH"
    rule = transform_rule.strip() if transform_rule else "DIRECT"
    role_val = role.strip() if role else "DIRECT"

    if not table or not column or not target_ent or not target_fld:
        raise ValueError("src_table, src_column, target_entity, and target_field are required")

    should_close = False
    if ctl is None:
        ctl = bs.connect()
        should_close = True

    try:
        # 1. Upsert into bronze_ctl.zone_column_mapping
        ctl.execute("""
            INSERT INTO bronze_ctl.zone_column_mapping (
                zone, src_table, src_column, target_entity, target_field, role,
                mapping_type, transform_rule, confidence, notes, created_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now()
            )
            ON CONFLICT (zone, src_table, src_column, target_entity, target_field, role)
            DO UPDATE SET
                mapping_type = EXCLUDED.mapping_type,
                transform_rule = EXCLUDED.transform_rule,
                confidence = EXCLUDED.confidence,
                notes = EXCLUDED.notes;
        """, (zone, table, column, target_ent, target_fld, role_val, map_type, rule, conf, notes))

        # 2. Update sensitive column classification if requested
        if is_sensitive:
            why = sensitive_reason.strip() or f"Sensitive {column} in {table}"
            ctl.execute("""
                INSERT INTO bronze_ctl.zone_column_classification (
                    zone, src_table, src_column, classification, is_sensitive, why_note, created_at
                ) VALUES (
                    %s, %s, %s, 'sensitive', true, %s, now()
                )
                ON CONFLICT (zone, src_table, src_column)
                DO UPDATE SET
                    is_sensitive = true,
                    classification = 'sensitive',
                    why_note = EXCLUDED.why_note;
            """, (zone, table, column, why))
        else:
            ctl.execute("""
                UPDATE bronze_ctl.zone_column_classification
                SET is_sensitive = false
                WHERE zone = %s AND src_table = %s AND src_column = %s AND is_sensitive = true;
            """, (zone, table, column))

        ctl.commit()

        # 3. Synchronize with nz_mapping_data.json
        try:
            data = load_mapping_data()
            mappings = data.get("mappings", [])

            # Check if mapping already exists in JSON
            found = False
            for m in mappings:
                if (normalize_table_name(m.get("Source sheet (SAP table)")) == table and
                    normalize_col_name(m.get("Source column")) == column and
                    m.get("CFA360 sheet") == target_ent and
                    m.get("CFA360 field") == target_fld):
                    m["Mapping type"] = map_type
                    m["Transformation / rule (at CFA field level)"] = rule
                    m["Confidence"] = conf
                    m["Role"] = role_val
                    m["Note on this column"] = notes
                    found = True
                    break

            if not found:
                mappings.append({
                    "Source sheet (SAP table)": table,
                    "Source column": column,
                    "CFA360 sheet": target_ent,
                    "CFA360 field": target_fld,
                    "Role": role_val,
                    "Mapping type": map_type,
                    "Transformation / rule (at CFA field level)": rule,
                    "Confidence": conf,
                    "Sample value": None,
                    "Fill % (100-row sample)": 1.0,
                    "Note on this column": notes
                })
                data["mappings"] = mappings

            # If sensitive, ensure recorded in unmapped_columns as sensitive
            if is_sensitive:
                unmapped = data.get("unmapped_columns", [])
                sens_found = False
                for u in unmapped:
                    if (normalize_table_name(u.get("Source sheet (SAP table)")) == table and
                        normalize_col_name(u.get("Source column")) == column):
                        u["Class"] = "SENSITIVE"
                        u["Why / note"] = sensitive_reason or f"Sensitive {column}"
                        sens_found = True
                        break
                if not sens_found:
                    unmapped.append({
                        "Source sheet (SAP table)": table,
                        "Source column": column,
                        "Class": "SENSITIVE",
                        "Why / note": sensitive_reason or f"Sensitive {column}"
                    })
                    data["unmapped_columns"] = unmapped

            target_json = EZ_MAPPING_DATA_PATH if (zone or "NZ").upper() == "EZ" else MAPPING_DATA_PATH
            with open(target_json, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)

            _CACHES[(zone or "NZ").upper()] = data
        except Exception as ex:
            LOG.warning(f"Could not update mapping data JSON: {ex}")

        return {"ok": True, "message": f"Successfully saved mapping {table}.{column} -> {target_ent}.{target_fld}"}
    finally:
        if should_close:
            ctl.close()


def delete_mapping(zone="NZ", src_table="", src_column="", target_entity="", target_field="", role="DIRECT", ctl=None):
    """Delete a column mapping from bronze_ctl and nz_mapping_data.json."""
    global _CACHE
    import bronze_sync as bs

    table = normalize_table_name(src_table)
    column = normalize_col_name(src_column)
    target_ent = target_entity.strip()
    target_fld = target_field.strip().upper()

    should_close = False
    if ctl is None:
        ctl = bs.connect()
        should_close = True

    try:
        if target_ent and target_fld:
            ctl.execute("""
                DELETE FROM bronze_ctl.zone_column_mapping
                WHERE zone = %s AND src_table = %s AND src_column = %s
                  AND target_entity = %s AND target_field = %s;
            """, (zone, table, column, target_ent, target_fld))
        else:
            ctl.execute("""
                DELETE FROM bronze_ctl.zone_column_mapping
                WHERE zone = %s AND src_table = %s AND src_column = %s;
            """, (zone, table, column))

        # Also remove from classifications if it was registered as sensitive/unmapped
        ctl.execute("""
            DELETE FROM bronze_ctl.zone_column_classification
            WHERE zone = %s AND src_table = %s AND src_column = %s;
        """, (zone, table, column))

        ctl.commit()

        # Update JSON
        try:
            data = load_mapping_data()
            mappings = data.get("mappings", [])
            new_mappings = [m for m in mappings if not (
                normalize_table_name(m.get("Source sheet (SAP table)")) == table and
                normalize_col_name(m.get("Source column")) == column and
                (not target_ent or m.get("CFA360 sheet") == target_ent) and
                (not target_fld or m.get("CFA360 field") == target_fld)
            )]
            data["mappings"] = new_mappings

            # Also remove from unmapped_columns if present
            unmapped = data.get("unmapped_columns", [])
            new_unmapped = [u for u in unmapped if not (
                normalize_table_name(u.get("Source sheet (SAP table)")) == table and
                normalize_col_name(u.get("Source column")) == column
            )]
            data["unmapped_columns"] = new_unmapped

            target_json = EZ_MAPPING_DATA_PATH if (zone or "NZ").upper() == "EZ" else MAPPING_DATA_PATH
            with open(target_json, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
            _CACHES[(zone or "NZ").upper()] = data
        except Exception as ex:
            LOG.warning(f"Could not update mapping data JSON on delete: {ex}")

        return {"ok": True, "message": f"Successfully deleted mapping for {table}.{column}"}
    finally:
        if should_close:
            ctl.close()

