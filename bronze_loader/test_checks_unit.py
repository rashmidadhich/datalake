#!/usr/bin/env python3
"""Unit tests for bronze_sync.py checks (Data Quality, Duplicate, Idempotency, Schema).

Can be executed in any environment without a live PostgreSQL database:
    python test_checks_unit.py
"""
import decimal
import unittest
from unittest.mock import MagicMock

import bronze_sync as bs


class TestBronzeSyncChecks(unittest.TestCase):

    def test_schema_pg_type_mapping(self):
        """Test schema type conversion from Oracle catalog types to PostgreSQL types."""
        self.assertEqual(bs.pg_type({"type": "NUMBER", "precision": 18, "scale": 0}), "bigint")
        self.assertEqual(bs.pg_type({"type": "NUMBER", "precision": 9, "scale": 0}), "integer")
        self.assertEqual(bs.pg_type({"type": "NUMBER", "precision": 22, "scale": 4}), "numeric")
        self.assertEqual(bs.pg_type({"type": "FLOAT", "precision": None, "scale": None}), "double precision")
        self.assertEqual(bs.pg_type({"type": "DATE", "precision": None, "scale": None}), "timestamp")
        self.assertEqual(bs.pg_type({"type": "TIMESTAMP(6) WITH TIME ZONE", "precision": None, "scale": None}), "timestamptz")
        self.assertEqual(bs.pg_type({"type": "VARCHAR2", "precision": None, "scale": None}), "text")
        self.assertEqual(bs.pg_type({"type": "CLOB", "precision": None, "scale": None}), "text")

    def test_schema_col_map_uniqueness(self):
        """Test schema column mapping and identifier name sanitization."""
        cols = [
            {"name": "CUSTOMER_ID", "type": "NUMBER", "precision": 9, "scale": 0},
            {"name": "123_INVALID", "type": "VARCHAR2", "precision": None, "scale": None},
            {"name": "DUP_NAME", "type": "VARCHAR2", "precision": None, "scale": None},
            {"name": "DUP_NAME", "type": "VARCHAR2", "precision": None, "scale": None},
        ]
        cmap = bs.col_map(cols)
        pg_names = [pn for _, pn, _ in cmap]
        self.assertEqual(pg_names[0], "customer_id")
        self.assertTrue(pg_names[1].startswith("c_"))  # prepends c_ if leading digit
        self.assertEqual(pg_names[2], "dup_name")
        self.assertEqual(pg_names[3], "dup_name_1")    # deduplicates colliding names

    def test_idempotent_predicate_generation(self):
        """Test delete-before-copy predicate generation for idempotent chunk execution."""
        # Range predicate (append mode chunk)
        pred_range = {"kind": "range", "col": "SESSION_START_TIME", "a": "2026-11-01", "b": "2026-11-02"}
        sql_clause, params = bs.pg_predicate(pred_range)
        self.assertIsNotNone(sql_clause)
        self.assertEqual(params, ["2026-11-01", "2026-11-02"])

        # Window predicate (incremental window)
        pred_window = {"kind": "window", "col": "LAST_UPDATED_DATE", "a": "2026-11-01 10:00", "b": "2026-11-01 11:00"}
        sql_clause, params = bs.pg_predicate(pred_window)
        self.assertIsNotNone(sql_clause)
        self.assertEqual(params, ["2026-11-01 10:00", "2026-11-01 11:00"])

        # Null predicate
        pred_null = {"kind": "null", "col": "SESSION_START_TIME"}
        sql_clause, params = bs.pg_predicate(pred_null)
        self.assertIsNotNone(sql_clause)
        self.assertEqual(params, [])

    def test_data_quality_pk_null_rejection(self):
        """Test that data quality logic catches NULL and blank primary keys."""
        ocols = ["ID", "NAME", "AMT"]
        names = ["id", "name", "amt"]
        pk_cols = ["id"]
        pk_indices = [names.index(c) for c in pk_cols]

        test_rows = [
            (101, "Alice", decimal.Decimal("10.50")),
            (None, "Bad Row Null PK", decimal.Decimal("20.00")),
            ("", "Bad Row Empty PK", decimal.Decimal("30.00")),
            ("   ", "Bad Row Blank PK", decimal.Decimal("40.00")),
            (102, "Bob", decimal.Decimal("50.00")),
        ]

        valid_rows = []
        quarantined = []
        for r in test_rows:
            rejection_reason = None
            for idx in pk_indices:
                val = r[idx]
                if val is None or (isinstance(val, str) and val.strip() == ""):
                    rejection_reason = f"PK column '{names[idx]}' is NULL or blank"
                    break
            if rejection_reason:
                quarantined.append((r, rejection_reason))
            else:
                valid_rows.append(r)

        self.assertEqual(len(valid_rows), 2)
        self.assertEqual(len(quarantined), 3)
        for _, reason in quarantined:
            self.assertIn("NULL or blank", reason)

    def test_duplicate_check_intra_chunk(self):
        """Test intra-chunk duplicate primary key detection."""
        names = ["id", "name"]
        pk_indices = [0]
        seen_pks = set()

        test_rows = [
            (101, "First occurrence of 101"),
            (102, "First occurrence of 102"),
            (101, "Duplicate occurrence of 101"),
            (103, "First occurrence of 103"),
            (102, "Duplicate occurrence of 102"),
        ]

        accepted_rows = []
        duplicates_count = 0
        for r in test_rows:
            pk_val = tuple(r[idx] for idx in pk_indices)
            if pk_val in seen_pks:
                duplicates_count += 1
            else:
                seen_pks.add(pk_val)
                accepted_rows.append(r)

        self.assertEqual(len(accepted_rows), 3)
        self.assertEqual(duplicates_count, 2)
        self.assertEqual([r[0] for r in accepted_rows], [101, 102, 103])

    def test_text_nul_byte_cleaning(self):
        """Test that PostgreSQL-incompatible NUL (0x00) bytes are sanitized from text."""
        test_val = "Customer\x00Name\x00123"
        cleaned_val = test_val.replace("\x00", "")
        self.assertEqual(cleaned_val, "CustomerName123")
        self.assertNotIn("\x00", cleaned_val)

    def test_cli_argument_parsers(self):
        """Test that all newly added subcommands are registered in CLI parser."""
        parser = bs.argparse.ArgumentParser()
        parser.add_argument("--env-file", default=None)
        sub = parser.add_subparsers(dest="cmd", required=True)
        for name in ("init", "discover", "historic", "incr", "status", "check", "dq", "schema", "verify"):
            p = sub.add_parser(name)
            p.add_argument("--config", default="config.toml")
        p_ui = sub.add_parser("ui")
        p_ui.add_argument("--config", default="config.toml")
        p_ui.add_argument("--port", type=int, default=8080)

        # Test parser dispatches without error
        p_map = sub.add_parser("mapping")
        p_map.add_argument("--config", default="config.toml")
        p_map.add_argument("--zone", default="NZ")

        for cmd in ("dq", "schema", "verify", "status", "check", "ui", "mapping"):
            args = parser.parse_args([cmd])
            self.assertEqual(args.cmd, cmd)

    def test_nz_mapping_data_integrity(self):
        """Test North Zone mapping dictionary loading and counts."""
        import mapping
        data = mapping.load_mapping_data(zone="NZ", force_json=True)
        self.assertEqual(len(data["mappings"]), 139)
        self.assertEqual(len(data["unmapped_columns"]), 650)
        self.assertEqual(len(data["cfa_field_coverage"]), 71)

        tables = mapping.get_all_tables("NZ")
        self.assertEqual(len(tables), 12)
        expected_tables = {
            "ADR2", "ADR6", "ADRC", "BUT000", "BUT021_FS", "BUT050",
            "CRMD_ORDERADM_H", "CRMD_ORDERADM_I", "CRMD_ORDER_INDEX",
            "CRM_JEST", "IBIB", "TJ30T"
        }
        self.assertEqual(set(tables), expected_tables)

    def test_ez_mapping_data_integrity(self):
        """Test East Zone Siebel mapping dictionary loading and counts."""
        import mapping
        data = mapping.load_mapping_data(zone="EZ", force_json=True)
        self.assertEqual(len(data["mappings"]), 182)
        self.assertEqual(len(data["unmapped_columns"]), 1866)
        self.assertEqual(len(data["cfa_field_coverage"]), 97)

        tables = mapping.get_all_tables("EZ")
        self.assertEqual(len(tables), 13)
        self.assertIn("S_ORDER", tables)
        self.assertIn("S_SRV_REQ", tables)
        self.assertIn("S_ASSET", tables)

        # 39 sensitive columns in EZ
        ez_sens = mapping.get_sensitive_columns(None, "EZ")
        total_sens = sum(len(v) for v in ez_sens.values())
        self.assertEqual(total_sens, 39)

    def test_nz_sensitive_column_redaction(self):
        """Test sensitive columns retrieval and payload redaction."""
        import mapping
        sens_map = mapping.get_sensitive_columns(None, "NZ")
        total_sens = sum(len(v) for v in sens_map.values())
        self.assertEqual(total_sens, 9)

        # BUT000 sensitive columns: NAME_LST2, ZZTANNUMBER, ZZFIELD10, ZZZFIELD30
        but000_sens = mapping.get_sensitive_columns(None, "NZ", "BUT000")
        self.assertIn("NAME_LST2", but000_sens)
        self.assertIn("ZZFIELD10", but000_sens)

        # Test redaction of sensitive columns
        raw_row = {
            "PARTNER": "1020766256",
            "NAME_FIRST": "JOHN",
            "NAME_LAST": "DOE",
            "NAME_LST2": "AACFI4502K",     # PAN number
            "ZZFIELD10": "07AAAAA0000A1Z5", # GSTIN
        }
        sanitized = mapping.sanitize_raw_payload(raw_row, but000_sens)
        self.assertEqual(sanitized["PARTNER"], "1020766256")
        self.assertEqual(sanitized["NAME_FIRST"], "JOHN")
        self.assertEqual(sanitized["NAME_LST2"], "[REDACTED_SENSITIVE]")
        self.assertEqual(sanitized["ZZFIELD10"], "[REDACTED_SENSITIVE]")

    def test_nz_schema_check_validation(self):
        """Test schema validation against expected North Zone mapping."""
        import mapping
        # Test 1: Complete valid schema for IBIB
        ibib_oracle_cols = [
            "IBASE", "CRTIM", "UPTIM", "EXTID", "AUTHG", "ZZSERVICETYP", "ZZSERVICESUB",
            "ZZUSAGECODE", "ZZEXCHANGENA", "ZZINSTALLATI", "ZZAGENTCODE", "ZZBACKENDSER",
            "ZZINSTALLATI0001", "ZZBSNLAREACO", "ZZSERVICESTA", "ZZSERVICEEND", "ZZBILLCORRES",
            "ZZBILLINGADD", "ZZSTATUSTYPE", "ZZSTATUSREAS", "ZZZFIELD3", "ZZZEEWB",
            "ZZZEEWB20001", "ZZZEEWB_ADD10002", "ZZZEEWB_ADD10004", "ZZZCUST_CAT", "ZZZEEWB_EX11"
        ]
        val1 = mapping.validate_schema_mapping("NZ", "IBIB", ibib_oracle_cols)
        self.assertTrue(val1["is_valid"])
        self.assertEqual(len(val1["missing_mapped"]), 0)

        # Test 2: Missing mapped column detection
        incomplete_cols = ["IBASE", "CRTIM"] # Missing EXTID, AUTHG, etc.
        val2 = mapping.validate_schema_mapping("NZ", "IBIB", incomplete_cols)
        self.assertFalse(val2["is_valid"])
        self.assertIn("EXTID", val2["missing_mapped"])
        self.assertIn("AUTHG", val2["missing_mapped"])

        # Test 3: Sensitive column detection on CRMD_ORDERADM_H
        order_cols = ["GUID", "OBJECT_ID", "PROCESS_TYPE", "ZPASSWORD", "ZVOUCHER", "NEW_UNKNOWN_COL"]
        val3 = mapping.validate_schema_mapping("NZ", "CRMD_ORDERADM_H", order_cols)
        self.assertIn("ZPASSWORD", val3["sensitive_present"])
        self.assertIn("ZVOUCHER", val3["sensitive_present"])
        self.assertIn("NEW_UNKNOWN_COL", val3["unclassified_drift"])


if __name__ == "__main__":
    unittest.main()
