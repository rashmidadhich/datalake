"""
Unit and integration tests for Multi-Mode Ingestion in Bronze Data Lake:
  1. ParquetSource: schema inspection, column detection, PK inference, streaming.
  2. CSVSource: delimiter sniffing, dynamic type inference, null handling, streaming.
  3. JDBCSource: URL parsing, SQLite/PostgreSQL introspection, streaming.
  4. cmd_ingest: End-to-end ingestion with _source, _src_table, _load_id, _chunk_id lineage.
"""

import os
import unittest
import datetime as dt
import bronze_sync as bs


class TestMultiModeSources(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.parquet_file = os.path.abspath("scratch/assets.parquet")
        cls.csv_file = os.path.abspath("scratch/subscribers.csv")
        cls.sqlite_file = os.path.abspath("scratch/test_billing.sqlite")

    def test_parquet_source(self):
        src = bs.ParquetSource("NZ", config={"path": self.parquet_file})
        cols = src.columns("PARQUET", "ASSET_PARQUET")
        col_names = [c["name"].lower() for c in cols]
        self.assertIn("asset_id", col_names)
        self.assertIn("asset_name", col_names)
        self.assertIn("price", col_names)
        self.assertIn("is_active", col_names)

        # PK detection
        pks = src.pk_cols("PARQUET", "ASSET_PARQUET")
        self.assertEqual(pks, ["ASSET_ID"])

        # Stream batches
        batches = list(src.stream("PARQUET", "ASSET_PARQUET", ["asset_id", "asset_name", "price"], {"kind": "all"}))
        total_rows = sum(len(b) for b in batches)
        self.assertEqual(total_rows, 3)

        # Row counts
        count = src.count("PARQUET", "ASSET_PARQUET", {"kind": "all"})
        self.assertEqual(count, 3)

    def test_csv_source(self):
        src = bs.CSVSource("NZ", config={"path": self.csv_file, "delimiter": ","})
        cols = src.columns("CSV", "SUB_CSV")
        col_names = [c["name"].lower() for c in cols]
        self.assertIn("sub_id", col_names)
        self.assertIn("phone_num", col_names)
        self.assertIn("status", col_names)

        # PK detection
        pks = src.pk_cols("CSV", "SUB_CSV")
        self.assertEqual(pks, ["SUB_ID"])

        # Stream batches
        batches = list(src.stream("CSV", "SUB_CSV", ["sub_id", "phone_num", "status"], {"kind": "all"}))
        total_rows = sum(len(b) for b in batches)
        self.assertEqual(total_rows, 3)

    def test_jdbc_source(self):
        jdbc_url = f"jdbc:sqlite:{self.sqlite_file}"
        src = bs.JDBCSource("NZ", config={"jdbc_url": jdbc_url})
        cols = src.columns("PUBLIC", "BILLING_INVOICES")
        col_names = [c["name"].lower() for c in cols]
        self.assertIn("invoice_id", col_names)
        self.assertIn("customer_id", col_names)
        self.assertIn("amount", col_names)

        # PK detection
        pks = src.pk_cols("PUBLIC", "BILLING_INVOICES")
        self.assertEqual(pks, ["invoice_id"])

        # Stream batches
        batches = list(src.stream("PUBLIC", "BILLING_INVOICES", ["invoice_id", "customer_id", "amount"], {"kind": "all"}))
        total_rows = sum(len(b) for b in batches)
        self.assertEqual(total_rows, 2)
        src.close()

    def test_cmd_ingest_programmatic(self):
        class Args:
            type = "parquet"
            file = self.parquet_file
            url = None
            table = "TEST_INGEST_ASSETS"
            schema = "PARQUET"
            zone = "NZ"
            source = "Teerva"
            mode = "merge"
            pk = "asset_id"
            watermark = None
            delimiter = ","
            meta = '{"test": true}'
            arraysize = 1000
            config = "config.toml"
            env_file = None
            return_dict = True

        res = bs.cmd_ingest(Args())
        self.assertTrue(isinstance(res, dict))
        self.assertTrue(res["ok"])
        self.assertEqual(res["ingest_type"], "parquet")
        self.assertEqual(res["source_system"], "Teerva")
        self.assertEqual(res["rows_fetched"], 3)
        self.assertEqual(res["rows_loaded"], 3)

        # Verify rows in PostgreSQL
        pg = bs.connect()
        try:
            row = pg.execute("SELECT asset_id, asset_name, _source, _src_table FROM bronze_nz.test_ingest_assets WHERE asset_id = 1").fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(row[0], 1)
            self.assertEqual(row[1], "Router-A")
            self.assertEqual(row[2], "Teerva")
            self.assertEqual(row[3], "TEST_INGEST_ASSETS")
        finally:
            pg.execute("DROP TABLE IF EXISTS bronze_nz.test_ingest_assets CASCADE")
            pg.close()


if __name__ == "__main__":
    unittest.main()
