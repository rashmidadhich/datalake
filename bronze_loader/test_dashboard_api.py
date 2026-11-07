"""
Test HTTP API endpoints of dashboard.py:
  1. GET /api/status
  2. POST /api/ingest (Parquet)
  3. POST /api/ingest (CSV)
  4. POST /api/ingest (JDBC)
"""

import json
import os
import threading
import time
import urllib.request
import unittest
from http.server import HTTPServer
import dashboard as db
import bronze_sync as bs


class TestDashboardAPI(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.port = 8899
        cls.server = HTTPServer(("127.0.0.1", cls.port), db.DashboardRequestHandler)
        cls.server_thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.server_thread.start()
        time.sleep(0.5)

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()

    def test_01_get_status(self):
        url = f"http://127.0.0.1:{self.port}/api/status"
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req) as resp:
            self.assertEqual(resp.status, 200)
            data = json.loads(resp.read().decode("utf-8"))
            self.assertIn("kpi", data)
            self.assertIn("tables", data)
            self.assertIn("source_catalog", data)
            # Verify ingest_type is present in tables
            for t in data["tables"]:
                self.assertIn("ingest_type", t)

    def test_02_post_ingest_parquet(self):
        url = f"http://127.0.0.1:{self.port}/api/ingest"
        payload = {
            "type": "parquet",
            "file": os.path.abspath("scratch/assets.parquet"),
            "table": "API_ASSETS_TEST",
            "zone": "NZ",
            "source": "Teerva",
            "mode": "merge",
            "pk": "asset_id"
        }
        data_bytes = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(url, data=data_bytes, headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req) as resp:
                self.assertEqual(resp.status, 200)
                res = json.loads(resp.read().decode("utf-8"))
                self.assertTrue(res.get("ok"))
                self.assertEqual(res.get("ingest_type"), "parquet")
                self.assertEqual(res.get("rows_loaded"), 3)
        finally:
            pg = bs.connect()
            pg.execute("DROP TABLE IF EXISTS bronze_nz.api_assets_test CASCADE")
            pg.close()


if __name__ == "__main__":
    unittest.main()
