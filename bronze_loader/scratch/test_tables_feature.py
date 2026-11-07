import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import bronze_sync as bs
import mapping
from dashboard import get_oracle_tables, get_oracle_columns

def test_oracle_table_pulling():
    print("\n--- 1. Testing OracleSource Live Introspection ---")
    src = bs.SOURCE_FACTORY("NZ", {})
    current = src.current_schema()
    schemas = src.schemas()
    all_tbls = src.all_accessible_tables()
    print(f"Current schema: {current}")
    print(f"Accessible schemas: {schemas}")
    print(f"Total tables found in Oracle: {len(all_tbls)}")
    assert len(all_tbls) >= 12, f"Expected at least 12 tables, found {len(all_tbls)}"
    
    # Check DATALAKE_LOADING tables
    dl_tbls = [t for t in all_tbls if t['owner'] == 'DATALAKE_LOADING']
    assert len(dl_tbls) == 12, f"Expected 12 DATALAKE_LOADING tables, found {len(dl_tbls)}"
    print("Found Siebel tables in DATALAKE_LOADING:", [t['table_name'] for t in dl_tbls[:5]])
    
    print("\n--- 2. Testing Live Column Introspection ---")
    cols = src.columns("DATALAKE_LOADING", "S_ASSET")
    print(f"Columns in DATALAKE_LOADING.S_ASSET: {len(cols)}")
    assert len(cols) == 338, f"Expected 338 columns in S_ASSET, got {len(cols)}"
    assert any(c['name'] == 'ROW_ID' for c in cols), "Expected ROW_ID in S_ASSET"
    src.close()
    print("OracleSource introspection tests PASSED!")

    print("\n--- 3. Testing mapping.get_all_tables(from_oracle=True) ---")
    ora_tables = mapping.get_all_tables(zone="NZ", from_oracle=True)
    print(f"mapping.get_all_tables(from_oracle=True) returned {len(ora_tables)} tables")
    assert "S_ASSET" in ora_tables, "Expected S_ASSET in oracle tables list"
    assert "S_ORDER" in ora_tables, "Expected S_ORDER in oracle tables list"
    print("mapping.get_all_tables(from_oracle=True) PASSED!")

    print("\n--- 4. Testing dashboard get_oracle_tables and get_oracle_columns ---")
    dash_tables = get_oracle_tables("NZ")
    assert dash_tables["connected"] is True, "Expected connected to be True"
    assert dash_tables["count"] >= 12, "Expected table count >= 12"
    
    dash_cols = get_oracle_columns("NZ", "DATALAKE_LOADING", "S_ASSET")
    assert dash_cols["count"] == 338, "Expected 338 columns from dashboard API"
    print("Dashboard Oracle functions PASSED!")

    print("\n>>> ALL ORACLE TABLE PULLING TESTS PASSED! <<<")

if __name__ == "__main__":
    test_oracle_table_pulling()
