import os
import sys

# Ensure root bronze_loader directory is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import psycopg2
from bronze_sync import connect
from mapping import (
    load_mapping_from_db,
    get_table_mappings,
    get_sensitive_columns,
    validate_schema_mapping,
)

def test_db_driven_lifecycle():
    print("Connecting to database...")
    ctl = connect()
    cur = ctl.cursor()
    
    test_table = "BUT000"
    test_col = "ZZ_TEST_DB_PULL"
    test_target_entity = "PARTY"
    test_target_field = "test_custom_id"
    test_role = "attribute"
    test_rule = "direct"
    
    try:
        print("\n--- 1. Testing Direct DB Pull ---")
        db_data = load_mapping_from_db(ctl, zone="NZ")
        assert db_data is not None, "Failed to load mapping from DB"
        print(f"Loaded {len(db_data['mappings'])} mappings, {len(db_data['unmapped_columns'])} unmapped cols, {len(db_data['cfa_field_coverage'])} schema fields directly from DB.")
        
        print("\n--- 2. Direct SQL INSERT into bronze_ctl.zone_column_mapping ---")
        cur.execute(
            """
            INSERT INTO bronze_ctl.zone_column_mapping
                (zone, src_table, src_column, target_entity, target_field, role, mapping_type, transform_rule, confidence, sample_value, fill_pct, notes)
            VALUES
                ('NZ', %s, %s, %s, %s, %s, 'DIRECT', %s, 'HIGH', 'SAMPLE_VAL', 99.5, 'Automated DB-driven test')
            ON CONFLICT (zone, src_table, src_column, target_entity, target_field, role) DO UPDATE
            SET transform_rule = EXCLUDED.transform_rule;
            """,
            (test_table, test_col, test_target_entity, test_target_field, test_role, test_rule)
        )
        ctl.commit()
        print(f"Inserted/updated {test_col} in bronze_ctl.zone_column_mapping via SQL.")
        
        # Verify get_table_mappings pulls it directly from DB
        table_maps = get_table_mappings(ctl=ctl, zone="NZ", table=test_table)
        matches = [m for m in table_maps if m['Source column'] == test_col]
        assert len(matches) == 1, f"Expected 1 mapping for {test_col}, found {len(matches)}"
        assert matches[0]['CFA360 field'] == test_target_field
        print(f"Verified {test_col} retrieved via get_table_mappings() from DB: target_field={matches[0]['CFA360 field']}")
        
        print("\n--- 3. Direct SQL INSERT into bronze_ctl.zone_column_classification (is_sensitive=TRUE) ---")
        cur.execute(
            """
            INSERT INTO bronze_ctl.zone_column_classification
                (zone, src_table, src_column, classification, is_sensitive, why_note, sample_value, fill_pct)
            VALUES
                ('NZ', %s, %s, 'PII_CONFIDENTIAL', TRUE, 'Test sensitive flag in DB', 'SECRET_INFO', 100.0)
            ON CONFLICT (zone, src_table, src_column) DO UPDATE
            SET is_sensitive = TRUE, why_note = 'Test sensitive flag in DB';
            """,
            (test_table, test_col)
        )
        ctl.commit()
        print(f"Flagged {test_col} as is_sensitive=TRUE in bronze_ctl.zone_column_classification via SQL.")
        
        # Verify get_sensitive_columns pulls it directly from DB
        sens_cols = get_sensitive_columns(ctl=ctl, zone="NZ", table=test_table)
        assert test_col in sens_cols, f"Expected {test_col} in sensitive columns: {sens_cols}"
        print(f"Verified {test_col} recognized in get_sensitive_columns() from DB: {sens_cols}")
        
        print("\n--- 4. Direct SQL UPDATE to toggle is_sensitive=FALSE ---")
        cur.execute(
            """
            UPDATE bronze_ctl.zone_column_classification
            SET is_sensitive = FALSE
            WHERE zone = 'NZ' AND src_table = %s AND src_column = %s;
            """,
            (test_table, test_col)
        )
        ctl.commit()
        sens_cols_after = get_sensitive_columns(ctl=ctl, zone="NZ", table=test_table)
        assert test_col not in sens_cols_after, f"Expected {test_col} NOT in sensitive columns after update: {sens_cols_after}"
        print(f"Verified {test_col} instantly unflagged in get_sensitive_columns() after SQL UPDATE: {sens_cols_after}")
        
        print("\n--- 5. Validate Schema Mapping with DB-driven data ---")
        val_res = validate_schema_mapping("NZ", test_table, [test_col, "PARTNER"], ctl=ctl)
        print(f"Validation result with DB: mapped_count={val_res['mapped_columns_count']}, missing_mapped={len(val_res['missing_mapped'])}")
        assert test_col in [m["Source column"] for m in table_maps], "Expected test_col to be in mapped columns."
        
    finally:
        print("\n--- 6. Cleanup test records from DB tables ---")
        cur.execute(
            "DELETE FROM bronze_ctl.zone_column_mapping WHERE zone = 'NZ' AND src_table = %s AND src_column = %s;",
            (test_table, test_col)
        )
        cur.execute(
            "DELETE FROM bronze_ctl.zone_column_classification WHERE zone = 'NZ' AND src_table = %s AND src_column = %s;",
            (test_table, test_col)
        )
        ctl.commit()
        cur.close()
        ctl.close()
        print("Cleaned up test rows successfully.")
        print("\n>>> ALL DB-DRIVEN MAPPING TESTS PASSED! <<<")

if __name__ == "__main__":
    test_db_driven_lifecycle()
