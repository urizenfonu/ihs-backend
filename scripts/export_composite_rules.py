#!/usr/bin/env python3
import sqlite3
import json
import sys

def export_rules(db_path, output_path):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    cursor = conn.execute("""
        SELECT id, name, description, severity, category, rule_type,
               enabled, conditions, logical_operator, time_window_minutes,
               aggregation_type, applies_to, region_id, cluster_id, site_id
        FROM composite_rules
        ORDER BY id
    """)

    rules = [dict(row) for row in cursor.fetchall()]
    conn.close()

    with open(output_path, 'w') as f:
        json.dump(rules, f, indent=2)

    print(f"✅ Exported {len(rules)} composite rules to {output_path}")

if __name__ == "__main__":
    source_db = sys.argv[1] if len(sys.argv) > 1 else "/home/urizen/ihs-repo/data/ihs.db"
    output = sys.argv[2] if len(sys.argv) > 2 else "./composite_rules_export.json"
    export_rules(source_db, output)
