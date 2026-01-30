#!/usr/bin/env python3
import sqlite3
import json
import sys
from pathlib import Path

def export_alarms(db_path, output_path):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    cursor = conn.execute("""
        SELECT id, timestamp, site, region, severity, category, message,
               status, details, threshold_id, composite_rule_id, asset_id,
               reading_id, source, acknowledged_at, acknowledged_by,
               resolved_at, resolved_by, created_at
        FROM alarms
        WHERE status NOT IN ('archived')
        ORDER BY timestamp DESC
    """)

    alarms = [dict(row) for row in cursor.fetchall()]
    conn.close()

    with open(output_path, 'w') as f:
        json.dump(alarms, f, indent=2)

    print(f"✅ Exported {len(alarms)} alarms to {output_path}")
    return len(alarms)

if __name__ == "__main__":
    source_db = sys.argv[1] if len(sys.argv) > 1 else "/home/urizen/ihs-repo/data/ihs.db"
    output = sys.argv[2] if len(sys.argv) > 2 else "./alarms_export.json"

    export_alarms(source_db, output)
