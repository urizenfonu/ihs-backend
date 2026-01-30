#!/usr/bin/env python3
import sqlite3
import json
import sys
from pathlib import Path

def import_alarms(json_path, db_path):
    with open(json_path, 'r') as f:
        alarms = json.load(f)

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    inserted = 0
    skipped = 0

    for alarm in alarms:
        try:
            cursor.execute("""
                INSERT OR IGNORE INTO alarms (
                    id, timestamp, site, region, severity, category, message,
                    status, details, threshold_id, composite_rule_id, asset_id,
                    reading_id, source, acknowledged_at, acknowledged_by,
                    resolved_at, resolved_by, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                alarm['id'], alarm['timestamp'], alarm['site'], alarm['region'],
                alarm['severity'], alarm['category'], alarm['message'], alarm['status'],
                alarm.get('details'), alarm.get('threshold_id'), alarm.get('composite_rule_id'),
                alarm.get('asset_id'), alarm.get('reading_id'), alarm.get('source'),
                alarm.get('acknowledged_at'), alarm.get('acknowledged_by'),
                alarm.get('resolved_at'), alarm.get('resolved_by'), alarm.get('created_at')
            ))
            if cursor.rowcount > 0:
                inserted += 1
            else:
                skipped += 1
        except Exception as e:
            print(f"⚠️  Error importing alarm {alarm.get('id')}: {e}")
            skipped += 1

    conn.commit()
    conn.close()

    print(f"✅ Imported {inserted} alarms")
    print(f"⏭️  Skipped {skipped} duplicates")

if __name__ == "__main__":
    json_file = sys.argv[1] if len(sys.argv) > 1 else "./alarms_export.json"
    target_db = sys.argv[2] if len(sys.argv) > 2 else "./data/ihs.db"

    import_alarms(json_file, target_db)
