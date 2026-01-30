#!/usr/bin/env python3
"""Import alarms on startup if export file exists"""
import sqlite3
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db.client import get_database

def import_alarms_if_exists():
    export_file = Path(__file__).parent.parent / 'alarms_export.json'

    if not export_file.exists():
        print(f"[AlarmImport] No export file found, skipping")
        return 0

    print(f"[AlarmImport] Loading alarms from {export_file}")

    with open(export_file, 'r') as f:
        alarms = json.load(f)

    conn = get_database()
    cursor = conn.cursor()

    # Disable FK checks during bulk import
    cursor.execute("PRAGMA foreign_keys = OFF")

    inserted = 0
    skipped = 0

    for alarm in alarms:
        try:
            # NULL out FKs that might not exist
            cursor.execute("""
                INSERT OR IGNORE INTO alarms (
                    id, timestamp, site, region, severity, category, message,
                    status, details, threshold_id, composite_rule_id, asset_id,
                    reading_id, source, acknowledged_at, acknowledged_by,
                    resolved_at, resolved_by, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?, ?, ?, ?, ?)
            """, (
                alarm['id'], alarm['timestamp'], alarm['site'], alarm['region'],
                alarm['severity'], alarm['category'], alarm['message'], alarm['status'],
                alarm.get('details'), alarm.get('threshold_id'), alarm.get('composite_rule_id'),
                alarm.get('source'), alarm.get('acknowledged_at'), alarm.get('acknowledged_by'),
                alarm.get('resolved_at'), alarm.get('resolved_by'), alarm.get('created_at')
            ))
            if cursor.rowcount > 0:
                inserted += 1
            else:
                skipped += 1
        except Exception as e:
            print(f"[AlarmImport] Error importing {alarm.get('id')}: {e}")
            skipped += 1

    conn.commit()

    # Re-enable FK checks
    cursor.execute("PRAGMA foreign_keys = ON")

    conn.close()

    print(f"[AlarmImport] ✅ Imported {inserted} alarms, skipped {skipped} duplicates")

    # Delete export file after import to prevent re-importing on next startup
    if inserted > 0:
        export_file.unlink()
        print(f"[AlarmImport] Deleted {export_file.name} (import complete)")

    return inserted

if __name__ == '__main__':
    import_alarms_if_exists()
