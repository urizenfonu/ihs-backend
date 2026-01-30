#!/usr/bin/env python3
import sys
from pathlib import Path

# Add parent to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from db.client import get_database

def main():
    print("Running database migrations...")
    db = get_database()
    print("Migrations applied successfully")

    # Report orphaned alarms
    cursor = db.execute('''
        SELECT COUNT(*)
        FROM alarms
        WHERE threshold_id IS NOT NULL
          AND threshold_id NOT IN (SELECT id FROM thresholds)
    ''')
    orphaned = cursor.fetchone()[0]

    if orphaned > 0:
        print(f"Found {orphaned} orphaned alarms")
        cursor = db.execute('''
            SELECT COUNT(*)
            FROM alarms
            WHERE status = 'archived'
        ''')
        archived = cursor.fetchone()[0]
        print(f"{archived} alarms marked as archived")
    else:
        print("No orphaned alarms found")

if __name__ == '__main__':
    main()
