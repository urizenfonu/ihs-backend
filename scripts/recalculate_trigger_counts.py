#!/usr/bin/env python3
"""Recalculate trigger counts for all thresholds from existing alarms"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db.client import get_database

def recalculate_trigger_counts():
    """
    Idempotent script to recalculate trigger_count and last_triggered
    for all thresholds based on existing alarms.
    """
    print("🔄 Recalculating trigger counts from alarms...")

    conn = get_database()
    cursor = conn.cursor()

    # Get all thresholds
    cursor.execute("SELECT id FROM thresholds")
    thresholds = cursor.fetchall()

    print(f"Found {len(thresholds)} thresholds\n")

    updated = 0

    for threshold in thresholds:
        threshold_id = threshold[0]

        # Count alarms for this threshold using composite_rule_id
        cursor.execute("""
            SELECT COUNT(*) as cnt, MAX(timestamp) as last_triggered
            FROM alarms
            WHERE composite_rule_id = ?
        """, (threshold_id,))

        result = cursor.fetchone()
        trigger_count = result[0] if result else 0
        last_triggered = result[1] if result and result[1] else None

        # Update threshold
        cursor.execute("""
            UPDATE thresholds
            SET trigger_count = ?, last_triggered = ?
            WHERE id = ?
        """, (trigger_count, last_triggered, threshold_id))

        if trigger_count > 0:
            print(f"  ✓ {threshold_id:35s} | triggers: {trigger_count:4d} | last: {last_triggered or 'never'}")
            updated += 1

    conn.commit()

    print(f"\n{'='*70}")
    print(f"✅ Recalculation complete!")
    print(f"   Updated {updated} thresholds with non-zero trigger counts")
    print(f"{'='*70}\n")

if __name__ == '__main__':
    recalculate_trigger_counts()
