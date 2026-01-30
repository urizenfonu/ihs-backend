#!/usr/bin/env python3
"""
Migrate old alarms to use composite_rule_id based on message patterns
"""
import sqlite3
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db.client import get_database, close_database

# Map message patterns to composite rule IDs
MESSAGE_PATTERNS = {
    'Fuel Low': 'fuel_low',
    'Fuel Drop': 'fuel_drop',
    'Refuel': 'refuel',
    'Grid Available': 'grid_available',
    'Grid Not Available': 'grid_not_available',
    'Grid Low Phase': 'grid_low_phase',
    'Grid High Frequency': 'grid_high_frequency',
    'Grid Low Frequency': 'grid_low_frequency',
    'Battery Low': 'battery_low',
    'Battery Discharge': 'battery_discharge',
    'Battery Charge': 'battery_charge',
    'Battery Floating': 'battery_floating',
    'Solar On': 'solar_on',
    'Solar Off': 'solar_off',
    'Gen On': 'gen_on',
    'Gen Off': 'gen_off',
    'Gen Low Phase': 'gen_low_phase',
    'Gen High Frequency': 'gen_high_frequency',
    'Gen Low Frequency': 'gen_low_frequency',
    'High Temperature': 'high_temperature',
    'Site Down': 'site_down',
    'testing': None  # Skip test alarms
}

def migrate_alarms():
    print(f"🔄 Migrating alarms to composite rules...")
    conn = get_database()
    cursor = conn.cursor()

    # Get all alarms with threshold_id
    cursor.execute("""
        SELECT id, threshold_id, message, composite_rule_id
        FROM alarms
        WHERE threshold_id IS NOT NULL AND threshold_id != ''
    """)
    alarms = cursor.fetchall()

    print(f"Found {len(alarms)} alarms with threshold_id\n")

    updated = 0
    skipped = 0

    for alarm_id, threshold_id, message, current_composite_id in alarms:
        # Skip if already has composite_rule_id
        if current_composite_id:
            skipped += 1
            continue

        # Find matching pattern
        composite_rule_id = None
        for pattern, rule_id in MESSAGE_PATTERNS.items():
            if pattern in message:
                composite_rule_id = rule_id
                break

        if not composite_rule_id:
            print(f"⚠️  No mapping for: {message[:60]}")
            skipped += 1
            continue

        # Update alarm
        cursor.execute("""
            UPDATE alarms
            SET composite_rule_id = ?,
                source = 'threshold_migrated'
            WHERE id = ?
        """, (composite_rule_id, alarm_id))

        updated += 1
        if updated % 50 == 0:
            print(f"  Updated {updated} alarms...")

    conn.commit()
    close_database()

    print(f"\n{'='*60}")
    print(f"✅ Migration complete!")
    print(f"   Updated: {updated}")
    print(f"   Skipped: {skipped}")
    print(f"{'='*60}\n")

    # Verify
    conn = get_database()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM alarms WHERE composite_rule_id IS NOT NULL")
    count = cursor.fetchone()[0]
    print(f"📊 Alarms with composite_rule_id: {count}")
    close_database()

if __name__ == '__main__':
    migrate_alarms()
