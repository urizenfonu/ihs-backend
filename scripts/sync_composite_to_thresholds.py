#!/usr/bin/env python3
"""
Sync composite_rules to thresholds table with trigger counts from alarms
"""
import sqlite3
import json
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db.client import get_database, close_database

def sync_rules_to_thresholds():
    print(f"🔄 Syncing composite_rules → thresholds...")
    conn = get_database()
    cursor = conn.cursor()

    # Get all composite rules
    cursor.execute("SELECT * FROM composite_rules")
    rules = cursor.fetchall()

    print(f"Found {len(rules)} composite rules\n")

    # Keep existing thresholds to avoid breaking historical alarms and custom thresholds.
    # We upsert (INSERT OR REPLACE) composite rule thresholds by id.
    #
    # NOTE: We still remove explicitly deprecated rule ids to keep the UI clean.
    deprecated_ids = ("battery_discharge", "battery_charge")
    cursor.execute(
        f"DELETE FROM thresholds WHERE id IN ({','.join(['?'] * len(deprecated_ids))})",
        deprecated_ids,
    )

    created = 0

    for rule in rules:
        rule_id = rule['id']
        conditions = json.loads(rule['conditions'])

        # Get trigger count from alarms
        cursor.execute("""
            SELECT COUNT(*) as cnt, MAX(timestamp) as last_triggered
            FROM alarms
            WHERE composite_rule_id = ?
        """, (rule_id,))
        alarm_data = cursor.fetchone()
        trigger_count = alarm_data['cnt'] if alarm_data else 0
        last_triggered = alarm_data['last_triggered'] if alarm_data else None

        # Use first condition for threshold data
        primary_condition = conditions[0] if conditions else {}

        # Prepare multi-condition data
        conditions_json = None
        logic_operator = None
        if len(conditions) > 1:
            # Multi-condition: store as JSON array
            conditions_json = json.dumps([
                {
                    "parameter": c.get("parameter", ""),
                    "condition": c.get("operator", ">="),
                    "value": c.get("value", 0),
                    "unit": c.get("unit", ""),
                }
                for c in conditions
            ])
            logic_operator = rule['logical_operator'] if 'logical_operator' in rule.keys() else 'AND'

        # Upsert into thresholds (id matches composite_rule.id)
        cursor.execute("""
            INSERT OR REPLACE INTO thresholds (
                id, category, parameter, condition, value, unit, severity,
                enabled, description, sites, trigger_count, last_triggered,
                applies_to, region_id, cluster_id, site_id, location_name,
                conditions, logic_operator
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            rule_id,
            rule['category'],
            primary_condition.get('parameter', ''),
            primary_condition.get('operator', '>='),
            primary_condition.get('value', 0),
            primary_condition.get('unit', ''),
            rule['severity'],
            rule['enabled'],
            rule['name'],
            json.dumps([]),
            trigger_count,
            last_triggered,
            rule['applies_to'] if 'applies_to' in rule.keys() else 'all',
            rule['region_id'] if 'region_id' in rule.keys() else None,
            rule['cluster_id'] if 'cluster_id' in rule.keys() else None,
            rule['site_id'] if 'site_id' in rule.keys() else None,
            None,
            conditions_json,
            logic_operator
        ))

        created += 1
        print(f"  ✓ {rule_id:30s} | {rule['name']:40s} | triggers: {trigger_count}")

    conn.commit()
    close_database()

    print(f"\n{'='*70}")
    print(f"✅ Sync complete!")
    print(f"   Created {created} thresholds from composite rules")
    print(f"{'='*70}\n")

if __name__ == '__main__':
    sync_rules_to_thresholds()
