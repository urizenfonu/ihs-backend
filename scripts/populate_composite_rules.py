#!/usr/bin/env python3
"""
Populate composite_rules table with default rules
"""
import sqlite3
import json
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db.client import get_database, close_database

SEVERITY_MAP = {
    'Fuel Low': 'critical',
    'Fuel Drop': 'critical',
    'Refuel': 'high',
    'Grid Available': 'info',
    'Grid Not Available': 'critical',
    'Grid Low Phase Voltage': 'critical',
    'Grid Available and on Load': 'info',
    'Grid Available But Not on Load': 'info',
    'Grid High Frequency': 'high',
    'Grid Low Frequency': 'critical',
    'Battery Floating': 'info',
    'Battery Low': 'critical',
    'Solar On': 'info',
    'Solar Off': 'critical',
    'Gen On': 'info',
    'Gen Off': 'critical',
    'Gen Low Phase Voltage': 'critical',
    'Gen on Load': 'info',
    'Gen On but Not on Load': 'info',
    'Gen High Frequency': 'high',
    'Gen Low Frequency': 'critical',
    'High Temperature': 'high',
    'Site Down': 'critical',
    'Site on Grid': 'info',
    'Site on Battery': 'info',
    'Site on Generator': 'info',
    'Site on Solar with Grid': 'info',
    'Site on Solar with Battery': 'info',
    'Site on Solar with Generator': 'info',
    'Tenant Down': 'critical',
    'Load Increase': 'info',
}

COMPOSITE_RULES = [
    {
        'category': 'Fuel Sensor',
        'parameter': 'Fuel Low',
        'conditions': [{'parameter': 'fuel_depth_cm', 'operator': '<=', 'value': 10, 'unit': 'cm'}]
    },
    {
        'category': 'Fuel Sensor',
        'parameter': 'Fuel Drop',
        'conditions': [{'operator': '>', 'value': 10, 'unit': 'L'}]
    },
    {
        'category': 'Fuel Sensor',
        'parameter': 'Refuel',
        'conditions': [{'operator': '>', 'value': 20, 'unit': 'L'}]
    },
    {
        'category': 'Grid ACEM',
        'parameter': 'Grid Available',
        'conditions': [{'parameter': 'voltage', 'operator': '>=', 'value': 174, 'unit': 'V'}]
    },
    {
        'category': 'Grid ACEM',
        'parameter': 'Grid Not Available',
        'conditions': [{'parameter': 'voltage', 'operator': '<', 'value': 174, 'unit': 'V'}]
    },
    {
        'category': 'Grid ACEM',
        'parameter': 'Grid Low Phase Voltage',
        'conditions': [{'parameter': 'voltage', 'operator': '<', 'value': 174, 'unit': 'V'}]
    },
    {
        'category': 'Grid ACEM',
        'parameter': 'Grid Available and on Load',
        'conditions': [
            {'parameter': 'voltage', 'operator': '>=', 'value': 174, 'unit': 'V'},
            {'parameter': 'current_sum', 'operator': '>', 'value': 3, 'unit': 'A'}
        ]
    },
    {
        'category': 'Grid ACEM',
        'parameter': 'Grid Available But Not on Load',
        'conditions': [
            {'parameter': 'voltage', 'operator': '>=', 'value': 174, 'unit': 'V'},
            {'parameter': 'current_sum', 'operator': '<', 'value': 3, 'unit': 'A'}
        ]
    },
    {
        'category': 'Grid ACEM',
        'parameter': 'Grid High Frequency',
        'conditions': [{'parameter': 'frequency', 'operator': '>', 'value': 55, 'unit': 'Hz'}]
    },
    {
        'category': 'Grid ACEM',
        'parameter': 'Grid Low Frequency',
        'conditions': [{'parameter': 'frequency', 'operator': '<', 'value': 45, 'unit': 'Hz'}]
    },
    {
        'category': 'Battery',
        'parameter': 'Battery Floating',
        'conditions': [
            {'parameter': 'battery_current', 'operator': '>=', 'value': -3, 'unit': 'A'},
            {'parameter': 'battery_current', 'operator': '<=', 'value': 3, 'unit': 'A'}
        ]
    },
    {
        'category': 'Battery',
        'parameter': 'Battery Low',
        'conditions': [{'parameter': 'battery_voltage', 'operator': '<=', 'value': 46, 'unit': 'V'}]
    },
    {
        'category': 'Solar',
        'parameter': 'Solar On',
        'conditions': [{'parameter': 'solar_current', 'operator': '>=', 'value': 5, 'unit': 'A'}]
    },
    {
        'category': 'Solar',
        'parameter': 'Solar Off',
        'conditions': [{'parameter': 'solar_current', 'operator': '<', 'value': 5, 'unit': 'A'}]
    },
    {
        'category': 'Gen ACEM',
        'parameter': 'Gen On',
        'conditions': [{'parameter': 'voltage', 'operator': '>=', 'value': 174, 'unit': 'V'}]
    },
    {
        'category': 'Gen ACEM',
        'parameter': 'Gen Off',
        'conditions': [{'parameter': 'voltage', 'operator': '<', 'value': 174, 'unit': 'V'}]
    },
    {
        'category': 'Gen ACEM',
        'parameter': 'Gen Low Phase Voltage',
        'conditions': [{'parameter': 'voltage', 'operator': '<', 'value': 174, 'unit': 'V'}]
    },
    {
        'category': 'Gen ACEM',
        'parameter': 'Gen on Load',
        'conditions': [
            {'parameter': 'voltage', 'operator': '>=', 'value': 174, 'unit': 'V'},
            {'parameter': 'current_sum', 'operator': '>', 'value': 3, 'unit': 'A'}
        ]
    },
    {
        'category': 'Gen ACEM',
        'parameter': 'Gen On but Not on Load',
        'conditions': [
            {'parameter': 'voltage', 'operator': '>=', 'value': 174, 'unit': 'V'},
            {'parameter': 'current_sum', 'operator': '<', 'value': 3, 'unit': 'A'}
        ]
    },
    {
        'category': 'Gen ACEM',
        'parameter': 'Gen High Frequency',
        'conditions': [{'parameter': 'frequency', 'operator': '>', 'value': 55, 'unit': 'Hz'}]
    },
    {
        'category': 'Gen ACEM',
        'parameter': 'Gen Low Frequency',
        'conditions': [{'parameter': 'frequency', 'operator': '<', 'value': 45, 'unit': 'Hz'}]
    },
    {
        'category': 'Temperature Sensor',
        'parameter': 'High Temperature',
        'conditions': [{'parameter': 'equipment_temp', 'operator': '>', 'value': 30, 'unit': '°C'}]
    },
    {
        'category': 'Power Alarms',
        'parameter': 'Site Down',
        'conditions': [{'operator': '==', 'value': 0, 'unit': 'KW'}]
    },
    {
        'category': 'Power Status',
        'parameter': 'Site on Grid',
        'conditions': [{'parameter': 'grid_power', 'operator': '>', 'value': 0.6, 'unit': 'KW'}]
    },
    {
        'category': 'Power Status',
        'parameter': 'Site on Battery',
        'conditions': [
            {'parameter': 'grid_power', 'operator': '==', 'value': 0, 'unit': 'KW'},
            {'parameter': 'battery_power', 'operator': '>=', 'value': 0.6, 'unit': 'KW'},
            {'parameter': 'gen_power', 'operator': '==', 'value': 0, 'unit': 'KW'},
            {'parameter': 'solar_power', 'operator': '==', 'value': 0, 'unit': 'KW'}
        ]
    },
    {
        'category': 'Power Status',
        'parameter': 'Site on Generator',
        'conditions': [
            {'parameter': 'grid_power', 'operator': '==', 'value': 0, 'unit': 'KW'},
            {'parameter': 'battery_power', 'operator': '==', 'value': 0, 'unit': 'KW'},
            {'parameter': 'gen_power', 'operator': '>=', 'value': 0.6, 'unit': 'KW'},
            {'parameter': 'solar_power', 'operator': '==', 'value': 0, 'unit': 'KW'}
        ]
    },
    {
        'category': 'Power Status',
        'parameter': 'Site on Solar with Grid',
        'conditions': [
            {'parameter': 'grid_power', 'operator': '>=', 'value': 0.6, 'unit': 'KW'},
            {'parameter': 'battery_power', 'operator': '==', 'value': 0, 'unit': 'KW'},
            {'parameter': 'gen_power', 'operator': '==', 'value': 0, 'unit': 'KW'},
            {'parameter': 'solar_power', 'operator': '>=', 'value': 0.6, 'unit': 'KW'}
        ]
    },
    {
        'category': 'Power Status',
        'parameter': 'Site on Solar with Battery',
        'conditions': [
            {'parameter': 'grid_power', 'operator': '==', 'value': 0, 'unit': 'KW'},
            {'parameter': 'battery_power', 'operator': '>=', 'value': 0.6, 'unit': 'KW'},
            {'parameter': 'gen_power', 'operator': '==', 'value': 0, 'unit': 'KW'},
            {'parameter': 'solar_power', 'operator': '>=', 'value': 0.6, 'unit': 'KW'}
        ]
    },
    {
        'category': 'Power Status',
        'parameter': 'Site on Solar with Generator',
        'conditions': [
            {'parameter': 'grid_power', 'operator': '==', 'value': 0, 'unit': 'KW'},
            {'parameter': 'battery_power', 'operator': '==', 'value': 0, 'unit': 'KW'},
            {'parameter': 'gen_power', 'operator': '>=', 'value': 0.6, 'unit': 'KW'},
            {'parameter': 'solar_power', 'operator': '>=', 'value': 0.6, 'unit': 'KW'}
        ]
    },
    {
        'category': 'Tenant',
        'parameter': 'Tenant Down',
        'conditions': [{'operator': '<', 'value': 50, 'unit': '%'}]
    },
    {
        'category': 'Tenant',
        'parameter': 'Load Increase',
        'conditions': [{'operator': '>', 'value': 115, 'unit': '%'}]
    },
]

def generate_rule_id(parameter):
    return parameter.lower().replace(' ', '_').replace('-', '_')

def populate_composite_rules():
    print(f"🔄 Populating composite_rules table...")
    conn = get_database()
    cursor = conn.cursor()

    # These two rules were early defaults but should not be generated anymore.
    # We delete them before inserting to keep the UI clean and prevent re-adding them.
    deprecated_rule_ids = ("battery_discharge", "battery_charge")
    cursor.execute(
        f"DELETE FROM composite_rules WHERE id IN ({','.join(['?'] * len(deprecated_rule_ids))})",
        deprecated_rule_ids,
    )

    cursor.execute("SELECT COUNT(*) FROM composite_rules")
    existing_count = cursor.fetchone()[0]

    print(f"ℹ️  Found {existing_count} existing composite rules")
    print(f"Syncing {len(COMPOSITE_RULES)} default composite rules...\n")

    created = 0
    for rule_data in COMPOSITE_RULES:
        rule_id = generate_rule_id(rule_data['parameter'])
        severity = SEVERITY_MAP.get(rule_data['parameter'], 'info')
        conditions_json = json.dumps(rule_data['conditions'])
        logical_operator = 'AND' if len(rule_data['conditions']) > 1 else None

        cursor.execute("""
            INSERT OR REPLACE INTO composite_rules (
                id, name, description, severity, category, rule_type,
                enabled, conditions, logical_operator
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            rule_id,
            rule_data['parameter'],
            rule_data['parameter'],
            severity,
            rule_data['category'],
            'threshold',
            1,
            conditions_json,
            logical_operator
        ))

        created += 1
        multi_cond = '🔗' if len(rule_data['conditions']) > 1 else '  '
        print(f"  {multi_cond} {rule_id:35s} | {rule_data['parameter']:30s} | {severity:8s}")

    conn.commit()
    close_database()

    print(f"\n{'='*80}")
    print(f"✅ Sync complete!")
    print(f"   Synced {created} composite rules")
    print(f"   Multi-condition rules: {sum(1 for r in COMPOSITE_RULES if len(r['conditions']) > 1)}")
    print(f"{'='*80}\n")

if __name__ == '__main__':
    populate_composite_rules()
