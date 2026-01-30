"""
Seed 33 alarm rules from Excel specification into composite_rules table
Run: python seed_rules.py
"""

RULES = [
    # ========== SIMPLE RULES (21) ==========

    # Fuel Sensor (1)
    {
        "id": "fuel_low",
        "name": "Fuel Low",
        "description": "When Fuel Level is <= 10 cm",
        "category": "Fuel Sensor",
        "severity": "critical",
        "rule_type": "simple",
        "enabled": True,
        "conditions": [{"parameter": "fuel_level", "operator": "<=", "value": 10, "unit": "cm"}],
        "logical_operator": None,
        "time_window_minutes": None,
        "aggregation_type": None
    },

    # Grid ACEM (3)
    {
        "id": "grid_available",
        "name": "Grid Available",
        "description": "When one or more phase voltages >= 174V",
        "category": "Grid ACEM",
        "severity": "info",
        "rule_type": "simple",
        "enabled": True,
        "conditions": [{"parameter": "voltage", "operator": ">=", "value": 174, "unit": "V"}],
        "logical_operator": None
    },
    {
        "id": "grid_not_available",
        "name": "Grid Not Available",
        "description": "When All phase voltages < 174V",
        "category": "Grid ACEM",
        "severity": "critical",
        "rule_type": "simple",
        "enabled": True,
        "conditions": [{"parameter": "voltage", "operator": "<", "value": 174, "unit": "V"}],
        "logical_operator": None
    },
    {
        "id": "grid_low_phase",
        "name": "Grid Low Phase Voltage",
        "description": "When one or two phase voltages < 174V",
        "category": "Grid ACEM",
        "severity": "warning",
        "rule_type": "simple",
        "enabled": True,
        "conditions": [{"parameter": "voltage", "operator": "<", "value": 174, "unit": "V"}],
        "logical_operator": None
    },

    # Battery (4)
    {
        "id": "battery_low",
        "name": "Battery Low",
        "description": "When Battery Voltage <= 46V",
        "category": "Battery",
        "severity": "critical",
        "rule_type": "simple",
        "enabled": True,
        "conditions": [{"parameter": "battery_voltage", "operator": "<=", "value": 46, "unit": "V"}],
        "logical_operator": None
    },
    {
        "id": "battery_discharge",
        "name": "Battery Discharge",
        "description": "When Battery Current is < -3A",
        "category": "Battery",
        "severity": "info",
        "rule_type": "simple",
        "enabled": True,
        "conditions": [{"parameter": "battery_current", "operator": "<", "value": -3, "unit": "A"}],
        "logical_operator": None
    },
    {
        "id": "battery_charge",
        "name": "Battery Charge",
        "description": "When Battery Current is > 3A",
        "category": "Battery",
        "severity": "info",
        "rule_type": "simple",
        "enabled": True,
        "conditions": [{"parameter": "battery_current", "operator": ">", "value": 3, "unit": "A"}],
        "logical_operator": None
    },
    {
        "id": "battery_floating",
        "name": "Battery Floating",
        "description": "When -3 <= Battery Current <= 3A",
        "category": "Battery",
        "severity": "info",
        "rule_type": "composite",
        "enabled": True,
        "conditions": [
            {"parameter": "battery_current", "operator": ">=", "value": -3, "unit": "A"},
            {"parameter": "battery_current", "operator": "<=", "value": 3, "unit": "A"}
        ],
        "logical_operator": "AND"
    },

    # Solar (2)
    {
        "id": "solar_on",
        "name": "Solar On",
        "description": "Solar Current >= 5A",
        "category": "Solar",
        "severity": "info",
        "rule_type": "simple",
        "enabled": True,
        "conditions": [{"parameter": "solar_current", "operator": ">=", "value": 5, "unit": "A"}],
        "logical_operator": None
    },
    {
        "id": "solar_off",
        "name": "Solar Off",
        "description": "Solar Current < 5A",
        "category": "Solar",
        "severity": "info",
        "rule_type": "simple",
        "enabled": True,
        "conditions": [{"parameter": "solar_current", "operator": "<", "value": 5, "unit": "A"}],
        "logical_operator": None
    },

    # Gen ACEM (3)
    {
        "id": "gen_on",
        "name": "Gen On",
        "description": "When one or more phase voltages >= 174V",
        "category": "Gen ACEM",
        "severity": "info",
        "rule_type": "simple",
        "enabled": True,
        "conditions": [{"parameter": "gen_voltage", "operator": ">=", "value": 174, "unit": "V"}],
        "logical_operator": None
    },
    {
        "id": "gen_off",
        "name": "Gen Off",
        "description": "When All phase voltages < 174V",
        "category": "Gen ACEM",
        "severity": "info",
        "rule_type": "simple",
        "enabled": True,
        "conditions": [{"parameter": "gen_voltage", "operator": "<", "value": 174, "unit": "V"}],
        "logical_operator": None
    },
    {
        "id": "gen_low_phase",
        "name": "Gen Low Phase Voltage",
        "description": "When one or more phase voltages < 174V and > 0V",
        "category": "Gen ACEM",
        "severity": "warning",
        "rule_type": "composite",
        "enabled": True,
        "conditions": [
            {"parameter": "gen_voltage", "operator": "<", "value": 174, "unit": "V"},
            {"parameter": "gen_voltage", "operator": ">", "value": 0, "unit": "V"}
        ],
        "logical_operator": "AND"
    },

    # Temperature (1)
    {
        "id": "high_temperature",
        "name": "High Temperature",
        "description": "When temperature > 30 Degrees C",
        "category": "Temperature Sensor",
        "severity": "warning",
        "rule_type": "simple",
        "enabled": True,
        "conditions": [{"parameter": "temperature", "operator": ">", "value": 30, "unit": "°C"}],
        "logical_operator": None
    },

    # Power Alarms (1)
    {
        "id": "site_down",
        "name": "Site Down",
        "description": "Rectifier with No Power",
        "category": "Power Alarms",
        "severity": "critical",
        "rule_type": "simple",
        "enabled": True,
        "conditions": [{"parameter": "rectifier_power", "operator": "==", "value": 0, "unit": "KW"}],
        "logical_operator": None
    },

    # Grid Frequency (2)
    {
        "id": "grid_high_frequency",
        "name": "Grid High Frequency",
        "description": "When Grid Frequency > 55 Hz",
        "category": "Grid ACEM",
        "severity": "warning",
        "rule_type": "simple",
        "enabled": True,
        "conditions": [{"parameter": "grid_frequency", "operator": ">", "value": 55, "unit": "Hz"}],
        "logical_operator": None
    },
    {
        "id": "grid_low_frequency",
        "name": "Grid Low Frequency",
        "description": "When Grid Frequency < 45 Hz and Grid is Available",
        "category": "Grid ACEM",
        "severity": "warning",
        "rule_type": "simple",
        "enabled": True,
        "conditions": [{"parameter": "grid_frequency", "operator": "<", "value": 45, "unit": "Hz"}],
        "logical_operator": None
    },

    # Gen Frequency (2)
    {
        "id": "gen_high_frequency",
        "name": "Gen High Frequency",
        "description": "When Grid Frequency > 55 Hz",
        "category": "Gen ACEM",
        "severity": "warning",
        "rule_type": "simple",
        "enabled": True,
        "conditions": [{"parameter": "gen_frequency", "operator": ">", "value": 55, "unit": "Hz"}],
        "logical_operator": None
    },
    {
        "id": "gen_low_frequency",
        "name": "Gen Low Frequency",
        "description": "When Grid Frequency < 45 Hz AND Grid is Available",
        "category": "Gen ACEM",
        "severity": "warning",
        "rule_type": "simple",
        "enabled": True,
        "conditions": [{"parameter": "gen_frequency", "operator": "<", "value": 45, "unit": "HZ"}],
        "logical_operator": None
    },

    # ========== MULTI-CONDITION RULES (10) ==========

    # Grid on Load (2)
    {
        "id": "grid_on_load",
        "name": "Grid Available and on Load",
        "description": "Voltage >= 174V AND Current > 3A",
        "category": "Grid ACEM",
        "severity": "info",
        "rule_type": "composite",
        "enabled": True,
        "conditions": [
            {"parameter": "voltage", "operator": ">=", "value": 174, "unit": "V"},
            {"parameter": "current_sum", "operator": ">", "value": 3, "unit": "A"}
        ],
        "logical_operator": "AND"
    },
    {
        "id": "grid_available_not_on_load",
        "name": "Grid Available But Not on Load",
        "description": "Voltage >= 174V AND Current < 3A",
        "category": "Grid ACEM",
        "severity": "info",
        "rule_type": "composite",
        "enabled": True,
        "conditions": [
            {"parameter": "voltage", "operator": ">=", "value": 174, "unit": "V"},
            {"parameter": "current_sum", "operator": "<", "value": 3, "unit": "A"}
        ],
        "logical_operator": "AND"
    },

    # Gen on Load (2)
    {
        "id": "gen_on_load",
        "name": "Gen on Load",
        "description": "Gen Voltage >= 174V AND Current > 3A",
        "category": "Gen ACEM",
        "severity": "info",
        "rule_type": "composite",
        "enabled": True,
        "conditions": [
            {"parameter": "gen_voltage", "operator": ">=", "value": 174, "unit": "V"},
            {"parameter": "gen_current", "operator": ">", "value": 3, "unit": "A"}
        ],
        "logical_operator": "AND"
    },
    {
        "id": "gen_on_not_on_load",
        "name": "Gen On but Not on Load",
        "description": "Gen Voltage >= 174V AND Current < 3A",
        "category": "Gen ACEM",
        "severity": "info",
        "rule_type": "composite",
        "enabled": True,
        "conditions": [
            {"parameter": "gen_voltage", "operator": ">=", "value": 174, "unit": "V"},
            {"parameter": "gen_current", "operator": "<", "value": 3, "unit": "A"}
        ],
        "logical_operator": "AND"
    },

    # Power Status (6) - Complex 4-condition rules
    {
        "id": "site_on_grid",
        "name": "Site on Grid",
        "description": "Grid > 0.6KW AND Battery = 0 AND Gen = 0 AND Solar = 0",
        "category": "Power Status",
        "severity": "info",
        "rule_type": "composite",
        "enabled": True,
        "conditions": [
            {"parameter": "grid_power", "operator": ">", "value": 0.6, "unit": "KW"},
            {"parameter": "battery_power", "operator": "==", "value": 0, "unit": "KW"},
            {"parameter": "gen_power", "operator": "==", "value": 0, "unit": "KW"},
            {"parameter": "solar_power", "operator": "==", "value": 0, "unit": "KW"}
        ],
        "logical_operator": "AND"
    },
    {
        "id": "site_on_battery",
        "name": "Site on Battery",
        "description": "Battery >= 0.6KW AND Grid = 0 AND Gen = 0 AND Solar = 0",
        "category": "Power Status",
        "severity": "info",
        "rule_type": "composite",
        "enabled": True,
        "conditions": [
            {"parameter": "grid_power", "operator": "==", "value": 0, "unit": "KW"},
            {"parameter": "battery_power", "operator": ">=", "value": 0.6, "unit": "KW"},
            {"parameter": "gen_power", "operator": "==", "value": 0, "unit": "KW"},
            {"parameter": "solar_power", "operator": "==", "value": 0, "unit": "KW"}
        ],
        "logical_operator": "AND"
    },
    {
        "id": "site_on_generator",
        "name": "Site on Generator",
        "description": "Gen >= 0.6KW AND Grid = 0 AND Battery = 0 AND Solar = 0",
        "category": "Power Status",
        "severity": "info",
        "rule_type": "composite",
        "enabled": True,
        "conditions": [
            {"parameter": "grid_power", "operator": "==", "value": 0, "unit": "KW"},
            {"parameter": "battery_power", "operator": "==", "value": 0, "unit": "KW"},
            {"parameter": "gen_power", "operator": ">=", "value": 0.6, "unit": "KW"},
            {"parameter": "solar_power", "operator": "==", "value": 0, "unit": "KW"}
        ],
        "logical_operator": "AND"
    },
    {
        "id": "site_on_solar_with_grid",
        "name": "Site on Solar with Grid",
        "description": "Solar >= 0.6KW AND Grid >= 0.6KW AND Battery = 0 AND Gen = 0",
        "category": "Power Status",
        "severity": "info",
        "rule_type": "composite",
        "enabled": True,
        "conditions": [
            {"parameter": "grid_power", "operator": ">=", "value": 0.6, "unit": "KW"},
            {"parameter": "battery_power", "operator": "==", "value": 0, "unit": "KW"},
            {"parameter": "gen_power", "operator": "==", "value": 0, "unit": "KW"},
            {"parameter": "solar_power", "operator": ">=", "value": 0.6, "unit": "KW"}
        ],
        "logical_operator": "AND"
    },
    {
        "id": "site_on_solar_with_battery",
        "name": "Site on Solar with Battery",
        "description": "Solar >= 0.6KW AND Battery >= 0.6KW AND Grid = 0 AND Gen = 0",
        "category": "Power Status",
        "severity": "info",
        "rule_type": "composite",
        "enabled": True,
        "conditions": [
            {"parameter": "grid_power", "operator": "==", "value": 0, "unit": "KW"},
            {"parameter": "battery_power", "operator": ">=", "value": 0.6, "unit": "KW"},
            {"parameter": "gen_power", "operator": "==", "value": 0, "unit": "KW"},
            {"parameter": "solar_power", "operator": ">=", "value": 0.6, "unit": "KW"}
        ],
        "logical_operator": "AND"
    },
    {
        "id": "site_on_solar_with_generator",
        "name": "Site on Solar with Generator",
        "description": "Solar >= 0.6KW AND Gen >= 0.6KW AND Grid = 0 AND Battery = 0",
        "category": "Power Status",
        "severity": "info",
        "rule_type": "composite",
        "enabled": True,
        "conditions": [
            {"parameter": "grid_power", "operator": "==", "value": 0, "unit": "KW"},
            {"parameter": "battery_power", "operator": "==", "value": 0, "unit": "KW"},
            {"parameter": "gen_power", "operator": ">=", "value": 0.6, "unit": "KW"},
            {"parameter": "solar_power", "operator": ">=", "value": 0.6, "unit": "KW"}
        ],
        "logical_operator": "AND"
    },

    # ========== RATE-OF-CHANGE RULES (2) ==========

    {
        "id": "fuel_drop",
        "name": "Fuel Drop",
        "description": "When Fuel Level Drop more than 10L at once",
        "category": "Fuel Sensor",
        "severity": "warning",
        "rule_type": "rate_change",
        "enabled": True,
        "conditions": [{"parameter": "fuel_level", "operator": ">", "value": 10, "unit": "L"}],
        "logical_operator": None
    },
    {
        "id": "refuel",
        "name": "Refuel",
        "description": "When there is increase in Fuel Level >= 20L",
        "category": "Fuel Sensor",
        "severity": "info",
        "rule_type": "rate_change",
        "enabled": True,
        "conditions": [{"parameter": "fuel_level", "operator": ">=", "value": 20, "unit": "L"}],
        "logical_operator": None
    },

    # ========== HISTORICAL AGGREGATION RULES (2) ==========

    {
        "id": "tenant_down",
        "name": "Tenant Down",
        "description": "Tenant Consumption Average Last 3 Days < 50%",
        "category": "Tenant",
        "severity": "critical",
        "rule_type": "historical",
        "enabled": True,
        "conditions": [{"parameter": "tenant_consumption", "operator": "<", "value": 50, "unit": "%"}],
        "logical_operator": None,
        "time_window_minutes": 4320,  # 3 days
        "aggregation_type": "avg"
    },
    {
        "id": "load_increase",
        "name": "Load Increase",
        "description": "Tenant Consumption Last 3 Days > 115%",
        "category": "Tenant",
        "severity": "warning",
        "rule_type": "historical",
        "enabled": True,
        "conditions": [{"parameter": "tenant_consumption", "operator": ">", "value": 115, "unit": "%"}],
        "logical_operator": None,
        "time_window_minutes": 4320,  # 3 days
        "aggregation_type": "avg"
    }
]

# Total: 33 rules
# Simple: 21
# Composite: 10
# Rate Change: 2
# Historical: 2

async def seed_database():
    """Seed the database with all 33 rules"""
    from database import Database
    from config import config

    db = Database(config.DATABASE_PATH)
    await db.init_schema()

    print(f"Seeding {len(RULES)} rules into database...")

    for rule in RULES:
        await db.insert_rule(rule)
        print(f"  ✓ {rule['id']}: {rule['name']}")

    print(f"\n✅ Successfully seeded {len(RULES)} rules!")

    # Verify
    count = await db.count_rules()
    print(f"✅ Database now contains {count} rules")

if __name__ == "__main__":
    import asyncio

    print(f"Total rules defined: {len(RULES)}")
    print("\nBreakdown:")
    print(f"  Simple: {len([r for r in RULES if r['rule_type'] == 'simple'])}")
    print(f"  Composite: {len([r for r in RULES if r['rule_type'] == 'composite'])}")
    print(f"  Rate Change: {len([r for r in RULES if r['rule_type'] == 'rate_change'])}")
    print(f"  Historical: {len([r for r in RULES if r['rule_type'] == 'historical'])}")
    print()

    # Seed database
    asyncio.run(seed_database())
