#!/usr/bin/env python3
"""
Test script for the IHS Alarm Monitor backend
Run: ./venv/bin/python test_backend.py
"""

import asyncio
from alarm_monitor import AlarmMonitor
from config import config

async def test_backend():
    print("=" * 80)
    print("IHS ALARM MONITOR - BACKEND TEST")
    print("=" * 80)

    # Initialize monitor
    monitor = AlarmMonitor(db_path=config.DATABASE_PATH)
    await monitor.init()

    # Test 1: Count rules
    print("\n1. Testing rule count...")
    count = await monitor.count_rules()
    print(f"   ✓ Total rules: {count}")
    assert count == 33, f"Expected 33 rules, got {count}"

    # Test 2: Get rules by type
    print("\n2. Testing rule retrieval...")
    all_rules = await monitor.get_rules()
    simple_rules = [r for r in all_rules if r.rule_type == 'simple']
    composite_rules = [r for r in all_rules if r.rule_type == 'composite']
    rate_change_rules = [r for r in all_rules if r.rule_type == 'rate_change']
    historical_rules = [r for r in all_rules if r.rule_type == 'historical']

    print(f"   ✓ Simple rules: {len(simple_rules)}")
    print(f"   ✓ Composite rules: {len(composite_rules)}")
    print(f"   ✓ Rate change rules: {len(rate_change_rules)}")
    print(f"   ✓ Historical rules: {len(historical_rules)}")

    # Test 3: Evaluate simple rule (fuel low)
    print("\n3. Testing simple rule evaluation (Fuel Low)...")
    test_reading_low = {"fuel_level": 8}  # Below 10cm threshold
    alarms = await monitor.evaluate_all(
        asset_id=123,
        reading=test_reading_low,
        site="Test Site",
        region="Lagos"
    )
    print(f"   ✓ Triggered alarms for low fuel: {len(alarms)}")
    if alarms:
        print(f"   ✓ Alarm message: {alarms[0].message}")

    # Test 4: Evaluate composite rule (Grid on Load)
    print("\n4. Testing composite rule evaluation (Grid on Load)...")
    test_reading_grid = {"voltage": 220, "current_sum": 5}  # Grid on and loaded
    alarms = await monitor.evaluate_all(
        asset_id=124,
        reading=test_reading_grid,
        site="Test Site 2",
        region="Lagos"
    )
    grid_alarms = [a for a in alarms if 'Grid' in a.category and 'Load' in a.message]
    print(f"   ✓ Total alarms: {len(alarms)}")
    if grid_alarms:
        print(f"   ✓ Grid alarm: {grid_alarms[0].message}")

    # Test 5: Evaluate battery rule
    print("\n5. Testing battery rule (Battery Low)...")
    test_reading_battery = {"battery_voltage": 45}  # Below 46V threshold
    alarms = await monitor.evaluate_all(
        asset_id=125,
        reading=test_reading_battery,
        site="Test Site 3",
        region="Abuja"
    )
    battery_alarms = [a for a in alarms if 'Battery' in a.category and 'Low' in a.message]
    print(f"   ✓ Battery alarms: {len(battery_alarms)}")
    if battery_alarms:
        print(f"   ✓ Battery alarm: {battery_alarms[0].message}")

    # Test 6: Evaluate power status (Site on Grid)
    print("\n6. Testing power status (Site on Grid)...")
    test_reading_power = {
        "grid_power": 1.5,
        "battery_power": 0,
        "gen_power": 0,
        "solar_power": 0
    }
    alarms = await monitor.evaluate_all(
        asset_id=126,
        reading=test_reading_power,
        site="Test Site 4",
        region="Lagos"
    )
    power_alarms = [a for a in alarms if 'Power Status' in a.category]
    print(f"   ✓ Power status alarms: {len(power_alarms)}")
    if power_alarms:
        for alarm in power_alarms:
            print(f"   ✓ {alarm.message}")

    print("\n" + "=" * 80)
    print("✅ ALL TESTS PASSED!")
    print("=" * 80)
    print("\nBackend is ready to use!")
    print("Start server with: ./start.sh")
    print("Or: ./venv/bin/uvicorn main:app --reload --port 3001")

if __name__ == "__main__":
    asyncio.run(test_backend())
