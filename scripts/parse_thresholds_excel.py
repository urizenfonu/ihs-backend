#!/usr/bin/env python3
"""
Parse thresholds from Excel file and generate default_thresholds.py

This script reads 'Alamrs and Status Conditions for New Dashboard.xlsx',
extracts threshold definitions, and generates a Python module with the data.
"""

import sys
import os
import re
import secrets
import string

# Add venv to path for openpyxl
venv_packages = os.path.join(os.path.dirname(__file__), '..', 'venv', 'lib', 'python3.13', 'site-packages')
sys.path.insert(0, venv_packages)

import openpyxl

def generate_id(size=8):
    """Generate random ID for threshold"""
    chars = string.ascii_letters + string.digits
    return ''.join(secrets.choice(chars) for _ in range(size))

def extract_operator(condition_text):
    """Extract comparison operator from condition text"""
    if not condition_text:
        return '>='

    condition_text = str(condition_text).lower()

    if '>=' in condition_text or '≥' in condition_text:
        return '>='
    elif '<=' in condition_text or '≤' in condition_text:
        return '<='
    elif ' > ' in condition_text or 'greater than' in condition_text:
        return '>'
    elif ' < ' in condition_text or 'less than' in condition_text:
        return '<'
    elif ' = ' in condition_text or 'equals' in condition_text or 'equal to' in condition_text:
        return '=='

    # Default for complex conditions
    return '>='

def map_component_to_category(component):
    """Map Excel component name to alarm category"""
    if not component:
        return 'Unknown'

    component = str(component).lower()

    if 'fuel' in component:
        return 'Fuel Sensor'
    elif 'battery' in component:
        return 'Battery'
    elif 'grid' in component:
        return 'Grid ACEM'
    elif 'gen' in component:
        return 'Gen ACEM'
    elif 'solar' in component:
        return 'Solar'
    elif 'temp' in component:
        return 'Temperature Sensor'
    elif 'power alarm' in component:
        return 'Power Alarms'
    elif 'power status' in component:
        return 'Power Status'
    elif 'power' in component:
        return 'Power Status'
    elif 'tenant' in component:
        return 'Tenant'
    else:
        return 'Unknown'

def infer_parameter(parameter_name, component):
    """Infer actual parameter name from description"""
    if not parameter_name:
        return 'unknown'

    param = str(parameter_name).lower()

    # Fuel parameters
    if 'fuel' in param:
        return 'fuel_level'

    # Battery parameters
    if 'battery' in param:
        if 'voltage' in param or 'low' in param:
            return 'battery_voltage'
        elif 'current' in param or 'charge' in param or 'discharge' in param or 'floating' in param:
            return 'battery_current'

    # Grid/Gen voltage
    if 'voltage' in param or 'available' in param or 'phase' in param:
        return 'voltage'

    # Frequency
    if 'frequency' in param:
        return 'frequency'

    # Temperature
    if 'temp' in param:
        return 'temperature'

    # Solar
    if 'solar' in param:
        return 'solar_current'

    # Power
    if 'power' in param or 'site on' in param or 'site down' in param:
        return 'power'

    # Current/Load
    if 'current' in param or 'load' in param:
        return 'load_current'

    # Tenant
    if 'tenant' in param:
        return 'tenant_consumption'

    return 'unknown'

def infer_severity(parameter_name):
    """Infer severity level from parameter name"""
    if not parameter_name:
        return 'info'

    param = str(parameter_name).lower()

    # Critical conditions
    if any(word in param for word in ['down', 'not available', 'off', 'low', 'drop']):
        return 'critical'

    # Warning conditions
    if any(word in param for word in ['high', 'refuel', 'discharge', 'charge']):
        return 'high'

    # Info/Status conditions
    if any(word in param for word in ['available', 'on', 'floating', 'status']):
        return 'info'

    return 'info'

def parse_thresholds_from_excel(excel_path):
    """Parse Excel file and return list of threshold dictionaries"""
    print(f"📊 Parsing Excel file: {excel_path}")

    wb = openpyxl.load_workbook(excel_path)
    sheet = wb.active

    thresholds = []
    row_count = 0
    last_component = None
    last_parameter_name = None

    for i, row in enumerate(sheet.iter_rows(min_row=4, values_only=True), 4):
        # Skip completely empty rows
        if not any(row):
            continue

        component = row[1]
        parameter_name = row[2]
        condition_text = row[3]
        value = row[4]
        unit = row[5]

        # Skip rows without value
        if value is None or value == '':
            continue

        # Handle continuation rows (empty component but has value)
        if component is None or component == '':
            component = last_component
            parameter_name = last_parameter_name
        else:
            last_component = component
            last_parameter_name = parameter_name

        category = map_component_to_category(component)
        parameter = infer_parameter(parameter_name, component)
        condition = extract_operator(condition_text)
        severity = infer_severity(parameter_name)

        threshold = {
            'id': f"threshold_{generate_id(8)}",
            'category': category,
            'parameter': parameter,
            'condition': condition,
            'value': float(value) if value else 0.0,
            'unit': str(unit) if unit else '',
            'severity': severity,
            'description': str(parameter_name) if parameter_name else '',
            'sites': '[]',
            'applies_to': 'all',
            'enabled': True,
            'trigger_count': 0,
            'last_triggered': None,
            'region_id': None,
            'cluster_id': None,
            'site_id': None,
            'location_name': None
        }

        thresholds.append(threshold)
        row_count += 1
        print(f"  Row {i}: {category} - {parameter_name} = {value} {unit}")

    print(f"\n✅ Parsed {row_count} thresholds")
    return thresholds

def generate_python_module(thresholds, output_path):
    """Generate default_thresholds.py module"""
    print(f"\n📝 Generating Python module: {output_path}")

    with open(output_path, 'w') as f:
        f.write('"""Default threshold definitions generated from Excel file"""\n\n')
        f.write('def get_default_thresholds():\n')
        f.write('    """Returns list of default threshold configurations"""\n')
        f.write('    return [\n')

        for threshold in thresholds:
            f.write('        {\n')
            for key, value in threshold.items():
                if isinstance(value, str):
                    f.write(f"            '{key}': '{value}',\n")
                elif isinstance(value, bool):
                    f.write(f"            '{key}': {value},\n")
                elif value is None:
                    f.write(f"            '{key}': None,\n")
                else:
                    f.write(f"            '{key}': {value},\n")
            f.write('        },\n')

        f.write('    ]\n')

    print(f"✅ Generated {len(thresholds)} threshold definitions")

def main():
    # Paths
    script_dir = os.path.dirname(os.path.abspath(__file__))
    excel_path = os.path.join(script_dir, '..', '..', 'ihs-repo', 'Alamrs and Status Conditions for New Dashboard.xlsx')
    output_path = os.path.join(script_dir, 'default_thresholds.py')

    # Verify Excel exists
    if not os.path.exists(excel_path):
        print(f"❌ Excel file not found: {excel_path}")
        sys.exit(1)

    # Parse Excel
    thresholds = parse_thresholds_from_excel(excel_path)

    # Generate Python module
    generate_python_module(thresholds, output_path)

    print(f"\n✨ Done! Module created at: {output_path}")
    print(f"\nNext steps:")
    print(f"  1. Review generated file: cat {output_path}")
    print(f"  2. Update init_system.py to import and seed thresholds")
    print(f"  3. Test: python3 scripts/init_system.py")

if __name__ == '__main__':
    main()
