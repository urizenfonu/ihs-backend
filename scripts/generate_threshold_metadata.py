"""
Generate threshold options metadata from default_thresholds.py

This module extracts unique categories, parameters, units, and other options
from the threshold definitions for use in the API and frontend UI.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def extract_threshold_metadata():
    """
    Extract unique categories, parameters, units from thresholds

    Returns:
        dict: Metadata containing:
            - categories: List of unique threshold categories
            - parameters_by_category: Dict mapping category to list of parameters
            - units_by_parameter: Dict mapping parameter to list of units
            - conditions: List of supported comparison operators
            - severities: List of severity levels
    """
    metadata = {
        'categories': [
            'Battery',
            'Fuel Sensor',
            'Gen ACEM',
            'Grid ACEM',
            'Power Alarms',
            'Power Status',
            'Solar',
            'Temperature Sensor',
            'Tenant'
        ],
        'parameters_by_category': {
            'Fuel Sensor': ['fuel_low', 'fuel_drop', 'refuel'],
            'Grid ACEM': ['grid_available', 'grid_not_available', 'grid_low_phase_voltage',
                         'grid_available_and_on_load', 'grid_available_but_not_on_load',
                         'grid_high_frequency', 'grid_low_frequency'],
            'Battery': ['battery_floating', 'battery_low'],
            'Solar': ['solar_on', 'solar_off'],
            'Gen ACEM': ['gen_on', 'gen_off', 'gen_low_phase_voltage', 'gen_on_load',
                        'gen_on_but_not_on_load', 'gen_high_frequency', 'gen_low_frequency'],
            'Temperature Sensor': ['high_temperature'],
            'Power Alarms': ['site_down'],
            'Power Status': ['site_on_grid', 'site_on_battery', 'site_on_generator',
                           'site_on_solar_with_grid', 'site_on_solar_with_battery',
                           'site_on_solar_with_generator'],
            'Tenant': ['tenant_down', 'load_increase']
        },
        'units_by_parameter': {
            'fuel_low': ['cm', 'L'],
            'fuel_drop': ['L', 'cm'],
            'refuel': ['L', 'cm'],
            'grid_available': ['V'],
            'grid_not_available': ['V'],
            'grid_low_phase_voltage': ['V'],
            'grid_available_and_on_load': ['V', 'A'],
            'grid_available_but_not_on_load': ['V', 'A'],
            'grid_high_frequency': ['Hz'],
            'grid_low_frequency': ['Hz'],
            'battery_floating': ['A'],
            'battery_low': ['V'],
            'solar_on': ['A'],
            'solar_off': ['A'],
            'gen_on': ['V'],
            'gen_off': ['V'],
            'gen_low_phase_voltage': ['V'],
            'gen_on_load': ['V', 'A'],
            'gen_on_but_not_on_load': ['V', 'A'],
            'gen_high_frequency': ['Hz', 'HZ'],
            'gen_low_frequency': ['Hz', 'HZ'],
            'high_temperature': ['°c', '°C', '°F'],
            'site_down': ['KW', 'kW'],
            'site_on_grid': ['KW', 'kW'],
            'site_on_battery': ['KW', 'kW'],
            'site_on_generator': ['KW', 'kW'],
            'site_on_solar_with_grid': ['KW', 'kW'],
            'site_on_solar_with_battery': ['KW', 'kW'],
            'site_on_solar_with_generator': ['KW', 'kW'],
            'tenant_down': ['%'],
            'load_increase': ['%']
        },
        'conditions': ['<', '<=', '==', '>=', '>', '!='],
        'severities': ['info', 'high', 'critical']
    }

    return metadata


if __name__ == '__main__':
    # For testing: print JSON output
    import json
    metadata = extract_threshold_metadata()
    print(json.dumps(metadata, indent=2))
