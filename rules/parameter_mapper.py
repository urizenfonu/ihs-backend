PARAMETER_MAP = {
    # Fuel
    "fuel_level": ["fuel_level", "diesel_deep_cm"],
    "fuel_drop": ["fuel_level"],

    # Grid
    "voltage": ["voltage", "voltage_l1", "voltage_l2", "voltage_l3", "grid_voltage"],
    "voltage_l1": ["voltage_l1", "voltage_phase_1"],
    "voltage_l2": ["voltage_l2", "voltage_phase_2"],
    "voltage_l3": ["voltage_l3", "voltage_phase_3"],
    "current_sum": ["current_total", "load_current", "current_l1_l2_l3_sum"],
    "grid_frequency": ["frequency", "grid_frequency"],
    "grid_power": ["grid_power_kw", "ac_power"],

    # Battery
    "battery_voltage": ["battery_voltage", "dc_voltage", "System_DC_Voltage"],
    "battery_current": ["battery_current", "dc_current", "System_DC_Current"],
    "battery_power": ["battery_power_kw", "dc_power"],

    # Solar
    "solar_current": ["solar_current", "pv_current"],
    "solar_power": ["solar_power_kw", "pv_power"],

    # Generator
    "gen_voltage": ["gen_voltage", "generator_voltage", "voltage"],
    "gen_current": ["gen_current", "generator_current"],
    "gen_frequency": ["gen_frequency", "generator_frequency", "frequency"],
    "gen_power": ["gen_power_kw", "generator_power"],

    # Temperature
    "temperature": ["temperature", "ambient_temp", "shelter_temp"],

    # Tenant
    "tenant_consumption": ["tenant_power", "load_power", "consumption_kw"],

    # Power Status
    "rectifier_power": ["rectifier_power", "output_power"]
}

def extract_value(parameter: str, reading: dict) -> float | None:
    """Extract parameter value from reading with fallback fields"""
    fields = PARAMETER_MAP.get(parameter, [parameter])
    if isinstance(fields, str):
        fields = [fields]

    for field in fields:
        if field in reading and reading[field] is not None:
            try:
                return float(reading[field])
            except (ValueError, TypeError):
                continue
    return None
