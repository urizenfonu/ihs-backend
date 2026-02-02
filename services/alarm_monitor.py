from typing import Dict, Any, List, Optional
from datetime import datetime
import json
import secrets
import re
from db.repositories.threshold_repository import ThresholdRepository
from db.repositories.alarm_repository import AlarmRepository
from db.repositories.asset_repository import AssetRepository
from db.repositories.reading_repository import ReadingRepository
from db.repositories.site_repository import SiteRepository


class AlarmMonitor:
    """
    Background service that periodically evaluates asset readings against thresholds
    and generates alarms when violations are detected.
    """

    def __init__(self):
        self.threshold_repo = ThresholdRepository()
        self.alarm_repo = AlarmRepository()
        self.asset_repo = AssetRepository()
        self.reading_repo = ReadingRepository()
        self.site_repo = SiteRepository()

    def evaluate_all_assets(self):
        """
        Main evaluation loop. Checks all assets against enabled thresholds
        and creates alarms for violations.
        """
        try:
            print(f"[AlarmMonitor] Starting evaluation at {datetime.now().isoformat()}")

            # Get enabled thresholds
            thresholds = self.threshold_repo.get_enabled()
            if not thresholds:
                print("[AlarmMonitor] No enabled thresholds found")
                return

            print(f"[AlarmMonitor] Evaluating {len(thresholds)} thresholds")

            # Get all assets
            assets = self.asset_repo.get_all()
            print(f"[AlarmMonitor] Checking {len(assets)} assets")

            alarms_created = 0

            for asset in assets:
                # Get latest reading for this asset
                reading = self.reading_repo.get_latest_by_asset_id(asset['id'])
                if not reading:
                    continue

                # Parse reading data
                try:
                    reading_data = json.loads(reading['data'])
                except json.JSONDecodeError:
                    print(f"[AlarmMonitor] Failed to parse reading data for asset {asset['id']}")
                    continue

                # Get site info
                site = self.site_repo.get_by_id(asset['site_id'])
                if not site:
                    continue

                # Evaluate each threshold
                for threshold in thresholds:
                    if self._should_evaluate_threshold(threshold, asset):
                        violation = self._evaluate_threshold(threshold, reading_data)

                        if violation:
                            # Check if we should create a new alarm (deduplication)
                            if not self._is_duplicate_alarm(asset['id'], threshold['id'], threshold['severity']):
                                alarm_id = self._create_alarm(
                                    threshold,
                                    asset,
                                    site,
                                    reading,
                                    reading_data,
                                    violation
                                )

                                if alarm_id:
                                    self.threshold_repo.increment_trigger_count(threshold['id'])
                                    alarms_created += 1
                                    print(f"[AlarmMonitor] Created alarm {alarm_id} for asset {asset['name']}")

            print(f"[AlarmMonitor] Evaluation complete. Created {alarms_created} new alarms")

        except Exception as e:
            print(f"[AlarmMonitor] Error during evaluation: {str(e)}")
            import traceback
            traceback.print_exc()

    def _should_evaluate_threshold(self, threshold: Dict, asset: Dict) -> bool:
        """
        Check if a threshold should be evaluated for a given asset type.
        Maps threshold categories to relevant asset types.
        """
        category = (threshold.get("category") or "").strip()
        asset_type = (asset.get("type") or "").strip()

        category_to_asset_types = {
            # Legacy categories
            "Fuel": {"FUEL_LEVEL"},
            "Battery": {"DC_METER", "RECTIFIER"},
            "Grid": {"AC_METER"},
            "Temperature": {"RECTIFIER", "GENERATOR"},
            "Generator": {"GENERATOR", "RECTIFIER"},
            "Solar": {"DC_METER"},
            # Composite-rule derived categories (current defaults)
            "Fuel Sensor": {"FUEL_LEVEL"},
            "Grid ACEM": {"AC_METER"},
            "Gen ACEM": {"GENERATOR"},
            "Temperature Sensor": {"RECTIFIER", "GENERATOR"},
        }

        valid_types = category_to_asset_types.get(category)
        if not valid_types:
            return False
        return asset_type in valid_types

    def _evaluate_threshold(self, threshold: Dict, reading_data: Dict) -> Optional[Dict]:
        """
        Evaluate if a reading violates a threshold.
        Returns violation details if violated, None otherwise.
        """
        # Multi-condition thresholds (synced from composite_rules) live in `thresholds.conditions`.
        raw_conditions = threshold.get("conditions")
        if raw_conditions:
            try:
                conditions = json.loads(raw_conditions) if isinstance(raw_conditions, str) else raw_conditions
            except Exception:
                conditions = None

            if isinstance(conditions, list) and conditions:
                logic = (threshold.get("logic_operator") or "AND").upper()
                results = []
                last_value = None
                last_condition = None
                last_threshold_value = None

                last_unit = None
                for cond in conditions:
                    if not isinstance(cond, dict):
                        continue
                    parameter = cond.get("parameter") or threshold.get("parameter") or ""
                    value = self._extract_value(parameter, reading_data)
                    if value is None:
                        results.append(False)
                        continue

                    op = cond.get("condition") or cond.get("operator")
                    threshold_value = cond.get("value")
                    if op is None or threshold_value is None:
                        results.append(False)
                        continue

                    last_value = value
                    last_condition = op
                    last_threshold_value = threshold_value
                    last_unit = cond.get("unit")
                    results.append(self._compare(value, op, threshold_value))

                triggered = all(results) if logic == "AND" else any(results)
                if triggered and last_value is not None:
                    return {
                        "current_value": last_value,
                        "threshold_value": last_threshold_value,
                        "condition": last_condition,
                        "unit": last_unit,
                    }
                return None

        # Single-condition threshold
        parameter = threshold.get("parameter") or ""
        value = self._extract_value(parameter, reading_data)
        if value is None:
            return None

        condition = threshold.get("condition")
        threshold_value = threshold.get("value")
        if condition is None or threshold_value is None:
            return None

        if self._compare(value, condition, threshold_value):
            return {"current_value": value, "threshold_value": threshold_value, "condition": condition, "unit": threshold.get("unit")}
        return None

    def _compare(self, value: float, condition: str, threshold_value: float) -> bool:
        if condition == "<=":
            return value <= threshold_value
        if condition == "<":
            return value < threshold_value
        if condition == ">=":
            return value >= threshold_value
        if condition == ">":
            return value > threshold_value
        if condition == "==":
            return value == threshold_value
        if condition == "!=":
            return value != threshold_value
        return False

    def _extract_value(self, parameter: str, reading_data: Dict) -> Optional[float]:
        """
        Extract parameter value from reading data.
        Handles multiple possible field names for the same parameter.
        """
        if not parameter:
            return None

        def first_number(*keys: str) -> Optional[float]:
            for key in keys:
                if key in reading_data:
                    try:
                        return float(reading_data[key])
                    except (ValueError, TypeError):
                        continue
            return None

        def avg_numbers(*keys: str) -> Optional[float]:
            vals = []
            for key in keys:
                if key in reading_data:
                    try:
                        v = float(reading_data[key])
                    except (ValueError, TypeError):
                        continue
                    vals.append(v)
            return (sum(vals) / len(vals)) if vals else None

        def sum_numbers(*keys: str) -> Optional[float]:
            vals = []
            for key in keys:
                if key in reading_data:
                    try:
                        v = float(reading_data[key])
                    except (ValueError, TypeError):
                        continue
                    vals.append(v)
            return sum(vals) if vals else None

        if parameter in {"voltage", "grid_voltage", "gen_voltage"}:
            return (
                avg_numbers("voltage_1", "voltage_2", "voltage_3")
                or avg_numbers("v_l1_n", "v_l2_n", "v_l3_n")
                or avg_numbers("Phase_L1_V", "Phase_L2_V", "Phase_L3_V")
                or avg_numbers("line_1_voltage", "line_2_voltage", "line_3_voltage")
                or first_number("voltage", "Voltage", "grid_voltage", "gen_voltage")
            )

        if parameter in {"current_sum", "load_current"}:
            return (
                sum_numbers("current_1", "current_2", "current_3")
                or sum_numbers("i_l1", "i_l2", "i_l3")
                or sum_numbers("Phase_L1_Current", "Phase_L2_Current", "Phase_L3_Current")
                or sum_numbers("I_L1 (Amps)", "I_L2 (Amps)", "I_L3 (Amps)")
                or first_number("current_sum", "load_current")
            )

        if parameter in {"frequency", "grid_frequency", "gen_frequency"}:
            raw = first_number("frequency", "AC_Frequency", "grid_frequency", "gen_frequency")
            if raw is not None and raw > 100:
                return raw / 10
            return raw

        if parameter in {"fuel_depth_cm", "diesel_deep_with_offset_cm", "diesel_deep_cm"}:
            return first_number("diesel_deep_with_offset_cm", "diesel_deep_cm", "Diesel Deep With Offset (CM)", "Diesel Deep (CM)")

        if parameter in {"battery_voltage"}:
            return first_number("Voltage", "Battery_V", "System_DC_Voltage", "engine_battery_voltage", "battery_voltage", "Battery (vdc)")

        if parameter in {"battery_current"}:
            return first_number("Current1", "Total_DC_Load_Current", "irms1_batt", "battery_current")

        if parameter in {"solar_current"}:
            return first_number("Current4", "irms2_solar_y2", "solar_current")

        if parameter in {"equipment_temp"}:
            return first_number("Equipment_Area_Temperature", "equipment_temp", "temperature", "Temperature")

        # Parameter mapping with fallback field names
        param_field_map = {
            'fuel_level': ['Fuel Level (L)', 'Diesel Deep (CM)', 'Diesel Deep With Offset (CM)', 'fuel_level'],
            'battery_voltage': ['Battery (vdc)', 'battery_voltage', 'dc_voltage', 'System_DC_Voltage'],
            'coolant_temp': ['Coolant_Temperature', 'coolant_temp'],
            'oil_pressure': ['Oil_Pressure', 'oil_pressure'],
            'engine_speed': ['Engine_Speed', 'engine_speed'],
            'voltage': ['V_L1_N (VAC)', 'V_L2_N (VAC)', 'V_L3_N (VAC)', 'voltage', 'Voltage', 'voltage_l1', 'grid_voltage'],
            'temperature': ['Temperature', 'temperature'],
            'load_current': ['I_L1 (Amps)', 'I_L2 (Amps)', 'I_L3 (Amps)', 'load_current'],
            'equipment_temp': ['equipment_temp', 'temperature'],
        }

        field_names = param_field_map.get(parameter, [parameter])

        for field_name in field_names:
            if field_name in reading_data:
                try:
                    return float(reading_data[field_name])
                except (ValueError, TypeError):
                    continue

        return None

    def _is_duplicate_alarm(self, asset_id: int, threshold_id: str, severity: str) -> bool:
        """
        Check if a similar alarm already exists.
        Prevents creating multiple active/acknowledged alarms for the same issue.
        """
        from db.client import get_database

        # Query for existing active/acknowledged alarms matching this fingerprint
        db = get_database()
        cursor = db.execute(
            """
            SELECT id FROM alarms
            WHERE asset_id = ?
              AND threshold_id = ?
              AND severity = ?
              AND status IN ('active', 'acknowledged')
            LIMIT 1
            """,
            (asset_id, threshold_id, severity)
        )

        result = cursor.fetchone()
        return result is not None

    def _identify_tenant(self, name: str):
        if not name:
            return None

        name_upper = name.upper()
        patterns = [
            r'IHS_([A-Z]+)_\d+',
            r'([A-Z]+)_\d+[A-Z]?',
            r'_([A-Z]{3,})_',
        ]

        for pattern in patterns:
            match = re.search(pattern, name_upper)
            if match:
                tenant = match.group(1)
                if tenant not in ['IHS', 'GEN', 'AC', 'DC', 'BAT']:
                    return tenant

        return None

    def _create_alarm(
        self,
        threshold: Dict,
        asset: Dict,
        site: Dict,
        reading: Dict,
        reading_data: Dict,
        violation: Dict
    ) -> Optional[str]:
        try:
            alarm_id = f"alarm_{secrets.token_hex(4)}"
            current_value = violation['current_value']
            unit = violation.get('unit') or threshold.get('unit') or ''

            message = self._generate_alarm_message(threshold, asset['name'], current_value, unit)

            sensor_name = asset.get('name')
            tenant = self._identify_tenant(sensor_name) or self._identify_tenant(site.get('name', ''))
            location = site.get('zone') or site.get('region') or site.get('name')

            details = {
                'parameter': threshold['parameter'],
                'currentValue': f"{current_value:.2f}{unit}",
                'threshold': f"{violation.get('condition', threshold.get('condition'))} {violation.get('threshold_value', threshold.get('value'))}{unit}",
                'asset': asset.get('name'),
                'equipment': asset.get('name'),
                'sensor': sensor_name,
                'siteId': site.get('external_id') or site.get('id'),
                'region': site.get('region', 'Unknown'),
                'location': location,
                'description': threshold.get('description', '')
            }

            if tenant:
                details['tenant'] = tenant

            # Create alarm
            self.alarm_repo.create({
                'id': alarm_id,
                'timestamp': datetime.now().isoformat(),
                'site': site['name'],
                'region': site.get('region', 'Unknown'),
                'severity': threshold['severity'],
                'category': threshold['category'],
                'message': message,
                'status': 'active',
                'details': json.dumps(details),
                'threshold_id': threshold['id'],
                'asset_id': asset['id'],
                'reading_id': reading['id'],
                'source': 'api'
            })

            return alarm_id

        except Exception as e:
            print(f"[AlarmMonitor] Failed to create alarm: {str(e)}")
            return None

    def _generate_alarm_message(self, threshold: Dict, asset_name: str, value: float, unit: str) -> str:
        desc = threshold.get('description', threshold['parameter'])
        return f"{desc}: {value:.2f}{unit} at {asset_name}"


# Singleton instance
_alarm_monitor_instance = None

def get_alarm_monitor() -> AlarmMonitor:
    """Get or create the global AlarmMonitor instance"""
    global _alarm_monitor_instance
    if _alarm_monitor_instance is None:
        _alarm_monitor_instance = AlarmMonitor()
    return _alarm_monitor_instance
