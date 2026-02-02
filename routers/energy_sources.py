from fastapi import APIRouter
from datetime import datetime
from db.repositories.alarm_repository import AlarmRepository
from db.repositories.reading_repository import ReadingRepository
from services.ihs_sites_cache import get_cached_sites_with_assets, trigger_refresh_if_stale
from services.ihs_sync_service import get_ihs_sync_service
import json
import re

router = APIRouter()

ENERGY_KEYS = [
    'total_energy',
    'total_energy_consumption',
    'total_pvc_energy',
    'Gen_Total_Energy',
    'gen_kwh',
    'Energy1',
    'Energy2',
    'Energy3',
    'Energy4',
    'Energy5',
    't1_e',
    't2_e',
    't3_e',
    'e1_batt',
    'e2_solar_y2',
]

FUEL_LEVEL_KEYS = [
    'Fuel Level (L)',
    'Diesel Deep (CM)',
    'Diesel Deep With Offset (CM)',
    'fuel_level',
    'diesel_deep_with_offset_cm',
    'diesel_deep_cm',
]

CONSUMPTION_KEYS = [
    'Consumption (L)',
    'consumption',
    'Fuel Consumption (L)',
]

SOLAR_ENERGY_KEYS = [
    'e2_solar_y2',
    'Energy2',
]

GENERATOR_RUNTIME_KEYS = [
    'Engine_Runtime',
    'engine_run_time',
    'engine_runtime',
    'runtime_hours',
]

AC_POWER_KEYS = [
    'Total_Active_Power (kW)',
    'Total Active Power (kW)',
    'total_power_kw',
    'total_active_power',
    'total_power',
    'power_l1',
    'power_l2',
    'power_l3',
    'Power1',
    'Power2',
    'Power3',
]

def _parse_float(value) -> float:
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        cleaned = value.replace(',', '').strip()
        if cleaned.lower() in {'', 'n/a', 'na', 'none', 'null'}:
            return 0.0
        match = re.search(r'-?\d+(\.\d+)?', cleaned)
        return float(match.group(0)) if match else 0.0
    return 0.0

def _extract_optional_value(data: dict, keys: list[str]) -> float | None:
    if not isinstance(data, dict):
        return None
    for key in keys:
        if key in data:
            raw = data.get(key)
            if raw is None:
                continue
            if isinstance(raw, str) and raw.strip().lower() in {'', 'n/a', 'na', 'none', 'null'}:
                continue
            return _parse_float(raw)
    return None

def _parse_timestamp(value) -> datetime | None:
    if not value:
        return None
    if isinstance(value, (int, float)):
        try:
            return datetime.utcfromtimestamp(value)
        except (ValueError, OSError):
            return None
    if isinstance(value, str):
        cleaned = value.replace('Z', '+00:00')
        try:
            return datetime.fromisoformat(cleaned)
        except ValueError:
            pass
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f"):
            try:
                return datetime.strptime(cleaned, fmt)
            except ValueError:
                continue
    return None

def _normalize_power_kw(value) -> float:
    power = _parse_float(value)
    if power == 0:
        return 0.0
    if abs(power) >= 1000:
        power = power / 1000.0
    return max(0.0, power)

def _extract_energy_kwh(data: dict) -> float:
    if not isinstance(data, dict):
        return 0.0
    total = 0.0
    for key in ENERGY_KEYS:
        if key in data:
            value = _parse_float(data.get(key))
            # Filter common sentinel/overflow values (prevents exploding totals)
            if abs(value) >= 1_000_000.0:
                continue
            total += max(0.0, value)
    return total

def _extract_solar_energy_kwh(data: dict) -> float:
    if not isinstance(data, dict):
        return 0.0
    total = 0.0
    for key in SOLAR_ENERGY_KEYS:
        if key not in data:
            continue
        value = _parse_float(data.get(key))
        if abs(value) >= 1_000_000.0:
            continue
        total += max(0.0, value)
    return total

def _extract_generator_runtime_hours(data: dict) -> float:
    if not isinstance(data, dict):
        return 0.0
    for key in GENERATOR_RUNTIME_KEYS:
        if key in data:
            runtime = _parse_float(data.get(key))
            if runtime:
                # Some payloads report runtime in seconds (e.g. 1,193,046.4s ~= 331.4h).
                if runtime >= 100_000:
                    runtime = runtime / 3600.0
                return max(0.0, runtime)
    return 0.0

def _extract_ac_power_kw(data: dict) -> float:
    if not isinstance(data, dict):
        return 0.0
    for key in AC_POWER_KEYS:
        if key in data:
            power = _normalize_power_kw(data.get(key))
            if power:
                return power
    return 0.0

def _attach_tenant_channels(asset: dict) -> dict:
    if asset.get('config') and isinstance(asset.get('config'), dict):
        return asset

    tenant_channels = asset.get('tenant_channels')
    if not tenant_channels:
        return asset

    try:
        tenant_names = json.loads(tenant_channels)
    except (json.JSONDecodeError, TypeError):
        tenant_names = []

    if not tenant_names:
        return asset

    asset = dict(asset)
    asset['config'] = {
        'channels': [
            {'type': 'tenant', 'name': name}
            for name in tenant_names if name
        ]
    }
    return asset

def _infer_asset_type(asset: dict) -> str:
    TYPE_MAP = {
        'grid connection': 'AC_METER',
        'grid': 'AC_METER',
        'solar panel': 'DC_METER',
        'solar': 'DC_METER',
        'battery bank': 'DC_METER',
        'generator': 'GENERATOR',
        'diesel tank': 'FUEL_LEVEL',
        'fuel tank': 'FUEL_LEVEL',
        'rectifier': 'RECTIFIER',
        'cold room': 'COLD_ROOM',
        'smoke detector': 'SMOKE_DETECTOR',
        'dc meter': 'DC_METER',
        'ac meter': 'AC_METER',
    }

    asset_type = asset.get("type")
    if isinstance(asset_type, str) and asset_type:
        normalized = TYPE_MAP.get(asset_type.lower())
        if normalized:
            return normalized
        return asset_type

    name_lower = (asset.get("name") or "").lower()

    # Name-based quick wins
    if "cold room" in name_lower:
        return "COLD_ROOM"
    if "smoke" in name_lower:
        return "SMOKE_DETECTOR"
    if any(k in name_lower for k in ["diesel", "fuel", "tank"]):
        return "FUEL_LEVEL"
    if "rectifier" in name_lower:
        return "RECTIFIER"
    if any(k in name_lower for k in ["generator", "gen", "engine"]):
        return "GENERATOR"
    if any(k in name_lower for k in ["ac meter", "ac_meter", "grid"]):
        return "AC_METER"

    config = asset.get("config") if isinstance(asset.get("config"), dict) else {}
    channels = config.get("channels", []) if isinstance(config.get("channels"), list) else []

    # DC meter assets are multi-channel and include indexed channel definitions.
    if "dc meter" in name_lower:
        return "DC_METER"

    channel_types = [str(c.get("type", "")).lower() for c in channels if isinstance(c, dict)]
    channel_names = [str(c.get("name", "")).lower() for c in channels if isinstance(c, dict)]
    joined = " ".join(channel_names)

    if any(t in {"tenant", "battery", "solar"} for t in channel_types):
        return "DC_METER"

    if any("diesel" in n or "fuel" in n for n in channel_names):
        return "FUEL_LEVEL"
    if any("engine" in n or "coolant" in n for n in channel_names):
        return "GENERATOR"
    if any("rectifier" in n or "dc_output" in n for n in channel_names):
        return "RECTIFIER"
    if "voltage_l" in joined and "current_l" in joined:
        return "AC_METER"
    if "relative_humidity" in joined or "humidity" in joined or "temperature" in joined:
        return "TEMPERATURE"

    return "UNKNOWN"


def _as_json_str(value) -> str:
    if value is None:
        return "{}"
    if isinstance(value, str):
        return value
    try:
        return json.dumps(value)
    except TypeError:
        return "{}"

def transform_site(site: dict, assets: list) -> dict:
    """Transform site dict to include zone object and assets."""
    zone_val = site.get('zone', 'Lagos')
    zone_str = zone_val.get('name') if isinstance(zone_val, dict) else zone_val
    zone_str = zone_str or 'Lagos'
    zone_external_id = site.get("zone_external_id")
    zone_obj = {
        "id": int(zone_external_id) if zone_external_id is not None else 0,
        "name": zone_str,
    }

    state_value = site.get('state') or site.get('region') or zone_str

    return {
        **site,
        'zone': zone_obj,
        'asset_count': len(assets),
        'assets': [_attach_tenant_channels(asset) for asset in assets],
        'region': site.get('region') or zone_str,
        'cluster_code': site.get('cluster_code'),
        'state': state_value,
    }

@router.get("/energy-sources-with-alarms")
def get_energy_sources_with_alarms(history_hours: int = 0, include_empty: bool = True):
    """Get all sites with assets and active alarms (for frontend dashboard)"""
    alarm_repo = AlarmRepository()
    reading_repo = ReadingRepository()

    cached_sites = get_cached_sites_with_assets()
    if not cached_sites:
        sync_service = get_ihs_sync_service()
        sync_service.sync_sites_and_assets()
        cached_sites = get_cached_sites_with_assets()

    trigger_refresh_if_stale(max_age_minutes=30)

    # Get active alarms early so we can keep alarm-only sites even if they have no assets.
    alarms = alarm_repo.get_all(status='active')
    alarm_sites = {a.get("site") for a in alarms if isinstance(a, dict) and a.get("site")}

    total_cached_sites = len(cached_sites)
    sites = []
    asset_local_id_by_external_id = {}
    asset_external_id_by_local_id = {}
    for site in cached_sites:
        zone_name = site.get("zone") or site.get("region") or "Unknown"
        site_assets = []
        for asset in site.get("assets", []) if isinstance(site.get("assets"), list) else []:
            config = None
            config_raw = asset.get("config")
            if isinstance(config_raw, str) and config_raw:
                try:
                    config = json.loads(config_raw)
                except json.JSONDecodeError:
                    config = None
            external_asset_id = asset.get("external_id") or asset.get("id")
            local_asset_id = asset.get("id")
            if external_asset_id is not None and local_asset_id is not None:
                external_asset_id_int = int(external_asset_id)
                local_asset_id_int = int(local_asset_id)
                asset_local_id_by_external_id[external_asset_id_int] = local_asset_id_int
                asset_external_id_by_local_id[local_asset_id_int] = external_asset_id_int
            site_assets.append(
                {
                    "id": external_asset_id,
                    "name": asset.get("name"),
                    "type": asset.get("type"),
                    "config": config,
                    "tenant_channels": asset.get("tenant_channels"),
                }
            )

        # Filter out placeholder sites (no assets, no alarms) to keep the UI clean and fast.
        if (not include_empty) and (not site_assets) and (site.get("name") not in alarm_sites):
            continue

        region_name = site.get("region") or zone_name

        sites.append(
            {
                "id": site.get("external_id") or site.get("id"),
                "name": site.get("name"),
                "zone": zone_name,
                "region": region_name,
                "zone_external_id": site.get("zone_external_id"),
                "state": site.get("state"),
                "cluster_code": site.get("cluster_code"),
                "assets": site_assets,
            }
        )
    result = []
    asset_external_ids = []
    asset_type_by_external_id = {}
    for site in sites:
        assets = site.get('assets', []) if isinstance(site.get('assets'), list) else []
        for asset in assets:
            asset_external_id = asset.get('id')
            if asset_external_id is None:
                continue
            asset_external_id_int = int(asset_external_id)
            asset_external_ids.append(asset_external_id_int)
            asset_type_by_external_id[asset_external_id_int] = _infer_asset_type(asset)
        result.append(transform_site(site, assets))

    # Build readings from DB cache (fast). We intentionally do not fetch IoT readings per request.
    # sync_all (scheduled) is responsible for keeping readings reasonably fresh.
    readings = []
    local_asset_ids = []
    for external_id in asset_external_ids:
        local_id = asset_local_id_by_external_id.get(int(external_id))
        if local_id is not None:
            local_asset_ids.append(int(local_id))

    if local_asset_ids:
        cached_readings = reading_repo.get_recent_by_asset_ids(local_asset_ids, limit_per_asset=25)
        for row in cached_readings:
            local_asset_id = row.get("asset_id")
            if local_asset_id is None:
                continue

            # Map local asset id back to external asset id used by the frontend.
            external_asset_id = asset_external_id_by_local_id.get(int(local_asset_id))
            if external_asset_id is None:
                continue

            parsed = {}
            raw = row.get("data")
            if isinstance(raw, str) and raw:
                try:
                    parsed = json.loads(raw)
                except json.JSONDecodeError:
                    parsed = {}

            # Back-compat: if older rows stored full reading objects, unwrap `.data`.
            if isinstance(parsed, dict) and "data" in parsed and isinstance(parsed.get("data"), dict):
                parsed = parsed.get("data") or {}

            readings.append(
                {
                    "id": row.get("id"),
                    "asset_id": external_asset_id,
                    "reading_type": asset_type_by_external_id.get(int(external_asset_id), "UNKNOWN"),
                    "timestamp": row.get("timestamp"),
                    "data": _as_json_str(parsed),
                }
            )

    history_readings = readings

    # Build readings map by asset_id
    readings_map = {}
    for reading in history_readings:
        asset_id = reading.get('asset_id')
        if asset_id not in readings_map:
            readings_map[asset_id] = []
        readings_map[asset_id].append(reading)

    # Attach fuel level + derived consumption for fuel readings
    for asset_id, asset_readings in readings_map.items():
        fuel_entries = []
        for reading in asset_readings:
            data = json.loads(reading['data']) if reading.get('data') else {}
            fuel_level = _extract_optional_value(data, FUEL_LEVEL_KEYS)
            if fuel_level is not None:
                reading['fuel_level'] = fuel_level
                timestamp = _parse_timestamp(reading.get('timestamp'))
                if timestamp:
                    fuel_entries.append((timestamp, reading, fuel_level))

            consumption = _extract_optional_value(data, CONSUMPTION_KEYS)
            if consumption is not None:
                reading['consumption'] = consumption

        if len(fuel_entries) < 2:
            continue

        fuel_entries.sort(key=lambda item: item[0])
        for idx in range(1, len(fuel_entries)):
            prev_time, prev_reading, prev_level = fuel_entries[idx - 1]
            curr_time, curr_reading, curr_level = fuel_entries[idx]
            if curr_reading.get('consumption') is not None:
                continue
            hours = (curr_time - prev_time).total_seconds() / 3600.0
            if hours <= 0:
                continue
            delta = prev_level - curr_level
            if delta <= 0:
                continue
            curr_reading['consumption'] = delta / hours

    total_energy_kwh = 0.0
    solar_energy_kwh = 0.0
    generator_runtime_delta = 0.0
    ac_readings_total = 0
    ac_readings_online = 0

    def safe_delta(latest_val: float, oldest_val: float) -> float:
        if latest_val <= 0:
            return 0.0
        if oldest_val > 0 and latest_val >= oldest_val:
            return latest_val - oldest_val
        # Counter reset or missing oldest; treat latest as period contribution (best-effort).
        return latest_val

    for asset_id, asset_readings in readings_map.items():
        if not asset_readings:
            continue
        # readings_repo already returns `ORDER BY asset_id, id DESC`, so asset_readings are newest->oldest.
        latest = asset_readings[0]
        oldest = asset_readings[-1] if len(asset_readings) > 1 else None
        data = json.loads(latest['data']) if latest.get('data') else {}
        reading_type = latest.get('reading_type')
        latest_energy = _extract_energy_kwh(data)
        oldest_energy = 0.0
        if oldest:
            oldest_data = json.loads(oldest['data']) if oldest.get('data') else {}
            oldest_energy = _extract_energy_kwh(oldest_data)
        total_energy_kwh += safe_delta(latest_energy, oldest_energy)

        if reading_type == 'DC_METER':
            latest_solar = _extract_solar_energy_kwh(data)
            oldest_solar = 0.0
            if oldest:
                oldest_data = json.loads(oldest['data']) if oldest.get('data') else {}
                oldest_solar = _extract_solar_energy_kwh(oldest_data)
            solar_energy_kwh += safe_delta(latest_solar, oldest_solar)
        elif reading_type == 'GENERATOR':
            latest_runtime = _extract_generator_runtime_hours(data)
            if oldest:
                oldest_data = json.loads(oldest['data']) if oldest.get('data') else {}
                oldest_runtime = _extract_generator_runtime_hours(oldest_data)
                if latest_runtime >= oldest_runtime > 0:
                    generator_runtime_delta += (latest_runtime - oldest_runtime)
                elif latest_runtime > 0:
                    generator_runtime_delta += latest_runtime
            elif latest_runtime > 0:
                generator_runtime_delta += latest_runtime
        elif reading_type == 'AC_METER':
            for r in asset_readings:
                r_data = json.loads(r['data']) if r.get('data') else {}
                ac_readings_total += 1
                if _extract_ac_power_kw(r_data) > 0:
                    ac_readings_online += 1

    grid_uptime_percent = (ac_readings_online / ac_readings_total * 100) if ac_readings_total else 0.0
    solar_contribution_percent = (solar_energy_kwh / total_energy_kwh * 100) if total_energy_kwh else 0.0

    returned_sites = len(result)
    returned_assets = len(asset_external_ids)
    return {
        'sites': result,
        'readings': readings_map,
        'alarms': alarms,
        'summary': {
            'totalEnergyKwh': total_energy_kwh,
            'gridUptimePercent': grid_uptime_percent,
            'generatorRuntimeHours': generator_runtime_delta,
            'solarContributionPercent': solar_contribution_percent,
        },
        'metadata': {
            'timestamp': __import__('time').time() * 1000,
            'source': 'db_cache',
            'totalSites': total_cached_sites,
            'returnedSites': returned_sites,
            'returnedAssets': returned_assets,
        }
    }
