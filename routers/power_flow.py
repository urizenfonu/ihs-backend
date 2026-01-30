from __future__ import annotations

import json
import random
from typing import Any, Dict, Iterable, List, Optional, Tuple

from fastapi import APIRouter, HTTPException, Query

from db.client import get_database
from db.repositories.reading_repository import ReadingRepository

router = APIRouter()

POWER_THRESHOLD_KW = 1.0

_MAX_SQL_VARS = 900  # keep under SQLite's default 999 variable limit


def _chunks(items: List[int], size: int) -> Iterable[List[int]]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        if isinstance(value, bool):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _pick(data: Dict[str, Any], keys: List[str], default: float = 0.0) -> float:
    for key in keys:
        if key in data:
            v = _to_float(data.get(key), None)
            if v is None:
                continue
            return v
    return default


def _avg(values: List[float]) -> float:
    vals = [v for v in values if v is not None]
    if not vals:
        return 0.0
    return sum(vals) / len(vals)


def _resolve_site_ids(
    region: Optional[str],
    state: Optional[str],
    site: Optional[str],
    sample_size: int,
) -> Tuple[List[int], Optional[str], Optional[str]]:
    db = get_database()

    if site:
        row = db.execute(
            "SELECT id, name FROM sites WHERE name = ? COLLATE NOCASE LIMIT 1", (site,)
        ).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail=f"Site '{site}' not found")
        return [int(row["id"])], None, str(row["name"])

    sample_size = max(1, int(sample_size))

    # Prefer sites that actually have readings (ordered by most recent reading id).
    # This prevents zero-only samples when a region has many sites without fresh telemetry.
    where: List[str] = []
    params_list: List[Any] = []
    label: Optional[str] = None
    state_label: Optional[str] = None

    if region:
        where.append("(s.region = ? OR s.zone = ?)")
        params_list.extend([region, region])
        label = region

    if state:
        where.append("s.state = ?")
        params_list.append(state)
        state_label = state

    where_clause = ("WHERE " + " AND ".join(where)) if where else ""
    params: Tuple[Any, ...] = tuple(params_list)

    cursor = db.execute(
        f"""
        SELECT a.site_id AS site_id
        FROM readings r
        JOIN assets a ON r.asset_id = a.id
        JOIN sites s ON a.site_id = s.id
        {where_clause}
        GROUP BY a.site_id
        ORDER BY MAX(r.id) DESC
        LIMIT ?
        """,
        (*params, sample_size),
    )

    site_ids = [int(r["site_id"]) for r in cursor.fetchall()]

    if not site_ids:
        # Fall back to sites list if there are no readings yet.
        cursor2 = db.execute(
            f"SELECT id FROM sites s {where_clause}",
            params,
        )
        site_ids = [int(r["id"]) for r in cursor2.fetchall()][:sample_size]

    if not site_ids:
        raise HTTPException(status_code=404, detail="No sites matched the requested scope")

    return site_ids, state_label, label



def _parse_config(raw: Any) -> Dict[str, Any]:
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str) and raw:
        try:
            return json.loads(raw)
        except Exception:
            return {}
    return {}


def _normalize_kw(value: Any) -> float:
    kw = _to_float(value, 0.0)
    if abs(kw) >= 1000:
        kw = kw / 1000.0
    return kw


def _get_assets_for_sites(site_ids: List[int]) -> Dict[int, Dict[str, Any]]:
    """Return mapping asset_id -> asset info for assets belonging to site_ids."""
    db = get_database()
    asset_site: Dict[int, Dict[str, Any]] = {}
    for chunk in _chunks(site_ids, _MAX_SQL_VARS):
        placeholders = ",".join(["?"] * len(chunk))
        cursor = db.execute(
            f"SELECT id, site_id, type, name, config FROM assets WHERE site_id IN ({placeholders})",
            tuple(chunk),
        )
        for row in cursor.fetchall():
            asset_site[int(row["id"])] = {
                "site_id": int(row["site_id"]),
                "type": str(row["type"] or ""),
                "name": str(row["name"] or ""),
                "config": _parse_config(row["config"]),
            }
    return asset_site


@router.get("/power-flow")
def get_power_flow(
    region: Optional[str] = Query(default=None),
    state: Optional[str] = Query(default=None),
    site: Optional[str] = Query(default=None),
    sample_size: int = Query(default=250, ge=1, le=2000),
):
    """Power flow snapshot.

    - If `site` is provided: returns site-level snapshot.
    - Else if `region` is provided: returns a sampled regional aggregation.
    - Else: returns a sampled nationwide aggregation.

    Note: sampling is used to avoid SQLite variable limits and heavy fan-out.
    """

    reading_repo = ReadingRepository()

    site_ids, state_label, _label = _resolve_site_ids(region=region, state=state, site=site, sample_size=sample_size)
    asset_site = _get_assets_for_sites(site_ids)
    asset_ids = list(asset_site.keys())

    # Fetch latest readings in batches to avoid SQLite variable limit.
    readings: List[Dict[str, Any]] = []
    for chunk in _chunks(asset_ids, _MAX_SQL_VARS):
        readings.extend(reading_repo.get_latest_by_asset_ids(chunk))

    # Per-site aggregation (lets us compute availability ratios)
    per_site: Dict[int, Dict[str, Any]] = {
        sid: {
            "grid_available": False,
            "grid_voltage": 0.0,
            "grid_frequency": 0.0,
            "grid_power": 0.0,
            "gen_power": 0.0,
            "solar_power": 0.0,
            "solar_current": 0.0,
            "solar_voltage": 0.0,
            "battery_net_kw": 0.0,
            "battery_voltage": 0.0,
            "battery_current": 0.0,
            "battery_soc": None,
            "rectifier_kw": 0.0,
            "rectifier_dc_v": 0.0,
            "fuel_level": 0.0,
            "tenant_load_kw": 0.0,
            "tenant_loads": {},
        }
        for sid in site_ids
    }

    for reading in readings:
        asset_id = int(reading.get("asset_id") or 0)
        asset_info = asset_site.get(asset_id)
        if not asset_info:
            continue
        site_id = asset_info.get("site_id")
        if not site_id or site_id not in per_site:
            continue

        raw = reading.get("data")
        try:
            data = json.loads(raw) if isinstance(raw, str) else (raw or {})
        except Exception:
            continue
        if not isinstance(data, dict):
            continue

        reading_type = str(reading.get("reading_type") or asset_info.get("type") or "").upper()
        site_bucket = per_site[site_id]

        if reading_type == "AC_METER":
            v1 = _pick(data, ["voltage_1", "voltage_l1", "Voltage_1 (VAC)", "V_L1_N", "V_L1_N (VAC)"])
            v2 = _pick(data, ["voltage_2", "voltage_l2", "Voltage_2 (VAC)", "V_L2_N", "V_L2_N (VAC)"])
            v3 = _pick(data, ["voltage_3", "voltage_l3", "Voltage_3 (VAC)", "V_L3_N", "V_L3_N (VAC)"])
            avg_v = _avg([v for v in [v1, v2, v3] if v > 0])

            freq = _pick(data, ["frequency", "Frequency (Hz)", "AC_Frequency"], 0.0)
            p_kw = _pick(
                data,
                [
                    "total_active_power",
                    "total_power_kw",
                    "total_power",
                    "Total_Active_Power (kW)",
                    "Total Active Power (kW)",
                    "Total_Active_Power (kw)",
                    "active_power_1",
                    "active_power_2",
                    "active_power_3",
                ],
                0.0,
            )

            if avg_v > 0:
                site_bucket["grid_voltage"] = avg_v
            if freq > 0:
                site_bucket["grid_frequency"] = freq

            site_bucket["grid_power"] += max(0.0, _normalize_kw(p_kw))
            if avg_v >= 174:
                site_bucket["grid_available"] = True

        elif reading_type == "GENERATOR":
            # Some generator meters report AC-like fields (same schema as AC meter)
            p_kw = _pick(
                data,
                [
                    "power_kw",
                    "gen_total_watt",
                    "Gen_Total_Power",
                    "total_power_kw",
                    "total_active_power",
                    "Total Power (KW)",
                    "Total_Active_Power (kW)",
                    "Total_Active_Power (kw)",
                    "P_SUM",
                    "p1",
                    "p2",
                    "p3",
                    "P1",
                    "P2",
                    "P3",
                ],
                0.0,
            )
            if p_kw == 0.0:
                p1 = _pick(data, ["p1", "P1"], 0.0)
                p2 = _pick(data, ["p2", "P2"], 0.0)
                p3 = _pick(data, ["p3", "P3"], 0.0)
                p_kw = p1 + p2 + p3
            site_bucket["gen_power"] += max(0.0, _normalize_kw(p_kw))

        elif reading_type == "DC_METER":
            asset_name = str(asset_info.get("name") or "").lower()
            config = asset_info.get("config") or {}
            channels = config.get("channels") if isinstance(config.get("channels"), list) else []

            system_v = _pick(data, ["Voltage", "System_DC_Voltage", "dc_voltage"], 0.0)

            # Channel-aware mapping (preferred when config provides indices)
            if channels:
                for ch in channels:
                    if not isinstance(ch, dict):
                        continue
                    ch_type = str(ch.get("type") or "").lower()
                    ch_name = str(ch.get("name") or "")
                    ch_index = ch.get("index")
                    if not isinstance(ch_index, int):
                        continue

                    p_raw = data.get(f"Power{ch_index}")
                    c_raw = data.get(f"Current{ch_index}")
                    power_kw = _normalize_kw(p_raw)
                    current = _to_float(c_raw, 0.0)

                    if ch_type == "battery":
                        site_bucket["battery_net_kw"] += power_kw
                        if system_v > 0:
                            site_bucket["battery_voltage"] = max(site_bucket["battery_voltage"], system_v)
                        if current > 0:
                            site_bucket["battery_current"] = max(site_bucket["battery_current"], current)
                    elif ch_type == "solar":
                        site_bucket["solar_power"] += abs(power_kw)
                        if system_v > 0:
                            site_bucket["solar_voltage"] = max(site_bucket["solar_voltage"], system_v)
                        if current > 0:
                            site_bucket["solar_current"] = max(site_bucket["solar_current"], current)
                    elif ch_type == "tenant":
                        if power_kw != 0:
                            site_bucket["tenant_load_kw"] += abs(power_kw)
                            site_bucket["tenant_loads"][ch_name or f"Tenant {ch_index}"] = abs(power_kw)
                continue

            # Fallback schema: dedicated battery/solar DC meter assets
            batt_kw = _normalize_kw(_pick(data, ["p1_batt", "battery_power", "Battery_Power", "Power1"], 0.0))
            solar_kw = _normalize_kw(_pick(data, ["p2_solar_y2", "solar_power", "Solar_Power", "Power2"], 0.0))

            batt_v = _pick(data, ["vrms1_batt", "battery_voltage", "Battery_V", "Battery"], 0.0)
            solar_v = _pick(data, ["vrms2_solar_y2", "vrms1_batt"], 0.0)

            batt_i = _pick(data, ["irms1_batt", "battery_current", "Current1"], 0.0)
            solar_i = _pick(data, ["irms2_solar_y2", "Current2"], 0.0)

            if "solar" in asset_name and solar_v == 0.0:
                solar_v = batt_v
            if "battery" in asset_name and batt_v == 0.0:
                batt_v = solar_v

            site_bucket["battery_net_kw"] += batt_kw
            site_bucket["solar_power"] += abs(solar_kw)

            if batt_v > 0:
                site_bucket["battery_voltage"] = max(site_bucket["battery_voltage"], batt_v)
            if batt_i > 0:
                site_bucket["battery_current"] = max(site_bucket["battery_current"], batt_i)
            if solar_v > 0:
                site_bucket["solar_voltage"] = max(site_bucket["solar_voltage"], solar_v)
            if solar_i > 0:
                site_bucket["solar_current"] = max(site_bucket["solar_current"], solar_i)

            soc_val = _pick(data, ["battery_soc", "state_of_charge"], None)
            if soc_val is not None:
                site_bucket["battery_soc"] = soc_val

        elif reading_type == "RECTIFIER":
            dc_v = _pick(data, ["System_DC_Voltage", "dc_voltage", "DC_Output_V", "Battery_V"], 0.0)
            dc_i = _pick(data, ["Total_DC_Load_Current", "Total_DC_Load_Current (A)", "Total_DC_Load_Amp"], 0.0)
            if dc_v > 0 and dc_i > 0:
                site_bucket["rectifier_kw"] += (dc_v * dc_i) / 1000.0
            if dc_v > 0:
                site_bucket["rectifier_dc_v"] = max(site_bucket["rectifier_dc_v"], dc_v)

        elif reading_type == "FUEL_LEVEL":
            fuel = _pick(data, ["fuel_level", "Fuel Level", "Fuel Level (L)", "fuel_level_liters"], 0.0)
            if fuel > 0:
                site_bucket["fuel_level"] = fuel

    # Collapse per-site into response
    total_sites = len(site_ids)
    grid_sites = sum(1 for s in per_site.values() if s["grid_available"])
    gen_sites = sum(1 for s in per_site.values() if s["gen_power"] > 0.1)
    solar_sites = sum(1 for s in per_site.values() if s["solar_power"] > 0.1)

    battery_charging = sum(1 for s in per_site.values() if s["battery_net_kw"] > 0.1)
    battery_discharging = sum(1 for s in per_site.values() if s["battery_net_kw"] < -0.1)

    grid_voltage = _avg([s["grid_voltage"] for s in per_site.values() if s["grid_voltage"] > 0])
    # Only accept plausible mains frequency values.
    grid_frequency = _avg(
        [
            s["grid_frequency"]
            for s in per_site.values()
            if 40.0 <= float(s["grid_frequency"] or 0.0) <= 70.0
        ]
    )

    grid_power = sum(s["grid_power"] for s in per_site.values())
    gen_power = sum(s["gen_power"] for s in per_site.values())
    solar_power = sum(s["solar_power"] for s in per_site.values())
    battery_net_kw = sum(s["battery_net_kw"] for s in per_site.values())
    rectifier_kw = sum(s["rectifier_kw"] for s in per_site.values())

    # Battery summary
    battery_voltage = _avg([s.get("battery_voltage", 0.0) for s in per_site.values() if s.get("battery_voltage", 0.0) > 0])
    battery_soc_vals = [s.get("battery_soc") for s in per_site.values() if s.get("battery_soc") is not None]
    battery_soc = _avg([float(v) for v in battery_soc_vals]) if battery_soc_vals else 0.0

    if battery_charging > 0 and battery_discharging == 0:
        battery_charging_flag = True
    elif battery_discharging > 0 and battery_charging == 0:
        battery_charging_flag = False
    else:
        battery_charging_flag = battery_net_kw >= 0

    battery_power = abs(battery_net_kw)

    soc = battery_soc

    # Load: prefer rectifier, then tenant channels, then sum of sources.
    tenant_load = sum(s.get("tenant_load_kw", 0.0) for s in per_site.values())
    total_load = rectifier_kw if rectifier_kw > 0 else (tenant_load if tenant_load > 0 else (grid_power + gen_power + solar_power + battery_power))

    # Active source priority (site-level uses strict single source, region-level is best-effort)
    if grid_sites > 0 and grid_power > POWER_THRESHOLD_KW:
        active_source = "grid"
    elif solar_power > POWER_THRESHOLD_KW:
        active_source = "solar"
    elif gen_power > POWER_THRESHOLD_KW:
        active_source = "generator"
    else:
        active_source = "battery"

    # Scale down aggregated voltage/frequency to avoid nonsense when aggregating many sites
    result = {
        "grid": {
            "available": grid_sites > 0,
            "voltage": round(grid_voltage) if grid_voltage > 0 else 0,
            "frequency": round(grid_frequency, 1) if grid_frequency > 0 else 0,
            "power": round(grid_power, 2),
        },
        "generator": {
            "status": "running" if gen_power > POWER_THRESHOLD_KW else "stopped",
            "runtime": 0,
            "fuel": round(_avg([s["fuel_level"] for s in per_site.values() if s["fuel_level"] > 0]), 1),
            "temp": 0,
            "power": round(gen_power, 2),
        },
        "solar": {
            "current": round(_avg([s["solar_current"] for s in per_site.values() if s["solar_current"] > 0]), 2),
            "output": 0,
            "power": round(solar_power, 2),
        },
        "battery": {
            "voltage": round(battery_voltage, 1) if battery_voltage > 0 else 0,
            "soc": round(soc, 1),
            "charging": bool(battery_charging_flag),
            "power": round(battery_power, 2),
        },
        "load": {
            "total": round(total_load, 2),
            "rectifier": round(rectifier_kw, 2),
            "hvac": 0,
            "tenant": round(tenant_load, 2) if tenant_load > 0 else round(total_load, 2),
            "tenants": [
                {"name": name, "power": round(power, 2)}
                for site in per_site.values()
                for name, power in (site.get("tenant_loads") or {}).items()
            ] if site else [],
        },
        "activeSource": active_source,
        "meta": {
            "scope": {
                "region": region,
                "state": state_label if state else state,
                "site": site,
                "sample_size": sample_size,
                "sites_count": total_sites,
            },
            "availability": {
                "grid": round((grid_sites / total_sites) * 100, 1) if total_sites else 0,
                "generator": round((gen_sites / total_sites) * 100, 1) if total_sites else 0,
                "solar": round((solar_sites / total_sites) * 100, 1) if total_sites else 0,
                "battery_charging": battery_charging,
                "battery_discharging": battery_discharging,
            },
        },
    }

    return result
