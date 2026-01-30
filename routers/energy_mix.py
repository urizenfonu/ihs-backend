from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Tuple

from fastapi import APIRouter, HTTPException, Query

from db.client import get_database
from services.energy_mix_persistence import (
    initialize_energy_mix_table,
    store_energy_mix_snapshot,
    get_historical_energy_mix,
    get_energy_mix_summary
)

logger = logging.getLogger(__name__)
router = APIRouter()

# Initialize the energy mix history table when module loads
initialize_energy_mix_table()

_MAX_SQL_VARS = 900
_SENTINEL_MAX_U32 = 4294967295.0
_SENTINEL_MAX_U32_KW = 4294967.295


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


def _pick(data: Dict[str, Any], keys: List[str]) -> Optional[float]:
    for key in keys:
        if key in data and data[key] is not None:
            try:
                return float(data[key])
            except (TypeError, ValueError):
                continue
    return None


def _parse_datetime(value: Any) -> Optional[datetime]:
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    if not isinstance(value, str):
        return None

    raw = value.strip()
    if not raw:
        return None

    for fmt in (
        "%m/%d/%Y %H:%M:%S",
        "%m/%d/%Y %H:%M",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
    ):
        try:
            return datetime.strptime(raw, fmt)
        except ValueError:
            continue

    try:
        return datetime.fromisoformat(raw)
    except ValueError:
        return None


def _to_kw(value: Optional[float]) -> float:
    if value is None:
        return 0.0
    # Generic best-effort conversion when values might be in watts.
    # Prefer callers to convert explicitly when the payload schema is known.
    if abs(value) >= 1000:
        return value / 1000.0
    return value


def _is_sentinel(value: float) -> bool:
    if not isinstance(value, (int, float)) or not float(value) == value:
        return True
    v = float(value)
    if abs(v) >= _SENTINEL_MAX_U32 - 0.5:
        return True
    if abs(v - _SENTINEL_MAX_U32_KW) <= 0.01:
        return True
    # Discard obviously invalid magnitudes (prevents chart scale blow-ups).
    if abs(v) >= 1_000_000:
        return True
    return False


def _sanitize_power_kw(value: Optional[float], *, max_kw: float) -> float:
    if value is None:
        return 0.0
    v = _to_float(value, 0.0)
    if not (v == v) or not isinstance(v, (int, float)):
        return 0.0
    if _is_sentinel(v):
        return 0.0
    # Heuristic: treat large values as watts, convert to kW.
    if abs(v) >= 10_000:
        v = v / 1000.0
    if not (v == v):
        return 0.0
    if abs(v) > max_kw:
        return 0.0
    return v


@dataclass
class Bucket:
    grid: float = 0.0
    generator: float = 0.0
    solar: float = 0.0
    battery: float = 0.0


def _resolve_site_ids(
    region: Optional[str],
    state: Optional[str],
    site: Optional[str],
    sample_size: int,
) -> Tuple[List[int], Dict[str, Any]]:
    db = get_database()

    if site:
        row = db.execute(
            "SELECT id, name, region, state FROM sites WHERE name = ? COLLATE NOCASE LIMIT 1",
            (site,),
        ).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail=f"Site '{site}' not found")
        return [int(row["id"])], {
            "region": row["region"],
            "state": row["state"],
            "site": row["name"],
        }

    sample_size = max(1, int(sample_size))

    where = []
    params: List[Any] = []

    if region:
        where.append("(s.region = ? OR s.zone = ?)")
        params.extend([region, region])

    if state:
        where.append("s.state = ?")
        params.append(state)

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    # Prefer sites that have readings recently.
    cursor = db.execute(
        f"""
        SELECT a.site_id AS site_id
        FROM readings r
        JOIN assets a ON r.asset_id = a.id
        JOIN sites s ON a.site_id = s.id
        {where_sql}
        GROUP BY a.site_id
        ORDER BY MAX(r.id) DESC
        LIMIT ?
        """,
        (*params, sample_size),
    )
    site_ids = [int(r["site_id"]) for r in cursor.fetchall()]

    if not site_ids:
        cursor2 = db.execute(
            f"SELECT id FROM sites {where_sql} LIMIT ?",
            (*params, sample_size),
        )
        site_ids = [int(r[0]) for r in cursor2.fetchall()]

    if not site_ids:
        raise HTTPException(status_code=404, detail="No sites matched the requested scope")

    return site_ids, {"region": region, "state": state, "site": None}


def _asset_ids_for_sites(site_ids: List[int]) -> List[int]:
    db = get_database()
    asset_ids: List[int] = []
    for chunk in _chunks(site_ids, _MAX_SQL_VARS):
        placeholders = ",".join(["?"] * len(chunk))
        cursor = db.execute(
            f"SELECT id FROM assets WHERE site_id IN ({placeholders})",
            tuple(chunk),
        )
        asset_ids.extend([int(r[0]) for r in cursor.fetchall()])
    return asset_ids


def _reading_time(reading: Dict[str, Any], data: Dict[str, Any]) -> Optional[datetime]:
    ts = _parse_datetime(reading.get("timestamp"))
    if ts:
        return ts
    return _parse_datetime(data.get("date"))


@router.get("/energy-mix")
def get_energy_mix(
    interval: str = Query("hourly"),
    region: Optional[str] = Query(default=None),
    state: Optional[str] = Query(default=None),
    site: Optional[str] = Query(default=None),
    history_hours: int = Query(default=24, ge=1, le=168),
    sample_size: int = Query(default=250, ge=1, le=2000),
):
    """Return a real (non-synthetic) energy mix timeseries.

    Values are aggregated kW per hour bucket across a sampled set of sites.
    """

    if interval != "hourly":
        raise HTTPException(status_code=400, detail="Only interval=hourly is supported")

    # First, try to get historical data from persistent storage
    hours = int(history_hours) if history_hours else 24
    historical_data = get_historical_energy_mix(hours)

    # If we have historical data and it's not all zeros, return it
    has_real_data = any(
        any(val > 0 for val in [item['grid'], item['generator'], item['solar'], item['battery']])
        for item in historical_data
    )

    if has_real_data:
        return historical_data

    # If no historical data or all zeros, fall back to real-time calculation
    # but also store the results for future requests
    db = get_database()

    site_ids, _scope = _resolve_site_ids(region=region, state=state, site=site, sample_size=sample_size)
    asset_ids = _asset_ids_for_sites(site_ids)

    if not asset_ids:
        # Return historical data even if it's all zeros if no assets are found
        return historical_data

    # Pull recent readings for assets, filtered by created_at to avoid scanning historical telemetry.
    readings: List[Dict[str, Any]] = []
    limit_per_asset = min(50, max(10, history_hours * 2))
    # Use a wider time window to ensure we get recent data for all assets
    created_at_cutoff = (datetime.now() - timedelta(hours=max(history_hours, 24))).strftime("%Y-%m-%d %H:%M:%S")

    for chunk in _chunks(asset_ids, _MAX_SQL_VARS - 2):
        placeholders = ",".join(["?"] * len(chunk))
        cursor = db.execute(
            f"""
            SELECT id, asset_id, reading_type, timestamp, data, created_at
            FROM (
              SELECT
                r.*,
                ROW_NUMBER() OVER (PARTITION BY asset_id ORDER BY id DESC) as rn
              FROM readings r
              WHERE asset_id IN ({placeholders})
                AND created_at >= ?
            )
            WHERE rn <= ?
            ORDER BY asset_id, id DESC
            """,
            (*chunk, created_at_cutoff, limit_per_asset),
        )
        readings.extend([dict(row) for row in cursor.fetchall()])

    cutoff = datetime.now() - timedelta(hours=history_hours)

    # Keep only the latest reading per (hour_bucket, asset_id) to avoid double-counting.
    latest_by_bucket: Dict[Tuple[str, int], Dict[str, Any]] = {}

    for r in readings:
        raw = r.get("data")
        try:
            data = json.loads(raw) if isinstance(raw, str) else (raw or {})
        except Exception:
            continue
        if not isinstance(data, dict):
            continue

        dt = _reading_time(r, data)
        if not dt or dt < cutoff:
            continue

        hour_key = dt.strftime("%H:00")
        asset_id = int(r.get("asset_id") or 0)
        if asset_id <= 0:
            continue

        key = (hour_key, asset_id)
        existing = latest_by_bucket.get(key)
        if not existing or int(r.get("id") or 0) > int(existing.get("id") or 0):
            latest_by_bucket[key] = {"reading": r, "data": data}

    now = datetime.now()
    hours = [now - timedelta(hours=i) for i in range(23, -1, -1)]
    buckets: Dict[str, Bucket] = {h.strftime("%H:00"): Bucket() for h in hours}

    for (hour_key, _asset_id), payload in latest_by_bucket.items():
        if hour_key not in buckets:
            continue

        r = payload["reading"]
        data = payload["data"]
        reading_type = str(r.get("reading_type") or "").upper()
        bucket = buckets[hour_key]

        if reading_type == "AC_METER":
            p_kw = _pick(
                data,
                [
                    "total_active_power",
                    "total_power_kw",
                    "Total_Active_Power (kW)",
                    "Total Active Power (kW)",
                    "Total_Active_Power (kw)",
                ],
            )
            bucket.grid += max(0.0, _sanitize_power_kw(p_kw, max_kw=5_000.0))

        elif reading_type == "GENERATOR":
            p_kw = _pick(
                data,
                [
                    "power_kw",
                    "total_active_power",
                    "total_power_kw",
                    "Gen_Total_Power (KW)",
                    "Gen_Total_Power",
                ],
            )
            bucket.generator += max(0.0, _sanitize_power_kw(p_kw, max_kw=2_000.0))

        elif reading_type == "DC_METER":
            batt_raw = _pick(data, ["Power1", "Power1 (Watt)", "battery_power", "Battery_Power", "p1_batt", "p1"])
            solar_raw = _pick(data, ["Power2", "Power2 (Watt)", "solar_power", "Solar_Power", "p2_solar_y2", "p2"])

            # DC meters commonly report watts for these fields; convert explicitly.
            batt_w = _to_float(batt_raw, 0.0)
            solar_w = _to_float(solar_raw, 0.0)
            batt_kw = _sanitize_power_kw(batt_w / 1000.0, max_kw=5_000.0)
            solar_kw = _sanitize_power_kw(solar_w / 1000.0, max_kw=5_000.0)

            # For mix distribution, treat negative (charging) as 0 contribution.
            bucket.battery += max(0.0, batt_kw)
            bucket.solar += max(0.0, solar_kw)

    result = []
    for h in hours:
        key = h.strftime("%H:00")
        b = buckets[key]
        energy_mix_entry = {
            "time": key,
            "grid": round(b.grid, 2),
            "generator": round(b.generator, 2),
            "solar": round(b.solar, 2),
            "battery": round(b.battery, 2),
        }
        result.append(energy_mix_entry)

        # Store this hour's data in persistent storage for future requests
        hour_energy_mix = {
            'grid': energy_mix_entry['grid'],
            'generator': energy_mix_entry['generator'],
            'solar': energy_mix_entry['solar'],
            'battery': energy_mix_entry['battery']
        }
        store_energy_mix_snapshot(key, hour_energy_mix, len(site_ids))

    return result
