from datetime import datetime, timedelta
from typing import Dict, List
import logging
from db.client import get_database
from services.energy_mix_persistence import store_energy_mix_snapshot, get_historical_energy_mix
from routers.energy_mix import _resolve_site_ids, _asset_ids_for_sites, _chunks, _reading_time, _pick, _sanitize_power_kw, _to_float, Bucket
import json
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

def calculate_current_energy_mix() -> Dict[str, float]:
    """Calculate the current energy mix across all sites."""
    db = get_database()
    
    # Get all sites (no filtering for this aggregate calculation)
    site_ids, _scope = _resolve_site_ids(region=None, state=None, site=None, sample_size=2000)
    asset_ids = _asset_ids_for_sites(site_ids)

    if not asset_ids:
        return {'grid': 0, 'generator': 0, 'solar': 0, 'battery': 0}

    # Get recent readings for all assets
    readings = []
    limit_per_asset = 10  # Just get the most recent reading per asset
    # Use a wider time window to ensure we get recent data
    created_at_cutoff = (datetime.now() - timedelta(hours=24)).strftime("%Y-%m-%d %H:%M:%S")

    # Process in chunks to avoid SQL variable limits
    from routers.energy_mix import _MAX_SQL_VARS
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

    # Calculate energy mix
    energy_mix = {'grid': 0, 'generator': 0, 'solar': 0, 'battery': 0}

    for r in readings:
        raw = r.get("data")
        try:
            data = json.loads(raw) if isinstance(raw, str) else (raw or {})
        except Exception:
            continue
        if not isinstance(data, dict):
            continue

        reading_type = str(r.get("reading_type") or "").upper()

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
            energy_mix['grid'] += max(0.0, _sanitize_power_kw(p_kw, max_kw=5_000.0))

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
            energy_mix['generator'] += max(0.0, _sanitize_power_kw(p_kw, max_kw=2_000.0))

        elif reading_type == "DC_METER":
            batt_raw = _pick(data, ["Power1", "Power1 (Watt)", "battery_power", "Battery_Power", "p1_batt", "p1"])
            solar_raw = _pick(data, ["Power2", "Power2 (Watt)", "solar_power", "Solar_Power", "p2_solar_y2", "p2"])

            # DC meters commonly report watts for these fields; convert explicitly.
            batt_w = _to_float(batt_raw, 0.0)
            solar_w = _to_float(solar_raw, 0.0)
            batt_kw = _sanitize_power_kw(batt_w / 1000.0, max_kw=5_000.0)
            solar_kw = _sanitize_power_kw(solar_w / 1000.0, max_kw=5_000.0)

            # For mix distribution, treat negative (charging) as 0 contribution.
            energy_mix['battery'] += max(0.0, batt_kw)
            energy_mix['solar'] += max(0.0, solar_kw)

    return energy_mix


def update_energy_mix_history():
    """Update the energy mix history with current data."""
    try:
        current_mix = calculate_current_energy_mix()
        hour_key = datetime.now().strftime("%Y-%m-%d %H:00")

        # Get the total number of sites to store with the data
        from routers.energy_mix import _resolve_site_ids
        site_ids, _ = _resolve_site_ids(region=None, state=None, site=None, sample_size=2000)

        store_energy_mix_snapshot(hour_key, current_mix, len(site_ids))
        logger.info(f"Updated energy mix history for {hour_key}: {current_mix}")
    except Exception as e:
        logger.error(f"Failed to update energy mix history: {e}")
        logger.exception(e)  # Log the full exception trace


def update_energy_mix_history_hourly():
    """Update the energy mix history for the previous hour with more complete data.
    
    This function runs hourly to capture a full hour of data rather than just
    the current moment's snapshot.
    """
    try:
        # Calculate the data for the previous hour
        from datetime import timedelta
        prev_hour = datetime.now() - timedelta(hours=1)
        hour_key = prev_hour.strftime("%Y-%m-%d %H:00")

        # Calculate energy mix for the previous hour specifically
        current_mix = calculate_energy_mix_for_hour(prev_hour)

        # Get the total number of sites to store with the data
        from routers.energy_mix import _resolve_site_ids
        site_ids, _ = _resolve_site_ids(region=None, state=None, site=None, sample_size=2000)

        store_energy_mix_snapshot(hour_key, current_mix, len(site_ids))
        logger.info(f"Updated energy mix history for previous hour {hour_key}: {current_mix}")
    except Exception as e:
        logger.error(f"Failed to update energy mix history for previous hour: {e}")
        logger.exception(e)  # Log the full exception trace


def calculate_energy_mix_for_hour(target_hour: datetime) -> Dict[str, float]:
    """Calculate energy mix for a specific hour using all readings from that hour."""
    from db.client import get_database
    from routers.energy_mix import _resolve_site_ids, _asset_ids_for_sites, _chunks, _reading_time, _pick, _sanitize_power_kw, _to_float
    import json
    from datetime import datetime, timedelta
    
    db = get_database()
    
    # Get all sites (no filtering for this aggregate calculation)
    site_ids, _scope = _resolve_site_ids(region=None, state=None, site=None, sample_size=2000)
    asset_ids = _asset_ids_for_sites(site_ids)

    if not asset_ids:
        return {'grid': 0, 'generator': 0, 'solar': 0, 'battery': 0}

    # Define the time range for the specific hour
    start_of_hour = target_hour.replace(minute=0, second=0, microsecond=0)
    end_of_hour = start_of_hour + timedelta(hours=1)
    
    start_str = start_of_hour.strftime("%Y-%m-%d %H:%M:%S")
    end_str = end_of_hour.strftime("%Y-%m-%d %H:%M:%S")

    # Get readings for the specific hour
    readings = []
    
    # Process in chunks to avoid SQL variable limits
    from routers.energy_mix import _MAX_SQL_VARS
    for chunk in _chunks(asset_ids, _MAX_SQL_VARS - 2):
        placeholders = ",".join(["?"] * len(chunk))
        cursor = db.execute(
            f"""
            SELECT id, asset_id, reading_type, timestamp, data, created_at
            FROM readings r
            WHERE asset_id IN ({placeholders})
                AND created_at >= ?
                AND created_at < ?
            ORDER BY asset_id, id DESC
            """,
            (*chunk, start_str, end_str),
        )
        readings.extend([dict(row) for row in cursor.fetchall()])

    # Calculate energy mix - average over the hour if multiple readings exist
    # or take the latest reading for each asset
    energy_mix = {'grid': 0, 'generator': 0, 'solar': 0, 'battery': 0}
    asset_last_readings = {}  # Track last reading per asset to avoid double counting

    for r in readings:
        asset_id = r.get("asset_id")
        if asset_id is None:
            continue
            
        # Only process the latest reading per asset for this hour
        if asset_id not in asset_last_readings:
            asset_last_readings[asset_id] = r
        else:
            # Keep the latest reading based on ID
            existing_id = asset_last_readings[asset_id]["id"]
            if r["id"] > existing_id:
                asset_last_readings[asset_id] = r

    # Process the latest reading for each asset
    for r in asset_last_readings.values():
        raw = r.get("data")
        try:
            data = json.loads(raw) if isinstance(raw, str) else (raw or {})
        except Exception:
            continue
        if not isinstance(data, dict):
            continue

        reading_type = str(r.get("reading_type") or "").upper()

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
            energy_mix['grid'] += max(0.0, _sanitize_power_kw(p_kw, max_kw=5_000.0))

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
            energy_mix['generator'] += max(0.0, _sanitize_power_kw(p_kw, max_kw=2_000.0))

        elif reading_type == "DC_METER":
            batt_raw = _pick(data, ["Power1", "Power1 (Watt)", "battery_power", "Battery_Power", "p1_batt", "p1"])
            solar_raw = _pick(data, ["Power2", "Power2 (Watt)", "solar_power", "Solar_Power", "p2_solar_y2", "p2"])

            # DC meters commonly report watts for these fields; convert explicitly.
            batt_w = _to_float(batt_raw, 0.0)
            solar_w = _to_float(solar_raw, 0.0)
            batt_kw = _sanitize_power_kw(batt_w / 1000.0, max_kw=5_000.0)
            solar_kw = _sanitize_power_kw(solar_w / 1000.0, max_kw=5_000.0)

            # For mix distribution, treat negative (charging) as 0 contribution.
            energy_mix['battery'] += max(0.0, batt_kw)
            energy_mix['solar'] += max(0.0, solar_kw)

    return energy_mix


def backfill_missing_energy_mix_data(days_to_backfill: int = 7):
    """Backfill missing energy mix data for the specified number of days."""
    from datetime import datetime, timedelta
    
    logger.info(f"Starting backfill for {days_to_backfill} days")
    
    # Get current historical data to identify gaps
    historical_data = get_historical_energy_mix(days_to_backfill * 24)  # Convert to hours
    
    # Identify which hours are missing data (all zeros)
    now = datetime.now()
    for i in range(days_to_backfill * 24):  # For each hour in the range
        target_hour = now - timedelta(hours=i)
        hour_key = target_hour.strftime("%Y-%m-%d %H:00")
        display_time = target_hour.strftime("%H:00")

        # Find if this hour exists in historical data
        hour_exists = any(item['time'] == display_time for item in historical_data)

        if not hour_exists:
            logger.info(f"Backfilling data for {hour_key}")
            try:
                # Calculate energy mix for this specific hour
                mix = calculate_energy_mix_for_hour(target_hour)

                # Get the total number of sites to store with the data
                from routers.energy_mix import _resolve_site_ids
                site_ids, _ = _resolve_site_ids(region=None, state=None, site=None, sample_size=2000)

                store_energy_mix_snapshot(hour_key, mix, len(site_ids))
                logger.info(f"Backfilled data for {hour_key}: {mix}")
            except Exception as e:
                logger.error(f"Failed to backfill data for {hour_key}: {e}")
    
    logger.info("Backfill completed")


# Run backfill on startup if needed
def run_initial_backfill():
    """Run an initial backfill of missing data on startup."""
    try:
        backfill_missing_energy_mix_data(days_to_backfill=7)
    except Exception as e:
        logger.error(f"Initial backfill failed: {e}")