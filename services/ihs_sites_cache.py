import json
import logging
import threading
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

from db.repositories.asset_repository import AssetRepository
from db.repositories.site_repository import SiteRepository
from db.repositories.sync_metadata_repository import SyncMetadataRepository
from services.ihs_sync_service import get_ihs_sync_service

logger = logging.getLogger(__name__)

_refresh_lock = threading.Lock()
_refresh_in_progress = False


def _parse_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value or not isinstance(value, str):
        return None
    try:
        normalized = value.replace("Z", "+00:00")
        dt = datetime.fromisoformat(normalized)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        return None


def _refresh_sites_and_assets():
    global _refresh_in_progress
    try:
        sync_service = get_ihs_sync_service()
        sync_service.sync_sites_and_assets()
    except Exception:
        logger.exception("Failed to refresh sites/assets from IHS API")
    finally:
        with _refresh_lock:
            _refresh_in_progress = False


def trigger_refresh_if_stale(max_age_minutes: int = 30) -> bool:
    repo = SyncMetadataRepository()
    metadata = repo.get_metadata() or {}

    if metadata.get("status") == "running":
        return False

    last_success = _parse_datetime(metadata.get("last_success_time"))
    if last_success is not None:
        now = datetime.now(timezone.utc)
        if now - last_success < timedelta(minutes=max_age_minutes):
            return False

    with _refresh_lock:
        global _refresh_in_progress
        if _refresh_in_progress:
            return False
        _refresh_in_progress = True

    threading.Thread(target=_refresh_sites_and_assets, daemon=True).start()
    return True


def get_cached_sites_with_assets() -> List[Dict]:
    site_repo = SiteRepository()
    asset_repo = AssetRepository()

    sites = site_repo.get_all_external()
    site_ids = [site["id"] for site in sites if site.get("id") is not None]

    assets_by_site_id: Dict[int, List[Dict]] = {}
    for asset in asset_repo.get_by_site_ids(site_ids):
        site_id = asset.get("site_id")
        if site_id is None:
            continue
        assets_by_site_id.setdefault(site_id, []).append(asset)

    result: List[Dict] = []
    for site in sites:
        site_id = site.get("id")
        assets = assets_by_site_id.get(site_id, []) if site_id is not None else []
        result.append({**site, "assets": assets})

    return result


def to_sites_endpoint_payload(cached_sites: List[Dict]) -> List[Dict]:
    payload: List[Dict] = []
    for site in cached_sites:
        zone_name = site.get("zone") or "Unknown"
        region_name = site.get("region") or zone_name
        assets_raw = site.get("assets", []) if isinstance(site.get("assets"), list) else []

        assets: List[Dict] = []
        for asset in assets_raw:
            config = None
            config_raw = asset.get("config")
            if isinstance(config_raw, str) and config_raw:
                try:
                    config = json.loads(config_raw)
                except json.JSONDecodeError:
                    config = None

            assets.append(
                {
                    "id": asset.get("external_id") or asset.get("id"),
                    "name": asset.get("name"),
                    "type": asset.get("type"),
                    "config": config,
                }
            )

        payload.append(
            {
                "id": site.get("external_id") or site.get("id"),
                "name": site.get("name"),
                "region": region_name,
                "zone": zone_name,
                "state": site.get("state"),
                "cluster_code": site.get("cluster_code"),
                "asset_count": len(assets),
                "assets": assets,
            }
        )

    return payload

