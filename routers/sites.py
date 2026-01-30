from fastapi import APIRouter
from services.ihs_sites_cache import (
    get_cached_sites_with_assets,
    to_sites_endpoint_payload,
    trigger_refresh_if_stale,
)
from services.ihs_sync_service import get_ihs_sync_service

router = APIRouter()


@router.get("/sites")
def get_sites(include_empty: bool = False):
    """Get all sites with their assets from DB (stale-while-revalidate)."""
    cached_sites = get_cached_sites_with_assets()

    if not cached_sites:
        sync_service = get_ihs_sync_service()
        sync_service.sync_sites_and_assets()
        cached_sites = get_cached_sites_with_assets()

    trigger_refresh_if_stale(max_age_minutes=30)
    payload = to_sites_endpoint_payload(cached_sites)
    if include_empty:
        return payload
    return [site for site in payload if site.get("assets")]
