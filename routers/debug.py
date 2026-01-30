from fastapi import APIRouter, HTTPException

from db.repositories.site_repository import SiteRepository
from services.ihs_client_factory import get_ihs_api_client

router = APIRouter()


@router.get("/debug/verify-site/{site_name}")
def verify_site_in_iot_api(site_name: str):
    """
    Debug helper: prove a site is present in the live IoT API.

    We avoid fetching the whole `/sites` list by using the DB `external_id`
    to jump to the likely page (IDs are ordered ascending in API responses).
    """
    if not site_name:
        raise HTTPException(status_code=400, detail="site_name is required")

    site_repo = SiteRepository()
    match = site_repo.get_by_name(site_name)
    if not match or match.get("external_id") is None:
        raise HTTPException(status_code=404, detail="Site not found in backend DB")

    external_id = int(match["external_id"])
    per_page = 100
    approx_page = max(1, external_id // per_page)

    client = get_ihs_api_client()

    found = None
    scanned_pages = []
    for page in {max(1, approx_page - 1), approx_page, approx_page + 1}:
        resp = client.get_sites(page=page, per_page=per_page)
        data = resp.get("data", []) if isinstance(resp, dict) else []
        scanned_pages.append(page)
        for api_site in data:
            if api_site.get("id") == external_id or api_site.get("name") == site_name:
                found = api_site
                break
        if found:
            break

    return {
        "siteName": site_name,
        "dbExternalId": external_id,
        "scannedPages": sorted(scanned_pages),
        "existsInIotApi": found is not None,
        "iotApiSite": (
            {
                "id": found.get("id"),
                "name": found.get("name"),
                "zone": found.get("zone"),
                "assetCount": len(found.get("assets") or []),
                "assets": found.get("assets") or [],
            }
            if found
            else None
        ),
    }
