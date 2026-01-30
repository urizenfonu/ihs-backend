from fastapi import APIRouter, HTTPException
from db.client import get_database
from db.repositories.sync_metadata_repository import SyncMetadataRepository
from services.ihs_sync_service import get_ihs_sync_service

router = APIRouter()


@router.get("/sync/status")
def get_sync_status():
    """Get IHS sync status and metadata"""
    repo = SyncMetadataRepository()
    metadata = repo.get_metadata()
    return metadata or {"status": "never_run"}


@router.post("/sync/force")
def force_sync():
    """Manually trigger IHS sync"""
    sync_service = get_ihs_sync_service()
    result = sync_service.sync_all()
    return {"success": True, "stats": result}


@router.post("/sync/reset-cache")
def reset_cache(confirm: bool = False):
    """
    Clear cached `sites`/`assets`/`readings` so the next sync starts fresh.
    Does NOT delete alarms/rules/thresholds.
    """
    if not confirm:
        raise HTTPException(status_code=400, detail="Pass ?confirm=true to reset cache")

    db = get_database()
    db.execute("PRAGMA foreign_keys = ON")

    # Detach alarms from assets so we can delete assets safely.
    detached = db.execute("UPDATE alarms SET asset_id = NULL WHERE asset_id IS NOT NULL").rowcount

    readings_deleted = db.execute("DELETE FROM readings").rowcount
    assets_deleted = db.execute("DELETE FROM assets").rowcount
    sites_deleted = db.execute("DELETE FROM sites WHERE external_id IS NOT NULL").rowcount
    db.commit()

    return {
        "success": True,
        "detachedAlarmAssets": detached,
        "deleted": {
            "readings": readings_deleted,
            "assets": assets_deleted,
            "sites": sites_deleted,
        },
    }
