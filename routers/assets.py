from fastapi import APIRouter
from db.repositories.asset_repository import AssetRepository
from db.repositories.reading_repository import ReadingRepository
from typing import Optional
import json

router = APIRouter()

@router.get("/assets/{asset_id}/readings/latest")
def get_latest_reading(asset_id: int):
    reading_repo = ReadingRepository()
    readings = reading_repo.get_latest_by_asset_ids([asset_id])

    if not readings:
        return None

    reading = readings[0]
    return {
        "id": reading['id'],
        "asset_id": reading['asset_id'],
        "timestamp": reading['timestamp'],
        "data": json.loads(reading['data']) if reading['data'] else {}
    }

@router.get("/assets/{asset_id}/readings")
def get_asset_readings(
    asset_id: int,
    page: int = 1,
    per_page: int = 100,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
):
    reading_repo = ReadingRepository()

    if start_date and end_date:
        readings = reading_repo.get_readings_in_range([asset_id], start_date, end_date)
    else:
        # Get all readings for the asset (limited)
        all_readings = reading_repo.get_by_asset_id(asset_id)
        start_idx = (page - 1) * per_page
        end_idx = start_idx + per_page
        readings = all_readings[start_idx:end_idx]

    return {
        "data": [
            {
                "id": r['id'],
                "asset_id": r['asset_id'],
                "timestamp": r['timestamp'],
                "data": json.loads(r['data']) if r['data'] else {}
            }
            for r in readings
        ],
        "total": len(readings),
        "page": page,
        "per_page": per_page
    }
