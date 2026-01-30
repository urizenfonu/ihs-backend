import requests
from typing import List, Dict, Optional
import time
import math
import logging

logger = logging.getLogger(__name__)


class IHSApiClient:
    def __init__(self, base_url: str, token: str):
        self.base_url = base_url.rstrip('/')
        self.token = token
        self.session = requests.Session()

    def _make_request(self, endpoint: str, params: dict = None) -> dict:
        url = f"{self.base_url}/{endpoint}"
        params = params or {}
        params['X-Access-Token'] = self.token

        for attempt in range(3):
            try:
                response = self.session.get(url, params=params, timeout=30)
                response.raise_for_status()
                return response.json()
            except Exception as e:
                if attempt == 2:
                    logger.error(f"Failed to fetch {endpoint} after 3 attempts: {e}")
                    raise
                time.sleep(2 ** attempt)  # Exponential backoff

    def get_sites(self, page: int = 1, per_page: int = 100) -> dict:
        return self._make_request('sites', {'page': page, 'per_page': per_page})

    def get_all_sites(self, per_page: int = 100) -> List[dict]:
        logger.info("Fetching all sites from IHS API...")

        page = 1
        all_sites: List[dict] = []
        total_pages: Optional[int] = None

        while True:
            resp = self.get_sites(page=page, per_page=per_page)
            data = resp.get("data", [])
            if not isinstance(data, list):
                data = []

            all_sites.extend(data)

            if total_pages is None:
                total_raw = resp.get("total")
                total_int: Optional[int] = None
                try:
                    if total_raw is not None:
                        total_int = int(total_raw)
                except (TypeError, ValueError):
                    total_int = None

                if total_int is not None and per_page > 0:
                    total_pages = max(1, math.ceil(total_int / per_page))
                    logger.info(f"Total sites: {total_int}, pages: {total_pages}")
                else:
                    total_pages_raw = resp.get("total_pages") or resp.get("pages")
                    try:
                        total_pages = int(total_pages_raw) if total_pages_raw is not None else None
                    except (TypeError, ValueError):
                        total_pages = None

            if total_pages is not None:
                if page >= total_pages:
                    break
            else:
                if len(data) < per_page:
                    break

            page += 1
            if page % 20 == 0:
                time.sleep(0.05)

        logger.info(f"Fetched {len(all_sites)} sites")
        return all_sites

    def get_asset_readings(self, asset_id: int, params: dict = None) -> dict:
        params = params or {}
        return self._make_request(f'assets/{asset_id}/readings', params)

    def get_latest_asset_reading(self, asset_id: int) -> Optional[dict]:
        try:
            response = self.get_asset_readings(asset_id, {'per_page': 1, 'page': 1})
            data = response.get('data', [])
            return data[0] if data else None
        except Exception as e:
            logger.warning(f"Failed to fetch reading for asset {asset_id}: {e}")
            return None
