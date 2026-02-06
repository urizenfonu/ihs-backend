import logging
from typing import Dict
from datetime import datetime
from services.ihs_api_client import IHSApiClient
from db.repositories.site_repository import SiteRepository
from db.repositories.asset_repository import AssetRepository
from db.repositories.reading_repository import ReadingRepository
from db.repositories.sync_metadata_repository import SyncMetadataRepository
import json
import os
import threading

logger = logging.getLogger(__name__)

ASSET_TYPE_MAP = {
    'fuel': 'FUEL_LEVEL',
    'diesel': 'FUEL_LEVEL',
    'generator': 'GENERATOR',
    'dc_meter': 'DC_METER',
    'dc meter': 'DC_METER',
    'ac_meter': 'AC_METER',
    'ac meter': 'AC_METER',
    'grid': 'AC_METER',
    'rectifier': 'RECTIFIER',
    'temperature': 'TEMPERATURE',
    'battery': 'DC_METER',
    'solar': 'DC_METER'
}


class IHSSyncService:
    def __init__(self):
        base_url = os.getenv('IHS_API_BASE_URL')
        token = os.getenv('IHS_API_TOKEN')

        if not base_url or not token:
            raise ValueError("IHS_API_BASE_URL and IHS_API_TOKEN must be set in .env")

        self.api_client = IHSApiClient(base_url, token)
        self.site_repo = SiteRepository()
        self.asset_repo = AssetRepository()
        self.reading_repo = ReadingRepository()
        self.metadata_repo = SyncMetadataRepository()
        self._sync_lock = threading.Lock()

    def _infer_asset_type(self, asset: dict) -> str:
        # Check channels first
        channels = asset.get('config', {}).get('channels', []) if asset.get('config') else []
        if channels:
            channel_names = [c.get('name', '').lower() for c in channels if isinstance(c, dict)]

            if any('diesel' in n or 'fuel' in n for n in channel_names):
                return 'FUEL_LEVEL'
            if any('battery' in n or 'solar' in n for n in channel_names):
                return 'DC_METER'
            if any('engine' in n or 'coolant' in n for n in channel_names):
                return 'GENERATOR'
            if any('rectifier' in n or 'dc_output' in n for n in channel_names):
                return 'RECTIFIER'
            if any('voltage_l' in n and 'current_l' in n for n in channel_names):
                return 'AC_METER'
            if 'temperature' in ' '.join(channel_names):
                return 'TEMPERATURE'

        # Fallback to name matching
        name_lower = asset.get('name', '').lower()
        for keyword, asset_type in ASSET_TYPE_MAP.items():
            if keyword in name_lower:
                return asset_type

        return 'UNKNOWN'

    def _extract_site_fields(self, ihs_site: dict) -> Dict:
        zone = ihs_site.get('zone') if isinstance(ihs_site, dict) else None
        zone_name = zone.get('name', 'Unknown') if isinstance(zone, dict) else 'Unknown'
        zone_external_id = zone.get('id') if isinstance(zone, dict) else None

        cluster = ihs_site.get('cluster') if isinstance(ihs_site, dict) else None
        cluster_name = None
        state_name = None
        region_name = None

        if isinstance(cluster, dict):
            cluster_name = cluster.get('name')
            state = cluster.get('state') if isinstance(cluster.get('state'), dict) else None
            if isinstance(state, dict):
                state_name = state.get('name')
                region = state.get('region') if isinstance(state.get('region'), dict) else None
                if isinstance(region, dict):
                    region_name = region.get('name')

        cluster_code = None
        if isinstance(ihs_site, dict):
            cluster_code = (
                cluster_name
                or ihs_site.get('cluster_code')
                or ihs_site.get('clusterCode')
            )

        state = None
        if isinstance(ihs_site, dict):
            state = state_name or ihs_site.get('state') or ihs_site.get('State')

        region = region_name or zone_name

        return {
            'name': ihs_site.get('name'),
            'region': region,
            'zone': zone_name,
            'state': state,
            'cluster_code': cluster_code,
            'zone_external_id': zone_external_id,
        }

    def _prune_stale_sites(self, api_external_ids: set[int]) -> Dict[str, int]:
        if not api_external_ids:
            logger.warning("Skip pruning stale sites; API returned 0 sites")
            return {'stale_sites': 0, 'stale_assets': 0, 'stale_readings': 0, 'alarms_detached': 0}

        local_external_ids = set(self.site_repo.get_external_ids())
        stale_external_ids = sorted(local_external_ids - api_external_ids)
        if not stale_external_ids:
            return {'stale_sites': 0, 'stale_assets': 0, 'stale_readings': 0, 'alarms_detached': 0}

        logger.info(f"Pruning {len(stale_external_ids)} stale sites not present in API")

        pruned_sites = 0
        pruned_assets = 0
        pruned_readings = 0
        detached_alarms = 0

        # Avoid SQLite parameter limits by chunking.
        CHUNK = 400
        for i in range(0, len(stale_external_ids), CHUNK):
            ext_chunk = stale_external_ids[i:i + CHUNK]
            site_ids = self.site_repo.get_ids_by_external_ids(ext_chunk)
            if not site_ids:
                continue

            asset_ids = self.asset_repo.get_ids_by_site_ids(site_ids)
            if asset_ids:
                from db.client import get_database
                db = get_database()
                placeholders = ",".join("?" for _ in asset_ids)
                cursor = db.execute(
                    f"UPDATE alarms SET asset_id = NULL WHERE asset_id IN ({placeholders})",
                    tuple(asset_ids),
                )
                db.commit()
                detached_alarms += cursor.rowcount
                pruned_readings += self.reading_repo.delete_by_asset_ids(asset_ids)

            pruned_assets += self.asset_repo.delete_by_site_ids(site_ids)
            pruned_sites += self.site_repo.delete_by_ids(site_ids)

        return {
            'stale_sites': pruned_sites,
            'stale_assets': pruned_assets,
            'stale_readings': pruned_readings,
            'alarms_detached': detached_alarms,
        }

    def sync_sites_and_assets(self) -> Dict:
        if not self._sync_lock.acquire(blocking=False):
            logger.info("IHS sites/assets sync already running; skipping")
            return {'sites': 0, 'assets': 0, 'readings': 0, 'skipped': True}

        logger.info("Starting IHS sites/assets sync...")
        self.metadata_repo.record_sync_start()

        stats = {'sites': 0, 'assets': 0, 'readings': 0}

        try:
            logger.info("Fetching sites from IHS API...")
            ihs_sites = self.api_client.get_all_sites()
            logger.info(f"Fetched {len(ihs_sites)} sites from IHS")

            api_external_ids: set[int] = set()
            for ihs_site in ihs_sites:
                # Sync site
                api_external_ids.add(int(ihs_site['id']))
                site_data = self._extract_site_fields(ihs_site)

                site_id = self.site_repo.upsert_by_external_id(ihs_site['id'], site_data)
                stats['sites'] += 1

                # Sync assets for this site
                ihs_assets = ihs_site.get('assets', [])
                for ihs_asset in ihs_assets:
                    asset_type = self._infer_asset_type(ihs_asset)

                    # Extract tenant channels from asset config
                    tenant_names = []
                    config = ihs_asset.get('config') or {}
                    channels = config.get('channels', []) if isinstance(config, dict) else []
                    for channel in channels:
                        if isinstance(channel, dict) and channel.get('type') == 'tenant':
                            tenant_name = channel.get('name')
                            if tenant_name:
                                tenant_names.append(tenant_name)

                    asset_data = {
                        'name': ihs_asset['name'],
                        'type': asset_type,
                        'site_id': site_id,
                        'tenant_channels': json.dumps(tenant_names) if tenant_names else None,
                        'config': json.dumps(ihs_asset.get('config')) if ihs_asset.get('config') is not None else None,
                    }

                    asset_id = self.asset_repo.upsert_by_external_id(ihs_asset['id'], asset_data)
                    stats['assets'] += 1

            prune_stats = self._prune_stale_sites(api_external_ids)
            stats.update(prune_stats)

            logger.info(f"Sync complete: {stats}")
            self.metadata_repo.record_sync_success(stats)
            return stats

        except Exception as e:
            logger.error(f"Sync failed: {e}", exc_info=True)
            self.metadata_repo.record_sync_failure(str(e))
            raise
        finally:
            self._sync_lock.release()

    def sync_all(self) -> Dict:
        if not self._sync_lock.acquire(blocking=False):
            logger.info("IHS sync already running; skipping")
            return {'sites': 0, 'assets': 0, 'readings': 0, 'skipped': True}

        logger.info("Starting IHS sync...")
        self.metadata_repo.record_sync_start()

        stats = {'sites': 0, 'assets': 0, 'readings': 0}

        try:
            # Step 1: Sync sites + assets (also stores config)
            logger.info("Fetching sites from IHS API...")
            ihs_sites = self.api_client.get_all_sites()
            logger.info(f"Fetched {len(ihs_sites)} sites from IHS")

            api_external_ids: set[int] = set()
            for ihs_site in ihs_sites:
                api_external_ids.add(int(ihs_site['id']))
                site_data = self._extract_site_fields(ihs_site)

                site_id = self.site_repo.upsert_by_external_id(ihs_site['id'], site_data)
                stats['sites'] += 1

                ihs_assets = ihs_site.get('assets', [])
                for ihs_asset in ihs_assets:
                    asset_type = self._infer_asset_type(ihs_asset)

                    tenant_names = []
                    config = ihs_asset.get('config') or {}
                    channels = config.get('channels', []) if isinstance(config, dict) else []
                    for channel in channels:
                        if isinstance(channel, dict) and channel.get('type') == 'tenant':
                            tenant_name = channel.get('name')
                            if tenant_name:
                                tenant_names.append(tenant_name)

                    asset_data = {
                        'name': ihs_asset['name'],
                        'type': asset_type,
                        'site_id': site_id,
                        'tenant_channels': json.dumps(tenant_names) if tenant_names else None,
                        'config': json.dumps(ihs_asset.get('config')) if ihs_asset.get('config') is not None else None,
                    }

                    asset_id = self.asset_repo.upsert_by_external_id(ihs_asset['id'], asset_data)
                    stats['assets'] += 1

                    try:
                        latest_reading = self.api_client.get_latest_asset_reading(ihs_asset['id'])
                        if latest_reading:
                            timestamp = (
                                latest_reading.get('timestamp')
                                or latest_reading.get('date')
                                or latest_reading.get('created_at')
                                or latest_reading.get('time')
                            )
                            # The IoT API `/assets/{id}/readings` items are flat objects (not nested under `.data`).
                            # Store the full reading payload so the frontend can normalize fields like voltage/power/etc.
                            payload = latest_reading
                            reading_data = {
                                'asset_id': asset_id,
                                'reading_type': asset_type,
                                'timestamp': timestamp,
                                'data': json.dumps(payload)
                            }
                            self.reading_repo.create(reading_data)
                            stats['readings'] += 1
                    except Exception as e:
                        logger.warning(f"Failed to fetch reading for asset {ihs_asset['id']}: {e}")

            prune_stats = self._prune_stale_sites(api_external_ids)
            stats.update(prune_stats)

            logger.info(f"Sync complete: {stats}")
            self.metadata_repo.record_sync_success(stats)
            return stats

        except Exception as e:
            logger.error(f"Sync failed: {e}", exc_info=True)
            self.metadata_repo.record_sync_failure(str(e))
            raise
        finally:
            self._sync_lock.release()


    def sync_readings_only(self) -> Dict:
        if not self._sync_lock.acquire(blocking=False):
            logger.info("IHS sync lock held; skipping readings-only sync")
            return {'readings': 0, 'skipped': True}

        try:
            assets = self.asset_repo.get_all()
            synced = 0

            for asset in assets:
                external_id = asset.get('external_id')
                if not external_id:
                    continue
                try:
                    latest_reading = self.api_client.get_latest_asset_reading(external_id)
                    if not latest_reading:
                        continue
                    timestamp = (
                        latest_reading.get('timestamp')
                        or latest_reading.get('date')
                        or latest_reading.get('created_at')
                        or latest_reading.get('time')
                    )
                    self.reading_repo.create({
                        'asset_id': asset['id'],
                        'reading_type': asset.get('type', 'UNKNOWN'),
                        'timestamp': timestamp,
                        'data': json.dumps(latest_reading),
                    })
                    synced += 1
                except Exception as e:
                    logger.warning(f"Failed to fetch reading for asset {external_id}: {e}")

            logger.info(f"Readings-only sync complete: {synced}/{len(assets)} assets")
            return {'readings': synced}
        finally:
            self._sync_lock.release()


# Singleton instance
_sync_service = None


def get_ihs_sync_service() -> IHSSyncService:
    global _sync_service
    if _sync_service is None:
        _sync_service = IHSSyncService()
    return _sync_service
