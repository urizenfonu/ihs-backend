from typing import List, Optional, Dict, Any
from db.client import get_database

class AssetRepository:
    def get_all(self) -> List[Dict]:
        db = get_database()
        cursor = db.execute('SELECT * FROM assets ORDER BY site_id, name')
        return [dict(row) for row in cursor.fetchall()]

    def get_by_site_id(self, site_id: int) -> List[Dict]:
        db = get_database()
        cursor = db.execute('SELECT * FROM assets WHERE site_id = ?', (site_id,))
        return [dict(row) for row in cursor.fetchall()]

    def get_by_site_ids(self, site_ids: List[int]) -> List[Dict]:
        if not site_ids:
            return []
        db = get_database()
        placeholders = ",".join("?" for _ in site_ids)
        cursor = db.execute(f'SELECT * FROM assets WHERE site_id IN ({placeholders})', tuple(site_ids))
        return [dict(row) for row in cursor.fetchall()]

    def get_ids_by_site_ids(self, site_ids: List[int]) -> List[int]:
        if not site_ids:
            return []
        db = get_database()
        placeholders = ",".join("?" for _ in site_ids)
        cursor = db.execute(f'SELECT id FROM assets WHERE site_id IN ({placeholders})', tuple(site_ids))
        return [row[0] for row in cursor.fetchall() if row[0] is not None]

    def delete_by_site_ids(self, site_ids: List[int]) -> int:
        if not site_ids:
            return 0
        db = get_database()
        placeholders = ",".join("?" for _ in site_ids)
        cursor = db.execute(f'DELETE FROM assets WHERE site_id IN ({placeholders})', tuple(site_ids))
        db.commit()
        return cursor.rowcount

    def get_by_type(self, asset_type: str) -> List[Dict]:
        db = get_database()
        cursor = db.execute('SELECT * FROM assets WHERE type = ?', (asset_type,))
        return [dict(row) for row in cursor.fetchall()]

    def get_by_id(self, asset_id: int) -> Optional[Dict]:
        db = get_database()
        cursor = db.execute('SELECT * FROM assets WHERE id = ?', (asset_id,))
        row = cursor.fetchone()
        return dict(row) if row else None

    def get_by_external_id(self, external_id: int) -> Optional[Dict]:
        db = get_database()
        cursor = db.execute('SELECT * FROM assets WHERE external_id = ?', (external_id,))
        row = cursor.fetchone()
        return dict(row) if row else None

    def create(self, asset: Dict[str, Any]) -> int:
        db = get_database()
        cursor = db.execute('''
            INSERT INTO assets (external_id, name, type, site_id, last_reading_timestamp, config)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (
            asset.get('external_id'),
            asset['name'],
            asset['type'],
            asset['site_id'],
            asset.get('last_reading_timestamp'),
            asset.get('config')
        ))
        db.commit()
        return cursor.lastrowid

    def update_last_reading(self, asset_id: int, timestamp: str):
        db = get_database()
        db.execute('UPDATE assets SET last_reading_timestamp = ? WHERE id = ?',
                       (timestamp, asset_id))
        db.commit()

    def get_by_region(self, region: str) -> List[Dict]:
        db = get_database()
        cursor = db.execute('''
            SELECT a.* FROM assets a
            JOIN sites s ON a.site_id = s.id
            WHERE s.region = ?
        ''', (region,))
        return [dict(row) for row in cursor.fetchall()]

    def get_by_zone(self, zone: str) -> List[Dict]:
        db = get_database()
        cursor = db.execute('''
            SELECT a.* FROM assets a
            JOIN sites s ON a.site_id = s.id
            WHERE s.zone = ?
        ''', (zone,))
        return [dict(row) for row in cursor.fetchall()]

    def upsert_by_external_id(self, external_id: int, asset_data: Dict) -> int:
        db = get_database()
        existing = self.get_by_external_id(external_id)
        if existing:
            db.execute(
                'UPDATE assets SET name = ?, type = ?, site_id = ?, tenant_channels = ?, config = ? WHERE id = ?',
                (asset_data['name'], asset_data['type'], asset_data['site_id'],
                 asset_data.get('tenant_channels'), asset_data.get('config'), existing['id'])
            )
            db.commit()
            return existing['id']

        cursor = db.execute(
            'INSERT INTO assets (external_id, name, type, site_id, tenant_channels, config) VALUES (?, ?, ?, ?, ?, ?)',
            (external_id, asset_data['name'], asset_data['type'], asset_data['site_id'],
             asset_data.get('tenant_channels'), asset_data.get('config'))
        )
        db.commit()
        return cursor.lastrowid
