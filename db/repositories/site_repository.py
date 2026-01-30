from typing import List, Optional, Dict, Any
from db.client import get_database

class SiteRepository:
    def get_all(self) -> List[Dict]:
        db = get_database()
        cursor = db.execute('SELECT * FROM sites ORDER BY is_lagos DESC, name')
        return [dict(row) for row in cursor.fetchall()]

    def get_all_external(self) -> List[Dict]:
        db = get_database()
        cursor = db.execute('SELECT * FROM sites WHERE external_id IS NOT NULL ORDER BY name')
        return [dict(row) for row in cursor.fetchall()]

    def get_external_ids(self) -> List[int]:
        db = get_database()
        cursor = db.execute('SELECT external_id FROM sites WHERE external_id IS NOT NULL')
        return [row[0] for row in cursor.fetchall() if row[0] is not None]

    def get_ids_by_external_ids(self, external_ids: List[int]) -> List[int]:
        if not external_ids:
            return []
        db = get_database()
        placeholders = ",".join("?" for _ in external_ids)
        cursor = db.execute(f'SELECT id FROM sites WHERE external_id IN ({placeholders})', tuple(external_ids))
        return [row[0] for row in cursor.fetchall() if row[0] is not None]

    def get_lagos(self) -> Optional[Dict]:
        db = get_database()
        cursor = db.execute('SELECT * FROM sites WHERE region = ? OR zone = ? LIMIT 1', ('Lagos', 'Lagos'))
        row = cursor.fetchone()
        return dict(row) if row else None

    def get_by_id(self, site_id: int) -> Optional[Dict]:
        db = get_database()
        cursor = db.execute('SELECT * FROM sites WHERE id = ?', (site_id,))
        row = cursor.fetchone()
        return dict(row) if row else None

    def create(self, site: Dict[str, Any]) -> int:
        db = get_database()
        cursor = db.execute('''
            INSERT INTO sites (name, region, zone, state, cluster_code, zone_external_id, is_lagos)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            site['name'],
            site['region'],
            site.get('zone'),
            site.get('state'),
            site.get('cluster_code'),
            site.get('zone_external_id'),
            1 if site.get('is_lagos', False) else 0
        ))
        db.commit()
        return cursor.lastrowid

    def create_lagos_if_not_exists(self) -> int:
        existing = self.get_lagos()
        if existing:
            return existing['id']

        return self.create({
            'name': 'Lagos',
            'region': 'Lagos',
            'zone': 'South West',
            'state': None,
            'cluster_code': None,
            'zone_external_id': None,
            'is_lagos': True
        })

    def get_by_external_id(self, external_id: int) -> Optional[Dict]:
        db = get_database()
        cursor = db.execute('SELECT * FROM sites WHERE external_id = ?', (external_id,))
        row = cursor.fetchone()
        return dict(row) if row else None

    def get_by_name(self, name: str) -> Optional[Dict]:
        db = get_database()
        cursor = db.execute('SELECT * FROM sites WHERE name = ? LIMIT 1', (name,))
        row = cursor.fetchone()
        return dict(row) if row else None

    def upsert_by_external_id(self, external_id: int, site_data: Dict) -> int:
        db = get_database()
        existing = self.get_by_external_id(external_id)
        if existing:
            db.execute(
                'UPDATE sites SET name = ?, region = ?, zone = ?, state = ?, cluster_code = ?, zone_external_id = ? WHERE id = ?',
                (
                    site_data['name'],
                    site_data.get('region'),
                    site_data.get('zone'),
                    site_data.get('state'),
                    site_data.get('cluster_code'),
                    site_data.get('zone_external_id'),
                    existing['id'],
                )
            )
            db.commit()
            return existing['id']

        cursor = db.execute(
            'INSERT INTO sites (external_id, name, region, zone, state, cluster_code, zone_external_id, is_lagos) VALUES (?, ?, ?, ?, ?, ?, ?, 0)',
            (
                external_id,
                site_data['name'],
                site_data.get('region'),
                site_data.get('zone'),
                site_data.get('state'),
                site_data.get('cluster_code'),
                site_data.get('zone_external_id'),
            )
        )
        db.commit()
        return cursor.lastrowid

    def delete_by_ids(self, site_ids: List[int]) -> int:
        if not site_ids:
            return 0
        db = get_database()
        placeholders = ",".join("?" for _ in site_ids)
        cursor = db.execute(f'DELETE FROM sites WHERE id IN ({placeholders})', tuple(site_ids))
        db.commit()
        return cursor.rowcount
