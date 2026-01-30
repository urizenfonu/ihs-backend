from typing import List, Optional, Dict, Any
from db.client import get_database

class ReadingRepository:
    def get_latest_by_asset_id(self, asset_id: int) -> Optional[Dict]:
        db = get_database()
        cursor = db.execute(
            '''
            SELECT * FROM readings
            WHERE asset_id = ?
            ORDER BY id DESC
            LIMIT 1
            ''',
            (asset_id,),
        )
        row = cursor.fetchone()
        return dict(row) if row else None

    def get_latest_by_asset_ids(self, asset_ids: List[int]) -> List[Dict]:
        if not asset_ids:
            return []

        db = get_database()
        placeholders = ','.join(['?'] * len(asset_ids))

        # Use the autoincrement `id` as the recency source (timestamp strings may not be ISO-sortable).
        cursor = db.execute(
            f'''
            SELECT id, asset_id, reading_type, timestamp, data, created_at
            FROM (
              SELECT
                r.*,
                ROW_NUMBER() OVER (PARTITION BY asset_id ORDER BY id DESC) as rn
              FROM readings r
              WHERE asset_id IN ({placeholders})
            )
            WHERE rn = 1
            ''',
            tuple(asset_ids),
        )
        return [dict(row) for row in cursor.fetchall()]

    def get_recent_by_asset_ids(self, asset_ids: List[int], limit_per_asset: int = 25) -> List[Dict]:
        if not asset_ids:
            return []
        limit_per_asset = max(1, int(limit_per_asset))

        db = get_database()
        placeholders = ','.join(['?'] * len(asset_ids))

        # Use the autoincrement `id` as the recency source (timestamp strings may not be ISO-sortable).
        cursor = db.execute(
            f'''
            SELECT id, asset_id, reading_type, timestamp, data, created_at
            FROM (
              SELECT
                r.*,
                ROW_NUMBER() OVER (PARTITION BY asset_id ORDER BY id DESC) as rn
              FROM readings r
              WHERE asset_id IN ({placeholders})
            )
            WHERE rn <= ?
            ORDER BY asset_id, id DESC
            ''',
            (*asset_ids, limit_per_asset),
        )
        return [dict(row) for row in cursor.fetchall()]

    def get_by_asset_id_in_range(self, asset_id: int, start_date: str, end_date: str) -> List[Dict]:
        db = get_database()
        cursor = db.execute('''
            SELECT * FROM readings
            WHERE asset_id = ?
              AND datetime(substr(timestamp, 7, 4) || '-' ||
                          substr(timestamp, 1, 2) || '-' ||
                          substr(timestamp, 4, 2) || ' ' ||
                          substr(timestamp, 12))
                  BETWEEN datetime(?) AND datetime(?)
            ORDER BY id DESC
        ''', (asset_id, start_date, end_date))
        return [dict(row) for row in cursor.fetchall()]

    def get_readings_in_range(self, asset_ids: List[int], start: str, end: str) -> List[Dict]:
        if not asset_ids:
            return []
        db = get_database()
        placeholders = ','.join('?' * len(asset_ids))

        # Use strftime to convert MM/DD/YYYY to YYYY-MM-DD for proper comparison
        cursor = db.execute(f'''
            SELECT * FROM readings
            WHERE asset_id IN ({placeholders})
              AND datetime(substr(timestamp, 7, 4) || '-' ||
                          substr(timestamp, 1, 2) || '-' ||
                          substr(timestamp, 4, 2) || ' ' ||
                          substr(timestamp, 12))
                  BETWEEN datetime(?) AND datetime(?)
            ORDER BY id DESC
        ''', (*asset_ids, start, end))
        return [dict(row) for row in cursor.fetchall()]

    def create(self, reading: Dict[str, Any]) -> int:
        db = get_database()
        cursor = db.execute('''
            INSERT INTO readings (asset_id, reading_type, timestamp, data)
            VALUES (?, ?, ?, ?)
        ''', (
            reading['asset_id'],
            reading['reading_type'],
            reading['timestamp'],
            reading['data']
        ))
        db.commit()
        return cursor.lastrowid

    def delete_by_asset_id(self, asset_id: int):
        db = get_database()
        db.execute('DELETE FROM readings WHERE asset_id = ?', (asset_id,))
        db.commit()

    def delete_by_asset_ids(self, asset_ids: List[int]) -> int:
        if not asset_ids:
            return 0
        db = get_database()
        placeholders = ",".join("?" for _ in asset_ids)
        cursor = db.execute(f'DELETE FROM readings WHERE asset_id IN ({placeholders})', tuple(asset_ids))
        db.commit()
        return cursor.rowcount

    def delete_older_than(self, days: int):
        db = get_database()
        db.execute('''
            DELETE FROM readings
            WHERE timestamp < datetime('now', '-' || ? || ' days')
        ''', (days,))
        db.commit()
