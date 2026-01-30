from typing import List, Optional, Dict, Any
from db.client import get_database

class AlarmRepository:
    def get_all(self, status: Optional[str] = None, severity: Optional[str] = None,
                category: Optional[str] = None, site: Optional[str] = None,
                source: Optional[str] = None) -> List[Dict]:
        db = get_database()
        query = 'SELECT * FROM alarms WHERE 1=1'
        params = []

        if status:
            query += ' AND status = ?'
            params.append(status)

        if severity:
            query += ' AND severity = ?'
            params.append(severity)

        if category:
            query += ' AND category = ?'
            params.append(category)

        if site:
            query += ' AND site = ?'
            params.append(site)

        if source:
            query += ' AND source = ?'
            params.append(source)

        query += ' ORDER BY timestamp DESC'

        cursor = db.execute(query, params)
        return [dict(row) for row in cursor.fetchall()]

    def get_by_id(self, alarm_id: str) -> Optional[Dict]:
        db = get_database()
        cursor = db.execute('SELECT * FROM alarms WHERE id = ?', (alarm_id,))
        row = cursor.fetchone()
        return dict(row) if row else None

    def create(self, alarm: Dict[str, Any]):
        db = get_database()
        db.execute('''
            INSERT INTO alarms (
                id, timestamp, site, region, severity, category, message, status,
                details, threshold_id, asset_id, reading_id, source
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            alarm['id'],
            alarm['timestamp'],
            alarm['site'],
            alarm.get('region'),
            alarm['severity'],
            alarm['category'],
            alarm['message'],
            alarm['status'],
            alarm.get('details'),
            alarm.get('threshold_id'),
            alarm.get('asset_id'),
            alarm.get('reading_id'),
            alarm.get('source', 'excel')
        ))
        db.commit()

    def get_all_with_threshold_info(self, status: Optional[str] = None, severity: Optional[str] = None,
                                     category: Optional[str] = None, site: Optional[str] = None,
                                     source: Optional[str] = None) -> List[Dict]:
        db = get_database()
        query = '''
            SELECT
                a.*,
                asset.name as asset_name,
                asset.type as asset_type,
                COALESCE(site_by_asset.id, site_by_name.id) as site_id,
                COALESCE(site_by_asset.zone, site_by_name.zone) as site_zone,
                COALESCE(site_by_asset.region, site_by_name.region) as site_region,
                CASE WHEN t.id IS NOT NULL THEN 1 ELSE 0 END as threshold_exists,
                t.description as threshold_description,
                t.parameter as threshold_parameter
            FROM alarms a
            LEFT JOIN thresholds t ON a.threshold_id = t.id
            LEFT JOIN assets asset ON a.asset_id = asset.id
            LEFT JOIN sites site_by_asset ON site_by_asset.id = asset.site_id
            LEFT JOIN sites site_by_name ON site_by_name.name = a.site
            WHERE 1=1
        '''
        params = []

        if status:
            query += ' AND a.status = ?'
            params.append(status)
        if severity:
            query += ' AND a.severity = ?'
            params.append(severity)
        if category:
            query += ' AND a.category = ?'
            params.append(category)
        if site:
            query += ' AND a.site = ?'
            params.append(site)
        if source:
            query += ' AND a.source = ?'
            params.append(source)

        query += ' ORDER BY a.timestamp DESC'

        cursor = db.execute(query, params)
        return [dict(row) for row in cursor.fetchall()]

    def get_by_id_with_threshold_info(self, alarm_id: str) -> Optional[Dict]:
        db = get_database()
        cursor = db.execute('''
            SELECT
                a.*,
                asset.name as asset_name,
                asset.type as asset_type,
                COALESCE(site_by_asset.id, site_by_name.id) as site_id,
                COALESCE(site_by_asset.zone, site_by_name.zone) as site_zone,
                COALESCE(site_by_asset.region, site_by_name.region) as site_region,
                CASE WHEN t.id IS NOT NULL THEN 1 ELSE 0 END as threshold_exists,
                t.description as threshold_description,
                t.parameter as threshold_parameter,
                t.value as threshold_value,
                t.unit as threshold_unit,
                t.condition as threshold_condition
            FROM alarms a
            LEFT JOIN thresholds t ON a.threshold_id = t.id
            LEFT JOIN assets asset ON a.asset_id = asset.id
            LEFT JOIN sites site_by_asset ON site_by_asset.id = asset.site_id
            LEFT JOIN sites site_by_name ON site_by_name.name = a.site
            WHERE a.id = ?
        ''', (alarm_id,))
        row = cursor.fetchone()
        return dict(row) if row else None

    def count_active_by_threshold(self, threshold_id: str) -> int:
        db = get_database()
        cursor = db.execute('''
            SELECT COUNT(*)
            FROM alarms
            WHERE threshold_id = ?
              AND status IN ('active', 'acknowledged')
        ''', (threshold_id,))
        return cursor.fetchone()[0]

    def archive_by_threshold_id(self, threshold_id: str) -> int:
        db = get_database()
        cursor = db.execute('''
            UPDATE alarms
            SET status = 'archived'
            WHERE threshold_id = ?
              AND status IN ('active', 'acknowledged')
        ''', (threshold_id,))
        db.commit()
        return cursor.rowcount

    def archive_all(self, include_archived: bool = False) -> int:
        """
        Archive alarms to keep the active list clean.

        By default, only non-archived alarms are affected.
        """
        db = get_database()
        if include_archived:
            cursor = db.execute("UPDATE alarms SET status = 'archived'")
        else:
            cursor = db.execute("UPDATE alarms SET status = 'archived' WHERE status != 'archived'")
        db.commit()
        return cursor.rowcount

    def update_status(self, alarm_id: str, status: str, by: Optional[str] = None, resolution_notes: Optional[str] = None):
        db = get_database()
        if status == 'acknowledged':
            db.execute('''
                UPDATE alarms
                SET status = ?, acknowledged_at = CURRENT_TIMESTAMP, acknowledged_by = ?
                WHERE id = ?
            ''', (status, by, alarm_id))
        elif status == 'resolved':
            db.execute('''
                UPDATE alarms
                SET status = ?, resolved_at = CURRENT_TIMESTAMP, resolved_by = ?, resolution_notes = ?
                WHERE id = ?
            ''', (status, by, resolution_notes, alarm_id))
        db.commit()

    def delete(self, alarm_id: str):
        db = get_database()
        db.execute('DELETE FROM alarms WHERE id = ?', (alarm_id,))
        db.commit()

    def delete_all(self) -> int:
        db = get_database()
        cursor = db.execute('DELETE FROM alarms')
        db.commit()
        return cursor.rowcount

    def get_active_counts_by_site(self) -> Dict[str, int]:
        db = get_database()
        cursor = db.execute('''
            SELECT a.site as site_name, COUNT(*) as cnt
            FROM alarms a
            WHERE a.status = 'active' AND a.site IS NOT NULL AND a.site != ''
            GROUP BY a.site
        ''')
        return {row['site_name']: row['cnt'] for row in cursor.fetchall()}
