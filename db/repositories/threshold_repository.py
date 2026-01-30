from typing import List, Optional, Dict, Any
from db.client import get_database

class ThresholdRepository:
    def get_all(self) -> List[Dict]:
        db = get_database()
        cursor = db.execute('SELECT * FROM thresholds ORDER BY category, parameter')
        return [dict(row) for row in cursor.fetchall()]

    def get_enabled(self) -> List[Dict]:
        db = get_database()
        cursor = db.execute('SELECT * FROM thresholds WHERE enabled = 1')
        return [dict(row) for row in cursor.fetchall()]

    def get_by_id(self, threshold_id: str) -> Optional[Dict]:
        db = get_database()
        cursor = db.execute('SELECT * FROM thresholds WHERE id = ?', (threshold_id,))
        row = cursor.fetchone()
        return dict(row) if row else None

    def create(self, threshold: Dict[str, Any]):
        db = get_database()
        db.execute('''
            INSERT INTO thresholds (
                id, category, parameter, condition, value, unit, severity, enabled,
                description, sites, trigger_count, last_triggered, applies_to,
                region_id, cluster_id, site_id, location_name, conditions, logic_operator
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            threshold['id'],
            threshold['category'],
            threshold['parameter'],
            threshold['condition'],
            threshold['value'],
            threshold['unit'],
            threshold['severity'],
            1 if threshold.get('enabled', True) else 0,
            threshold.get('description'),
            threshold['sites'],
            threshold.get('trigger_count', 0),
            threshold.get('last_triggered'),
            threshold['applies_to'],
            threshold.get('region_id'),
            threshold.get('cluster_id'),
            threshold.get('site_id'),
            threshold.get('location_name'),
            threshold.get('conditions'),
            threshold.get('logic_operator')
        ))
        db.commit()

    def update(self, threshold_id: str, updates: Dict[str, Any]):
        db = get_database()
        allowed_fields = {k: v for k, v in updates.items()
                         if k not in ('id', 'created_at', 'updated_at')}

        if not allowed_fields:
            return

        fields = ', '.join([f'{k} = ?' for k in allowed_fields.keys()])
        values = list(allowed_fields.values()) + [threshold_id]

        db.execute(f'''
            UPDATE thresholds
            SET {fields}, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        ''', values)
        db.commit()

    def delete(self, threshold_id: str):
        db = get_database()
        db.execute('DELETE FROM thresholds WHERE id = ?', (threshold_id,))
        db.commit()

    def increment_trigger_count(self, threshold_id: str):
        db = get_database()
        db.execute('''
            UPDATE thresholds
            SET trigger_count = trigger_count + 1, last_triggered = CURRENT_TIMESTAMP
            WHERE id = ?
        ''', (threshold_id,))
        db.commit()
