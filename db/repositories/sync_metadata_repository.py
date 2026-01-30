from typing import Optional, Dict
from db.client import get_database
import json
from datetime import datetime


class SyncMetadataRepository:
    def get_metadata(self) -> Optional[Dict]:
        db = get_database()
        cursor = db.execute('SELECT * FROM sync_metadata WHERE id = 1')
        row = cursor.fetchone()
        if row:
            data = dict(row)
            errors = data.get('errors')
            if isinstance(errors, str):
                try:
                    data['errors'] = json.loads(errors)
                except:
                    data['errors'] = []
            elif isinstance(errors, list):
                data['errors'] = errors
            else:
                data['errors'] = []
            return data
        return None

    def update_metadata(self, data: Dict):
        db = get_database()
        fields = []
        values = []
        for key, value in data.items():
            if key == 'errors' and isinstance(value, list):
                value = json.dumps(value)
            fields.append(f"{key} = ?")
            values.append(value)

        values.append(datetime.now().isoformat())
        fields.append("updated_at = ?")

        query = f"UPDATE sync_metadata SET {', '.join(fields)} WHERE id = 1"
        db.execute(query, tuple(values))
        db.commit()

    def record_sync_start(self):
        self.update_metadata({
            'last_sync_time': datetime.now().isoformat(),
            'status': 'running'
        })

    def record_sync_success(self, stats: Dict):
        self.update_metadata({
            'last_success_time': datetime.now().isoformat(),
            'status': 'success',
            'sites_synced': stats.get('sites', 0),
            'assets_synced': stats.get('assets', 0),
            'readings_synced': stats.get('readings', 0),
            'errors': []
        })

    def record_sync_failure(self, error: str):
        metadata = self.get_metadata() or {}
        errors = metadata.get('errors') or []
        if not isinstance(errors, list):
            errors = []
        errors.append({'time': datetime.now().isoformat(), 'error': error})
        errors = errors[-10:]  # Keep last 10 errors

        self.update_metadata({
            'status': 'failed',
            'errors': errors
        })
