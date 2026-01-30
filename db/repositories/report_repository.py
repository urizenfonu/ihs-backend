from typing import List, Optional, Dict
from db.client import get_database
import json
import uuid

class ReportRepository:
    def save_report(self, report_type: str, period_days: int, filters: dict,
                    summary: dict, data: dict, created_by: str = None) -> str:
        db = get_database()
        report_id = str(uuid.uuid4())

        db.execute('''
            INSERT INTO generated_reports (
                id, report_type, period_days, filters, summary, data, created_by
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            report_id,
            report_type,
            period_days,
            json.dumps(filters),
            json.dumps(summary),
            json.dumps(data),
            created_by
        ))
        db.commit()
        return report_id

    def get_report(self, report_id: str) -> Optional[Dict]:
        db = get_database()
        cursor = db.execute(
            'SELECT * FROM generated_reports WHERE id = ?',
            (report_id,)
        )
        row = cursor.fetchone()
        if not row:
            return None

        report = dict(row)
        report['filters'] = json.loads(report['filters']) if report['filters'] else {}
        report['summary'] = json.loads(report['summary'])
        report['data'] = json.loads(report['data'])
        return report

    def list_reports(self, report_type: str = None, limit: int = 20) -> List[Dict]:
        db = get_database()

        if report_type:
            cursor = db.execute('''
                SELECT id, report_type, generated_at, period_days, filters, summary, status, created_by
                FROM generated_reports
                WHERE report_type = ?
                ORDER BY generated_at DESC
                LIMIT ?
            ''', (report_type, limit))
        else:
            cursor = db.execute('''
                SELECT id, report_type, generated_at, period_days, filters, summary, status, created_by
                FROM generated_reports
                ORDER BY generated_at DESC
                LIMIT ?
            ''', (limit,))

        reports = []
        for row in cursor.fetchall():
            report = dict(row)
            report['filters'] = json.loads(report['filters']) if report['filters'] else {}
            report['summary'] = json.loads(report['summary'])
            reports.append(report)
        return reports

    def delete_report(self, report_id: str):
        db = get_database()
        db.execute('DELETE FROM generated_reports WHERE id = ?', (report_id,))
        db.commit()
