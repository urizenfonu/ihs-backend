from fastapi import APIRouter, Query
from typing import Dict, Any
from db.repositories.threshold_repository import ThresholdRepository
import json
import secrets
import string
import re

def generate_id(size=8):
    chars = string.ascii_letters + string.digits
    return ''.join(secrets.choice(chars) for _ in range(size))

router = APIRouter()

def _parse_threshold_expression(expr: str):
    if not isinstance(expr, str):
        return None, None, None
    expr = expr.strip()
    if not expr:
        return None, None, None
    match = re.match(r"^(<=|>=|==|!=|<|>)\s*([0-9]+(?:\.[0-9]+)?)\s*([A-Za-z%°]+)?$", expr)
    if not match:
        return None, None, None
    return match.group(1), float(match.group(2)), match.group(3) or None


def _transform_threshold_row(t: dict):
    threshold_copy = dict(t)
    threshold_copy['sites'] = json.loads(threshold_copy.get('sites') or "[]")
    threshold_copy['enabled'] = bool(threshold_copy.get('enabled'))
    threshold_copy['triggerCount'] = threshold_copy.get('trigger_count', 0)

    if 'logic_operator' in threshold_copy:
        threshold_copy['logicOperator'] = threshold_copy['logic_operator']

    if threshold_copy.get('conditions'):
        try:
            threshold_copy['conditions'] = json.loads(threshold_copy['conditions'])
        except Exception:
            threshold_copy['conditions'] = None
    else:
        threshold_copy['conditions'] = None

    return threshold_copy


@router.get("/")
def get_thresholds():
    try:
        repo = ThresholdRepository()
        thresholds = repo.get_all()

        # Transform sites and conditions from JSON strings to arrays
        transformed = []
        for t in thresholds:
            transformed.append(_transform_threshold_row(t))

        return transformed
    except Exception as e:
        return {"error": f"Failed to fetch thresholds: {str(e)}"}, 500

@router.get("/{threshold_id}")
def get_threshold(threshold_id: str):
    """
    Fetch a threshold by id.

    If the threshold row is missing but historical alarms still reference it,
    return a synthetic "legacy" threshold so the UI can still render alarm details.
    """
    try:
        repo = ThresholdRepository()
        threshold = repo.get_by_id(threshold_id)
        if threshold:
            return _transform_threshold_row(threshold)

        from db.client import get_database

        db = get_database()
        cursor = db.execute(
            """
            SELECT
                COUNT(*) as cnt,
                MAX(timestamp) as last_triggered
            FROM alarms
            WHERE threshold_id = ?
            """,
            (threshold_id,),
        )
        stats = cursor.fetchone()
        if not stats or stats["cnt"] == 0:
            return {"error": "Threshold not found"}, 404

        cursor = db.execute(
            """
            SELECT category, severity, details
            FROM alarms
            WHERE threshold_id = ?
            ORDER BY timestamp DESC
            LIMIT 1
            """,
            (threshold_id,),
        )
        latest = cursor.fetchone()

        details = None
        if latest and latest["details"]:
            try:
                details = json.loads(latest["details"])
            except Exception:
                details = None

        description = None
        parameter = None
        condition = None
        value = None
        unit = None

        if isinstance(details, dict):
            description = details.get("description") or None
            parameter = details.get("parameter") or None
            raw_expr = details.get("threshold")
            condition, value, unit = _parse_threshold_expression(raw_expr) if raw_expr else (None, None, None)

        return {
            "id": threshold_id,
            "category": latest["category"] if latest else "Unknown",
            "parameter": parameter or "unknown",
            "condition": condition or "==",
            "value": value if value is not None else 0,
            "unit": unit or "",
            "severity": latest["severity"] if latest else "info",
            "enabled": False,
            "description": description,
            "sites": [],
            "trigger_count": stats["cnt"],
            "triggerCount": stats["cnt"],
            "last_triggered": stats["last_triggered"],
            "applies_to": "all",
            "region_id": None,
            "cluster_id": None,
            "site_id": None,
            "location_name": None,
            "conditions": None,
            "logic_operator": "AND",
            "logicOperator": "AND",
            "legacy": True,
            "existsInDb": False,
        }
    except Exception as e:
        return {"error": f"Failed to fetch threshold: {str(e)}"}, 500

@router.post("/")
def create_threshold(data: Dict[str, Any]):
    try:
        repo = ThresholdRepository()

        threshold_data = {
            'id': f"threshold_{generate_id(8)}",
            'category': data['category'],
            'parameter': data['parameter'],
            'condition': data['condition'],
            'value': data['value'],
            'unit': data['unit'],
            'severity': data['severity'],
            'enabled': data.get('enabled', True),
            'description': data.get('description'),
            'sites': json.dumps(data.get('sites', [])),
            'trigger_count': 0,
            'applies_to': data.get('appliesTo', 'all'),
            'region_id': data.get('regionId'),
            'cluster_id': data.get('clusterId'),
            'site_id': data.get('siteId'),
            'location_name': data.get('locationName'),
            'conditions': json.dumps(data['conditions']) if data.get('conditions') else None,
            'logic_operator': data.get('logicOperator', 'AND')
        }

        repo.create(threshold_data)
        return {"success": True}
    except Exception as e:
        return {"error": f"Failed to create threshold: {str(e)}"}, 500

@router.put("/{threshold_id}")
def update_threshold(threshold_id: str, data: Dict[str, Any]):
    try:
        repo = ThresholdRepository()

        # Transform sites to JSON string if present
        if 'sites' in data:
            data['sites'] = json.dumps(data['sites'])

        # Transform conditions to JSON string if present
        if 'conditions' in data:
            data['conditions'] = json.dumps(data['conditions']) if data['conditions'] else None

        # Handle logicOperator field
        if 'logicOperator' in data:
            data['logic_operator'] = data.pop('logicOperator')

        repo.update(threshold_id, data)
        return {"success": True}
    except Exception as e:
        return {"error": f"Failed to update threshold: {str(e)}"}, 500

@router.delete("/{threshold_id}")
def delete_threshold(threshold_id: str, force_archive: bool = Query(False)):
    """
    Delete threshold. Blocks if active alarms exist unless force_archive=true
    """
    try:
        from db.repositories.alarm_repository import AlarmRepository

        repo = ThresholdRepository()
        alarm_repo = AlarmRepository()

        # Check threshold exists
        threshold = repo.get_by_id(threshold_id)
        if not threshold:
            return {"error": "Threshold not found"}, 404

        # Check for active alarms
        active_count = alarm_repo.count_active_by_threshold(threshold_id)

        if active_count > 0:
            if not force_archive:
                return {
                    "error": "Cannot delete threshold with active alarms",
                    "active_alarm_count": active_count,
                    "suggestion": "Resolve alarms first, or use force_archive=true"
                }, 400

            # Archive related alarms
            archived_count = alarm_repo.archive_by_threshold_id(threshold_id)
            print(f"[Threshold Delete] Archived {archived_count} alarms")

        # Delete threshold
        repo.delete(threshold_id)

        return {
            "success": True,
            "archived_alarms": active_count if force_archive else 0
        }
    except Exception as e:
        return {"error": f"Failed to delete threshold: {str(e)}"}, 500

@router.get("/{threshold_id}/alarms")
def get_threshold_alarms(threshold_id: str):
    """Get alarm counts for threshold"""
    try:
        from db.repositories.alarm_repository import AlarmRepository

        alarm_repo = AlarmRepository()
        alarms = alarm_repo.get_all()

        threshold_alarms = [a for a in alarms if a.get('threshold_id') == threshold_id]

        by_status = {}
        for alarm in threshold_alarms:
            status = alarm['status']
            by_status[status] = by_status.get(status, 0) + 1

        return {
            "threshold_id": threshold_id,
            "total_alarms": len(threshold_alarms),
            "by_status": by_status
        }
    except Exception as e:
        return {"error": f"Failed to fetch threshold alarms: {str(e)}"}, 500
