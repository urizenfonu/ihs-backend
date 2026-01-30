from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse
from typing import Optional
from db.repositories.alarm_repository import AlarmRepository
import json
import re

router = APIRouter()

_LEGACY_THRESHOLD_SOURCES = {"excel", "threshold_migrated"}


def _parse_threshold_expression(expr: str):
    """
    Attempt to parse strings like '< 45.0HZ' or '>= 10.0 L' into (condition, value, unit).
    Returns (None, None, None) if parsing fails.
    """
    if not isinstance(expr, str):
        return None, None, None
    expr = expr.strip()
    if not expr:
        return None, None, None
    match = re.match(r"^(<=|>=|==|!=|<|>)\s*([0-9]+(?:\.[0-9]+)?)\s*([A-Za-z%°]+)?$", expr)
    if not match:
        return None, None, None
    condition = match.group(1)
    value = float(match.group(2))
    unit = match.group(3) or None
    return condition, value, unit


def _is_legacy_threshold_reference(alarm: dict) -> bool:
    threshold_id = alarm.get("threshold_id")
    if not threshold_id:
        return False
    source = alarm.get("source")
    if source in _LEGACY_THRESHOLD_SOURCES:
        return True
    return isinstance(threshold_id, str) and threshold_id.startswith("threshold_")


def _build_threshold_summary(alarm: dict):
    details = alarm.get("details")
    if isinstance(details, str):
        try:
            details = json.loads(details)
        except Exception:
            details = None

    description = alarm.get("threshold_description")
    parameter = alarm.get("threshold_parameter")
    raw_expr = None

    if isinstance(details, dict):
        description = details.get("description") or description
        parameter = details.get("parameter") or parameter
        raw_expr = details.get("threshold") or raw_expr

    condition, value, unit = _parse_threshold_expression(raw_expr) if raw_expr else (None, None, None)

    return {
        "description": description,
        "parameter": parameter,
        "condition": condition,
        "value": value,
        "unit": unit,
        "raw": raw_expr,
    }


@router.get("")
@router.get("/")
def get_alarms(
    status: Optional[str] = Query(None),
    severity: Optional[str] = Query(None),
    category: Optional[str] = Query(None),
    site: Optional[str] = Query(None),
    source: Optional[str] = Query(None),
    include_archived: bool = Query(False)
):
    try:
        repo = AlarmRepository()

        # Filter archived unless explicitly requested
        if not include_archived:
            alarms = [a for a in repo.get_all_with_threshold_info(status, severity, category, site, source)
                     if a['status'] != 'archived']
        else:
            alarms = repo.get_all_with_threshold_info(status, severity, category, site, source)

        # Transform
        transformed = []
        for alarm in alarms:
            alarm_copy = dict(alarm)
            if alarm_copy.get('details'):
                try:
                    alarm_copy['details'] = json.loads(alarm_copy['details'])
                except:
                    pass
            details = alarm_copy.get('details')
            if isinstance(details, dict):
                asset_name = alarm_copy.get('asset_name')
                if asset_name and not details.get('asset'):
                    details['asset'] = asset_name
                if asset_name and not details.get('equipment'):
                    details['equipment'] = asset_name
                site_id = alarm_copy.get('site_id')
                if site_id and not details.get('siteId'):
                    details['siteId'] = site_id
                site_region = alarm_copy.get('site_region')
                if site_region and not details.get('region'):
                    details['region'] = site_region
                alarm_copy['details'] = details

            # Add threshold flags
            alarm_copy['thresholdExists'] = bool(alarm_copy.pop('threshold_exists', 0))
            if not alarm_copy['thresholdExists'] and alarm_copy.get('threshold_id'):
                if _is_legacy_threshold_reference(alarm_copy):
                    alarm_copy['thresholdLegacy'] = True
                    alarm_copy['thresholdDeleted'] = False
                    alarm_copy['thresholdSummary'] = _build_threshold_summary(alarm_copy)
                else:
                    alarm_copy['thresholdDeleted'] = True

            transformed.append(alarm_copy)

        return transformed
    except Exception as e:
        return {"error": f"Failed to fetch alarms: {str(e)}"}, 500

@router.get("/counts-by-site")
def get_alarm_counts_by_site():
    """Get active alarm counts grouped by site ID"""
    try:
        repo = AlarmRepository()
        return repo.get_active_counts_by_site()
    except Exception as e:
        return {"error": f"Failed to fetch alarm counts: {str(e)}"}, 500

@router.post("/clear")
def clear_alarms(action: str = Query("archive", pattern="^(archive|delete)$")):
    """
    Clear alarms without touching thresholds.

    - archive (default): set all non-archived alarms to status='archived'
    - delete: permanently delete all alarms
    """
    try:
        repo = AlarmRepository()
        if action == "delete":
            affected = repo.delete_all()
        else:
            affected = repo.archive_all()
        return {"success": True, "action": action, "affected": affected}
    except Exception as e:
        return {"error": f"Failed to clear alarms: {str(e)}"}, 500

@router.get("/{alarm_id}")
def get_alarm_details(alarm_id: str):
    """Get detailed alarm with threshold info"""
    try:
        repo = AlarmRepository()
        alarm = repo.get_by_id_with_threshold_info(alarm_id)

        if not alarm:
            return JSONResponse({"error": "Alarm not found"}, status_code=404)

        if alarm.get('details'):
            try:
                alarm['details'] = json.loads(alarm['details'])
            except:
                pass
        details = alarm.get('details')
        if isinstance(details, dict):
            asset_name = alarm.get('asset_name')
            if asset_name and not details.get('asset'):
                details['asset'] = asset_name
            if asset_name and not details.get('equipment'):
                details['equipment'] = asset_name
            site_id = alarm.get('site_id')
            if site_id and not details.get('siteId'):
                details['siteId'] = site_id
            site_region = alarm.get('site_region')
            if site_region and not details.get('region'):
                details['region'] = site_region
            alarm['details'] = details

        # Add threshold flags
        alarm['thresholdExists'] = bool(alarm.pop('threshold_exists', 0))
        if not alarm['thresholdExists'] and alarm.get('threshold_id'):
            if _is_legacy_threshold_reference(alarm):
                alarm['thresholdLegacy'] = True
                alarm['thresholdDeleted'] = False
            else:
                alarm['thresholdDeleted'] = True

            alarm['thresholdSummary'] = {
                **_build_threshold_summary(alarm),
                "value": alarm.pop("threshold_value", None),
                "unit": alarm.pop("threshold_unit", None),
                "condition": alarm.pop("threshold_condition", None),
            }
        else:
            alarm.pop("threshold_description", None)
            alarm.pop("threshold_parameter", None)
            alarm.pop("threshold_value", None)
            alarm.pop("threshold_unit", None)
            alarm.pop("threshold_condition", None)

        return alarm
    except Exception as e:
        return {"error": f"Failed to fetch alarm: {str(e)}"}, 500

@router.get("/stats")
def get_alarm_stats(include_archived: bool = Query(False)):
    """Get alarm statistics by source and severity"""
    try:
        repo = AlarmRepository()
        all_alarms = repo.get_all()

        # Filter archived unless requested
        if not include_archived:
            all_alarms = [a for a in all_alarms if a['status'] != 'archived']

        # Count by source
        by_source = {'excel': 0, 'api': 0}
        by_severity = {'critical': 0, 'high': 0, 'info': 0}

        for alarm in all_alarms:
            source = alarm.get('source', 'excel')
            by_source[source] = by_source.get(source, 0) + 1

            severity = alarm.get('severity', 'info')
            by_severity[severity] = by_severity.get(severity, 0) + 1

        return {
            "total": len(all_alarms),
            "by_source": by_source,
            "by_severity": by_severity
        }
    except Exception as e:
        return {"error": f"Failed to fetch stats: {str(e)}"}, 500

@router.put("/{alarm_id}")
def update_alarm(alarm_id: str, data: dict):
    try:
        repo = AlarmRepository()
        status = data.get('status')
        by = data.get('by')
        resolution_notes = data.get('resolution_notes')

        if status in ['acknowledged', 'resolved']:
            repo.update_status(alarm_id, status, by, resolution_notes)
            return {"success": True}
        else:
            return {"error": "Invalid status"}, 400
    except Exception as e:
        return {"error": f"Failed to update alarm: {str(e)}"}, 500

@router.delete("/{alarm_id}")
def delete_alarm(alarm_id: str):
    try:
        repo = AlarmRepository()
        repo.delete(alarm_id)
        return {"success": True}
    except Exception as e:
        return {"error": f"Failed to delete alarm: {str(e)}"}, 500
