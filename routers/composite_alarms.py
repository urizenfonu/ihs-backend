from fastapi import APIRouter, Query, HTTPException
from pydantic import BaseModel
from typing import Optional
from alarm_monitor import AlarmMonitor
from config import config

router = APIRouter()
monitor = None

class EvaluateRequest(BaseModel):
    asset_id: int
    reading: dict
    site: Optional[str] = "Unknown"
    region: Optional[str] = "Unknown"

async def get_monitor():
    global monitor
    if monitor is None:
        monitor = AlarmMonitor(db_path=config.DATABASE_PATH)
        await monitor.init()
        rule_count = await monitor.count_rules()
        print(f"✅ {rule_count} composite rules loaded")
    return monitor

@router.post("/evaluate")
async def evaluate_alarms(request: EvaluateRequest):
    """Evaluate all 33 rules for a single reading and save alarms to database"""
    try:
        mon = await get_monitor()
        alarms = await mon.evaluate_all(
            asset_id=request.asset_id,
            reading=request.reading,
            site=request.site,
            region=request.region
        )

        # Save alarms to database
        for alarm in alarms:
            try:
                await mon.db.create_alarm(alarm)
            except Exception as e:
                print(f"Failed to save alarm {alarm.id}: {str(e)}")

        alarm_dicts = [
            {
                "id": alarm.id,
                "timestamp": alarm.timestamp.isoformat(),
                "site": alarm.site,
                "region": alarm.region,
                "severity": alarm.severity,
                "category": alarm.category,
                "message": alarm.message,
                "status": alarm.status,
                "composite_rule_id": alarm.composite_rule_id,
                "asset_id": alarm.asset_id,
                "conditions_met": alarm.conditions_met,
                "total_conditions": alarm.total_conditions,
                "samples": alarm.samples,
                "rate_of_change": alarm.rate_of_change
            }
            for alarm in alarms
        ]

        return {
            "alarms": alarm_dicts,
            "count": len(alarm_dicts)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/rules/")
async def get_rules(
    category: Optional[str] = Query(None, description="Category filter")
):
    """Get composite rules"""
    try:
        mon = await get_monitor()
        rules = await mon.get_rules(category=category)

        rule_dicts = [
            {
                "id": rule.id,
                "name": rule.name,
                "description": rule.description,
                "severity": rule.severity,
                "category": rule.category,
                "rule_type": rule.rule_type,
                "enabled": rule.enabled,
                "conditions": [
                    {
                        "parameter": c.parameter,
                        "operator": c.operator,
                        "value": c.value,
                        "unit": c.unit,
                        "source": c.source
                    }
                    for c in rule.conditions
                ],
                "logical_operator": rule.logical_operator,
                "time_window_minutes": rule.time_window_minutes,
                "aggregation_type": rule.aggregation_type
            }
            for rule in rules
        ]

        return {
            "rules": rule_dicts,
            "count": len(rule_dicts)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/rules/stats")
async def get_rule_stats():
    """Get rule statistics by type"""
    try:
        mon = await get_monitor()
        all_rules = await mon.get_rules()

        stats = {
            "total": len(all_rules),
            "by_type": {},
            "by_category": {},
            "by_severity": {}
        }

        for rule in all_rules:
            stats["by_type"][rule.rule_type] = stats["by_type"].get(rule.rule_type, 0) + 1
            stats["by_category"][rule.category] = stats["by_category"].get(rule.category, 0) + 1
            stats["by_severity"][rule.severity] = stats["by_severity"].get(rule.severity, 0) + 1

        return stats
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
