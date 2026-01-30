from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import Optional, Dict, Any
from io import BytesIO
from services.report_service import ReportService
from services.csv_export_service import CSVExportService

router = APIRouter()
report_service = ReportService()
csv_service = CSVExportService()

class GenerateReportRequest(BaseModel):
    report_type: str
    period_days: int
    filters: Dict[str, Any] = {}
    granularity: str = 'daily'
    refuel_threshold_liters: float = 100.0
    diesel_price_per_liter: Optional[float] = None
    include_cost_analysis: bool = False
    uptime_threshold: float = 95.0

@router.post("/generate")
def generate_report(request: GenerateReportRequest):
    try:
        params = {
            'period_days': request.period_days,
            'filters': request.filters
        }

        if request.report_type == 'site_uptime':
            params['uptime_threshold'] = request.uptime_threshold
        elif request.report_type == 'energy_consumption':
            params['granularity'] = request.granularity
            params['include_cost_analysis'] = request.include_cost_analysis
        elif request.report_type == 'diesel_utilization':
            params['refuel_threshold_liters'] = request.refuel_threshold_liters
            params['diesel_price_per_liter'] = request.diesel_price_per_liter

        report_id = report_service.generate_report(
            report_type=request.report_type,
            params=params
        )

        return {"report_id": report_id, "status": "completed"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/list")
def list_reports(report_type: Optional[str] = Query(None), limit: int = Query(20)):
    reports = report_service.repo.list_reports(report_type, limit)
    return reports

@router.get("/{report_id}/download/csv")
def download_report_csv(report_id: str):
    report = report_service.repo.get_report(report_id)
    if not report:
        raise HTTPException(status_code=404, detail="Report not found")

    csv_content = csv_service.export_report(
        report_type=report['report_type'],
        report_data=report['data']
    )

    timestamp = report['generated_at'].replace(':', '-').replace(' ', '_')
    filename = f"{report['report_type']}_{timestamp}.csv"

    return StreamingResponse(
        BytesIO(csv_content.encode('utf-8')),
        media_type='text/csv',
        headers={
            'Content-Disposition': f'attachment; filename="{filename}"'
        }
    )

@router.get("/{report_id}")
def get_report(report_id: str):
    report = report_service.repo.get_report(report_id)
    if not report:
        raise HTTPException(status_code=404, detail="Report not found")
    return report
