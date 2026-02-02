import csv
from io import StringIO
from typing import Dict

class CSVExportService:
    def export_site_uptime_report(self, report_data: dict) -> str:
        output = StringIO()
        writer = csv.writer(output)

        writer.writerow([
            'Site ID', 'Site Name', 'Region', 'Uptime %',
            'Total Hours', 'Online Hours', 'Offline Hours',
            'Downtime Periods Count'
        ])

        for site in report_data.get('sites', []):
            writer.writerow([
                site.get('site_id', ''),
                site.get('site_name', ''),
                site.get('region', ''),
                f"{site.get('uptime_percent', 0):.2f}",
                site.get('total_hours', 0),
                site.get('online_hours', 0),
                site.get('offline_hours', 0),
                len(site.get('downtime_periods', []))
            ])

        writer.writerow([])
        writer.writerow(['SUMMARY'])
        summary = report_data.get('summary', {})
        writer.writerow(['Total Sites', summary.get('total_sites', 0)])
        writer.writerow(['Average Uptime %', f"{summary.get('avg_uptime_percent', 0):.2f}"])
        writer.writerow(['Sites Meeting Target', summary.get('sites_meeting_target', 0)])
        writer.writerow(['Sites Below Target', summary.get('sites_below_target', 0)])

        return output.getvalue()

    def export_alarm_summary_report(self, report_data: dict) -> str:
        output = StringIO()
        writer = csv.writer(output)

        writer.writerow([
            'Site Name', 'Total Alarms', 'Critical', 'Warning', 'Info'
        ])

        for site in report_data.get('top_sites', []):
            writer.writerow([
                site.get('site_name', ''),
                site.get('alarm_count', 0),
                site.get('critical_count', 0),
                site.get('warning_count', 0),
                site.get('info_count', 0)
            ])

        writer.writerow([])
        writer.writerow(['SUMMARY'])
        summary = report_data.get('summary', {})
        writer.writerow(['Total Alarms', summary.get('total_alarms', 0)])

        by_severity = summary.get('by_severity', {})
        writer.writerow(['Critical', by_severity.get('critical', 0)])
        writer.writerow(['Warning', by_severity.get('warning', 0)])
        writer.writerow(['Info', by_severity.get('info', 0)])
        writer.writerow(['MTTA (hours)', f"{summary.get('mtta_hours', 0):.2f}"])
        writer.writerow(['MTTR (hours)', f"{summary.get('mttr_hours', 0):.2f}"])

        return output.getvalue()

    def export_energy_consumption_report(self, report_data: dict) -> str:
        output = StringIO()
        writer = csv.writer(output)

        writer.writerow([
            'Site Name', 'Total kWh', 'Grid kWh', 'Generator kWh',
            'Solar kWh', 'Battery kWh'
        ])

        for site in report_data.get('top_sites', []):
            by_source = site.get('by_source', {})
            writer.writerow([
                site.get('site_name', ''),
                f"{site.get('total_kwh', 0):.2f}",
                f"{by_source.get('grid', 0):.2f}",
                f"{by_source.get('generator', 0):.2f}",
                f"{by_source.get('solar', 0):.2f}",
                f"{by_source.get('battery', 0):.2f}"
            ])

        writer.writerow([])
        writer.writerow(['SUMMARY'])
        summary = report_data.get('summary', {})
        writer.writerow(['Total Energy (kWh)', f"{summary.get('total_kwh', 0):.2f}"])
        writer.writerow(['Grid Dependency %', f"{summary.get('grid_dependency_percent', 0):.2f}"])
        writer.writerow(['Renewable %', f"{summary.get('renewable_percent', 0):.2f}"])

        writer.writerow([])
        writer.writerow(['SOURCE BREAKDOWN'])
        writer.writerow(['Source', 'kWh', 'Percent', 'Peak kW'])

        by_source = summary.get('by_source', {})
        for source in ['grid', 'generator', 'solar', 'battery']:
            src_data = by_source.get(source, {})
            writer.writerow([
                source.capitalize(),
                f"{src_data.get('kwh', 0):.2f}",
                f"{src_data.get('percent', 0):.2f}",
                f"{src_data.get('peak_kw', 0):.2f}"
            ])

        return output.getvalue()

    def export_diesel_utilization_report(self, report_data: dict) -> str:
        output = StringIO()
        writer = csv.writer(output)

        writer.writerow([
            'Site Name', 'Liters Consumed', 'Refuel Count',
            'Runtime Hours', 'Efficiency (L/hr)'
        ])

        for site in report_data.get('top_consumers', []):
            writer.writerow([
                site.get('site_name', ''),
                f"{site.get('liters_consumed', 0):.2f}",
                site.get('refuel_count', 0),
                f"{site.get('runtime_hours', 0):.2f}",
                f"{site.get('efficiency_lph', 0):.2f}"
            ])

        writer.writerow([])
        writer.writerow(['SUMMARY'])
        summary = report_data.get('summary', {})
        writer.writerow(['Total Diesel Consumed (L)', f"{summary.get('total_liters_consumed', 0):.2f}"])
        writer.writerow(['Total Refuels', summary.get('total_refuels', 0)])
        writer.writerow(['Average Efficiency (L/hr)', f"{summary.get('avg_efficiency_lph', 0):.2f}"])
        writer.writerow(['Total Runtime (hours)', f"{summary.get('total_runtime_hours', 0):.2f}"])

        if summary.get('total_cost'):
            writer.writerow(['Total Cost', f"${summary.get('total_cost', 0):.2f}"])

        return output.getvalue()

    def export_report(self, report_type: str, report_data: dict) -> str:
        exporters = {
            'site_uptime': self.export_site_uptime_report,
            'alarm_summary': self.export_alarm_summary_report,
            'energy_consumption': self.export_energy_consumption_report,
            'diesel_utilization': self.export_diesel_utilization_report
        }

        exporter = exporters.get(report_type)
        if not exporter:
            raise ValueError(f"No CSV exporter for report type: {report_type}")

        return exporter(report_data)
