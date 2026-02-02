from datetime import datetime, timedelta
from typing import Dict, List, Optional
from db.repositories.site_repository import SiteRepository
from db.repositories.asset_repository import AssetRepository
from db.repositories.reading_repository import ReadingRepository
from db.repositories.alarm_repository import AlarmRepository
from db.repositories.report_repository import ReportRepository
import json
from collections import defaultdict

def to_db_timestamp(dt: datetime) -> str:
    """Convert datetime to ISO format for SQLite datetime()"""
    return dt.strftime('%Y-%m-%d %H:%M:%S')

class SiteUptimeCalculator:
    def __init__(self):
        self.site_repo = SiteRepository()
        self.asset_repo = AssetRepository()
        self.reading_repo = ReadingRepository()

    def generate(self, period_days: int, filters: dict, uptime_threshold: float = 95.0) -> dict:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=period_days)

        sites = self._get_filtered_sites(filters)
        site_uptimes = []

        for site in sites:
            uptime_data = self._calculate_site_uptime(
                site, to_db_timestamp(start_date), to_db_timestamp(end_date), period_days
            )
            if uptime_data:
                site_uptimes.append(uptime_data)

        summary = self._aggregate_summary(site_uptimes, uptime_threshold)
        trend = self._calculate_trend(site_uptimes)

        return {
            'summary': summary,
            'sites': site_uptimes,
            'trend': trend
        }

    def _get_filtered_sites(self, filters: dict) -> List[Dict]:
        all_sites = self.site_repo.get_all()

        if filters.get('region'):
            all_sites = [s for s in all_sites if s['region'] == filters['region']]

        if filters.get('site'):
            all_sites = [s for s in all_sites if s['name'] == filters['site']]

        return all_sites

    def _calculate_site_uptime(self, site: dict, start: str, end: str, period_days: int) -> Optional[Dict]:
        assets = self.asset_repo.get_by_site_id(site['id'])
        if not assets:
            return None

        energy_assets = [a for a in assets if a['type'] in ['GENERATOR', 'AC_METER', 'DC_METER']]
        if not energy_assets:
            return None

        asset_ids = [a['id'] for a in energy_assets]
        readings = self.reading_repo.get_readings_in_range(asset_ids, start, end)

        if not readings:
            return None

        hourly_status = self._calculate_hourly_status(readings)

        if not hourly_status:
            return None
        total_hours = period_days * 24
        online_hours = sum(1 for is_online in hourly_status.values() if is_online)
        offline_hours = total_hours - online_hours
        uptime_percent = (online_hours / total_hours * 100) if total_hours else 0

        downtime_periods = self._extract_downtime_periods(hourly_status)

        return {
            'site_id': site['id'],
            'site_name': site['name'],
            'region': site['region'],
            'uptime_percent': round(uptime_percent, 2),
            'total_hours': total_hours,
            'online_hours': online_hours,
            'offline_hours': offline_hours,
            'downtime_periods': downtime_periods
        }

    def _calculate_hourly_status(self, readings: List[Dict]) -> Dict[str, bool]:
        hourly_power = defaultdict(float)

        for reading in readings:
            try:
                # Parse MM/DD/YYYY HH:MM:SS format
                ts_str = reading['timestamp']
                dt = datetime.strptime(ts_str, '%m/%d/%Y %H:%M:%S')
                hour_key = dt.strftime('%Y-%m-%d %H:00')

                data = json.loads(reading['data']) if isinstance(reading['data'], str) else reading['data']
                power = self._extract_power(data)
                hourly_power[hour_key] += power
            except Exception as e:
                continue

        return {hour: power > 0 for hour, power in hourly_power.items()}

    def _extract_power(self, data: dict) -> float:
        for key in ['gen_total_watt', 'total_active_power', 'active_power', 'POWER', 'power', 'kw']:
            if key in data:
                try:
                    val = float(data[key])
                    if key == 'gen_total_watt':
                        return val / 1000
                    return val
                except (ValueError, TypeError):
                    continue
        return 0.0

    def _extract_downtime_periods(self, hourly_status: Dict[str, bool]) -> List[Dict]:
        periods = []
        sorted_hours = sorted(hourly_status.keys())

        downtime_start = None
        for hour in sorted_hours:
            is_online = hourly_status[hour]

            if not is_online and downtime_start is None:
                downtime_start = hour
            elif is_online and downtime_start is not None:
                duration = self._calculate_duration(downtime_start, hour)
                periods.append({
                    'start': downtime_start,
                    'end': hour,
                    'duration_hours': duration
                })
                downtime_start = None

        if downtime_start:
            last_hour = sorted_hours[-1] if sorted_hours else downtime_start
            duration = self._calculate_duration(downtime_start, last_hour)
            periods.append({
                'start': downtime_start,
                'end': last_hour,
                'duration_hours': duration
            })

        return periods

    def _calculate_duration(self, start: str, end: str) -> int:
        try:
            start_dt = datetime.strptime(start, '%Y-%m-%d %H:%M')
            end_dt = datetime.strptime(end, '%Y-%m-%d %H:%M')
            return int((end_dt - start_dt).total_seconds() / 3600)
        except Exception:
            return 1

    def _aggregate_summary(self, site_uptimes: List[Dict], threshold: float) -> Dict:
        if not site_uptimes:
            return {
                'total_sites': 0,
                'avg_uptime_percent': 0,
                'sites_meeting_target': 0,
                'sites_below_target': 0
            }

        total_sites = len(site_uptimes)
        avg_uptime = sum(s['uptime_percent'] for s in site_uptimes) / total_sites
        meeting_target = sum(1 for s in site_uptimes if s['uptime_percent'] >= threshold)
        below_target = total_sites - meeting_target

        return {
            'total_sites': total_sites,
            'avg_uptime_percent': round(avg_uptime, 2),
            'sites_meeting_target': meeting_target,
            'sites_below_target': below_target
        }

    def _calculate_trend(self, site_uptimes: List[Dict]) -> List[Dict]:
        return []


class AlarmSummaryGenerator:
    def __init__(self):
        self.alarm_repo = AlarmRepository()

    def generate(self, period_days: int, filters: dict) -> dict:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=period_days)

        alarms = self._get_filtered_alarms(to_db_timestamp(start_date), to_db_timestamp(end_date), filters)

        summary = self._calculate_summary(alarms)
        top_sites = self._calculate_top_sites(alarms)
        top_categories = self._calculate_top_categories(alarms)
        trend = self._calculate_trend(alarms, period_days)
        recurring = self._find_recurring_alarms(alarms)

        return {
            'summary': summary,
            'top_sites': top_sites,
            'top_categories': top_categories,
            'trend': trend,
            'recurring_alarms': recurring
        }

    def _get_filtered_alarms(self, start: str, end: str, filters: dict) -> List[Dict]:
        alarms = self.alarm_repo.get_all_with_threshold_info(
            severity=filters.get('severity'),
            category=filters.get('category'),
            site=filters.get('site')
        )

        filtered = []
        for alarm in alarms:
            try:
                alarm_time = datetime.fromisoformat(alarm['timestamp'].replace('Z', '+00:00'))
                start_time = datetime.fromisoformat(start.replace('Z', '+00:00'))
                end_time = datetime.fromisoformat(end.replace('Z', '+00:00'))

                if start_time <= alarm_time <= end_time:
                    filtered.append(alarm)
            except Exception:
                continue

        return filtered

    def _calculate_summary(self, alarms: List[Dict]) -> Dict:
        by_severity = {'critical': 0, 'warning': 0, 'info': 0}
        by_status = {'active': 0, 'acknowledged': 0, 'resolved': 0}

        total_ack_time = 0
        ack_count = 0
        total_resolve_time = 0
        resolve_count = 0

        for alarm in alarms:
            severity = alarm.get('severity', '').lower()
            if severity in by_severity:
                by_severity[severity] += 1

            status = alarm.get('status', '').lower()
            if status in by_status:
                by_status[status] += 1

            if alarm.get('acknowledged_at'):
                try:
                    created = datetime.fromisoformat(alarm['timestamp'].replace('Z', '+00:00'))
                    acked = datetime.fromisoformat(alarm['acknowledged_at'].replace('Z', '+00:00'))
                    total_ack_time += (acked - created).total_seconds() / 3600
                    ack_count += 1
                except Exception:
                    pass

            if alarm.get('resolved_at'):
                try:
                    created = datetime.fromisoformat(alarm['timestamp'].replace('Z', '+00:00'))
                    resolved = datetime.fromisoformat(alarm['resolved_at'].replace('Z', '+00:00'))
                    total_resolve_time += (resolved - created).total_seconds() / 3600
                    resolve_count += 1
                except Exception:
                    pass

        mtta = total_ack_time / ack_count if ack_count else 0
        mttr = total_resolve_time / resolve_count if resolve_count else 0

        return {
            'total_alarms': len(alarms),
            'by_severity': by_severity,
            'by_status': by_status,
            'mtta_hours': round(mtta, 2),
            'mttr_hours': round(mttr, 2)
        }

    def _calculate_top_sites(self, alarms: List[Dict]) -> List[Dict]:
        site_counts = defaultdict(lambda: {'total': 0, 'critical': 0, 'warning': 0, 'info': 0})

        for alarm in alarms:
            site = alarm.get('site', 'Unknown')
            site_counts[site]['total'] += 1
            severity = alarm.get('severity', '').lower()
            if severity in ['critical', 'warning', 'info']:
                site_counts[site][severity] += 1

        top_sites = sorted(
            [{'site_name': site, 'alarm_count': counts['total'],
              'critical_count': counts['critical'],
              'warning_count': counts['warning'],
              'info_count': counts['info']}
             for site, counts in site_counts.items()],
            key=lambda x: x['alarm_count'],
            reverse=True
        )[:10]

        return top_sites

    def _calculate_top_categories(self, alarms: List[Dict]) -> List[Dict]:
        category_counts = defaultdict(int)

        for alarm in alarms:
            category = alarm.get('category', 'Unknown')
            category_counts[category] += 1

        top_categories = sorted(
            [{'category': cat, 'count': count} for cat, count in category_counts.items()],
            key=lambda x: x['count'],
            reverse=True
        )[:5]

        return top_categories

    def _calculate_trend(self, alarms: List[Dict], period_days: int) -> List[Dict]:
        daily_counts = defaultdict(lambda: {'critical': 0, 'warning': 0, 'info': 0, 'total': 0})

        for alarm in alarms:
            try:
                date = alarm['timestamp'][:10]
                severity = alarm.get('severity', '').lower()
                daily_counts[date]['total'] += 1
                if severity in daily_counts[date]:
                    daily_counts[date][severity] += 1
            except Exception:
                continue

        trend = [
            {
                'date': date,
                'count': counts['total'],
                'by_severity': {
                    'critical': counts['critical'],
                    'warning': counts['warning'],
                    'info': counts['info']
                }
            }
            for date, counts in sorted(daily_counts.items())
        ]

        return trend

    def _find_recurring_alarms(self, alarms: List[Dict]) -> List[Dict]:
        site_param_counts = defaultdict(int)

        for alarm in alarms:
            site = alarm.get('site', 'Unknown')
            param = alarm.get('threshold_parameter', alarm.get('category', 'Unknown'))
            key = f"{site}:{param}"
            site_param_counts[key] += 1

        recurring = [
            {
                'site_name': key.split(':')[0],
                'parameter': key.split(':')[1],
                'occurrence_count': count
            }
            for key, count in site_param_counts.items()
            if count > 2
        ]

        return sorted(recurring, key=lambda x: x['occurrence_count'], reverse=True)[:10]


class EnergyConsumptionAnalyzer:
    def __init__(self):
        self.site_repo = SiteRepository()
        self.asset_repo = AssetRepository()
        self.reading_repo = ReadingRepository()

    def generate(self, period_days: int, filters: dict, granularity: str = 'daily',
                 include_cost_analysis: bool = False) -> dict:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=period_days)

        sites = self._get_filtered_sites(filters)
        all_consumption = self._calculate_consumption(sites, to_db_timestamp(start_date), to_db_timestamp(end_date))

        summary = self._aggregate_summary(all_consumption)
        top_sites = self._get_top_sites(all_consumption)
        trend = self._calculate_trend(all_consumption, granularity)

        result = {
            'summary': summary,
            'top_sites': top_sites,
            'trend': trend
        }

        if include_cost_analysis:
            result['cost_analysis'] = self._calculate_costs(summary)

        return result

    def _get_filtered_sites(self, filters: dict) -> List[Dict]:
        all_sites = self.site_repo.get_all()

        if filters.get('region'):
            all_sites = [s for s in all_sites if s['region'] == filters['region']]

        if filters.get('site'):
            all_sites = [s for s in all_sites if s['name'] == filters['site']]

        return all_sites

    def _calculate_consumption(self, sites: List[Dict], start: str, end: str) -> List[Dict]:
        results = []

        for site in sites:
            assets = self.asset_repo.get_by_site_id(site['id'])
            consumption = self._calculate_site_consumption(assets, start, end)

            results.append({
                'site_id': site['id'],
                'site_name': site['name'],
                'region': site['region'],
                'consumption': consumption
            })

        return results

    def _calculate_site_consumption(self, assets: List[Dict], start: str, end: str) -> Dict:
        source_types = {
            'AC_METER': 'grid',
            'GENERATOR': 'generator',
            'DC_METER': 'solar',
        }

        consumption = {'grid': {'kwh': 0, 'peak_kw': 0},
                      'generator': {'kwh': 0, 'peak_kw': 0},
                      'solar': {'kwh': 0, 'peak_kw': 0},
                      'battery': {'kwh': 0, 'peak_kw': 0}}

        for asset in assets:
            source_key = source_types.get(asset['type'])
            if not source_key:
                continue

            readings = self.reading_repo.get_by_asset_id_in_range(asset['id'], start, end)

            for reading in readings:
                try:
                    data = json.loads(reading['data']) if isinstance(reading['data'], str) else reading['data']
                    power_kw = self._extract_power(data)

                    if power_kw > 0:
                        consumption[source_key]['kwh'] += power_kw * (5 / 60)
                        consumption[source_key]['peak_kw'] = max(consumption[source_key]['peak_kw'], power_kw)
                except Exception:
                    continue

        return consumption

    def _extract_power(self, data: dict) -> float:
        for key in ['gen_total_watt', 'total_active_power', 'active_power', 'POWER', 'power', 'kw']:
            if key in data:
                try:
                    val = float(data[key])
                    if key == 'gen_total_watt':
                        return val / 1000
                    return val
                except (ValueError, TypeError):
                    continue
        return 0.0

    def _aggregate_summary(self, all_consumption: List[Dict]) -> Dict:
        totals = {source: {'kwh': 0, 'peak_kw': 0} for source in ['grid', 'generator', 'solar', 'battery']}

        for site_data in all_consumption:
            for source, values in site_data['consumption'].items():
                totals[source]['kwh'] += values['kwh']
                totals[source]['peak_kw'] = max(totals[source]['peak_kw'], values['peak_kw'])

        total_kwh = sum(v['kwh'] for v in totals.values())

        by_source = {}
        for source, values in totals.items():
            percent = (values['kwh'] / total_kwh * 100) if total_kwh else 0
            by_source[source] = {
                'kwh': round(values['kwh'], 2),
                'percent': round(percent, 2),
                'peak_kw': round(values['peak_kw'], 2)
            }

        grid_dep = (totals['grid']['kwh'] / total_kwh * 100) if total_kwh else 0
        renewable = (totals['solar']['kwh'] / total_kwh * 100) if total_kwh else 0

        return {
            'total_kwh': round(total_kwh, 2),
            'by_source': by_source,
            'grid_dependency_percent': round(grid_dep, 2),
            'renewable_percent': round(renewable, 2)
        }

    def _get_top_sites(self, all_consumption: List[Dict]) -> List[Dict]:
        sites_with_totals = []

        for site_data in all_consumption:
            total = sum(v['kwh'] for v in site_data['consumption'].values())
            sites_with_totals.append({
                'site_name': site_data['site_name'],
                'total_kwh': round(total, 2),
                'by_source': {k: round(v['kwh'], 2) for k, v in site_data['consumption'].items()}
            })

        return sorted(sites_with_totals, key=lambda x: x['total_kwh'], reverse=True)[:10]

    def _calculate_trend(self, all_consumption: List[Dict], granularity: str) -> List[Dict]:
        return []

    def _calculate_costs(self, summary: Dict) -> Dict:
        rates = {'grid': 0.15, 'generator': 0.25, 'solar': 0.05, 'battery': 0.10}

        total_cost = 0
        by_source = {}

        for source, values in summary['by_source'].items():
            cost = values['kwh'] * rates.get(source, 0)
            by_source[source] = round(cost, 2)
            total_cost += cost

        return {
            'total_cost': round(total_cost, 2),
            'by_source': by_source
        }


class DieselUtilizationAnalyzer:
    def __init__(self):
        self.site_repo = SiteRepository()
        self.asset_repo = AssetRepository()
        self.reading_repo = ReadingRepository()

    def generate(self, period_days: int, filters: dict, refuel_threshold_liters: float = 100.0,
                 diesel_price_per_liter: float = None) -> dict:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=period_days)

        sites = self._get_filtered_sites(filters)
        diesel_data = self._calculate_diesel_usage(sites, to_db_timestamp(start_date), to_db_timestamp(end_date),
                                                     refuel_threshold_liters)

        summary = self._aggregate_summary(diesel_data, diesel_price_per_liter)
        top_consumers = self._get_top_consumers(diesel_data)
        trend = self._calculate_trend(diesel_data)
        inefficient = self._find_inefficient_sites(diesel_data)
        refuel_events = self._collect_refuel_events(diesel_data)

        return {
            'summary': summary,
            'top_consumers': top_consumers,
            'trend': trend,
            'inefficient_sites': inefficient,
            'refuel_events': refuel_events
        }

    def _get_filtered_sites(self, filters: dict) -> List[Dict]:
        all_sites = self.site_repo.get_all()

        if filters.get('region'):
            all_sites = [s for s in all_sites if s['region'] == filters['region']]

        if filters.get('site'):
            all_sites = [s for s in all_sites if s['name'] == filters['site']]

        return all_sites

    def _calculate_diesel_usage(self, sites: List[Dict], start: str, end: str,
                                refuel_threshold: float) -> List[Dict]:
        results = []

        for site in sites:
            assets = self.asset_repo.get_by_site_id(site['id'])
            fuel_assets = [a for a in assets if a['type'] == 'FUEL_LEVEL']

            if not fuel_assets:
                continue

            for fuel_asset in fuel_assets:
                readings = self.reading_repo.get_by_asset_id_in_range(fuel_asset['id'], start, end)
                usage = self._analyze_fuel_readings(readings, refuel_threshold)

                if usage:
                    results.append({
                        'site_name': site['name'],
                        **usage
                    })
                    break

        return results

    def _analyze_fuel_readings(self, readings: List[Dict], refuel_threshold: float) -> Optional[Dict]:
        if not readings:
            return None

        sorted_readings = sorted(readings, key=lambda r: r['timestamp'])
        total_consumed = 0
        refuel_count = 0
        refuels = []

        prev_level = None
        for reading in sorted_readings:
            try:
                data = json.loads(reading['data']) if isinstance(reading['data'], str) else reading['data']
                current_level = self._extract_fuel_level(data)

                if prev_level is not None:
                    diff = current_level - prev_level

                    if diff > refuel_threshold:
                        refuel_count += 1
                        refuels.append({
                            'timestamp': reading['timestamp'],
                            'liters_added': round(diff, 2)
                        })
                    elif diff < 0:
                        total_consumed += abs(diff)

                prev_level = current_level
            except Exception:
                continue

        runtime_hours = len(readings) * (5 / 60)
        efficiency = total_consumed / runtime_hours if runtime_hours else 0

        return {
            'liters_consumed': round(total_consumed, 2),
            'refuel_count': refuel_count,
            'runtime_hours': round(runtime_hours, 2),
            'efficiency_lph': round(efficiency, 2),
            'refuels': refuels
        }

    def _extract_fuel_level(self, data: dict) -> float:
        for key in ['fuel_level', 'FUEL_LEVEL', 'level', 'LEVEL', 'value']:
            if key in data:
                try:
                    val = data[key]
                    if val is None:
                        continue
                    return float(val)
                except (ValueError, TypeError):
                    continue
        return 0.0

    def _aggregate_summary(self, diesel_data: List[Dict], diesel_price: Optional[float]) -> Dict:
        total_liters = sum(d['liters_consumed'] for d in diesel_data)
        total_refuels = sum(d['refuel_count'] for d in diesel_data)
        total_runtime = sum(d['runtime_hours'] for d in diesel_data)
        avg_efficiency = sum(d['efficiency_lph'] for d in diesel_data) / len(diesel_data) if diesel_data else 0

        summary = {
            'total_liters_consumed': round(total_liters, 2),
            'total_refuels': total_refuels,
            'avg_efficiency_lph': round(avg_efficiency, 2),
            'total_runtime_hours': round(total_runtime, 2)
        }

        if diesel_price:
            summary['total_cost'] = round(total_liters * diesel_price, 2)

        return summary

    def _get_top_consumers(self, diesel_data: List[Dict]) -> List[Dict]:
        return sorted(
            [{
                'site_name': d['site_name'],
                'liters_consumed': d['liters_consumed'],
                'refuel_count': d['refuel_count'],
                'runtime_hours': d['runtime_hours'],
                'efficiency_lph': d['efficiency_lph']
            } for d in diesel_data],
            key=lambda x: x['liters_consumed'],
            reverse=True
        )[:10]

    def _calculate_trend(self, diesel_data: List[Dict]) -> List[Dict]:
        return []

    def _find_inefficient_sites(self, diesel_data: List[Dict]) -> List[Dict]:
        if not diesel_data:
            return []

        avg_efficiency = sum(d['efficiency_lph'] for d in diesel_data) / len(diesel_data)
        threshold = avg_efficiency * 1.2

        inefficient = [
            {
                'site_name': d['site_name'],
                'efficiency_lph': d['efficiency_lph'],
                'expected_lph': round(avg_efficiency, 2)
            }
            for d in diesel_data
            if d['efficiency_lph'] > threshold
        ]

        return sorted(inefficient, key=lambda x: x['efficiency_lph'], reverse=True)

    def _collect_refuel_events(self, diesel_data: List[Dict]) -> List[Dict]:
        all_refuels = []

        for d in diesel_data:
            for refuel in d.get('refuels', []):
                all_refuels.append({
                    'site_name': d['site_name'],
                    'timestamp': refuel['timestamp'],
                    'liters_added': refuel['liters_added']
                })

        return sorted(all_refuels, key=lambda x: x['timestamp'], reverse=True)[:20]


class ReportService:
    def __init__(self):
        self.uptime_calc = SiteUptimeCalculator()
        self.alarm_gen = AlarmSummaryGenerator()
        self.energy_analyzer = EnergyConsumptionAnalyzer()
        self.diesel_analyzer = DieselUtilizationAnalyzer()
        self.repo = ReportRepository()

    def generate_report(self, report_type: str, params: dict) -> str:
        generators = {
            'site_uptime': self.uptime_calc,
            'alarm_summary': self.alarm_gen,
            'energy_consumption': self.energy_analyzer,
            'diesel_utilization': self.diesel_analyzer
        }

        generator = generators.get(report_type)
        if not generator:
            raise ValueError(f"Unknown report type: {report_type}")

        report_data = generator.generate(**params)

        report_id = self.repo.save_report(
            report_type=report_type,
            period_days=params.get('period_days'),
            filters=params.get('filters', {}),
            summary=report_data['summary'],
            data=report_data
        )

        return report_id
