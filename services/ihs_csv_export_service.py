import csv
import json
from datetime import datetime
from io import StringIO
from typing import Dict, List
from services.ihs_api_client import IHSApiClient


def flatten_dict(d, parent_key='', sep='_'):
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        elif isinstance(v, list):
            items.append((new_key, json.dumps(v)))
        else:
            items.append((new_key, v))
    return dict(items)


class IHSCsvExportService:
    def __init__(self, ihs_client: IHSApiClient):
        self.client = ihs_client

    def generate_assets_csv(self) -> str:
        pull_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        asset_limit = 10000
        per_page = 100
        all_assets = []
        page = 1

        while len(all_assets) < asset_limit:
            resp = self.client.get_sites(page=page, per_page=per_page)
            sites_data = resp.get("data", [])

            if not sites_data:
                break

            for site in sites_data:
                site_info = {
                    'site_id': site.get('id'),
                    'site_name': site.get('name'),
                    'site_zone_id': site.get('zone', {}).get('id'),
                    'site_zone_name': site.get('zone', {}).get('name')
                }

                assets = site.get('assets', [])
                for asset in assets:
                    flattened_asset = flatten_dict(asset)
                    combined_record = {
                        **site_info,
                        **flattened_asset,
                        'pull_timestamp': pull_timestamp
                    }
                    all_assets.append(combined_record)

                    if len(all_assets) >= asset_limit:
                        break

                if len(all_assets) >= asset_limit:
                    break

            total_sites = resp.get("total", 0)
            if page * per_page >= total_sites and len(sites_data) < per_page:
                break

            page += 1

        if not all_assets:
            return ""

        fieldnames = set()
        for item in all_assets:
            fieldnames.update(item.keys())

        priority = ['pull_timestamp', 'id', 'name', 'type', 'site_id', 'site_name', 'site_zone_name']
        sorted_fieldnames = [f for f in priority if f in fieldnames]
        sorted_fieldnames += sorted([f for f in fieldnames if f not in priority])

        output = StringIO()
        writer = csv.DictWriter(output, fieldnames=sorted_fieldnames)
        writer.writeheader()
        writer.writerows(all_assets)

        return output.getvalue()

    def generate_sites_csv(self) -> str:
        pull_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        limit = 10000
        per_page = 100
        all_sites = []
        page = 1

        while len(all_sites) < limit:
            resp = self.client.get_sites(page=page, per_page=per_page)
            data = resp.get("data", [])

            if not data:
                break

            all_sites.extend(data)

            total = resp.get("total", 0)
            if len(all_sites) >= total or len(data) < per_page:
                break

            page += 1

        all_sites = all_sites[:limit]

        if not all_sites:
            return ""

        flattened_data = []
        for site in all_sites:
            flat_site = flatten_dict(site)
            flat_site['pull_timestamp'] = pull_timestamp
            flattened_data.append(flat_site)

        fieldnames = set()
        for item in flattened_data:
            fieldnames.update(item.keys())

        priority = ['pull_timestamp', 'id', 'name', 'zone_id', 'zone_name']
        sorted_fieldnames = [f for f in priority if f in fieldnames]
        sorted_fieldnames += sorted([f for f in fieldnames if f not in priority])

        output = StringIO()
        writer = csv.DictWriter(output, fieldnames=sorted_fieldnames)
        writer.writeheader()
        writer.writerows(flattened_data)

        return output.getvalue()
