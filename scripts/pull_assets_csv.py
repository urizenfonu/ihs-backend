import csv
import json
import os
import sys
from pathlib import Path
from datetime import datetime

# Add project root to sys.path
project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

from dotenv import load_dotenv
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

def main():
    load_dotenv()
    
    # Capture the pull timestamp in a human-readable format
    pull_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    base_url = os.getenv("IHS_API_BASE_URL")
    token = os.getenv("IHS_API_TOKEN")
    
    if not base_url or not token:
        print("Error: IHS_API_BASE_URL and IHS_API_TOKEN must be set in .env")
        return

    client = IHSApiClient(base_url, token)
    
    asset_limit = 10000
    per_page = 100
    all_assets = []
    page = 1
    
    print(f"Starting to pull assets via sites endpoint (limit ~{asset_limit} assets) at {pull_timestamp}...")
    
    try:
        while len(all_assets) < asset_limit:
            print(f"Fetching sites page {page}...")
            # Assets are embedded in sites
            resp = client.get_sites(page=page, per_page=per_page)
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
                    # Combine asset data with parent site info
                    flattened_asset = flatten_dict(asset)
                    # Prefix asset fields to distinguish? Or just keep as is.
                    # Usually "id" would be asset id.
                    
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

            # Check for end of pagination
            total_sites = resp.get("total", 0)
            # Rough estimation: if we processed all sites on this page and still need more assets, continue.
            # But if we ran out of sites, stop.
            if page * per_page >= total_sites and len(sites_data) < per_page:
                break
                
            page += 1
            
        print(f"Fetched {len(all_assets)} assets. Writing to CSV...")
        
        if not all_assets:
            print("No assets found.")
            return

        # Get all unique keys for header
        fieldnames = set()
        for item in all_assets:
            fieldnames.update(item.keys())
        
        # Priority columns
        priority = ['pull_timestamp', 'id', 'name', 'type', 'site_id', 'site_name', 'site_zone_name']
        sorted_fieldnames = [f for f in priority if f in fieldnames]
        sorted_fieldnames += sorted([f for f in fieldnames if f not in priority])
        
        output_file = 'assets_export.csv'
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=sorted_fieldnames)
            writer.writeheader()
            writer.writerows(all_assets)
            
        print(f"Successfully exported {len(all_assets)} assets to {output_file}")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
