import csv
import json
import os
import sys
from pathlib import Path
from datetime import datetime

# Add project root to sys.path to import existing services
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
    
    limit = 10000
    per_page = 100
    all_sites = []
    page = 1
    
    print(f"Starting to pull up to {limit} sites from {base_url} at {pull_timestamp}...")
    
    try:
        while len(all_sites) < limit:
            print(f"Fetching page {page}...")
            resp = client.get_sites(page=page, per_page=per_page)
            data = resp.get("data", [])
            
            if not data:
                break
                
            all_sites.extend(data)
            
            # Check if we've reached the end or the limit
            total = resp.get("total", 0)
            if len(all_sites) >= total or len(data) < per_page:
                break
                
            page += 1
            
        # Truncate to limit if we over-fetched
        all_sites = all_sites[:limit]
        
        print(f"Fetched {len(all_sites)} sites. Flattening data and writing to CSV...")
        
        if not all_sites:
            print("No data found.")
            return

        flattened_data = []
        for site in all_sites:
            flat_site = flatten_dict(site)
            flat_site['pull_timestamp'] = pull_timestamp
            flattened_data.append(flat_site)
        
        # Get all unique keys for the CSV header
        fieldnames = set()
        for item in flattened_data:
            fieldnames.update(item.keys())
        
        # Prioritize some fields at the beginning
        priority = ['pull_timestamp', 'id', 'name', 'zone_id', 'zone_name']
        sorted_fieldnames = [f for f in priority if f in fieldnames]
        sorted_fieldnames += sorted([f for f in fieldnames if f not in priority])
        
        output_file = 'sites_export.csv'
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=sorted_fieldnames)
            writer.writeheader()
            writer.writerows(flattened_data)
            
        print(f"Successfully exported {len(all_sites)} sites to {output_file}")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
