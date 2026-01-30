from fastapi import APIRouter
from db.repositories.site_repository import SiteRepository
from db.repositories.asset_repository import AssetRepository
from db.repositories.reading_repository import ReadingRepository
import json
from typing import Dict, List

router = APIRouter()

@router.get("/regional-data")
def get_regional_data():
    """Get regional overview data grouped by zones"""
    site_repo = SiteRepository()
    sites = site_repo.get_all()

    zone_map: Dict[str, List[Dict]] = {}
    for site in sites:
        zone = site.get('zone') or site.get('region') or 'Unknown'
        zone_map.setdefault(zone, []).append(site)

    regional_data: Dict[str, Dict] = {}
    for zone_name, zone_sites in zone_map.items():
        region_id = zone_name.lower().replace(' ', '-')

        state_map: Dict[str, List[Dict]] = {}
        for site in zone_sites:
            state = site.get('state') or 'Unknown'
            state_map.setdefault(state, []).append(site)

        zones_payload = []
        for state_name, state_sites in state_map.items():
            cluster_codes = {
                s.get('cluster_code')
                for s in state_sites
                if s.get('cluster_code')
            }
            zones_payload.append({
                'id': f"{region_id}:{state_name.lower().replace(' ', '-')}",
                'name': state_name,
                'state': state_name,
                'clusters': len(cluster_codes),
                'sites': len(state_sites),
                'uptime': 0,
                'energyMix': {'grid': 0, 'generator': 0, 'solar': 0, 'battery': 0},
                'alerts': 0,
                'dieselConsumption': 0,
                'gridSupply': 0,
                'genHours': 0
            })

        regional_data[region_id] = {
            'id': region_id,
            'name': zone_name,
            'zones': sorted(zones_payload, key=lambda z: z['name']),
            'totalSites': len(zone_sites),
            'avgUptime': 0,
            'totalEnergy': 0,
            'totalAlerts': 0,
            'totalDiesel': 0,
            'avgGridSupply': 0,
            'totalGenHours': 0
        }

    return regional_data

@router.get("/regional-data/{region}/metrics")
def get_regional_metrics(region: str):
    """Get detailed metrics for a specific region"""
    site_repo = SiteRepository()
    asset_repo = AssetRepository()
    reading_repo = ReadingRepository()

    region_map = {
        'abuja': 'Abuja',
        'asaba': 'Asaba',
        'enugu': 'Enugu',
        'ibadan': 'Ibadan',
        'kano': 'Kano',
        'lagos': 'Lagos',
        'phc': 'PHC'
    }

    region_name = region_map.get(region.lower())
    if not region_name:
        return {'error': 'Invalid region'}, 400

    # Get all sites
    sites = site_repo.get_all()

    # Filter sites by region name in site name
    region_sites = [
        site for site in sites
        if region_name.upper() in site['name'].upper()
    ]

    if not region_sites:
        return {
            'regional': {
                'avgUptime': 0,
                'energyMix': {'grid': 0, 'generator': 0, 'solar': 0, 'battery': 0},
                'totalAlerts': 0,
                'totalDiesel': 0,
                'avgGridSupply': 0,
                'totalGenHours': 0
            },
            'zones': []
        }

    # Get assets for region sites
    asset_ids = []
    for site in region_sites:
        site['assets'] = asset_repo.get_by_site_id(site['id'])
        asset_ids.extend([a['id'] for a in site['assets']])

    # Get latest readings
    readings = reading_repo.get_latest_by_asset_ids(asset_ids) if asset_ids else []

    # Calculate metrics
    energy_mix = {'grid': 0, 'generator': 0, 'solar': 0, 'battery': 0}
    total_diesel = 0

    for reading in readings:
        data = json.loads(reading['data']) if reading['data'] else {}
        reading_type = reading['reading_type']

        if reading_type == 'AC_METER':
            power = float(data.get('Total_Active_Power (kW)', 0))
            energy_mix['grid'] += power
        elif reading_type == 'GENERATOR':
            power = float(data.get('Gen_Total_Power (kW)', 0))
            energy_mix['generator'] += power
        elif reading_type == 'DC_METER':
            battery_power = float(data.get('Power1 (kW)', 0))
            solar_power = float(data.get('Power2 (kW)', 0))
            energy_mix['battery'] += battery_power
            energy_mix['solar'] += solar_power
        elif reading_type == 'FUEL_LEVEL':
            consumption = float(data.get('Consumption (L)', 0))
            total_diesel += consumption

    # Group by zones
    zone_groups = {}
    for site in region_sites:
        zone = site.get('zone') or site.get('region') or 'Unknown'
        if zone not in zone_groups:
            zone_groups[zone] = []
        zone_groups[zone].append(site)

    zones = []
    for zone_name, zone_sites in zone_groups.items():
        zones.append({
            'id': zone_name.lower().replace(' ', '-'),
            'name': zone_name,
            'state': zone_name,
            'sites': len(zone_sites),
            'clusters': 0,
            'uptime': 0,
            'energyMix': {'grid': 0, 'generator': 0, 'solar': 0, 'battery': 0},
            'alerts': 0,
            'dieselConsumption': 0,
            'gridSupply': 0,
            'genHours': 0
        })

    return {
        'regional': {
            'avgUptime': 0,
            'energyMix': energy_mix,
            'totalAlerts': 0,
            'totalDiesel': total_diesel,
            'avgGridSupply': 0,
            'totalGenHours': 0
        },
        'zones': zones
    }
