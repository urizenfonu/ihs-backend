from fastapi import APIRouter
from db.repositories.site_repository import SiteRepository
from db.repositories.asset_repository import AssetRepository
from db.repositories.reading_repository import ReadingRepository
from utils.tenant_normalizer import normalize_tenant_name
import json
from typing import Dict, List
import re

router = APIRouter()

def _parse_float(value) -> float:
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        cleaned = value.replace(',', '').strip()
        if cleaned.lower() in {'', 'n/a', 'na', 'none', 'null'}:
            return 0.0
        match = re.search(r'-?\d+(\.\d+)?', cleaned)
        return float(match.group(0)) if match else 0.0
    return 0.0

def _normalize_power_kw(value) -> float:
    power = _parse_float(value)
    if power == 0:
        return 0.0
    if abs(power) >= 1000:
        power = power / 1000.0
    return max(0.0, power)

def _clamp_power_kw(power_kw: float, max_kw: float = 2000.0) -> float:
    """Return 0 for out-of-range power readings (device sentinel/garbage)."""
    try:
        value = float(power_kw)
    except (TypeError, ValueError):
        return 0.0
    if value < 0:
        return 0.0
    if value > max_kw:
        return 0.0
    return value

def _first_nonzero(values) -> float:
    for value in values:
        power = _normalize_power_kw(value)
        if power != 0:
            return power
    return 0.0

def _sum_power_fields(data: Dict, keys: List[str]) -> float:
    if not isinstance(data, dict):
        return 0.0
    return sum(_normalize_power_kw(data.get(key)) for key in keys if key in data)

def _normalize_tenant_id(value: str) -> str:
    if not value:
        return ''
    return re.sub(r'[_\s]+', '-', value.strip().lower())

def _format_type_label(value: str) -> str:
    if not value:
        return 'Unknown'
    mapping = {
        'DC_METER': 'DC Meter',
        'AC_METER': 'Grid',
        'GENERATOR': 'Generator',
        'FUEL_LEVEL': 'Fuel',
        'RECTIFIER': 'Rectifier',
        'UNKNOWN': 'Unknown'
    }
    if value in mapping:
        return mapping[value]
    return value.replace('_', ' ').title()

def _infer_dc_meter_type(data: Dict) -> str:
    if not isinstance(data, dict):
        return 'Battery'
    keys = {str(key).lower() for key in data.keys()}
    has_solar = any('solar' in key for key in keys) or 'power2' in keys or 'p2' in keys
    has_battery = any('batt' in key for key in keys) or 'power1' in keys or 'p1' in keys
    if has_solar and not has_battery:
        return 'Solar'
    return 'Battery'

def _derive_source_type(asset: Dict, reading_data: Dict) -> str:
    asset_type = asset.get('type')
    name = (asset.get('name') or '').lower()

    if asset_type == 'GENERATOR' or 'generator' in name or 'gen ' in name:
        return 'Generator'
    if asset_type == 'AC_METER' or 'ac meter' in name or 'grid' in name:
        return 'Grid'
    if asset_type == 'RECTIFIER' or 'rectifier' in name:
        return 'Rectifier'
    if asset_type == 'FUEL_LEVEL' or 'fuel' in name or 'diesel' in name:
        return 'Fuel'
    if asset_type == 'DC_METER' or 'dc meter' in name:
        return _infer_dc_meter_type(reading_data)

    return _format_type_label(asset_type)

def identify_tenant(name: str) -> str:
    """Extract tenant name from asset/site name"""
    name_upper = name.upper()

    # Common patterns: IHS_TENANT_SITECODE, TENANT_SITECODE, etc.
    patterns = [
        r'IHS_([A-Z]+)_\d+',  # IHS_MTN_0001
        r'([A-Z]+)_\d+[A-Z]?',  # MTN_0001B
        r'_([A-Z]{3,})_',  # _AIRTEL_
    ]

    for pattern in patterns:
        match = re.search(pattern, name_upper)
        if match:
            tenant = match.group(1)
            if tenant not in ['IHS', 'GEN', 'AC', 'DC', 'BAT']:
                return tenant

    return 'UNKNOWN'

def _extract_tenants_from_site(site: Dict) -> List[str]:
    tenants = set()
    for asset in site.get('assets', []):
        tenant_channels = asset.get('tenant_channels')
        if not tenant_channels:
            continue
        try:
            tenant_names = json.loads(tenant_channels)
        except (json.JSONDecodeError, TypeError):
            continue
        for tenant_name in tenant_names:
            normalized = normalize_tenant_name(tenant_name)
            if normalized:
                tenants.add(normalized)

    return list(tenants)

def group_sites_by_tenant(sites: List[Dict]) -> Dict[str, List[Dict]]:
    """Group sites by tenant name extracted from tenant channels."""
    tenant_sites: Dict[str, List[Dict]] = {}

    for site in sites:
        tenants = _extract_tenants_from_site(site)
        if not tenants:
            continue
        for tenant in tenants:
            tenant_sites.setdefault(tenant, []).append(site)

    return tenant_sites

@router.get("/tenants")
def get_tenants():
    """Get all tenants with their sites and basic metrics using tenant_channels"""
    asset_repo = AssetRepository()
    reading_repo = ReadingRepository()

    # Query all assets
    assets = asset_repo.get_all()

    # Build tenant data dictionary
    tenant_data = {}

    for asset in assets:
        site_id = asset['site_id']

        # Parse config (stored as JSON string in DB) if present
        config = None
        config_raw = asset.get('config')
        if isinstance(config_raw, str) and config_raw:
            try:
                config = json.loads(config_raw)
            except (json.JSONDecodeError, TypeError):
                config = None

        # Tenant association is primarily from tenant_channels, but some assets
        # only indicate tenants via config.channels[].
        tenant_names = []
        tenant_channels = asset.get('tenant_channels')
        if tenant_channels:
            try:
                tenant_names = json.loads(tenant_channels)
            except (json.JSONDecodeError, TypeError):
                tenant_names = []

        if not tenant_names and isinstance(config, dict):
            channels = config.get('channels', [])
            if isinstance(channels, list):
                tenant_names = [ch.get('name') for ch in channels if isinstance(ch, dict) and str(ch.get('type') or '').lower() == 'tenant']

        if not tenant_names:
            continue

        seen_norm = set()
        deduped = []
        for tn in tenant_names:
            norm = normalize_tenant_name(tn)
            if norm and norm not in seen_norm:
                seen_norm.add(norm)
                deduped.append(tn)
        tenant_names = deduped

        asset_type = asset.get('type')
        name_lower = (asset.get('name') or '').lower()
        is_dc_meter = asset_type == 'DC_METER' or 'dc meter' in name_lower

        # Process each tenant for this asset
        for tenant_name in tenant_names:
            # Normalize tenant name
            normalized = normalize_tenant_name(tenant_name)

            if normalized not in tenant_data:
                tenant_data[normalized] = {
                    'name': normalized,
                    'id': _normalize_tenant_id(normalized),
                    'sites': set(),
                    'assets': 0,
                    'totalSources': 0,
                    'asset_ids': [],
                    'asset_channel_indices': {},
                    'totalUsage': 0,
                    'energySources': {'grid': 0, 'generator': 0, 'solar': 0, 'battery': 0},
                    'siteList': []
                }

            # Add site (set prevents duplicates)
            tenant_data[normalized]['sites'].add(site_id)
            tenant_data[normalized]['assets'] += 1
            tenant_data[normalized]['asset_ids'].append(asset['id'])

            # Total Sources is client-facing and should reflect tenant-owned channels for multi-tenant DC meters.
            channel_count = 1
            if is_dc_meter and isinstance(config, dict):
                channels = config.get('channels', [])
                if isinstance(channels, list):
                    indices = []
                    for ch in channels:
                        if not isinstance(ch, dict):
                            continue
                        if str(ch.get('type') or '').lower() != 'tenant':
                            continue
                        ch_name = ch.get('name')
                        ch_norm = normalize_tenant_name(ch_name) if ch_name else None
                        if ch_norm != normalized:
                            continue
                        idx = ch.get('index')
                        if isinstance(idx, int):
                            indices.append(idx)
                    if indices:
                        indices = sorted(set(indices))
                        tenant_data[normalized]['asset_channel_indices'][asset['id']] = indices
                        channel_count = max(1, len(indices))
            tenant_data[normalized]['totalSources'] += channel_count

    # Calculate metrics from readings
    for tenant_info in tenant_data.values():
        asset_ids = tenant_info['asset_ids']
        if asset_ids:
            readings = reading_repo.get_latest_by_asset_ids(asset_ids)

            for reading in readings:
                data = json.loads(reading['data']) if reading['data'] else {}
                reading_type = reading['reading_type']

                if reading_type == 'AC_METER':
                    power = _first_nonzero([
                        data.get('Total_Active_Power (kW)'),
                        data.get('Total Active Power (kW)'),
                        data.get('total_active_power'),
                        data.get('total_power_kw'),
                        data.get('total_power'),
                    ])
                    if power == 0:
                        power = _sum_power_fields(
                            data,
                            ['active_power_1', 'active_power_2', 'active_power_3']
                        )
                    if power == 0:
                        power = _sum_power_fields(
                            data,
                            ['power_l1', 'power_l2', 'power_l3', 'Power1', 'Power2', 'Power3']
                        )
                    tenant_info['energySources']['grid'] += _clamp_power_kw(power)
                elif reading_type == 'GENERATOR':
                    power = _first_nonzero([
                        data.get('Gen_Total_Power (kW)'),
                        data.get('Gen_Total_Power'),
                        data.get('Gen Total Power (kW)'),
                        data.get('power_kw'),
                        data.get('total_power_kw'),
                    ])
                    if power == 0:
                        power = _sum_power_fields(data, ['P1', 'P2', 'P3', 'p1', 'p2', 'p3'])
                    tenant_info['energySources']['generator'] += _clamp_power_kw(power)
                elif reading_type == 'DC_METER':
                    # DC Meter readings can be multi-tenant. When channel indices are available for this tenant,
                    # only sum the tenant-owned channels (Power{index}). This avoids double counting shared DC meters
                    # across multiple tenants.
                    indices = tenant_info.get('asset_channel_indices', {}).get(reading.get('asset_id'), [])
                    if indices:
                        tenant_power = 0.0
                        for idx in indices:
                            power = _normalize_power_kw(data.get(f'Power{idx}'))
                            if power == 0:
                                power = _normalize_power_kw(data.get(f'power{idx}'))
                            tenant_power += power
                        tenant_info['energySources']['battery'] += tenant_power
                    else:
                        battery_power = _first_nonzero([
                            data.get('Power1 (kW)'),
                            data.get('Power1'),
                            data.get('battery_power'),
                            data.get('Battery Power (kW)'),
                        ])
                        solar_power = _first_nonzero([
                            data.get('Power2 (kW)'),
                            data.get('Power2'),
                            data.get('solar_power'),
                            data.get('Solar Power (kW)'),
                        ])

                        if battery_power == 0 and solar_power == 0:
                            total_power = _sum_power_fields(
                                data,
                                ['Power1', 'Power2', 'Power3', 'Power4', 'Power5', 'Power6']
                            )
                            if total_power == 0:
                                total_power = _sum_power_fields(
                                    data,
                                    ['power1', 'power2', 'power3', 'power4', 'power5', 'power6']
                                )
                            battery_power = total_power

                        tenant_info['energySources']['battery'] += battery_power
                        tenant_info['energySources']['solar'] += solar_power

            tenant_info['totalUsage'] = sum(tenant_info['energySources'].values())

    # Convert sets to counts and clean up
    result = []
    site_repo = SiteRepository()
    sites_by_id = {site['id']: site for site in site_repo.get_all()}

    for tenant_info in tenant_data.values():
        # Convert site set to count and get sample names
        site_ids = list(tenant_info['sites'])
        tenant_info['sites'] = len(site_ids)
        tenant_info['siteList'] = [
            sites_by_id[sid]['name'] for sid in site_ids[:5] if sid in sites_by_id
        ]
        region_names = set()
        for sid in site_ids:
            site = sites_by_id.get(sid)
            if not site:
                continue
            region = site.get('region') or site.get('zone') or 'Unknown'
            if region:
                region_names.add(region)

        if len(region_names) == 1:
            region_value = next(iter(region_names))
        elif len(region_names) > 1:
            region_value = 'Multiple'
        else:
            region_value = 'Unknown'

        tenant_info['region'] = region_value
        tenant_info['state'] = region_value

        # Remove internal fields
        del tenant_info['asset_ids']
        if 'asset_channel_indices' in tenant_info:
            del tenant_info['asset_channel_indices']

        result.append(tenant_info)

    return sorted(result, key=lambda x: x['name'])

@router.get("/tenants/mapping")
def get_tenant_mapping(siteId: str = None, tenantId: str = None):
    """Get tenant to site mapping"""
    site_repo = SiteRepository()
    asset_repo = AssetRepository()

    sites = site_repo.get_all()

    for site in sites:
        site['assets'] = asset_repo.get_by_site_id(site['id'])

    tenant_groups = group_sites_by_tenant(sites)

    mapping = []
    for tenant_name, tenant_sites in tenant_groups.items():
        tenant_id = _normalize_tenant_id(tenant_name)

        for site in tenant_sites:
            mapping.append({
                'tenantName': tenant_name,
                'tenantId': tenant_id,
                'siteId': site['id'],
                'siteName': site['name'],
                'zone': site.get('zone', 'Unknown')
            })

    # Apply filters
    if siteId:
        mapping = [m for m in mapping if str(m['siteId']) == siteId]
    if tenantId:
        target_id = _normalize_tenant_id(tenantId)
        mapping = [m for m in mapping if _normalize_tenant_id(m['tenantId']) == target_id]

    return mapping

@router.get("/tenants/sources")
def get_tenant_sources(tenantId: str = None):
    """Get asset sources for a tenant based on tenant channels."""
    if not tenantId:
        return []

    tenant_id = _normalize_tenant_id(tenantId)
    site_repo = SiteRepository()
    asset_repo = AssetRepository()
    reading_repo = ReadingRepository()

    sites_by_id = {site['id']: site for site in site_repo.get_all()}
    sources = []
    seen_assets = set()
    tenant_assets = []

    for asset in asset_repo.get_all():
        tenant_names = []
        tenant_channels = asset.get('tenant_channels')
        if tenant_channels:
            try:
                tenant_names = json.loads(tenant_channels)
            except (json.JSONDecodeError, TypeError):
                tenant_names = []

        if not tenant_names:
            config = None
            config_raw = asset.get('config')
            if isinstance(config_raw, str) and config_raw:
                try:
                    config = json.loads(config_raw)
                except (json.JSONDecodeError, TypeError):
                    config = None
            if isinstance(config, dict):
                channels = config.get('channels', [])
                if isinstance(channels, list):
                    tenant_names = [ch.get('name') for ch in channels if isinstance(ch, dict) and str(ch.get('type') or '').lower() == 'tenant']

        if not tenant_names:
            continue

        for tenant_name in tenant_names:
            normalized = normalize_tenant_name(tenant_name)
            if not normalized:
                continue
            if _normalize_tenant_id(normalized) != tenant_id:
                continue
            if asset['id'] in seen_assets:
                break
            tenant_assets.append((asset, normalized))
            seen_assets.add(asset['id'])
            break

    asset_ids = [asset['id'] for asset, _ in tenant_assets]
    readings = reading_repo.get_latest_by_asset_ids(asset_ids) if asset_ids else []
    reading_by_asset_id = {}
    for reading in readings:
        reading_by_asset_id[reading['asset_id']] = reading

    for asset, tenant_name in tenant_assets:
        site = sites_by_id.get(asset['site_id'])
        reading = reading_by_asset_id.get(asset['id'])
        reading_data = json.loads(reading['data']) if reading and reading.get('data') else {}
        derived_type = _derive_source_type(asset, reading_data)

        asset_type = asset.get('type')
        name_lower = (asset.get('name') or '').lower()
        is_dc_meter = asset_type == 'DC_METER' or 'dc meter' in name_lower

        indices = []
        config = None
        config_raw = asset.get('config')
        if isinstance(config_raw, str) and config_raw:
            try:
                config = json.loads(config_raw)
            except (json.JSONDecodeError, TypeError):
                config = None
        if is_dc_meter and isinstance(config, dict):
            channels = config.get('channels', [])
            if isinstance(channels, list):
                for ch in channels:
                    if not isinstance(ch, dict):
                        continue
                    if str(ch.get('type') or '').lower() != 'tenant':
                        continue
                    ch_name = ch.get('name')
                    ch_norm = normalize_tenant_name(ch_name) if ch_name else None
                    if ch_norm != tenant_name:
                        continue
                    idx = ch.get('index')
                    if isinstance(idx, int):
                        indices.append(idx)
        indices = sorted(set(indices))

        if is_dc_meter and indices:
            # Represent tenant-owned DC meter channels as separate sources
            for idx in indices:
                sources.append({
                    'assetId': asset['id'],
                    'assetName': f"{asset.get('name')} ({tenant_name})",
                    'assetType': derived_type,
                    'channelIndex': idx,
                    'siteId': asset.get('site_id'),
                    'siteName': site.get('name') if site else None,
                    'zone': site.get('zone') or site.get('region') or 'Unknown' if site else 'Unknown',
                    'tenantName': tenant_name
                })
        else:
            sources.append({
                'assetId': asset['id'],
                'assetName': asset.get('name'),
                'assetType': derived_type,
                'siteId': asset.get('site_id'),
                'siteName': site.get('name') if site else None,
                'zone': site.get('zone') or site.get('region') or 'Unknown' if site else 'Unknown',
                'tenantName': tenant_name
            })

    return sources
