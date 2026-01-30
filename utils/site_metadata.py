import json
import os
import re
from typing import Dict, Optional

_SITE_MAP: Optional[Dict[str, Dict]] = None


def _normalize_site_id(value: str) -> str:
    if not value:
        return ""
    trimmed = value.strip()
    if not trimmed:
        return ""
    trimmed = re.sub(r"^IHS[_-]?", "", trimmed, flags=re.IGNORECASE)
    trimmed = re.sub(r"[^A-Za-z0-9_]", "", trimmed)
    return trimmed.lower()


def _load_site_map() -> Dict[str, Dict]:
    global _SITE_MAP
    if _SITE_MAP is not None:
        return _SITE_MAP

    data_path = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "data", "site-matrix.json")
    )
    try:
        with open(data_path, "r", encoding="utf-8") as handle:
            data = json.load(handle)
    except FileNotFoundError:
        _SITE_MAP = {}
        return _SITE_MAP
    except json.JSONDecodeError:
        _SITE_MAP = {}
        return _SITE_MAP

    site_map: Dict[str, Dict] = {}
    for entry in data:
        site_id = entry.get("site_id")
        normalized = _normalize_site_id(site_id)
        if normalized:
            site_map[normalized] = entry

    _SITE_MAP = site_map
    return _SITE_MAP


def get_site_metadata(site_name: str) -> Optional[Dict]:
    if not site_name:
        return None

    site_map = _load_site_map()
    if not site_map:
        return None

    normalized = _normalize_site_id(site_name)
    if normalized in site_map:
        return site_map[normalized]

    prefixed = _normalize_site_id(f"IHS_{site_name}")
    return site_map.get(prefixed)
