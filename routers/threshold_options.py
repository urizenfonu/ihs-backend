from fastapi import APIRouter
from scripts.generate_threshold_metadata import extract_threshold_metadata
import json

router = APIRouter()

def _append_unique(items, value):
    if value is None:
        return
    if isinstance(value, str) and not value.strip():
        return
    if value not in items:
        items.append(value)

def _enrich_threshold_metadata_from_db(metadata: dict) -> dict:
    """
    The frontend uses threshold-options to populate unit dropdowns.
    If DB-backed thresholds/composite rules contain parameters/units not present
    in the static metadata list, editing an existing threshold can show a blank unit.

    This function merges discovered categories/parameters/units from the DB into
    the returned metadata to keep the UI consistent for both create and edit flows.
    """
    try:
        from db.client import get_database
    except Exception:
        return metadata

    db = get_database()

    categories = list(metadata.get("categories") or [])
    parameters_by_category = dict(metadata.get("parameters_by_category") or {})
    units_by_parameter = dict(metadata.get("units_by_parameter") or {})

    def ensure_category_parameter(category: str, parameter: str):
        if category:
            _append_unique(categories, category)
        if category and parameter:
            parameters_by_category.setdefault(category, [])
            _append_unique(parameters_by_category[category], parameter)

    def ensure_parameter_unit(parameter: str, unit: str):
        if not parameter:
            return
        units_by_parameter.setdefault(parameter, [])
        _append_unique(units_by_parameter[parameter], unit)

    # From composite_rules (source of truth for synced thresholds)
    try:
        cursor = db.execute("SELECT category, conditions FROM composite_rules")
        for row in cursor.fetchall():
            category = row["category"]
            try:
                conditions = json.loads(row["conditions"] or "[]")
            except Exception:
                conditions = []
            for cond in conditions if isinstance(conditions, list) else []:
                parameter = (cond or {}).get("parameter")
                unit = (cond or {}).get("unit")
                ensure_category_parameter(category, parameter)
                ensure_parameter_unit(parameter, unit)
    except Exception:
        pass

    # From thresholds (covers custom thresholds and any legacy data)
    try:
        cursor = db.execute("SELECT category, parameter, unit, conditions FROM thresholds")
        for row in cursor.fetchall():
            category = row["category"]
            parameter = row["parameter"]
            ensure_category_parameter(category, parameter)
            ensure_parameter_unit(parameter, row["unit"])

            try:
                conditions = json.loads(row["conditions"] or "[]")
            except Exception:
                conditions = []
            for cond in conditions if isinstance(conditions, list) else []:
                ensure_parameter_unit(parameter, (cond or {}).get("unit"))
    except Exception:
        pass

    # Stable output: sort where safe, keep unit order per parameter.
    metadata["categories"] = sorted({c for c in categories if isinstance(c, str) and c.strip()})
    metadata["parameters_by_category"] = {
        category: sorted({p for p in (params or []) if isinstance(p, str) and p.strip()})
        for category, params in parameters_by_category.items()
        if isinstance(category, str) and category.strip()
    }
    metadata["units_by_parameter"] = {
        parameter: units
        for parameter, units in units_by_parameter.items()
        if isinstance(parameter, str) and parameter.strip()
    }

    return metadata

@router.get("/threshold-options")
def get_threshold_options():
    """
    Get available threshold configuration options from Excel data

    Returns threshold metadata including:
    - categories: Available threshold categories
    - parameters_by_category: Parameters organized by category
    - units_by_parameter: Units organized by parameter
    - conditions: Available comparison operators
    - severities: Available severity levels

    This is used by the frontend threshold creation UI to populate dropdowns.
    """
    try:
        metadata = extract_threshold_metadata()
        metadata = _enrich_threshold_metadata_from_db(metadata)
        return metadata
    except Exception as e:
        return {"error": str(e)}, 500
