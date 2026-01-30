def normalize_tenant_name(name: str) -> str:
    """Normalize tenant names to standard format

    Args:
        name: Raw tenant name from asset channels

    Returns:
        Normalized tenant name
    """
    if not name:
        return 'Unknown'

    name_upper = name.upper()

    if 'MTN' in name_upper:
        return 'MTN Nigeria'
    if 'MAINONE' in name_upper or 'MAIN ONE' in name_upper:
        return 'MainOne'
    if '9MOBILE' in name_upper or 'ETISALAT' in name_upper:
        return '9Mobile'
    if 'AIRTEL' in name_upper:
        return 'Airtel'
    if 'GLO' in name_upper:
        return 'Glo Mobile'

    return name  # Return as-is if no match
