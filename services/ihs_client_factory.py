import os
from typing import Optional

from services.ihs_api_client import IHSApiClient

_CLIENT: Optional[IHSApiClient] = None


def get_ihs_api_client() -> IHSApiClient:
    global _CLIENT
    if _CLIENT is not None:
        return _CLIENT

    base_url = os.getenv("IHS_API_BASE_URL")
    token = os.getenv("IHS_API_TOKEN")
    if not base_url or not token:
        raise ValueError("IHS_API_BASE_URL and IHS_API_TOKEN must be set in .env")

    _CLIENT = IHSApiClient(base_url, token)
    return _CLIENT

