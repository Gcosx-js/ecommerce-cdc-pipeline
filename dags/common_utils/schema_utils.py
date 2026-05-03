import requests
from common_utils.config import SCHEMA_REGISTRY_URL


def get_schema_str(schema_id: int) -> str:
    url = f"{SCHEMA_REGISTRY_URL}/schemas/ids/{schema_id}"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()["schema"]