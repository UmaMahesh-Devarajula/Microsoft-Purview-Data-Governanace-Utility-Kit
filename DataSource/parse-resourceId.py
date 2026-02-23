from typing import Dict, List

def parse_resource_id(resource_id: str) -> Dict[str, str]:
    """
    Parse an Azure resourceId string into subscriptionId, resourceGroup, and resourceName.
    Expected format:
    /subscriptions/<subId>/resourceGroups/<rg>/providers/<provider>/<type>/<resourceName>
    """
    parts = resource_id.strip("/").split("/")
    if len(parts) < 6:
        raise ValueError(f"Invalid resourceId format: {resource_id}")
    try:
        subscription_id = parts[1]
        resource_group = parts[3]
        resource_name = parts[-1]
    except IndexError:
        raise ValueError(f"Invalid resourceId format: {resource_id}")
    return {"subscriptionId": subscription_id, "resourceGroup": resource_group, "resourceName": resource_name}