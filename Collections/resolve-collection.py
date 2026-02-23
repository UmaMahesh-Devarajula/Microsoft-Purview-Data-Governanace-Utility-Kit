from purview-client import get_purview_admin_client

def resolve_collection_name(user_collection_name: str) -> str:
    admin_client = get_purview_admin_client()
    try:
        collection_list = admin_client.collections.list_collections()
        for collection in collection_list:
            if collection.get("friendlyName", "").lower() == user_collection_name.lower():
                return collection.get("name", user_collection_name)
    except Exception:
        # If admin call fails, fall back to user-provided name
        pass
    return user_collection_name