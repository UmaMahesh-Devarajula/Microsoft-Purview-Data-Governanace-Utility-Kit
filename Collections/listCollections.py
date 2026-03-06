from PurviewClient.purviewclient import get_purview_admin_client

def listCollections():
    client = get_purview_admin_client()
    collections = list(client.collections.list_collections())
    print(collections)

if "__name__" == "__main__":
    list_collections()

