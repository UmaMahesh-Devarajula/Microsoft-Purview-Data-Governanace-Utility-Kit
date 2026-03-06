from PurviewClient.purviewclient import get_purview_admin_client
from tabulate import tabulate

def listCollections():
    client = get_purview_admin_client()
    collections = list(client.collections.list_collections())
    rows = []
    for c in collections:
        rows.append([
            c.get("name"),
            c.get("friendlyName"),
            c.get("description"),
            c.get("parentCollection", {}).get("referenceName", "-"),
            c["systemData"].get("createdAt"),
            c.get("collectionProvisioningState")
        ])

    # Define headers
    headers = ["Name", "Friendly Name", "Description", "Parent", "Created At", "State"]

    # Print table
    print(tabulate(rows, headers=headers, tablefmt="grid"))

if "__name__" == "__main__":
    list_collections()

