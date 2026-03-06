from PurviewClient.purviewclient import get_purview_admin_client

def deleteCollection():
    client = get_purview_admin_client()
    c_name = input("enter collection name to delete: ")

    try:
        response = client.collections.delete_collection(collection_name= c_name)
        print(f"collection {c_name} is deleted sucessfully")
    except Exception as e:
        print(f"Error in deleting collection {c_name}:", e)
        return

if "__name__" == "__main__":
    deleteCollection()