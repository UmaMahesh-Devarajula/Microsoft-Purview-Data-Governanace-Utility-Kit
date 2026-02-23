from credential import get_credentials
from authenticate import authenticate
from azure.purview.administration.account import PurviewAccountClient

def get_purview_admin_client():
    credentials = get_credentials()
    creds = authenticate()
    return PurviewAccountClient(
        endpoint=f"https://{creds['purview_account_name']}.purview.azure.com",
        credential=credentials,
        logging_enable=True
    )