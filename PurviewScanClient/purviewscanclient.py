from PurviewCredentials.credential import get_credentials
from Authenticate.authenticate import authenticate
from azure.purview.scanning import PurviewScanningClient

def get_purview_scan_client():
    credentials = get_credentials()
    creds = authenticate()
    return PurviewScanningClient(
        endpoint=f"https://{creds['purview_account_name']}.scan.purview.azure.com",
        credential=credentials,
        logging_enable=True
    )