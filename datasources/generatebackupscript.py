import os
import json
from typing import Dict, List

BACKUP_DIR = "RecoverDatasources"

def generate_backup_script(source_type: str, props: Dict[str, str], payload: Dict):
    os.makedirs(BACKUP_DIR, exist_ok=True)
    filename = os.path.join(BACKUP_DIR, f"backup-{props.get('ds_name','datasource')}_registration.py")
    script = f'''import json
from azure.identity import ClientSecretCredential
from azure.purview.scanning import PurviewScanningClient
from authenticate import authenticate

creds = authenticate()

def get_credentials():
    return ClientSecretCredential(
        client_id=creds["client_id"],
        client_secret=creds["client_secret"],
        tenant_id=creds["tenant_id"]
    )

def get_purview_scan_client():
    credentials = get_credentials()
    return PurviewScanningClient(
        endpoint=f"https://{{creds['purview_account_name']}}.scan.purview.azure.com",
        credential=credentials,
        logging_enable=True
    )

def recreate_datasource():
    client = get_purview_scan_client()
    data_source = {json.dumps(payload, indent=2)}
    response = client.data_sources.create_or_update(data_source_name="{props.get('ds_name','')}", body=data_source)
    print("Data source recreated:", response)

recreate_datasource()
'''
    with open(filename, "w", encoding="utf-8") as f:
        f.write(script)
    print(f"Backup script created: {filename}")
