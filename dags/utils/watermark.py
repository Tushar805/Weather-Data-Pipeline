import json
from datetime import datetime, timedelta
from azure.storage.blob import BlobServiceClient
from azure.identity import ClientSecretCredential
import sys
import os

sys.path.insert(0, "/opt/airflow/config")

from pipeline_config import AZURE_CONFIG


def get_adls_client():
    credential = ClientSecretCredential(
        tenant_id     = AZURE_CONFIG["tenant_id"],
        client_id     = AZURE_CONFIG["client_id"],
        client_secret = AZURE_CONFIG["client_secret"]
    )
    account_url = f"https://{AZURE_CONFIG['storage_account']}.blob.core.windows.net"
    return BlobServiceClient(account_url=account_url, credential=credential)


def read_watermark():
    """Read current watermark from ADLS"""
    try:
        client      = get_adls_client()
        container   = AZURE_CONFIG["container"]
        blob_path   = AZURE_CONFIG["watermark_path"]
        blob_client = client.get_blob_client(container=container, blob=blob_path)
        data        = blob_client.download_blob().readall()
        watermark   = json.loads(data)
        print(f"Watermark read: {watermark}")
        return watermark
    except Exception as e:
        print(f"Error reading watermark: {e}")
        raise


def update_watermark(new_date: str, status: str = "success"):
    """Update watermark in ADLS after successful run"""
    try:
        client      = get_adls_client()
        container   = AZURE_CONFIG["container"]
        blob_path   = AZURE_CONFIG["watermark_path"]
        blob_client = client.get_blob_client(container=container, blob=blob_path)

        watermark = {
            "last_loaded_date"      : new_date,
            "last_run_status"       : status,
            "last_run_timestamp"    : datetime.utcnow().isoformat() + "Z"
        }

        blob_client.upload_blob(
            json.dumps(watermark, indent=2),
            overwrite=True
        )
        print(f"Watermark updated to: {new_date}")
        return watermark
    except Exception as e:
        print(f"Error updating watermark: {e}")
        raise


def compute_date_range():
    """
    Read watermark and compute start/end dates for next run.
    start_date = last_loaded_date + 1 day
    end_date   = start_date (one day per run)
    """
    watermark  = read_watermark()
    last_date  = datetime.strptime(watermark["last_loaded_date"], "%Y-%m-%d")
    start_date = last_date + timedelta(days=1)
    end_date   = start_date  # one day per run

    return {
        "start_date" : start_date.strftime("%Y-%m-%d"),
        "end_date"   : end_date.strftime("%Y-%m-%d")
    }