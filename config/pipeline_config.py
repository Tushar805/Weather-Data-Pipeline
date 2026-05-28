# ============================================================
# Pipeline Configuration
# ============================================================

AZURE_CONFIG = {
    # Storage
    "storage_account"  : "",
    "container"        : "",
    "watermark_path"   : "",

    # Service Principal (for Airflow → ADLS) (Basically the App registration we created in Entra ID for Airflow)
    "tenant_id"        : "",
    "client_id"        : "",
    "client_secret"    : "",
}

ADF_CONFIG = {
    "subscription_id"  : "",
    "resource_group"   : "",
    "factory_name"     : "",
    "pipeline_name"    : "",
}

EMAIL_CONFIG = {
    "smtp_host"        : "smtp.gmail.com",
    "smtp_port"        : 587,
    "sender_email"     : "email@gmail.com",
    "sender_password"  : "",  # Gmail app password
    "recipient_email"  : "email@gmail.com",
}

PIPELINE_CONFIG = {
    "start_date"       : "2024-01-01",   # Initial watermark date
    "schedule"         : "0 1 * * *",   # Daily at 6:30 AM
    "lookback_days"    : 1,             # How many days per run
}

DATABRICKS_CONFIG = {
    "workspace_url"       : "",  # your databricks workspace URL(URL from address bar)
    "token"               : "",  # datacricks access token
    "bronze_to_silver_job_id" : "442070253877021",  
    "silver_to_gold_job_id"   : "617757422856012",  
}