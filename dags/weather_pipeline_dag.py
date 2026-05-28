# ============================================================
# WEATHER PIPELINE DAG
# Orchestrates: watermark read → ADF trigger → watermark update
# Schedule: Daily at 6 AM
# Alerts: Email on any task failure
# ============================================================

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta, timezone
import sys
import os

# Must be set BEFORE any config imports
sys.path.insert(0, "/opt/airflow/config")
sys.path.insert(0, "/opt/airflow/dags/utils")

from pipeline_config import AZURE_CONFIG, ADF_CONFIG, EMAIL_CONFIG, PIPELINE_CONFIG
from watermark import compute_date_range, update_watermark


# ============================================================
# EMAIL ALERT FUNCTION
# ============================================================

def send_failure_email(context):
    """Called automatically on any task failure"""
    import smtplib
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart

    task_instance = context.get("task_instance")
    dag_id        = context.get("dag").dag_id
    task_id       = task_instance.task_id
    exec_date     = context.get("execution_date")
    log_url       = task_instance.log_url

    subject = f"❌ Pipeline FAILED — {dag_id} | Task: {task_id}"
    body    = f"""
    <h2>Pipeline Failure Alert</h2>
    <table>
        <tr><td><b>DAG</b></td><td>{dag_id}</td></tr>
        <tr><td><b>Failed Task</b></td><td>{task_id}</td></tr>
        <tr><td><b>Execution Date</b></td><td>{exec_date}</td></tr>
        <tr><td><b>Log URL</b></td><td><a href="{log_url}">View Logs</a></td></tr>
    </table>
    <p>Please investigate and rerun the pipeline.</p>
    """

    msg                     = MIMEMultipart("alternative")
    msg["Subject"]          = subject
    msg["From"]             = EMAIL_CONFIG["sender_email"]
    msg["To"]               = EMAIL_CONFIG["recipient_email"]
    msg.attach(MIMEText(body, "html"))

    with smtplib.SMTP(EMAIL_CONFIG["smtp_host"], EMAIL_CONFIG["smtp_port"]) as server:
        server.starttls()
        server.login(EMAIL_CONFIG["sender_email"], EMAIL_CONFIG["sender_password"])
        server.sendmail(
            EMAIL_CONFIG["sender_email"],
            EMAIL_CONFIG["recipient_email"],
            msg.as_string()
        )
    print(f"Failure email sent for task: {task_id}")


# ============================================================
# DEFAULT ARGS — apply to all tasks
# ============================================================

default_args = {
    "owner"             : "data-engineering",
    "depends_on_past"   : False,
    "start_date"        : datetime(2024, 1, 8),
    "retries"           : 1,
    "retry_delay"       : timedelta(minutes=5),
    "on_failure_callback": send_failure_email,   # email on every task failure
}


# ============================================================
# TASK FUNCTIONS
# ============================================================

def task_read_watermark(**kwargs):
    """Read watermark and push dates to XCom for next tasks"""
    dates = compute_date_range()
    kwargs["ti"].xcom_push(key="start_date", value=dates["start_date"])
    kwargs["ti"].xcom_push(key="end_date",   value=dates["end_date"])
    print(f"Date range computed: {dates['start_date']} to {dates['end_date']}")
    return dates


def task_trigger_adf(**kwargs):
    """Trigger ADF pipeline with start/end dates from watermark"""
    from azure.identity import ClientSecretCredential
    from azure.mgmt.datafactory import DataFactoryManagementClient

    # Pull dates from XCom
    ti         = kwargs["ti"]
    start_date = ti.xcom_pull(key="start_date", task_ids="read_watermark")
    end_date   = ti.xcom_pull(key="end_date",   task_ids="read_watermark")

    print(f"Triggering ADF pipeline: {start_date} to {end_date}")

    # Authenticate
    credential = ClientSecretCredential(
        tenant_id     = AZURE_CONFIG["tenant_id"],
        client_id     = AZURE_CONFIG["client_id"],
        client_secret = AZURE_CONFIG["client_secret"]
    )

    # ADF client
    adf_client = DataFactoryManagementClient(
        credential      = credential,
        subscription_id = ADF_CONFIG["subscription_id"]
    )

    # Trigger pipeline run
    run_response = adf_client.pipelines.create_run(
        resource_group_name = ADF_CONFIG["resource_group"],
        factory_name        = ADF_CONFIG["factory_name"],
        pipeline_name       = ADF_CONFIG["pipeline_name"],
        parameters          = {
            "p_start_date" : start_date,
            "p_end_date"   : end_date
        }
    )

    run_id = run_response.run_id
    print(f"ADF pipeline triggered. Run ID: {run_id}")
    kwargs["ti"].xcom_push(key="adf_run_id", value=run_id)
    return run_id


def task_monitor_adf(**kwargs):
    """Poll ADF pipeline run status until completion"""
    import time
    from azure.identity import ClientSecretCredential
    from azure.mgmt.datafactory import DataFactoryManagementClient

    ti     = kwargs["ti"]
    run_id = ti.xcom_pull(key="adf_run_id", task_ids="trigger_adf_pipeline")

    credential = ClientSecretCredential(
        tenant_id     = AZURE_CONFIG["tenant_id"],
        client_id     = AZURE_CONFIG["client_id"],
        client_secret = AZURE_CONFIG["client_secret"]
    )

    adf_client = DataFactoryManagementClient(
        credential      = credential,
        subscription_id = ADF_CONFIG["subscription_id"]
    )

    print(f"Monitoring ADF run: {run_id}")

    # Poll every 30 seconds until terminal state
    terminal_states = {"Succeeded", "Failed", "Cancelled"}
    while True:
        run_status = adf_client.pipeline_runs.get(
            resource_group_name = ADF_CONFIG["resource_group"],
            factory_name        = ADF_CONFIG["factory_name"],
            run_id              = run_id
        )
        status = run_status.status
        print(f"ADF run status: {status}")

        if status in terminal_states:
            break
        time.sleep(30)

    if status != "Succeeded":
        raise Exception(f"ADF pipeline run FAILED with status: {status}. Run ID: {run_id}")

    print(f"ADF pipeline completed successfully. Run ID: {run_id}")
    return status


def task_run_databricks_job(job_type: str):
    """Factory function — returns a callable for each job type"""
    def run_job(**kwargs):
        import requests
        import time
        from datetime import datetime, timezone

        ti         = kwargs["ti"]
        start_date = ti.xcom_pull(key="start_date", task_ids="read_watermark")
        end_date   = ti.xcom_pull(key="end_date",   task_ids="read_watermark")
        ingestion_date = datetime.now(timezone.utc).strftime("%Y/%m/%d")

        # Import here to avoid top-level Docker path issues
        import sys
        sys.path.insert(0, "/opt/airflow/config")
        from pipeline_config import DATABRICKS_CONFIG

        workspace_url = DATABRICKS_CONFIG["workspace_url"]
        token         = DATABRICKS_CONFIG["token"]
        job_id        = DATABRICKS_CONFIG[f"{job_type}_job_id"]

        headers = {
            "Authorization" : f"Bearer {token}",
            "Content-Type"  : "application/json"
        }

        # Trigger job run with date parameters
        trigger_url = f"{workspace_url}/api/2.1/jobs/run-now"
        payload = {
            "job_id": int(job_id),
            "notebook_params": {
                "p_start_date" : start_date,
                "p_end_date"   : end_date,
                "p_ingestion_date" : ingestion_date
            }
        }
        print(f"Notebook params being sent: {payload['notebook_params']}")
        print(f"Triggering Databricks job: {job_type} | dates: {start_date} to {end_date}")
        response = requests.post(trigger_url, headers=headers, json=payload)
        response.raise_for_status()

        run_id = response.json()["run_id"]
        print(f"Databricks job triggered. Run ID: {run_id}")

        # Poll until completion
        status_url    = f"{workspace_url}/api/2.1/jobs/runs/get"
        terminal_states = {"SUCCESS", "FAILED", "CANCELLED", "TIMEDOUT", "SKIPPED"}

        while True:
            status_response = requests.get(
                status_url,
                headers=headers,
                params={"run_id": run_id}
            )
            status_response.raise_for_status()
            run_data      = status_response.json()
            life_cycle    = run_data["state"]["life_cycle_state"]
            result_state  = run_data["state"].get("result_state", "")

            print(f"Databricks run {run_id} — state: {life_cycle} | result: {result_state}")

            if life_cycle == "TERMINATED":
                if result_state != "SUCCESS":
                    raise Exception(
                        f"Databricks {job_type} job FAILED. "
                        f"Run ID: {run_id} | Result: {result_state}"
                    )
                print(f"✅ Databricks {job_type} completed successfully")
                break
            elif life_cycle in terminal_states:
                raise Exception(f"Databricks job ended unexpectedly: {life_cycle}")

            time.sleep(20)

        return run_id
    return run_job


def task_update_watermark(**kwargs):
    """Update watermark to end_date after successful pipeline run"""
    ti       = kwargs["ti"]
    end_date = ti.xcom_pull(key="end_date", task_ids="read_watermark")
    update_watermark(new_date=end_date, status="success")
    print(f"Watermark updated to: {end_date}")


# ============================================================
# DAG DEFINITION
# ============================================================

with DAG(
    dag_id              = "weather_pipeline_daily",
    default_args        = default_args,
    description         = "Daily weather data pipeline — Open-Meteo → ADF → Databricks",
    start_date      = datetime(2024, 1, 8),
    schedule   = PIPELINE_CONFIG["schedule"],  # 0 6 * * *
    catchup             = True,   # True so it runs for missed days 2024-01-08 to 2024-01-11
    max_active_runs     = 1,      # Never run two days in parallel
    tags                = ["weather", "data-engineering", "production"]
) as dag:

    read_watermark = PythonOperator(
        task_id         = "read_watermark",
        python_callable = task_read_watermark,
        
    )

    trigger_adf = PythonOperator(
        task_id         = "trigger_adf_pipeline",
        python_callable = task_trigger_adf,
        
    )

    monitor_adf = PythonOperator(
        task_id         = "monitor_adf_pipeline",
        python_callable = task_monitor_adf,
        
    )

    run_bronze_silver = PythonOperator(
        task_id         = "run_bronze_to_silver",
        python_callable = task_run_databricks_job("bronze_to_silver"),
        
    )

    run_silver_gold = PythonOperator(
        task_id         = "run_silver_to_gold",
        python_callable = task_run_databricks_job("silver_to_gold"),
        
    )

    update_watermark_task = PythonOperator(
        task_id         = "update_watermark",
        python_callable = task_update_watermark,
        
    )

    # DAG flow
    read_watermark >> trigger_adf >> monitor_adf >> run_bronze_silver >> run_silver_gold >> update_watermark_task