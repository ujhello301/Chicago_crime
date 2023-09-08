from airflow import DAG
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
#from airflow.providers.google.cloud.hooks.gcs import GCSHook
from datetime import datetime, timedelta

from airflow.utils.dates import days_ago
import os

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")



# Define DAG arguments
default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}
#gcs_hook = GCSHook()

# Define DAG
dag = DAG('project_delete_gcs_files', default_args=default_args, schedule_interval=None)

delete_gcs_file = GCSDeleteObjectsOperator(
        task_id='delete_gcs_file',
        bucket_name=BUCKET,
        prefix='project/Chicago_Crimes',
        dag=dag
    )

delete_gcs_file