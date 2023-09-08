from airflow import DAG
from airflow.contrib.operators.dataproc_operator import DataprocClusterCreateOperator, DataprocClusterDeleteOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

from datetime import datetime, timedelta
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.dates import days_ago
import os

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BIGQUERY_DATASET='chicago_crime_all'
TABLE_NAME = 'et_chicago_crimes'
BUCKET = os.environ.get("GCP_GCS_BUCKET")

# Define the Dataproc cluster name and region
cluster_name = 'my-dataproc-cluster'
region = 'europe-west6'

# Define DAG arguments
default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 30},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 30},
    },
}

# Define DAG
dag = DAG('project_transform_bq_data', default_args=default_args, schedule_interval=None)

# Define l;p Dataproc cluster creation operator
create_cluster = DataprocClusterCreateOperator(
    task_id='create_dataproc_cluster',
    project_id=PROJECT_ID,
    cluster_name=cluster_name,
    cluster_config=CLUSTER_CONFIG,
    region=region,
    dag=dag
)


# Define Dataproc cluster deletion operator
delete_cluster = DataprocClusterDeleteOperator(
    task_id='delete_dataproc_cluster',
    cluster_name=cluster_name,
    region=region,
    trigger_rule=TriggerRule.ALL_DONE,
#    dag=dag
)




#create_cluster >> process_data_job >> delete_cluster
