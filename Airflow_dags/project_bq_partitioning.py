from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from datetime import datetime
from airflow.utils.dates import days_ago
import os


BUCKET = os.environ.get("GCP_GCS_BUCKET")
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BIGQUERY_DATASET='chicago_crime_all'
INPUT_TABLE_NAME = 'et_chicago_crimes'
PARTITIONED_TABLE_NAME = 'CHICAGO_CRIMES_PARTITIONED'


# Set up the BigQueryHook
bq_hook = BigQueryHook()

# Create a DAG object
default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

dag = DAG(
    'project_bigquery_sql_dag',
    default_args=default_args,
    schedule_interval=None
)

# Define the path to the SQL file
sql_file = 'bq_partitioning.sql'



# Define the job configuration
job_config = {
    'jobReference': {
        'projectId': PROJECT_ID,
        'jobId': 'my-job'
    },
    'configuration': {
        'query': {
            'query': 'select * FROM `chicago_crime_all.et_chicago_crimes` limit 100',
            'useLegacySql': False
        }
    }
}

# Define the arguments to pass to the SQL file
arguments = {
    'INPUT_TABLE_NAME': INPUT_TABLE_NAME,
    'PARTITIONED_TABLE_NAME': PARTITIONED_TABLE_NAME,
    'PROJECT_ID': PROJECT_ID,
    'BIGQUERY_DATASET': BIGQUERY_DATASET
}

# Define the BigQueryExecuteQueryFileOperator
bigquery_operator = BigQueryInsertJobOperator(
    task_id='execute_sql_file',
    configuration={
        "query": {
            "query": "{% include 'bq_partitioning.sql' %}",
            "useLegacySql": False
        }
    },
    params={
            'projectId': PROJECT_ID,
            'datasetId': BIGQUERY_DATASET,
            'tableId': INPUT_TABLE_NAME
        },
    location='europe-west6',
    dag=dag
)

# Set task dependencies
bigquery_operator
