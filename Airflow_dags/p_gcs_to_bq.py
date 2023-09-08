import os


from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator



BUCKET = os.environ.get("GCP_GCS_BUCKET")
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
#BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET")
BIGQUERY_DATASET='chicago_crime_all'
TABLE_NAME = 'et_chicago_crimes'
destination = f'{PROJECT_ID}:{BIGQUERY_DATASET}.{TABLE_NAME}'

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}


dag = DAG('project_gcs_to_bq', default_args=default_args, schedule_interval=timedelta(days=1))

gcs_to_bq_task = GoogleCloudStorageToBigQueryOperator(
    task_id='gcs_to_bq',
    bucket='{{ params.bucket }}',
    source_objects=['project/*.csv'],
    destination_project_dataset_table='{{ params.destination }}',
    schema_fields=[
        {'name': 'ID', 'type': 'INTEGER','mode': 'NULLABLE'}, 
        {'name': 'Case_Number', 'type': 'STRING','mode': 'NULLABLE'}, 
        {'name': 'Date', 'type': 'STRING','mode': 'NULLABLE'}, 
        {'name': 'Block', 'type': 'STRING','mode': 'NULLABLE'}, 
        {'name': 'IUCR', 'type': 'STRING','mode': 'NULLABLE'}, 
        {'name': 'Primary_Type', 'type': 'STRING','mode': 'NULLABLE'}, 
        {'name': 'Description', 'type': 'STRING','mode': 'NULLABLE'}, 
        {'name': 'Location_Description', 'type': 'STRING','mode': 'NULLABLE'}, 
        {'name': 'Arrest', 'type': 'STRING','mode': 'NULLABLE'}, 
        {'name': 'Domestic', 'type': 'STRING','mode': 'NULLABLE'}, 
        {'name': 'Beat', 'type': 'STRING','mode': 'NULLABLE'}, 
        {'name': 'District', 'type': 'STRING','mode': 'NULLABLE'}, 
        {'name': 'Ward', 'type': 'STRING','mode': 'NULLABLE'}, 
        {'name': 'Community_Area', 'type': 'STRING','mode': 'NULLABLE'}, 
        {'name': 'FBI_Code', 'type': 'STRING','mode': 'NULLABLE'}, 
        {'name': 'X_Coordinate', 'type': 'STRING','mode': 'NULLABLE'}, 
        {'name': 'Y_Coordinate', 'type': 'STRING','mode': 'NULLABLE'}, 
        {'name': 'Year', 'type': 'INTEGER','mode': 'NULLABLE'}, 
        {'name': 'Updated_On', 'type': 'STRING','mode': 'NULLABLE'}, 
        {'name': 'Latitude', 'type': 'FLOAT','mode': 'NULLABLE'}, 
        {'name': 'Longitude', 'type': 'FLOAT','mode': 'NULLABLE'}, 
        {'name': 'Location', 'type': 'STRING','mode': 'NULLABLE'}],
    field_delimiter=',',
    quote_character='"',
    write_disposition='WRITE_TRUNCATE',
    params={
            'bucket': BUCKET,
            'destination': destination
    },
    dag=dag
)
gcs_to_bq_task