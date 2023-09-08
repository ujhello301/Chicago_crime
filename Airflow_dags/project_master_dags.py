import os
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from kaggle.api.kaggle_api_extended import KaggleApi
from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.contrib.operators.dataproc_operator import DataprocClusterCreateOperator, DataprocClusterDeleteOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.utils.trigger_rule import TriggerRule

BUCKET = os.environ.get("GCP_GCS_BUCKET")
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
#BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET")
BIGQUERY_DATASET='chicago_crime_all'
TABLE_NAME = 'et_chicago_crimes'
destination = f'{PROJECT_ID}:{BIGQUERY_DATASET}.{TABLE_NAME}'
INPUT_TABLE_NAME = TABLE_NAME
PARTITIONED_TABLE_NAME = 'CHICAGO_CRIMES_PARTITIONED'


years = list(range(2001,2023))
#local_filepath='/home/subin/project_master/dataset_files'
# Define dataset name and download path
dataset_name = "salikhussaini49/chicago-crimes"
local_filepath = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

#"./dataset_files"

def download_dataset(dataset_name, local_filepath):
    
    # Set Kaggle API credentials (replace with your own credentials)
    api = KaggleApi()
    api.authenticate()

    # Download dataset
    print (f'file path is {local_filepath}')
    logging.info(f'Downloading files...')
    api.dataset_download_files(dataset_name, path=local_filepath, unzip=True)
    logging.info('Downloading completed!')
    

# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
def upload_to_gcs(bucket, years, local_filepath):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(BUCKET)
    print("Bucket name is ",bucket)


    for year in years:
            filename=f"Chicago_Crimes_{year}.csv"
            logging.info(f'Uploading: {filename}')
            object_name=f"project/{filename}"
            blob = bucket.blob(object_name)
            local_file=f"{local_filepath}/{filename}"
            blob.upload_from_filename(local_file)
            logging.info(f'{filename} uploaded to GCS bucket')
    
    # Remove downloaded zip file
    logging.info(f'Removing downloaded files')
    #os.remove(local_filepath)


default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
dag = DAG('project_masterdag', default_args=default_args, schedule_interval=timedelta(days=1),catchup=False,tags=['dtc-de'])


download_dataset_task = PythonOperator(
    task_id="download_dataset_task",
    python_callable=download_dataset,
    op_kwargs={
        "dataset_name": dataset_name,
        "local_filepath": local_filepath,
    },
    dag=dag
)


# TODO: Homework - research and try XCOM to communicate output values between 2 tasks/operators
local_to_gcs_task = PythonOperator(
    task_id="local_to_gcs_task",
    python_callable=upload_to_gcs,
    op_kwargs={
        "bucket": BUCKET,
        "years": years,
        "local_filepath": local_filepath,
    },
    dag=dag
)

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

# Set up the BigQueryHook
bq_hook = BigQueryHook()

# Define the BigQueryExecuteQueryFileOperator
partition_bq_table = BigQueryInsertJobOperator(
    task_id='partition_bq_table',
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

# Define the Dataproc cluster name and region
cluster_name = 'my-dataproc-cluster'
region = 'europe-west6'

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

# Define l;p Dataproc cluster creation operator
create_cluster = DataprocClusterCreateOperator(
    task_id='create_dataproc_cluster',
    project_id=PROJECT_ID,
    cluster_name=cluster_name,
    cluster_config=CLUSTER_CONFIG,
    region=region,
    dag=dag
)

CRIME_TABLE = 'CRIME_TYPE_COUNT'
CRIMES_DAILY_TABLE = 'CRIMES_DAILY'


# Define the PySpark job
pyspark_job = {
    'reference': {
        'project_id': PROJECT_ID
    },
    'placement': {
        'cluster_name': cluster_name
    },
    'pyspark_job': {
        'main_python_file_uri': f'gs://{BUCKET}/spark_job.py',
        'jar_file_uris': ['gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.29.0.jar'],
        'args': [
            '--input_table', f"{PROJECT_ID}.{BIGQUERY_DATASET}.{PARTITIONED_TABLE_NAME}",
            '--output_table_crime', f'{BIGQUERY_DATASET}.{CRIME_TABLE}',
            '--output_table_daily', f'{BIGQUERY_DATASET}.{CRIMES_DAILY_TABLE}']
    }
}

# Define the PySpark job
spark_processing = DataprocSubmitJobOperator(
    task_id='spark_processing',
    job=pyspark_job,
    region=region,
    dag=dag
)




# Define Dataproc cluster deletion operator
delete_cluster = DataprocClusterDeleteOperator(
    task_id='delete_dataproc_cluster',
    cluster_name=cluster_name,
    region=region,
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag
)

#Delete files from GCS bucket
delete_gcs_files = GCSDeleteObjectsOperator(
        task_id='delete_gcs_files',
        bucket_name=BUCKET,
        prefix='project/Chicago_Crimes',
        dag=dag
    )


download_dataset_task >> local_to_gcs_task >> gcs_to_bq_task >> partition_bq_table >> create_cluster >> spark_processing >> (delete_cluster,delete_gcs_files)
  