#import subprocess
#import sys

#subprocess.check_call([sys.executable, "-m", "pip", "install", "kaggle"])

import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from kaggle.api.kaggle_api_extended import KaggleApi
from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

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
with DAG(
    dag_id="project_data_ingestion_gcs_dag",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:

    download_dataset_task = PythonOperator(
        task_id="download_dataset_task",
        python_callable=download_dataset,
        op_kwargs={
            "dataset_name": dataset_name,
            "local_filepath": local_filepath,
        },
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
    )

    download_dataset_task >> local_to_gcs_task
  