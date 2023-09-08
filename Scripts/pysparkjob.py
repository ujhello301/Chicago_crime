from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp
import argparse
import os


PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BIGQUERY_DATASET='chicago_crime_all'
INPUT_TABLE_NAME = 'et_chicago_crimes_lite'

# Initialize the argparse parser
parser = argparse.ArgumentParser()

# Add the command-line arguments
parser.add_argument('--input_table', type=str, help='Description of param')

# Parse the command-line arguments
args = parser.parse_args()

# Access the values of the command-line arguments
input_table = args.input_table


# create a Spark session
spark = SparkSession.builder \
    .appName("bigquery-to-pyspark") \
    .getOrCreate()


# set GCP credentials for accessing BigQuery
spark.conf.set("spark.jars", "gs://spark-lib/bigquery/spark-bigquery-latest.jar")
spark.conf.set("google.cloud.auth.service.account.enable", "true")
spark.conf.set("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
spark.conf.set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/path/to/keyfile.json")

# define the project ID and dataset ID of the BigQuery table to read from
project_id = PROJECT_ID
dataset_id = BIGQUERY_DATASET
table_id = INPUT_TABLE_NAME

output_table=f"{project_id}.{dataset_id}.TOTAL_CRIMES"
# read the BigQuery table into a PySpark DataFrame
df = spark.read \
    .format("bigquery") \
    .option("project", project_id) \
    .option("dataset", dataset_id) \
    .option("table", table_id) \
    .load()

# perform any necessary data transformations on the DataFrame
# for example, you could filter rows, group by columns, or rename columns
output_df = df.groupBy("Primary_Type").count()

# write the transformed DataFrame back to BigQuery
output_df.write \
    .format("bigquery") \
    .option("table", output_table) \
    .option("temporaryGcsBucket", "your-gcs-bucket") \
    .mode("overwrite") \
    .save()
