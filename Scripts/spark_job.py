from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import to_timestamp
import argparse

# Initialize the argparse parser
parser = argparse.ArgumentParser()

print("Parsing arguments")
# Add the command-line arguments
parser.add_argument('--input_table', type=str, help='Description of param')
parser.add_argument('--output_table_crime', type=str, help='Description of param')
parser.add_argument('--output_table_daily', type=str, help='Description of param')


# Parse the command-line arguments
args = parser.parse_args()

# Access the values of the command-line arguments
input_table = args.input_table
output_table_crime = args.output_table_crime
output_table_daily = args.output_table_daily 


# Use the parameter values in your PySpark job
print(input_table)
print(output_table_crime)
print(output_table_daily)

# Define the main function that will be executed on the Dataproc cluster
def main(input_table,output_table_crime,output_table_daily):
    # Create a SparkSession
    spark = SparkSession.builder.appName("BigQuery to BigQuery with PySpark").getOrCreate()
    print('Created session')


    # Read the input BigQuery table into a DataFrame
    df = spark.read.format("bigquery") \
        .option("table", input_table) \
        .load()

    print('df reading complete')
    #df = df.withColumn("timestamp", to_timestamp("date", "MM/dd/yyyy hh:mm:ss a") )
	


    # Group by the crime type column and get the count of records
    output_totalcrimes_df = df.groupBy("Primary_Type").count()

    print('totalcrimes df created')

    # Group by the date column and get the count of records
    output_crimes_by_day_df = df.groupBy("New_Date").count()

    print('daywise count df created')


    # Write the output DataFrame to the output BigQuery table
    output_totalcrimes_df.write.format("bigquery") \
        .option("table", output_table_crime) \
        .option("writeMethod","direct") \
        .option("createDisposition", "CREATE_IF_NEEDED") \
        .mode("overwrite") \
        .save()

    output_crimes_by_day_df.write.format("bigquery") \
        .option("table", output_table_daily) \
        .option("writeMethod","direct") \
        .option("createDisposition", "CREATE_IF_NEEDED") \
        .mode("overwrite") \
        .save()

    print('Data written to BQ table')

    # Stop the SparkSession
    spark.stop()

main(input_table,output_table_crime,output_table_daily)