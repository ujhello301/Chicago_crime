# Chicago_crime
Problem Statement
The Chicago city police department has collected a large dataset of crimes that have occurred in the city over the past several years, and they are looking to analyze this data in order to better understand crime patterns and trends. However, the dataset is currently in a CSV format and is not optimized for efficient querying and analysis. Therefore, developing an end-to-end data pipeline that can transform the CSV data into a format that is more suitable for analysis. The goal of this project is to create a streamlined and efficient process for analyzing crime data that can be used to inform decision-making and improve public safety in the city.

Used Technologies
Airflow: To orchestrate the workflow
Terraform: As Infrastructure as code tool to build the resources efficiently
Docker: To containerize the code and infrastructure
Google Cloud VM: Machine instance where services like docker and airflow are hosted
Google Cloud Storage: As Data Lake
Google BigQuery: As Data Warehouse
Apache Spark: Run data transformation
Google Dataproc Cluster: To run the Spark engine
Google Looker Studio: Visualization of the findings
Dataset used
This dataset reflects reported incidents of crime (with the exception of murders where data exists for each victim) that occurred in the City of Chicago from 2001 to 2022. Data is extracted from the Chicago Police Department's CLEAR (Citizen Law Enforcement Analysis and Reporting) system. Dataset is residing in Kaggle and is downloaded using Kaggle API from here.

Project Architecture
The end-to-end data pipeline includes the below steps:

The yearly files are downloaded using Kaggle API to the local machine or VM
The downloaded CSV files are then uploaded to a folder in Google Cloud bucket
This folder is loaded to a BigQuery table with all columns as string
A new table is created from this original table with correct data types as well as partitioned by Month and Clustered by Primary_Type of Crime for optimised performance
Spin up a dataproc cluster and execute the pyspark job where this data is transformed
Final table after transformation is pushed down as new table to BiqQuery
Configure Google Looker Studio to power dashboards from this final table
You can find the detailed information on the diagram below:

architecture chicago crimes

Reproducing from scratch
1. To reproduce this code entirely from scratch, you will need to create a GCP account:
To set up GCP, please follow the steps below:

If you don't have a GCP account, please create a free trial.

Setup new project and write down your Project ID.

Configure service account to get access to this project and download auth-keys (.json). Please check the service account has all the permissions below:

Viewer
Storage Admin
Storage Object Admin
BigQuery Admin
(if you have any trouble with permissions when you are running the airflow dag, just add these permissions aswell)

BigQuery Data Editor
BigQuery Data Owner
BigQuery Data Viewer
BigQuery Job User
BigQuery User
Download SDK for local setup.

Set environment variable to point to your downloaded auth-keys:

export GOOGLE_APPLICATION_CREDENTIALS="<path/to/your/service-account-authkeys>.json"

# Refresh token/session, and verify authentication
gcloud auth application-default login
Enable the following options under the APIs and services section:
Identity and Access Management (IAM) API
IAM service account credentials API
Compute Engine API (if you are going to use VM instance)
2. You'll need your IaC to build your infrastructure. In this project, Terraform is used
Download Terraform!

Download here: https://www.terraform.io/downloads
Initializing Terraform

Create a new directory with main.tf, and initialize your config file. How to Start
OPTIONAL Create variables.tf files to store your variables
terraform init
terraform plan
terraform apply
If you would like to remove your stack from the Cloud, use the terraform destroy command.

3. Set up Docker, Dockerfile, and docker-compose to run Airflow
The next steps provide you with the instructions of running Apache Airflow, which will allow you to run the entire orchestration, taking into account that you have already set up a GCP account.

You can run Airflow locally using docker-compose. Before running it, please make sure you have at least 5 GB of free RAM. Alternatively, you can launch Airflow on a virtual machine in GCP (in this case, please refer to this video).

Setup In the project repository, you can find the Dockerfile and the docker-compose.yaml file that are required to run Airflow.

The lightweight version of docker-compose file contains the minimum required set of components to run data pipelines. The only things you need to specify before launching it are your Project ID (GCP_PROJECT_ID) and Cloud Storage name (GCP_GCS_BUCKET) in the docker-compose.yaml. Please specify these variables according to your actual GCP setup.

You can easily run Airflow using the following commands:

docker-compose build to build the image (takes ~15 mins for the first-time);
docker-compose up airflow-init to initialize the Airflow scheduler, DB and other stuff;
docker-compose up to kick up the all the services from the container.
Now you can launch Airflow UI and run the DAGs.

Note: If you want to stop Airflow, please type docker-compose down command in your terminal.

4. Run the DAGs
Open the http://localhost:8080/ address in your browser and login using airflow username and airflow password.

In the screenshot below:

Run the project_masterdag and wait for it to complete.
image

image

Final Dashboard
The dashboard can be found here

image image image

Improvements
Need to integrate unit tests
Create a CI/CD with Github actions
