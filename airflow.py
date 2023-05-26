import os
from datetime import datetime
from airflow import models
from airflow.providers.google.cloud.operators.dataproc import (
    ClusterGenerator,
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator
)

# Param Dataproc cluster initializations 
DAG_ID = "dataproc_cluster_jobs"
PROJECT_ID = "savvy-hull-383206"
BUCKET_NAME = "demos-airflow"
CLUSTER_NAME = "newcluster"
REGION = "europe-central2"
ZONE = "europe-central2-c"

#PySPark scripts paths
SCRIPT_BUCKET_PATH = "just_bucket4/py_scripts"

# GCS -> AGGREGATE -> BQ
SCRIPT_NAME_1 = "gcs_to_bq.py"

# PySpark job configs for Job2
PYSPARK_JOB_1 = {
                "reference": {"project_id": PROJECT_ID},
                "placement": {"cluster_name": CLUSTER_NAME},
                "pyspark_job": {"main_python_file_uri": f"gs://{SCRIPT_BUCKET_PATH}/{SCRIPT_NAME_1}"}

                }

# DAH definition is here
with models.DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2023, 5, 26),
    catchup=False,
    tags=["example", "dataproc"],
) as dag:

    # PySpark task to read data from GCS , perform agrregate on data and write data into Bigquery
    pyspark_task_gcs_to_bq = DataprocSubmitJobOperator(
        task_id="pyspark_task_gcs_to_bq", 
        job=PYSPARK_JOB_1, 
        region=REGION, 
        project_id=PROJECT_ID
    )

# Set task dependencies
pyspark_task_gcs_to_bq