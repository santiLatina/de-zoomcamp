import os
import logging

from datetime import datetime

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

#https://nyc-tlc.s3.amazonaws.com/trip+data/fhv_tripdata_2019-01.csv

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow")

URL_PREFIX = "https://s3.amazonaws.com/nyc-tlc/trip+data"
URL_TEMPLATE = URL_PREFIX + "/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv"


OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + "/output_{{ execution_date.strftime(\'%Y-%m\') }}.csv"
OUTPUT_FILE_PARQUET = OUTPUT_FILE_TEMPLATE.replace('.csv', '.parquet')

#parquet_file = "/output_{{ execution_date.strftime(\'%Y-%m\') }}.csv").replace('.csv', '.parquet')
#BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')


def format_to_parquet(src_file):
    if not src_file.endswith('.csv'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace('.csv', '.parquet'))


# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
def upload_to_gcs(bucket, object_name, local_file):
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
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


default_args = {
    "owner": "airflow",
    "start_date": datetime(2019, 12, 1),
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="data_ingestion_gcs_dag_hw",
    end_date = datetime(2020,7,1),
    schedule_interval="0 0 2 * *",
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:

    download_dataset_task_hw = BashOperator(
        task_id="download_dataset_task_hw",
        bash_command=f"curl -sSL {URL_TEMPLATE} > {OUTPUT_FILE_TEMPLATE}"
    )

    format_to_parquet_task_hw = PythonOperator(
        task_id="format_to_parquet_task_hw",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": f"{OUTPUT_FILE_TEMPLATE}",
        },
    )

    # TODO: Homework - research and try XCOM to communicate output values between 2 tasks/operators
    local_to_gcs_task_hw = PythonOperator(
        task_id="local_to_gcs_task_hw",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/{OUTPUT_FILE_PARQUET}",
            "local_file": f"{OUTPUT_FILE_PARQUET}",
        },
    )

    clean_disk = BashOperator(
        task_id="clean_disk",
        bash_command=f"rm {OUTPUT_FILE_TEMPLATE} {OUTPUT_FILE_PARQUET}",
    )

    download_dataset_task_hw >> format_to_parquet_task_hw >> local_to_gcs_task_hw >> clean_disk

    #bigquery_external_table_task_hw = BigQueryCreateExternalTableOperator(
    #    task_id="bigquery_external_table_task_hw",
    #    table_resource={
    #        "tableReference": {
    #            "projectId": PROJECT_ID,
    #            "datasetId": BIGQUERY_DATASET,
    #            "tableId": "external_table",
    #        },
    #        "externalDataConfiguration": {
    #            "sourceFormat": "PARQUET",
    #            "sourceUris": [f"gs://{BUCKET}/raw/{parquet_file}"],
    #        },
    #    },
    #)

    #download_dataset_task_hw >> format_to_parquet_task_hw >> local_to_gcs_task_hw >> bigquery_external_table_task_hw
