import os

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from scripts import transform_files,ingest_postgres, upload_to_gcs

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

PG_USER = os.getenv('POSTGRES_USER')
PG_HOST = os.getenv('PG_HOST')
PG_PASSWORD = os.getenv('POSTGRES_PASSWORD')
PG_PORT = os.getenv('PG_PORT')
PG_DATABASE = os.getenv('POSTGRES_DB')
PG_TABLE = os.getenv('PG_TABLE')

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

MONTHS = [10, 11, 12]

URL_PREFIX = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/'
OUTPUT_FILE_PATH = f'{AIRFLOW_HOME}/concatenated_green_tripdata_2020.csv'

local_workflow = DAG(
    "LocalIngestionDag",
    schedule_interval="0 5 * * *",
    start_date="2024-02-01"
)

with local_workflow:
    for month in MONTHS:
        URL_FILE_NAME = f'green_tripdata_2020-{month}.csv.gz'
        URL_TEMPLATE = URL_PREFIX + URL_FILE_NAME
        OUTPUT_FILE_TEMPLATE = f'{AIRFLOW_HOME}{URL_FILE_NAME}'

        curl_task = BashOperator(
            task_id=f'curl_task_month_{month}',
            bash_command=f'curl -sSL {URL_TEMPLATE} > {OUTPUT_FILE_TEMPLATE}'
        )

    transform_files_task = PythonOperator(
        task_id='transform_files_task',
        python_callable=transform_files,
        op_kwargs=dict(
            months=MONTHS,
            AIRFLOW_HOME=AIRFLOW_HOME,
            OUTPUT_FILE_PATH=OUTPUT_FILE_PATH,
        ),
    )

    ingest_postgres_task = PythonOperator(
        task_id="ingest_postgres_task",
        python_callable=ingest_postgres,
        op_kwargs=dict(
            user=PG_USER,
            password=PG_PASSWORD,
            host=PG_HOST,
            port=PG_PORT,
            db=PG_DATABASE,
            table_name=PG_TABLE,
            csv_file_path=OUTPUT_FILE_PATH
        ),
    )

    upload_to_gcs_task = PythonOperator(
        task_id="upload_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"airflow/{PG_TABLE}",
            "local_file": OUTPUT_FILE_PATH,
        },
    ) 
    curl_task >>  transform_files_task >> ingest_postgres_task >> upload_to_gcs_task