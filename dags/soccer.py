from airflow import DAG
from airflow.decorators import task, dag
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
import sys

sys.path.append("/opt/airflow/includes")

from soccer_etl import extract_sqlite_to_postgres, preprocess_data, transform_and_load_data

default_args = {
    'owner' : 'airflow',
    'depends_on_past' : False,
    'start_date' : datetime.now(),
    'retries' : 1,
    'retry_delay' : timedelta(minutes=5),
}

def extract_soccer_data(**kwargs):
    extract_sqlite_to_postgres('/opt/airflow/data/database.sqlite')

def preprocess_soccer_data(**kwargs):
    preprocess_data()

def transform_and_load_soccer_data(**kwargs):
    transform_and_load_data()

with DAG(
    'soccer_dag',
    default_args = default_args,
    description = f'Soccer DAG',
    schedule_interval = None,
    catchup = False
) as dag:

    extract_soccer_data = PythonOperator(
          task_id = "extract_all_soccer_data",
          python_callable = extract_soccer_data
    )

    preprocess_soccer_data = PythonOperator(
        task_id = "preprocess_all_soccer_data",
        python_callable = preprocess_soccer_data
    )

    transform_and_load_soccer_data = PythonOperator(
        task_id = 'transform_and_load_soccer_data',
        python_callable = transform_and_load_soccer_data
    )

    extract_soccer_data >> preprocess_soccer_data >> transform_and_load_soccer_data