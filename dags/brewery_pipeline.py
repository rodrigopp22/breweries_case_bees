from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

import os
import sys

sys.path.insert(0, os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..')))

from dags.modules.extract_brewery_data import run as extract_data
from dags.modules.transform_brewery_data import run as transform_data
from dags.modules.load_tb_brewery_by_location import run as load_data

default_args = {
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 16),
    'retries': 1,
    'retry_delay': timedelta(seconds=30)
}

dag = DAG(
    'bees_pipeline',
    default_args=default_args,
    schedule_interval='*/30 * * * *',
    description='Pipeline para o case de DE da BEES',
    default_view='graph',
    catchup=False
)

extract_task = PythonOperator(
    task_id='extract_brewery_data_from_api',
    python_callable=extract_data,
    execution_timeout=timedelta(minutes=30),
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data_from_bronze_to_silver',
    python_callable=transform_data,
    execution_timeout=timedelta(minutes=30),
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_aggregate_view_in_gold',
    python_callable=load_data,
    execution_timeout=timedelta(minutes=30),
    dag=dag,
)

extract_task >> transform_task >> load_task
