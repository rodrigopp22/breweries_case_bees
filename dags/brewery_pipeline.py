from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from dags.modules.extract_brewery_data import run as extract_data
from dags.modules.transform_brewery_data import run as transform_data
from dags.modules.load_tb_brewery_by_location import run as load_data

dag = DAG(
    'bees_pipeline',
    start_date=datetime(2024, 10, 16),
    description='Pipeline para o case de DE da BEES',
    catchup=False
)

extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    retries=3,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    retries=3,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    retries=3,
    dag=dag,
)

extract_task >> transform_task >> load_task
