from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from modules.extract_brewery_data import run as extract_data
from modules.transform_brewery_data import run as transform_data
from modules.load_tb_brewery_by_location import run as load_data

dag = DAG(
    'bees_pipeline',
    start_date= datetime(2024,10,16),
    description='Pipeline para o case de DE da BEES',
    catchup=False
)

extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag,
)

extract_task >> transform_task >> load_task
