from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from datetime import datetime
from transform.dim_logic import *

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': 300,
}

with DAG(
    'dim_loader',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['dimension_table'],
) as dag:

    start = EmptyOperator(task_id='start')


    @task(task_id="load_data_operation")
    def load_data_operation():
        transform_and_load_operations()

    load_data_operation()


    @task(task_id="load_data_flight")
    def load_data_flight():
         transform_and_load_flight()

    load_data_flight()

    @task(task_id="load_data_passenger")
    def load_data_passenger():
        transform_and_load_passengers()

    load_data_passenger()

    end = EmptyOperator(task_id='end')