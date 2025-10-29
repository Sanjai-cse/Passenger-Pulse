import csv
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

def insert_data_from_csv_into_postgres(**kwargs):

    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    csv_file_path = '/opt/airflow/data/delay_reasons_data.csv'

    insert_query = """
    INSERT INTO delay_ext (delay_code, description)
    VALUES ( %s, %s)
    """

    with open(csv_file_path, mode='r') as file:
        reader = csv.reader(file)
        next(reader)
        for row in reader:
            pg_hook.run(insert_query, parameters=(row[0], row[1]))

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': 300,
}

with DAG(
    'delay_ext_dag',
    default_args=default_args,
    description='A simple DAG to insert data from a CSV file into PostgreSQL',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    insert_data_from_csv_task = PythonOperator(
        task_id='insert_data_from_csv_into_postgres_task',
        python_callable=insert_data_from_csv_into_postgres,
        provide_context=True
    )

start_task = EmptyOperator(
    task_id='start_task'
)

end_task = EmptyOperator(
     task_id='end_task'
)

start_task>>insert_data_from_csv_task>>end_task
