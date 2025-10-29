from datetime import datetime
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'retries': 0
}

def validate_input(**context):
    input_year = context['dag_run'].conf.get('input_year')
    if not input_year:
        raise ValueError("input_year must be provided in configuration!")
    if not isinstance(input_year, int):
        raise TypeError("input_year must be an integer")
    return input_year

def execute_load(**context):
    hook = PostgresHook(postgres_conn_id='postgres_default')
    print(f"Connection Details: {hook.get_uri()}")
    conn = hook.get_conn()
    cursor = conn.cursor()

    input_year = context['dag_run'].conf['input_year']

    try:
        cursor.execute("SELECT load_date_range(%s);", (input_year,))
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()

with DAG(
    'date_dimension_loader',
    default_args=default_args,
    description='DAG to load dates using PostgresHook',
    schedule_interval=None,
    catchup=False,
    tags=['dimension', 'date'],
    params={
        "input_year": 0
    }
) as dag:

    validate_params = PythonOperator(
        task_id='validate_parameters',
        python_callable=validate_input,
        provide_context=True
    )

    execute_load_task = PythonOperator(
        task_id='execute_date_load',
        python_callable=execute_load,
        provide_context=True
    )

    validate_params >> execute_load_task