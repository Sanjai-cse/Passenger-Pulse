from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime

default_args = {
     'owner': 'airflow',
     'depends_on_past': False,
     'start_date': datetime(2023, 1, 1),
     'retries': 1,
}

start_task = EmptyOperator(
     task_id='start_task'
)


# Single function to handle both extraction and loading
def etl_process():
     hook = PostgresHook(postgres_conn_id='postgres_default')

     conn = hook.get_conn()
     df = hook.get_pandas_df("SELECT * FROM extract.delay_ext")

     hook.insert_rows(
          table='transform.delay_sfm',
          rows=df.values.tolist(),
          target_fields=df.columns.tolist()
     )

     conn.close()


with DAG(
          'delay_sfm_dag',
          default_args=default_args,
          description='Transform passenger data using pure Python',
          schedule_interval=None,
          catchup=False,
) as dag:
     transform_task = PythonOperator(
          task_id='transforming_and_loading_data',
          python_callable=etl_process
     )

end_task = EmptyOperator(
     task_id='end_task'
)

start_task >> transform_task >> end_task
