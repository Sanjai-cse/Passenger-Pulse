from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import pandas as pd
import re

default_args = {
     'owner': 'airflow',
     'depends_on_past': False,
     'start_date': datetime(2023, 1, 1),
     'retries': 1,
}


start_task = EmptyOperator(
     task_id='start_task'
)

def clean_operator_contact(contact):
     if pd.isna(contact) or contact.strip() == "":
          return None
     contact = contact.split('x')[0]
     contact = re.sub(r'\D', '', contact)
     return f"+{contact}" if contact else None


def extract_operation_data():
     hook = PostgresHook(postgres_conn_id='postgres_default')
     return hook.get_pandas_df('SELECT * FROM "extract"."operations_ext"')


def transform_operation_data(df):
     df['operator_contact'] = df['operator_contact'].apply(clean_operator_contact)
     return df.where(pd.notnull(df), None)


def load_operation_data(df):
     hook = PostgresHook(postgres_conn_id='postgres_default')
     hook.insert_rows(
          table='transform.operations_sfm',
          rows=df.values.tolist(),
          target_fields=df.columns.tolist()
     )


def etl_process():
     raw_df = extract_operation_data()

     transformed_df = transform_operation_data(raw_df)

     load_operation_data(transformed_df)


with DAG(
          'operation_sfm_dag',
          default_args=default_args,
          description='Transform operations data',
          schedule_interval=None,
          catchup=False,
) as dag:
     transform_task = PythonOperator(
          task_id='transforming_and_loading_data',
          python_callable=etl_process,
     )

end_task = EmptyOperator(
     task_id='end_task'
)

start_task>>transform_task>>end_task