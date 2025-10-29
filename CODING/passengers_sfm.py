from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import pandas as pd

default_args = {
     'owner': 'airflow',
     'depends_on_past': False,
     'start_date': datetime(2023, 1, 1),
     'retries': 1,
}

FEEDBACK_MAPPING = {
     'Poor': 1,
     'Average': 2,
     'Good': 3,
     'Excellent': 4
}

start_task = EmptyOperator(
     task_id='start_task'
)

def extract_data():
     hook = PostgresHook(postgres_conn_id='postgres_default')
     conn = hook.get_conn()
     df = hook.get_pandas_df("SELECT * FROM extract.passengers_ext")
     conn.close()
     return df


def transform_data(df):
     df['dob'] = pd.to_datetime(df['dob']).dt.date

     # Convert numeric columns
     df['total_fare'] = df['total_fare'].astype(float)
     df['reward_points'] = df['reward_points'].astype(int)
     df['luggage_weight'] = df['luggage_weight'].astype(float)

     feedback_columns = [
          'service_quality_feedback',
          'cleanliness_feedback',
          'timeliness_feedback',
          'overall_experience_feedback',
          'meal_feedback',
          'gate_location_feedback',
          'other_services_feedback',
          'before_boarding_services_feedback'
     ]

     for col in feedback_columns:
          df[col] = df[col].map(FEEDBACK_MAPPING)

     return df


def load_data(df):
     hook = PostgresHook(postgres_conn_id='postgres_default')
     conn = hook.get_conn()

     hook.insert_rows(
          table='transform.passengers_sfm',
          rows=df.values.tolist(),
          target_fields=df.columns.tolist()
     )
     conn.close()


def etl_process():
     raw_df = extract_data()

     transformed_df = transform_data(raw_df)

     load_data(transformed_df)

end_task = EmptyOperator(
     task_id='end_task'
)

with DAG(
          'passengers_sfm_dag',
          default_args=default_args,
          description='Transform passenger data using pure Python',
          schedule_interval=None,
          catchup=False,
) as dag:
     transform_task = PythonOperator(
          task_id='transforming_and_loading_data',
          python_callable=etl_process
     )

start_task>>transform_task>>end_task