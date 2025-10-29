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

start_task = EmptyOperator(
     task_id='start_task'
)

def extract_flight_data():
     hook = PostgresHook(postgres_conn_id='postgres_default')
     conn = hook.get_conn()
     df = hook.get_pandas_df("SELECT * FROM extract.flight_ext")
     conn.close()
     return df


def transform_flight_data(df):
    date_format = '%d-%m-%Y %H:%M'

    for col in ['scheduled_departure', 'scheduled_arrival']:
        df[col] = pd.to_datetime(
            df[col],
            format=date_format,
            errors='coerce'
        )
        df[col] = df[col].apply(lambda x: x if pd.notnull(x) else None)
        df[col] = df[col].astype(object)

    df['travel_duration_hours'] = pd.to_numeric(
        df['travel_duration_hours'].replace('', pd.NA),
        errors='coerce'
    )
    df['travel_duration_hours'] = df['travel_duration_hours'].apply(
        lambda x: x if pd.notnull(x) else None
    )

    return df


def load_flight_data(df):
     hook = PostgresHook(postgres_conn_id='postgres_default')
     conn = hook.get_conn()
     cursor = conn.cursor()

     columns = df.columns.tolist()
     values_placeholder = ','.join(['%s'] * len(columns))
     insert_sql = f"""
        INSERT INTO transform.flight_sfm 
        ({','.join(columns)})
        VALUES ({values_placeholder})
    """

     data = [tuple(None if pd.isna(x) else x for x in record)
             for record in df.to_records(index=False)]

     cursor.executemany(insert_sql, data)
     conn.commit()

     cursor.close()
     conn.close()

def etl_process():
     raw_df = extract_flight_data()

     transformed_df = transform_flight_data(raw_df)

     load_flight_data(transformed_df)

end_task = EmptyOperator(
     task_id='end_task'
)

with DAG(
          'flight_sfm_dag',
          default_args=default_args,
          description='Transform flight data using Python',
          schedule_interval=None,
          catchup=False,
) as dag:
     transform_task = PythonOperator(
          task_id='transforming_and_loading_data',
          python_callable=etl_process
     )

start_task>>transform_task>>end_task