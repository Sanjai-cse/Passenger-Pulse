from airflow.providers.postgres.hooks.postgres import PostgresHook
from io import StringIO
import pandas as pd
from datetime import datetime
from contextlib import closing


def transform_and_load_operations():
     hook = PostgresHook(postgres_conn_id='postgres_default')
     conn = hook.get_conn()

     source_table = 'transform.operations_sfm'
     target_table = 'dimension.dim_operations'

     columns_mapping = {
          'source_columns': [
               'OPERATION_ID',
               'FLIGHT_ID',
               'AIRPORT_CODE',
               'OPERATION_TYPE',
               'OPERATION_TIME',
               'OPERATOR_CONTACT',
               'OPERATOR_NAME'
          ],
          'target_columns': [
               'OPERATION_ID_TXT',
               'FLIGHT_ID_TXT',
               'AIRPORT_CODE_TXT',
               'OPERATION_TYPE_TXT',
               'OPERATION_TIME',
               'OPERATOR_CONTACT_TXT',
               'OPERATOR_NAME_TXT',
               'ETL_LOAD_NBR',
               'ETL_LOADED_DATE',
               'FILENAME_TXT'
          ]
     }

     with closing(conn.cursor()) as cursor:
          # Build dynamic insert statement
          select_columns = ', '.join(columns_mapping['source_columns'])
          insert_columns = ', '.join(columns_mapping['target_columns'])
          static_values = " 1, CURRENT_TIMESTAMP, 'Operations_data'"

          cursor.execute(
               f"INSERT INTO {target_table} ({insert_columns}) "
               f"SELECT {select_columns}, {static_values} "
               f"FROM {source_table}"
          )
          conn.commit()


def transform_and_load_passengers():
     """Data transformation and loading logic"""
     hook = PostgresHook(postgres_conn_id='postgres_default')
     conn = hook.get_conn()

     source_table = 'transform.passengers_sfm'
     target_table = 'dimension.dim_passenger'

     columns_mapping = {
          'source_columns': [
               'first_name',
               'last_name',
               'gender',
               'dob',
               'reward_points'
          ],
          'target_columns': [
               'first_name_txt',
               'last_name_txt',
               'gender_txt',
               'dob',
               'reward_points',
               'etl_load_nbr',
               'etl_loaded_date',
               'filename_txt'
          ]
     }

     with closing(conn.cursor()) as cursor:
          # Build dynamic insert statement
          select_columns = ', '.join(columns_mapping['source_columns'])
          insert_columns = ', '.join(columns_mapping['target_columns'])
          static_values = " 1, CURRENT_TIMESTAMP, 'Passengers_data'"

          cursor.execute(
               f"INSERT INTO {target_table} ({insert_columns}) "
               f"SELECT {select_columns}, {static_values} "
               f"FROM {source_table}"
          )
          conn.commit()

def transform_and_load_flight():
     source_hook = PostgresHook(postgres_conn_id='postgres_default')
     dest_hook = PostgresHook(postgres_conn_id='postgres_default')

     # Extract from source
     flight_data = source_hook.get_pandas_df("SELECT * FROM transform.flight_sfm")
     delay_mapping = source_hook.get_pandas_df("SELECT * FROM transform.delay_sfm")

     # Merge + transform
     merged_data = pd.merge(flight_data, delay_mapping, on='delay_code', how='left')

     final_data = merged_data[[
          'flight_id', 'airline', 'source_airport_code', 'destination_airport_code',
          'status', 'delay_code', 'description', 'scheduled_departure', 'scheduled_arrival',
          'travel_duration_hours'
     ]].rename(columns={
          'flight_id': 'flight_id_txt',
          'airline': 'airline_txt',
          'source_airport_code': 'source_airport_code_txt',
          'destination_airport_code': 'destination_airport_code_txt',
          'status': 'status_txt',
          'delay_code': 'delay_code_txt',
          'description': 'description_txt',
          'travel_duration_hours': 'travel_duration_hours_dc'
     })

     final_data['date'] = pd.to_datetime(final_data['scheduled_departure']).dt.date

     # ETL metadata
     final_data['etl_load_nbr'] = 1
     final_data['etl_loaded_date'] = datetime.now()
     final_data['filename_txt'] = 'flight_data, delay_reasons_data'

     # Buffer to CSV
     buffer = StringIO()
     final_data.to_csv(buffer, index=False, header=False)
     buffer.seek(0)

     # Load into Postgres
     dest_conn = dest_hook.get_conn()
     cursor = dest_conn.cursor()
     cursor.copy_expert(
          sql="""
        COPY dimension.dim_flight (
            flight_id_txt, airline_txt, source_airport_code_txt, destination_airport_code_txt,
            status_txt, delay_code_txt, description_txt, scheduled_departure, scheduled_arrival,
            travel_duration_hours_dc, date, etl_load_nbr, etl_loaded_date, filename_txt
        )
        FROM STDIN WITH CSV
        """,
          file=buffer
     )
     dest_conn.commit()

