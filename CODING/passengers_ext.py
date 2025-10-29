import csv
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

def insert_data_from_csv_into_postgres(**kwargs):
     pg_hook = PostgresHook(postgres_conn_id='postgres_default')
     csv_file_path = '/opt/airflow/data/passengers_data.csv'

     insert_query = """
    INSERT INTO PASSENGERS_EXT (First_Name, Last_Name, Gender, DOB, Age_Group, 
                                Passenger_Type, Flight_ID, Payment_Method, Travel_Class, 
                                Meal_Preference, Total_Fare_Amount, Reward_Points, 
                                Service_Quality_Feedback, Cleanliness_Feedback, 
                                Timeliness_Feedback, Overall_Experience_Feedback, 
                                Luggage_Status, Luggage_Weight, Meal_Feedback, 
                                Gate_Location_Feedback, Other_Services_Feedback, 
                                Before_Boarding_Services_Feedback)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """

     check_query = """
    SELECT 1 FROM PASSENGERS_EXT 
    WHERE First_Name = %s AND Last_Name = %s AND DOB = %s AND Flight_ID = %s;
    """

     with open(csv_file_path, mode='r') as file:
          reader = csv.reader(file)
          next(reader)  # Skip header
          for row in reader:
               if len(row) < 22:
                    continue

               first_name = row[0]
               last_name = row[1]
               dob = row[3]
               flight_id = row[6]

               result = pg_hook.get_records(check_query, parameters=(first_name, last_name, dob, flight_id))
               if not result:
                    pg_hook.run(insert_query, parameters=row)


default_args = {
     'owner': 'airflow',
     'retries': 1,
     'retry_delay': 300,
}

with DAG(
          'passengers_ext_dag',
          default_args=default_args,
          description='A DAG to insert new data from CSV into PostgreSQL, skipping existing records',
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