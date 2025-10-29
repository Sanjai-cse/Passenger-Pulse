from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def run_fact_insert():
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    sql = """
            WITH PassengerOrder AS (
              SELECT
                p.*,
                dp.passenger_sk,
                ROW_NUMBER() OVER (
                  PARTITION BY p.flight_id, p.travel_class, p.dob 
                  ORDER BY p.first_name, p.last_name, p.dob  -- Replace with actual priority column if available
                ) AS rn
              FROM transform.passengers_sfm p
              JOIN dimension.dim_passenger dp 
                ON p.first_name = dp.first_name_txt
                AND p.last_name = dp.last_name_txt
                AND p.dob = dp.dob 
            ),

            FlightDates AS (
              SELECT
                flight_id,
                scheduled_departure,
                DATE(scheduled_departure) AS flight_date,
                ROW_NUMBER() OVER (
                  PARTITION BY flight_id 
                  ORDER BY scheduled_departure
                ) AS date_order
              FROM transform.flight_sfm
            ),

            PassengerBuckets AS (
              SELECT
                *,
                CASE
                  WHEN travel_class = 'Economy' THEN ((rn - 1) / 200) + 1
                  WHEN travel_class = 'First Class' THEN ((rn - 1) / 120) + 1
                  WHEN travel_class = 'Business' THEN ((rn - 1) / 80) + 1
                END AS bucket
              FROM PassengerOrder
            )

            INSERT INTO dimension.sample_fact (
              flight_id, passenger_sk, passenger_type_txt, luggage_status_txt,
              travel_class_txt, meal_preference_txt, service_quality_feedback_nbr,
              cleanliness_feedback_nbr, timeliness_feedback_nbr, overall_experience_feedback_nbr,
              meal_feedback_nbr, gate_location_feedback_nbr, other_service_feedback_nbr,
              before_boarding_services_feedback_nbr, payment_method_txt, total_fare_amount,
              luggage_weight, etl_load_nbr, etl_loaded_date
            )
            SELECT
              pb.flight_id,
              pb.passenger_sk,
              pb.passenger_type AS passenger_type_txt,
              pb.luggage_status AS luggage_status_txt,
              pb.travel_class AS travel_class_txt,
              pb.meal_preference AS meal_preference_txt,
              pb.service_quality_feedback AS service_quality_feedback_nbr,
              pb.cleanliness_feedback AS cleanliness_feedback_nbr,
              pb.timeliness_feedback AS timeliness_feedback_nbr,
              pb.overall_experience_feedback AS overall_experience_feedback_nbr,
              pb.meal_feedback AS meal_feedback_nbr,
              pb.gate_location_feedback AS gate_location_feedback_nbr,
              pb.other_services_feedback AS other_service_feedback_nbr,
              pb.before_boarding_services_feedback AS before_boarding_services_feedback_nbr,
              pb.payment_method AS payment_method_txt,
              pb.total_fare AS total_fare_amount,
              pb.luggage_weight,
              1 AS etl_load_nbr,
              CURRENT_TIMESTAMP AS etl_loaded_date
            FROM PassengerBuckets pb
            JOIN FlightDates fd 
              ON pb.flight_id = fd.flight_id 
              AND pb.bucket = fd.date_order
            ORDER BY pb.passenger_sk ASC;
              """

    cursor.execute(sql)
    conn.commit()
    cursor.close()
    conn.close()

with DAG(
    dag_id='sample_fact_dag',
    default_args=default_args,
    description='ETL DAG to insert into fact table from CTE joins',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=['ETL', 'fact_table', 'passenger_data'],
) as dag:


    insert_fact_data = PythonOperator(
        task_id='insert_fact_table_data',
        python_callable=run_fact_insert
    )

    insert_fact_data
