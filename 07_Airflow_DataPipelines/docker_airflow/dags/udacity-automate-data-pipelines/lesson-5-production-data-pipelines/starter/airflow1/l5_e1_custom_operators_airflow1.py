#Instructions
#In this exercise, we’ll consolidate repeated code into Operator Plugins
#1 - Replace both uses of the PythonOperator with the HasRowsOperator custom operator
#2 - Execute the DAG

# Remember to run "/opt/airflow/start.sh" command to start the web server. Once the Airflow web server is ready,  open the Airflow UI using the "Access Airflow" button. Turn your DAG “On”, and then Run your DAG. If you get stuck, you can take a look at the solution file in the workspace/airflow/dags folder in the workspace and the video walkthrough on the next page.

import datetime
import logging

from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook

from custom_operators.s3_to_redshift import S3ToRedshiftOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator


from udacity.common import sql_statements_new


#
# TODO: Replace the data quality checks with the HasRowsOperator
#
def check_greater_than_zero(*args, **kwargs):
    table = kwargs["params"]["table"]
    redshift_hook = PostgresHook("redshift")
    records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
    if len(records) < 1 or len(records[0]) < 1:
        raise ValueError(f"Data quality check failed. {table} returned no results")
    num_records = records[0][0]
    if num_records < 1:
        raise ValueError(f"Data quality check failed. {table} contained 0 rows")
    logging.info(f"Data quality on table {table} check passed with {records[0][0]} records")


dag = DAG(
    "demonstrating_custom_operators_legacy",
    start_date=datetime.datetime(2018, 1, 1, 0, 0, 0, 0),
    end_date=datetime.datetime(2018, 12, 1, 0, 0, 0, 0),
    schedule_interval="@monthly",
    max_active_runs=1
)

create_trips_table = PostgresOperator(
    task_id="create_trips_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sql_statements_new.CREATE_TRIPS_TABLE_SQL
)

copy_trips_task = S3ToRedshiftOperator(
    task_id="load_trips_from_s3_to_redshift",
    dag=dag,
    table="trips",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="sean-murdock",
    s3_key="data-pipelines/divvy/partitioned/{execution_date.year}/{execution_date.month}/divvy_trips.csv"
)

#
# TODO: Replace this data quality check with the HasRowsOperator
#
check_trips = PythonOperator(
    task_id='check_trips_data',
    dag=dag,
    python_callable=check_greater_than_zero,
    provide_context=True,
    params={
        'table': 'trips',
    }
)

create_stations_table = PostgresOperator(
    task_id="create_stations_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sql_statements_new.CREATE_STATIONS_TABLE_SQL,
)

copy_stations_task = S3ToRedshiftOperator(
    task_id="load_stations_from_s3_to_redshift",
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="sean-murdock",
    s3_key="data-pipelines/divvy/unpartitioned/divvy_stations_2017.csv",
    table="stations"
)

#
# TODO: Replace this data quality check with the HasRowsOperator
#
check_stations = PythonOperator(
    task_id='check_stations_data',
    dag=dag,
    python_callable=check_greater_than_zero,
    provide_context=True,
    params={
        'table': 'stations',
    }
)

create_trips_table >> copy_trips_task
create_stations_table >> copy_stations_task
copy_stations_task >> check_stations
copy_trips_task >> check_trips
