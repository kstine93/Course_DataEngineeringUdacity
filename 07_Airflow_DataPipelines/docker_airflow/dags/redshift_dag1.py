#Code for setting up a simple DAG in Airflow

from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresHook, PostgresOperator

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

#Note: When you have imported a Python file with SQL statements in it, you can then reference them
#easily doing something like:
# import redshift_sql
# CREATE_USERS_TABLE
#However, this only works if this Python file is in a findable directory. Given that Airflow is running on Docker
#On my machine, I can't seem to get this to work, so specifying SQL fully here:

COPY_SQL = """
    COPY {}
    FROM '{}'
    ACCESS_KEY_ID '{{}}'
    SECRET_ACCESS_KEY '{{}}'
    IGNOREHEADER 1
    DELIMITER ','
"""

COPY_STATIONS_SQL = COPY_SQL.format(
    "stations",
    "s3://udacity-dend/data-pipelines/divvy/unpartitioned/divvy_stations_2017.csv"
)

CREATE_STATIONS_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS stations (
    id INTEGER NOT NULL,
    name VARCHAR(250) NOT NULL,
    city VARCHAR(100) NOT NULL,
    latitude DECIMAL(9, 6) NOT NULL,
    longitude DECIMAL(9, 6) NOT NULL,
    dpcapacity INTEGER NOT NULL,
    online_date TIMESTAMP NOT NULL,
    PRIMARY KEY(id))
    DISTSTYLE ALL;
"""

#---------------
redshift_dag = DAG(
    'redshift_dag1' #Name of the DAG [REQUIRED]
    ,start_date=datetime.now() #Start date of DAG [REQUIRED]
    ,description="Connecting to Redshift via Airflow"
    ,schedule_interval='@monthly'
)

#---------------
def load_data_to_redshift(**context):
    aws_hook = AwsBaseHook("aws_credentials") #ID defined when creating Airflow connection
    credentials = aws_hook.get_credentials()
    redshift_hook = PostgresHook("redshift_connection") #ID defined when creating Airflow connection
    sql_query = COPY_STATIONS_SQL.format(credentials.access_key,credentials.secret_key)
    redshift_hook.run(sql_query)

#---------------
create_table = PostgresOperator(
    task_id = "create_table",
    postgres_conn_id="redshift_connection", #ID defined when creating Airflow connection
    sql = CREATE_STATIONS_TABLE_SQL,
    dag=redshift_dag
)

copy_to_redshift = PythonOperator(
    task_id = 'copy_to_redshift',
    python_callable=load_data_to_redshift,
    dag=redshift_dag
)

#---------------
create_table >> copy_to_redshift