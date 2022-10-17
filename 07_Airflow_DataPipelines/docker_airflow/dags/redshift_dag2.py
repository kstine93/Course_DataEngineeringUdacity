#Code for setting up a simple DAG in Airflow

from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresHook, PostgresOperator

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

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
    'redshift_dag2' #Name of the DAG [REQUIRED]
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
def check_greater_than_zero(**context):
    redshift_hook = PostgresHook("redshift_connection") #ID defined when creating Airflow connection
    #Note: we are getting the table name here because it's actually ADDED to the context as part of the PythonOperator  below.
    #Not sure why we do this- maybe this is a better way in Airflow to pass variables?
    table = context["params"]["table"]
    records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
    #logging.info(records)
    
    if records is None or len(records[0]) < 1:
        logging.error(f"No records present in destination table {table}")
        raise ValueError(f"No records present in destination table {table}")
    
    logging.info(f"Check passed: table '{table}' has more than 0 records.")

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
    dag=redshift_dag,
    #This 'service-level agreement' is specifying that our code MUST run within 1 hour or an email warning will be sent.
    #Question: Is it possible to configure this behavior (e.g., Raise an error instead and halt execution)?
    sla=timedelta(hours=1)
)

check_table_records = PythonOperator(
    task_id='check_table_records'
    ,dag=redshift_dag
    ,python_callable=check_greater_than_zero
    ,params={
        'table':'stations'
    }
)
#---------------
create_table >> copy_to_redshift
copy_to_redshift >> check_table_records