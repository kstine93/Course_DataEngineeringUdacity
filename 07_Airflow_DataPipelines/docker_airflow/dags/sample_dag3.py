#Code for setting up a simple DAG in Airflow

from datetime import datetime, timedelta
import logging
import json

from airflow import DAG
from airflow.operators.python import PythonOperator

from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
#-------------------

var_json = {
    "s3_bucket":"udacity-dend",
    "s3_prefix":"data-pipelines"
}

#Note: It's VERY important to serialize the json - otherwise the dict will be improperly imported as a string
#and can't be correctly parsed coming back out
Variable.set('s3_config', var_json, serialize_json=True)

dag = DAG(
    'myFourthDag' #Name of the DAG [REQUIRED]
    ,start_date=datetime.now() #Start date of DAG [REQUIRED]
    ,description="testing a first DAG and seeing how it works"
    ,schedule_interval='@monthly'
)

def list_s3_keys():
    hook = S3Hook(aws_conn_id='aws_credentials')
    config = Variable.get('s3_config',deserialize_json=True)
    bucket = config["s3_bucket"]
    prefix = config["s3_prefix"]
    logging.info(f"listing keys from {bucket}/{prefix}")
    keys = hook.list_keys(bucket,prefix=prefix)
    for key in keys:
        logging.info(f"- s3://{bucket}/{key}")

complete_task = PythonOperator(
    task_id = "list_s3_keys",
    python_callable=list_s3_keys,
    dag=dag
)

#-------------------
dag_5 = DAG(
    'myFifthDag' #Name of the DAG [REQUIRED]
    ,start_date=datetime.now()-timedelta(3) #Start date of DAG [REQUIRED]
    ,description="testing a first DAG and seeing how it works"
    ,schedule_interval='@daily'
)

def log_dag_execution_date(**context):
    #Note: for some context variables, it might be necessary to use context.get('')
    logging.info(f"--- Execution Date: {context['execution_date']} ---")

list_dates = PythonOperator(
    task_id = "list_dates",
    python_callable=log_dag_execution_date,
    dag=dag_5
)