#Code for setting up a simple DAG in Airflow

from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator

dag = DAG(
    'myFirstDag' #Name of the DAG [REQUIRED]
    ,start_date=datetime.now() #Start date of DAG [REQUIRED]
    ,description="testing a first DAG and seeing how it works"
    ,schedule_interval='@monthly'
)

#Other schedule_intervals:
# @once  @hourly  @daily  @weekly  @monthly  @yearly  None
#
# You can also use the CRON format to specify scheduling more specifically
#
# NOTE: @daily is the default!
#
# NOTE: If the start date in the past, your DAG will run as many times as needed to 'catch up' to the current date
# (e.g., if I set the schedule as '@daily' and put the start time to 5 days ago, the DAG will run 4 times.)

def greet():
    logging.info("GENERAL KENOBI") #Note: using logger allows us to see output in Airflow's logs (better than print)

greet_task = PythonOperator(
    task_id = "myFirstTask",
    python_callable=greet,
    dag=dag
)

dag2 = DAG(
    'mySecondDag' #Name of the DAG [REQUIRED]
    ,start_date=datetime.now()-timedelta(180) #Start date of DAG [REQUIRED]
    ,description="testing a first DAG and seeing how it works"
    ,schedule_interval='@monthly'
)

greet_task2 = PythonOperator(
    task_id = "myFirstTask",
    python_callable=greet,
    dag=dag2
)