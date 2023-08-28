#Code for setting up a simple DAG in Airflow

from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator

from airflow.models import Variable
#-------------------


#Create variables in DAGs - maybe an easier way than using the command line
var_json = {"test_key":"test_val"}
Variable.set('MyVar',var_json)


dag = DAG(
    'myThirdDag' #Name of the DAG [REQUIRED]
    ,start_date=datetime.now() #Start date of DAG [REQUIRED]
    ,description="testing a first DAG and seeing how it works"
    ,schedule_interval='@monthly'
)

def greet():
    logging.info("Hello There!")
    #Note: using logger allows us to see output in Airflow's logs (better than print)

greet_task = PythonOperator(
    task_id = "greet_task",
    python_callable=greet,
    dag=dag
)

def farewell():
    logging.info("Goodbye")

farewell_task = PythonOperator(
    task_id = "farewell_task",
    python_callable=farewell,
    dag=dag
)

complete_task = PythonOperator(
    task_id = "complete_task",
    python_callable=lambda: logging.info("We're Done!")
)

#We can programmatically denote task dependencies using ">>" and "<< operators"
# greet_task comes before farewell_task
greet_task >> farewell_task
farewell_task >> complete_task

#Alternatively, this does the same thing (also 'set_upstream')
greet_task.set_downstream(farewell_task)
complete_task.set_upstream(farewell_task)