import pendulum
import logging

from airflow.decorators import dag, task

@dag(
    schedule_interval='@hourly',
    start_date=pendulum.now()
)
def task_dependencies():

    @task()
    def hello_world():
        logging.info("Hello World")

    @task()
    def addition(first,second):
        logging.info(f"{first} + {second} = {first+second}")
        return first+second

    @task()
    def subtraction(first,second):
        logging.info(f"{first -second} = {first-second}")
        return first-second

    @task()
    def division(first,second):
        logging.info(f"{first} / {second} = {int(first/second)}")
        return int(first/second)


    res_hello_world = hello_world()
    res_addition = addition(first=3,second=64)
    res_subtraction = subtraction(first=95, second=103)
    res_division = division(first=res_addition, second=res_subtraction)

    #Dependency defintion:
    res_hello_world >> res_addition
    res_hello_world >> res_subtraction

    res_addition >> res_division
    res_subtraction >> res_division
# TODO: call the hello world task function
# TODO: call the addition function with some constants (numbers)
# TODO: call the subtraction function with some constants (numbers)
# TODO: call the division function with some constants (numbers)
# TODO: create the dependency graph for the first three tasks
# TODO: Configure the task dependencies such that the graph looks like the following:
#
#                    ->  addition_task
#                   /                 \
#   hello_world_task                   -> division_task
#                   \                 /
#                    ->subtraction_task

#  TODO: assign the result of the addition function to a variable
#  TODO: assign the result of the subtraction function to a variable
#  TODO: pass the result of the addition function, and the subtraction functions to the division function
# TODO: create the dependency graph for the last three tasks


task_dependencies_dag=task_dependencies()