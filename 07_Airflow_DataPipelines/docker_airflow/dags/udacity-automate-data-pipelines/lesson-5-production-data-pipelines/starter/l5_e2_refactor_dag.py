#Instructions
#In this exercise, we’ll refactor a DAG with a single overloaded task into a DAG with several tasks with well-defined boundaries
#1 - Read through the DAG and identify points in the DAG that could be split apart
#2 - Split the DAG into multiple tasks
#3 - Add necessary dependency flows for the new tasks
#4 - Run the DAG

# Remember to run "/opt/airflow/start.sh" command to start the web server. Once the Airflow web server is ready,  open the Airflow UI using the "Access Airflow" button. Turn your DAG “On”, and then Run your DAG. If you get stuck, you can take a look at the solution file in the workspace/airflow/dags folder in the workspace and the video walkthrough on the next page.

import pendulum
import logging

from airflow.decorators import dag, task
from airflow.hooks.postgres_hook import PostgresHook

from airflow.operators.postgres_operator import PostgresOperator
from operators.recreate_postgres_table import RecreatePostgresTable


@dag (
    start_date=pendulum.now()
)
def demonstrating_refactoring():

#
# TODO: Finish refactoring this function into the appropriate set of tasks,
#       instead of keeping this one large task.
#


    recreate_younger_riders_table_task = RecreatePostgresTable(
        task_id="recreate_younger_riders_table",
        postgres_conn_id = "redshift_connection",
        table = "younger_riders",
        create_table_as_sql = "SELECT * FROM trips WHERE birthyear > 2000"
    )

    @task()
    def check_youngest_rider(*args, **kwargs):
        redshift_hook = PostgresHook("redshift_connection")

        # Find all trips where the rider was under 18
        # redshift_hook.run("""
        #     BEGIN;
        #     DROP TABLE IF EXISTS younger_riders;
        #     CREATE TABLE younger_riders AS (
        #         SELECT * FROM trips WHERE birthyear > 2000
        #     );
        #     COMMIT;
        # """)
        records = redshift_hook.get_records("""
            SELECT birthyear FROM younger_riders ORDER BY birthyear DESC LIMIT 1
        """)
        if len(records) > 0 and len(records[0]) > 0:
            logging.info(f"Youngest rider was born in {records[0][0]}")

        # # Find out how often each bike is ridden
        # redshift_hook.run("""
        #     BEGIN;
        #     DROP TABLE IF EXISTS lifetime_rides;
        #     CREATE TABLE lifetime_rides AS (
        #         SELECT bikeid, COUNT(bikeid)
        #         FROM trips
        #         GROUP BY bikeid
        #     );
        #     COMMIT;
        # """)

        # Count the number of stations by city
        # redshift_hook.run("""
        #     BEGIN;
        #     DROP TABLE IF EXISTS city_station_counts;
        #     CREATE TABLE city_station_counts AS(
        #         SELECT city, COUNT(city)
        #         FROM stations
        #         GROUP BY city
        #     );
        #     COMMIT;
        # """)

    recreate_city_station_counts_table_task = RecreatePostgresTable(
        task_id="recreate_city_station_counts_table",
        postgres_conn_id = "redshift_connection",
        table = "city_station_counts",
        create_table_as_sql = """
                SELECT city, COUNT(city)
                FROM stations
                GROUP BY city
        """
    )

    recreate_lifetime_rides_table_task = RecreatePostgresTable(
        task_id="recreate_lifetime_rides_table",
        postgres_conn_id = "redshift_connection",
        table = "lifetime_rides",
        create_table_as_sql = """
                SELECT bikeid, COUNT(bikeid)
                FROM trips
                GROUP BY bikeid
        """
    )


    # create_oldest_task = PostgresOperator(
    #     task_id="create_oldest",
    #     sql="""
    #         BEGIN;
    #         DROP TABLE IF EXISTS older_riders;
    #         CREATE TABLE older_riders AS (
    #             SELECT * FROM trips WHERE birthyear > 0 AND birthyear <= 1945
    #         );
    #         COMMIT;
    #     """,
    #     postgres_conn_id="redshift_connection"
    # )

    recreate_older_riders_table_task = RecreatePostgresTable(
        task_id="recreate_older_riders_table",
        postgres_conn_id = "redshift_connection",
        table = "older_riders",
        create_table_as_sql = "SELECT * FROM trips WHERE birthyear > 0 AND birthyear <= 1945"
    )


    @task()
    def check_oldest():
        redshift_hook = PostgresHook("redshift_connection")
        records = redshift_hook.get_records("""
            SELECT birthyear FROM older_riders ORDER BY birthyear ASC LIMIT 1
        """)
        if len(records) > 0 and len(records[0]) > 0:
            logging.info(f"Oldest rider was born in {records[0][0]}")


    check_oldest_rider_task = check_oldest()
    check_youngest_rider_task = check_youngest_rider()


    # recreate_lifetime_rides_table_task
    recreate_younger_riders_table_task >> check_youngest_rider_task
    recreate_older_riders_table_task >> check_oldest_rider_task
    # load_and_analyze >> create_oldest_task
    # create_oldest_task >> log_oldest_task

demonstrating_refactoring_dag = demonstrating_refactoring()
