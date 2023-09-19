# Remember to run "/opt/airflow/start.sh" command to start the web server. Once the Airflow web server is ready,  open the Airflow UI using the "Access Airflow" button. Turn your DAG “On”, and then Run your DAG. If you get stuck, you can take a look at the solution file in the workspace/airflow/dags folder in the workspace and the video walkthrough on the next page.

import pendulum

from airflow.decorators import dag,task, task_group

# from custom_operators.facts_calculator import FactsCalculatorOperator
# from custom_operators.has_rows import HasRowsOperator
# from custom_operators.s3_to_redshift import S3ToRedshiftOperator
from airflow.operators.empty import EmptyOperator
from udacity_operators.s3_to_redshift import S3ToRedshiftOperator
from udacity_operators.facts_calculator import FactsCalculatorOperator
from udacity_operators.has_rows import HasRowsOperator
from operators.data_quality_checks_postgres import (
    PostgresCheckNumRows,
    PostgresCheckColumns,
    PostgresCheckColumnNumericRange,
    PostgresCheckColumnNominalValues
)

#
# TODO: Create a DAG which performs the following functions:
#
#       1. Loads Trip data from S3 to RedShift
#       2. Performs a data quality check on the Trips table in RedShift
#           2a. TODO: Look at Trips table in S3 / Redshift to see what an actually GOOD data quality check would be
#               (maybe not just # of rows)https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html#taskgroups
#           2b. TODO: Look into TASK GROUPS - I want a suite of interchangeable atomic tests that are grouped in a
#               'data quality check group' on the DAG level
#               https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html#taskgroups
#           2c. Data Quality check ideas: # of rows equals origin (no values dropped or added),
#               outliers for numeric values, check unique for nominal values within allowed list,
#               check NULL or like-empty (e.g., "-") frequencies
#       3. Uses the FactsCalculatorOperator to create a Facts table in Redshift
#           a. **NOTE**: to complete this step you must complete the FactsCalcuatorOperator
#              skeleton defined in plugins/operators/facts_calculator.py
#
@dag(start_date=pendulum.now())
def full_pipeline():

    load_trips_from_s3_to_redshift_task = S3ToRedshiftOperator(
        task_id="load_trips_from_s3_to_redshift",
        redshift_conn_id="redshift_connection",
        aws_credentials_id="aws_credentials",
        s3_bucket="kstine-airflow-pipeline",
        s3_key="pipelines/divvy/unpartitioned/divvy_trips_2018.csv",
        table="trips"
    )


#
# TODO: Perform a data quality check on the Trips table
#
    @task_group()
    def trips_data_quality_check():
        """
        NOTES: Can I make this data quality check generic? Maybe yes, maybe no...
            1. I think there are some data quality checks that ARE purely generic:
                1a. does table exist?
                    1a1. Arguably doesn't need to be checked - this is inherent in all other checks.
                1b. count of rows > 0?
                    1b1. This is already covered by '# of rows' check below. Wegwerfen.
            2. There are also some data quality checks that require SOME minor configuration:
                2a. Is # of rows within given inclusive range (low:int, high:int)?
                2b. Is # of columns within given inclusive range (low:int, high:int)?
                2c. Are all numeric values in a given column within given inclusive range (col:str, low:int, high:int)?
                2d. Are all string values in a given column within given list (col:str, allow_list:list[str])?
            3. There are some data quality checks that require LOTS of configuration and should not be standardized:
                3a. Are values in column A within a certain range if column B has value "X"?
        This all points to that I really need a suite of data quality checks that accept some MINOR configuration.
        How can I best do this?
            1. Let's take a look at the 'check_greater_than_zero' custom operator we already have -
                this accepts parameters and we can easily extend this to include more parameters.
            2. We can even configure our task_group to have some default parameters (e.g., table name, connection_ref)
                to further reduce the need for manual configuration.
            3. I think we will always need to manually set up the task group for data quality checks, because
                I can't foresee a good situation where we have a 'standard' set of quality checks - they all need
                configuration and I want that to be deliberate. Let's make the data quality check options obvious
                and provide examples, but let people set up their own custom quality check suites always.
        """
        # EXAMPLE:
        check_trips_task = HasRowsOperator(
                 task_id = "check_trips",
                 redshift_conn_id = "redshift_connection",
                 table = "trips",
        )

        check_num_rows_task = PostgresCheckNumRows(
            task_id = "check_num_rows",
            postgres_conn_id = "redshift_connection",
            table = "trips",
            max_rows = 200000,
            min_rows = 1
        )

        check_cols_task = PostgresCheckColumns(
            task_id = "check_cols",
            postgres_conn_id = "redshift_connection",
            table = "trips",
            col_names = [
                'birthyear',
                'gender',
                'usertype',
                'to_station_name',
                'to_station_id',
                'from_station_name',
                'from_station_id',
                'tripduration',
                'bikeid',
                'end_time',
                'start_time',
                'trip_id'
            ]
        )

        check_birthyear_range_task = PostgresCheckColumnNumericRange(
            task_id = "check_birthyear_range",
            postgres_conn_id = "redshift_connection",
            table = "trips",
            column = "birthyear",
            min_value = pendulum.now().year - 120,
            max_value = pendulum.now().year - 5
        )

        # ASSUMING tripduration is in seconds, we'll set a max trip length of 36 hours (1.5 days)
        maxTripSeconds = (36 * 24 * 60 * 60)
        check_tripduration_range_task = PostgresCheckColumnNumericRange(
            task_id = "check_tripduration_range",
            postgres_conn_id = "redshift_connection",
            table = "trips",
            column = "tripduration",
            min_value = 5,
            max_value = maxTripSeconds
        )


        check_usertype_values_task = PostgresCheckColumnNominalValues(
            task_id = "check_usertype_values",
            postgres_conn_id = "redshift_connection",
            table = "trips",
            column = "usertype",
            allowed_values = ['Subscriber','Customer']
        )

        check_gender_values_task = PostgresCheckColumnNominalValues(
            task_id = "check_gender_values",
            postgres_conn_id = "redshift_connection",
            table = "trips",
            column = "gender",
            allowed_values = ['Male','Female','']
        )


    create_fact_table_task = FactsCalculatorOperator(
        task_id = "create_fact_table",
        redshift_conn_id="redshift_connection",
        origin_table="trips",
        destination_table="fact_tripduration",
        fact_column="tripduration",
        groupby_column="bikeid",
    )

    trips_data_quality_check_task = trips_data_quality_check()
    load_trips_from_s3_to_redshift_task >> trips_data_quality_check_task
    load_trips_from_s3_to_redshift_task >> create_fact_table_task



full_pipeline_dag = full_pipeline()