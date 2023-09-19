import pendulum
import logging

from airflow.decorators import dag, task
from airflow.secrets.metastore import MetastoreBackend
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator


# Use the PostgresHook to create a connection using the Redshift credentials from Airflow
# Use the PostgresOperator to create the Trips table
# Use the PostgresOperator to run the LOCATION_TRAFFIC_SQL


from udacity.common import sql_statements_new

COPY_ALL_TRIPS_SQL = sql_statements_new.COPY_SQL.format(
    "trips",
    "s3://kstine-airflow-pipeline/pipelines/divvy/unpartitioned/divvy_trips_2018.csv"
)

DROP_LOCATION_TRAFFIC_SQL = "DROP TABLE IF EXISTS station_traffic;"

LOCATION_TRAFFIC_SQL = """
CREATE TABLE station_traffic AS
SELECT
    DISTINCT(t.from_station_id) AS station_id,
    t.from_station_name AS station_name,
    num_departures,
    num_arrivals
FROM trips t
JOIN (
    SELECT
        from_station_id,
        COUNT(from_station_id) AS num_departures
    FROM trips
    GROUP BY from_station_id
) AS fs ON t.from_station_id = fs.from_station_id
JOIN (
    SELECT
        to_station_id,
        COUNT(to_station_id) AS num_arrivals
    FROM trips
    GROUP BY to_station_id
) AS ts ON t.from_station_id = ts.to_station_id
"""

@dag(
    start_date=pendulum.now()
)
def load_data_to_redshift():


    @task
    def load_task():
        metastoreBackend = MetastoreBackend()
        aws_connection=metastoreBackend.get_connection("aws_credentials")
        logging.info(vars(aws_connection))
        hook = PostgresHook(
            postgres_conn_id = "redshift_connection",
        )
        hook.run(COPY_ALL_TRIPS_SQL.format(
            aws_connection.login,
            aws_connection.password
        ))


    """
    QUESTION:
    Why are we using both "PostgresHook" and "PostgresOperator" here?
    I can see that both objects connect to redshift. In the hook, we only get a connection, but
    methods built into the operator (e.g., ".run()") allow for actions to be done.

    By comparison, the PostgresOperator also takes connection information, but also an ID ("create_table")
    and SQL to be run - so it's like pre-loading a cannon ready to be fired. This allows us to define
    dependencies below.
    But why don't we just do this with the hook as well? Is there any real difference?

    PARTIAL ANSWER:
    We can see in this code that the 'load_task' task needs to be explicitly decorated and configured.
    This is nice because in this case we actually need to edit the SQL with username and PW before
    executing it. So in that way: HOOKS ARE MORE VERBOSE - THEY JUST DEFINE CONNECTION.
    On the other hand, the operator definitions below are much more succinct - they don't require
    much additional code and they're assumed to be tasks without decoration.
    >> I can see the flexibility of HOOKs and the simplicity of OPERATORS in this context.
    """
    create_table_task = PostgresOperator(
        task_id="create_table",
        postgres_conn_id = "redshift_connection",
        sql = sql_statements_new.CREATE_TRIPS_TABLE_SQL
    )

    drop_location_traffic_task= PostgresOperator(
        task_id="drop_location_traffic",
        postgres_conn_id = "redshift_connection",
        sql = DROP_LOCATION_TRAFFIC_SQL
    )

    location_traffic_task= PostgresOperator(
        task_id="calculate_location_traffic",
        postgres_conn_id = "redshift_connection",
        sql = LOCATION_TRAFFIC_SQL
    )

    load_data = load_task()

    create_table_task >> load_data
    load_data >> drop_location_traffic_task
    drop_location_traffic_task >> location_traffic_task

s3_to_redshift_dag = load_data_to_redshift()
