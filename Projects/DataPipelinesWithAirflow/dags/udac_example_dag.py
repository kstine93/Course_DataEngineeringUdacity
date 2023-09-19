import pendulum
from datetime import timedelta
import os
import logging

from airflow.decorators import dag, task, task_group
from airflow.operators.dummy_operator import DummyOperator
from operators.stage_redshift import StageToRedshiftOperator
from operators.load_fact import LoadFactOperator
from operators.load_dimension import LoadDimensionOperator
from operators.data_quality import (
    PostgresCheckNumRowsOperator,
    PostgresCheckColumnsOperator,
    PostgresCheckColumnNumericRangeOperator,
    PostgresCheckColumnNumericRangeOperator,
    PostgresCheckColumnNominalValuesOperator
)

from helpers import SqlQueries

#-------------
@dag (
    start_date=pendulum.now(),
    schedule="@hourly",
    catchup=False,
    default_args = {
        "owner":"kstine",
        "retries":3,
        "depends_on_past":False,
        "email_on_retry":False,
        "retry_delay":timedelta(minutes=1),
        "catchup":False
    }
)
def udac_example_dag():

    start_operator = DummyOperator(task_id='Begin_execution')

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='stage_events_to_redshift_task',
        redshift_conn_id='redshift_connection',
        aws_conn_id='aws_credentials',
        s3_source="s3://kstine-airflow-pipeline/project/log-data/",
        table_sink="staging_events",
        jsonpath = "s3://kstine-airflow-pipeline/project/log-data-jsonpath.json"
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='stage_songs_to_redshift_task',
        redshift_conn_id='redshift_connection',
        aws_conn_id='aws_credentials',
        s3_source="s3://kstine-airflow-pipeline/project/song-data/",
        table_sink="staging_songs"
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id='redshift_connection',
        aws_conn_id='aws_credentials',
        select_query=SqlQueries.songplay_table_insert,
        table_sink="songplays"
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id='redshift_connection',
        aws_conn_id='aws_credentials',
        select_query=SqlQueries.user_table_insert,
        table_sink="users",
        empty_table_sink_first=True
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id='redshift_connection',
        aws_conn_id='aws_credentials',
        select_query=SqlQueries.song_table_insert,
        table_sink="songs",
        empty_table_sink_first=True
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id='redshift_connection',
        aws_conn_id='aws_credentials',
        select_query=SqlQueries.artist_table_insert,
        table_sink="artists",
        empty_table_sink_first=True
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id='redshift_connection',
        aws_conn_id='aws_credentials',
        select_query=SqlQueries.time_table_insert,
        table_sink="time",
        empty_table_sink_first=True
    )

    # run_quality_checks = DataQualityOperator(
    #     task_id='Run_data_quality_checks'
    # )

    @task_group(
        default_args={"retries":0},
        tooltip="Data Quality checks for all Sparkify tables in this DAG",
    )
    def run_quality_checks():
        """Suite of data quality checks on Postgres database"""

        """Desired checks:
        NOMINAL: users.gender within ['M','F']
        NOMINAL: users.level within ['paid','free']
        NUMERIC RANGE: songs.year within 1700 - pendulum.now.year()
        - ROW COUNT: songplays between 500.000 and 10.000.000
        HAS COLUMNS time contains [start_time,hour,day,week,month,year,weekday]

        """

        check_num_rows_songplays = PostgresCheckNumRowsOperator(
            task_id = "check_num_rows_songplays",
            postgres_conn_id = "redshift_connection",
            table = "songplays",
            max_rows = 10000000,
            min_rows = 5000
        )

        check_cols_time = PostgresCheckColumnsOperator(
            task_id = "check_cols_time",
            postgres_conn_id = "redshift_connection",
            table = "time",
            col_names = [
                'start_time',
                'hour',
                'day',
                'week',
                'month',
                'year',
                'weekday'
            ]
        )

        check_range_artists_year = PostgresCheckColumnNumericRangeOperator(
            task_id = "check_range_artists_year",
            postgres_conn_id = "redshift_connection",
            table = "songs",
            column = "year",
            min_value = 0,
            max_value = pendulum.now().year
        )

        check_user_gender_values = PostgresCheckColumnNominalValuesOperator(
            task_id = "check_user_gender_values",
            postgres_conn_id = "redshift_connection",
            table = "users",
            column = "gender",
            allowed_values = ['M','F']
        )

        check_user_level_values = PostgresCheckColumnNominalValuesOperator(
            task_id = "check_user_level_values",
            postgres_conn_id = "redshift_connection",
            table = "users",
            column = "level",
            allowed_values = ['paid','free']
        )
        #-------------
        #-------------


    end_operator = DummyOperator(task_id='Stop_execution')

    run_quality_checks_task = run_quality_checks()

    #-------------
    start_operator >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table

    load_songplays_table >> [load_song_dimension_table,
                             load_user_dimension_table,
                             load_artist_dimension_table,
                             load_time_dimension_table
                             ] >> run_quality_checks_task

    run_quality_checks_task >> end_operator

#-------------
udac_example = udac_example_dag()