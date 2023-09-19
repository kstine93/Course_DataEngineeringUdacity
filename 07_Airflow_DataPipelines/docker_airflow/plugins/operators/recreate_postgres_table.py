import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class RecreatePostgresTable(BaseOperator):
    """Basic wrapper to drop and then recreate a table in Postgres.
    Created since Redshift (or Airflow) was raising a fuss about multi-command SQL
    with both DROP and CREATE statements.
    """

    @apply_defaults
    def __init__(self,
                 task_id,
                 postgres_conn_id,
                 table,
                 create_table_as_sql,
                 *args, **kwargs):

        super(RecreatePostgresTable, self).__init__(task_id=task_id, *args, **kwargs)
        self.table = table
        self.postgres_conn_id = postgres_conn_id
        self.create_table_as_sql = create_table_as_sql

    def execute(self, context):
        #Note: The 'context' parameter is a value passed by Airflow with the airflow context info
        # (e.g., the execution_date)
        postgres_hook = PostgresHook(self.postgres_conn_id)
        postgres_hook.run(f"DROP TABLE IF EXISTS {self.table}")
        postgres_hook.run(f"CREATE TABLE {self.table} AS {self.create_table_as_sql}")

