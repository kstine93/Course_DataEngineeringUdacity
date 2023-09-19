import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class HasRowsOperator(BaseOperator):
    """Note from Kevin:
    Here's a custom operator. Custom operators are just classes that extend the BaseOperator.
    There are some requirements to allow Airflow to properly use them though:
    1. Must have an 'execute' method. This is what Airflow calls to actually invoke the operator.
    2. Parent operator must be instantiated with 'super'
    """

    @apply_defaults
    def __init__(self,
                 task_id,
                 redshift_conn_id,
                 table,
                 *args, **kwargs):

        super(HasRowsOperator, self).__init__(task_id=task_id, *args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        #Note: The 'context' parameter is a value passed by Airflow with the airflow context info
        # (e.g., the execution_date)
        redshift_hook = PostgresHook(self.redshift_conn_id)
        records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {self.table}")
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f"Data quality check failed. {self.table} returned no results")
        num_records = records[0][0]
        if num_records < 1:
            raise ValueError(f"Data quality check failed. {self.table} contained 0 rows")
        logging.info(f"Data quality on table {self.table} check passed with {records[0][0]} records")

