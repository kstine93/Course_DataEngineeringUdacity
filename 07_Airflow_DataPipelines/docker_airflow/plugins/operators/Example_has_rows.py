import logging

from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresHook, PostgresOperator
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

#All airflow operators inherit from BaseOperator
class HasRowsOperator(BaseOperator):

    #Initializing this operator is just about storing user-provided data as part of the instance
    def __init__(self,
                 redshift_conn_id,
                 table,
                 *args,
                 **kwargs):

        super().__init__(*args,**kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id

    #The 'execute' step (custom Airflow Operator function) is what will be called when Airflow runs your DAG.
    def execute(self,context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        records = PostgresHook.get_records(f"SELECT COUNT(*) FROM {self.table}")
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f"Data quality check FAILED: {self.table} returned no results")
        num_records = records[0][0]
        if num_records < 1:
            raise ValueError(f"Data quality check FAILED: {self.table} contains 0 rows")
        logging.info(f"Data Quality check SUCCEEDED: more than 0 rows in {self.table} ({num_records} rows found)")
