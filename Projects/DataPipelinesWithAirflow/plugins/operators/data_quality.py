"""
Suite of data-checking operators intended to run against a Postgres database.
"""

import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

#---------------------
def psycopg2ToFlatList(result:list) -> list:
    """Intended for 1-column SQL results from Psycopg2, which
    are returned by default as a list of tuple rows. This transforms
    this structure into one flattened list"""
    return [row[0] for row in result]

#---------------------
class PostgresCheckNumRowsOperator(BaseOperator):
    """Checks whether the given Postgres table has a row count in
    between the given maximum and minimum values.
    """
    @apply_defaults
    def __init__(self,
                 task_id,
                 postgres_conn_id,
                 table,
                 max_rows,
                 min_rows=1,
                 *args, **kwargs):

        super(PostgresCheckNumRowsOperator, self).__init__(task_id=task_id, *args, **kwargs)
        self.table = table
        self.postgres_conn_id = postgres_conn_id
        self.max_rows = max_rows
        self.min_rows = min_rows

    def execute(self, context):
        #Note: The 'context' parameter is a value passed by Airflow with the airflow context info
        # (e.g., the execution_date)
        postgres_hook = PostgresHook(self.postgres_conn_id)
        records = postgres_hook.get_records(f"SELECT COUNT(*) FROM {self.table}")

        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f"Data quality check failed. {self.table} returned no results")

        num_records = records[0][0]
        if num_records < self.min_rows:
            raise ValueError(f"FAIL: {self.table} has {num_records} rows -- less than minimum expected {self.min_rows} rows.")
        if num_records > self.max_rows:
            raise ValueError(f"FAIL: {self.table} has {num_records} rows -- more than maximum expected {self.max_rows} rows.")

        logging.info(f"PASS: Table {self.table} has {num_records} rows: within '{self.min_rows} - {self.max_rows}' expected range")


#---------------------
class PostgresCheckColumnsOperator(BaseOperator):
    """Checks whether the given Postgres table has exactly the expected columns
    """
    @apply_defaults
    def __init__(self,
                 task_id:str,
                 postgres_conn_id:str,
                 table:str,
                 col_names:list,
                 *args, **kwargs):

        super(PostgresCheckColumnsOperator,self).__init__(task_id=task_id, *args, **kwargs)
        self.table = table
        self.postgres_conn_id = postgres_conn_id
        self.col_names = col_names

    def execute(self, context):
        postgres_hook = PostgresHook(self.postgres_conn_id)
        records = postgres_hook.get_records(f"SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = '{self.table}'")

        table_cols = psycopg2ToFlatList(records)

        inTableButNotInSchema = set(table_cols) - set(self.col_names)
        inSchemaButNotInTable = set(self.col_names) - set(table_cols)

        if len(inTableButNotInSchema) > 0:
            raise ValueError(f"FAIL: Table '{self.table}' had unexpected extra column(s) {inTableButNotInSchema}. Expected columns: {self.col_names}")

        if len(inSchemaButNotInTable) > 0:
            raise ValueError(f"FAIL: Table '{self.table}' missing expected columns {inSchemaButNotInTable}. Expected columns: {self.col_names}")

        logging.info(f"PASS: Table '{self.table}' columns and schema columns match: {self.col_names}")


#---------------------
class PostgresCheckColumnNumericRangeOperator(BaseOperator):
    """Checks whether the values in a given numeric column in a Postgres table falls within
    a user-provided range.
    """
    @apply_defaults
    def __init__(self,
                 task_id:str,
                 postgres_conn_id:str,
                 table:str,
                 column:str,
                 min_value:float,
                 max_value:float,
                 *args, **kwargs):

        super(PostgresCheckColumnNumericRangeOperator,self).__init__(task_id=task_id, *args, **kwargs)
        self.table = table
        self.postgres_conn_id = postgres_conn_id
        self.column = column
        self.min_value = min_value
        self.max_value = max_value

    def execute(self, context):
        postgres_hook = PostgresHook(self.postgres_conn_id)
        records = postgres_hook.get_records(f"SELECT {self.column} FROM {self.table}")

        tableValues = psycopg2ToFlatList(records)

        maxTableValue = max(tableValues)
        minTableValue = min(tableValues)

        if minTableValue < self.min_value:
            raise ValueError(f"FAIL: Column '{self.column}' in table '{self.table}' had minimum value '{minTableValue}' - below limit '{self.min_value}'")

        if maxTableValue > self.max_value:
            raise ValueError(f"FAIL: Column '{self.column}' in table '{self.table}' had maximum value '{maxTableValue}' - above limit '{self.max_value}'")

        logging.info(f"PASS: Values in column '{self.column}' in table '{self.table}' fall within range {self.min_value} - {self.max_value}")



#---------------------
class PostgresCheckColumnNominalValuesOperator(BaseOperator):
    """Checks whether the values in a given string column in a Postgres table only include values
    in a user-provided list.
    """
    @apply_defaults
    def __init__(self,
                 task_id:str,
                 postgres_conn_id:str,
                 table:str,
                 column:str,
                 allowed_values:list,
                 *args, **kwargs):

        super(PostgresCheckColumnNominalValuesOperator,self).__init__(task_id=task_id, *args, **kwargs)
        self.table = table
        self.postgres_conn_id = postgres_conn_id
        self.column = column
        self.allowed_values = allowed_values

    def execute(self, context):
        postgres_hook = PostgresHook(self.postgres_conn_id)
        records = postgres_hook.get_records(f"SELECT DISTINCT {self.column} FROM {self.table}")

        table_vals = psycopg2ToFlatList(records)

        inTableButNotInSchema = set(table_vals) - set(self.allowed_values)

        if len(inTableButNotInSchema) > 0:
            raise ValueError(f"FAIL: Column {self.column} in table '{self.table}' had non-permitted value(s) {inTableButNotInSchema}. Allowed values: {self.allowed_values}")

        logging.info(f"PASS: Column {self.column} in table '{self.table}' contained only expected values: {self.allowed_values}")