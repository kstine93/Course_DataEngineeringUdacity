#Code for setting up a simple DAG in Airflow

from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresHook, PostgresOperator

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

from airflow.operators import HasRowsOperator, S3ToRedshiftOperator

'''
Self-exercies: Setting up custom operators + Airflow.
Here's what I want to do:
1. Set up a custom operator for data checking INCREASED number of rows (use HasRows operator as example)
    a. Let's assume this is an append action. Operator should print out before value + after value
2. Set up a custom operator for 
'''