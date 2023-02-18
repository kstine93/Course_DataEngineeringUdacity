import logging

from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresHook, PostgresOperator
from airflow.models import BaseOperator

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

from airflow.utils.decorators import apply_defaults

'''
Purpose of this operator:
1. Move data from S3 to RedShift, but without data deletion / overwriting (in an APPEND manner)
'''
class S3ToRedshiftOperator(BaseOperator):
    #We are telling our operator here that we want our S3 key to be templatable.
    #This means that Airflow will use its environment variables to fill in that value.
    #Question: Where is Airflow drawing this from? Airflow Connections? Airflow runtime context?
    template_fields = ("s3_key",)
    copy_to_redshift_sql = '''
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        IGNOREHEADER {}
        DELIMITER {}
    '''    

    def __init__(self,
                 redshift_conn_id: str,
                 redshift_table: str,
                 aws_credentials_id: str,
                 s3_bucket: str,
                 s3_key: str,
                 s3_delimiter: str,
                 s3_ignore_headers: bool = True,
                 *args,
                 **kwargs):
                 
        super().__init__(*args,**kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.redshift_table = redshift_table
        self.aws_credentials = aws_credentials_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.s3_delimiter = s3_delimiter
        self.s3_ignore_headers = s3_ignore_headers

    def execute(self, context):
        #Giving Airflow 'aws_credentials_id' string - which tells Airflow which pre-loaded connection to use.
        aws_hook = AwsBaseHook(aws_conn_id=self.aws_credentials_id)
        #GET our credentials - this probably doesn't even talk to AWS at all - but rather just tells Airflow
        #to feed back our connection details to us. This is done because the SQL code REQUIRES plaintext access keys
        #If we were using Boto3 or something else for Redshift, maybe we wouldn't have to load in credentials like this.
        credentials = aws_hook.get_credentials()

        #Similarly, here we're just telling Airflow how to connect to Redshift.
        redshift_conn = PostgresHook(postgres_conn_id = self.redshift_conn_id)

        self.log.info("Copying data from S3 to Redshift")
        #Ok, so here is where we are actually getting the key - which is apparently stored in the runtime context.
        #I can imagine this would work even without the templating used at the beginning of this class - why do we need the templating?
        rendered_key = self.s3_key.format(**context)

        s3_path = f"s3://{self.s3_bucket}/{rendered_key}"
        formatted_sql = S3ToRedshiftOperator.copy_to_redshift_sql.format(
            self.redshift_table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.s3_ignore_headers,
            self.s3_delimiter
        )
        
        redshift_conn.run(formatted_sql)