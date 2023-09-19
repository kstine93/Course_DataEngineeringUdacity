from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.secrets.metastore import MetastoreBackend

class StageToRedshiftOperator(BaseOperator):
    # ui_color = '#358140'
    copy_sql = """
        COPY {sink}
        FROM '{source}'
        ACCESS_KEY_ID '{access_id}'
        SECRET_ACCESS_KEY '{secret_key}'
        JSON '{jsonpath}'
    """

    @apply_defaults
    def __init__(self,
                 task_id:str,
                 redshift_conn_id:str,
                 aws_conn_id:str,
                 s3_source:str,
                 table_sink:str,
                 jsonpath:str='auto',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(task_id=task_id,*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_conn_id = aws_conn_id
        self.s3_source = s3_source
        self.table_sink = table_sink
        #Read about JSONpaths here: https://docs.aws.amazon.com/redshift/latest/dg/copy-usage_notes-copy-from-json.html
        #Sometimes help to avoid Redshift mis-interpreting (or dropping) data
        self.jsonpath = jsonpath

    def __get_aws_credentials(self):
        metastoreBackend = MetastoreBackend()
        aws_connection=metastoreBackend.get_connection(self.aws_conn_id)
        return aws_connection


    def execute(self, context):
        hook = PostgresHook(self.redshift_conn_id)
        aws_credentials = self.__get_aws_credentials()
        query = self.copy_sql.format(
            sink = self.table_sink,
            source = self.s3_source,
            access_id = aws_credentials.login,
            secret_key = aws_credentials.password,
            jsonpath = self.jsonpath
        )
        hook.run(query)
        self.log.info(f"Data successfully copied from '{self.s3_source}' to '{self.table_sink}'")




