from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.secrets.metastore import MetastoreBackend


class LoadTableOperator(BaseOperator):

    @apply_defaults
    def __init__(self,
                 task_id:str,
                 redshift_conn_id:str,
                 aws_conn_id:str,
                 select_query:str,
                 table_sink:str,
                 empty_table_sink_first:bool = False,
                 *args, **kwargs):

        super(LoadTableOperator, self).__init__(task_id=task_id,*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_conn_id = aws_conn_id
        self.select_query = select_query
        self.table_sink = table_sink
        self.empty_table_sink_first = empty_table_sink_first


    def __get_aws_credentials(self):
        metastoreBackend = MetastoreBackend()
        aws_connection=metastoreBackend.get_connection(self.aws_conn_id)
        return aws_connection


    def execute(self, context):
        hook = PostgresHook(self.redshift_conn_id)
        aws_credentials = self.__get_aws_credentials()

        if self.empty_table_sink_first:
            hook.run(f"TRUNCATE TABLE {self.table_sink}")

        query = f"INSERT INTO {self.table_sink} {self.select_query}"
        hook.run(query)
        self.log.info(f"Query successfully executed on table {self.table_sink}")