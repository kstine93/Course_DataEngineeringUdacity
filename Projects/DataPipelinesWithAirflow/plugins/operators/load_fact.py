from operators.load_table import LoadTableOperator

class LoadFactOperator(LoadTableOperator):
    """Wrapper class for sending preset values to 'LoadTableOperator'"""

    #Color shown on task in Airflow GUI:
    ui_color = '#F98866'

    def __init__(self,
                 task_id:str,
                 redshift_conn_id:str,
                 aws_conn_id:str,
                 select_query:str,
                 table_sink:str,
                 *args, **kwargs):

        #Setting default values:
        empty_table_sink_first = False

        super(LoadFactOperator, self).__init__(
            task_id=task_id,
            redshift_conn_id=redshift_conn_id,
            aws_conn_id=aws_conn_id,
            select_query=select_query,
            table_sink=table_sink,
            empty_table_sink_first = empty_table_sink_first,
            *args,
            **kwargs)