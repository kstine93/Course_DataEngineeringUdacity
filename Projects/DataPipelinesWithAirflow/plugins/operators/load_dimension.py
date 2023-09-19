from operators.load_table import LoadTableOperator

class LoadDimensionOperator(LoadTableOperator):
    """Wrapper class for sending preset values to 'LoadTableOperator'"""

    #Color shown on task in Airflow GUI:
    ui_color = '#80BD9E'

    def __init__(self,
                 task_id:str,
                 redshift_conn_id:str,
                 aws_conn_id:str,
                 select_query:str,
                 table_sink:str,
                 empty_table_sink_first:bool=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(
            task_id=task_id,
            redshift_conn_id=redshift_conn_id,
            aws_conn_id=aws_conn_id,
            select_query=select_query,
            table_sink=table_sink,
            empty_table_sink_first = empty_table_sink_first,
            *args,
            **kwargs)