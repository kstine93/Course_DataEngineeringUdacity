import json
import pendulum
from airflow.decorators import dag, task

#Position A. Configuring DAG:
@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"]
)
def my_new_dag():
    #------------
    @task()
    def extract():
        '''
        Basic function to get data and return it for use later in the DAG.
        '''
        data_string = '{"1001": 301.27, "1002": 433.21, "1003": 502.22}'
        order_data_dict = json.loads(data_string)
        return order_data_dict

    #------------
    @task(multiple_outputs=True)
    def transform(order_data_dict: dict):
        """
        #### Transform task
        A simple Transform task which takes in the collection of order data and computes the total order value.
        """
        total_order_value = 0

        for value in order_data_dict.values():
            total_order_value += value

        return {"total_order_value": total_order_value}

    #------------
    @task()
    def load(total_order_value: float):
        """
        #### Load task
        A simple Load task which takes in the result of the Transform task and
        instead of saving it to end user review, just prints it out.
        """

        print(f"Total order value is: {total_order_value:.2f}")

    #Pos. C: Writing script (just like Python code)
    order_data = extract()
    order_summary = transform(order_data)
    load(order_summary["total_order_value"])

#Pos. D: Activating the DAG:
res = my_new_dag()