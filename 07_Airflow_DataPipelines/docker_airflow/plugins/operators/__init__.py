from airflow.plugins_manager import AirflowPlugin

import operators

#Defining plugin class - this is apparently what notifies Airflow that we have additional plugins to
#load under a specific name
class KevinsCustomPlugins(AirflowPlugin):
    name = "kevins_custom_plugins"
    operators = [
        operators.hasRowsOperator,
        operators.S3ToRedshiftOperator
    ]