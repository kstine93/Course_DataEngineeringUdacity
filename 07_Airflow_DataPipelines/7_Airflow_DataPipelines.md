# 06- Airflow & Data Pipelines
_Udacity - Data Engineering Nanodegree_

## Directed Acyclic Graphs (DAGs)
![image](media/dag_diagram1.png)

- Directed: Work moves in one direction, from start to finish
  - (e.g., Node A in the image above has a relationship with Node B, but Node B does **NOT** have a relationship with Node A)
- Acyclic: No looping

---

## Introduction to Apache Airflow
Airflow is an open-source tool for creating DAG-based, schedulable data pipelines.
It also offers monitoring tools for visualising and evaluating the status of the pipeline.

With Airflow you can specify DAGs in languages like Python and run on schedules or external triggers. It has native integrations with Spark, Hadoop, Presto, AWS, and other tools (and offers development tools for creating more integrations).

To that point: **it's not a good idea to run any heavy code with Airflow - rather, Airflow should TRIGGER other tools** such as Spark or Redshift - which can do the heavy lifting.

**Question:** If a task in a DAG is simply triggering another script (like a webhook might) - how does the task then know that the other script has been completed and that subsequent tasks can start?

**Note:** Airflow will prioritize already-running tasks over new tasks. 
**Question:** Will Airflow prioritize the next task from an already-running DAG over starting a new DAG's first task?
---

## What makes up Airflow?

- **Scheduler** - for orchestrating jobs according to a schedule
- **Work Queue** - holds the state of running DAGS and tasks
- **Worker Processes** - execute the operations defined in each DAG
- **Metadata Database** - which saves credentials, connections, history, and configuration
- **Web UI** - provides control to user

---

## Airflow Operators

Airflow comes pre-built with certain 'operators' which allow you to perform certain operations very easily, including:
- Bash Operator (executes bash command)
- Python Operator (calls Python function)
- EmailOperator (sends an email)

Additionally, there are [community packages](https://airflow.apache.org/docs/apache-airflow-providers/index.html) that introduce additional operators, including:
- SimpleHttpOperator
- MySqlOperator
- MsSqlOperator
- PostgresOperator
- OracleOperator
- JdbcOperator
- DockerOperator
- HiveOperator
- S3FileTransformOperator
- PrestoToMySqlOperator
- SlackAPIOperator
- RedshiftToS3Operator
- S3ToRedshiftOperator
- etc...

---

## Airflow Dependency Defintions
In airflow we can define dependencies using `>>` and `<<` operators, as well as `set_upstream()` and `set_downstream()` methods.

Ex: Let's imagine that we have an ETL which looks like this:

```
        -> task_B
      /           \
task_A             -> task_D
      \           /
        -> task_C
```

To define these dependencies, we would need to use this code:

```
task_A >> task_B
task_A >> task_C

task_B >> task_D
task_C >> task_D
```

Or, in another way using the methods instead:

```
task_D.set_upstream(task_B)
task_D.set_upstream(task_C)

task_B.set_upstream(task_A)
task_C.set_upstream(task_A)
```

---

## Airflow Connections & Hooks
Airflow often has to integrate with external systems. To facilitate that, it has a **Connection** system for storing credentials that are used to talk to external systems.

A Connection is a set of parameters (e.g., username, password) - along with an ID to identify the Connection uniquely.
Each connection is required to have a 'type' which tells Airflow how to pass along the credentials to a specific service. These include (but not limited to):
- Amazon Web Services Connection
- Google Cloud Platform Connection
- Google Cloud SQL Connection
- gRPC
- MySQL Connection
- Oracle Connection
- PostgresSQL Connection
- SSH Connection



**Hooks** are high-level code to interface with an external platform and which lets you easily talk with this external platform without using low-level code like an API.

Hooks integrate with Connections to gather credentials for accessing these various platforms.

Existing hooks include the following, but custom hooks are also possible to code up:
- airflow.hooks.base_hook
- airflow.hooks.S3_hook
- airflow.hooks.dbapi_hook
- airflow.hooks.docker_hook
- airflow.hooks.druid_hook
- airflow.hooks.hdfs_hook
- airflow.hooks.hive_hook
- airflow.hooks.http_hook
- airflow.hooks.jdbc_hook
- airflow.hooks.mssql_hook
- airflow.hooks.mysql_hook
- airflow.hooks.oracle_hook
- airflow.hooks.pig_hook
- airflow.hooks.postgres_hook (also works with Redshift)
- airflow.hooks.presto_hook
- airflow.hooks.samba_hook
- airflow.hooks.slack_hook
- airflow.hooks.sqlite_hook
- airflow.hooks.webhdfs_hook
- etc...

---

## Airflow context variables
Airflow context variables allow you to get runtime information of the DAG *within the application run within your DAG*
So if we want to put the DAG run_id in our application code and put it in logging messages, we can do that.

**Important: 'execution_date' prints the execution date- what this means is *the date on which the DAG was SUPPOSED to be run*.**
If you have backlogged your DAG to run since a date in the past, this will show PAST dates. This can be great for using this
variable to split data (e.g., each month, import data from previous month and figure out what the previous month was by looking
at the 'ds' variable from the DAG execution)

| {{ data_interval_start }}              | Start of the data interval (pendulum.DateTime).                                                                                                                                        |                              **AWS EC2**                             |
|----------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|:--------------------------------------------------------------------:|
| {{ data_interval_end }}                | End of the data interval (pendulum.DateTime).                                                                                                                                          | Yes                                                                  |
| {{ ds }}                               | The DAG run’s logical date as YYYY-MM-DD. Same as {{ dag_run.logical_date \| ds }}.                                                                                                    | Does not use node categorization                                     |
| {{ ds_nodash }}                        | Same as {{ dag_run.logical_date \| ds_nodash }}.                                                                                                                                       | Only if you configure HDFS on EC2 yourself using multi-step process. |
| {{ ts }}                               | Same as {{ dag_run.logical_date \| ts }}. Example: 2018-01-01T00:00:00+00:00.                                                                                                          | ECS uses s3a                                                         |
| {{ ts_nodash_with_tz }}                | Same as {{ dag_run.logical_date \| ts_nodash_with_tz }}. Example: 20180101T000000+0000.                                                                                                | Lower                                                                |
| {{ ts_nodash }}                        | Same as {{ dag_run.logical_date \| ts_nodash }}. Example: 20180101T000000.                                                                                                             |                                                                      |
| {{ prev_data_interval_start_success }} | Start of the data interval from prior successful DAG run (pendulum.DateTime or None).                                                                                                  |                                                                      |
| {{ prev_data_interval_end_success }}   | End of the data interval from prior successful DAG run (pendulum.DateTime or None).                                                                                                    |                                                                      |
| {{ prev_start_date_success }}          | Start date from prior successful dag run (if available) (pendulum.DateTime or None).                                                                                                   |                                                                      |
| {{ dag }}                              | The DAG object.                                                                                                                                                                        |                                                                      |
| {{ task }}                             | The Task object.                                                                                                                                                                       |                                                                      |
| {{ macros }}                           | A reference to the macros package, described below.                                                                                                                                    |                                                                      |
| {{ task_instance }}                    | The task_instance object.                                                                                                                                                              |                                                                      |
| {{ ti }}                               | Same as {{ task_instance }}.                                                                                                                                                           |                                                                      |
| {{ params }}                           | A reference to the user-defined params dictionary which can be overridden by the dictionary passed through trigger_dag -c if you enabled dag_run_conf_overrides_params in airflow.cfg. |                                                                      |
| {{ var.value.my_var }}                 | Global defined variables represented as a dictionary.                                                                                                                                  |                                                                      |
| {{ var.json.my_var.path }}             | Global defined variables represented as a dictionary. With deserialized JSON object, append the path to the key within the JSON object.                                                |                                                                      |
| {{ conn.my_conn_id }}                  | Connection represented as a dictionary.                                                                                                                                                |                                                                      |
| {{ task_instance_key_str }}            | A unique, human-readable key to the task instance formatted {dag_id}__{task_id}__{ds_nodash}.                                                                                          |                                                                      |
| {{ conf }}                             | The full configuration object located at airflow.configuration.conf which represents the content of your airflow.cfg.                                                                  |                                                                      |
| {{ run_id }}                           | The run_id of the current DAG run.                                                                                                                                                     |                                                                      |
| {{ dag_run }}                          | A reference to the DagRun object.                                                                                                                                                      |                                                                      |
| {{ test_mode }}                        | Whether the task instance was called using the CLI’s test subcommand.                                                                                                                  |                                                                      |

---

## Other Airflow resources:
- Cool repo in which DAGS are specified to *maintain Airflow* (e.g., deleting old logs once per week, etc.)
  - https://github.com/teamclairvoyant/airflow-maintenance-dags