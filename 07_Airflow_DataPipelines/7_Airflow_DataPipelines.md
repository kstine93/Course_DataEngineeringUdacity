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
This is because Airflow workers are not working on distributed systems, so they are limited to the memory and processing power on the machine Airflow is running on.

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

## Data Lineage
Data Lineage describes the entire path of how data is created, transformed and stored.
This could be thought of as a transformation sequence - a list of commands performed on data that take it from A -> B.
However, rather than *performing* these steps, "Data Lineage" is often recorded to build understanding (and confidence) in data.
For this reason (also to help communicate with non-programmers) data lineage is often **graphical** as it is in DAG graphs.

---

## Good Scheduling Practices
Regularly recurring pipelines which ingest data should aim to ingest only all data since the last run. For example, a weekly-run
dag should probably process only (new) data from the past week. This is where getting the 'Context' like in `sample_dag3.py`
is helpful.

Also, the **size** of the data can help us understand how often to run our pipelines. Maybe if we don't want our DAGs running for
hours, we need to process data more frequently (and in smaller chunks).

Another useful feature of Airflow is that **DAGs can have end dates**. This is nifty because it lets us set up sunsets for
older pipelines and have the newer pipelines start immediately afterwards - no manual work required!

e.g.:
```python
dag = DAG(
  dag_id="end_date_example_dag"
  ,start_date=datetime.datetime.now()
  ,end_Date=datetime.datetime.now() + datetime.timedelta(7)
  #If there is ever a case when we could run >1 DAG at a time (e.g., backfilling), how many concurrent DAGs do we want?
  #For example, if analyses from May depend on analyses from April, we can't run April + May DAGs concurrently.
  ,max_active_runs=1
)
```

>Note: If you remove the historical logs of DAG runs, that can trigger Airflow to **RE-RUN (BACKFILL) YOUR DAGS** because it thinks that these DAGs never ran!! **Be careful when clearing old DAG log data**

---

## Data Partitioning
Data partitioning has a host of advantages, including:
- Limiting the amount of data processed at one single time
- Allowing greater parallelization of tasks
- Allowing more robust backups / distribution of data

**Logical Partitioning**: Partitioning based on the information being dealt with. For example, 'customers' should be processed separately from 'employees', even though they may have some fields in common.

**Size Paritioning**: Partitioning based on the size of the data to be processed (e.g., in to 1-GB chunks)

>**How to partition with Airflow?**<br>
Since Airflow DAGs should be designed to simply *trigger* work on other machines (e.g., give a command to Redshift to copy from S3), Airflow partitioning is really a task of partitioning in other systems.<br>
For example, if we were to send COPY commands to Redshift to get data from S3, we could tell Airflow to send COPY commands which only reflect partitions of the data (e.g., "COPY FROM s3://march" , "COPY FROM s3://april"). So we could 'inject' relevant partitioning details into the commands we send out to other tools with Airflow.<br>
**NOTE:** This is an argument for making our triggered jobs quite simple - but with a lot of configurable options. Then we can simply configure our DAGs to change our our jobs behave, rather than having to go into each job to make configurations manually.
---

## DAG Templating
Airflow has some special operators which allow you to enter context information directly into your DAGs.
This format looks like this: `{{ execution_date }}`.
When Airflow runs this Python script, *it will automatically inject the variables into your code*

Let's say we have some SQL which will be run on Redshift to process a specific table. When we run our DAG, we can
inject partitioning information (e.g., year, data type)

```python
COPY_SQL = """
COPY trips
FROM 's3://test/repo/{year}/{month}'
ACCESS_KEY_ID 'access_key_here'
SECRET_ACCESS_KEY 'secret_key_here'
IGNOREHEADER 1
DELIMITER ','
"""

date = datetime({{ execution_date }})
COPY_SQL.format(date.year, date.month)
```

This kind of double-curly bracketing seems to be related to the templating engine "Jinja" (also available as a Python module).
**TODO: Look into Jinja more and see if this is what's going on here or not...**
[more info on jinja here](https://realpython.com/primer-on-jinja-templating/)

---

## Ways to measure data quality (in Airflow and elsewhere)
Some common ways to measure data quality are:
- Data must be a **certain size** (and no larger)
- Data must be accurate to **some margin of error**
- Data must arrive within a given timeframe
- Pipelines must not take too long to run
- Data must contain all required information and no additional (particularly sensitive) information

Airflow offers **SLAs** to allow the user to specify fail criteria that Airflow can use in monitoring our DAGs and flagging violations.
---

## Other Airflow resources:
- Cool repo in which DAGS are specified to *maintain Airflow* (e.g., deleting old logs once per week, etc.)
  - https://github.com/teamclairvoyant/airflow-maintenance-dags
