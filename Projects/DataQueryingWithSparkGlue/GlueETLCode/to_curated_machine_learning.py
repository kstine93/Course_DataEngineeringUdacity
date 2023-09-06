import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node trusted_step_trainer
trusted_step_trainer_node1693985769506 = glueContext.create_dynamic_frame.from_catalog(
    database="landing",
    table_name="trusted_step_trainer",
    transformation_ctx="trusted_step_trainer_node1693985769506",
)

# Script generated for node trusted_customer
trusted_customer_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": [
            "s3://glue-kstine-bucket-udacity/Project_GlueSpark/trusted/customer/"
        ],
        "recurse": True,
    },
    transformation_ctx="trusted_customer_node1",
)

# Script generated for node trusted_accelerometer
trusted_accelerometer_node1694037883103 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": [
            "s3://glue-kstine-bucket-udacity/Project_GlueSpark/trusted/accelerometer/"
        ],
        "recurse": True,
    },
    transformation_ctx="trusted_accelerometer_node1694037883103",
)

# Script generated for node SQL Query
SqlQuery9 = """
/*
This query is intended to filter the landing_accelerometer data to only records which have an email in trusted_customer.
Since trusted_customer has already been filtered to be trusted, this is how we can remove records from landing_accelerometer
for customers that have not consented to processing.
*/
SELECT 
    step.sensorreadingtime,
    step.serialNumber,
    step.distancefromobject,
    acc.timestamp,
    acc.x,
    acc.y,
    acc.z
FROM trusted_step_trainer step
LEFT JOIN trusted_customer cus ON cus.serialNumber = step.serialNumber
INNER JOIN trusted_accelerometer acc ON cus.email = acc.user
    AND step.sensorReadingTime = acc.timestamp
"""
SQLQuery_node1693985849467 = sparkSqlQuery(
    glueContext,
    query=SqlQuery9,
    mapping={
        "trusted_step_trainer": trusted_step_trainer_node1693985769506,
        "trusted_customer": trusted_customer_node1,
        "trusted_accelerometer": trusted_accelerometer_node1694037883103,
    },
    transformation_ctx="SQLQuery_node1693985849467",
)

# Script generated for node curated_machine_learning
curated_machine_learning_node3 = glueContext.getSink(
    path="s3://glue-kstine-bucket-udacity/Project_GlueSpark/curated/machine_learning/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="curated_machine_learning_node3",
)
curated_machine_learning_node3.setCatalogInfo(
    catalogDatabase="landing", catalogTableName="curated_machine_learning"
)
curated_machine_learning_node3.setFormat("glueparquet")
curated_machine_learning_node3.writeFrame(SQLQuery_node1693985849467)
job.commit()
