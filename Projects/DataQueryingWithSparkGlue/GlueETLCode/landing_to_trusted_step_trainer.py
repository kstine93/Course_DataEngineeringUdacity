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

# Script generated for node landing_step_trainer
landing_step_trainer_node1693985769506 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": [
            "s3://glue-kstine-bucket-udacity/Project_GlueSpark/landing/step_trainer/"
        ],
        "recurse": True,
    },
    transformation_ctx="landing_step_trainer_node1693985769506",
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

# Script generated for node SQL Query
SqlQuery0 = """
/*
This query is intended to filter the landing_accelerometer data to only records which have an email in trusted_customer.
Since trusted_customer has already been filtered to be trusted, this is how we can remove records from landing_accelerometer
for customers that have not consented to processing.
*/
SELECT *
FROM landing_step_trainer
WHERE serialNumber IN (
    SELECT serialNumber
    FROM trusted_customer
    GROUP BY serialNumber
)
"""
SQLQuery_node1693985849467 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={
        "landing_step_trainer": landing_step_trainer_node1693985769506,
        "trusted_customer": trusted_customer_node1,
    },
    transformation_ctx="SQLQuery_node1693985849467",
)

# Script generated for node trusted_step_trainer
trusted_step_trainer_node3 = glueContext.getSink(
    path="s3://glue-kstine-bucket-udacity/Project_GlueSpark/trusted/step_trainer/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="trusted_step_trainer_node3",
)
trusted_step_trainer_node3.setCatalogInfo(
    catalogDatabase="landing", catalogTableName="trusted_step_trainer"
)
trusted_step_trainer_node3.setFormat("glueparquet")
trusted_step_trainer_node3.writeFrame(SQLQuery_node1693985849467)
job.commit()
