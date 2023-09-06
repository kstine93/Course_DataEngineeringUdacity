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

# Script generated for node landing_accelerometer
landing_accelerometer_node1693985769506 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": [
            "s3://glue-kstine-bucket-udacity/Project_GlueSpark/landing/accelerometer/"
        ],
        "recurse": True,
    },
    transformation_ctx="landing_accelerometer_node1693985769506",
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
SqlQuery7 = """
/*
This query is intended to filter the trusted_customer data to only records which have a record
in landing_accelerometer. This should (in theory) clear out any unneeded customer data from analyses.
*/
SELECT *
FROM trusted_customer
WHERE email IN (
    SELECT user
    FROM landing_accelerometer
    GROUP BY user
)
"""
SQLQuery_node1693985849467 = sparkSqlQuery(
    glueContext,
    query=SqlQuery7,
    mapping={
        "landing_accelerometer": landing_accelerometer_node1693985769506,
        "trusted_customer": trusted_customer_node1,
    },
    transformation_ctx="SQLQuery_node1693985849467",
)

# Script generated for node curated_customer
curated_customer_node3 = glueContext.getSink(
    path="s3://glue-kstine-bucket-udacity/Project_GlueSpark/curated/customer/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="curated_customer_node3",
)
curated_customer_node3.setCatalogInfo(
    catalogDatabase="landing", catalogTableName="curated_customer"
)
curated_customer_node3.setFormat("glueparquet")
curated_customer_node3.writeFrame(SQLQuery_node1693985849467)
job.commit()
