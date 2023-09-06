import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node landing_customer
landing_customer_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": [
            "s3://glue-kstine-bucket-udacity/Project_GlueSpark/landing/customer/"
        ],
        "recurse": True,
    },
    transformation_ctx="landing_customer_node1",
)

# Script generated for node PrivacyFilter
PrivacyFilter_node1693511080366 = Filter.apply(
    frame=landing_customer_node1,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="PrivacyFilter_node1693511080366",
)

# Script generated for node trusted_customer
trusted_customer_node3 = glueContext.getSink(
    path="s3://glue-kstine-bucket-udacity/Project_GlueSpark/trusted/customer/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="trusted_customer_node3",
)
trusted_customer_node3.setCatalogInfo(
    catalogDatabase="landing", catalogTableName="trusted_customer"
)
trusted_customer_node3.setFormat("glueparquet")
trusted_customer_node3.writeFrame(PrivacyFilter_node1693511080366)
job.commit()
