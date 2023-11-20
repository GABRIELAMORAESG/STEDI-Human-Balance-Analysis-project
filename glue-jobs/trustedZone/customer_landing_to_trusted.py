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

# Script generated for node customer_landing
customer_landing_node1700412848712 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://gabi-udacity-glue/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="customer_landing_node1700412848712",
)

# Script generated for node ustomer Records who agreed to share their data for research purposes
ustomerRecordswhoagreedtosharetheirdataforresearchpurposes_node1700412882831 = Filter.apply(
    frame=customer_landing_node1700412848712,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="ustomerRecordswhoagreedtosharetheirdataforresearchpurposes_node1700412882831",
)

# Script generated for node customer_trusted
customer_trusted_node1700412934931 = glueContext.getSink(
    path="s3://gabi-udacity-glue/customer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="customer_trusted_node1700412934931",
)
customer_trusted_node1700412934931.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="customer_trusted"
)
customer_trusted_node1700412934931.setFormat("glueparquet")
customer_trusted_node1700412934931.writeFrame(
    ustomerRecordswhoagreedtosharetheirdataforresearchpurposes_node1700412882831
)
job.commit()
