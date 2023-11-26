import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Amazon S3
AmazonS3_node1701005505509 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://gabi-udacity-glue/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1701005505509",
)

# Script generated for node Amazon S3
AmazonS3_node1701005536360 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://gabi-udacity-glue/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1701005536360",
)

# Script generated for node Join
Join_node1701005552522 = Join.apply(
    frame1=AmazonS3_node1701005505509,
    frame2=AmazonS3_node1701005536360,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1701005552522",
)

# Script generated for node Drop Fields
DropFields_node1701006260696 = DropFields.apply(
    frame=Join_node1701005552522,
    paths=["z", "user", "y", "x", "timestamp"],
    transformation_ctx="DropFields_node1701006260696",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1701006289737 = DynamicFrame.fromDF(
    DropFields_node1701006260696.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1701006289737",
)

# Script generated for node Amazon S3
AmazonS3_node1701006346644 = glueContext.getSink(
    path="s3://gabi-udacity-glue/customer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1701006346644",
)
AmazonS3_node1701006346644.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="customer_curated"
)
AmazonS3_node1701006346644.setFormat("glueparquet")
AmazonS3_node1701006346644.writeFrame(DropDuplicates_node1701006289737)
job.commit()
