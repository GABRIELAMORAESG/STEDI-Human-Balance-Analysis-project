import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Amazon S3
AmazonS3_node1701003953136 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://gabi-udacity-glue/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1701003953136",
)

# Script generated for node Amazon S3
AmazonS3_node1701003966679 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://gabi-udacity-glue/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1701003966679",
)

# Script generated for node Join
Join_node1701004020385 = Join.apply(
    frame1=AmazonS3_node1701003953136,
    frame2=AmazonS3_node1701003966679,
    keys1=["serialNumber"],
    keys2=["serialNumber"],
    transformation_ctx="Join_node1701004020385",
)

# Script generated for node Select Fields
SelectFields_node1701004063217 = SelectFields.apply(
    frame=Join_node1701004020385,
    paths=["sensorReadingTime", "serialNumber", "distanceFromObject"],
    transformation_ctx="SelectFields_node1701004063217",
)

# Script generated for node Amazon S3
AmazonS3_node1701004144178 = glueContext.getSink(
    path="s3://gabi-udacity-glue/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1701004144178",
)
AmazonS3_node1701004144178.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="step_trainer_trusted"
)
AmazonS3_node1701004144178.setFormat("glueparquet")
AmazonS3_node1701004144178.writeFrame(SelectFields_node1701004063217)
job.commit()
