import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node step_trainer
step_trainer_node1701007936572 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://gabi-udacity-glue/step_trainer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="step_trainer_node1701007936572",
)

# Script generated for node accelerometer
accelerometer_node1701007893582 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://gabi-udacity-glue/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="accelerometer_node1701007893582",
)

# Script generated for node Join
Join_node1701007971028 = Join.apply(
    frame1=accelerometer_node1701007893582,
    frame2=step_trainer_node1701007936572,
    keys1=["timestamp"],
    keys2=["sensorreadingtime"],
    transformation_ctx="Join_node1701007971028",
)

# Script generated for node Drop Fields
DropFields_node1701008403395 = DropFields.apply(
    frame=Join_node1701007971028,
    paths=["sensorreadingtime"],
    transformation_ctx="DropFields_node1701008403395",
)

# Script generated for node Amazon S3
AmazonS3_node1701008456141 = glueContext.getSink(
    path="s3://gabi-udacity-glue/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1701008456141",
)
AmazonS3_node1701008456141.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="machine_learning_curated"
)
AmazonS3_node1701008456141.setFormat("glueparquet")
AmazonS3_node1701008456141.writeFrame(DropFields_node1701008403395)
job.commit()
