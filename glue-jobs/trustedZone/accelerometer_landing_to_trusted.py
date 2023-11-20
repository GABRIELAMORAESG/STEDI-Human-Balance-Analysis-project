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

# Script generated for node customer_trusted
customer_trusted_node1700413403062 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://gabi-udacity-glue/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="customer_trusted_node1700413403062",
)

# Script generated for node accelerometer_landing
accelerometer_landing_node1700413455235 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://gabi-udacity-glue/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="accelerometer_landing_node1700413455235",
)

# Script generated for node Join
Join_node1700413527054 = Join.apply(
    frame1=customer_trusted_node1700413403062,
    frame2=accelerometer_landing_node1700413455235,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1700413527054",
)

# Script generated for node Drop Fields
DropFields_node1700413623260 = DropFields.apply(
    frame=Join_node1700413527054,
    paths=[
        "serialnumber",
        "sharewithpublicasofdate",
        "birthday",
        "registrationdate",
        "sharewithresearchasofdate",
        "customername",
        "sharewithfriendsasofdate",
        "email",
        "lastupdatedate",
        "phone",
    ],
    transformation_ctx="DropFields_node1700413623260",
)

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1700413639533 = glueContext.getSink(
    path="s3://gabi-udacity-glue/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="accelerometer_trusted_node1700413639533",
)
accelerometer_trusted_node1700413639533.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="accelerometer_trusted"
)
accelerometer_trusted_node1700413639533.setFormat("glueparquet")
accelerometer_trusted_node1700413639533.writeFrame(DropFields_node1700413623260)
job.commit()
