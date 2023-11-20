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

# Script generated for node step_trainer_landing
step_trainer_landing_node1700417134650 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://gabi-udacity-glue/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="step_trainer_landing_node1700417134650",
)

# Script generated for node Amazon S3
AmazonS3_node1700417117404 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://gabi-udacity-glue/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1700417117404",
)

# Script generated for node Join
Join_node1700417170668 = Join.apply(
    frame1=AmazonS3_node1700417117404,
    frame2=step_trainer_landing_node1700417134650,
    keys1=["serialnumber"],
    keys2=["serialnumber"],
    transformation_ctx="Join_node1700417170668",
)

# Script generated for node Drop Fields
DropFields_node1700417532816 = DropFields.apply(
    frame=Join_node1700417170668,
    paths=[
        "serialnumber",
        "birthday",
        "sharewithpublicasofdate",
        "sharewithresearchasofdate",
        "registrationdate",
        "customername",
        "sharewithfriendsasofdate",
        "email",
        "lastupdatedate",
        "phone",
    ],
    transformation_ctx="DropFields_node1700417532816",
)

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1700417185950 = glueContext.getSink(
    path="s3://gabi-udacity-glue/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="step_trainer_trusted_node1700417185950",
)
step_trainer_trusted_node1700417185950.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="step_trainer_trusted"
)
step_trainer_trusted_node1700417185950.setFormat("glueparquet")
step_trainer_trusted_node1700417185950.writeFrame(DropFields_node1700417532816)
job.commit()
