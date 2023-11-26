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
AmazonS3_node1701003377815 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://gabi-udacity-glue/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1701003377815",
)

# Script generated for node Amazon S3
AmazonS3_node1701003380556 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://gabi-udacity-glue/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1701003380556",
)

# Script generated for node Union Join
UnionJoin_node1701003404535 = Join.apply(
    frame1=AmazonS3_node1701003377815,
    frame2=AmazonS3_node1701003380556,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="UnionJoin_node1701003404535",
)

# Script generated for node Select Fields
SelectFields_node1701003640360 = SelectFields.apply(
    frame=UnionJoin_node1701003404535,
    paths=["z", "y", "user", "timestamp"],
    transformation_ctx="SelectFields_node1701003640360",
)

# Script generated for node Amazon S3
AmazonS3_node1701003571480 = glueContext.getSink(
    path="s3://gabi-udacity-glue/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1701003571480",
)
AmazonS3_node1701003571480.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="accelerometer_trusted"
)
AmazonS3_node1701003571480.setFormat("glueparquet")
AmazonS3_node1701003571480.writeFrame(SelectFields_node1701003640360)
job.commit()
