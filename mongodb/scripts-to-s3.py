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

# Script generated for node Data Catalog table
DataCatalogtable_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="betgame-db",
    table_name="mongobetgame_balance_log",
    transformation_ctx="DataCatalogtable_node1",
    additional_options = {"database":"betgame",
            "collection":"balance_log"}
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = ApplyMapping.apply(
    frame=DataCatalogtable_node1,
    mappings=[
        ("args", "array", "args", "array"),
        ("user_id", "string", "user_id", "string"),
        ("before", "double", "before", "double"),
        ("currency", "string", "currency", "string"),
        ("_id", "string", "_id", "string"),
        ("after", "double", "after", "double"),
        ("event", "string", "event", "string"),
        ("create_at", "double", "create_at", "double"),
        ("desc", "string", "desc", "string"),
    ],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=ApplyMapping_node2,
    connection_type="s3",
    format="glueparquet",
    connection_options={"path": "s3://athena-shard-test", "partitionKeys": ["user_id"]},
    format_options={"compression": "snappy"},
    transformation_ctx="S3bucket_node3",
)

job.commit()
