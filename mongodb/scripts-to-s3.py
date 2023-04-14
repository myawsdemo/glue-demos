import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext, SparkConf
from awsglue.context import GlueContext
from awsglue.job import Job
import time

# 获取传递给AWS Glue作业的参数
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# 创建SparkContext和GlueContext
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

documentdb_uri = "mongodb://docdb-123.cluster-c8gkhofhtia4.us-east-1.docdb.amazonaws.com:27017"

read_docdb_options = {
    "uri": documentdb_uri,
    "database": "betgame",
    "collection": "{COLLECTION-NAME}",
    "username": "1234",
    "password": "xxxxx",
    "ssl": "true",
    "ssl.domain_match": "false",
    "partitioner": "MongoSamplePartitioner",
    "partitionerOptions.partitionSizeMB": "10",
    "partitionerOptions.partitionKey": "_id"
}


# 读取MongoDB数据源
mongo_dynamic_frame = glueContext.create_dynamic_frame.from_options(connection_type="documentdb",
                                                               connection_options=read_docdb_options)


# 转换数据，增加ID列，并写入Amazon S3
output_path = "s3://mongodb-glue-test"
output_format = "parquet"
output_mode = "append"

glueContext.write_dynamic_frame.from_options(
    frame=mongo_dynamic_frame,
    connection_type="s3",
    connection_options={
        "path": output_path,
        "partitionKeys": ["user_id"]
    },
    format=output_format,
    transformation_ctx="output"
)