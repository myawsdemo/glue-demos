import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext, SparkConf
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import boto3
import time
import logging
import json

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# 获取传递给AWS Glue作业的参数
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# 创建SparkContext和GlueContext
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

def oid_to_str(oid):
    return str(oid)

# 将函数注册为 Spark SQL UDF
oid_to_str_udf = udf(oid_to_str, StringType())

secrets_client = boto3.client('secretsmanager')
# 获取密钥值
secret = secrets_client.get_secret_value(SecretId='test/docdb')
secret_json = json.loads(secret['SecretString'])

# 从密钥值中提取密码
username = secret_json['username']
password = secret_json['password']
host = secret_json['host']
port = secret_json['port']

logger.info("password: {}", secret)

documentdb_uri = "mongodb://" + host + ":" + str(port)
read_docdb_options = {
    "uri": documentdb_uri,
    "database": "betgame",
    "collection": "balance_log",
    "username": username,
    "password": password,
    "ssl": "true",
    "ssl.domain_match": "false",
    "partitioner": "MongoSamplePartitioner",
    "partitionerOptions.partitionSizeMB": "10",
    "partitionerOptions.partitionKey": "_id"
}

# Create the DynamicFrame using the options
mongo_dynamic_frame = glueContext.create_dynamic_frame.from_options(
    connection_type="documentdb",
    connection_options=read_docdb_options,
    push_down_predicate="create_at > 0",
    transformation_ctx="input"
)

# 读取MongoDB数据源
#mongo_dynamic_frame = glueContext.create_dynamic_frame.from_options(connection_type="documentdb",
#                                                               connection_options=read_docdb_options)

# 将 ObjectId 字段转换为字符串字段
df = mongo_dynamic_frame.toDF()
df = df.withColumn("_id", oid_to_str_udf(df["_id"]))

# 将 DataFrame 转换为 Glue DynamicFrame
dynamic_frame = DynamicFrame.fromDF(df, glueContext, "my_dynamic_frame")

# 转换数据，增加ID列，并写入Amazon S3
output_path = "s3://mongodb-glue-test"
output_format = "parquet"
output_mode = "append"

glueContext.write_dynamic_frame.from_options(
    frame=dynamic_frame,
    connection_type="s3",
    connection_options={
        "path": output_path,
        "partitionKeys": ["user_id"],
        "mode": "overwrite" # 防止重写
    },
    format=output_format,
    transformation_ctx="output"
)

job.commit()