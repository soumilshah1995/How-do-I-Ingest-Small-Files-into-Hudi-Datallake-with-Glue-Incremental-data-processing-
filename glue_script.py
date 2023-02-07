try:
    import sys
    from awsglue.transforms import *
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from pyspark.sql.session import SparkSession
    from awsglue.dynamicframe import DynamicFrame
    from pyspark.sql.functions import col, to_timestamp, monotonically_increasing_id, to_date, when
    from pyspark.sql.functions import *
    from awsglue.utils import getResolvedOptions
    from pyspark.sql.types import *
    from datetime import datetime
    import boto3
    from functools import reduce
except Exception as e:
    pass


spark = SparkSession.builder.config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer') \
    .config('spark.sql.hive.convertMetastoreParquet', 'false') \
    .config('spark.sql.legacy.pathOptionBehavior.enabled', 'true') \
    .config('spark.shuffle.storage.path', 's3://soumilshah-hudi-demos/shuffle') \
    .getOrCreate()

sc = spark.sparkContext
glueContext = GlueContext(sc)
job = Job(glueContext)
logger = glueContext.get_logger()
db_name = "hudidb"
table_name = "small_files"

recordkey = 'emp_id'
path = "s3://soumilshah-hudi-demos/tmp/"
groupSize = "1048576"
method = 'upsert'
table_type = "COPY_ON_WRITE"


connection_options={
    "path": path,
    "connectionName": "hudi-connection",

    "hoodie.datasource.write.storage.type": table_type,
    'className': 'org.apache.hudi',
    'hoodie.table.name': table_name,
    'hoodie.datasource.write.recordkey.field': recordkey,
    'hoodie.datasource.write.table.name': table_name,
    'hoodie.datasource.write.operation': method,

    'hoodie.datasource.hive_sync.enable': 'true',
    "hoodie.datasource.hive_sync.mode":"hms",
    'hoodie.datasource.hive_sync.sync_as_datasource': 'false',
    'hoodie.datasource.hive_sync.database': db_name,
    'hoodie.datasource.hive_sync.table': table_name,
    'hoodie.datasource.hive_sync.use_jdbc': 'false',
    'hoodie.datasource.hive_sync.partition_extractor_class': 'org.apache.hudi.hive.MultiPartKeysValueExtractor',
    'hoodie.datasource.write.hive_style_partitioning': 'true',
}

glue_df = glueContext.create_dynamic_frame.from_options("s3",
                                                        {
                                                            'paths': ["s3://soumilshah-hudi-demos/data/"],
                                                            'recurse': True,
                                                            'groupFiles': 'inPartition',
                                                            'groupSize': groupSize
                                                        },
                                                        format="json")
WriteDF = (
    glueContext.write_dynamic_frame.from_options(
        frame=glue_df,
        connection_type="marketplace.spark",
        connection_options=connection_options,
        transformation_ctx="glue_df",
    )
)
job.commit()
