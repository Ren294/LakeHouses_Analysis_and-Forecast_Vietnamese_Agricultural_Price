from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, FloatType
import faostat
from pyspark.sql import DataFrame
from .config import get_config_minio

s3_config = get_config_minio()


def create_spark_session(appName):
    warehouse_dir = "s3a://gold/hive/warehouse"

    return SparkSession.builder \
        .appName(appName) \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.executor.instances", "2") \
        .config("spark.executor.cores", "2") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
        .config("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar") \
        .config("spark.jars.packages",
                "org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0,"
                "org.apache.hadoop:hadoop-aws:3.3.1,"
                "com.amazonaws:aws-java-sdk-bundle:1.11.1026,"
                "org.postgresql:postgresql:42.7.1") \
        .config("spark.hadoop.fs.s3a.endpoint", s3_config["endpoint"]) \
        .config("spark.hadoop.fs.s3a.access.key", s3_config["access_key"]) \
        .config("spark.hadoop.fs.s3a.secret.key", s3_config["secret_key"]) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.sql.warehouse.dir", warehouse_dir) \
        .config("javax.jdo.option.ConnectionDriverName", "org.postgresql.Driver") \
        .config("javax.jdo.option.ConnectionURL", "jdbc:postgresql://host.docker.internal:5431/metastore") \
        .config("javax.jdo.option.ConnectionUserName", "postgres") \
        .config("javax.jdo.option.ConnectionPassword", "postgres") \
        .config("spark.sql.hive.metastore.version", "2.3.9") \
        .config("datanucleus.autoCreateSchema", "True") \
        .config("datanucleus.autoCreateTables", "True") \
        .enableHiveSupport() \
        .getOrCreate()


def write_to_hudi(df, table_name, s3_base_path, partitionpath = None, operation="upsert", recordkey="record_id", 
                  precombine="timestamp", mode="overwrite"):
    
    hudi_options = {
        "hoodie.table.name": table_name,
        "hoodie.datasource.write.recordkey.field": recordkey,
        "hoodie.datasource.write.table.name": table_name,
        "hoodie.datasource.write.operation": operation,
        "hoodie.datasource.write.precombine.field": precombine
    }
    if partitionpath is not None:
        hudi_options["hoodie.datasource.write.partitionpath.field"] = partitionpath

    print(f"Writing to Hudi table: {table_name} at {s3_base_path}")
    df.write.format("hudi") \
        .options(**hudi_options) \
        .mode(mode) \
        .save(s3_base_path)


def read_from_hudi(spark, base_path, table_name):
    s3_path = f"{base_path}/{table_name}"
    df = spark.read.format("hudi").load(s3_path)
    return df


def create_table(spark, tablename, path, schema="default"):
    spark.sql(f"""
      CREATE TABLE IF NOT EXISTS {schema}.{tablename}
      USING hudi
      LOCATION '{path}'
    """)
