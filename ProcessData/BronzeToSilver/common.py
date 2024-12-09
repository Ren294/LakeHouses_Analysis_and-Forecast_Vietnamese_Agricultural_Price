from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, FloatType
import faostat
from pyspark.sql import DataFrame
from config import get_config_minio

s3_config = get_config_minio()


def create_spark_session(appName):
    spark = SparkSession.builder \
        .appName(appName) \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
        .config("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar") \
        .config("spark.jars.packages",
                "org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0,"
                "org.apache.hadoop:hadoop-aws:3.3.1,"
                "com.amazonaws:aws-java-sdk-bundle:1.11.1026") \
        .config("spark.hadoop.fs.s3a.endpoint", s3_config["endpoint"]) \
        .config("spark.hadoop.fs.s3a.access.key", s3_config["access_key"]) \
        .config("spark.hadoop.fs.s3a.secret.key", s3_config["secret_key"]) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
    return spark


def write_to_hudi(df, table_name, s3_base_path, partitionpath, operation="upsert", recordkey="record_id", precombine="timestamp"):
    hudi_options = {
        "hoodie.table.name": table_name,
        "hoodie.datasource.write.recordkey.field": recordkey,
        "hoodie.datasource.write.table.name": table_name,
        "hoodie.datasource.write.operation": operation,
        "hoodie.datasource.write.precombine.field": precombine,
        "hoodie.datasource.write.partitionpath.field": partitionpath
    }

    s3_path = f"{s3_base_path}/{table_name}"

    df.write.format("hudi") \
        .options(**hudi_options) \
        .mode("overwrite") \
        .save(s3_path)


def read_from_hudi(spark, base_path, table_name):
    s3_path = f"{base_path}/{table_name}"

    df = spark.read.format("hudi").load(s3_path)

    return df
