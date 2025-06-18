from pyspark.sql import SparkSession
from .config import get_config_minio
import trino
from minio import Minio

import numpy as np
from sklearn.preprocessing import MinMaxScaler
from sklearn.linear_model import LinearRegression

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


def read_warehouse_pandas(spark, query):
    return spark.sql(f"{query}").toPandas()

def read_from_hudi(spark, base_path, table_name):
    s3_path = f"{base_path}/{table_name}"
    df = spark.read.format("hudi").load(s3_path)
    return df

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
    
def create_table(spark, tablename, path, schema="default"):
    spark.sql(f"""
      CREATE TABLE IF NOT EXISTS {schema}.{tablename}
      USING hudi
      LOCATION '{path}'
    """)

def trino_connection():
    return trino.dbapi.connect(
        host='host.docker.internal',      
        port=8080,                      
        user='admin',
        catalog='hive',        
        schema='default',          
    )   

def minio_client(bucket_name):
    client = Minio(
        endpoint="host.docker.internal:9000",
        access_key=s3_config["access_key"],
        secret_key=s3_config["secret_key"],
        secure=False  
    )
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)

    return client

def detrend(time_series, trend_model = None):
    time = np.arange(len(time_series))
    X_trend = np.vstack([time, time**2]).T
    if trend_model is None:
        trend_model = LinearRegression().fit(X_trend, time_series)
    trend_fit = trend_model.predict(X_trend)
    time_series_detrended = time_series - trend_fit
    return time_series_detrended, trend_model

def deseason(time_series, seasonal_avg = None, period = 12):
    if seasonal_avg is None:
        seasonal_avg = np.array([time_series[i::period].mean() for i in range(period)])
    time_series_deseasoned = time_series - \
        np.tile(seasonal_avg, len(time_series) // period + 1)[:len(time_series)]
    return time_series_deseasoned, seasonal_avg

def scale_minmax(time_series, scaler = None):
    if time_series.ndim == 1:
        time_series = time_series.reshape(-1, 1)
    if scaler is None:
        scaler = MinMaxScaler()
        time_series_scaled = scaler.fit_transform(time_series)
    else:
        time_series_scaled = scaler.transform(time_series)
    return time_series_scaled , scaler

def inverse_transform(time_series, scaler):
    if time_series.ndim == 1:
        time_series = time_series.reshape(-1, 1)
        return scaler.inverse_transform(np.array(time_series)).flatten()
    return scaler.inverse_transform(np.array(time_series))

def add_season(time_series, seasonal_avg, period = 12):
    seasonal_pattern = np.tile(seasonal_avg, len(time_series) // period + 1)[:len(time_series)]
    return time_series + seasonal_pattern

def add_trend(time_series, trend_model, pedict_indx, prediction_length = 24 ):
    future_time = np.arange(pedict_indx, pedict_indx + prediction_length)
    future_X_trend = np.vstack([future_time, future_time**2]).T
    future_trend = trend_model.predict(future_X_trend)
    return time_series + future_trend

def scale_custom_range(time_series):
    time_series = np.array(time_series, dtype=np.float32)
    original_first = time_series[0]
    scale_min = 0.02 * original_first
    scale_max = original_first

    data_min = time_series.min()
    data_max = time_series.max()

    # Min-max scaling về [scale_min, scale_max]
    scaled_series = scale_min + (time_series - data_min) * (scale_max - scale_min) / (data_max - data_min)

    return np.array(scaled_series)
       