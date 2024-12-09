from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, concat, regexp_replace, to_timestamp, date_format, monotonically_increasing_id
import pyspark.sql.functions as F
from pyspark.sql.types import *
from functools import reduce
from common import read_from_hudi, write_to_hudi, create_spark_session, create_table


def create_fact_weather(spark, path):
    spark_df = read_from_hudi(
        spark, "s3a://silver/weather_data", "weather_merged")
    dim_provice = spark.sql(
        "SELECT ProviceCode,ProviceName FROM default.dim_provice")
    spark_df = spark_df.join(dim_provice, on="ProviceName", how="inner")
    spark_df = spark_df.withColumn("DateInt", F.regexp_replace(
        F.date_format("DateTime", "yyyyMMdd"), "-", "").cast("int"))
    spark_df = spark_df.drop("ProviceName").drop("recordId")
    spark_df = spark_df.withColumn("recordId", F.concat(
        F.col("ProviceCode"), F.lit("_"), F.col("DateInt")))
    spark_df
    write_to_hudi(spark_df, "fact_weather", path, partitionpath="ProviceCode",
                  recordkey="recordId", precombine="Datetime")
    create_table(spark, "fact_weather", path)


if __name__ == "__main__":
    spark = create_spark_session("Warehouse_Weather")
    path = "s3a://gold/warehouse/fact_weather"
    create_fact_weather(spark, path)
    spark.stop()
