from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, concat, regexp_replace, to_timestamp
import pyspark.sql.functions as F
from pyspark.sql.types import *
from functools import reduce
from .common import read_from_hudi, write_to_hudi, create_spark_session


def WeatherSilver(inputpath, outputpath):
    spark = create_spark_session("CleanAndMerge_Weather")
    final_df = None

    year = 2025
    while (year > 1990):
        try:
            spark_df = read_from_hudi(spark, year, inputpath)
            final_df = spark_df if final_df is None else final_df.union(
                spark_df)
            print(f"Complet merge weather table in {year}")
            year = year - 1
        except Exception as e:
            print(f"Error in year {year}: {e}")
            year = year - 1

    columns = ["cities", "datetime", "tempmax", "tempmin", "temp", "windgust", "windspeed",
               "winddir", "dew", "humidity", "precip", "precipprob", "precipcover", "uvindex", "solarenergy", "severerisk"]

    final_df = final_df\
        .select(
            col("cities").cast("string"),
            col("datetime").cast("string"),
            col("tempmax").cast("float"),
            col("tempmin").cast("float"),
            col("temp").cast("float"),
            col("windgust").cast("float"),
            col("windspeed").cast("float"),
            col("winddir").cast("float"),
            col("dew").cast("float"),
            col("humidity").cast("float"),
            col("precip").cast("float"),
            col("precipprob").cast("float"),
            col("precipcover").cast("float"),
            col("uvindex").cast("int"),
            col("solarenergy").cast("float"),
            col("severerisk").cast("int")
        )\
        .selectExpr([f"`{col}` as `{col.capitalize()}`" for col in columns])\
        .withColumnRenamed("Cities", "ProvinceName")\
        .withColumn("ProvinceName", F.regexp_replace(F.col("ProvinceName"), "Đ", "D"))\
        .withColumn("recordId", F.concat(F.col("ProvinceName"),F.lit("_"),  F.col("Datetime")))
    write_to_hudi(final_df, "weather_merged", outputpath,
                  partitionpath="ProvinceName", precombine="Datetime", recordkey="recordId")
    spark.stop()


# if __name__ == "__main__":
#     spark = create_spark_session("CleanAndMerge_Weather")
#     path = "s3a://silver/weather_data"
#     main(spark, path)
#     spark.stop()
