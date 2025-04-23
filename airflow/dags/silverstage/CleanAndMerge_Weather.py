from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, concat, regexp_replace, to_timestamp
import pyspark.sql.functions as F
from pyspark.sql.types import *
from functools import reduce
from .common import read_from_hudi, write_to_hudi, create_spark_session


def WeatherSilver(path):
    spark = create_spark_session("CleanAndMerge_Weather")
    final_df = None

    year = 2023
    while (year > 1990):
        try:
            spark_df = read_from_hudi(spark, path, year)
            final_df = spark_df if final_df is None else final_df.union(
                spark_df)
            print(f"Complet merge weather table in {year}")
            year = year - 1
        except:
            break

    columns = ["cities", "datetime", "tempmax", "tempmin", "temp", "windgust", "windspeed",
               "winddir", "dew", "humidity", "precip", "precipprob", "precipcover", "uvindex", "solarenergy", "severerisk"]

    final_df = final_df.select(columns)\
        .selectExpr([f"`{col}` as `{col.capitalize()}`" for col in columns])\
        .withColumnRenamed("Cities", "ProviceName")\
        .withColumn("ProviceName", F.regexp_replace(F.col("ProviceName"), "Đ", "D"))\
        .withColumn("recordId", F.concat(F.col("ProviceName"), F.col("Datetime")))
    final_df.printSchema()
    write_to_hudi(final_df, "weather_merged", path,
                  partitionpath="ProviceName", precombine="Datetime", recordkey="recordId")
    spark.stop()


# if __name__ == "__main__":
#     spark = create_spark_session("CleanAndMerge_Weather")
#     path = "s3a://silver/weather_data"
#     main(spark, path)
#     spark.stop()
