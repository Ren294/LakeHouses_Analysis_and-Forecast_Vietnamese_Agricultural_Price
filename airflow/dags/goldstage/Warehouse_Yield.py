from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, concat, regexp_replace, to_timestamp, date_format, monotonically_increasing_id
import pyspark.sql.functions as F
from pyspark.sql.types import *
from functools import reduce
from .common import read_from_hudi, write_to_hudi, create_spark_session, create_table
from .config import get_code_crops


def GSOGold(path):
    spark = create_spark_session("Warehouse_Yield")
    code_crops_list = get_code_crops()
    df = read_from_hudi(spark, "s3a://silver", "gso_data/gso_merged")
    df_code_crops = spark.createDataFrame(
        code_crops_list, ["crop", "CropsCode"])
    df = df.join(df_code_crops, "crop")\
        .withColumn(
        "DateInt",
        F.concat(F.col("year"), F.lit("0101")).cast("int")
    )
    dim_provice = spark.sql("SELECT ProviceCode,ProviceName FROM default.dim_provice")\
        .withColumn("ProviceName", F.regexp_replace(F.col("ProviceName"), " ", ""))
    df = df.join(dim_provice, on=dim_provice.ProviceName == df.cities, how="inner")\
        .drop("crop").drop("year").drop("ProviceName").drop('cities').drop('record_id')\
        .withColumn(
        "recordId",
        F.concat_ws("_", F.col("DateInt"), F.col(
            "ProviceCode"), F.col("CropsCode"))
    )\
        .withColumnRenamed("production", "Production_ThousT_year") \
        .withColumnRenamed("area", "Area_ThousHa_year") \
        .withColumnRenamed("yield", "Yield_QuintalPerHa_year")
    write_to_hudi(df, "fact_yeild", path, recordkey="recordId",
                  partitionpath="ProviceCode", precombine="timestamp")
    create_table(spark, "fact_yield", "s3a://gold/warehouse/fact_yield")
    spark.stop()


# if __name__ == "__main__":
#     path = "s3a://gold/warehouse/fact_yield"
#     create_fact_yield(spark, path)
