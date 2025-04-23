from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, concat, regexp_replace, to_timestamp
import pyspark.sql.functions as F
from pyspark.sql.types import *
from functools import reduce
from .common import read_from_hudi, write_to_hudi, create_spark_session
from .config import get_selected_items_faostat, get_crops_gos


def read_data_for_crop(spark, crop, path_bronze):
    df_prod = spark.read.format("hudi").load(
        f"{path_bronze}{crop}/gso_production_{crop}_table")
    df_area = spark.read.format("hudi").load(
        f"{path_bronze}{crop}/gso_planted_area_{crop}_table")
    return df_prod, df_area


def process_year_columns(df, year_columns):
    for col_name in year_columns:
        df = df.withColumn(col_name, F.col(col_name).cast("float"))

    first_year = int(year_columns[0].split('_')[1])
    last_year = int(year_columns[-1].split('_')[1])

    year_pairs = [f"'{year}',year_{year}"
                  for year in range(first_year, last_year + 1)]
    year_string = f"stack({last_year - first_year + 1},\
      {', '.join(year_pairs)})"
    return df, year_string


def normalize_data(df_prod, df_area, crop, year_string):
    df_prod = df_prod.selectExpr(
        "cities", "crop", year_string + " as (year, production)")
    df_area = df_area.selectExpr(
        "cities", "crop", year_string + " as (year, area)")

    if crop == "peanut":
        df_prod = df_prod.withColumn("production", F.col("production") / 1000)
        df_area = df_area.withColumn("area", F.col("area") / 1000)
    if crop == "sugar_cane":
        df_area = df_area.withColumn("area", F.col("area") / 1000)
    if crop == "sweet_potatoes":
        df_area = df_area.withColumn("area",
                                     F.when(F.col("year") > 2017, F.col("area") / 1000).otherwise(F.col("area")))
    return df_prod, df_area


def merge_data(df_prod, df_area):
    df_area = df_area.withColumnRenamed(
        "cities", "cities_a").withColumnRenamed("year", "year_a")
    merged_df = df_prod.join(df_area.select("cities_a", "year_a", "area"),
                             (df_prod.cities == df_area.cities_a) & (
                                 df_prod.year == df_area.year_a),
                             "left")
    merged_df = merged_df.drop("year_a").drop("cities_a")
    return merged_df


def add_yield_column(spark, erged_df, crop, year_columns, path_bronze):
    try:
        df_yield = spark.read.format("hudi").load(
            f"{path_bronze}{crop}/gso_yield_{crop}_table")
        for col_name in year_columns:
            df_yield = df_yield.withColumn(
                col_name, F.col(col_name).cast("float"))

        first_year = int(year_columns[0].split('_')[1])
        last_year = int(year_columns[-1].split('_')[1])
        year_pairs = [f"'{year}', year_{year}"
                      for year in range(first_year, last_year + 1)]
        year_string = f"stack({last_year - first_year + 1}, \
          {', '.join(year_pairs)})"
        df_yield = df_yield.selectExpr(
            "cities", "crop", year_string + " as (year, yield)")

        df_yield = df_yield.withColumnRenamed(
            "cities", "cities_y").withColumnRenamed("year", "year_y")
        merged_df = merged_df.join(df_yield.select("cities_y", "year_y", "yield"),
                                   (merged_df.cities == df_yield.cities_y) & (
                                       merged_df.year == df_yield.year_y),
                                   "left").drop("year_y").drop("cities_y")
    except:
        merged_df = merged_df.withColumn("yield", F.round(
            F.col("production") * 10 / F.col("area"), 1))
    return merged_df


def finalize_data(final_df):
    return final_df.withColumn("cities", F.regexp_replace(final_df["cities"], " ", ""))\
        .withColumn("record_id", F.concat_ws("_", F.col("cities"), F.col("crop"), F.col("year")))\
        .withColumn("timestamp", F.current_timestamp())


def GSOSilver(path):
    spark = create_spark_session("CleanAndMerge_FaoStat")
    path_bronze = "s3a://bronze/gso_data/"
    final_df = None
    for crop in get_crops_gos():
        df_prod, df_area = read_data_for_crop(spark, crop, path_bronze)

        year_columns = [
            col for col in df_prod.columns if col.startswith('year_')]
        df_prod, year_string = process_year_columns(df_prod, year_columns)
        df_area, _ = process_year_columns(df_area, year_columns)

        df_prod, df_area = normalize_data(df_prod, df_area, crop, year_string)

        merged_df = merge_data(df_prod, df_area)
        merged_df = add_yield_column(
            spark, merged_df, crop, year_columns, path_bronze)

        final_df = merged_df if final_df is None else final_df.union(merged_df)

    final_df = finalize_data(final_df)

    write_to_hudi(final_df, "gso_merged", path, partitionpath="cities")
    spark.stop()


# if __name__ == "__main__":
#     path = "s3a://silver/gso_data"
#     main(spark, path)
