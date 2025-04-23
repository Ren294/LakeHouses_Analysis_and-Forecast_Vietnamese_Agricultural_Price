from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, concat, regexp_replace, to_timestamp
import pyspark.sql.functions as F
from pyspark.sql.types import *
from functools import reduce
from common import read_from_hudi, write_to_hudi, create_spark_session
from config import get_selected_items_faostat


def create_pivot(df, suffix="year"):

    group_by_columns = ["AreaCode", "Area", "ItemCode", "Item", "Year"]
    if suffix != "year":
        group_by_columns.append("Months")

    df = df.withColumn("element_unit",
                       F.concat(
                           F.regexp_replace(
                               "Element", "[^a-zA-Z0-9]+", ""),
                           F.lit("_I_"),
                           F.col("ElementCode"),
                           F.lit("_I_"),
                           F.col("Unit"),
                           F.lit("_I_"),
                           F.lit(suffix)
                       ))

    pivot_df = df.groupBy(group_by_columns) \
        .pivot("element_unit") \
        .agg(F.first("Value"))

    return pivot_df


def process_and_merge_tables(spark, base_path, selected_items):

    # Read all tables
    production_clp = read_from_hudi(spark, "production_clp", base_path)
    production_pi = read_from_hudi(spark, "production_pi", base_path)
    production_vap = read_from_hudi(spark, "production_vap", base_path)
    trade_clp = read_from_hudi(spark, "trade_clp", base_path)
    price_pp = read_from_hudi(spark, "price_pp", base_path)

    production_clp = production_clp.filter(col("Item").isin(selected_items))
    production_pi = production_pi.filter(col("Item").isin(selected_items))
    production_vap = production_vap.filter(col("Item").isin(selected_items))
    trade_clp = trade_clp.filter(col("Item").isin(selected_items))
    price_pp = price_pp.filter(col("Item").isin(selected_items))

    clp_pivot = create_pivot(production_clp)
    pi_pivot = create_pivot(production_pi)
    vap_pivot = create_pivot(production_vap)
    trade_pivot = create_pivot(trade_clp)

    price_pivot = create_pivot(price_pp, suffix="month")

    yearly_dfs = [clp_pivot, pi_pivot, vap_pivot, trade_pivot]
    merged_yearly = reduce(lambda df1, df2:
                           df1.join(df2, ["AreaCode", "Area",
                                    "ItemCode", "Item", "Year"], "outer"),
                           yearly_dfs)

    merged_df = merged_yearly.crossJoin(price_pivot.select("Months").distinct()) \
        .join(price_pivot, ["AreaCode", "Area", "ItemCode", "Item", "Year", "Months"], "outer")

    month_mapping = {
        "January": "01", "February": "02", "March": "03", "April": "04",
        "May": "05", "June": "06", "July": "07", "August": "08",
        "September": "09", "October": "10", "November": "11", "December": "12",
        "Annual value": "01"
    }

    month_map_expr = F.create_map([F.lit(x)
                                  for x in sum(month_mapping.items(), ())])

    merged_df = merged_df.withColumn(
        "timestamp",
        F.to_timestamp(
            F.concat(
                F.col("Year").cast("string"),
                F.lit("-"),
                F.coalesce(month_map_expr[F.col("Months")], F.lit("01")),
                F.lit("-01")
            )
        )
    )

    merged_df = merged_df.withColumn(
        "record_id",
        F.concat(
            F.col("AreaCode").cast("string"),
            F.lit("_"),
            F.col("ItemCode").cast("string"),
            F.lit("_"),
            F.col("Year").cast("string"),
            F.lit("-"),
            F.col("Months")
        )
    )
    merged_df = merged_df.withColumn("ElementCode", F.lit(999))
    return merged_df


def sanitize_column_names(df):
    sanitized_column_names = {col_name: re.sub(
        r"[^a-zA-Z0-9]", "_", col_name) for col_name in df.columns}

    for old_name, new_name in sanitized_column_names.items():
        df = df.withColumnRenamed(old_name, new_name)

    return df


def FaoStatSilver(path):
    spark = create_spark_session("CleanAndMerge_FaoStat")
    selected_items = get_selected_items_faostat()
    merged_df = process_and_merge_tables(spark, path, selected_items)
    merged_df = sanitize_column_names(merged_df)
    # merged_df.write.csv("/Users/ren/Downloads/Data/faostat/data/test3")
    write_to_hudi(merged_df, "faostat_merged",
                  path, partitionpath="ElementCode")
    spark.stop()
    print("Successfully merged and saved FAOSTAT data")


# if __name__ == "__main__":
#     path = "s3a://silver/faostat_data"
#     main(spark, path)
