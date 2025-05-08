from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, concat, regexp_replace, to_timestamp, date_format, monotonically_increasing_id
import pyspark.sql.functions as F
from pyspark.sql.types import *
from functools import reduce
from .common import write_to_hudi, create_spark_session, create_table
from .config import get_selected_items_faostat
import pandas as pd
from datetime import datetime
import json
import requests


def read_data_crops():
    url = "https://faostatservices.fao.org/api/v1/en/definitions/types/item"
    response = requests.get(url)
    data = response.json()
    items = data.get("data", [])
    return pd.DataFrame(items)


def process_and_write_data(spark, path):
    item_filter_list = get_selected_items_faostat()
    schema = StructType([
        StructField("Domain_Code", StringType(), True),
        StructField("Domain", StringType(), True),
        StructField("Item_Code", StringType(), True),
        StructField("Item", StringType(), True),
        StructField("Description", StringType(), True),
        StructField("CPC_Code", StringType(), True),
        StructField("HS_Code", StringType(), True),
        StructField("HS07_Code", StringType(), True),
        StructField("HS12_Code", StringType(), True)
    ])
    input_df = spark.createDataFrame(read_data_crops(), schema=schema)

    filtered_df = (
        input_df
        .filter(F.col("Item").isin(item_filter_list))
        .select("Item", "Description", "Item_Code")
        .withColumnRenamed("Item", "Crops")
        .withColumnRenamed("Description", "Crop_Description")
        .withColumnRenamed("Item_Code", "Crops_Code")
    )
    write_to_hudi(filtered_df, "dim_crops", path,
                  recordkey="Crops_Code", precombine="Crops_Code")
    create_table(spark, "dim_crops", path)


def DimCropsGold(path):
    spark = create_spark_session("DimCrops")
    process_and_write_data(spark, path)
    spark.stop()
