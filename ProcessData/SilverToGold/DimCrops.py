from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, concat, regexp_replace, to_timestamp, date_format, monotonically_increasing_id
import pyspark.sql.functions as F
from pyspark.sql.types import *
from functools import reduce
from common import write_to_hudi, create_spark_session, create_table
from config import get_selected_items_faostat
import pandas as pd
from datetime import datetime
import json
import faostat


def read_data_crops():
    all_items = []
    datasets = faostat.list_datasets()

    for dataset in datasets[1:]:
        code = dataset[0]
        label = dataset[1]
        print(f"Processing dataset: {label} (Code: {code})")
        try:
            items = faostat.get_par_df(code, 'item')
            items['dataset_code'] = code
            items['dataset_label'] = label
            all_items.append(items)
        except Exception as e:
            print(f"Lỗi khi lấy item cho dataset {label}: {e}")
    return pd.concat(all_items, ignore_index=True)


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


if __name__ == "__main__":
    path = "s3a://gold/warehouse/dim_crops"
    spark = create_spark_session("DimCrops")
    process_and_write_data(spark, path)
    spark.stop()
