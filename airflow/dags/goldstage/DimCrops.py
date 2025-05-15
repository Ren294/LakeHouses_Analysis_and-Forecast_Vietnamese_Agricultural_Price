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

    crop_category_map = {
        "Plums, dried": "Fruits",
        "Groundnuts, shelled": "Nuts and Oil Seeds",
        "Maize (corn)": "Staple Crops",
        "Mushrooms and truffles": "Fungi",
        "Apples": "Fruits",
        "Sweet potatoes": "Staple Crops",
        "Cucumbers and gherkins": "Vegetables",
        "Cashew nuts, in shell": "Nuts and Oil Seeds",
        "Beans, dry": "Leguminous Crops",
        "Plums and sloes": "Fruits",
        "Bananas": "Fruits",
        "Avocados": "Fruits",
        "Groundnuts, excluding shelled": "Nuts and Oil Seeds",
        "Carrots and turnips": "Vegetables",
        "Tangerines, mandarins, clementines": "Fruits",
        "Chillies and peppers, dry (Capsicum spp., Pimenta spp.), raw": "Spices and Aromatics",
        "Grapes": "Fruits",
        "Mangoes, guavas and mangosteens": "Fruits",
        "Onions and shallots, dry (excluding dehydrated)": "Vegetables",
        "Coffee, green": "Industrial Crops",
        "Cashew nuts, shelled": "Nuts and Oil Seeds",
        "Tea leaves": "Industrial Crops",
        "Coconuts, in shell": "Nuts and Oil Seeds",
        "Sesame seed": "Nuts and Oil Seeds",
        "Papayas": "Fruits",
        "Potatoes": "Staple Crops",
        "Watermelons": "Fruits",
        "Sugar cane": "Industrial Crops",
        "Cabbages": "Vegetables",
        "Cauliflowers and broccoli": "Vegetables",
        "Strawberries": "Fruits",
        "Rice": "Staple Crops",
        "Yams": "Staple Crops",
        "Coconuts, desiccated": "Nuts and Oil Seeds",
        "Pepper (Piper spp.), raw": "Spices and Aromatics",
        "Soya beans": "Leguminous Crops",
        "Ginger, raw": "Spices and Aromatics",
        "Cassava, fresh": "Staple Crops",
        "Pomelos and grapefruits": "Fruits",
        "Pears": "Fruits",
        "Oranges": "Fruits",
        "Leeks and other alliaceous vegetables": "Vegetables",
        "Tomatoes": "Vegetables",
        "Eggplants (aubergines)": "Vegetables",
        "Pineapples": "Fruits",
        "Green garlic": "Spices and Aromatics",
        "Cashew nuts, shelled": "Nuts and Oil Seeds",
        "Cashew nuts, in shell": "Nuts and Oil Seeds",
        "Sweet potatoes": "Staple Crops",
        "Bananas": "Fruits",
        "Sugar cane": "Industrial Crops"
    }

    # Convert to broadcast variable or mapping expression
    mapping_expr = F.create_map([lit(x)
                                for x in sum(crop_category_map.items(), ())])

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
        .withColumn("Category", mapping_expr.getItem(F.col("Crops")))
    )

    write_to_hudi(filtered_df, "dim_crops", path,
                  recordkey="Crops_Code", precombine="Crops_Code")
    create_table(spark, "dim_crops", path)


def DimCropsGold(path):
    spark = create_spark_session("DimCrops")
    process_and_write_data(spark, path)
    spark.stop()
