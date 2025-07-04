from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, concat, regexp_replace, to_timestamp, date_format, monotonically_increasing_id
import pyspark.sql.functions as F
from pyspark.sql.types import *
from functools import reduce
from common import write_to_hudi, create_spark_session, create_table
from config import get_province
import pandas as pd
from datetime import datetime
import json


def create_dim_province(spark, path):
    df = pd.read_csv('vietnamprovince.csv', sep='\t', header=None)
    df.columns = ['ProvinceNameUtf8', 'ProvinceCode', 'Center', 'Area',
                  'Population', 'Density', 'Urban', 'HDI', 'GDP', 'Region']
    provinces = get_province()
    df["ProvinceName"] = [provinces[name] for name in df['ProvinceNameUtf8']]
    df['Timestamp'] = datetime.now()
    df['Area'] = df['Area'].replace({',': ''}, regex=True).astype(float)
    df['ProvinceCode'] = [f"{code:02}" for code in df['ProvinceCode']]
    df = df.drop(['Density', 'Urban', 'HDI', 'GDP',
                 'Population', 'Center'], axis=1)

    spark_df = spark.createDataFrame(df)
    write_to_hudi(spark_df, "dim_province", path, recordkey="ProvinceCode",
                  partitionpath="Region", precombine="Timestamp")
    create_table(spark, "dim_province", path)


if __name__ == "__main__":
    spark = create_spark_session("Warehouse_FaoStat")
    path = "s3a://gold/warehouse/dim_province"
    create_dim_province(spark, path)
    spark.stop()
