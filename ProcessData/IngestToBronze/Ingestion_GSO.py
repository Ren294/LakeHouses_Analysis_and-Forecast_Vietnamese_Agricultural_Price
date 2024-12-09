from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, FloatType
import faostat
from pyspark.sql import DataFrame
from common import create_spark_session, write_to_hudi
from config import get_url_gos
import pandas as pd
from datetime import datetime
import json


def main(spark, path):
    for crop, measure_url in get_url_gos().items():
        for measure, url in measure_url.items():
            if url == None:
                continue
            data = pd.read_csv(url, skiprows=1)
            df = pd.DataFrame(data)
            df.replace("..", 0, inplace=True)
            df.rename(columns={'Cities, provincies': 'cities'}, inplace=True)
            df.columns = df.columns.str.replace('Prel. ', '', regex=False)
            df.columns = [df.columns[0]] + \
                [f"year_{col}" for col in df.columns[1:]]
            df['timestamp'] = datetime.now()
            df['source'] = url
            df['crop'] = crop
            df['measure'] = measure
            spark_df = spark.createDataFrame(df)
            write_to_hudi(spark_df, f"gso_{measure}_{crop}_table",
                          f"{path}/{crop}", recordkey="cities", precombine="datetime", partitionpath="cities")
            print(f"Data written to {measure} of \
              {crop} table in MinIO successfully.")


if __name__ == "__main__":
    spark = create_spark_session("Ingestion data from GSO")
    path = "s3a://bronze/gso_data"
    main(spark, path)
    spark.stop()
