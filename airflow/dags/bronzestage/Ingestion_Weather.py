from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, FloatType
import faostat
from pyspark.sql import DataFrame
from common import create_spark_session, write_to_hudi
from config import get_province, get_api_key_weather, get_weather_baseurl
import requests
from datetime import datetime, timedelta
import re
from pyspark.sql.utils import AnalysisException
import pandas as pd


def check_file_exist(spark, year, path):
    while (year > 1990):
        try:
            df = spark.read.text(path + f"/{year}")
            year = year - 1
        except AnalysisException as e:
            break
    return year


def choice_column():
    return [
        "cities", "datetime", "tempmax", "tempmin", "temp", "feelslikemax",
        "feelslikemin", "feelslike", "dew", "humidity", "precip", "precipprob",
        "precipcover", "preciptype", "snow", "snowdepth", "windgust", "windspeed",
        "winddir", "sealevelpressure", "cloudcover", "visibility", "solarradiation",
        "solarenergy", "uvindex", "severerisk", "sunrise", "sunset", "moonphase",
        "conditions", "description", "icon", "stations"
    ]


def retrieve_weather_data(year, weather_data, api_key, columns):
    months = [f"{year}-{month:02}" for month in range(1, 13)]
    for province, province_name in get_province().items():
        for month in months:
            try:
                date = f"{month}-01"
                url = f"{get_weather_baseurl()}\
                  {province}/{date}/{date}?unitGroup=metric&include=days&key={api_key}&contentType=csv".replace(" ", "")
                response = requests.get(url)
                if response.status_code != 200:
                    print(f"Failed to retrieve data for {province} on \
                      {date}: Status code {response.status_code}")
                    continue
                csv_data = response.text.splitlines()
                temp_df = pd.DataFrame(columns=columns)
                for i, row in enumerate(csv_data):
                    if i == 0:
                        continue
                    row_data = re.split(r',(?=(?:[^"]*"[^"]*")*[^"]*$)', row)
                    row_dict = {columns[j]: row_data[j] if j < len(
                        row_data) else None for j in range(len(columns))}
                    row_dict["cities"] = province_name
                    temp_df = pd.concat(
                        [temp_df, pd.DataFrame([row_dict])], ignore_index=True)

                weather_data = pd.concat(
                    [weather_data, temp_df], ignore_index=True)

            except Exception as e:
                print(f"An error occurred while retrieving data for \
                  {province} on {month}: {e}")
    return weather_data


def WeatherBronze(path):
    spark = create_spark_session("Ingestion_Weather")
    year = check_file_exist(spark, 2023, path)
    for api_key in get_api_key_weather():
        columns = choice_column()
        weather_data = pd.DataFrame(columns=columns)
        retrieve_weather_data(year, weather_data, api_key, columns)
        spark_df = spark.createDataFrame(weather_data)\
            .withColumn("timestamp", F.current_timestamp())\
            .withColumn("recordId", F.concat(F.col("cities"), F.lit("_"), F.col("datetime")))
        write_to_hudi(spark_df, f"{year}", path,
                      recordkey="recordId", partitionpath="cities")
        print(f"Data written weather {year} table in MinIO successfully.")
        year -= 1
    spark.stop()


# if __name__ == "__main__":
#     spark = create_spark_session("Ingestion_Weather")
#     path = "s3a://bronze/weather_data"
#     main(spark, path)
#     spark.stop()
