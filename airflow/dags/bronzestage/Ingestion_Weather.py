from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, FloatType
import faostat
from pyspark.sql import DataFrame
from .common import create_spark_session, write_to_hudi
from .config import get_province, get_api_key_weather, get_weather_baseurl, choice_column
import requests
from datetime import datetime, timedelta
import re
from pyspark.sql.utils import AnalysisException
import pandas as pd
from airflow.exceptions import AirflowSkipException, AirflowFailException

def WeatherBronze(path, **kwargs):
    def fetch_with_keys(province, date):
        for idx, key in enumerate(api_keys):
        
            url = f"{get_weather_baseurl()}{province}/{date}/{date}?unitGroup=metric&include=days&key={key}&contentType=csv"

            print(f"[INFO] Fetching weather data in {date}...")
            print(f"[INFO] Using API Key #{idx+1} ({key[:4]}...)")

            response = requests.get(url)

            if response.status_code == 200:        
                return response.text
            elif response.status_code == 429:
                print(f"[WARNING] API key #{idx+1} out of requests (429). Trying next key.")
            else:
                print(f"[ERROR] Key #{idx+1}: status code {response.status_code}")
                continue
        return None
    
    execution_date = kwargs['execution_date']
    year = execution_date.year
    month = execution_date.month
    date = f"{year}-{month:02d}-01"
    
    api_keys = get_api_key_weather()
    columns = choice_column()
    weather_data = pd.DataFrame(columns=columns)
    failed_provinces = []
    count = 0
    for province, province_name in get_province().items():
        data = fetch_with_keys(province_name, date)
        if data:
            csv_data = data.splitlines()
            row_data = re.split(r',(?=(?:[^"\"]*"[^"]*")*[^"]*$)', csv_data[1])
            row_dict = {columns[j]: row_data[j] if j < len(
                    row_data) else None for j in range(len(columns))}
            row_dict["cities"] = province_name
            weather_data = pd.concat([weather_data, pd.DataFrame([row_dict])], ignore_index=True)
            count += 1
            print(f"[SUCCESS] Complet fetch data in '{province_name}'({count}/63).")
        else:
            failed_provinces.append(province_name)

    if failed_provinces:
        raise AirflowFailException(
            f"[FAIL] Out of requests: {', '.join(failed_provinces)}."
        )
    
    print(f"[INFO] Starting spark session.")
    spark = create_spark_session("Write_Weather_Hudi")
    spark_df = spark.createDataFrame(weather_data)

    print(f"[INFO] Add timestamp to data.")
    spark_df = spark_df\
        .withColumn("timestamp", F.current_timestamp())\
        .withColumn("recordId", F.concat(F.col("cities"), F.lit("_"), F.col("datetime")))
    print(f"[INFO] Write to hudi.")
    write_to_hudi(spark_df, f"{year}", path,
                  recordkey="recordId", partitionpath="cities", mode="append")
    spark.stop()



# if __name__ == "__main__":
#     spark = create_spark_session("Ingestion_Weather")
#     path = "s3a://bronze/weather_data"
#     main(spark, path)
#     spark.stop()
