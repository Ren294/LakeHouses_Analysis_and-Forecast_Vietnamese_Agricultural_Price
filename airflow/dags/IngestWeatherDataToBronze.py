from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException, AirflowFailException
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import pandas as pd
import requests
import re
import os
from bronzestage.common import create_spark_session, write_to_hudi
from bronzestage.config import get_province, get_api_key_weather, get_weather_baseurl, choice_column

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2010, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(days=1),
}

dag = DAG(
    "Bronze_weather_ingestion_dag",
    default_args=default_args,
    description="DAG thu thập và lưu trữ dữ liệu thời tiết",
    schedule_interval="@monthly",  
    catchup=True,
    max_active_runs=1
)

def get_weather_data(**kwargs):
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
    
    ti = kwargs['ti']
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
    else:
        file_path = f"/opt/airflow/data/weather_vietnam_{year}_{month:02d}.csv"
        weather_data.to_csv(file_path, index=False)
        ti.xcom_push(key="weather_csv_path", value=file_path)

def write_weather_data(**kwargs):
    ti = kwargs['ti']
    execution_date = kwargs['execution_date']
    year = execution_date.year
    file_path = ti.xcom_pull(key="weather_csv_path", task_ids="get_weather_data")
    print(f"[INFO] Starting spark session.")
    spark = create_spark_session("Write_Weather_Hudi")
    print(f"[INFO] Reading csv file.")
    spark_df = spark.read.option("header", True).csv(file_path)
    print(f"[INFO] Add timestamp to data.")
    spark_df = spark_df\
        .withColumn("timestamp", F.current_timestamp())\
        .withColumn("recordId", F.concat(F.col("cities"), F.lit("_"), F.col("datetime")))
    print(f"[INFO] Write to hudi.")
    write_to_hudi(spark_df, f"{year}", "s3a://bronze/weather_data",
                  recordkey="recordId", partitionpath="cities", mode="append")
    try:
        os.remove(file_path)
    except Exception as e:
        print(f"[ERROR]: Remove file fail {file_path} – {e}")
    spark.stop()



get_weather_task = PythonOperator(
    task_id="get_weather_data",
    python_callable=get_weather_data,
    provide_context=True,
    dag=dag,
)

write_weather_task = PythonOperator(
    task_id="write_weather_data",
    python_callable=write_weather_data,
    provide_context=True,
    dag=dag,
)

get_weather_task>>write_weather_task