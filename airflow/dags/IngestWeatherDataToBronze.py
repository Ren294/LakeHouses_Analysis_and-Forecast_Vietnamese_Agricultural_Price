from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
import pandas as pd
import requests
import re
from bronzestage.common import create_spark_session, write_to_hudi
from bronzestage.config import get_province, get_api_key_weather, get_weather_baseurl

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "Bronze_weather_ingestion_dag",
    default_args=default_args,
    description="DAG thu thập và lưu trữ dữ liệu thời tiết",
    schedule_interval="@daily",
    catchup=False,
)


def check_file_exist(**kwargs):
    spark = create_spark_session("Check_Weather_File")
    path = kwargs["dag_run"].conf.get("path", "s3a://bronze/weather_data")
    year = 2023
    while year > 1990:
        try:
            spark.read.text(f"{path}/{year}")
            year -= 1
        except:
            break
    kwargs['ti'].xcom_push(key='last_year', value=year)
    spark.stop()


def retrieve_weather_data(**kwargs):
    ti = kwargs['ti']
    year = ti.xcom_pull(task_ids='check_file_exist', key='last_year')
    api_keys = get_api_key_weather()
    path = kwargs["dag_run"].conf.get("path", "s3a://bronze/weather_data")
    columns = ["cities", "datetime", "temp",
               "humidity", "windgust", "conditions"]
    weather_data = pd.DataFrame(columns=columns)

    for api_key in api_keys:
        for province, province_name in get_province().items():
            for month in range(1, 13):
                date = f"{year}-{month:02}-01"
                url = f"{get_weather_baseurl()}{province}/{date}/{date}?unitGroup=metric&include=days&key={api_key}&contentType=csv"
                response = requests.get(url)
                if response.status_code != 200:
                    continue
                csv_data = response.text.splitlines()
                for i, row in enumerate(csv_data[1:]):
                    row_data = re.split(r',(?=(?:[^"\"]*"[^"]*")*[^"]*$)', row)
                    weather_data = pd.concat([weather_data, pd.DataFrame([{columns[j]: row_data[j] if j < len(
                        columns) else None for j in range(len(columns))}])], ignore_index=True)

    ti.xcom_push(key='weather_data', value=weather_data.to_json())


def write_weather_data(**kwargs):
    ti = kwargs['ti']
    year = ti.xcom_pull(task_ids='check_file_exist', key='last_year')
    path = kwargs["dag_run"].conf.get("path", "s3a://bronze/weather_data")
    weather_data_json = ti.xcom_pull(
        task_ids='retrieve_weather_data', key='weather_data')
    weather_data = pd.read_json(weather_data_json)

    spark = create_spark_session("Write_Weather_Hudi")
    spark_df = spark.createDataFrame(weather_data)
    spark_df = spark_df.withColumn("timestamp", F.current_timestamp())
    write_to_hudi(spark_df, f"{year}", path,
                  recordkey="datetime", partitionpath="cities")

    spark.stop()


check_file_task = PythonOperator(
    task_id="check_file_exist",
    python_callable=check_file_exist,
    provide_context=True,
    dag=dag,
)

retrieve_weather_task = PythonOperator(
    task_id="retrieve_weather_data",
    python_callable=retrieve_weather_data,
    provide_context=True,
    dag=dag,
)

write_weather_task = PythonOperator(
    task_id="write_weather_data",
    python_callable=write_weather_data,
    provide_context=True,
    dag=dag,
)

check_file_task >> retrieve_weather_task >> write_weather_task
