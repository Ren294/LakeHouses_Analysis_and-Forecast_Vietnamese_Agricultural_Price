from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pyspark.sql import functions as F
import faostat
from bronzestage.Ingestion_Weather import WeatherBronze
from silverstage.CleanAndMerge_Weather import WeatherSilver
from goldstage.Warehouse_Weather import WeatherGold
from config import Weather_paths

weather_paths = Weather_paths()

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "Weather_dag",
    default_args=default_args,
    description="Collect, process and store GSO data",
    schedule_interval="@monthly",
    catchup=False,
)

bronze_weather = PythonOperator(
    task_id="load_weather_bronze",
    python_callable=WeatherBronze,
    provide_context=True,
    op_kwargs={"path": weather_paths["Weather_bronze_path"]},
    dag=dag,
)

silver_weather = PythonOperator(
    task_id="load_weather_silver",
    python_callable=WeatherSilver,
    provide_context=True,
    op_kwargs={"path": weather_paths["Weather_silver_path"]},
    dag=dag,
)

gold_weather = PythonOperator(
    task_id="load_weather_gold",
    python_callable=WeatherGold,
    provide_context=True,
    op_kwargs={"path": weather_paths["Weather_gold_path"]},
    dag=dag,
)

bronze_weather >> silver_weather >> gold_weather
