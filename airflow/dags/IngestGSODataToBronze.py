from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from pyspark.sql import SparkSession
from bronzestage.common import create_spark_session, write_to_hudi
from bronzestage.config import get_url_gos

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "Bronze_gso_ingestion_dag",
    default_args=default_args,
    description="DAG thu thập và lưu trữ dữ liệu GSO",
    schedule_interval="@monthly",
    catchup=False,
)


def ingest_gso_data(**kwargs):
    path = kwargs["dag_run"].conf.get("path", "s3a://bronze/gso_data")
    spark = create_spark_session("Ingestion data from GSO")
    for crop, measure_url in get_url_gos().items():
        for measure, url in measure_url.items():
            if url is None:
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
                          f"{path}/{crop}", recordkey="cities", precombine="timestamp", partitionpath="cities")
            print(
                f"Data written to {measure} of {crop} table in MinIO successfully.")
    spark.stop()


ingest_gso_task = PythonOperator(
    task_id="ingest_gso_data",
    python_callable=ingest_gso_data,
    provide_context=True,
    dag=dag,
)

ingest_gso_task
