from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pyspark.sql import functions as F
import faostat
from bronzestage.common import create_spark_session, write_to_hudi
from bronzestage.config import get_parameter_sets_faostat

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "Bronze_faostat_ingestion_dag",
    default_args=default_args,
    description="DAG thu thập và lưu trữ dữ liệu FAOStat",
    schedule_interval="@monthly",
    catchup=False,
)


def load_data_to_hudi(**kwargs):
    params = kwargs["params"]
    path = kwargs["dag_run"].conf.get("path", "s3a://bronze/faostat_data")
    spark = create_spark_session(
        f"FaoStat {params['table_name']} to Hudi on MinIO")
    vietnam_code = 237
    mypars = {"area": vietnam_code}
    data = faostat.get_data_df(
        params["domain_code"], pars=mypars, strval=False)
    df = spark.createDataFrame(data)
    for col in df.columns:
        df = df.withColumnRenamed(col, col.replace(" ", ""))
    df = df.withColumn("record_id", F.concat_ws("_", *[F.col(col) for col in params["record_id_cols"]])) \
           .withColumn("timestamp", F.to_timestamp(F.col("Year").cast("string"), "yyyy"))
    df = df.na.drop(subset=["record_id"])
    df = df.withColumn("Year", F.col("Year").cast("int"))
    df = df.withColumn("Value", F.col("Value").cast("float"))
    write_to_hudi(df, params["table_name"], path, partitionpath="ElementCode")
    spark.stop()


def process_all_datasets(**kwargs):
    parameter_sets = get_parameter_sets_faostat()
    for params in parameter_sets:
        load_data_to_hudi(params=params, dag_run=kwargs['dag_run'])


load_faostat_task = PythonOperator(
    task_id="load_faostat_data",
    python_callable=process_all_datasets,
    provide_context=True,
    dag=dag,
)

load_faostat_task
