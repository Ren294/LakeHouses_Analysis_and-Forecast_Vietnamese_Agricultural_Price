from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pyspark.sql import functions as F
import faostat
from goldstage.DimDate import DimDateGold
from config import Dim_paths

dim_paths = Dim_paths()

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "DimDate_dag",
    default_args=default_args,
    description="Create dimdate data",
    schedule_interval="@monthly",  
    catchup=False,
    max_active_runs=1
)


create_dimdate_task = PythonOperator(
    task_id="create_dimdate_task",
    python_callable=DimDateGold,
    provide_context=True,
    op_kwargs={"path": dim_paths["Dim_Date_path"]},
    dag=dag,
)
