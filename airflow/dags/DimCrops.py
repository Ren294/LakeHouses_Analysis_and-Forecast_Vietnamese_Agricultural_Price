from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pyspark.sql import functions as F
import faostat
from goldstage.DimCrops import DimCropsGold
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
    "DimCrops_dag",
    default_args=default_args,
    description="Create dimcrops data",
    schedule_interval="@monthly",  
    catchup=False,
    max_active_runs=1
)


create_dimcrops_task = PythonOperator(
    task_id="create_dimcrops_task",
    python_callable=DimCropsGold,
    provide_context=True,
    op_kwargs={"path": dim_paths["Dim_Crops_path"]},
    dag=dag,
)
