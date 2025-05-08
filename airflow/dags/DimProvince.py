from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pyspark.sql import functions as F
import faostat
from goldstage.DimProvince import DimProvinceGold
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
    "DimProvince_dag",
    default_args=default_args,
    description="Create dimprovince data",
    schedule_interval="@monthly",  
    catchup=False,
    max_active_runs=1
)


create_dimprovince_task = PythonOperator(
    task_id="create_dimprovince_task",
    python_callable=DimProvinceGold,
    provide_context=True,
    op_kwargs={"path": dim_paths["Dim_Province_path"]},
    dag=dag,
)
