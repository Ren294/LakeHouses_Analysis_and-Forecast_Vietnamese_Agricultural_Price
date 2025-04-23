from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pyspark.sql import functions as F
import faostat
from bronzestage.Ingestion_GSO import GSOBronze
from silverstage.CleanAndMerge_GSO import GSOSilver
from goldstage.Warehouse_Yield import GSOGold
from config import GSO_paths

gso_paths = GSO_paths()

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "GSO_dag",
    default_args=default_args,
    description="Collect, process and store GSO data",
    schedule_interval="@monthly",
    catchup=False,
)

bronze_gso = PythonOperator(
    task_id="load_gso_bronze",
    python_callable=GSOBronze,
    provide_context=True,
    op_kwargs={"path": gso_paths.GSO_bronze_path},
    dag=dag,
)

silver_gso = PythonOperator(
    task_id="load_gso_silver",
    python_callable=GSOSilver,
    provide_context=True,
    op_kwargs={"path": gso_paths.GSO_silver_path},
    dag=dag,
)

gold_gso = PythonOperator(
    task_id="load_faostat_gold",
    python_callable=GSOGold,
    provide_context=True,
    op_kwargs={"path": gso_paths.GSO_gold_path},
    dag=dag,
)

bronze_gso >> silver_gso >> gold_gso
