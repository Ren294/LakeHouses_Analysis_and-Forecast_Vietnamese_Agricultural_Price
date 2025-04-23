from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pyspark.sql import functions as F
import faostat
from bronzestage.Ingestion_FaoStat import FaoStatBronze
from silverstage.CleanAndMerge_FaoStat import FaoStatSilver
from goldstage.Warehouse_FaoStat import FaoStatGold
from config import Faostat_paths

faostat_paths = Faostat_paths()

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "Faostat_dag",
    default_args=default_args,
    description="Collect, process and store Faostat data",
    schedule_interval="@monthly",
    catchup=False,
)

bronze_faostat = PythonOperator(
    task_id="load_faostat_bronze",
    python_callable=FaoStatBronze,
    provide_context=True,
    op_kwargs={"path": faostat_paths["Faostat_bronze_path"]},
    dag=dag,
)

silver_faostat = PythonOperator(
    task_id="load_faostat_silver",
    python_callable=FaoStatSilver,
    provide_context=True,
    op_kwargs={"path": faostat_paths["Faostat_silver_path"]},
    dag=dag,
)

gold_faostat = PythonOperator(
    task_id="load_faostat_gold",
    python_callable=FaoStatGold,
    provide_context=True,
    op_kwargs={"path": faostat_paths["Faostat_gold_path"]},
    dag=dag,
)

bronze_faostat >> silver_faostat >> gold_faostat
