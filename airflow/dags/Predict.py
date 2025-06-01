from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pyspark.sql import functions as F
import faostat
from model.PredictCropPrice2 import predict_crop_price
from config import get_all_crop_codes, Predict_paths

predict_paths = Predict_paths()

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "Predict_dag",
    default_args=default_args,
    description="Predict",
    schedule_interval="@monthly",
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1
)

# for code in get_all_crop_codes():
#     task = PythonOperator(
#         task_id=f'predict_crop_price_{code}',
#         python_callable=predict_crop_price,
#         provide_context=True,
#         op_kwargs={'crop_code': code, 
#                 'path': predict_paths['Predict_Crop_Price_path']},
#         dag=dag,
#     )

task = PythonOperator(
    task_id=f'predict_crop_price',
    python_callable=predict_crop_price,
    provide_context=True,
    op_kwargs={'path': predict_paths['Predict_Crop_Price_path']},
    dag=dag,
)
