from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pyspark.sql import functions as F
import faostat
from model.MultivariateModel import train_crop_model
from config import get_all_crop_codes

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "TrainModel_dag",
    default_args=default_args,
    description="Train Model",
    schedule_interval="@monthly",
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1
)

for code in get_all_crop_codes():
    # task1 = PythonOperator(
    #     task_id=f'train_model_rnn_crop_{code}',
    #     python_callable=train_crop_model,
    #     provide_context=True,
    #     op_kwargs={'crop_code': code, 'model_type': 'rnn'},
    #     dag=dag,
    # )

    task = PythonOperator(
        task_id=f'train_model_lstm_crop_{code}',
        python_callable=train_crop_model,
        provide_context=True,
        op_kwargs={'crop_code': code , 'model_type': 'lstm'},
        dag=dag,
    )
