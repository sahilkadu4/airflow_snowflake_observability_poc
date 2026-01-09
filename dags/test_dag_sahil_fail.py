import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from airflow import DAG
from airflow.operators.python import PythonOperator, get_current_context
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
import time

from observability_callbacks import dag_success_callback, dag_failure_callback

def always_fail():
    raise Exception("Intentional failure for testing failure callback.")

with DAG(
    dag_id="test_dag_sahil_fail",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    on_success_callback=dag_success_callback,
    on_failure_callback=dag_failure_callback,
) as dag:

    t1 = PythonOperator(
        task_id="t1",
        python_callable=always_fail,
        on_failure_callback=dag_failure_callback,
    )

t1
