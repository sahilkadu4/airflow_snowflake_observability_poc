from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime

from observability_callbacks import (
    dag_success_callback,
    dag_failure_callback,
)

with DAG(
    dag_id="observability_test_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    on_success_callback=dag_success_callback,
    on_failure_callback=dag_failure_callback,
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    start >> end
