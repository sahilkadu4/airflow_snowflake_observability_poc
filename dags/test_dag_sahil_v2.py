
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from airflow import DAG
from airflow.operators.python import PythonOperator, get_current_context
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
import time


from observability_callbacks import dag_success_callback, dag_failure_callback
from task_callbacks import task_success_callback, task_failure_callback


def dummy_work():
    time.sleep(5)


with DAG(
    dag_id="test_dag_sahil_new_v2",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    on_success_callback=dag_success_callback,
    on_failure_callback=dag_failure_callback,
) as dag:


    t1 = PythonOperator(
        task_id="t1",
        python_callable=dummy_work,
        on_success_callback=task_success_callback,
        on_failure_callback=task_failure_callback,
    )

    t2 = PythonOperator(
        task_id="t2",
        python_callable=dummy_work,
        on_success_callback=task_success_callback,
        on_failure_callback=task_failure_callback,
    )

    def write_observability():
        ctx = get_current_context()
        dag = ctx.get("dag")
        dag_id = getattr(dag, "dag_id", None) or ctx.get("dag_id")
        dag_run = ctx.get("dag_run")
        dag_run_id = getattr(dag_run, "run_id", None) or ctx.get("run_id")
        run_type = getattr(dag_run, "run_type", None) or ctx.get("run_type") or "manual"
        if hasattr(run_type, 'value'):
            run_type = run_type.value

        execution_date = ctx.get("execution_date") or getattr(dag_run, "execution_date", None) or ctx.get("logical_date")
        start_time = getattr(dag_run, "start_date", None)
        end_time = getattr(dag_run, "end_date", None)

        def _to_iso(dt):
            if dt is None:
                return None
            try:
                return dt.isoformat()
            except Exception:
                return str(dt)

        execution_date_s = _to_iso(execution_date)
        start_time_s = _to_iso(start_time)
        end_time_s = _to_iso(end_time)

        # Simple INSERT for PoC (non-idempotent) — logged_at set by Snowflake
        sql = """
        INSERT INTO MONITORING.AIRFLOW_DAG_RUNS
            (dag_id, dag_run_id, run_type, execution_date, start_time, end_time, status, logged_at)
        VALUES (%s, %s, %s, %s::timestamp_ntz, %s::timestamp_ntz, %s::timestamp_ntz, %s, current_timestamp())
        """

        params = [dag_id, dag_run_id, str(run_type), execution_date_s, start_time_s, end_time_s, 'SUCCESS']

        hook = SnowflakeHook(snowflake_conn_id='snowflake')
        # add a visible print so scheduler logs show the intent
        print(f"Writing observability row for {dag_id} / {dag_run_id}")
        hook.run(sql, parameters=params)
        print("Observability write executed")

    write_obs = PythonOperator(
        task_id='write_observability',
        python_callable=write_observability,
    )

    [t1, t2] >> write_obs
