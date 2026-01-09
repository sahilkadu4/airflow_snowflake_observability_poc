from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import logging

log = logging.getLogger(__name__)

SNOWFLAKE_CONN_ID = "snowflake"


def _insert_task_run(context, status: str):
    ti = context["task_instance"]
    # normalize times to ISO strings to avoid driver-specific issues
    def _to_iso(dt):
        if dt is None:
            return None
        try:
            return dt.isoformat()
        except Exception:
            return str(dt)


    start_time = ti.start_date
    end_time = ti.end_date
    start_time_s = _to_iso(start_time)
    end_time_s = _to_iso(end_time)
    # Compute duration in seconds if possible
    try:
        duration = (end_time - start_time).total_seconds() if start_time and end_time else None
    except Exception:
        duration = None

    sql = """
    MERGE INTO MONITORING.AIRFLOW_TASK_RUNS t
    USING (
        SELECT %s as dag_id,
               %s as dag_run_id,
               %s as task_id,
               %s as try_number,
               %s as task_state,
               %s::timestamp_ntz as start_time,
               %s::timestamp_ntz as end_time,
               %s::float as duration_seconds,
               current_timestamp() as logged_at
    ) s
    ON t.dag_run_id = s.dag_run_id AND t.task_id = s.task_id AND t.try_number = s.try_number
    WHEN MATCHED THEN UPDATE SET
        dag_id = s.dag_id,
        task_state = s.task_state,
        start_time = s.start_time,
        end_time = s.end_time,
        duration_seconds = s.duration_seconds,
        logged_at = s.logged_at
    WHEN NOT MATCHED THEN INSERT (dag_id, dag_run_id, task_id, try_number, task_state, start_time, end_time, duration_seconds, logged_at)
    VALUES (s.dag_id, s.dag_run_id, s.task_id, s.try_number, s.task_state, s.start_time, s.end_time, s.duration_seconds, s.logged_at)
    """

    params = [
        ti.dag_id,
        ti.run_id,
        ti.task_id,
        ti.try_number,
        status,
        start_time_s,
        end_time_s,
        duration,
    ]

    try:
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        hook.run(sql, parameters=params)
        log.info(
            "Upserted task run: dag=%s task=%s run=%s try=%s state=%s",
            ti.dag_id,
            ti.task_id,
            ti.run_id,
            ti.try_number,
            status,
        )
    except Exception as e:
        log.exception("Failed to upsert task run into Snowflake: %s", e)


def task_success_callback(context):
    _insert_task_run(context, "SUCCESS")


def task_failure_callback(context):
    _insert_task_run(context, "FAILED")
