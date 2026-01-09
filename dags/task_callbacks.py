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
    INSERT INTO MONITORING.AIRFLOW_TASK_RUNS
        (dag_id, dag_run_id, task_id, try_number, task_state, start_time, end_time, duration_seconds, logged_at)
    VALUES (%s, %s, %s, %s, %s, %s::timestamp_ntz, %s::timestamp_ntz, %s::float, current_timestamp())
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
