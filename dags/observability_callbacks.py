from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import logging

log = logging.getLogger(__name__)


def _upsert_dag_run(context, status: str):
    print("### OBSERVABILITY CALLBACK ENTERED ###")
    dag = context.get("dag")
    dag_id = getattr(dag, "dag_id", None) or context.get("dag_id")
    dag_run = context.get("dag_run")
    dag_run_id = getattr(dag_run, "run_id", None) or context.get("run_id")
    run_type = getattr(dag_run, "run_type", None) or context.get("run_type") or "manual"
    # Convert run_type to string if it's an Enum
    if hasattr(run_type, 'value'):
        run_type = str(run_type.value)
    else:
        run_type = str(run_type)

    execution_date = context.get("execution_date") or getattr(dag_run, "execution_date", None) or context.get("logical_date")
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

    sql = """
    MERGE INTO MONITORING.AIRFLOW_DAG_RUNS t
    USING (
        SELECT %s as dag_id,
               %s as dag_run_id,
               %s as run_type,
               %s::timestamp_ntz as execution_date,
               %s::timestamp_ntz as start_time,
               %s::timestamp_ntz as end_time,
               %s as status,
               current_timestamp() as logged_at
    ) s
    ON t.dag_run_id = s.dag_run_id
    WHEN MATCHED THEN UPDATE SET
        dag_id = s.dag_id,
        run_type = s.run_type,
        execution_date = s.execution_date,
        start_time = s.start_time,
        end_time = s.end_time,
        status = s.status,
        logged_at = s.logged_at
    WHEN NOT MATCHED THEN INSERT (dag_id, dag_run_id, run_type, execution_date, start_time, end_time, status, logged_at)
    VALUES (s.dag_id, s.dag_run_id, s.run_type, s.execution_date, s.start_time, s.end_time, s.status, s.logged_at)
    """

    params = [dag_id, dag_run_id, run_type, execution_date_s, start_time_s, end_time_s, status]

    try:
        hook = SnowflakeHook(snowflake_conn_id="snowflake")
        hook.run(sql, parameters=params)
        log.info("Upserted DAG run %s / %s into MONITORING.AIRFLOW_DAG_RUNS", dag_id, dag_run_id)
    except Exception as e:
        log.exception("Failed to upsert DAG run into Snowflake: %s", e)


def dag_success_callback(context):
    log.info("DAG SUCCESS CALLBACK TRIGGERED for %s", getattr(context.get('dag'), 'dag_id', context.get('dag_id')))
    _upsert_dag_run(context, 'SUCCESS')


def dag_failure_callback(context):
    log.info("DAG FAILURE CALLBACK TRIGGERED for %s", getattr(context.get('dag'), 'dag_id', context.get('dag_id')))
    _upsert_dag_run(context, 'FAILED')
