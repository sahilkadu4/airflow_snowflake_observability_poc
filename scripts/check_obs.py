from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from pprint import pprint

hook = SnowflakeHook(snowflake_conn_id='snowflake')
rows = hook.get_records(
    "select dag_id, dag_run_id, status, logged_at from MONITORING.AIRFLOW_DAG_RUNS where dag_id=%s order by logged_at desc limit 5",
    parameters=['observability_test_dag']
)
print('rows_count=', len(rows))
if rows:
    pprint(rows)
