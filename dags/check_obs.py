from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from pprint import pprint
import sys


def check(dag_id: str):
    hook = SnowflakeHook(snowflake_conn_id='snowflake')
    if dag_id == 'all':
        rows = hook.get_records(
            "select dag_id, dag_run_id, status, logged_at from MONITORING.AIRFLOW_DAG_RUNS order by logged_at desc limit 10"
        )
    else:
        rows = hook.get_records(
            "select dag_id, dag_run_id, status, logged_at from MONITORING.AIRFLOW_DAG_RUNS where dag_id=%s order by logged_at desc limit 5",
            parameters=[dag_id]
        )
    print('rows_count=', len(rows))
    if rows:
        pprint(rows)


if __name__ == '__main__':
    dag = sys.argv[1] if len(sys.argv) > 1 else 'observability_test_dag'
    check(dag)
