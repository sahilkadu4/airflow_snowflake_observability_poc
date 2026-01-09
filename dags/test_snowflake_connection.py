from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime


def test_snowflake_connection():
    # Use the `snowflake` connection id which can be provided via
    # the AIRFLOW_CONN_SNOWFLAKE environment variable (.env)
    hook = SnowflakeHook(snowflake_conn_id="snowflake")
    result = hook.get_first("SELECT CURRENT_TIMESTAMP;")
    print("Snowflake responded with:", result)


with DAG(
    dag_id="test_snowflake_connection",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    test_task = PythonOperator(
        task_id="test_snowflake",
        python_callable=test_snowflake_connection,
    )
