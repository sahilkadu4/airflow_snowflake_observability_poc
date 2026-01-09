from airflow import settings
from airflow.models import Connection

sess = settings.Session()
# Remove existing if present
sess.query(Connection).filter(Connection.conn_id=='snowflake').delete()
conn = Connection(
    conn_id='snowflake',
    conn_type='snowflake',
    host='PXXYFSC-MP64784.snowflakecomputing.com',
    login='sahilkadu44',
    password='Backtothefuture@4@',
    schema='MONITORING',
    extra='{"account": "PXXYFSC-MP64784", "warehouse": "DBT_CS_WH", "database": "DBT_DATABASE", "role": "ACCOUNTADMIN"}'
)
sess.add(conn)
sess.commit()
print('Connection snowflake created')
