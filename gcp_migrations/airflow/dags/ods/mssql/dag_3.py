from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

def run_txn(**_):
    hook = MsSqlHook(mssql_conn_id="mssql_default")
    conn = hook.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("BEGIN TRAN")
            cur.execute("UPDATE dbo.FILE_REC SET FILE_STATUS = %s WHERE FILE_NAME = %s", (3, "abc.csv"))
            cur.execute("INSERT INTO dbo.AUDIT_LOG(evt, info) VALUES(%s, %s)", ("FILE_STATUS_CHANGED", "abc.csv"))
            cur.execute("COMMIT TRAN")
    except Exception:
        cur.execute("ROLLBACK TRAN")
        raise
    finally:
        conn.close()

from airflow.operators.python import PythonOperator
with DAG("mssql_txn_example", start_date=days_ago(1), schedule=None, catchup=False) as dag:
    PythonOperator(task_id="txn_update", python_callable=run_txn)
