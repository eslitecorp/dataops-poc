from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

with DAG(
    dag_id="mssql_odbc_qmark_style",
    start_date=days_ago(1),
    schedule=None,
    catchup=False,
    tags=["mssql","odbc"],
) as dag:

    run_sql_qmark = SQLExecuteQueryOperator(
        task_id="update_file_rec_qmark",
        conn_id="mssql_odbc",
        sql="""
        UPDATE dbo.FILE_REC
        SET    FILE_STATUS = ?,
               SOURCEID    = ?,
               EXECUTIONID = ?
        WHERE  FILE_NAME   = ?
        """,
        parameters=[2, "PIPELINE_A", "2025-10-28T09:00:00+08:00", "abc.csv"],
    )
