from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator

with DAG(
    dag_id="mssql_simple_example",
    start_date=days_ago(1),
    schedule=None,
    catchup=False,
    tags=["mssql"],
) as dag:

    run_sql = MsSqlOperator(
        task_id="update_file_rec",
        mssql_conn_id="mssql_default",
        sql="""
        UPDATE dbo.FILE_REC
        SET    FILE_STATUS = %(status)s,
               SOURCEID    = %(source_id)s,
               EXECUTIONID = %(exec_id)s
        WHERE  FILE_NAME   = %(file_name)s
        """,
        parameters={
            "status": 2,
            "source_id": "PIPELINE_A",
            "exec_id": "2025-10-28T09:00:00+08:00",
            "file_name": "abc.csv",
        },
    )
