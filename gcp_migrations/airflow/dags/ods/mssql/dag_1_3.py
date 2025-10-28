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

    select_count = MsSqlOperator(
        task_id="count_files",
        mssql_conn_id="mssql_default",
        sql="SELECT COUNT(*) FROM dbo.FILE_REC WHERE FILE_STATUS = %(s)s",
        parameters={"s": 2},
        do_xcom_push=True,  # 會把第一列第一欄 push 到 XCom
    )

