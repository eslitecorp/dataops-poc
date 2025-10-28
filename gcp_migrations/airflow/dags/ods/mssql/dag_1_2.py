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

    call_sp = MsSqlOperator(
        task_id="call_sp_recalc",
        mssql_conn_id="mssql_default",
        sql="EXEC dbo.usp_recalculate_metrics @p_date = %(dt)s, @p_force = %(force)s",
        parameters={"dt": "2025-10-28", "force": 1},
    )
