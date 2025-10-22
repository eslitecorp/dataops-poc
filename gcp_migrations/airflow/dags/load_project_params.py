from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
import os

SQL_QUERY = """
SELECT
    PARAM_NAME,
    PARAM_VALUE,
    CATEGORY,
    [DESC]
FROM [StageETLDB].[dbo].[PROJECT_PARAMS]
WHERE TYPE = 3
   OR PARAM_NAME = 'fileStatusNonExist'
   OR PARAM_NAME = 'fileStatusPatternNotMatched';
"""

def fetch_project_params(**context):
    hook = MsSqlHook(mssql_conn_id="mssql_stage_etl")
    rows = hook.get_records(SQL_QUERY)
    return [{"name": r[0], "value": r[1], "category": r[2], "desc": r[3]} for r in rows]

def apply_project_params(**context):
    params = context["ti"].xcom_pull(task_ids="fetch_project_params")
    env_vars = {}
    for p in params or []:
        name, val, cat = p["name"], p["value"], p["category"]
        if cat == "int":
            try: val = int(val)
            except ValueError: continue
        env_vars[f"param_{name}"] = val
    os.environ.update({k: str(v) for k, v in env_vars.items()})
    for k, v in env_vars.items(): print(f"{k}={v}")

with DAG(
    dag_id="load_project_params",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["ssis-refactor","sqlserver-client","param-loader"],
) as dag:
    t_fetch = PythonOperator(task_id="fetch_project_params", python_callable=fetch_project_params, provide_context=True)
    t_apply = PythonOperator(task_id="apply_project_params", python_callable=apply_project_params, provide_context=True)
    t_fetch >> t_apply