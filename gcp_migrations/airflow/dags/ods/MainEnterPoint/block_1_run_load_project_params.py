from __future__ import annotations
from datetime import datetime
from airflow.decorators import dag, task
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.models.xcom_arg import XComArg

DAG_ID = "MainEnterPoint__load_project_params"
MSSQL_CONN_ID = "mssql_stage"

SQL = """
SELECT
    PARAM_NAME,
    PARAM_VALUE,
    CATEGORY_DESC
FROM [StageETLDB].[dbo].[PROJECT_PARAMS]
WHERE [TYPE] = 3
   OR PARAM_NAME IN ('fileStatusNonExist','fileStatusPatternNotMatched');
"""

# 對應 SSIS 腳本裡會被指派的目標鍵
TARGET_KEYS = {
    "srcFileTypeCheckFile": "param_srcFileTypeCheckFile",
    "srcFileTypeRawData": "param_srcFileTypeRawData",
    "srcFileTypeEmpty": "param_srcFileTypeEmpty",
    "fileStatusNonExist": "param_fileStatusNonExist",
    "fileStatusPatternNotMatched": "param_fileStatusPatternNotMatched",
}

@dag(
    dag_id=DAG_ID,
    start_date=datetime(2025, 1, 1),
    schedule=None,      # 手動或外部觸發
    catchup=False,
    tags=["ssis-migration", "params"],
)
def load_project_params():
    """Mirror SSIS：
    Block1：讀 PROJECT_PARAMS
    Block2：照名稱+型別轉型並映射
    輸出：params_dict（XCom）
    """

    @task
    def fetch_params_from_mssql() -> list[dict]:
        hook = MsSqlHook(mssql_conn_id=MSSQL_CONN_ID)
        rows = hook.get_records(SQL)
        return [
            {"PARAM_NAME": r[0], "PARAM_VALUE": r[1], "CATEGORY_DESC": r[2]}
            for r in rows
        ]

    @task
    def map_and_cast_params(rows: list[dict]) -> dict:
        out: dict[str, int | str] = {}
        for row in rows:
            name = str(row.get("PARAM_NAME"))
            value = row.get("PARAM_VALUE")
            category = (row.get("CATEGORY_DESC") or "").strip().lower()

            if name not in TARGET_KEYS:
                continue
            key = TARGET_KEYS[name]

            # 對應 SSIS：CATEGORY_DESC == 'int' 時 TryParse 成功才指派
            if category == "int":
                try:
                    value = int(str(value).strip())
                except (TypeError, ValueError):
                    continue

            out[key] = value

        return out

    rows = fetch_params_from_mssql()
    params_dict = map_and_cast_params(rows)

    @task
    def done(params: dict):
        # 讓 UI 可見輸出，供下游參考
        return {"loaded_params": params}

    done(XComArg(params_dict))

load_project_params()
