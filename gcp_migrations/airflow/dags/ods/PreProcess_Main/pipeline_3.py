# dags/preprocess_checkfile_branch.py
from __future__ import annotations
import os, glob
from datetime import datetime, timedelta
from typing import Any, Dict

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.utils.trigger_rule import TriggerRule

DAG_ID = "preprocess_checkfile_branch"
DEFAULT_ARGS = {"owner": "dataops", "retries": 0}

with DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    dagrun_timeout=timedelta(hours=2),
    params={
        "srcOngoingFileFolder": r"/data/incoming",
        "srcRawDataFileName": "ODS_SYS_checkfile_20250101.csv",
    },
    tags=["ssis-migration", "preprocess"],
) as dag:

    # Block 1: Trigger（前置）
    trigger_prev = TriggerDagRunOperator(
        task_id="trigger_prev_step",
        trigger_dag_id="PUT_YOUR_UPSTREAM_DAG_ID_HERE",
        reset_dag_run=True,
        wait_for_completion=True,
        poke_interval=30,
    )

    # Block 2: C# -> Python（組出 srcEncodedFilePath）
    def build_src_encoded_file_path(**context) -> str:
        p: Dict[str, Any] = context["params"]
        folder: str = p.get("srcOngoingFileFolder", "")
        raw_name: str = p.get("srcRawDataFileName", "")
        dot_idx = raw_name.rfind(".")
        base = raw_name[:dot_idx] if dot_idx > 0 else raw_name
        pattern = base + "_*.csv"
        files = sorted(glob.glob(os.path.join(folder, pattern)))
        joined = ";".join(files) if files else ""
        context["ti"].xcom_push(key="srcEncodedFilePath", value=joined)
        return joined

    build_path = PythonOperator(
        task_id="build_src_encoded_file_path",
        python_callable=build_src_encoded_file_path,
    )

    # 分支：LEN(@[User::srcEncodedFilePath])==0 往左；>0 往下
    def decide_left_or_down(**context) -> str:
        v = context["ti"].xcom_pull(
            task_ids="build_src_encoded_file_path", key="srcEncodedFilePath"
        ) or ""
        return "down_trigger_step_1" if len(v) > 0 else "go_left_when_empty"

    branch = BranchPythonOperator(
        task_id="branch_by_src_encoded_len",
        python_callable=decide_left_or_down,
    )

    # ← 左邊（占位）
    go_left = EmptyOperator(task_id="go_left_when_empty")

    # ↓ 往下：Trigger → Trigger → SQL → Trigger
    down_trigger_1 = TriggerDagRunOperator(
        task_id="down_trigger_step_1",
        trigger_dag_id="PUT_YOUR_DOWNSTREAM_DAG_ID_1",
        reset_dag_run=True,
        wait_for_completion=True,
        poke_interval=30,
    )

    down_trigger_2 = TriggerDagRunOperator(
        task_id="down_trigger_step_2",
        trigger_dag_id="PUT_YOUR_DOWNSTREAM_DAG_ID_2",
        reset_dag_run=True,
        wait_for_completion=True,
        poke_interval=30,
    )

    # 依照截圖的 SQL：兩個 ? 皆對應 srcRawDataFileName；結果欄位命名 file_status
    SQL_CHECK = """
    SELECT TOP 1 ISNULL(file_status, 0) AS file_status
    FROM [dbo].[FILE_REC]
    WHERE FILE_NAME = ?
    UNION
    SELECT -1
    FROM [dbo].[FILE_REC]
    WHERE FILE_NAME = ?
    HAVING COUNT(*) = 0
    """

    def query_file_status(**context) -> int:
        """
        執行上面的查詢，將結果（單一整數）以 XCom: key='srcFileStatus' 輸出。
        映射對應 SSIS：結果名稱 file_status → 變數 User::srcFileStatus。
        """
        p: Dict[str, Any] = context["params"]
        file_name: str = p["srcRawDataFileName"]

        hook = MsSqlHook(mssql_conn_id="mssql_ods")  # ← 換成你的 Connection
        # 參數序：?0, ?1 均為檔名
        rows = hook.get_records(SQL_CHECK, parameters=(file_name, file_name))
        # 預期只回一列一欄
        status = int(rows[0][0]) if rows else -1

        context["ti"].xcom_push(key="srcFileStatus", value=status)
        return status

    check_file_status = PythonOperator(
        task_id="check_file_status",
        python_callable=query_file_status,
    )

    left_trigger = TriggerDagRunOperator(
        task_id="left_trigger",
        trigger_dag_id="xxxxxxxx",
        reset_dag_run=True,
        wait_for_completion=True,
        poke_interval=30,
    )

    down_trigger_3 = TriggerDagRunOperator(
        task_id="down_trigger_step_3_final",
        trigger_dag_id="PUT_YOUR_DOWNSTREAM_DAG_ID_3",
        reset_dag_run=True,
        wait_for_completion=True,
        poke_interval=30,
    )

    # Flow
    trigger_prev >> build_path >> branch
    branch >> go_left >> left_trigger
    branch >> down_trigger_1 >> down_trigger_2 >> check_file_status >> down_trigger_3
