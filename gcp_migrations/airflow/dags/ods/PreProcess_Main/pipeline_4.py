from __future__ import annotations

from datetime import datetime
from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.python import BranchPythonOperator
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

DAG_ID = "preprocess_verify_header"

with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    params={
        "srcNatName": "TW",
        "srcSysName": "SYS_A",
        "srcEkorg": "EK01",
        "dbTableName": "RAW_TABLE",
        "dbRawTableName": "RAW_TABLE",
        "srcRawDataFileName": "ODS_SYS_A_20250101.csv",
        "ServerExecutionID": 123456,
    },
) as dag:

    # --- 第一個方塊：執行 SQL，回傳 'Y'/'N' 到 XCom ---------------------------------
    check_header_sql = r"""
    SELECT
      CASE
        WHEN A.headerRows > 0 AND (1 + B.FAIL - C.fail_all) > 0 THEN 'Y'
        ELSE 'N'
      END AS checkHeaderError
    FROM
      (SELECT CAST(ISNULL(PARAM_VALUE,'0') AS INT) AS headerRows
         FROM StageETLDB.dbo.USER_PARAMS
        WHERE PARAM_NAME='ColumnNameRowNo' AND NATIONALITY=? AND SYS_NAME=? AND EKORG=?) A,
      (SELECT FAIL
         FROM [StagingDB_TW].[dbo].[VERI_PRESTAGE_LOG]
        WHERE [FILE_NAME]=? AND [ITEM_PKEY]=(
              SELECT PKEY
                FROM [StagingDB_TW].[dbo].[VERI_PRESTAGE_ITEM]
               WHERE [VERI_PROC_PKEY]=(SELECT [PARAM_VALUE]
                                          FROM [StageETLDB].[dbo].[PROJECT_PARAMS]
                                         WHERE PARAM_NAME='Verify_RawData_Column_Width'))
          AND EXECUTIONID=? AND TABLE_NAME=?) B,
      (SELECT COUNT(*) AS fail_all
         FROM [StagingDB_TW].[dbo].[VERI_PRESTAGE_LOG_FAIL_DETAIL] d
         JOIN [StagingDB_TW].[dbo].[VERI_PRESTAGE_LOG] l ON d.LOG_PKEY=l.LOG_PKEY
        WHERE l.[FILE_NAME]=? AND l.[ITEM_PKEY]=(
              SELECT PKEY
                FROM [StagingDB_TW].[dbo].[VERI_PRESTAGE_ITEM]
               WHERE [VERI_PROC_PKEY]=(SELECT [PARAM_VALUE]
                                          FROM [StageETLDB].[dbo].[PROJECT_PARAMS]
                                         WHERE PARAM_NAME='Verify_RawData_Column_Width'))
          AND l.EXECUTIONID=? AND l.TABLE_NAME=?) C;
    """

    def _p(key: str, *, ctx) -> str | int:
        conf = (ctx.get("dag_run").conf if ctx.get("dag_run") else {}) or {}
        return conf.get(key, ctx["params"][key])

    check_header = MsSqlOperator(
        task_id="check_header",
        mssql_conn_id="mssql_ssis",
        sql=check_header_sql,
        parameters=lambda **ctx: [
            _p("srcNatName", ctx=ctx),
            _p("srcSysName", ctx=ctx),
            _p("srcEkorg", ctx=ctx),
            _p("srcRawDataFileName", ctx=ctx),
            _p("ServerExecutionID", ctx=ctx),
            _p("dbTableName", ctx=ctx),
            _p("srcRawDataFileName", ctx=ctx),
            _p("ServerExecutionID", ctx=ctx),
            _p("dbTableName", ctx=ctx),
        ],
        do_xcom_push=True,
        handler=lambda cur: (cur.fetchone() or [None])[0],  # -> 'Y' or 'N'
    )

    # 分支：'Y' 走左；其他走右
    branch = BranchPythonOperator(
        task_id="branch_on_checkHeaderError",
        python_callable=lambda **c: "left_t1_update_status" if str(
            (c["ti"].xcom_pull(task_ids="check_header") or "")
        ).upper() == "Y" else "right_t1_update_status"
    )

    # 供 Trigger conf 共用：把 params/dagrun.conf 透傳到下游 DAG
    def common_conf(**ctx):
        conf = (ctx.get("dag_run").conf if ctx.get("dag_run") else {}) or {}
        return {**ctx["params"], **conf, "from_parent_dag": DAG_ID}

    # ------------------ 左邊（3 個 Trigger，依序等待完成） ------------------
    left_t1 = TriggerDagRunOperator(
        task_id="left_t1_update_status",
        trigger_dag_id="ods_update_status_header_error",  # ← 改成你的 DAG 名
        wait_for_completion=True,
        reset_dag_run=False,
        conf=common_conf,
        poke_interval=30,
    )

    left_t2 = TriggerDagRunOperator(
        task_id="left_t2_move_to_error_folder",
        trigger_dag_id="ods_move_file_to_error",          # ← 改成你的 DAG 名
        wait_for_completion=True,
        reset_dag_run=False,
        conf=common_conf,
        poke_interval=30,
    )

    left_t3 = TriggerDagRunOperator(
        task_id="left_t3_cleanup_temp",
        trigger_dag_id="ods_cleanup_temp_files",          # ← 改成你的 DAG 名
        wait_for_completion=True,
        reset_dag_run=False,
        conf=common_conf,
        poke_interval=30,
    )

    # ------------------ 右邊（4 個 Trigger，依序等待完成） ------------------
    right_t1 = TriggerDagRunOperator(
        task_id="right_t1_update_status",
        trigger_dag_id="ods_update_status_no_cleaning_needed",  # ← 改
        wait_for_completion=True,
        reset_dag_run=False,
        conf=common_conf,
        poke_interval=30,
    )

    right_t2 = TriggerDagRunOperator(
        task_id="right_t2_move_to_done",
        trigger_dag_id="ods_move_file_to_done",                 # ← 改
        wait_for_completion=True,
        reset_dag_run=False,
        conf=common_conf,
        poke_interval=30,
    )

    right_t3 = TriggerDagRunOperator(
        task_id="right_t3_archive_done",
        trigger_dag_id="ods_archive_done_folder",               # ← 改
        wait_for_completion=True,
        reset_dag_run=False,
        conf=common_conf,
        poke_interval=30,
    )

    right_t4 = TriggerDagRunOperator(
        task_id="right_t4_cleanup_temp",
        trigger_dag_id="ods_cleanup_temp_files",                # 可與左共用
        wait_for_completion=True,
        reset_dag_run=False,
        conf=common_conf,
        poke_interval=30,
    )

    # 連線
    chain(check_header, branch, left_t1, left_t2, left_t3)
    chain(check_header, branch, right_t1, right_t2, right_t3, right_t4)
