from __future__ import annotations

import re
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from airflow.operators.branch import BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule

DAG_ID = "preprocess_prestage_full"
MSSQL_CONN_ID = "mssql_ssis"

default_args = {
    "owner": "dataops",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

# ---- 對應成功路線第 2 節點（原 C# ScriptTask） ----
def _extract_write_in_event(file_name: str, raw_pattern: str, **_):
    """
    等價 C#：
      1) 若檔名含 .ZIP/.7Z/.RAR（不分大小寫），移除最後一組 '(... )'
      2) 以 raw_pattern (Regex, group 1) 擷取 writeInEvent（忽略大小寫）
    回傳值進 XCom。
    """
    upper = file_name.upper()
    if any(ext in upper for ext in (".ZIP", ".7Z", ".RAR")):
        left = file_name.rfind("(")
        right = file_name.rfind(")")
        if left != -1 and right != -1 and right > left:
            file_name = file_name[:left] + file_name[right + 1 :]

    regex = re.compile(raw_pattern, flags=re.IGNORECASE)
    m = regex.search(file_name)
    write_in_event = m.group(1) if m and m.lastindex and m.lastindex >= 1 else ""
    return write_in_event


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    render_template_as_native_obj=True,
    params={
        # 基礎參數（可由 dagrun.conf 覆寫）
        "db_name": "ODS",
        "prestage_table": "dbo.PRESTAGE_TABLE",
        "bulk_src_path": r"C:\data\incoming\file.csv",

        # SSIS 對應參數
        "srcRawDataFileName": "file.csv",
        "srcNatName": "TW",
        "srcSysName": "SYS_A",
        "srcEkorg": "EK01",
        "dbTableName": "T_ABC",

        # 解析檔名的 Regex（群組1為 writeInEvent）
        "param_fileNamePattern": r".*CHECKFILE-([A-Z0-9_]+)\.CSV$",

        # 執行識別
        "execution_id": "{{ ts_nodash }}",
        "ServerExecutionID": "{{ ts_nodash }}",
    },
) as dag:

    # 第一方塊：記錄流程 → Trigger
    log_to_syssislog = TriggerDagRunOperator(
        task_id="log_to_syssislog",
        trigger_dag_id="util_syssislog_writer",
        conf={
            "file_name": "{{ dag_run.conf.get('srcRawDataFileName', params.srcRawDataFileName) }}",
            "execution_id": "{{ params.execution_id }}",
            "stage": "preprocess:start",
        },
        wait_for_completion=False,
    )

    # 第二方塊：前置清理 → Trigger
    pre_clean_relations = TriggerDagRunOperator(
        task_id="pre_clean_relations",
        trigger_dag_id="util_pre_clean_relations",
        conf={
            "target_table": "{{ dag_run.conf.get('prestage_table', params.prestage_table) }}",
            "file_name": "{{ dag_run.conf.get('srcRawDataFileName', params.srcRawDataFileName) }}",
            "execution_id": "{{ params.execution_id }}",
        },
        wait_for_completion=False,
    )

    # TRUNCATE prestage
    truncate_prestage = MsSqlOperator(
        task_id="truncate_prestage",
        mssql_conn_id=MSSQL_CONN_ID,
        database="{{ dag_run.conf.get('db_name', params.db_name) }}",
        sql="TRUNCATE TABLE {{ dag_run.conf.get('prestage_table', params.prestage_table) }};",
    )

    # BULK INSERT → prestage
    bulk_insert_prestage = MsSqlOperator(
        task_id="bulk_insert_prestage",
        mssql_conn_id=MSSQL_CONN_ID,
        database="{{ dag_run.conf.get('db_name', params.db_name) }}",
        sql=r"""
        DECLARE @src NVARCHAR(4000) =
            N'{{ dag_run.conf.get("bulk_src_path", params.bulk_src_path) }}';

        BULK INSERT {{ dag_run.conf.get('prestage_table', params.prestage_table) }}
        FROM @src
        WITH (FIRSTROW=2, FIELDTERMINATOR=',', ROWTERMINATOR='0x0a', TABLOCK);
        """,
    )

    # 失敗終點：BULK 失敗 → Trigger（終點）
    on_bulk_failed = TriggerDagRunOperator(
        task_id="on_bulk_failed",
        trigger_dag_id="util_remediate_after_failure",
        conf={
            "file_name": "{{ dag_run.conf.get('srcRawDataFileName', params.srcRawDataFileName) }}",
            "execution_id": "{{ dag_run.conf.get('ServerExecutionID', params.execution_id) }}",
            "reason": "bulk_insert_failed",
        },
        wait_for_completion=False,
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    # 成功路線 第1節點：Trigger
    on_bulk_success_trigger = TriggerDagRunOperator(
        task_id="on_bulk_success_trigger",
        trigger_dag_id="util_after_bulk_success",
        conf={
            "file_name": "{{ dag_run.conf.get('srcRawDataFileName', params.srcRawDataFileName) }}",
            "execution_id": "{{ dag_run.conf.get('ServerExecutionID', params.execution_id) }}",
            "stage": "after_prestage_bulk",
        },
        wait_for_completion=False,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # 成功路線 第2節點：C# 改寫為 PythonOperator（回傳 writeInEvent）
    extract_write_in_event = PythonOperator(
        task_id="extract_write_in_event",
        python_callable=_extract_write_in_event,
        op_kwargs={
            "file_name": "{{ dag_run.conf.get('srcRawDataFileName', params.srcRawDataFileName) }}",
            "raw_pattern": "{{ dag_run.conf.get('param_fileNamePattern', params.param_fileNamePattern) }}",
        },
    )

    # 成功路線 第3節點：若前一步沒有值，向 STAGE_TAB_ACT 補 DEF_OP；輸出到 XCom
    resolve_write_in_event_sql = MsSqlOperator(
        task_id="resolve_write_in_event_sql",
        mssql_conn_id=MSSQL_CONN_ID,
        database="{{ dag_run.conf.get('db_name', params.db_name) }}",
        sql="""
        SELECT CASE
          WHEN LEN({{ (ti.xcom_pull(task_ids='extract_write_in_event') or '') | tojson }}) > 0
            THEN {{ (ti.xcom_pull(task_ids='extract_write_in_event') or '') | tojson }}
          ELSE (
            SELECT DEF_OP
            FROM dbo.STAGE_TAB_ACT
            WHERE NATIONALITY = {{ dag_run.conf.get('srcNatName', params.srcNatName) | tojson }}
              AND SYS_NAME    = {{ dag_run.conf.get('srcSysName', params.srcSysName) | tojson }}
              AND EKORG       = {{ dag_run.conf.get('srcEkorg', params.srcEkorg) | tojson }}
              AND TABLE_NAME  = {{ dag_run.conf.get('dbTableName', params.dbTableName) | tojson }}
          )
        END;
        """,
        do_xcom_push=True,
    )

    # 取出上一 SQL 的單一值，成為最終 writeInEvent
    def _save_write_in_event_from_sql(ti, **_):
        rows = ti.xcom_pull(task_ids="resolve_write_in_event_sql") or []
        value = (rows[0][0] if rows and rows[0] else "") if isinstance(rows, list) else ""
        return value

    save_write_in_event = PythonOperator(
        task_id="save_write_in_event",
        python_callable=_save_write_in_event_from_sql,
    )

    # 成功路線 第4節點：UPDATE dbo.FILE_REC（語句/參數對應）
    update_file_rec = MsSqlOperator(
        task_id="update_file_rec",
        mssql_conn_id=MSSQL_CONN_ID,
        database="{{ dag_run.conf.get('db_name', params.db_name) }}",
        sql="""
        UPDATE dbo.FILE_REC
        SET FILE_OP = %(file_op)s
        WHERE FILE_NAME   = %(file_name)s
          AND EXECUTIONID = %(execution_id)s;
        """,
        parameters={
            "file_op": "{{ (ti.xcom_pull(task_ids='save_write_in_event') "
                        "or ti.xcom_pull(task_ids='extract_write_in_event') or '') }}",
            "file_name": "{{ dag_run.conf.get('srcRawDataFileName', params.srcRawDataFileName) }}",
            "execution_id": "{{ dag_run.conf.get('ServerExecutionID', params.execution_id) }}",
        },
    )

    # 分支：@[User::writeInEvent]=="D" 往左，否則往右
    def _route_by_write_in_event(ti, **_):
        val = (ti.xcom_pull(task_ids="save_write_in_event")
               or ti.xcom_pull(task_ids="extract_write_in_event")
               or "")
        return "end_left_delete_trigger" if str(val).upper() == "D" else "end_right_upsert_trigger"

    branch_write_in_event = BranchPythonOperator(
        task_id="branch_write_in_event",
        python_callable=_route_by_write_in_event,
    )

    # 左路線（終點）：Trigger
    end_left_delete_trigger = TriggerDagRunOperator(
        task_id="end_left_delete_trigger",
        trigger_dag_id="pipeline_handle_delete",   # 你的刪除流程 DAG
        conf={
            "file_name": "{{ dag_run.conf.get('srcRawDataFileName', params.srcRawDataFileName) }}",
            "execution_id": "{{ dag_run.conf.get('ServerExecutionID', params.execution_id) }}",
            "writeInEvent": "{{ ti.xcom_pull(task_ids='save_write_in_event') or '' }}",
        },
        wait_for_completion=False,
    )

    # 右路線（終點）：Trigger
    end_right_upsert_trigger = TriggerDagRunOperator(
        task_id="end_right_upsert_trigger",
        trigger_dag_id="pipeline_handle_upsert",   # 你的 I/IU 等流程 DAG
        conf={
            "file_name": "{{ dag_run.conf.get('srcRawDataFileName', params.srcRawDataFileName) }}",
            "execution_id": "{{ dag_run.conf.get('ServerExecutionID', params.execution_id) }}",
            "writeInEvent": "{{ ti.xcom_pull(task_ids='save_write_in_event') or '' }}",
        },
        wait_for_completion=False,
    )

    # ---------- DAG 依賴 ----------
    log_to_syssislog >> pre_clean_relations >> truncate_prestage >> bulk_insert_prestage
    bulk_insert_prestage >> on_bulk_failed   # 失敗終點
    bulk_insert_prestage >> on_bulk_success_trigger >> extract_write_in_event
    extract_write_in_event >> resolve_write_in_event_sql >> save_write_in_event
    save_write_in_event >> update_file_rec >> branch_write_in_event
    branch_write_in_event >> [end_left_delete_trigger, end_right_upsert_trigger]
