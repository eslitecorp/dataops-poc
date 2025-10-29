from __future__ import annotations
from datetime import datetime
from typing import Dict, Any, List
import logging

from airflow import DAG
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from airflow.operators.python import PythonOperator
from airflow.operators.branch import BranchPythonOperator
from airflow.providers.airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable

DAG_ID = "etl_params_operator_with_foreach_group"
MSSQL_CONN_ID = "mssql_stage"

SQL_PROJECT_PARAMS = """
SELECT
    CAST([NAME]  AS NVARCHAR(128))  AS paramName,
    CAST([VALUE] AS NVARCHAR(256))  AS paramValue,
    *
FROM StageETLDB.dbo.PROJECT_PARAMS
WHERE [TYPE] IN (2, 3);
"""

SQL_POOLING_NEED = """
SELECT PARAM_VALUE
FROM [StageETLDB].[dbo].[USER_PARAMS]
WHERE PARAM_NAME = 'poolingNeedVeriButWithoutCheckFileMins'
  AND NATIONALITY = ?
"""

def rows_as_dicts(cursor) -> List[Dict[str, Any]]:
    cols = [c[0] for c in cursor.description]
    return [dict(zip(cols, r)) for r in cursor.fetchall()]

def single_value_as_named_dict(cursor):
    row = cursor.fetchone()
    value = None if row is None else row[0]
    # 等同 SSIS: User::param_poolingNeedVeriButWithoutCheckFileMins
    return {"param_poolingNeedVeriButWithoutCheckFileMins": value}

def _merge_params(ti, **context):
    """
    收集 foreach_params.script_task 的所有 mapped 輸出 (list[dict])，
    再併入 select_pooling_need 的 dict，最後回傳一個整合後的 dict。
    """
    mapped_results = ti.xcom_pull(task_ids="foreach_params.script_task")  # list[dict]（Airflow 會自動收集 mapped 輸出）
    merged = {}
    for d in mapped_results or []:
        if isinstance(d, dict):
            merged.update(d)

    extra = ti.xcom_pull(task_ids="select_pooling_need") or {}
    merged.update(extra)

    ti.xcom_push(key="merged_params", value=merged)

def _compute_file_routing(ti, **context):
    """
    1) 讀取前面合併好的參數（需包含 param_srcFileTypeCheckFile / param_srcFileTypeRawData 等）
    2) 套用 C# 腳本等價邏輯（Regex 判斷、ZIP 處理、欄位組裝）
    3) 把 routing context（含 srcFileType 等）以 XCom 推出
    """
    p = context["params"]

    src_sys_name   = p["srcSysName"]
    src_ekorg      = p.get("srcEkorg", "")
    db_table_name  = p["dbTableName"]
    file_name      = p["srcRawDataFileName"]
    src_file_folder= p["srcFileFolder"]

    # tablePatternMap：優先讀 Variable，沒有就用 params
    table_pattern_map = Variable.get(
        "tablePatternMap", default_var=p.get("tablePatternMap", {}), deserialize_json=True
    )

    import re

    key = db_table_name.upper()
    raw_pattern = table_pattern_map.get(key)
    if not raw_pattern:
        raise ValueError(f"tablePatternMap 缺少 {key} 的規則")

    # 先判斷 CHECKFILE
    checkfile_pattern = rf"ODS{src_sys_name.split('_')[0].upper()}(_|\-)CHECKFILE(_|\-)([0-9]{{8}})(.*)\.CSV"
    if re.match(checkfile_pattern, file_name.upper()):
        routing = {
            "srcCheckFileName": file_name,
            "srcCheckFilePath": f"{src_file_folder}{file_name}",
            "srcFileType": 1,  # CHECKFILE
        }
        ti.xcom_push(key="routing_ctx", value=routing)
        return

    # RawData 規則
    name = file_name
    upper = name.upper()
    is_zip = any(ext in upper for ext in [".ZIP", ".RAR", ".7Z"])
    if is_zip and "(" in name and ")" in name and name.rfind(")") > name.rfind("("):
        l, r = name.rfind("("), name.rfind(")")
        name = name[:l] + name[r + 1 :]

    m = re.match(raw_pattern, name)
    if not m:
        raise ValueError(f"raw data 檔名不符合規則：pattern={raw_pattern}, file={name}")

    dateYYYYMMDD = m.group(1)
    sys_name = f"{src_sys_name}_{src_ekorg}" if src_ekorg else src_sys_name
    db_schema_name = f"prestage_{sys_name}"
    db_stage_table_name = f"{sys_name}.{db_table_name}"

    routing = {
        "param_fileNamePattern": raw_pattern,
        "dateYYYYMMDD": dateYYYYMMDD,
        "srcFileType": 0,  # RawData
        "packageName": f"{sys_name}_{db_table_name}.dtsx",
        "dbSchemaName": db_schema_name,
        "dbPrestageTableName": f"{db_schema_name}.{db_table_name}",
        "dbStageTableName": db_stage_table_name,
        "srcRawDataFileName": p["srcRawDataFileName"],
    }
    ti.xcom_push(key="routing_ctx", value=routing)

def _compute_file_routing(ti, **context):
    """
    1) 讀取前面合併好的參數（需包含 param_srcFileTypeCheckFile / param_srcFileTypeRawData 等）
    2) 套用 C# 腳本等價邏輯（Regex 判斷、ZIP 處理、欄位組裝）
    3) 把 routing context（含 srcFileType 等）以 XCom 推出
    """
    p = context["params"]

    src_sys_name   = p["srcSysName"]
    src_ekorg      = p.get("srcEkorg", "")
    db_table_name  = p["dbTableName"]
    file_name      = p["srcRawDataFileName"]
    src_file_folder= p["srcFileFolder"]

    # tablePatternMap：優先讀 Variable，沒有就用 params
    table_pattern_map = Variable.get(
        "tablePatternMap", default_var=p.get("tablePatternMap", {}), deserialize_json=True
    )

    import re

    key = db_table_name.upper()
    raw_pattern = table_pattern_map.get(key)
    if not raw_pattern:
        raise ValueError(f"tablePatternMap 缺少 {key} 的規則")

    # 先判斷 CHECKFILE
    checkfile_pattern = rf"ODS{src_sys_name.split('_')[0].upper()}(_|\-)CHECKFILE(_|\-)([0-9]{{8}})(.*)\.CSV"
    if re.match(checkfile_pattern, file_name.upper()):
        routing = {
            "srcCheckFileName": file_name,
            "srcCheckFilePath": f"{src_file_folder}{file_name}",
            "srcFileType": 1,  # CHECKFILE
        }
        ti.xcom_push(key="routing_ctx", value=routing)
        return

    # RawData 規則
    name = file_name
    upper = name.upper()
    is_zip = any(ext in upper for ext in [".ZIP", ".RAR", ".7Z"])
    if is_zip and "(" in name and ")" in name and name.rfind(")") > name.rfind("("):
        l, r = name.rfind("("), name.rfind(")")
        name = name[:l] + name[r + 1 :]

    m = re.match(raw_pattern, name)
    if not m:
        raise ValueError(f"raw data 檔名不符合規則：pattern={raw_pattern}, file={name}")

    dateYYYYMMDD = m.group(1)
    sys_name = f"{src_sys_name}_{src_ekorg}" if src_ekorg else src_sys_name
    db_schema_name = f"prestage_{sys_name}"
    db_stage_table_name = f"{sys_name}.{db_table_name}"

    routing = {
        "param_fileNamePattern": raw_pattern,
        "dateYYYYMMDD": dateYYYYMMDD,
        "srcFileType": 0,  # RawData
        "packageName": f"{sys_name}_{db_table_name}.dtsx",
        "dbSchemaName": db_schema_name,
        "dbPrestageTableName": f"{db_schema_name}.{db_table_name}",
        "dbStageTableName": db_stage_table_name,
        "srcRawDataFileName": p["srcRawDataFileName"],
    }
    ti.xcom_push(key="routing_ctx", value=routing)

def _choose_branch(ti, **_):
    routing = ti.xcom_pull(task_ids="compute_file_routing", key="routing_ctx") or {}
    merged  = ti.xcom_pull(task_ids="merge_params", key="merged_params") or {}

    src_file_type = routing.get("srcFileType")
    want_checkfile = merged.get("param_srcFileTypeCheckFile")
    want_rawdata   = merged.get("param_srcFileTypeRawData")

    if src_file_type == want_checkfile:
        return "trigger_checkfile_pipeline"
    if src_file_type == want_rawdata:
        return "trigger_rawdata_pipeline"
    return "no_op_end"




with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    params={
        # 來自 SSIS $Package::*
        "srcSysName": "SAP_MM_TW",
        "srcEkorg": "",
        "dbTableName": "FILE_REC",
        "srcRawDataFileName": "ODS_SAP-CHECKFILE-20250101.csv",
        "srcFileFolder": "/data/incoming/",

        # 若沒在 Variable 設定，可在 params 放預設
        "tablePatternMap": {  # 例：每個表名對應一條 regex，分組1是日期
            "FILE_REC": r".*([0-9]{8}).*\.CSV$"
        },

        # 供 select_pooling_need 的預設
        "srcNatName": "TW",

        # 左/右兩條要觸發的 DAG 名稱
        "left_checkfile_dag_id": "checkfile_pipeline",
        "right_rawdata_dag_id": "rawdata_pipeline",
    },
    doc_md="承接前3個方塊；此處加入第四方塊(C#)等價邏輯 + 分支 + 兩個 Trigger。",
):
    

    select_params = MsSqlOperator(
        task_id="select_params",
        mssql_conn_id=MSSQL_CONN_ID,
        sql=SQL_PROJECT_PARAMS,
        handler=rows_as_dicts,   # XCom: list[dict]
        do_xcom_push=True,
    )

    
    with TaskGroup(group_id="foreach_params", tooltip="SSIS Foreach 容器") as foreach_params:

        @task(task_id="script_task")
        def script_task(record: Dict[str, Any]) -> Dict[str, Any]:
            name = str(record.get("paramName"))
            raw = record.get("paramValue")
            try:
                value = int(raw)
            except (TypeError, ValueError):
                logging.warning(f"組態值設定錯誤: {name} = {raw}")
                value = None
            key = f"param_{name}"
            logging.info(f"設定 {key} = {value}")
            return {key: value}

        script_task.expand(record=select_params.output)

    
    select_pooling_need = MsSqlOperator(
        task_id="select_pooling_need",
        mssql_conn_id=MSSQL_CONN_ID,
        sql=SQL_POOLING_NEED,
        parameters=( "{{ params.srcNatName }}" ,),   # SSIS 的 ? 位置參數 ← srcNatName
        handler=single_value_as_named_dict,          # XCom: {"param_poolingNeedVeriButWithoutCheckFileMins": <val>}
        do_xcom_push=True,
        doc_md="第三方塊：帶參數查詢並把單值結果寫入命名變數（XCom dict）。",
    )


    

    merge_params_op = PythonOperator(
        task_id="merge_params",
        python_callable=_merge_params,
    )

    compute_file_routing_op = PythonOperator(
        task_id="compute_file_routing",
        python_callable=_compute_file_routing,
    )

    branch_op = BranchPythonOperator(
        task_id="branch_by_filetype",
        python_callable=_choose_branch,
    )

    # ---------- 兩個 TriggerDagRunOperator 作為終點 ----------
    trigger_checkfile = TriggerDagRunOperator(
        task_id="trigger_checkfile_pipeline",
        trigger_dag_id="{{ params.left_checkfile_dag_id }}",
        reset_dag_run=True,
        wait_for_completion=False,
        conf="{{ ti.xcom_pull(task_ids='compute_file_routing', key='routing_ctx') }}",
    )

    trigger_rawdata = TriggerDagRunOperator(
        task_id="trigger_rawdata_pipeline",
        trigger_dag_id="{{ params.right_rawdata_dag_id }}",
        reset_dag_run=True,
        wait_for_completion=False,
        conf="{{ ti.xcom_pull(task_ids='compute_file_routing', key='routing_ctx') }}",
    )

    no_op_end = PythonOperator(
        task_id="no_op_end",
        python_callable=lambda: None,
    )
   


    select_params >> foreach_params >> select_pooling_need >> merge_params_op
    merge_params_op >> compute_file_routing_op >> branch_op
    branch_op >> [trigger_checkfile, trigger_rawdata, no_op_end]