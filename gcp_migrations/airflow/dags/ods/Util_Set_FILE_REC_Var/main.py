# gcp_migrations/airflow/dags/ods_preprocess_file_event.py
from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Optional

from airflow import DAG
from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

DAG_ID = "Util_Set_FILE_REC_Var"

class MsSqlSelectScalarToXComOperator(BaseOperator):
    """
    Execute a SELECT and push the first cell (row0,col0) to XCom.
    - sql, parameters 為可 template 欄位
    """
    template_fields = ("sql", "parameters", "xcom_key")

    def __init__(
        self,
        *,
        mssql_conn_id: str,
        sql: str,
        parameters: dict | None = None,
        xcom_key: str = "result",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.mssql_conn_id = mssql_conn_id
        self.sql = sql
        self.parameters = parameters or {}
        self.xcom_key = xcom_key

    def execute(self, context: Context):
        hook = MsSqlHook(mssql_conn_id=self.mssql_conn_id)
        row = hook.get_first(self.sql, parameters=self.parameters)  # returns a tuple or None
        value = row[0] if row and len(row) else None
        context["ti"].xcom_push(key=self.xcom_key, value=value)
        self.log.info("Pushed XCom key=%s value=%r", self.xcom_key, value)
        return value
    

class ExtractWriteEventOperator(BaseOperator):
    """
    Mirror of the original C# Script Task:

    Inputs (templated):
      - src_sys_name: string (kept for parity; not used in logic here)
      - file_pattern: regex string; its first capturing group MUST be the event code to extract
      - file_name: the raw file name (may contain compressed suffix markers and '(...)')

    Behavior:
      1) If file_name (case-insensitive) contains .ZIP/.7Z/.RAR -> remove the last '(...)' segment.
      2) re.search(file_pattern, file_name, flags=re.IGNORECASE)
      3) If matched, event = group(1); push to XCom with key = xcom_key and also return it.

    Output:
      - XCom: key=<xcom_key>, value=<event>
      - return value: <event>
    """
    template_fields = ("src_sys_name", "file_pattern", "file_name", "xcom_key")

    def __init__(
        self,
        *,
        src_sys_name: str,
        file_pattern: str,
        file_name: str,
        xcom_key: str = "cf_FILE_OP_child",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.src_sys_name = src_sys_name
        self.file_pattern = file_pattern
        self.file_name = file_name
        self.xcom_key = xcom_key

    @staticmethod
    def _strip_last_parentheses_segment(name: str) -> str:
        """
        Remove the last '(...)' from the string if both '(' and ')' exist and ')' is after '('.
        """
        l = name.rfind("(")
        r = name.rfind(")")
        if l != -1 and r != -1 and l < r:
            return name[:l] + name[r + 1 :]
        return name

    def execute(self, context: Context) -> Optional[str]:
        fname = self.file_name
        upper = fname.upper()

        # Step 1: compressed case → drop last '(...)'
        if any(ext in upper for ext in (".ZIP", ".7Z", ".RAR")):
            fname = self._strip_last_parentheses_segment(fname)

        # Step 2: match regex; first group is the event we want
        m = re.search(self.file_pattern, fname, flags=re.IGNORECASE)
        event: Optional[str] = m.group(1) if m else None

        # Step 3: XCom push (explicit) + return (implicit XCom of task return)
        ti = context["ti"]
        ti.xcom_push(key=self.xcom_key, value=event)
        self.log.info("Extracted writeInEvent=%r from file_name=%r using pattern=%r", event, fname, self.file_pattern)
        return event


def copy_child_variables(**context):
    """
    模擬 SSIS Script Task 的變數同步行為：
    將 *_child XCom 的值寫回母變數 key
    """
    ti = context["ti"]

    mappings = {
        "cf_FILE_OP_child": "cf_FILE_OP",
        "cf_CHECK_VALUE_child": "cf_CHECK_VALUE",
        "param_WorkDate_child": "param_WorkDate",
    }

    for child_key, parent_key in mappings.items():
        val = ti.xcom_pull(key=child_key)
        ti.xcom_push(key=parent_key, value=val)
        print(f"Copied {child_key} → {parent_key} = {val!r}")

    return "success"


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    params={
        # 與原 C# 對齊的輸入（可用 dagrun.conf 覆蓋）
        "srcSysName": "TW_SAP",  # e.g. "TW_SAP"
        # file pattern：第一個 () 必須是要擷取的事件代號（I / IU / D 等）
        # 例：ODS + 系統 + '_' + 事件 + '_' + 日期 + .CSV
        #     ODS_TW_SAP_I_20250101.CSV
        "param_fileNamePattern": r"ODS_[A-Z0-9_]+_(I|IU|D)_[0-9]{8}\.CSV",
        "srcRawDataFileName": "ODS_TW_SAP_I_20250101.CSV",
    },
    doc_md=__doc__,
) as dag:

    extract_event = ExtractWriteEventOperator(
        task_id="extract_write_event",
        src_sys_name="{{ dag_run.conf.get('srcSysName', params.srcSysName) }}",
        file_pattern="{{ dag_run.conf.get('param_fileNamePattern', params.param_fileNamePattern) }}",
        file_name="{{ dag_run.conf.get('srcRawDataFileName', params.srcRawDataFileName) }}",
        xcom_key="cf_FILE_OP_child",
    )

    resolve_file_op = MsSqlSelectScalarToXComOperator(
        task_id="resolve_file_op",
        mssql_conn_id="mssql_ssis",   # 改成你的 Airflow Connection ID
        xcom_key="cf_FILE_OP_child",  # 與 SSIS 變數同名，覆蓋或補值都一致
        # 說明：
        # 1) 先讀上一節點的 XCom，如果非空，直接回傳該值
        # 2) 否則查表 STAGE_TAB_ACT 取 DEF_OP
        # 3) 參數化 where 條件用 %(...)s；值由 params 或 dagrun.conf 注入
        sql="""
        SELECT CASE
            WHEN LEN({{ (ti.xcom_pull(task_ids='extract_write_event', key='cf_FILE_OP_child') or '') | tojson | safe }}) > 2
            THEN {{ (ti.xcom_pull(task_ids='extract_write_event', key='cf_FILE_OP_child') or '') | tojson | safe }}
            ELSE (
                SELECT DEF_OP
                FROM dbo.STAGE_TAB_ACT
                WHERE NATIONALITY = %(nat)s
                AND SYS_NAME    = %(sys)s
                AND EKORG       = %(ekorg)s
                AND TABLE_NAME  = %(tbl)s
            )
        END
        """,
        parameters={
            "nat":  "{{ dag_run.conf.get('srcNatName',  params.srcNatName)  }}",
            "sys":  "{{ dag_run.conf.get('srcSysName',  params.srcSysName)  }}",
            "ekorg":"{{ dag_run.conf.get('srcEkorg',    params.srcEkorg)    }}",
            "tbl":  "{{ dag_run.conf.get('dbTableName', params.dbTableName) }}",
        },
    )

    get_check_value = MsSqlSelectScalarToXComOperator(
        task_id="get_check_value",
        mssql_conn_id="mssql_ssis",              # 改成你的連線 ID
        xcom_key="cf_CHECK_VALUE_child",         # 對齊 SSIS 變數名稱
        sql="""
            SELECT CHECK_VALUE
            FROM dbo.STAGE_TAB_ACT
            WHERE NATIONALITY = %(nat)s
            AND SYS_NAME    = %(sys)s
            AND EKORG       = %(ekorg)s
            AND TABLE_NAME  = %(tbl)s
        """,
        # SSIS 畫面中的參數 0/1/2/3 對應到下列四個命名參數
        parameters={
            "nat":  "{{ dag_run.conf.get('srcNatName',  params.srcNatName)  }}",
            "sys":  "{{ dag_run.conf.get('srcSysName',  params.srcSysName)  }}",
            "ekorg":"{{ dag_run.conf.get('srcEkorg',    params.srcEkorg)    }}",
            "tbl":  "{{ dag_run.conf.get('dbTableName', params.dbTableName) }}",
        },
    )

    get_work_date = MsSqlSelectScalarToXComOperator(
        task_id="get_work_date",
        mssql_conn_id="mssql_ssis",          # 換成你的連線 ID
        xcom_key="param_WorkDate_child",     # 對齊 SSIS 變數 User::param_WorkDate_child
        sql="""
            SELECT PARAM_VALUE
            FROM StageETLDB.dbo.USER_PARAMS
            WHERE TYPE = 2 AND PARAM_NAME = 'WorkDate'
        """,
    )

    sync_variables = PythonOperator(
        task_id="sync_variables",
        python_callable=copy_child_variables,
        provide_context=True,
    )

    extract_event >> resolve_file_op >> get_check_value >> get_work_date >> sync_variables
   