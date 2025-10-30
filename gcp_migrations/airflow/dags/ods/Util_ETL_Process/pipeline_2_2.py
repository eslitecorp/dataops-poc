from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Tuple
from datetime import datetime as dt

from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from pydantic import BaseModel

# ---------- 基本設定 ----------
DAG_ID = "stage_branch_with_abcd"
MSSQL_CONN_ID = "mssql_staging"

UPSTREAM_DAG = "prestage_or_checkfile"               # 第一段：決定成敗
FAILURE_HANDLER_DAG = "on_failure_compensation"      # 第一段失敗

# 第二段：stage 成功後，先觸發這個，再做 calc → 分流
STAGE_SUCCESS_TRIGGER_DAG = "on_stage_success_pre"   # 成功分支_先觸發
STAGE_FAILURE_TRIGGER_DAG = "on_stage_failure_pre"   # 失敗分支_直接觸發

# 第三段：calc 後依 varlIntoStageRows 分流
ROWS_EQ_0_DAG = "on_rows_eq_0"                       # varlIntoStageRows == 0
ROWS_GT_0_DAG = "on_rows_gt_0"                       # varlIntoStageRows > 0

CONF_JINJA = "{{ dag_run.conf or {} }}"

# ---------- Pydantic 模型（B） ----------
class GiftRow(BaseModel):
    GIFT_NO: str
    GIFT_NAME: str | None = None
    GIFT_COST: float | None = None
    FILE_NAME: str | None = None
    PKEY: int | None = None

# ---------- ABCD ----------
def a_fetch_rows(**context) -> List[Dict[str, Any]]:
    conf = context["dag_run"].conf or {}
    file_name: str = conf.get("file_name", "abc.csv")
    src_query: str = conf.get(
        "sql_query_prestage",
        """
        SELECT *
        FROM prestage_LMS.GIFT g
        WHERE g.FILE_NAME = %(file_name)s
          AND g.PKEY NOT IN (
              SELECT ISNULL(PRESTAGE_PKEY,0)
              FROM dbo.veri_prestage_log_fail_detail fd
              JOIN dbo.VERI_PRESTAGE_LOG l ON fd.LOG_PKEY = l.PKEY
              WHERE l.file_name = %(file_name)s AND l.executionid = 0
          );
        """,
    )
    hook = MsSqlHook(mssql_conn_id=MSSQL_CONN_ID)
    rows = hook.get_records(src_query, parameters={"file_name": file_name})
    cols = [d[0] for d in hook.get_cursor().description]
    return [dict(zip(cols, r)) for r in rows]

def b_transform_with_pydantic(**context) -> List[Dict[str, Any]]:
    rows = context["ti"].xcom_pull(task_ids="stage_write_with_rollback.abcd.a_fetch_rows") or []
    return [GiftRow(**r).model_dump() for r in rows]

def c_add_etl_date(**context) -> List[Dict[str, Any]]:
    rows = context["ti"].xcom_pull(task_ids="stage_write_with_rollback.abcd.b_transform_with_pydantic") or []
    now = dt.utcnow()
    for r in rows: r["ETL_DATE"] = now
    return rows

def d_bulk_insert(**context) -> int:
    conf = context["dag_run"].conf or {}
    dest_table: str = conf.get("db_stage_table_name", "stage_LMS.GIFT")
    rows = context["ti"].xcom_pull(task_ids="stage_write_with_rollback.abcd.c_add_etl_date") or []
    if not rows: return 0
    cols = ["GIFT_NO","GIFT_NAME","GIFT_COST","FILE_NAME","PKEY","ETL_DATE"]
    vals = [tuple(row.get(c) for c in cols) for row in rows]
    placeholders = ",".join(["%s"] * len(cols))
    sql = f"INSERT INTO {dest_table} ({','.join(cols)}) VALUES ({placeholders})"
    hook = MsSqlHook(mssql_conn_id=MSSQL_CONN_ID)
    conn = hook.get_conn(); cur = conn.cursor()
    try: cur.fast_executemany = True
    except Exception: pass
    cur.executemany(sql, vals); conn.commit()
    return len(vals)

# ---------- calc 結果 → 變數（XCom） ----------
def save_varl_into_stage_rows(**context) -> int:
    """
    讀取 MsSqlOperator(calc_into_stage_rows) 的 SELECT 結果，
    取第一列第一欄，存成 XCom key='varlIntoStageRows'
    （對應 SSIS: User::varlIntoStageRows）
    """
    rows = context["ti"].xcom_pull(task_ids="calc_into_stage_rows") or []
    value = int(rows[0][0]) if rows and rows[0] else 0
    context["ti"].xcom_push(key="varlIntoStageRows", value=value)
    return value

def branch_by_rows(**context) -> str:
    """
    依 varlIntoStageRows 分流：
    ==0 → fire_rows_eq_0
    >0  → fire_rows_gt_0
    """
    ti = context["ti"]
    v = int(ti.xcom_pull(key="varlIntoStageRows") or 0)
    return "fire_rows_eq_0" if v == 0 else "fire_rows_gt_0"

# ---------- DAG ----------
with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "dataops", "retries": 0},
    tags=["ssis-refactor", "taskgroup", "mssql"],
    description="Upstream→(fail->TriggerA_fail | success->stage_write) → success TriggerA → calc → branch(==0|>0) → TriggerB",
) as dag:

    # 第0段：先觸發上游並等待結果
    fire_upstream = TriggerDagRunOperator(
        task_id="fire_upstream",
        trigger_dag_id=UPSTREAM_DAG,
        conf=CONF_JINJA,
        wait_for_completion=True,
        poke_interval=10,
        allowed_states=["success","failed"],
        failed_states=["failed"],
        reset_dag_run=True,
    )

    fire_on_failure = TriggerDagRunOperator(
        task_id="fire_on_failure",
        trigger_dag_id=FAILURE_HANDLER_DAG,
        conf=CONF_JINJA,
        trigger_rule=TriggerRule.ONE_FAILED,  # 上游失敗
        wait_for_completion=False,
        reset_dag_run=True,
    )

    # 第1段：成功 → 進入「刪除 + ABCD(TaskGroup)」
    with TaskGroup(group_id="stage_write_with_rollback", tooltip="刪除→ABCD") as stage_write_with_rollback:

        delete_before_upsert = MsSqlOperator(
            task_id="delete_before_upsert",
            mssql_conn_id=MSSQL_CONN_ID,
            sql="""
                EXEC [dbo].[DELETE_STAGE_BEFORE_UPSERT]
                    @srcSysNameEkorg    = {{ (dag_run.conf or {}).get('srcSysNameEkorg') | sqlsafe }},
                    @dbTableName        = {{ (dag_run.conf or {}).get('dbTableName') | sqlsafe }},
                    @srcRawDataFileName = {{ (dag_run.conf or {}).get('srcRawDataFileName') | sqlsafe }},
                    @serverExecutionId  = {{ (dag_run.conf or {}).get('serverExecutionId', 0) }};
            """,
            autocommit=True,
        )

        with TaskGroup(group_id="abcd", tooltip="A抽取→B轉型→C補欄位→D寫入") as abcd:
            a = PythonOperator(task_id="a_fetch_rows", python_callable=a_fetch_rows)
            b = PythonOperator(task_id="b_transform_with_pydantic", python_callable=b_transform_with_pydantic)
            c = PythonOperator(task_id="c_add_etl_date", python_callable=c_add_etl_date)
            d = PythonOperator(task_id="d_bulk_insert", python_callable=d_bulk_insert)
            a >> b >> c >> d

        delete_before_upsert >> abcd

    # 第2段：stage 成功/失敗 → 各自接 Trigger
    # 失敗：直接觸發（不再往下）
    fire_stage_failure_pre = TriggerDagRunOperator(
        task_id="fire_stage_failure_pre",
        trigger_dag_id=STAGE_FAILURE_TRIGGER_DAG,
        conf=CONF_JINJA,
        trigger_rule=TriggerRule.ONE_FAILED,  # 隻要 stage_write_with_rollback 失敗
        wait_for_completion=False,
        reset_dag_run=True,
    )

    # 成功：先觸發 A（而且等它完成），再做 calc
    fire_stage_success_pre = TriggerDagRunOperator(
        task_id="fire_stage_success_pre",
        trigger_dag_id=STAGE_SUCCESS_TRIGGER_DAG,
        conf=CONF_JINJA,
        trigger_rule=TriggerRule.ALL_SUCCESS,
        wait_for_completion=True,   # ★ 等待 Trigger A 完成，才繼續 calc
        poke_interval=10,
        allowed_states=["success", "failed"],
        failed_states=["failed"],
        reset_dag_run=True,
    )

    # 第3段：calc（執行 SQL → 取結果 → 寫入 XCom: varlIntoStageRows）
    calc_into_stage_rows = MsSqlOperator(
        task_id="calc_into_stage_rows",
        mssql_conn_id=MSSQL_CONN_ID,
        sql=r"""
SELECT ISNULL(ROW_COUNT_REAL,0)-ISNULL(FAIL_AMT,0) AS INTO_STAGE_NO
FROM (
  SELECT exeEndInfo.ROW_COUNT_REAL,
         CASE WHEN main.FILE_TYPE = 1 THEN 0 ELSE ISNULL(veriFailCount.FAIL_AMT,0) END AS FAIL_AMT
  FROM (SELECT * FROM STAGE_TAB_HIS) tabs
  JOIN tbs.SYS_NAME main_SYSNAME          ON tabs.SYS_NAME = main_SYSNAME.SYS_NAME
  JOIN tbs.EKORG main_EKORG               ON tabs.EKORG = main_EKORG.EKORG
  JOIN tbs.FILE_NAME main_FILENAME        ON tabs.FILE_NAME = main_FILENAME.FILE_NAME
  JOIN tbs.WORK_DATE main_WORKDATE        ON tabs.WORK_DATE = main_WORKDATE.WORK_DATE
  LEFT JOIN V_FILE_HIS exeEndInfo
         ON main.FILE_NAME = exeEndInfo.FILE_NAME
        AND main.EXECUTIONID = exeEndInfo.EXECUTIONID
  LEFT JOIN V_FILE_VERI_FAIL_COUNT veriFailCount
         ON main.FILE_NAME = veriFailCount.FILE_NAME
        AND main.EXECUTIONID = veriFailCount.EXECUTIONID
  LEFT JOIN V_FILE_VERI_CHECKFILE veriCheckfile
         ON main.FILE_NAME = veriCheckfile.FILE_NAME
        AND main.EXECUTIONID = veriCheckfile.EXECUTIONID
  LEFT JOIN (SELECT PARAM_VALUE FROM [StageETLDB].[dbo].[USER_PARAMS] WHERE TYPE = 2 AND PARAM_NAME = 'WorkDate') p2
         ON 1=1
  WHERE (main.FILE_TYPE = ? OR (main.FILE_TYPE = 0 AND main.FILE_NAME <> ''))
    AND main.EXECUTIONID = ?
    AND main.FILE_NAME = ?
    AND main.SYS_NAME = ?
) STA
        """,
        parameters=(
            "{{ (dag_run.conf or {}).get('file_type', 0) }}",
            "{{ (dag_run.conf or {}).get('serverExecutionId', 0) }}",
            "{{ (dag_run.conf or {}).get('srcRawDataFileName', 'abc.csv') }}",
            "{{ (dag_run.conf or {}).get('srcSysName', 'SYS1') }}",
        ),
        do_xcom_push=True,
    )

    set_varl_into_stage_rows = PythonOperator(
        task_id="set_varl_into_stage_rows",
        python_callable=save_varl_into_stage_rows,
    )

    decide_rows_branch = BranchPythonOperator(
        task_id="decide_rows_branch",
        python_callable=branch_by_rows,
    )

    # 第4段：條件分流 → 兩個 Trigger
    fire_rows_eq_0 = TriggerDagRunOperator(
        task_id="fire_rows_eq_0",
        trigger_dag_id=ROWS_EQ_0_DAG,
        conf=CONF_JINJA,
        wait_for_completion=False,
        reset_dag_run=True,
    )

    fire_rows_gt_0 = TriggerDagRunOperator(
        task_id="fire_rows_gt_0",
        trigger_dag_id=ROWS_GT_0_DAG,
        conf=CONF_JINJA,
        wait_for_completion=False,
        reset_dag_run=True,
    )


    fire_upstream >> [stage_write_with_rollback, fire_on_failure]

    stage_write_with_rollback >> fire_stage_success_pre
    stage_write_with_rollback >> fire_stage_failure_pre

    fire_stage_success_pre >> calc_into_stage_rows >> set_varl_into_stage_rows >> decide_rows_branch
    decide_rows_branch >> [fire_rows_eq_0, fire_rows_gt_0]
