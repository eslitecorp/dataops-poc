from __future__ import annotations
from datetime import datetime, timedelta
import os, shutil

from airflow import DAG
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from airflow.operators.branch import BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.operators.empty import EmptyOperator

DAG_ID = "preprocess_file_status_branch"

with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args={"owner": "dataops", "retries": 0, "retry_delay": timedelta(minutes=3)},
    params={
        # 供查檔狀態
        "srcRawDataFileName": "ODS_SAP_MM_20250101.CSV",
        "param_fileStatusReloadFail": 8,
        # 右分支需要的 3 參數（等同 SSIS 子封裝對應）
        "srcErrorFileFolder": "/data/error",
        "srcRawDataFilePath": "/data/in/ODS_SAP_MM_20250101.CSV",
        # 通知信（可由 dag_run.conf 覆寫）
        "email_to": ["ops@example.com"],
        "email_cc": [],
        "email_subject": "[ETL][ReloadFail] {{ dag_run.conf.get('srcRawDataFileName', params.srcRawDataFileName) }} moved to error folder",
    },
    doc_md="""
方塊3：查 `FILE_REC` 的 `file_status` → 填入變數 → 分流  
- 左：`srcFileStatus != param_fileStatusReloadFail`（保留為 Trigger 或你要的後續任務）  
- 右：搬檔到錯誤資料夾 → 寄信（**同一支 DAG** 內完成）
""",
) as dag:

    # 1) 查詢 file_status
    query_file_status = MsSqlOperator(
        task_id="query_file_status",
        mssql_conn_id="mssql_ssis",  # TODO: 改成你的 connection id
        sql="""
        SELECT TOP 1 ISNULL(file_status, 0) AS file_status
        FROM [dbo].[FILE_REC]
        WHERE [FILE_NAME] = ?
        UNION
        SELECT -1
        FROM [dbo].[FILE_REC]
        WHERE [FILE_NAME] = ?
        HAVING COUNT(*) = 0
        """,
        parameters=[
            "{{ dag_run.conf.get('srcRawDataFileName', params.srcRawDataFileName) }}",
            "{{ dag_run.conf.get('srcRawDataFileName', params.srcRawDataFileName) }}",
        ],
        do_xcom_push=True,
    )

    # 2) Branch：決定往左或往右
    def _branch_on_status(ti, **context) -> str:
        rows = ti.xcom_pull(task_ids="query_file_status") or []
        status = int(rows[0][0]) if rows and rows[0] else 0
        ti.xcom_push(key="srcFileStatus", value=status)

        reload_fail = int(
            (context.get("dag_run") and context["dag_run"].conf.get("param_fileStatusReloadFail"))
            or context["params"]["param_fileStatusReloadFail"]
        )
        return "go_left" if status != reload_fail else "move_to_error_folder"

    decide_next = BranchPythonOperator(
        task_id="decide_next",
        python_callable=_branch_on_status,
    )

    # ============ 左分支（保持你原本設計：可觸發別的 DAG 或接你要的任務） ============
    go_left = TriggerDagRunOperator(
        task_id="go_left",
        trigger_dag_id="downstream_when_ok",  # TODO: 換成左分支實際 DAG id，或改成其他任務
        conf={
            "srcRawDataFileName": "{{ dag_run.conf.get('srcRawDataFileName', params.srcRawDataFileName) }}",
            "srcFileStatus": "{{ ti.xcom_pull(task_ids='decide_next', key='srcFileStatus') }}",
        },
        wait_for_completion=False,
        reset_dag_run=True,
    )

    # ============ 右分支（同一 DAG 內）============
    def _move_to_error_folder(**context) -> str:
        conf = (context.get("dag_run") and context["dag_run"].conf) or {}
        p = {**context["params"], **conf}

        dest_dir  = p["srcErrorFileFolder"]                # = SSIS 的 $Package::srcErrorFileFolder
        dest_name = p.get("srcRawDataFileName")            # = SSIS 的 $Package::srcRawDataFileName
        src_path  = p["srcRawDataFilePath"]                # = SSIS 的 $Package::srcRawDataFilePath

        os.makedirs(dest_dir, exist_ok=True)
        if not os.path.exists(src_path):
            raise FileNotFoundError(f"sourceFilePath not found: {src_path}")

        # 目標路徑
        dest_path = os.path.join(dest_dir, dest_name or os.path.basename(src_path))
        if os.path.exists(dest_path):  # 避免覆蓋：已存在就加時間戳
            base, ext = os.path.splitext(os.path.basename(dest_path))
            from datetime import datetime as _dt
            ts = _dt.utcnow().strftime("%Y%m%dT%H%M%SZ")
            dest_path = os.path.join(dest_dir, f"{base}__dup_{ts}{ext}")

        shutil.move(src_path, dest_path)
        return dest_path  # 存到 XCom，給寄信用

    move_to_error_folder = PythonOperator(
        task_id="move_to_error_folder",
        python_callable=_move_to_error_folder,
    )

    send_notification = EmailOperator(
        task_id="send_notification",
        to="{{ dag_run.conf.get('email_to', params.email_to) }}",
        cc="{{ dag_run.conf.get('email_cc', params.email_cc) }}",
        subject="{{ dag_run.conf.get('email_subject', params.email_subject) }}",
        html_content="""
        <p>Reload Fail 分支已執行。</p>
        <ul>
          <li><b>Source</b>: {{ dag_run.conf.get('srcRawDataFilePath', params.srcRawDataFilePath) }}</li>
          <li><b>Destination Folder</b>: {{ dag_run.conf.get('srcErrorFileFolder', params.srcErrorFileFolder) }}</li>
          <li><b>Moved To</b>: {{ ti.xcom_pull(task_ids='move_to_error_folder') }}</li>
          <li><b>Status</b>: {{ ti.xcom_pull(task_ids='decide_next', key='srcFileStatus') }}</li>
          <li><b>Run Id</b>: {{ run_id }}</li>
          <li><b>Execution Time (UTC)</b>: {{ ts }}</li>
        </ul>
        """,
    )

    # （可選）收斂占位
    join = EmptyOperator(task_id="join", trigger_rule="none_failed_min_one_success")

    # 串接
    query_file_status >> decide_next
    decide_next >> go_left
    decide_next >> move_to_error_folder >> send_notification
    [go_left, send_notification] >> join
