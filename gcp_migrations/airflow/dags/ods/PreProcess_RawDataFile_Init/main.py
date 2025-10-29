from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
from airflow import DAG
from plugins.sensors.file_unlocked_sensor import FileUnlockedSensor
from plugins.operators.file_rename_operator import FileRenameByEkorgOperator

DAG_ID = "ods_wait_and_rename"

MSSQL_CONN_ID = "mssql_ssis"   # 改成你的 Connection ID

SQL_FILE_STATUS = """
SELECT TOP 1 ISNULL(file_status,0) AS file_status
FROM [dbo].[FILE_REC]
WHERE [FILE_NAME] = %s
UNION
SELECT -1
FROM [dbo].[FILE_REC]
WHERE [FILE_NAME] = %s
HAVING COUNT(*) = 0
"""

def fetch_file_status(mssql_conn_id: str, file_name: str, **_):
    """
    以兩個相同參數查詢 file_status；若無資料則回傳 -1（對齊 SQL）
    並將結果 push 到 XCom key='file_status'
    """
    hook = MsSqlHook(mssql_conn_id=mssql_conn_id)
    rows = hook.get_records(SQL_FILE_STATUS, parameters=(file_name, file_name))
    status = int(rows[0][0]) if rows and rows[0] and rows[0][0] is not None else -1
    return status  # PythonOperator 會把 return 值存到 XCom（key='return_value'）

def route_by_file_status(ti, **_):
    s = int(ti.xcom_pull(task_ids="query_file_status"))  # 取上一個任務 return_value
    # 三向條件，完全等同你的 SSIS 表達式
    go_left  = (s <= 0)
    go_mid   = (s > 0) and not (40 < s < 80) and not (90 < s) and not (s == 8)
    # go_right = 其他情形
    if go_left:
        return "path_left"
    elif go_mid:
        return "path_mid"
    else:
        return "path_right"

with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 0},
) as dag:

    SRC_DIR = "{{ params.srcRawDataFilePath }}"      # e.g. /data/incoming
    SRC_NAME = "{{ params.srcRawDataFileName }}"     # e.g. PO_20251029.csv
    EKORG    = "{{ params.ekorg }}"                  # e.g. 1000

    wait_unlocked = FileUnlockedSensor(
        task_id="wait_file_unlocked",
        path=f"{SRC_DIR}/{SRC_NAME}",
        stable_secs=4,
        poke_interval=2,
        mode="reschedule",
        timeout=60*60,
    )

    rename_by_ekorg = FileRenameByEkorgOperator(
        task_id="rename_by_ekorg",
        src_dir=SRC_DIR,
        src_filename=SRC_NAME,
        ekorg=EKORG,
        overwrite=False,    # 若需要覆蓋，改 True
    )

    query_file_status = PythonOperator(
        task_id="query_file_status",
        python_callable=fetch_file_status,
        op_kwargs={
            "mssql_conn_id": MSSQL_CONN_ID,
            # 兩個 ? 都是檔名；此處直接從上一節點的 XCom 取回新檔名
            "file_name": "{{ ti.xcom_pull(task_ids='rename_by_ekorg', key='res_srcRawDataFileName') }}",
        },
    )

    branch = BranchPythonOperator(
        task_id="branch_by_status",
        python_callable=route_by_file_status,
    )
    path_left  = EmptyOperator(task_id="path_left")
    path_mid   = EmptyOperator(task_id="path_mid")
    path_right = EmptyOperator(task_id="path_right")


    wait_unlocked >> rename_by_ekorg


    insert_file_rec = MsSqlOperator(
        task_id="insert_file_rec",
        mssql_conn_id=MSSQL_CONN_ID,
        sql="""
            INSERT INTO [dbo].[FILE_REC] (
                [FILE_NAME],[ROW_COUNT],[CHECK_SUM],[FILE_TYPE],[ETL_FILE_LOADED_DATE],
                [SOURCEID],[EXECUTIONID],[SYS_NAME],[FILE_OP],[CHECK_VALUE],
                [TABLE_NAME],[WORK_DATE],[EKORG],[MACHINE_NAME]
            ) VALUES (
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
            )
        """,
        parameters=[
            "{{ ti.xcom_pull(task_ids='rename_by_ekorg', key='res_srcRawDataFileName') }}",  # $Package::fileName
            "{{ params.rowCount }}",
            "{{ params.checkSum }}",
            "{{ params.fileType }}",
            "{{ params.etlFileLoadedTime }}",
            "{{ params.TaskID }}",
            "{{ params.ServerExecutionID }}",
            "{{ params.sysName }}",
            "{{ params.fileOp }}",
            "{{ params.checkValue }}",
            "{{ params.tableName }}",
            "{{ params.workDate }}",
            "{{ params.ekorg }}",
            "{{ params.machineName }}",
        ],
    )

    trigger_next = TriggerDagRunOperator(
        task_id="trigger_next_dag",
        trigger_dag_id="ods_next_stage",  # 改成實際要觸發的 DAG ID
        conf={
            "fileName": "{{ ti.xcom_pull(task_ids='rename_by_ekorg', key='res_srcRawDataFileName') }}",
            "ekorg": "{{ params.ekorg }}",
        },
        wait_for_completion=False,  # 若要等待可改 True
    )

    mid_trigger_next = TriggerDagRunOperator(
        task_id="mid__trigger_next",
        trigger_dag_id="ods_mid_stage",      # 改成要觸發的 DAG ID
        wait_for_completion=False,           # 若要等對方跑完再回報，改 True
        conf={
            # 你想傳給下一個 DAG 的上下文（範例）
            "fileName": "{{ ti.xcom_pull(task_ids='rename_by_ekorg', key='res_srcRawDataFileName') }}",
            "ekorg": "{{ params.ekorg }}",
            "fileStatus": "{{ ti.xcom_pull(task_ids='query_file_status') }}",
            "workDate": "{{ params.workDate }}",
        },
    )

    update_file_rec = MsSqlOperator(
        task_id="update_file_rec",
        mssql_conn_id=MSSQL_CONN_ID,
        sql="""
            UPDATE [dbo].[FILE_REC]
            SET CHECK_VALUE=%s,
                FILE_OP=%s,
                TABLE_NAME=%s,
                ETL_FILE_LOADED_DATE=(
                    CASE
                        WHEN (ETL_FILE_LOADED_DATE IS NOT NULL AND [EXECUTIONID]=%s)
                        THEN ETL_FILE_LOADED_DATE
                        ELSE GETDATE()
                    END
                ),
                EXECUTIONID=%s,
                WORK_DATE=%s,
                MACHINE_NAME=%s
            WHERE FILE_NAME=%s
        """,
        parameters=[
            "{{ params.checkValue }}",
            "{{ params.fileOp }}",
            "{{ params.dbTableName }}",
            "{{ params.ServerExecutionID }}",
            "{{ params.ServerExecutionID }}",
            "{{ params.workDate }}",
            "{{ params.machineName }}",
            "{{ ti.xcom_pull(task_ids='rename_by_ekorg', key='res_srcRawDataFileName') }}",
        ],
    )

    right_trigger_next = TriggerDagRunOperator(
        task_id="right__trigger_next",
        trigger_dag_id="ods_right_stage",     # 改成實際要觸發的 DAG ID
        wait_for_completion=False,
        conf={
            "fileName": "{{ ti.xcom_pull(task_ids='rename_by_ekorg', key='res_srcRawDataFileName') }}",
            "ekorg": "{{ params.ekorg }}",
            "fileStatus": "{{ ti.xcom_pull(task_ids='query_file_status') }}",
            "workDate": "{{ params.workDate }}",
        },
    )

    rename_by_ekorg >> query_file_status >> branch
    branch >> [path_left, path_mid, path_right]
    path_left >> insert_file_rec >> trigger_next
    path_mid >> mid_trigger_next
    path_right >> update_file_rec >> right_trigger_next
