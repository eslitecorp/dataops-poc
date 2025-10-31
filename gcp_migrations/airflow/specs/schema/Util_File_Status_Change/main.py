from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from datetime import datetime

SQL_UPDATE = """
UPDATE dbo.FILE_REC
SET    FILE_STATUS = ?,
       [SOURCEID]  = ?,
       [EXECUTIONID] = ?
WHERE  FILE_NAME = ?
"""

SQL_LOG_TO_HIS = """
INSERT INTO dbo.FILE_REC_HIS (
    [FILE_NAME],
    [FILE_STATUS],
    [ROW_COUNT_REAL],
    [CHECK_SUM_REAL],
    [CHECK_FILE_NAME],
    [ROW_COUNT],
    [CHECK_SUM],
    [SYS_NAME],
    [ETL_FILE_LOADED_DATE],
    [SOURCEID],
    [EXECUTIONID],
    [FILE_OP],
    [HEADER_SKIP_ROWS],
    [CHECK_VALUE],
    [WORK_DATE],
    [EKORG],
    [TABLE_NAME],
    [FILE_TYPE],
    [MACHINE_NAME]
)
SELECT
    [FILE_NAME],
    [FILE_STATUS],
    [ROW_COUNT_REAL],
    [CHECK_SUM_REAL],
    [CHECK_FILE_NAME],
    [ROW_COUNT],
    [CHECK_SUM],
    [SYS_NAME],
    GETDATE(),
    %(source_id)s,            -- ← SSIS 的 System::TaskID
    %(execution_id)s,         -- ← SSIS 的 System::ServerExecutionID
    [FILE_OP],
    [HEADER_SKIP_ROWS],
    [CHECK_VALUE],
    [WORK_DATE],
    [EKORG],
    [TABLE_NAME],
    [FILE_TYPE],
    [MACHINE_NAME]
FROM dbo.FILE_REC
WHERE FILE_NAME = %(file_name)s;  -- ← SSIS 的 $Package::fileName
"""

SQL_UPDATE_TAB_STATUS_IF = r"""
DECLARE @fileStatus INT;
DECLARE @sysName     NVARCHAR(20);
DECLARE @ekorg       NVARCHAR(50);
DECLARE @tableName   NVARCHAR(128);

SELECT
    @sysName    = SYS_NAME,
    @ekorg      = EKORG,
    @tableName  = TABLE_NAME,
    @fileStatus = FILE_STATUS
FROM dbo.FILE_REC
WHERE FILE_NAME = %(file_name)s;

IF (@fileStatus > 0 AND @fileStatus < 10)
BEGIN
    UPDATE dbo.STAGE_TAB_ACT
    SET TAB_STATUS = 3
    WHERE SYS_NAME   = @sysName
      AND EKORG      = @ekorg
      AND TABLE_NAME = @tableName;
END
"""

with DAG(
    dag_id="Util_File_Status_Change",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["ssis_migration", "error_handle"],
) as dag:
    
    # Step 1: 更新 FILE_REC 的 FILE_STATUS
    update_file_status = MsSqlOperator(
        task_id="update_FILE_REC_FILE_STATUS",
        doc_md="更新 FILE_REC 的 FILE_STATUS 欄位",
        sql=SQL_UPDATE,
        parameters={
            "file_status": "{{ dag_run.conf.get('file_status', params.get('file_status', 0)) }}",
            "file_name":   "{{ dag_run.conf.get('file_name', params.get('file_name', 'UNKNOWN')) }}",
            "source_id":   "{{ dag_run.conf.get('task_id_hint', ti.task_id) }}",
            "execution_id":"{{ dag_run.run_id }}",
        }
        #parameters=[
        #    "{{ params.file_status | int }}",
        #    "{{ params.task_id }}",
        #    "{{ params.server_execution_id | int }}",
        #    "{{ params.file_name }}",
        #],
    )

    # Step 2: 寫入 FILE_REC_HIS 紀錄
    log_into_file_rec_his = MsSqlOperator(
        task_id="log_into_FILE_REC_HIS",
        mssql_conn_id="mssql_ssis",
        sql=SQL_LOG_TO_HIS,
        parameters={
            "source_id":    "{{ dag_run.conf.get('task_id_hint', ti.task_id) }}",
            "execution_id": "{{ dag_run.run_id }}",
            "file_name":    "{{ dag_run.conf.get('file_name', params.get('file_name','UNKNOWN')) }}",
        },
    )

    # Step 3: 更新 TAB_STATUS 為錯誤
    update_tab_status_error = MsSqlOperator(
        task_id="update_TAB_STATUS_as_error",
        mssql_conn_id="mssql_ssis",
        sql=SQL_UPDATE_TAB_STATUS_IF,
        parameters={
            "file_name": "{{ dag_run.conf.get('file_name', params.get('file_name','UNKNOWN')) }}",
        },
    )

    # 流程鏈結 (由上而下)
    update_file_status >> log_into_file_rec_his >> update_tab_status_error
