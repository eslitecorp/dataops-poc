from __future__ import annotations
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from plugins.mssql_utils import (
    run_scalar,
    run_query_fetchall,
    run_txn,
    call_stored_procedure,
    bulk_insert_tsql,
    batch_insert_executemany,
    get_pandas_df,
)

MSSQL_CONN_ID = "mssql_default"

with DAG(
    dag_id="mssql_hook_demo",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["mssql", "hook-demo"],
) as dag:

    t1 = PythonOperator(
        task_id="count_pending_files",
        python_callable=lambda: print(
            "PENDING files =",
            run_scalar("SELECT COUNT(*) FROM dbo.FILE_REC WHERE FILE_STATUS = ?", [0], MSSQL_CONN_ID),
        ),
    )

    t2 = PythonOperator(
        task_id="list_recent_files",
        python_callable=lambda: print(
            run_query_fetchall(
                "SELECT TOP 5 FILE_NAME, FILE_STATUS FROM dbo.FILE_REC ORDER BY CREATED_AT DESC",
                None,
                MSSQL_CONN_ID,
            )
        ),
    )

    def _txn():
        stmts = [
            ("UPDATE dbo.FILE_REC SET FILE_STATUS = ? WHERE FILE_NAME = ?", [2, "abc.csv"]),
            ("INSERT INTO dbo.AUDIT_LOG(evt, info) VALUES(?,?)", ["FILE_STATUS_CHANGED", "abc.csv"]),
        ]
        run_txn(stmts, MSSQL_CONN_ID)

    t3 = PythonOperator(task_id="txn_update_and_audit", python_callable=_txn)

    t4 = PythonOperator(
        task_id="exec_sp",
        python_callable=lambda: call_stored_procedure(
            "dbo.usp_recalculate_metrics", {"p_date": "2025-10-28", "force": 1}, MSSQL_CONN_ID
        ),
    )

    def _bulk():
        bulk_insert_tsql(
            table="dbo.StagingTable",
            file_path=r"\\shared\incoming\sales_20251028.csv",
            with_options={"FIRSTROW": "2", "FIELDTERMINATOR": "','", "ROWTERMINATOR": "'0x0a'", "TABLOCK": ""},
            mssql_conn_id=MSSQL_CONN_ID,
        )

    t5 = PythonOperator(task_id="bulk_insert", python_callable=_bulk)

    def _batch():
        rows = [
            ["f1.csv", 2],
            ["f2.csv", 2],
            ["f3.csv", 2],
        ]
        inserted = batch_insert_executemany(
            table="dbo.FILE_REC_TMP",
            columns=["FILE_NAME", "FILE_STATUS"],
            rows=rows,
            mssql_conn_id=MSSQL_CONN_ID,
        )
        print("Inserted rows:", inserted)

    t6 = PythonOperator(task_id="batch_insert_fast", python_callable=_batch)

    t7 = PythonOperator(
        task_id="df_peek",
        python_callable=lambda: print(
            get_pandas_df(
                "SELECT TOP 3 FILE_NAME, FILE_STATUS FROM dbo.FILE_REC WHERE FILE_STATUS = ?",
                [2],
                MSSQL_CONN_ID,
            )
        ),
    )

    t1 >> t2 >> t3 >> [t4, t5, t6] >> t7
