from __future__ import annotations

from airflow import DAG
from airflow.models.param import Param
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

# 1) 先把所有來源系統列成參數（可增減）
ALL_PIPELINES = [
    "LMS", "EC", "POS", "POS_NEC", "SAP", "SMS", "FS", "OM", "TEAM", "ME"
]

with DAG(
    dag_id="SSIS__parallel_etl_skeleton",
    description="Skeleton: run many identical ETL pipelines in parallel (EmptyOperator only).",
    start_date=datetime(2025, 1, 1),
    schedule=None,                 # 先手動觸發；要排程再改
    catchup=False,
    max_active_runs=1,             # 一次只跑一個 logical run；每個管線內仍可並行
    default_args={
        "retries": 0,
        "owner": "dataops",
    },
    params={
        # 之後若要擴充可用 Branch/ShortCircuit 讀 params 做選擇性執行
        "pipelines": Param(ALL_PIPELINES, type="array", description="Which pipelines to include in this run (skeleton builds all groups; selection logic can be added later)."),
        "steps": Param(["extract", "cleanse", "validate", "load"], type="array", description="Pipeline inner steps"),
    },
    dagrun_timeout=timedelta(hours=4),
    tags=["ssis-migration", "skeleton", "parallel"],
) as dag:

    # ── Global start / end ───────────────────────────────────────────────────────
    start = EmptyOperator(task_id="start")
    fanout = EmptyOperator(task_id="fanout")   # 像 SSIS 的「同時啟動」
    gather = EmptyOperator(task_id="gather")   # 等全部子管線完成再收斂
    end = EmptyOperator(task_id="end")

    start >> fanout

    # 2) 產生「相同結構」的多條管線（TaskGroup），預設全部並行
    for system in ALL_PIPELINES:
        with TaskGroup(group_id=f"etl__{system}") as tg:
            s = EmptyOperator(task_id="start")          # 該系統管線起點

            # 依相同步驟名，建立骨架節點（之後把這些換成真實 Operators 即可）
            prev = s
            for step in ["extract", "cleanse", "validate", "load"]:
                node = EmptyOperator(task_id=step)
                prev >> node
                prev = node

            e = EmptyOperator(task_id="end")            # 該系統管線終點
            prev >> e

        # fan-out 到每個系統的管線，完成後再匯流到 gather
        fanout >> tg >> gather

    gather >> end
