from __future__ import annotations

from datetime import datetime
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule

DAG_ID = "ssrs_like_branch_trigger"

# 你要觸發的三個 DAG（請換成實際 DAG ID）
UPSTREAM_DAG = "stage_etl_or_some_job"          # 第一個要被觸發、用來決定成敗的 DAG
SUCCESS_HANDLER_DAG = "on_success_followup"     # 成功後要再觸發的 DAG
FAILURE_HANDLER_DAG = "on_failure_compensation" # 失敗後要再觸發的 DAG

with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args={
        "owner": "dataops",
        "retries": 0,  # 重要：避免第一次失敗時還在重試，導致「失敗分支」提早/重複判定
    },
    description="Trigger → (success -> trigger A) | (failure -> trigger B)",
    max_active_runs=1,
    tags=["ssis-refactor", "trigger-only"],
) as dag:

    # 1) 先觸發「決定成敗」的子 DAG
    fire_upstream = TriggerDagRunOperator(
        task_id="fire_upstream",
        trigger_dag_id=UPSTREAM_DAG,
        # 將本 DAG 的 conf 透傳給子 DAG（可選）
        conf="{{ dag_run.conf or {} }}",
        reset_dag_run=True,          # 若該 execution_date 已存在則重置（依需求調整）
        wait_for_completion=True,    # 等子 DAG 執行結束，才能知道成功/失敗
        poke_interval=10,            # 等待輪詢秒數（依需求調整）
        allowed_states=["success", "failed"],
        failed_states=["failed"],
    )

    # 2) 若「1」成功 → 觸發成功後續
    fire_on_success = TriggerDagRunOperator(
        task_id="fire_on_success",
        trigger_dag_id=SUCCESS_HANDLER_DAG,
        conf="{{ dag_run.conf or {} }}",
        trigger_rule=TriggerRule.ALL_SUCCESS,  # 僅在 upstream 成功時執行
        reset_dag_run=True,
        wait_for_completion=False,             # 後續不必等待（依需求調整）
    )

    # 3) 若「1」失敗 → 觸發失敗補償/回滾
    fire_on_failure = TriggerDagRunOperator(
        task_id="fire_on_failure",
        trigger_dag_id=FAILURE_HANDLER_DAG,
        conf="{{ dag_run.conf or {} }}",
        trigger_rule=TriggerRule.ONE_FAILED,   # 只要 upstream 有失敗就執行
        reset_dag_run=True,
        wait_for_completion=False,
    )

    fire_upstream >> [fire_on_success, fire_on_failure]
