from unittest.mock import patch, MagicMock
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from gcp_migrations.airflow.dags.ods.Main_Container_Process.main import update_stage_tab_act_prepare

def test_sql_and_params_template_rendered(monkeypatch):
    # 模擬渲染後的 parameters（Jinja 渲染在運行時）
    ti = MagicMock()
    context = {
        "dag_run": MagicMock(conf={"srcNatName":"TW","srcSysName":"SAP","srcEkorg":"1000","tableName":"ORDERS"}),
        "params": {},
        "ti": ti,
        "task": update_stage_tab_act_prepare,
    }

    # spy MsSqlHook.run 被呼叫的 sql & parameters
    with patch.object(MsSqlHook, "run", return_value=None) as spy_run:
        # 直接呼叫 operator.execute(context)
        update_stage_tab_act_prepare.execute(context=context)

    # 斷言 SQL 內容有 ? 順序
    called_sql = spy_run.call_args.kwargs["sql"].strip()
    assert "UPDATE [dbo].[STAGE_TAB_ACT]" in called_sql
    # 斷言參數序與值
    called_params = spy_run.call_args.kwargs["parameters"]
    assert called_params == ("TW", "SAP", "1000", "ORDERS")