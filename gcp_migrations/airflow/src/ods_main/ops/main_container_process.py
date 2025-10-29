from airflow.decorators import task

@task(task_id="get_do_table_list")
def get_do_table_list(ti=None):
    return ti.xcom_pull(task_ids="decide_process_table", key="do_table_list") or []

@task(task_id="build_sql_param_list")
def build_sql_param_list(file_list: list, dag_conf: dict, params: dict):
    """
    產出與 file_list 長度一致的 parameters 清單，供 MsSqlOperator.expand() 使用。
    參數順序：0: srcNatName, 1: srcSysName, 2: srcEkorg, 3: tableName
    """
    def p(k): return (dag_conf or {}).get(k, params[k])
    one = (p("srcNatName"), p("srcSysName"), p("srcEkorg"), p("tableName"))
    return [one for _ in range(len(file_list or []))]

@task(task_id="build_trigger_conf_list")
def build_trigger_conf_list(file_list: list, dag_conf: dict, params: dict):
    """
    產出 TriggerDagRunOperator.expand(conf=...) 的清單（每個檔名一個 dict）。
    """
    def p(k): return (dag_conf or {}).get(k, params[k])
    out = []
    for f in (file_list or []):
        out.append({
            "file_name": f,
            "srcNatName": p("srcNatName"),
            "srcSysName": p("srcSysName"),
            "srcEkorg":   p("srcEkorg"),
            "tableName":  p("tableName"),
        })
    return out
