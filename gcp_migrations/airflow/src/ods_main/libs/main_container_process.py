from airflow.models import Variable
import re, json, logging
from datetime import datetime


def decide_and_prepare(**context) -> bool:
    """
    讀取 table_name、concurrent_files、pattern_table_map，決定是否要處理該 table。
    - 回傳 True 表示要繼續（ShortCircuit 會讓下游執行），False 則跳過下游。
    - 會在 XCom 推送：
        do_table: int(1/0)
        do_table_list: List[str]（符合該 table 的檔名清單）
        file_exist_map: Dict[str, bool]（更新後）
    """
    ti = context["ti"]

    # 參數來源可依你專案改成 dagrun.conf / Variable / params
    table_name: str = context["params"].get("table_name") \
        or context["dag_run"].conf.get("table_name")

    # 例：Variable 用 JSON 形式存；若你有別的來源就換掉這段
    pattern_table_map = json.loads(
        Variable.get("pattern_table_map_json", default_var="{}")
    )  # {"^ORDERS_.*\\.csv$": "ORDERS", ...}

    file_exist_map = json.loads(
        Variable.get("file_exist_map_json", default_var="{}")
    )      # {"ORDERS": false, ...}

    # 支援字串或 list；和 SSIS 的「逗號分隔」對齊
    concurrent = context["params"].get("concurrent_files") \
        or context["dag_run"].conf.get("concurrent_files", "")
    if isinstance(concurrent, str):
        concurrent_files = [x.strip() for x in concurrent.split(",") if x.strip()]
    else:
        concurrent_files = list(concurrent or [])

    def regex_match(pattern_map: dict[str, str], file_name: str, zip_file: bool) -> str:
        """
        對齊原 C#：若為壓縮檔，先移除 () 中段，再逐一用 regex 比對 pattern。
        命中則回傳該 pattern（字串），否則回傳空字串。
        """
        try:
            name = file_name
            if zip_file:
                # 移除第一段 (...) 內容；和原 C# 的 Substring 行為相近
                name = re.sub(r"\([^)]*\)", "", name)
            for pattern, _table in pattern_map.items():
                if re.match(pattern, name):
                    return pattern
            return ""
        except Exception:
            return ""

    mapping_table_files: list[str] = []
    process_file = False
    upper_table = (table_name or "").upper()

    for f in concurrent_files:
        if not f:
            continue
        is_zip = any(ext in f.upper() for ext in (".ZIP", ".RAR", ".7Z"))
        matched_pattern = regex_match(pattern_table_map, f, is_zip)

        if matched_pattern:
            file_table = pattern_table_map[matched_pattern]
            if upper_table == file_table.upper():
                process_file = True
                file_exist_map[upper_table] = True
                mapping_table_files.append(f)

    # 對齊 SSIS：User::do_table（1/0），User::doTableList（list）
    ti.xcom_push(key="do_table", value=1 if process_file else 0)
    ti.xcom_push(key="do_table_list", value=mapping_table_files)
    ti.xcom_push(key="file_exist_map", value=file_exist_map)

    logging.info(
        "table=%s, process_file=%s, files=%s",
        table_name, process_file, mapping_table_files
    )
    # ShortCircuit 依返回值決定是否放行下游
    return process_file