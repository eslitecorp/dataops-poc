

### SSIS視覺化流程方塊 與 DAG 的對照表
```markdown
|  順序 | Airflow `dag_id`                      | 對應程式碼檔案                              | SSIS 對應 UI 區塊描述                  |
| :-: | :------------------------------------ | :----------------------------------- | :------------------------------- |
|  1  | `MainEnterPoint__load_project_params` | `block_1_run_load_project_params.py` | 系統前置作業－從 `PROJECT_PARAMS` 載入系統參數 |
|  2  | `MainEnterPoint__load_user_params` | `block_2_run_load_user_params.py` | 系統前置作業－從 `USER_PARAMS` 載入系統參數 |
|  3  | `MainEnterPoint__load_and_reorder` | `block_3_run_load_and_reorder.py` | 系統前置作業－從資料表 `STAGE_TAB_ACT` 取得來源系統資料表的載入順序 |
|  4  | `MainEnterPoint__main_etl_block` | `block_4_run_main_etl_block.py` | 主ETL作業區塊 |
|  5  | `MainEnterPoint__insert_into_file_rec` | `block_5_run_insert_file_rec.py` | 時序容器+寄發通知信 |
```