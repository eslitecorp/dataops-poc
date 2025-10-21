# 📦 SSIS 檔案處理狀態管理模組 (File Record Control Module)

本模組由三段 SQL 腳本構成，用於 **ETL 檔案載入任務的狀態追蹤、稽核與同步控制**。  
其主要功能是確保每一個檔案在處理過程中的狀態、執行批次、載入結果與對應中繼表狀態皆能一致、可追蹤、可稽核。

---

## 🧩 模組結構概覽

| 步驟 | 腳本名稱 | 功能說明 |
|------|-----------|-----------|
| 1️⃣ | `update_file_rec.sql` | 更新主控表 (`FILE_REC`) 的檔案狀態與執行資訊 |
| 2️⃣ | `insert_file_rec_his.sql` | 將檔案處理結果歸檔至歷史表 (`FILE_REC_HIS`) |
| 3️⃣ | `update_stage_tab_act.sql` | 根據檔案狀態，更新中繼表 (`STAGE_TAB_ACT`) 狀態為完成 |

---

## 🧠 核心用途與目標

> 本模組為企業級 ETL 系統中的「**控制層（Control Layer）**」或「**元資料層（Metadata Layer）**」，  
> 主要用於管理與稽核 **檔案型資料載入流程的處理狀態**。

### 功能目標：
- ✅ 追蹤每個檔案的處理狀態 (`FILE_STATUS`)
- 🧾 紀錄批次執行資訊 (`SOURCEID`, `EXECUTIONID`)
- 📚 建立歷史稽核資料 (`FILE_REC_HIS`)
- 🔄 同步中繼表狀態 (`STAGE_TAB_ACT`)
- 🧮 驗證資料品質（`ROW_COUNT`, `CHECK_SUM`）
- 🔍 提供資料血統與重跑依據

---

## ⚙️ 詳細說明

### STEP 1 — 更新主控表 `FILE_REC`

```sql
UPDATE dbo.FILE_REC
SET FILE_STATUS = ?,
    SOURCEID = ?,
    EXECUTIONID = ?
WHERE FILE_NAME = ?;
```
說明：
更新指定檔案的處理狀態、來源代碼與執行批次。
通常在每個檔案開始處理或完成時執行。

---

### STEP 2 — 歸檔歷史紀錄 FILE_REC_HIS

```sql
INSERT INTO dbo.FILE_REC_HIS (
    [FILE_NAME], [FILE_STATUS], [ROW_COUNT_REAL], [CHECK_SUM_REAL],
    [CHECK_FILE_NAME], [ROW_COUNT], [CHECK_SUM], [SYS_NAME],
    [ETL_FILE_LOADED_DATE], [SOURCEID], [EXECUTIONID], [FILE_OP],
    [HEADER_SKIP_ROWS], [CHECK_VALUE], [WORK_DATE], [EKORG],
    [TABLE_NAME], [FILE_TYPE], [MACHINE_NAME]
)
SELECT
    [FILE_NAME], [FILE_STATUS], [ROW_COUNT_REAL], [CHECK_SUM_REAL],
    [CHECK_FILE_NAME], [ROW_COUNT], [CHECK_SUM], [SYS_NAME],
    GETDATE(), ?, ?, ?, [HEADER_SKIP_ROWS], [CHECK_VALUE],
    [WORK_DATE], [EKORG], [TABLE_NAME], [FILE_TYPE], [MACHINE_NAME]
FROM dbo.FILE_REC
WHERE FILE_NAME = ?;
```
說明：
將 FILE_REC 當前檔案紀錄完整備份至 FILE_REC_HIS 歷史表，
並加入執行時間、批次、操作類型等資訊，以利後續稽核。

---

### STEP 3 — 更新中繼表狀態 STAGE_TAB_ACT

```sql
DECLARE @fileStatus INT;
DECLARE @sysName NVARCHAR(20);
DECLARE @ekorg NVARCHAR(50);
DECLARE @tableName NVARCHAR(128);

SELECT 
    @sysName = SYS_NAME,
    @ekorg = EKORG,
    @tableName = TABLE_NAME,
    @fileStatus = FILE_STATUS
FROM dbo.FILE_REC
WHERE FILE_NAME = ?;

IF @fileStatus > 10 AND @fileStatus < 80
BEGIN
    UPDATE [dbo].[STAGE_TAB_ACT]
    SET TAB_STATUS = 3
    WHERE SYS_NAME = @sysName
      AND EKORG = @ekorg
      AND TABLE_NAME = @tableName;
END;
```
說明：
當檔案狀態介於 10～80（代表「處理中」或「完成」）時，
同步更新中繼表狀態為 3（代表該表可供下游流程使用）。

---

+----------------+      +--------------------+      +--------------------+
|  FILE_REC      | ---> |  FILE_REC_HIS      | ---> |  STAGE_TAB_ACT      |
| (主控紀錄表)   |      | (歷史稽核表)       |      | (中繼狀態表)         |
+----------------+      +--------------------+      +--------------------+
      |                         |                          |
      | 更新狀態、批次          | 歸檔完整紀錄              | 同步表可用性
      |                         |                          |
      +-------------------------+--------------------------+
                             控制層（Control Layer）

---

