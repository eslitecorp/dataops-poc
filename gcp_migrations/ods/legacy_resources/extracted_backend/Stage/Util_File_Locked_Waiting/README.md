# 🧩 wait_for_file_unlocked.cs

## 📘 Overview
`wait_for_file_unlocked.cs` 是一個用於 **SQL Server Integration Services (SSIS)** 的 Script Task，  
用來 **偵測指定檔案是否仍被其他程序鎖定**，並在確認檔案可被安全讀取後，才允許後續流程繼續執行。  

這段程式常見於 **ETL 前置檢查階段**，例如確保來源系統完成檔案寫入後再開始匯入。

---

## ⚙️ Behavior Summary
| 項目 | 說明 |
|------|------|
| **輸入變數** | `$Package::fileConnectionString` → 要檢查的完整檔案路徑 |
| **主要功能** | 不斷嘗試以唯讀模式開啟檔案，直到檔案可成功開啟為止 |
| **輪詢間隔** | 每 2 秒重試一次 |
| **錯誤處理** | 若檔案不存在或遇到非 I/O 例外，立即標示為失敗 |
| **事件紀錄** | 透過 `Dts.Events.FireInformation / FireWarning / FireError` 回報狀態 |
| **成功條件** | 檔案存在且不被任何進程鎖定 |
| **失敗條件** | 檔案不存在、等待超時、或非預期錯誤 |
