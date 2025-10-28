# SSIS 檔案壓縮流程說明與雲端重構建議

## 🧩 原始 SSIS 流程說明

此流程屬於 **SSIS 控制流程 (Control Flow)**，不是資料流 (Data Flow)。

```text
[Extract / Copy Files]
      ↓
[File System Task: Move File]
      ↓
[Script Task: zipFileAndRemove]
      ↓
[Archive / Send / Cleanup]
```

---

### Script Task (C#) 主要行為
```csharp
string srcFileFolder = Dts.Variables["$Package::srcFileFolder"].Value.ToString();
string fileName = Dts.Variables["$Package::fileName"].Value.ToString();
string postName = Dts.Variables["$Package::postName"].Value.ToString();
```
- srcFileFolder：前一個步驟搬檔的目標資料夾
- fileName：要壓縮的檔案名稱
- postName：zip 檔名後綴（如 _OK、_20251021）

---

zipFileAndRemove() 功能:
- 開啟 srcFileFolder 中的指定檔案
- 呼叫 BinaryReadToEnd() 將整個檔案讀成 byte[]
- 新建 zip 檔案，將檔案內容寫入 zip entry
- 刪除原始檔案
> ⚠️ BinaryReadToEnd() 只是將整個檔案載入記憶體再寫入 zip，不是監聽或抽取資料。

---

### 為什麼 SSIS 要先搬檔再壓縮？

原因：
- 工作區隔離：避免前一批與新批次混在一起
- 可重試與清理方便：壓縮完可整批刪除
- 對帳與審核：以資料夾劃分批次
> 這是典型的 ETL 批次封存設計，並非緩存器或資料流 Buffer。

---

### 問題與風險
```
| 問題        | 說明                                                |
| --------- | ------------------------------------------------- |
| 不必要的記憶體操作 | `BinaryReadToEnd()` 將整檔載入記憶體，對大檔不友善               |
| 潛在競態條件    | 若 File System Task 與 Script Task 同時操作同一資料夾，可能誤刪檔案 |
| 無需逐檔 ZIP  | 多數檔案 < 100KB，壓縮無實際效益                              |
| 路徑組合不安全   | 使用 `"\\"` 拼字串，應改用 `Path.Combine()`                |
```