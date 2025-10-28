# parse_source_file_path_list.cs

## 📘 概要
這個 **SSIS Script Task** 的用途是：  
從套件參數 `$Package::sourceFilePath`（內容為以分號 `;` 分隔的多個檔案路徑字串）中，解析出每個實際路徑，並轉換為 `List<string>` 物件，存入 SSIS 變數 `User::srcFilePathList`，以供後續流程（例如 Foreach Loop Container）逐一處理檔案。

此程式的核心邏輯即為：  
> **「分號分隔字串 → 可迭代的檔案清單」**

---

## ⚙️ 功能說明
### ✳️ 功能摘要
- 讀取 `$Package::sourceFilePath`
- 使用 `;` 分隔字串
- 轉成 `List<string>` 並指派給 `User::srcFilePathList`
- 將任務結果標記為成功 (`Dts.TaskResult = Success`)