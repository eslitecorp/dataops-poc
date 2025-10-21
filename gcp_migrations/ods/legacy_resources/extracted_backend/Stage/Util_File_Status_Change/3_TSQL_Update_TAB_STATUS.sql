DECLARE @fileStatus INT;
DECLARE @sysName NVARCHAR(20);
DECLARE @ekorg NVARCHAR(50);
DECLARE @tableName NVARCHAR(128);

-- 取得當前處理檔案對應參數
SELECT 
    @sysName = SYS_NAME,
    @ekorg = EKORG,
    @tableName = TABLE_NAME,
    @fileStatus = FILE_STATUS
FROM dbo.FILE_REC
WHERE FILE_NAME = ?;

-- 若檔案狀態介於 10 到 80 之間，則更新階段表狀態
IF @fileStatus > 10 AND @fileStatus < 80
BEGIN
    UPDATE [dbo].[STAGE_TAB_ACT]
    SET TAB_STATUS = 3
    WHERE SYS_NAME = @sysName
      AND EKORG = @ekorg
      AND TABLE_NAME = @tableName;
END;
