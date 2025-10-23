USE [msdb]
GO

BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'[Uncategorized (Local)]' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'[Uncategorized (Local)]'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'DAILY_STAGE_MAIN_POSNEC', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=0, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'沒有可用的描述。', 
		@category_name=N'[Uncategorized (Local)]', 
		@owner_login_name=N'ESLITE\ODS01', @job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'pooling', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'SSIS', 
		@command=N'/ISSERVER "\"\SSISDB\EsliteStage\Stage\Main_TW.POSNEC.dtsx\"" /SERVER ODSSQLDB1 /Par "\"$ServerOption::LOGGING_LEVEL(Int16)\"";1 /Par "\"$ServerOption::SYNCHRONIZED(Boolean)\"";True /CALLERINFO SQLAGENT /REPORTING E', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'sync to ODSDB', 
		@step_id=2, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'
declare @workDate varchar(100);

SELECT @workDate = REPLACE([PARAM_VALUE],''-'','''') 
  FROM [StageETLDB].[dbo].[USER_PARAMS]
 where PARAM_NAME = ''WorkDate''

declare @tabs varchar(max)
declare @tabs_obj int
declare my_cursor cursor local static read_only forward_only
for 
	select t.name, t.object_id
		from [ODSDB].[sys].[schemas] s
		join [ODSDB].[sys].[tables] t on s.schema_id = t.schema_id
		where s.name = ''POSNEC'' and t.name not like ''%_2%''

open my_cursor
fetch next from my_cursor into @tabs, @tabs_obj
while @@fetch_status = 0
	begin 
		if @tabs != ''''
			begin
				declare @initResult int = 0;
	--			
				if((SELECT ISNULL(SUM(CASE WHEN [FILE_NUMS] = [FILE_NUMS_COMP] THEN 1 ELSE 0 END),0) AS FILE_COMP
					  FROM [StagingDB_TW].dbo.[V_FILE_INFO]
				     where SYS_NAME = ''POSNEC'' and TABLE_NAME = ''''+ @tabs +'''') > 0)
					 BEGIN
						BEGIN TRANSACTION

						declare @odsTableName varchar(128);
						declare @workDateTableName varchar(128);
						set @odsTableName = ''POSNEC.'' + @tabs
						set @workDateTableName = ''POSNEC.'' + @tabs + ''_'' + @workDate

						declare @cols varchar(max);
						select @cols = STRING_AGG(tmp.name,'','') from (select top 1000 name from ODSDB.sys.Columns where name not in (''ETL_DATE'',''ETL_WORK_DATE'') and object_id = @tabs_obj order by column_id) tmp
				
						exec [ODSDB].[dbo].[WORK_DATE_TABLE_INIT] @workDateTableName, @odsTableName, 0
				
						exec(''insert into ODSDB.''+@workDateTableName+''( ''+@cols+'',ETL_DATE,ETL_WORK_DATE) select '' + @cols + '' , GETDATE(), '''''' + @workDate + '''''' from StagingDB_TW.POSNEC.'' + @tabs + '' where ETL_DATE >= cast(cast(GETDATE() as date) as datetime);'')
				
						exec [ODSDB].[dbo].[DELETE_BY_PK] @workDateTableName,@odsTableName,@odsTableName,0

						exec(''insert into ODSDB.''+ @odsTableName + '' select * from ODSDB.''+@workDateTableName);

						set @initResult = 1;

						IF @initResult = 1
							--沒問題就確認交易完成
							COMMIT TRANSACTION
						ELSE
							--若是有問題就取消交易
							ROLLBACK TRANSACTION
					END
			end
		fetch next from my_cursor into @tabs, @tabs_obj
	end
close my_cursor
deallocate my_cursor', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'Daily 03:00', 
		@enabled=1, 
		@freq_type=4, 
		@freq_interval=1, 
		@freq_subday_type=1, 
		@freq_subday_interval=0, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=0, 
		@active_start_date=20211108, 
		@active_end_date=99991231, 
		@active_start_time=30000, 
		@active_end_time=235959, 
		@schedule_uid=N'23d6b30c-40e9-402b-a86b-f421fa37387d'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:
GO

