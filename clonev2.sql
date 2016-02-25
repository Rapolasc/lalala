
	DECLARE @CustomerDatabaseId int = 2089
	DECLARE @CustomerDatabase nvarchar(100)
	DECLARE @CustomerDatabaseName varchar(300)
	DECLARE @NewCustomerDatabaseName varchar(300)
	DECLARE @ToDiskPathLog varchar(3000)
	DECLARE @ToDiskPath  varchar(3000)
	DECLARE @ToDisk varchar(100) 
	DECLARE @projecttype int 
	DECLARE @suffix nvarchar(10)
	DECLARE @clonecustomerdatabaseid int
	DECLARE @restorecustomerdatabase nvarchar(50)
	DECLARE @sql NVARCHAR(2000);
	DECLARE @sql2 NVARCHAR(2000);
	
	SET @projecttype = (SELECT ProjectTypeId FROM [DtecNet_DwGeneric].[Db].[CustomerDatabase] (NOLOCK) WHERE ID = @customerdatabaseid)
	SET @suffix = (SELECT [Suffix] FROM [DtecNet_DwGeneric].[Db].[ProjectType] (NOLOCK) WHERE ID = @projecttype)
	SET @customerdatabase = (SELECT left (NAME, CHARINDEX(@suffix,NAME)-1) FROM [DtecNet_DwGeneric].[Db].[CustomerDatabase]where ID = @customerdatabaseid)
	SET @customerdatabasename = @customerdatabase + @suffix
	SET @NewCustomerDatabaseName = @customerdatabase + '_Clone' + @suffix;
	SET @ToDiskPathLog = 'E:\SqlData\'+@NewCustomerDatabaseName+'_Log.ldf'
	SET @ToDiskPath = 'F:\SqlData\Customer.Databases\'+@NewCustomerDatabaseName+'\'+@NewCustomerDatabaseName
	SET @ToDisk = 'D:\test\' + @customerdatabasename +'.bak';
	
	
	
	DECLARE @DataPath nvarchar(500);
	DECLARE @DataAggPath nvarchar(500);
	DECLARE @DataPath2 nvarchar(500);
	DECLARE @DataAggPath2 nvarchar(500);
	DECLARE @DiffDataPath nvarchar(500);
	DECLARE @DiffDataAggPath nvarchar(500);
	DECLARE @LogPath nvarchar(500);
	DECLARE @DirTree TABLE (subdirectory nvarchar(255), depth INT);


	SET @ProjectType = (SELECT ProjectTypeId FROM [DtecNet_DwGeneric].[Db].[CustomerDatabase] (NOLOCK) WHERE ID = @customerdatabaseid)
	SET @DataPath = (select Value from DtecNet_DwSys.dbo.SysParameter where [Name]='CustomerDataPath') + @NewCustomerDatabaseName
	SET @DataAggPath = (select Value from DtecNet_DwSys.dbo.SysParameter where [Name]='CustomerDataAggPath') + @NewCustomerDatabaseName
	SET @DataPath2 = (select Value from DtecNet_DwSys.dbo.SysParameter where [Name]='CustomerDataPath2') + @NewCustomerDatabaseName
	SET @DataAggPath2 = (select Value from DtecNet_DwSys.dbo.SysParameter where [Name]='CustomerDataAggPath2') + @NewCustomerDatabaseName
	SET @LogPath = (select Value from DtecNet_DwSys.dbo.SysParameter where [Name]='CustomerLogPath')


	   
	-- @DataPath values
	INSERT INTO @DirTree(subdirectory, depth)
	EXEC master.sys.xp_dirtree @DataPath
	-- Create the @DataPath directory
	IF NOT EXISTS (SELECT 1 FROM @DirTree WHERE subdirectory = @NewCustomerDatabaseName)
	EXEC master.dbo.xp_create_subdir @DataPath

	-- Remove all records from @DirTree
	DELETE FROM @DirTree
	-- @DataAggPath values
	INSERT INTO @DirTree(subdirectory, depth)
	EXEC master.sys.xp_dirtree @DataAggPath
	-- Create the @DataAggPath directory
	IF NOT EXISTS (SELECT 1 FROM @DirTree WHERE subdirectory = @NewCustomerDatabaseName) and (@ProjectType in (4,5,6,7)) 
	EXEC master.dbo.xp_create_subdir @DataAggPath

	DELETE FROM @DirTree
	-- CustomerDataPath2 values
	INSERT INTO @DirTree(subdirectory, depth)
	EXEC master.sys.xp_dirtree @DataPath2
	-- Create the CustomerDataPath2 directory
	IF NOT EXISTS (SELECT 1 FROM @DirTree WHERE subdirectory = @NewCustomerDatabaseName) and (@ProjectType in (3,4,5,6,7)) 
	EXEC master.dbo.xp_create_subdir @DataPath2

	DELETE FROM @DirTree
	-- CustomerDataAggPath2 values
	INSERT INTO @DirTree(subdirectory, depth)
	EXEC master.sys.xp_dirtree @DataAggPath2
	-- Create the CustomerDataAggPath2 directory
	IF NOT EXISTS (SELECT 1 FROM @DirTree WHERE subdirectory = @NewCustomerDatabaseName) and (@ProjectType in (4,5,6,7)) 
	EXEC master.dbo.xp_create_subdir @DataAggPath2


	-- Remove all records from @DirTree
	DELETE FROM @DirTree
	-- @LogPath values
	INSERT INTO @DirTree(subdirectory, depth)
	EXEC master.sys.xp_dirtree @LogPath
	-- Create the @LogPath directory
	IF NOT EXISTS (SELECT 1 FROM @DirTree WHERE subdirectory = @NewCustomerDatabaseName)
	EXEC master.dbo.xp_create_subdir @LogPath



BACKUP DATABASE @CustomerDatabaseName
	TO DISK =@ToDisk; 

IF  OBJECT_ID('tempdb..#NewFiles ') IS NOT NULL DROP TABLE #NewFiles	

SELECT --@NewCustomerDatabaseName+substring(name, len(@customerdatabasename)+1, 300 ) NewFileName, 
name FileName,
         CASE WHEN Type_Desc='LOG' 
                        THEN @ToDiskPathLog
                  WHEN Data_Space_Id=1
                        THEN @ToDiskPath+substring(name, len(@customerdatabasename)+1, 300 )+'.mdf'
                  ELSE @ToDiskPath+substring(name, len(@customerdatabasename)+1, 300 )+'.ndf'
            END         NewPhysicalName,
            @NewCustomerDatabaseName+substring(name, len(@customerdatabasename)+1, 300 ) NewFileName
Into #NewFiles                
FROM sys.[master_files]
WHERE [database_id] IN (DB_ID(@CustomerDatabaseName))



DECLARE @MoveTxt varchar(max)
SELECT @MoveTxt=ISNULL(@MoveTxt,'')+'MOVE N'''+ FileName+''' TO N'''+ NewPhysicalName+''','+CHAR(13)+CHAR(10)
FROM #NewFiles

print @MoveTxt



   set @Sql = ' 
RESTORE DATABASE ['+@NewCustomerDatabaseName+']
FROM  DISK = N'''+@todisk+''' 
WITH  FILE = 1, 
'+@MoveTxt+
'NOUNLOAD,  REPLACE,  STATS = 10'

exec (@Sql)




DECLARE @RenameTxt varchar(max)
SELECT @RenameTxt=ISNULL(@RenameTxt,'')+ 'ALTER DATABASE'+' ' + @NewCustomerDatabaseName+' MODIFY FILE (NAME='+Filename+', NEWNAME='+NewFileName+');'+CHAR(13)+CHAR(10)
--+'GO'+CHAR(13)+CHAR(10)
FROM #NewFiles



set @Sql2 = ''+@RenameTxt+''

exec (@sql2)





INSERT INTO  [DtecNet_DwGeneric].[Db].[CustomerDatabase] ([ProjectTypeId] ,[ContentTypeId],[CustomerId],[InsiteProjectId],[Suffix],[Name],[ServerName],[HasStaticApproach],[UseBlackListOnly4Verification],[SizeEstimateMb],[DailyDataScope],[WeeklyDataScope],[MonthlyDataScope],[RsClosedPeriodId],[ModifiedAt],[DataFrom],[Enabled],[MaintenanceCompletedAt],[UseGidFrom],[VerificationModeId],[DataTill],[UseInitialStatus],[UseTitleDependancy],[LastUpdateBatchId],[ImportsiteClosedDate],[HideWebDuplicatedRowsFromDate],[HideClmsDuplicatedRowsFromDate],[Droped],[LastCompletedSyncStartedAt],[LastCompletedSyncFinishedAt],[AvgSyncDuration],[CompletedSyncsPerLast28days],[UseTitleValidTillFilter])
	SELECT [ProjectTypeId] ,[ContentTypeId],[CustomerId],[InsiteProjectId],[Suffix],@NewCustomerDatabaseName,[ServerName],[HasStaticApproach],[UseBlackListOnly4Verification],[SizeEstimateMb],[DailyDataScope],[WeeklyDataScope],[MonthlyDataScope],[RsClosedPeriodId],[ModifiedAt],[DataFrom],[Enabled],[MaintenanceCompletedAt],[UseGidFrom],[VerificationModeId],[DataTill],[UseInitialStatus],[UseTitleDependancy],[LastUpdateBatchId],[ImportsiteClosedDate],[HideWebDuplicatedRowsFromDate],[HideClmsDuplicatedRowsFromDate],[Droped],[LastCompletedSyncStartedAt],[LastCompletedSyncFinishedAt],[AvgSyncDuration],[CompletedSyncsPerLast28days],[UseTitleValidTillFilter]
	FROM [DtecNet_DwGeneric].[Db].[CustomerDatabase] (nolock)
	WHERE Id = @customerdatabaseid;

	
	SET @clonecustomerdatabaseid = (select ID from [DtecNet_DwGeneric].[Db].[CustomerDatabase](nolock) where Name = @NewCustomerDatabaseName)
	
	INSERT INTO [DtecNet_DwGeneric].[Fact].[UsedProject] ([CustomerDatabaseId],[ProjectId])
	SELECT @clonecustomerdatabaseid,[ProjectId]
	FROM [DtecNet_DwGeneric].[Fact].[UsedProject] (NOLOCK)
	WHERE CustomerDatabaseId= @customerdatabaseid
	
	INSERT INTO [DtecNet_DwGeneric].[Db].[DependentTitle] ([CustomerDatabaseId],[GidReferenceId],[TitleId],[ValidFromDate],[ValidTillDate],[DependentTitleStatusId],[CompanyId],[OwnerCustomerId])
	SELECT @clonecustomerdatabaseid,[GidReferenceId],[TitleId],[ValidFromDate],[ValidTillDate],[DependentTitleStatusId],[CompanyId],[OwnerCustomerId]
	FROM [DtecNet_DwGeneric].[Db].[DependentTitle] (NOLOCK)
	WHERE CustomerDatabaseId= @customerdatabaseid
	
	
	IF @projecttype = 3 
BEGIN	

BEGIN TRY
BEGIN TRANSACTION 

	---------------WEB part

	
	INSERT INTO [DtecNet_DwGeneric].[Db].[ContentItemFilter] ([CustomerDatabaseId],[ContentItemId],[ValidFromDate],[ValidTillDate],[Exclude])
	SELECT @clonecustomerdatabaseid,[ContentItemId],[ValidFromDate],[ValidTillDate],[Exclude]
	FROM [DtecNet_DwGeneric].[Db].[ContentItemFilter] (NOLOCK)
	WHERE CustomerDatabaseId= @customerdatabaseid


	INSERT INTO [DtecNet_DwGeneric].[Db].[EnforcedTitle] ([CustomerDatabaseId],[GidReferenceId])
	SELECT @clonecustomerdatabaseid,[GidReferenceId]
	FROM [DtecNet_DwGeneric].[Db].[EnforcedTitle] (NOLOCK)
	WHERE CustomerDatabaseId= @customerdatabaseid

	INSERT INTO [DtecNet_DwGeneric].[Db].[DomainEnforcement] ([CustomerDatabaseId],[DomainHashId])
	SELECT @clonecustomerdatabaseid,[DomainHashId]
	FROM [DtecNet_DwGeneric].[Db].[DomainEnforcement] (NOLOCK)
	WHERE CustomerDatabaseId= @customerdatabaseid


	INSERT INTO [DtecNet_DwGeneric].[Db].[EffortFilter] ([CustomerDatabaseId],[ScanEffortId],[ProjectId],[ValidFromDate],[ValidTillDate],[Exclude])
	SELECT @clonecustomerdatabaseid,[ScanEffortId],[ProjectId],[ValidFromDate],[ValidTillDate],[Exclude]
	FROM [DtecNet_DwGeneric].[Db].[EffortFilter] (NOLOCK)
	WHERE CustomerDatabaseId= @customerdatabaseid

	INSERT INTO [DtecNet_DwGeneric].[Db].[DomainFilter] ([CustomerDatabaseId],[DomainId],[ValidFromDate],[ValidTillDate],[Exclude],[IsHosting])
	SELECT @clonecustomerdatabaseid,[DomainId],[ValidFromDate],[ValidTillDate],[Exclude],[IsHosting]
	FROM [DtecNet_DwGeneric].[Db].[DomainFilter] (NOLOCK)
	WHERE CustomerDatabaseId= @customerdatabaseid


	INSERT INTO  [DtecNet_DwGeneric].[Db].[WebTechnologyFilter]	([CustomerDatabaseId],[WebTechnologyId],[ValidFromDate],[ValidTillDate],[Exclude])
	SELECT @clonecustomerdatabaseid,[WebTechnologyId],[ValidFromDate],[ValidTillDate],[Exclude]
	FROM [DtecNet_DwGeneric].[Db].[WebTechnologyFilter] (NOLOCK)
	WHERE CustomerDatabaseId= @customerdatabaseid

	INSERT INTO  [DtecNet_DwGeneric].[Db].[StatisticsSnapshot]([DayPeriodId],[CustomerDatabaseId],[NetworkId],[AllDaysCount],[Infringements],[InfringementsAll],[InfringementsPreviousDay],[InfringementsAvgPrevious7Days],[InfringementsAvgPrevious28Days],[VerifiedUniqueInfringements],[VerifiedUniqueInfringementsAll],[VerifiedUniqueInfringementsPreviousDay],[VerifiedUniqueInfringementsAvgPrevious7Days],[VerifiedUniqueInfringementsAvgPrevious28Days],[NotVerifiedInfringements],[NotVerifiedInfringementsAll],[NotVerifiedInfringementsTop100FileHashes],[NotVerifiedInfringementsAllTop100FileHashes],[DistinctTitles],[DistinctTitlesReported],[DistinctMatchTitles],[DistinctMatchTitlesReported],[DistinctFileHashes],[DistinctFileHashesReported],[VerificationDurationAvg],[VerificationDurationAvgAll],[VerificationDurationAvgAllVerifiedLast7Days],[VerificationDurationAvgAllVerifiedLast28Days],[InfringementsVerifiedLast7Days],[InfringementsAllVerifiedLast7Days],[InfringementsVerifiedLast28Days],[InfringementsAllVerifiedLast28Days],[DistinctFileHashesVerifiedLast7Days],[DistinctFileHashesAllVerifiedLast7Days],[DistinctFileHashesVerifiedLast28Days],[DistinctFileHashesAllVerifiedLast28Days])
	SELECT [DayPeriodId],@clonecustomerdatabaseid,[NetworkId],[AllDaysCount],[Infringements],[InfringementsAll],[InfringementsPreviousDay],[InfringementsAvgPrevious7Days],[InfringementsAvgPrevious28Days],[VerifiedUniqueInfringements],[VerifiedUniqueInfringementsAll],[VerifiedUniqueInfringementsPreviousDay],[VerifiedUniqueInfringementsAvgPrevious7Days],[VerifiedUniqueInfringementsAvgPrevious28Days],[NotVerifiedInfringements],[NotVerifiedInfringementsAll],[NotVerifiedInfringementsTop100FileHashes],[NotVerifiedInfringementsAllTop100FileHashes],[DistinctTitles],[DistinctTitlesReported],[DistinctMatchTitles],[DistinctMatchTitlesReported],[DistinctFileHashes],[DistinctFileHashesReported],[VerificationDurationAvg],[VerificationDurationAvgAll],[VerificationDurationAvgAllVerifiedLast7Days],[VerificationDurationAvgAllVerifiedLast28Days],[InfringementsVerifiedLast7Days],[InfringementsAllVerifiedLast7Days],[InfringementsVerifiedLast28Days],[InfringementsAllVerifiedLast28Days],[DistinctFileHashesVerifiedLast7Days],[DistinctFileHashesAllVerifiedLast7Days],[DistinctFileHashesVerifiedLast28Days],[DistinctFileHashesAllVerifiedLast28Days]
	FROM [DtecNet_DwGeneric].[Db].[StatisticsSnapshot] (NOLOCK)
	WHERE CustomerDatabaseId= @customerdatabaseid


	 
COMMIT TRANSACTION

END TRY
BEGIN CATCH
  ROLLBACK TRANSACTION
  print ERROR_MESSAGE()
END CATCH  
END

	IF @projecttype IN (4,5)
BEGIN	

INSERT INTO [DtecNet_DwGeneric].[Verification].[ShowStatus]
([CustomerDatabaseId],[FileHashId],[FirstShowAt],[CreatedAt],[PopularityLastRank],[PopularityTopRank],[PopularityTopRankAt],[ReportedTotalCount])
SELECT @clonecustomerdatabaseid,[FileHashId],[FirstShowAt],[CreatedAt],[PopularityLastRank],[PopularityTopRank],[PopularityTopRankAt],[ReportedTotalCount]
FROM[DtecNet_DwGeneric].[Verification].[ShowStatus] (NOLOCK)
WHERE CustomerDatabaseId= @customerdatabaseid


INSERT INTO [DtecNet_DwGeneric].[Notice].[SendingUser]([CustomerDatabaseId],[UserId])
SELECT @clonecustomerdatabaseid,[UserId]
FROM [DtecNet_DwGeneric].[Notice].[SendingUser] (NOLOCK)
WHERE CustomerDatabaseId= @customerdatabaseid

INSERT INTO [DtecNet_DwGeneric].[Db].[SourceTable] ([CustomerDatabaseId],[ServerName],[TableName],[WhereSql],[DiffNote])
SELECT @clonecustomerdatabaseid,[ServerName],[TableName],[WhereSql],[DiffNote]
FROM [DtecNet_DwGeneric].[Db].[SourceTable] (NOLOCK)
where CustomerDatabaseId= @customerdatabaseid

INSERT INTO [DtecNet_DwGeneric].[Db].[ReloadRequest] ([CustomerDatabaseId],[DataRangeFrom], [DataRangeTill])
SELECT @clonecustomerdatabaseid,[DataRangeFrom], [DataRangeTill]
FROM [DtecNet_DwGeneric].[Db].[ReloadRequest] (NOLOCK)
WHERE CustomerDatabaseId= @customerdatabaseid

INSERT INTO [DtecNet_DwGeneric].[Db].[MigrationMapping] ([CustomerDatabaseId], [SourceCustomerDatabaseId])
SELECT @clonecustomerdatabaseid,[SourceCustomerDatabaseId]
FROM [DtecNet_DwGeneric].[Db].[MigrationMapping] (NOLOCK)
WHERE CustomerDatabaseId= @customerdatabaseid

INSERT INTO [DtecNet_DwGeneric].[Db].[IspPopularity] ([CustomerDatabaseId],[IspId],[CountryId],[PopularityLastRank],[PopularityTopRank],[PopularityTopRankAt],[ReportedTotalCount],[InfringementsLastDay],[InfringementsPreviousDay],[InfringementsAvgPrevious7Days],[InfringementsAvgPrevious28Days])
SELECT @clonecustomerdatabaseid,[IspId],[CountryId],[PopularityLastRank],[PopularityTopRank],[PopularityTopRankAt],[ReportedTotalCount],[InfringementsLastDay],[InfringementsPreviousDay],[InfringementsAvgPrevious7Days],[InfringementsAvgPrevious28Days]
FROM [DtecNet_DwGeneric].[Db].[IspPopularity] (NOLOCK)
WHERE CustomerDatabaseId= @customerdatabaseid


INSERT INTO [DtecNet_DwGeneric].[Fact].[UsedHashTitle] ([CustomerDatabaseId],[HashTitleId])
SELECT @clonecustomerdatabaseid,[HashTitleId]
FROM [DtecNet_DwGeneric].[Fact].[UsedHashTitle] (NOLOCK)
WHERE CustomerDatabaseId= @customerdatabaseid

INSERT INTO [DtecNet_DwGeneric].[Fact].[UsedImportLog]	([CustomerDatabaseId],[ImportLogId],[RecordCount])
SELECT @clonecustomerdatabaseid,[ImportLogId],[RecordCount]
FROM [DtecNet_DwGeneric].[Fact].[UsedImportLog] (NOLOCK)
WHERE CustomerDatabaseId= @customerdatabaseid



INSERT INTO  [DtecNet_DwGeneric].[Fact].[UsedTitle] ([CustomerDatabaseId],[TitleId],[FirstSeenAt])
SELECT @clonecustomerdatabaseid,[TitleId],[FirstSeenAt]
FROM [DtecNet_DwGeneric].[Fact].[UsedTitle] (NOLOCK)
WHERE CustomerDatabaseId= @customerdatabaseid


INSERT INTO [DtecNet_DwGeneric].[Fact].[UsedGidReference] ([CustomerDatabaseId],[GidReferenceId],[FirstSeenAt])
SELECT @clonecustomerdatabaseid,[GidReferenceId],[FirstSeenAt]
FROM [DtecNet_DwGeneric].[Fact].[UsedGidReference] (NOLOCK)
WHERE CustomerDatabaseId= @customerdatabaseid

END

	 EXEC DtecNet_DwGeneric.dbo.p_CreateNewCustomerJob @CustomerDbId = @clonecustomerdatabaseid
	 EXEC DtecNet_DwGeneric.dbo.p_CreateNewCustomerDbCube @CustomerDbId = @clonecustomerdatabaseid