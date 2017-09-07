# sql-queries
ALTER PROCEDURE [dbo].[Proc_UpdateKeywordScheduler] 
	
AS
BEGIN

	Declare @keyword nvarchar(50)
	Declare @MaxCount int,@Emplid int,@PostedByRoleIDSupervisor varchar(100),@PostedByRoleIDEmployee varchar(100)
	Declare @Flag int,@KeyWordid int,@MaxStarRating varchar(100),@MaxSmileyRating varchar(100),@ShareWithEmployeeID varchar(100)
	set @flag = 1
	set @MaxStarRating = 6
	set @MaxSmileyRating = 4
	set @PostedByRoleIDSupervisor = 3
	set @ShareWithEmployeeID = 1
	set @PostedByRoleIDEmployee = 4
	Declare @TEMPdata TABLE(Rn_No int,WordCloudKeyWord INT,keyword nvarchar(max),LastScan datetime,RecCreatedDate datetime)
	/*Insert Empllid for used keyword */
	Declare @TempuserData as Table(Rn_No int,Emplid int,KeyCount int)
	Declare @TempuserUnionQueryData as Table(ROW_NUM int,Rn_No int,Emplid int,KeyCount int)

	/*Insert Keyword data into @TEMPdata with row_number*/		
	INSERT INTO @TEMPdata
	select Row_Number() over (order by WordCloudKeyWord) as Rn_No,WordCloudKeyWord,keyword,LastScan,RecCreatedDate from  TX_WordCloudKeyWord --where WordCloudKeyWord=1
	
	select @MaxCount = Count(1) from @TEMPdata
	/*Looping the keywords to get the keyword details*/
	while (@flag <= @MaxCount)
	begin
		set @keyword = (select keyword from @TEMPdata where Rn_No = @flag)
		Set @KeyWordid= (select WordCloudKeyWord from @TEMPdata where Rn_No = @flag)
		
		Declare @sqlquery varchar(8000) 
		Declare @RecCreatedDate datetime
		Declare @LastScan varchar(100)
		Declare @StartScan datetime 
		set @StartScan = getdate()
		/*used to get updated data for keyword based on last scan */
		set @LastScan = (select LastScan from @TEMPdata where Rn_No = @flag);
		if(@LastScan is not null)
		begin
		/*Get Keyword Details by scanning for both supervisor and employee.
		For employee details will come based on share employee id , PostedByRoleID,StarRatingID,SmileyRatingID 
		For supervisor details will come based on share employee id , PostedByRoleID */
			Set @sqlquery= 'select distinct Row_Number() over (order by Emplid), Emplid, count(1) as KeyCount from TD_PerformanceFeedListing PFL 
			inner join TX_PerformanceFeedReviewComment PFRC on PFL.PerformanceFeedListingID = PFRC.PerformanceFeedListingID
			WHERE CONTAINS(comment , ''"' + @keyword + '"'') AND  PFL.FEEDTypeID in (1,4)
			AND PFRC.RecCreatedDate > CAST(''' + @LastScan + '''  AS DATETIME) 
			AND PFL.PostedByRoleID = CAST(''' + @PostedByRoleIDEmployee + ''' AS INT)
			AND PFL.ShareWithEmployee = CAST(''' + @ShareWithEmployeeID + ''' AS INT)
			AND (PFRC.StarRatingID >= ''' + @MaxStarRating + ''' or PFRC.SmileyRatingID >= ''' + @MaxSmileyRating + ''')
			group by PFL.Emplid
			UNION all
			select distinct Row_Number() over (order by Emplid),Emplid, count(1) as KeyCount from TD_PerformanceFeedListing PFL 
			WHERE CONTAINS(comment , ''"' + @Keyword + '"'')
			AND PFL.RecCreatedDate > CAST(''' + @LastScan + '''  AS DATETIME) 
			AND PFL.PostedByRoleID = CAST(''' + @PostedByRoleIDSupervisor + ''' AS INT)
			AND   PFL.FEEDTypeID in (1,4) 
			AND PFL.ShareWithEmployee = CAST(''' + @ShareWithEmployeeID + ''' AS INT)
			Group by Emplid'
		
			/*Insert Keyword into temp tbale to get Emplid who has used the looped keyword*/
			insert into @TempuserData(Rn_No,Emplid,KeyCount)
			exec (@sqlquery)

			/*Adding row_num for combine data got in temp table for both employee and supervisor*/
			insert into @TempuserUnionQueryData(ROW_NUM,Rn_No,Emplid,KeyCount)
			SELECT ROW_NUMBER() OVER(ORDER BY Rn_No) ROW_NUM,Rn_No,Emplid,KeyCount
			FROM(
			select * from @TempuserData)Temp

			declare  @maxUserCount int
			Declare @count int
			/*Total count of records*/
			select @maxUserCount = count(1) from @TempuserUnionQueryData

			/*Sum of keyword count for particular keyword*/
			select @count =  sum(KeyCount) from @TempuserUnionQueryData 

			/*Increment Count in table and last scan datetime*/
			update TX_WordCloudKeyWord set [Count] = [Count] + ISNULL(@count,0), LastScan = @StartScan where WordCloudKeyWord = @KeyWordid and RecDeletedBy is null
		
			Declare @UserFlag int
			Set @UserFlag=1
			/*loop Count to update the emplid*/
			while (@UserFlag <= @maxUserCount)
			begin
				
				Declare @keywordcountupdatetemp int
				declare @EmplidUpdate int
				Set @keywordcountupdatetemp =(Select KeyCount from @TempuserUnionQueryData where ROW_NUM = @UserFlag)
				select @EmplidUpdate = Emplid from @TempuserUnionQueryData where ROW_NUM = @UserFlag
				if not Exists (select 1 from [dbo].[TX_UserKeyWord] (nolock) where EmpIID = @EmplidUpdate and WordCloudKeyWord = @KeyWordid)
						begin
						print 'insert'
							insert into [dbo].[TX_UserKeyWord] (EmpIID,WordCloudKeyWord,[Count],RecCreatedBy,RecCreatedDate)
							values(@Emplid,@KeyWordid,ISNULL(@keywordcountupdatetemp,0),@Emplid,getdate())
						end
				else
						begin
							update  [dbo].[TX_UserKeyWord] set [Count] = [Count] + ISNULL(@keywordcountupdatetemp,0)
							, RecUpdatedDate = getdate() 
							where  EmpIID = @EmplidUpdate and WordCloudKeyWord = @KeyWordid 
                        end
				set @UserFlag = @UserFlag + 1
			end
		end
		if(@LastScan is null)
		begin
			/*Get Keyword Details by scanning for both supervisor and employee.
			For employee details will come based on share employee id , PostedByRoleID,StarRatingID,SmileyRatingID 
			For supervisor details will come based on share employee id , PostedByRoleID */
			Set @sqlquery= 'select distinct Row_Number() over (order by Emplid),Emplid, count(1) as KeyCount from TD_PerformanceFeedListing PFL 
				inner join TX_PerformanceFeedReviewComment PFRC on PFL.PerformanceFeedListingID = PFRC.PerformanceFeedListingID
				WHERE CONTAINS(comment , ''"' + @keyword + '"'') AND  PFL.FEEDTypeID in (1,4)  
				AND PFL.PostedByRoleID = CAST(''' + @PostedByRoleIDEmployee + ''' AS INT)  
				AND PFL.ShareWithEmployee = CAST(''' + @ShareWithEmployeeID + ''' AS INT)
				AND (PFRC.StarRatingID >= CAST(''' + @MaxStarRating + ''' AS INT) or PFRC.SmileyRatingID >= CAST(''' + @MaxSmileyRating + ''' AS INT))
				group by PFL.Emplid
				UNION all
				select distinct Row_Number() over (order by Emplid),Emplid, count(1) as KeyCount from TD_PerformanceFeedListing PFL 
				WHERE CONTAINS(comment , ''"' + @Keyword + '"'')
				AND PFL.PostedByRoleID = CAST(''' + @PostedByRoleIDSupervisor + ''' AS INT)
				AND   PFL.FEEDTypeID in (1,4) 
				AND PFL.ShareWithEmployee = CAST(''' + @ShareWithEmployeeID + ''' AS INT)
				Group by Emplid'

				/*Insert Keyword into temp tbale to get Emplid who has used the looped keyword*/
				insert into @TempuserData(Rn_No,Emplid,KeyCount)
				exec (@sqlquery)

				/*Adding row_num for combine data got in temp table for both employee and supervisor*/
				insert into @TempuserUnionQueryData(ROW_NUM,Rn_No,Emplid,KeyCount)
				SELECT ROW_NUMBER() OVER(ORDER BY Rn_No) ROW_NUM,Rn_No,Emplid,KeyCount
				FROM(
				select * from @TempuserData)Temp

				declare @maxUserUnionCount int
				declare @countuser int

				/*Total count of records*/
				select @maxUserUnionCount = count(1) from @TempuserUnionQueryData
			
				/*Sum of keyword count for particular keyword*/
				select @countuser =  sum(KeyCount) from @TempuserUnionQueryData
			

				/*Increment Count in table and last scan datetime*/
				update TX_WordCloudKeyWord set Count =  isnull(@countuser,0), LastScan = @StartScan where WordCloudKeyWord = @KeyWordid and RecDeletedBy is null

				Declare @UserFlagIU int
				Set @UserFlagIU =1
				/*loop Count to insert if emplid and keyword not there else update*/
				while (@UserFlagIU <= @maxUserUnionCount)
				begin
					Declare @keywordcounttemp int
					Select @keywordcounttemp =(Select KeyCount from @TempuserUnionQueryData where ROW_NUM = @UserFlagIU)
					select @Emplid = Emplid from @TempuserUnionQueryData where ROW_NUM = @UserFlagIU
					/*check if emplid and keyword are present then insert else update*/
					if not Exists (select 1 from [dbo].[TX_UserKeyWord] (nolock) where EmpIID = @Emplid and WordCloudKeyWord = @KeyWordid)
						begin
						print 'insert'
							insert into [dbo].[TX_UserKeyWord] (EmpIID,WordCloudKeyWord,[Count],RecCreatedBy,RecCreatedDate)
							values(@Emplid,@KeyWordid,ISNULL(@keywordcounttemp,0),@Emplid,getdate())
						end
					else
						begin
						print 'update'
							update  [dbo].[TX_UserKeyWord] set [Count] = [Count] + ISNULL(@keywordcounttemp,0)
							,RecUpdatedDate = getdate() 
							where  EmpIID = @Emplid and WordCloudKeyWord= @KeyWordid
						end
					set @UserFlagIU = @UserFlagIU + 1
				end
		end
			delete from @TempuserData
			delete from @TempuserUnionQueryData
			set @flag = @flag + 1
	end
END
