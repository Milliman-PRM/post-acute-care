

IF OBJECT_ID('mi_ATM881_ACOI.demo.A_Enrollment_Key', 'U') IS NOT NULL
DROP TABLE mi_ATM881_ACOI.demo.A_Enrollment_Key;

SELECT
	a.*
INTO
	mi_ATM881_ACOI.demo.A_Enrollment_Key
FROM
	(
	SELECT
		a.[INCURRED_YEAR_AND_MONTH]
		,a.[MEMBER_MONTH_START_DATE]
		,a.[MEMBER_KEY]
		,a.[MEMBER_ID]
		,[BENCH_SLICE_KEY]
		,x.Medicare_Status as [ELIG_STATUS]
		,case
			when x.hist_assgn = 1 then 'Assigned'
			else 'Assignable' end as [ASSN_STATUS]
		,case when x.subpop1 is null or x.subpop1 = 'Not Mapped' then 'Not Mapped' else x.subpop1 end as area
		,'N/A' as PCP_Name
		,[CCHG_CAT_CODE_AND_DESC]
		,AGE
		,GENDER
		,1 as MM
		,case when b.[HCC_COMMUNITY_RISK] is not null then 1 else 0 end as MM_risk
		,case when b.HCC_NE = 1 then 1 else 0 end as MM_ne
		,b.[HCC_COMMUNITY_RISK]
	FROM
		mi_ATM881_ACOI.demo.dat_MEMBMTHS as a
		left join mi_ATM881_ACOI.demo.dat_membermonth_udd as x
			on a.[MEMBER_ID] = x.[MEMBER_ID]
			and a.[INCURRED_YEAR_AND_MONTH] = x.[INCURRED_YEAR_AND_MONTH]
		left join mi_ATM881_ACOI.dbo.A_tmpHCC1 as b
			on a.[MEMBER_KEY] = b.[MEMBER_KEY]
			and (a.[INCURRED_YEAR]-1) = b.[INCURRED_YEAR]

	UNION ALL

	SELECT
		a.[INCURRED_YEAR_AND_MONTH]
		,a.[MEMBER_MONTH_START_DATE]
		,a.[MEMBER_KEY]
		,a.[MEMBER_ID]
		,[BENCH_SLICE_KEY]
		,x.Medicare_Status as [ELIG_STATUS]
		,'Assigned_Current' as [ASSN_STATUS]
		,case when x.subpop1 is null or x.subpop1 = 'Not Mapped' then 'Not Mapped' else x.subpop1 end as area
		,'N/A' as PCP_Name
		,[CCHG_CAT_CODE_AND_DESC]
		,AGE
		,GENDER
		,1 as MM
		,case when b.[HCC_COMMUNITY_RISK] is not null then 1 else 0 end as MM_risk
		,case when b.HCC_NE = 1 then 1 else 0 end as MM_ne
		,b.[HCC_COMMUNITY_RISK]
	FROM
		mi_ATM881_ACOI.demo.dat_MEMBMTHS as a
		left join mi_ATM881_ACOI.demo.dat_membermonth_udd as x
			on a.[MEMBER_ID] = x.[MEMBER_ID]
			and a.[INCURRED_YEAR_AND_MONTH] = x.[INCURRED_YEAR_AND_MONTH]
		left join mi_ATM881_ACOI.dbo.A_tmpHCC1 as b
			on a.[MEMBER_KEY] = b.[MEMBER_KEY]
			and (a.[INCURRED_YEAR]-1) = b.[INCURRED_YEAR]
	WHERE
		x.curr_assgn = 1 
/*
	UNION ALL

	SELECT
		a.[INCURRED_YEAR_AND_MONTH]
		,a.[MEMBER_MONTH_START_DATE]
		,a.[MEMBER_KEY]
		,a.[MEMBER_ID]
		,[BENCH_SLICE_KEY]
		,x.Medicare_Status as [ELIG_STATUS]
		,'Assigned_Current_Current' as [ASSN_STATUS]
		,case when x.subpop1 is null or x.subpop1 = 'Not MappedL' then 'Not Mapped' else x.subpop1 end as area
		,'N/A' as PCP_Name
		,[CCHG_CAT_CODE_AND_DESC]
		,AGE
		,GENDER
		,1 as MM
		,case when b.[HCC_COMMUNITY_RISK] is not null then 1 else 0 end as MM_risk
		,case when b.HCC_NE = 1 then 1 else 0 end as MM_ne
		,b.[HCC_COMMUNITY_RISK]
	FROM
		mi_ATM881_ACOI.demo.dat_MEMBMTHS as a
		left join mi_ATM881_ACOI.demo.dat_membermonth_udd as x
			on a.[MEMBER_ID] = x.[MEMBER_ID]
			and a.[INCURRED_YEAR_AND_MONTH] = x.[INCURRED_YEAR_AND_MONTH]
		left join mi_ATM881_ACOI.dbo.A_tmpHCC1 as b
			on a.[MEMBER_KEY] = b.[MEMBER_KEY]
			and (a.[INCURRED_YEAR]-1) = b.[INCURRED_YEAR]
	WHERE
		x.curr_assgn = 1 
		and year([MEMBER_MONTH_START_DATE]) = (select max(year([MEMBER_MONTH_START_DATE])) from mi_ATM881_ACOI.demo.dat_MEMBMTHS)*/
	) as a
GO



IF OBJECT_ID('mi_ATM881_ACOI.demo.A_Enrollment_Summary1', 'U') IS NOT NULL
DROP TABLE mi_ATM881_ACOI.demo.A_Enrollment_Summary1;

SELECT
	[INCURRED_YEAR_AND_MONTH]
	,[MEMBER_MONTH_START_DATE]
	,[BENCH_SLICE_KEY]
	,[ELIG_STATUS]
	,[ASSN_STATUS]
	,area, PCP_Name
	,sum([MM]) as [MM]
	,sum([MM_risk]) as [MM_risk]
	,sum([MM_ne]) as [MM_ne]
	,sum([HCC_COMMUNITY_RISK]) as [HCC_COMMUNITY_RISK]
	,sum([AGE]) as [AGE]
INTO
	mi_ATM881_ACOI.demo.A_Enrollment_Summary1
FROM
	mi_ATM881_ACOI.demo.A_Enrollment_Key
GROUP BY
	[INCURRED_YEAR_AND_MONTH]
	,[MEMBER_MONTH_START_DATE]
	,[BENCH_SLICE_KEY]
	,[ELIG_STATUS]
	,[ASSN_STATUS]
	,area, PCP_Name
GO

IF OBJECT_ID('mi_ATM881_ACOI.demo.A_Enrollment_Summary2', 'U') IS NOT NULL
DROP TABLE mi_ATM881_ACOI.demo.A_Enrollment_Summary2;

SELECT
	[INCURRED_YEAR_AND_MONTH]
	,[MEMBER_MONTH_START_DATE]
	,[ELIG_STATUS]
	,[ASSN_STATUS]
	,area, PCP_Name
	,sum([MM]) as [MM]
	,sum([MM_risk]) as [MM_risk]
	,sum([MM_ne]) as [MM_ne]
	,sum([HCC_COMMUNITY_RISK]) as [HCC_COMMUNITY_RISK]
	,sum([AGE]) as [AGE]
INTO
	mi_ATM881_ACOI.demo.A_Enrollment_Summary2
FROM
	mi_ATM881_ACOI.demo.A_Enrollment_Summary1
GROUP BY
	[INCURRED_YEAR_AND_MONTH]
	,[MEMBER_MONTH_START_DATE]
	,[ELIG_STATUS]
	,[ASSN_STATUS]
	,area, PCP_Name
GO



IF OBJECT_ID('mi_ATM881_ACOI.demo.A_Enrollment_Summary2b', 'U') IS NOT NULL
DROP TABLE mi_ATM881_ACOI.demo.A_Enrollment_Summary2b;

SELECT
	b.[INCURRED_YEAR_AND_MONTH]
	,b.[MEMBER_MONTH_START_DATE]
	,b.[ELIG_STATUS]
	,b.[ASSN_STATUS]
	,b.area, b.PCP_Name
	,coalesce([MM],0) as [MM]
	,coalesce([MM_risk],0) as [MM_risk]
	,coalesce([MM_ne],0) as [MM_ne]
	,coalesce([HCC_COMMUNITY_RISK],0) as [HCC_COMMUNITY_RISK]
	,coalesce([AGE],0) as [AGE]
INTO
	mi_ATM881_ACOI.demo.A_Enrollment_Summary2b
FROM
	(SELECT
		[INCURRED_YEAR_AND_MONTH]
		,[MEMBER_MONTH_START_DATE]
		,[ELIG_STATUS]
		,[ASSN_STATUS]
		,area, PCP_Name
		,sum([MM]) as [MM]
		,sum([MM_risk]) as [MM_risk]
		,sum([MM_ne]) as [MM_ne]
		,sum([HCC_COMMUNITY_RISK]) as [HCC_COMMUNITY_RISK]
		,sum([AGE]) as [AGE]
	FROM
		mi_ATM881_ACOI.demo.A_Enrollment_Key
	GROUP BY
		[INCURRED_YEAR_AND_MONTH]
		,[MEMBER_MONTH_START_DATE]
		,[ELIG_STATUS]
		,[ASSN_STATUS]
		,area, PCP_Name) as a
	right join
	(SELECT
		[INCURRED_YEAR_AND_MONTH]
		,[MEMBER_MONTH_START_DATE]
		,[ELIG_STATUS]
		,[ASSN_STATUS]
		,area, PCP_Name
	FROM
		((SELECT DISTINCT [INCURRED_YEAR_AND_MONTH],[MEMBER_MONTH_START_DATE]
		FROM mi_ATM881_ACOI.demo.A_Enrollment_Key) as x
		cross join
		(SELECT DISTINCT [ELIG_STATUS],[ASSN_STATUS], area, PCP_Name
		FROM mi_ATM881_ACOI.demo.A_Enrollment_Key) as y)
	) as b
		on a.[MEMBER_MONTH_START_DATE] = b.[MEMBER_MONTH_START_DATE]
		and a.[ELIG_STATUS] = b.[ELIG_STATUS]
		and a.[ASSN_STATUS] = b.[ASSN_STATUS]
		and a.area = b.area
		and a.PCP_Name = b.PCP_Name

GO


IF OBJECT_ID('mi_ATM881_ACOI.demo.A_Enrollment_Summary3', 'U') IS NOT NULL
DROP TABLE mi_ATM881_ACOI.demo.A_Enrollment_Summary3;

SELECT
	[INCURRED_YEAR_AND_MONTH]
	,[MEMBER_MONTH_START_DATE]
	,[BENCH_SLICE_KEY]
	,sum([MM]) as [MM]
	,sum([MM_risk]) as [MM_risk]
	,sum([MM_ne]) as [MM_ne]
	,sum([HCC_COMMUNITY_RISK]) as [HCC_COMMUNITY_RISK]
	,sum([AGE]) as [AGE]
INTO
	mi_ATM881_ACOI.demo.A_Enrollment_Summary3
FROM
	mi_ATM881_ACOI.demo.A_Enrollment_Summary1
GROUP BY
	[INCURRED_YEAR_AND_MONTH]
	,[MEMBER_MONTH_START_DATE]
	,[BENCH_SLICE_KEY]
GO


/*

(2376745 row(s) affected)
Warning: Null value is eliminated by an aggregate or other SET operation.

(2109 row(s) affected)
Warning: Null value is eliminated by an aggregate or other SET operation.

(2109 row(s) affected)
Warning: Null value is eliminated by an aggregate or other SET operation.

(3468 row(s) affected)
Warning: Null value is eliminated by an aggregate or other SET operation.

(51 row(s) affected)
1min

*/