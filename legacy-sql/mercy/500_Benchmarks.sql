

IF OBJECT_ID('[mi_ATM881_ACOI].[dbo].[A_Enrollment_Bench_Strat]', 'U') IS NOT NULL
DROP TABLE [mi_ATM881_ACOI].[dbo].[A_Enrollment_Bench_Strat];

SELECT
	[MEMBER_MONTH_START_DATE]
	,[INCURRED_YEAR_AND_MONTH]
	,[ELIG_STATUS]
	,[ASSN_STATUS]
	,area, PCP_Name
	,cm_strat_idx
	,drg_strat_idx
	,count(*) as MemMos
INTO
	[mi_ATM881_ACOI].[dbo].[A_Enrollment_Bench_Strat]
FROM
	(
	SELECT
		[MEMBER_MONTH_START_DATE]
		,[INCURRED_YEAR_AND_MONTH]
		,[ELIG_STATUS]
		,[ASSN_STATUS]
		,area, PCP_Name
		,member_id ,HCC_COMMUNITY_RISK ,gender ,age
		,case
			when HCC_COMMUNITY_RISK is null then 0
			when HCC_COMMUNITY_RISK <0.50  then 1
			when HCC_COMMUNITY_RISK <0.60  then 2
			when HCC_COMMUNITY_RISK <0.70  then 3
			when HCC_COMMUNITY_RISK <0.80  then 4
			when HCC_COMMUNITY_RISK <0.90  then 5
			when HCC_COMMUNITY_RISK <1.00  then 6
			when HCC_COMMUNITY_RISK <1.10  then 7
			when HCC_COMMUNITY_RISK <1.20  then 8
			when HCC_COMMUNITY_RISK <1.35  then 9
			when HCC_COMMUNITY_RISK <1.50  then 10
			when HCC_COMMUNITY_RISK <1.75  then 11
			when HCC_COMMUNITY_RISK <2.00  then 12
			else 13
			end as cm_strat_idx
		,case
			when gender = 'M' and age <70  then 1
			when gender = 'M' and age <75  then 2
			when gender = 'M' and age <80  then 3
			when gender = 'M' and age <85  then 4
			when gender = 'M' and age <999  then 5
			when gender = 'F' and age <70  then 6
			when gender = 'F' and age <75  then 7
			when gender = 'F' and age <80  then 8
			when gender = 'F' and age <85  then 9
			when gender = 'F' and age <999  then 10
			else 0
			end as drg_strat_idx
	FROM
		[mi_ATM881_ACOI].[dbo].[A_Enrollment_Key]
	) as a
GROUP BY
	[MEMBER_MONTH_START_DATE]
	,[INCURRED_YEAR_AND_MONTH]
	,[ELIG_STATUS]
	,[ASSN_STATUS]
	,area, PCP_Name
	,cm_strat_idx
	,drg_strat_idx
	


IF OBJECT_ID('mi_ATM881_ACOI.dbo.A_tmp_benchCM1', 'U') IS NOT NULL
DROP TABLE mi_ATM881_ACOI.dbo.A_tmp_benchCM1;

SELECT
	[MEMBER_MONTH_START_DATE]
	,[INCURRED_YEAR_AND_MONTH]
	,[ELIG_STATUS]
	,[ASSN_STATUS]
	,area, PCP_Name
	,flg
	,sum(memmos) as memmos
	,Label1 as cm_cat
	,sum(LM_util) as LM_util
	,sum(WM_util) as WM_util
INTO
	mi_ATM881_ACOI.dbo.A_tmp_benchCM1
FROM
	(
	SELECT
		[MEMBER_MONTH_START_DATE]
		,[INCURRED_YEAR_AND_MONTH]
		,[ELIG_STATUS]
		,[ASSN_STATUS]
		,area, PCP_Name
		,cm_strat_idx
		,case when cm_strat_idx = 0 then 0 else 1 end as flg
		,memmos
		,Label1
		,memmos*[Util_per_bm] as LM_util
		,0 as WM_util
	FROM
		[mi_ATM881_ACOI].[dbo].[A_Enrollment_Bench_Strat] as a
		left join [ref_ACOI].[dbo].[ref_LM_CM_Bench] as b
			on a.cm_strat_idx = b.idx2

	UNION ALL

	SELECT
		[MEMBER_MONTH_START_DATE]
		,[INCURRED_YEAR_AND_MONTH]
		,[ELIG_STATUS]
		,[ASSN_STATUS]
		,area, PCP_Name
		,cm_strat_idx
		,case when cm_strat_idx = 0 then 0 else 1 end as flg
		,0 as memmos
		,Label1
		,0 as LM_util
		,memmos*[Util_per_bm] as WM_util
	FROM
		[mi_ATM881_ACOI].[dbo].[A_Enrollment_Bench_Strat] as a
		left join [ref_ACOI].[dbo].[ref_WM_CM_Bench] as b
			on a.cm_strat_idx = b.idx2
	) as a
GROUP BY
	[MEMBER_MONTH_START_DATE]
	,[INCURRED_YEAR_AND_MONTH]
	,[ELIG_STATUS]
	,[ASSN_STATUS]
	,area, PCP_Name
	,flg
	,Label1
GO


IF OBJECT_ID('mi_ATM881_ACOI.dbo.A_tmp_benchCM2', 'U') IS NOT NULL
DROP TABLE mi_ATM881_ACOI.dbo.A_tmp_benchCM2;

SELECT
	a.[MEMBER_MONTH_START_DATE]
	,a.[INCURRED_YEAR_AND_MONTH]
	,a.[ELIG_STATUS]
	,a.[ASSN_STATUS]
	,a.area, a.PCP_Name
	,a.memmos
	,b.cm_cat
	,b.LM_util/b.memmos * a.memmos as LM_util
	,b.WM_util/b.memmos * a.memmos as WM_util
INTO
	mi_ATM881_ACOI.dbo.A_tmp_benchCM2
FROM
	(select * from mi_ATM881_ACOI.dbo.A_tmp_benchCM1 where flg=0) as a
	left join (select * from mi_ATM881_ACOI.dbo.A_tmp_benchCM1 where flg=1) as b
		on a.[MEMBER_MONTH_START_DATE] = b.[MEMBER_MONTH_START_DATE]
		and a.[INCURRED_YEAR_AND_MONTH] = b.[INCURRED_YEAR_AND_MONTH]
		and a.[ELIG_STATUS] = b.[ELIG_STATUS]
		and a.[ASSN_STATUS] = b.[ASSN_STATUS]
		and a.area = b.area
		and a.PCP_Name = b.PCP_Name
GO


IF OBJECT_ID('mi_ATM881_ACOI.dbo.A_Bnchmrk_Cost_Model', 'U') IS NOT NULL
DROP TABLE mi_ATM881_ACOI.dbo.A_Bnchmrk_Cost_Model;

SELECT
	a.[MEMBER_MONTH_START_DATE]
	,a.[INCURRED_YEAR_AND_MONTH]
	,a.[ELIG_STATUS]
	,a.[ASSN_STATUS]
	,a.area, a.PCP_Name
	,sum(a.memmos) as memmos
	,cm_cat
	,sum(LM_util) as LM_util
	,sum(WM_util) as WM_util
INTO
	mi_ATM881_ACOI.dbo.A_Bnchmrk_Cost_Model
FROM
	(select [MEMBER_MONTH_START_DATE],[INCURRED_YEAR_AND_MONTH],[ELIG_STATUS],[ASSN_STATUS],area,PCP_Name,memmos,cm_cat,LM_util,WM_util from mi_ATM881_ACOI.dbo.A_tmp_benchCM2
	UNION ALL
	select [MEMBER_MONTH_START_DATE],[INCURRED_YEAR_AND_MONTH],[ELIG_STATUS],[ASSN_STATUS],area,PCP_Name,memmos,cm_cat,LM_util,WM_util from mi_ATM881_ACOI.dbo.A_tmp_benchCM1 where flg=1) as a
GROUP BY
	a.[MEMBER_MONTH_START_DATE]
	,a.[INCURRED_YEAR_AND_MONTH]
	,a.[ELIG_STATUS]
	,a.[ASSN_STATUS]
	,a.area, a.PCP_Name
	,cm_cat
GO


/*

(136336 row(s) affected)
Warning: Null value is eliminated by an aggregate or other SET operation.

(110370 row(s) affected)

(54924 row(s) affected)
Warning: Null value is eliminated by an aggregate or other SET operation.

(109374 row(s) affected)

5mins

*/



