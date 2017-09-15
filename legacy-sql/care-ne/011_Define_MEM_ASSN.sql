

IF OBJECT_ID('MI_CareNE_ACOI.dbo.tmpref_MEM_ASSN', 'U') IS NOT NULL
DROP TABLE MI_CareNE_ACOI.dbo.tmpref_MEM_ASSN;

SELECT DISTINCT
	b.member_id
	,'01-01-2017' as start_dt
	,'12-31-2017' as end_dt
INTO
	MI_CareNE_ACOI.dbo.tmpref_MEM_ASSN
FROM
	MI_CareNE_ACOI.dbo.dat_MEMBMTHS as b
WHERE
	[_MI_USER_DIM_08_] in ('NEXTGEN_ASSIGNED')



IF OBJECT_ID('MI_CareNE_ACOI.dbo.dat_membermonth_udd', 'U') IS NOT NULL
DROP TABLE MI_CareNE_ACOI.dbo.dat_membermonth_udd;

SELECT
	b.member_id
	, b.[INCURRED_YEAR_AND_MONTH]
	,case
		when b.[MEDICARE_BASIS] in ('11','21','31') then 'ESRD'
		when b.[MEDICARE_BASIS] in ('20') then 'Disabled'
		when b.[_ENR_UDF_01_] in ('01','02','04','08') then 'Aged_Dual'
		else 'Aged_Non_Dual' end
		as Medicare_Status
	,case when c.member_id is null then 0 else 1 end
		as hist_assgn
	,case when d.member_id is null then 0 else 1 end
		as curr_assgn
	,'All' as subpop1
INTO
	MI_CareNE_ACOI.dbo.dat_membermonth_udd
FROM
	MI_CareNE_ACOI.dbo.dat_MEMBMTHS as b
	left join MI_CareNE_ACOI.dbo.tmpref_MEM_ASSN as c
		on b.MEMBER_ID = c.member_id
		and b.[MEMBER_MONTH_START_DATE] >= convert(datetime,c.[start_dt],110)
		and b.[MEMBER_MONTH_START_DATE] <= convert(datetime,c.[end_dt],110)
	left join MI_CareNE_ACOI.dbo.tmpref_MEM_ASSN as d
		on b.MEMBER_ID = d.member_id
		and convert(datetime,d.[start_dt],110) = '01-01-2017'


/*

(16159 row(s) affected)

(935640 row(s) affected)


1 mins

*/