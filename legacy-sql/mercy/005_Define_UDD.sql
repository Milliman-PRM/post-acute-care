


IF OBJECT_ID('mi_ATM881_ACOI.dbo.dat_membermonth_udd', 'U') IS NOT NULL
DROP TABLE mi_ATM881_ACOI.dbo.dat_membermonth_udd;

SELECT
	b.member_id
	, b.[INCURRED_YEAR_AND_MONTH]
	,case
		when b.[MEDICARE_BASIS] in ('11','21','31') then 'ESRD'
		when b.[MEDICARE_BASIS] in ('20') then 'Disabled'
		when b.[_ENR_UDF_05_] in ('01','02','04','08') then 'Aged_Dual'
		else 'Aged_Non_Dual' end
		as Medicare_Status
	,case when c.member_id is null then 0 else 1 end
		as hist_assgn
	,case when d.member_id is null then 0 else 1 end
		as curr_assgn
	,c.subpop1
INTO
	mi_ATM881_ACOI.dbo.dat_membermonth_udd
FROM
	mi_ATM881.MI.VW_MEMBMTHS_ALLCOLS as b
	left join mi_ATM881_ACOI.dbo.tmpref_MEM_ASSN as c
		on b.MEMBER_ID = c.member_id
		and b.[MEMBER_MONTH_START_DATE] >= convert(datetime,c.[start_dt],110)
		and b.[MEMBER_MONTH_START_DATE] <= convert(datetime,c.[end_dt],110)
	left join mi_ATM881_ACOI.dbo.tmpref_MEM_ASSN as d
		on b.MEMBER_ID = d.member_id
		and convert(datetime,d.[start_dt],110) = '01-01-2016'
WHERE
	b.[PROD_TYPE_KEY] = 1


/*

Warning: The join order has been enforced because a local join hint is used.

(5804210 row(s) affected)

5 mins

*/