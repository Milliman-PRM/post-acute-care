

IF OBJECT_ID('MI_CareNE_ACOI.dbo.A_Enrollment_Summary4a', 'U') IS NOT NULL
DROP TABLE MI_CareNE_ACOI.dbo.A_Enrollment_Summary4a;

SELECT
	a.[INCURRED_YEAR_AND_MONTH]
	,a.[MEMBER_MONTH_START_DATE]
	,a.[ELIG_STATUS]
	,a.[ASSN_STATUS]
	,a.area, a.PCP_Name
	,a.[CCHG_CAT_CODE_AND_DESC] as cchg_desc
	,sum(a.[MM]) as [MM]
	,sum(a.[MM_risk]) as [MM_risk]
	,sum(a.[MM_ne]) as [MM_ne]
	,sum(a.[HCC_COMMUNITY_RISK]) as [HCC_COMMUNITY_RISK]
	,sum(a.[AGE]) as [AGE]
INTO
	MI_CareNE_ACOI.dbo.A_Enrollment_Summary4a
FROM
	MI_CareNE_ACOI.dbo.A_Enrollment_Key as a
GROUP BY
	a.[INCURRED_YEAR_AND_MONTH]
	,a.[MEMBER_MONTH_START_DATE]
	,a.[ELIG_STATUS]
	,a.[ASSN_STATUS]
	,a.area, a.PCP_Name
	,a.[CCHG_CAT_CODE_AND_DESC]
GO



IF OBJECT_ID('MI_CareNE_ACOI.dbo.tmp_Claim_Summary2_A', 'U') IS NOT NULL
DROP TABLE MI_CareNE_ACOI.dbo.tmp_Claim_Summary2_A;

SELECT
	d.INCURRED_YEAR_AND_MONTH
	,d.MEMBER_MONTH_START_DATE
	,d.ELIG_STATUS
	,d.ASSN_STATUS
	,d.area, d.PCP_Name
	,'CCHG' as cat
	,d.[cchg_desc] as cat_detail
	,d.MM
	,x.HCG_SUBCAT1
	,c.paid
INTO
	MI_CareNE_ACOI.dbo.tmp_Claim_Summary2_A
FROM
	MI_CareNE_ACOI.dbo.A_Enrollment_Summary4a as d
	cross join
	(select distinct HCG_SUBCAT1_ as HCG_SUBCAT1 from ref_ACOI.dbo.ref_SvcCat_Mapping) as x
	left join
	(SELECT
		b.INCURRED_YEAR_AND_MONTH
		,b.ELIG_STATUS
		,b.ASSN_STATUS
		,b.area, b.PCP_Name
		,b.[CCHG_CAT_CODE_AND_DESC] as cchg_desc
		,x.HCG_SUBCAT1_ as HCG_SUBCAT1
		,sum(cast(a.amt_paid as float)) as paid
	FROM
		MI_CareNE_ACOI.dbo.dat_Claims as a
		right join MI_CareNE_ACOI.dbo.A_Enrollment_Key as b
			on b.[MEMBER_ID] = a.[MEMBER_ID]
			and b.[INCURRED_YEAR_AND_MONTH] = a.[INCURRED_YEAR_AND_MONTH]
		left join ref_ACOI.dbo.ref_SvcCat_Mapping as x
			on a.HCG_MR_LINE = x.HCG_MR_LINE
	WHERE
		b.MM =1
		and substring(a.HCG_MR_LINE,1,3) not in ('P81')
	GROUP BY
		b.INCURRED_YEAR_AND_MONTH
		,b.ELIG_STATUS
		,b.ASSN_STATUS
		,b.area, b.PCP_Name
		,b.[CCHG_CAT_CODE_AND_DESC]
		,x.HCG_SUBCAT1_
	) as c
		on d.[INCURRED_YEAR_AND_MONTH] = c.[INCURRED_YEAR_AND_MONTH]
		and d.[ELIG_STATUS] = c.[ELIG_STATUS]
		and d.[ASSN_STATUS] = c.[ASSN_STATUS]
		and d.area = c.area
		and d.PCP_Name = c.PCP_Name
		and d.cchg_desc = c.cchg_desc
		and x.HCG_SUBCAT1 = c.HCG_SUBCAT1
GO


IF OBJECT_ID('MI_CareNE_ACOI.dbo.tmp_Claim_Summary2_Ba', 'U') IS NOT NULL
DROP TABLE MI_CareNE_ACOI.dbo.tmp_Claim_Summary2_Ba;

SELECT
	a.INCURRED_YEAR_AND_MONTH
	,a.MEMBER_MONTH_START_DATE
	,a.ELIG_STATUS
	,a.ASSN_STATUS
	,a.area, a.PCP_Name
	,a.cpd_cat as cat_detail
	,sum(a.MM) as MM
INTO
	MI_CareNE_ACOI.dbo.tmp_Claim_Summary2_Ba
FROM
	MI_CareNE_ACOI.dbo.A_CPD2 as a
GROUP BY
	a.INCURRED_YEAR_AND_MONTH
	,a.MEMBER_MONTH_START_DATE
	,a.ELIG_STATUS
	,a.ASSN_STATUS
	,a.area, a.PCP_Name
	,a.cpd_cat
GO


IF OBJECT_ID('MI_CareNE_ACOI.dbo.tmp_Claim_Summary2_Bb', 'U') IS NOT NULL
DROP TABLE MI_CareNE_ACOI.dbo.tmp_Claim_Summary2_Bb;

SELECT
	a.INCURRED_YEAR_AND_MONTH
	,a.MEMBER_MONTH_START_DATE
	,a.ELIG_STATUS
	,a.ASSN_STATUS
	,a.area, a.PCP_Name
	,a.cpd_cat as cat_detail
	,b.HCG_SUBCAT1
	,sum(b.paid) as paid
INTO
	MI_CareNE_ACOI.dbo.tmp_Claim_Summary2_Bb
FROM
	MI_CareNE_ACOI.dbo.A_CPD2 as a
	left join MI_CareNE_ACOI.dbo.A_tmp_CPD5 as b
		on a.[member_key] = b.[member_id]
		and a.[INCURRED_YEAR_AND_MONTH] = b.[INCURRED_YEAR_AND_MONTH]
		and a.[ELIG_STATUS] = b.[ELIG_STATUS]
		and a.[ASSN_STATUS] = b.[ASSN_STATUS]
		and a.area = b.area
		and a.PCP_Name = b.PCP_Name
GROUP BY
	a.INCURRED_YEAR_AND_MONTH
	,a.MEMBER_MONTH_START_DATE
	,a.ELIG_STATUS
	,a.ASSN_STATUS
	,a.area, a.PCP_Name
	,a.cpd_cat
	,b.HCG_SUBCAT1
GO


IF OBJECT_ID('MI_CareNE_ACOI.dbo.tmp_Claim_Summary2_B', 'U') IS NOT NULL
DROP TABLE MI_CareNE_ACOI.dbo.tmp_Claim_Summary2_B;

SELECT
	a.INCURRED_YEAR_AND_MONTH
	,a.MEMBER_MONTH_START_DATE
	,a.ELIG_STATUS
	,a.ASSN_STATUS
	,a.area, a.PCP_Name
	,'PBPY Cost Range' as cat
	,a.cat_detail
	,a.MM
	,x.HCG_SUBCAT1_ as HCG_SUBCAT1
	,coalesce(b.paid,0) as paid
INTO
	MI_CareNE_ACOI.dbo.tmp_Claim_Summary2_B
FROM
	MI_CareNE_ACOI.dbo.tmp_Claim_Summary2_Ba as a
	cross join
	(select distinct HCG_SUBCAT1_ from ref_ACOI.dbo.ref_SvcCat_Mapping) as x
	left join MI_CareNE_ACOI.dbo.tmp_Claim_Summary2_Bb as b
		on a.[INCURRED_YEAR_AND_MONTH] = b.[INCURRED_YEAR_AND_MONTH]
		and a.[ELIG_STATUS] = b.[ELIG_STATUS]
		and a.[ASSN_STATUS] = b.[ASSN_STATUS]
		and a.area = b.area
		and a.PCP_Name = b.PCP_Name
		and a.cat_detail = b.cat_detail
		and x.HCG_SUBCAT1_ = b.HCG_SUBCAT1
GO



IF OBJECT_ID('MI_CareNE_ACOI.dbo.A_Claim_Summary2', 'U') IS NOT NULL
DROP TABLE MI_CareNE_ACOI.dbo.A_Claim_Summary2;

SELECT
	a.*
INTO
	MI_CareNE_ACOI.dbo.A_Claim_Summary2
FROM
	(
	SELECT
		*
	FROM
		MI_CareNE_ACOI.dbo.tmp_Claim_Summary2_A

	UNION ALL

	SELECT
		*
	FROM
		MI_CareNE_ACOI.dbo.tmp_Claim_Summary2_B
	) as a
GO


/*

Warning: Null value is eliminated by an aggregate or other SET operation.

(11809 row(s) affected)

(354270 row(s) affected)

(5829 row(s) affected)

(139327 row(s) affected)

(174870 row(s) affected)

(529140 row(s) affected)


5min

*/