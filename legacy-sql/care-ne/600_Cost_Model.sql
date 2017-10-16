

IF OBJECT_ID('MI_CareNE_ACOI.dbo.A_Cost_Model', 'U') IS NOT NULL
DROP TABLE MI_CareNE_ACOI.dbo.A_Cost_Model;

SELECT
	c.INCURRED_YEAR_AND_MONTH
	, c.[MEMBER_MONTH_START_DATE]
	, c.ELIG_STATUS
	, c.ASSN_STATUS
	,c.area, c.PCP_Name
	, c.MM
	, c.MM_risk
	, c.[HCC_COMMUNITY_RISK]
	, c.[AGE]
	, a.[HCG_MR_LINE_DESC_LONG_]
	, a.admits
    , a.days
    , case when HCG_SETTING_ in ('Facility Inpatient','Facility Inpatient - Maternity') then a.admits else a.utils end as utils
	, a.paid
INTO
	MI_CareNE_ACOI.dbo.A_Cost_Model
FROM
	(SELECT
		a.INCURRED_YEAR_AND_MONTH
		, b.ELIG_STATUS
		, b.ASSN_STATUS
		, b.area, b.PCP_Name
		, x.[HCG_SETTING_]
		, x.[HCG_MR_LINE_DESC_LONG_]
		, sum(cast(a.MR_ADMITS_CASES_RAW as float)) as admits
		, sum(cast(a.mr_units_days_raw as float)) as days
		, sum(cast(a.utils as float)) as utils
		, sum(cast(a.amt_paid as float)) as paid
	FROM
		MI_CareNE_ACOI.dbo.dat_Claims as a
		right join MI_CareNE_ACOI.dbo.A_Enrollment_Key as b
			on b.[MEMBER_KEY] = a.[MEMBER_KEY]
			and b.[INCURRED_YEAR_AND_MONTH] = a.[INCURRED_YEAR_AND_MONTH]
		left join ref_ACOI.dbo.ref_SvcCat_Mapping as x
			on a.HCG_MR_LINE = x.HCG_MR_LINE
	WHERE
		b.MM = 1
		and substring(a.HCG_MR_LINE,1,3) not in ('P81')
	GROUP BY
		a.INCURRED_YEAR_AND_MONTH
		, b.ELIG_STATUS
		, b.ASSN_STATUS
		, b.area, b.PCP_Name
		, x.[HCG_SETTING_]
		, x.[HCG_MR_LINE_DESC_LONG_]
	) as a
	right join MI_CareNE_ACOI.dbo.A_Enrollment_Summary2 as c
		on c.[INCURRED_YEAR_AND_MONTH] = a.[INCURRED_YEAR_AND_MONTH]
		and c.[ELIG_STATUS] = a.[ELIG_STATUS]
		and c.[ASSN_STATUS] = a.[ASSN_STATUS]
		and c.area = a.area
		and c.PCP_Name = a.PCP_Name
GO



IF OBJECT_ID('MI_CareNE_ACOI.dbo.A_Cost_Model_wBnch', 'U') IS NOT NULL
DROP TABLE MI_CareNE_ACOI.dbo.A_Cost_Model_wBnch;

SELECT DISTINCT
	c.INCURRED_YEAR_AND_MONTH
	, c.[MEMBER_MONTH_START_DATE]
	, c.ELIG_STATUS
	, c.ASSN_STATUS
	, c.area, c.PCP_Name
	, c.MM
	, c.MM_risk
	, c.[HCC_COMMUNITY_RISK]
	, c.[AGE]
	, x.HCG_SUBCAT0_ as HCG_MR_LINE_IDX_
	, x.HCG_SETTING_ as HCG_SETTING
	, x.HCG_MR_LINE_DESC_ as HCG_MR_LINE_DESC
	, x.HCG_MR_LINE_DESC_LONG_ as HCG_MR_LINE
	, x.UTIL_BASIS_ as UTIL_BASIS
	, a.admits
    , a.days
    , a.utils
	, a.paid
	, null as cases_LM
	, null as cases_WM
	, b.LM_util as utils_LM
	, b.WM_util as utils_WM
INTO
	MI_CareNE_ACOI.dbo.A_Cost_Model_wBnch
FROM
	MI_CareNE_ACOI.dbo.A_Cost_Model as a
	full join MI_CareNE_ACOI.dbo.A_Bnchmrk_Cost_Model as b
		on b.[INCURRED_YEAR_AND_MONTH] = a.[INCURRED_YEAR_AND_MONTH]
		and b.[ELIG_STATUS] = a.[ELIG_STATUS]
		and b.[ASSN_STATUS] = a.[ASSN_STATUS]
		and b.[PCP_Name] = a.[PCP_Name]
		and b.[area] = a.[area]
		and b.cm_cat = a.HCG_MR_LINE_DESC_LONG_
	left join ref_ACOI.dbo.ref_SvcCat_Mapping as x
		on x.HCG_MR_LINE_DESC_LONG_ = COALESCE(a.HCG_MR_LINE_DESC_LONG_,b.cm_cat)
	right join MI_CareNE_ACOI.dbo.A_Enrollment_Summary2 as c
		on c.[INCURRED_YEAR_AND_MONTH] =  COALESCE(a.[INCURRED_YEAR_AND_MONTH],b.[INCURRED_YEAR_AND_MONTH])
		and c.[ELIG_STATUS] = COALESCE(a.[ELIG_STATUS],b.[ELIG_STATUS])
		and c.[ASSN_STATUS] = COALESCE(a.[ASSN_STATUS],b.[ASSN_STATUS])
		and c.[PCP_Name] = COALESCE(a.[PCP_Name],b.[PCP_Name])
		and c.[area] = COALESCE(a.[area],b.[area])
GO



/*


(22889 row(s) affected)

(24537 row(s) affected)

1min

*/
