

IF OBJECT_ID('MI_CareNE_ACOI.dbo.A_Cost_Model_leak', 'U') IS NOT NULL
DROP TABLE MI_CareNE_ACOI.dbo.A_Cost_Model_leak;

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
	, a.IN_ACO_FLG
	, a.HCG_MR_LINE_IDX_
	, a.HCG_SETTING
    , a.HCG_MR_LINE_DESC
	, a.HCG_MR_LINE
    , a.UTIL_BASIS
	, a.admits
    , a.days
    , case when HCG_SETTING in ('Facility Inpatient','Facility Inpatient - Maternity') then a.admits else a.utils end as utils
	, a.paid
INTO
	MI_CareNE_ACOI.dbo.A_Cost_Model_leak
FROM
	(SELECT
		case when a.[CLAIM_IN_NETWORK]='In Network' then 1 else 0 end as IN_ACO_FLG
		, a.INCURRED_YEAR_AND_MONTH
		, b.ELIG_STATUS
		, b.ASSN_STATUS
		,b.area, b.PCP_Name
		, x.HCG_MR_LINE_IDX_
		, x.HCG_SETTING_ as HCG_SETTING
		, x.HCG_MR_LINE_DESC_ as HCG_MR_LINE_DESC
		, a.HCG_MR_LINE
		, x.UTIL_BASIS_ as UTIL_BASIS
		, sum(cast(a.MR_ADMITS_CASES_RAW as float)) as admits
		, sum(cast(a.mr_units_days_raw as float)) as days
		, sum(cast(a.utils as float)) as utils
		, sum(cast(a.amt_paid as float)) as paid
	FROM
		MI_CareNE_ACOI.dbo.dat_Claims as a
		left join MI_CareNE_ACOI.dbo.A_Enrollment_Key as b
			on b.[MEMBER_KEY] = a.[MEMBER_KEY]
			and b.[INCURRED_YEAR_AND_MONTH] = a.[INCURRED_YEAR_AND_MONTH]
		left join ref_ACOI.dbo.ref_SvcCat_Mapping as x
			on a.HCG_MR_LINE = x.HCG_MR_LINE
	WHERE
		b.MM = 1
		and substring(a.HCG_MR_LINE,1,3) not in ('P81')
	GROUP BY
		case when a.[CLAIM_IN_NETWORK]='In Network' then 1 else 0 end
		,a.INCURRED_YEAR_AND_MONTH
		, b.ELIG_STATUS
		, b.ASSN_STATUS
		,b.area, b.PCP_Name
		, x.HCG_MR_LINE_IDX_
		, x.HCG_SETTING_
		, x.HCG_MR_LINE_DESC_
		, a.HCG_MR_LINE
		, x.UTIL_BASIS_
	) as a
	right join MI_CareNE_ACOI.dbo.A_Enrollment_Summary2 as c
		on c.[INCURRED_YEAR_AND_MONTH] = a.[INCURRED_YEAR_AND_MONTH]
		and c.[ELIG_STATUS] = a.[ELIG_STATUS]
		and c.[ASSN_STATUS] = a.[ASSN_STATUS]
		and c.area = a.area
		and c.PCP_Name = a.PCP_Name
;


/*

(37863 row(s) affected)

1min

*/

