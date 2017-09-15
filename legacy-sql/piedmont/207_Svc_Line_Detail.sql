

IF OBJECT_ID('mi_ATM945_ACOI.dbo.A_tmp_SvcCatDet1', 'U') IS NOT NULL
DROP TABLE mi_ATM945_ACOI.dbo.A_tmp_SvcCatDet1;

SELECT
	c.INCURRED_YEAR_AND_MONTH
	, c.[MEMBER_MONTH_START_DATE]
	, c.ELIG_STATUS
	, c.ASSN_STATUS
	, c.PCP_Name, c.area
	, c.MM
	, a.HCG_MR_LINE_IDX_
	, a.HCG_SETTING
    , a.HCG_MR_LINE_DESC
	, a.HCG_MR_LINE
	, a.PROC_CODE_AND_DESC
	, a.CODE_TYPE
	, a.claim_lines
	, a.paid
INTO
	mi_ATM945_ACOI.dbo.A_tmp_SvcCatDet1
FROM
	(SELECT
		a.INCURRED_YEAR_AND_MONTH
		, b.ELIG_STATUS
		, b.ASSN_STATUS
		, b.PCP_Name, b.area
		, x.HCG_MR_LINE_IDX_
		, case when x.HCG_SETTING_ in ('Facility Inpatient','Facility Inpatient - Maternity','Skilled Nursing','Home Health') then null else x.HCG_SETTING_ end as HCG_SETTING
		, x.HCG_MR_LINE_DESC_ as HCG_MR_LINE_DESC
		, a.HCG_MR_LINE
		, 'HCPCS/CPT'as CODE_TYPE
		, a.[PROC_CODE_AND_DESC]
		, sum(case
			when a.SV_STAT='P' then 1
			when a.SV_STAT='R' then -1
			else 0 end) as claim_lines
		, sum(cast(a.amt_paid as float)) as paid
	FROM
		mi_ATM945_ACOI.dbo.dat_Claims as a
		right join mi_ATM945_ACOI.dbo.A_Enrollment_Key as b
			on b.[MEMBER_KEY] = a.[MEMBER_KEY]
			and b.[INCURRED_YEAR_AND_MONTH] = a.[INCURRED_YEAR_AND_MONTH]
		left join ref_ACOI.dbo.ref_SvcCat_Mapping as x
			on a.HCG_MR_LINE = x.HCG_MR_LINE
	WHERE
		b.MM = 1
		and x.HCG_SETTING_ not in ('Prescription Drug')
	GROUP BY
		a.INCURRED_YEAR_AND_MONTH
		, b.ELIG_STATUS
		, b.ASSN_STATUS
		, b.PCP_Name, b.area
		, x.HCG_MR_LINE_IDX_
		, x.HCG_SETTING_
		, x.HCG_MR_LINE_DESC_
		, a.HCG_MR_LINE
		, a.[PROC_CODE_AND_DESC]

	UNION

	SELECT
		a.INCURRED_YEAR_AND_MONTH
		, b.ELIG_STATUS
		, b.ASSN_STATUS
		, b.PCP_Name, b.area
		, x.HCG_MR_LINE_IDX_
		, case when x.HCG_SETTING_ in ('Facility Inpatient','Facility Inpatient - Maternity','Skilled Nursing','Home Health') then null else x.HCG_SETTING_ end as HCG_SETTING
		, x.HCG_MR_LINE_DESC_ as HCG_MR_LINE_DESC
		, a.HCG_MR_LINE
		, 'Revenue Code'as CODE_TYPE
		, a.[REV_CODE_AND_DESC] as [PROC_CODE_AND_DESC]
		, sum(case
			when a.SV_STAT='P' then 1
			when a.SV_STAT='R' then -1
			else 0 end) as claim_lines
		, sum(cast(a.amt_paid as float)) as paid
	FROM
		mi_ATM945_ACOI.dbo.dat_Claims as a
		right join mi_ATM945_ACOI.dbo.A_Enrollment_Key as b
			on b.[MEMBER_KEY] = a.[MEMBER_KEY]
			and b.[INCURRED_YEAR_AND_MONTH] = a.[INCURRED_YEAR_AND_MONTH]
		left join ref_ACOI.dbo.ref_SvcCat_Mapping as x
			on a.HCG_MR_LINE = x.HCG_MR_LINE
	WHERE
		b.MM = 1
		and x.HCG_SETTING_ not in ('Prescription Drug')
	GROUP BY
		a.INCURRED_YEAR_AND_MONTH
		, b.ELIG_STATUS
		, b.ASSN_STATUS
		, b.PCP_Name, b.area
		, x.HCG_MR_LINE_IDX_
		, x.HCG_SETTING_
		, x.HCG_MR_LINE_DESC_
		, a.HCG_MR_LINE
		, a.[REV_CODE_AND_DESC]
	) as a
	right join mi_ATM945_ACOI.dbo.A_Enrollment_Summary2 as c
		on c.[INCURRED_YEAR_AND_MONTH] = a.[INCURRED_YEAR_AND_MONTH]
		and c.[ELIG_STATUS] = a.[ELIG_STATUS]
		and c.[ASSN_STATUS] = a.[ASSN_STATUS]
		and c.[PCP_Name] = a.[PCP_Name]
		and c.[area] = a.[area]
;


IF OBJECT_ID('mi_ATM945_ACOI.dbo.A_Svc_Cat_Detail', 'U') IS NOT NULL
DROP TABLE mi_ATM945_ACOI.dbo.A_Svc_Cat_Detail;

SELECT
	INCURRED_YEAR_AND_MONTH
	, [MEMBER_MONTH_START_DATE]
	, ELIG_STATUS
	, ASSN_STATUS
	, PCP_Name, area
	, MM
	, HCG_MR_LINE_IDX_
	, HCG_SETTING
    , HCG_MR_LINE_DESC
	, HCG_MR_LINE
	, case when claim_lines<10 then 'under 10 claim lines' else PROC_CODE_AND_DESC end as PROC_CODE_AND_DESC
	, CODE_TYPE
	, sum(claim_lines) as claim_lines
	, sum(paid) as paid
INTO
	mi_ATM945_ACOI.dbo.A_Svc_Cat_Detail
FROM
	mi_ATM945_ACOI.dbo.A_tmp_SvcCatDet1
GROUP BY
	INCURRED_YEAR_AND_MONTH
	, [MEMBER_MONTH_START_DATE]
	, ELIG_STATUS
	, ASSN_STATUS
	, PCP_Name, area
	, MM
	, HCG_MR_LINE_IDX_
	, HCG_SETTING
    , HCG_MR_LINE_DESC
	, HCG_MR_LINE
	, case when claim_lines<10 then 'under 10 claim lines' else PROC_CODE_AND_DESC end
	, CODE_TYPE

	
/*

(1159513 row(s) affected)
Warning: Null value is eliminated by an aggregate or other SET operation.

(283992 row(s) affected)


10mins


*/


