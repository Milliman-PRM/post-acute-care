select count(*) from A_tmp_SvcCatDet1_Mapped_V4
select count(*) from A_tmp_SvcCatDet1_Mapped_V5

select distinct hcg_mr_line
from A_tmp_SvcCatDet1_Mapped
where HCG_MR_LINE like '%P15%'
select top 500 * from A_tmp_SvcCatDet1_Mapped_V4
where PROC_CODE_AND_DESC is not null
order by INCURRED_YEAR_AND_MONTH, MEMBER_MONTH_START_DATE, PROC_CODE_AND_DESC

select top 500 * from A_tmp_SvcCatDet1_Mapped_V5
where PROC_CODE_AND_DESC is not null
order by INCURRED_YEAR_AND_MONTH, MEMBER_MONTH_START_DATE, PROC_CODE_AND_DESC
IF OBJECT_ID('mi_ATM881_ACOI.dbo.A_tmp_SvcCatDet1_Mapped_V5', 'U') IS NOT NULL
DROP TABLE mi_ATM881_ACOI.dbo.A_tmp_SvcCatDet1_Mapped_V5;

SELECT
	  c.INCURRED_YEAR_AND_MONTH
	, c.[MEMBER_MONTH_START_DATE]
	, c.ELIG_STATUS
	, c.ASSN_STATUS
	, c.PCP_Name
	, c.area
	, c.MM
	, a.HCG_MR_LINE_IDX_
	, a.HCG_SETTING
    , a.HCG_MR_LINE_DESC
	, a.HCG_MR_LINE
	, a.PROC_CODE_AND_DESC
	, a.CODE_TYPE
	, a.claim_lines
	, a.paid
	, Mapping
	, [Mapping Type]
INTO
	mi_ATM881_ACOI.dbo.A_tmp_SvcCatDet1_Mapped_V5
FROM
	(SELECT
		  a.INCURRED_YEAR_AND_MONTH
		, b.ELIG_STATUS
		, b.ASSN_STATUS
		, b.PCP_Name, b.area
		, x.HCG_MR_LINE_IDX_
		, case when x.HCG_SETTING_ in ('Facility Inpatient','Facility Inpatient - Maternity','Skilled Nursing','Home Health') then null 
			   else x.HCG_SETTING_ end as HCG_SETTING
		, x.HCG_MR_LINE_DESC_ as HCG_MR_LINE_DESC
		, a.HCG_MR_LINE
		, 'HCPCS/CPT'as CODE_TYPE
		, a.[PROC_CODE_AND_DESC]
		, CASE WHEN A.HCG_MR_LINE = 'P84' THEN
				  CASE WHEN C.[Drug Mapping] is NULL THEN 'Other'
					   ELSE C.[Drug Mapping] END
			   WHEN A.HCG_MR_LINE in ('O16a', 'O16b', 'P34a', 'P34b') THEN
				  CASE WHEN C.[Drug Mapping] is NULL THEN 'Other'
					   ELSE C.[Drug Mapping] END
			   WHEN A.HCG_MR_LINE = 'P15' THEN
				  CASE WHEN C.[Drug Mapping] is null then 'Other'
					   ELSE C.[Drug Mapping] END
			   ELSE 'Not Mapped' END [Mapping]
		,  CASE WHEN A.HCG_MR_LINE = 'P84' THEN 'Ancillary - DME and Supplies'
				WHEN A.HCG_MR_LINE in ('O16a', 'O16b') THEN 'Facility Outpatient - Drugs (O16)'
				WHEN A.HCG_MR_LINE in ('P34a', 'P34b') THEN 'Professional - Drugs (P34)'
				WHEN A.HCG_MR_LINE = 'P15' THEN 'Professional - Office Surgery'
				ELSE 'Not Mapped' END [Mapping Type]
		, sum(case
			when a.SV_STAT='P' then 1
			when a.SV_STAT='R' then -1
			else 0 end) as claim_lines
		, sum(cast(a.amt_paid as float)) as paid
	FROM
		mi_ATM881_ACOI.dbo.dat_Claims as a
		right join mi_ATM881_ACOI.dbo.A_Enrollment_Key as b
			on b.[MEMBER_KEY] = a.[MEMBER_KEY]
			and b.[INCURRED_YEAR_AND_MONTH] = a.[INCURRED_YEAR_AND_MONTH]
		left join ref_ACOI.dbo.ref_SvcCat_Mapping as x
			on a.HCG_MR_LINE = x.HCG_MR_LINE
		left join
			(
			select [Drug Mapping], [Code], 'Drug' [Type] from Drug_Mapping
			union
			select [DME Mapping], [Code], 'DME' [Type] from DME_Mapping
			union
			select [Mapping], [Code], 'Office Surgery' [Type] from OS_Mapping
			) as C on A.[PROC_CODE] = C.[Code]
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
		, CASE WHEN A.HCG_MR_LINE = 'P84' THEN
				  CASE WHEN C.[Drug Mapping] is NULL THEN 'Other'
					   ELSE C.[Drug Mapping] END
			   WHEN A.HCG_MR_LINE in ('O16a', 'O16b', 'P34a', 'P34b') THEN
				  CASE WHEN C.[Drug Mapping] is NULL THEN 'Other'
					   ELSE C.[Drug Mapping] END
			   WHEN A.HCG_MR_LINE = 'P15' THEN
				  CASE WHEN C.[Drug Mapping] is null then 'Other'
					   ELSE C.[Drug Mapping] END
			   ELSE 'Not Mapped' END 
		,  CASE WHEN A.HCG_MR_LINE = 'P84' THEN 'Ancillary - DME and Supplies'
				WHEN A.HCG_MR_LINE in ('O16a', 'O16b') THEN 'Facility Outpatient - Drugs (O16)'
				WHEN A.HCG_MR_LINE in ('P34a', 'P34b') THEN 'Professional - Drugs (P34)'
				WHEN A.HCG_MR_LINE = 'P15' THEN 'Professional - Office Surgery'
				ELSE 'Not Mapped' END

	UNION

	SELECT
		a.INCURRED_YEAR_AND_MONTH
		, b.ELIG_STATUS
		, b.ASSN_STATUS
		, b.PCP_Name, b.area
		, x.HCG_MR_LINE_IDX_
		, case when x.HCG_SETTING_ in ('Facility Inpatient','Facility Inpatient - Maternity','Skilled Nursing','Home Health') then null 
			   else x.HCG_SETTING_ end as HCG_SETTING
		, x.HCG_MR_LINE_DESC_ as HCG_MR_LINE_DESC
		, a.HCG_MR_LINE
		, 'Revenue Code'as CODE_TYPE
		, a.[REV_CODE_AND_DESC] as [PROC_CODE_AND_DESC]
		, CASE WHEN A.HCG_MR_LINE = 'P84' THEN
				  CASE WHEN C.[Drug Mapping] is NULL THEN 'Other'
					   ELSE C.[Drug Mapping] END
			   WHEN A.HCG_MR_LINE in ('O16a', 'O16b', 'P34a', 'P34b') THEN
				  CASE WHEN C.[Drug Mapping] is NULL THEN 'Other'
					   ELSE C.[Drug Mapping] END
			   WHEN A.HCG_MR_LINE = 'P15' THEN
				  CASE WHEN C.[Drug Mapping] is null then 'Other'
					   ELSE C.[Drug Mapping] END
			   ELSE 'Not Mapped' END [Mapping]
		,  CASE WHEN A.HCG_MR_LINE = 'P84' THEN 'Ancillary - DME and Supplies'
				WHEN A.HCG_MR_LINE in ('O16a', 'O16b') THEN 'Facility Outpatient - Drugs (O16)'
				WHEN A.HCG_MR_LINE in ('P34a', 'P34b') THEN 'Professional - Drugs (P34)'
				WHEN A.HCG_MR_LINE = 'P15' THEN 'Professional - Office Surgery'
				ELSE 'Not Mapped' END [Mapping Type]
		, sum(case
			when a.SV_STAT='P' then 1
			when a.SV_STAT='R' then -1
			else 0 end) as claim_lines
		, sum(cast(a.amt_paid as float)) as paid
	FROM
		mi_ATM881_ACOI.dbo.dat_Claims as a
		right join mi_ATM881_ACOI.dbo.A_Enrollment_Key as b
			on b.[MEMBER_KEY] = a.[MEMBER_KEY]
			and b.[INCURRED_YEAR_AND_MONTH] = a.[INCURRED_YEAR_AND_MONTH]
		left join ref_ACOI.dbo.ref_SvcCat_Mapping as x
			on a.HCG_MR_LINE = x.HCG_MR_LINE
		left join
			(
			select [Drug Mapping], [Code], 'Drug' [Type] from Drug_Mapping
			union
			select [DME Mapping], [Code], 'DME' [Type] from DME_Mapping
			) as C on A.[PROC_CODE] = C.[Code]
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
		, CASE WHEN A.HCG_MR_LINE = 'P84' THEN
				  CASE WHEN C.[Drug Mapping] is NULL THEN 'Other'
					   ELSE C.[Drug Mapping] END
			   WHEN A.HCG_MR_LINE in ('O16a', 'O16b', 'P34a', 'P34b') THEN
				  CASE WHEN C.[Drug Mapping] is NULL THEN 'Other'
					   ELSE C.[Drug Mapping] END
			   WHEN A.HCG_MR_LINE = 'P15' THEN
				  CASE WHEN C.[Drug Mapping] is null then 'Other'
					   ELSE C.[Drug Mapping] END
			   ELSE 'Not Mapped' END 
		,  CASE WHEN A.HCG_MR_LINE = 'P84' THEN 'Ancillary - DME and Supplies'
				WHEN A.HCG_MR_LINE in ('O16a', 'O16b') THEN 'Facility Outpatient - Drugs (O16)'
				WHEN A.HCG_MR_LINE in ('P34a', 'P34b') THEN 'Professional - Drugs (P34)'
				WHEN A.HCG_MR_LINE = 'P15' THEN 'Professional - Office Surgery'
				ELSE 'Not Mapped' END
	) as a
	right join mi_ATM881_ACOI.dbo.A_Enrollment_Summary2 as c
		on c.[INCURRED_YEAR_AND_MONTH] = a.[INCURRED_YEAR_AND_MONTH]
		and c.[ELIG_STATUS] = a.[ELIG_STATUS]
		and c.[ASSN_STATUS] = a.[ASSN_STATUS]
		and c.[PCP_Name] = a.[PCP_Name]
		and c.[area] = a.[area]
;

/*
IF OBJECT_ID('mi_ATM881_ACOI.dbo.A_Svc_Cat_Detail', 'U') IS NOT NULL
DROP TABLE mi_ATM881_ACOI.dbo.A_Svc_Cat_Detail;

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
	mi_ATM881_ACOI.dbo.A_Svc_Cat_Detail
FROM
	mi_ATM881_ACOI.dbo.A_tmp_SvcCatDet1
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
*/
	
/*

(4906484 row(s) affected)
Warning: Null value is eliminated by an aggregate or other SET operation.

(1051073 row(s) affected)

1 hour


*/


