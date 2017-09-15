

IF OBJECT_ID('MI_CareNE_ACOI.dbo.A_tmp_ACSA1', 'U') IS NOT NULL
DROP TABLE MI_CareNE_ACOI.dbo.A_tmp_ACSA1;

SELECT
    B.[CS_CLAIM_ID_KEY]
	,max(case when A.CI_MEASURE_ID = 'PQI_01_TOTAL_2016H' then 1 else 0 end) as PQI1
	,max(case when A.CI_MEASURE_ID = 'PQI_02_TOTAL_2016H' then 1 else 0 end) as PQI2
	,max(case when A.CI_MEASURE_ID = 'PQI_03_TOTAL_2016H' then 1 else 0 end) as PQI3
	,max(case when A.CI_MEASURE_ID = 'PQI_5_TOTAL_2016H' then 1 else 0 end) as PQI5
	,max(case when A.CI_MEASURE_ID = 'PQI_07_TOTAL_2016H' then 1 else 0 end) as PQI7
	,max(case when A.CI_MEASURE_ID = 'PQI_08_TOTAL_2016H' then 1 else 0 end) as PQI8
	,max(0) as PQI9
	,max(case when A.CI_MEASURE_ID = 'PQI_10_TOTAL_2016H' then 1 else 0 end) as PQI10
	,max(case when A.CI_MEASURE_ID = 'PQI_11_TOTAL_2016H' then 1 else 0 end) as PQI11
	,max(case when A.CI_MEASURE_ID = 'PQI_12_TOTAL_2016H' then 1 else 0 end) as PQI12
	,max(0) as PQI13
	,max(case when A.CI_MEASURE_ID = 'PQI_14_TOTAL_2016H' then 1 else 0 end) as PQI14
	,max(case when A.CI_MEASURE_ID = 'PQI_15_2016H' then 1 else 0 end) as PQI15
	,max(case when A.CI_MEASURE_ID = 'PQI_16_TOTAL_2016H' then 1 else 0 end) as PQI16
INTO
	MI_CareNE_ACOI.dbo.A_tmp_ACSA1
FROM
	MI_CareNE.EBM.CI_EVENTS_NUM A 
	LEFT JOIN MI_CareNE.dbo.Services B
		ON A.[SERVICES_KEY] = B.[SERVICES_KEY]
WHERE
	CI_MEASURE_ID like '%PQI%'
GROUP BY
	B.[CS_CLAIM_ID_KEY]
GO



IF OBJECT_ID('MI_CareNE_ACOI.dbo.A_tmp_ACSA2', 'U') IS NOT NULL
DROP TABLE MI_CareNE_ACOI.dbo.A_tmp_ACSA2;

SELECT
	a.[MEMBER_KEY]
	,[PRVDR_OSCAR_NUM] as [BILL_PROV_ID]
	, ltrim(rtrim([PRVDR_OSCAR_NUM])) + ' - ' + coalesce(ltrim(rtrim([PRVDR_OSCAR_NAME])),'')  as [BILL_PROV_NAME]
	,0 as [PROV_FLAG]
	,a.INCURRED_YEAR_AND_MONTH
	,[PQI1]
	,[PQI2]
	,[PQI3]
	,[PQI5]
	,[PQI7]
	,[PQI8]
	,[PQI9]
	,[PQI10]
	,[PQI11]
	,[PQI12]
	,[PQI13]
	,[PQI14]
	,[PQI15]
	,[PQI16]
	,case when
		(ISNULL(PQI1,0) + ISNULL(PQI2,0) + ISNULL(PQI3,0) + ISNULL(PQI5,0) + ISNULL(PQI7,0) + ISNULL(PQI8,0) + ISNULL(PQI9,0) +
		ISNULL(PQI10,0) + ISNULL(PQI11,0) + ISNULL(PQI12,0) + ISNULL(PQI13,0) + ISNULL(PQI14,0) + ISNULL(PQI15,0) + ISNULL(PQI16,0)) > 0 then 1 else 0 end as PQI_tot
	,a.MR_ADMITS_CASES_RAW
	,a.amt_paid
into
	MI_CareNE_ACOI.dbo.A_tmp_ACSA2
from
	MI_CareNE_ACOI.dbo.dat_Claims as a
	left join MI_CareNE_ACOI.dbo.A_tmp_ACSA1 as b
		on a.[CS_CLAIM_ID_KEY] = b.[CS_CLAIM_ID_KEY]
where
	a.HCG_MR_LINE in ('I11a','I11b','I12')
	and a.MR_ADMITS_CASES_RAW != 0
GO



IF OBJECT_ID('MI_CareNE_ACOI.dbo.A_ACSA', 'U') IS NOT NULL
DROP TABLE MI_CareNE_ACOI.dbo.A_ACSA;

SELECT
	c.INCURRED_YEAR_AND_MONTH
	, c.[MEMBER_MONTH_START_DATE]
	, c.ELIG_STATUS
	, c.ASSN_STATUS
	, c.area, c.PCP_Name
	, [BILL_PROV_ID]
	, [BILL_PROV_NAME]
	, [PROV_FLAG]
	, c.MM
	, c.MM_risk
	, c.MM_ne
	, c.[HCC_COMMUNITY_RISK]
	, c.[AGE]
	,sum(coalesce([PQI1]*MR_ADMITS_CASES_RAW,0)) as PQI1
	,sum(coalesce([PQI2]*MR_ADMITS_CASES_RAW,0)) as  PQI2
	,sum(coalesce([PQI3]*MR_ADMITS_CASES_RAW,0)) as PQI3
	,sum(coalesce([PQI5]*MR_ADMITS_CASES_RAW,0)) as PQI5
	,sum(coalesce([PQI7]*MR_ADMITS_CASES_RAW,0)) as PQI7
	,sum(coalesce([PQI8]*MR_ADMITS_CASES_RAW,0)) as PQI8
	,sum(coalesce([PQI9]*MR_ADMITS_CASES_RAW,0)) as PQI9
	,sum(coalesce([PQI10]*MR_ADMITS_CASES_RAW,0)) as PQI10
	,sum(coalesce([PQI11]*MR_ADMITS_CASES_RAW,0)) as PQI11
	,sum(coalesce([PQI12]*MR_ADMITS_CASES_RAW,0)) as PQI12
	,sum(coalesce([PQI13]*MR_ADMITS_CASES_RAW,0)) as PQI13
	,sum(coalesce([PQI14]*MR_ADMITS_CASES_RAW,0)) as PQI14
	,sum(coalesce([PQI15]*MR_ADMITS_CASES_RAW,0)) as PQI15
	,sum(coalesce([PQI16]*MR_ADMITS_CASES_RAW,0)) as PQI16
	,sum(coalesce([PQI_tot]*MR_ADMITS_CASES_RAW,0)) as PQI_tot
	,sum(coalesce([PQI_tot]*amt_paid,0)) as PQI_tot_paid
	,sum(coalesce(MR_ADMITS_CASES_RAW,0)) as admits
	,sum(coalesce(amt_paid,0)) as paid
INTO
	MI_CareNE_ACOI.dbo.A_ACSA
FROM
	MI_CareNE_ACOI.dbo.A_tmp_ACSA2 as a
	right join MI_CareNE_ACOI.dbo.A_Enrollment_Key as b
		on b.[MEMBER_KEY] = a.[MEMBER_KEY]
		and b.[INCURRED_YEAR_AND_MONTH] = a.[INCURRED_YEAR_AND_MONTH]
	right join MI_CareNE_ACOI.dbo.A_Enrollment_Summary2 as c
		on b.[INCURRED_YEAR_AND_MONTH] = c.[INCURRED_YEAR_AND_MONTH]
		and b.[ELIG_STATUS] = c.[ELIG_STATUS]
		and  b.ASSN_STATUS = c.ASSN_STATUS
		and b.PCP_Name = c.PCP_Name
		and b.area = c.area
WHERE
	b.MM =1
GROUP BY
	c.INCURRED_YEAR_AND_MONTH
	, c.[MEMBER_MONTH_START_DATE]
	, c.ELIG_STATUS
	, c.ASSN_STATUS
	, c.area, c.PCP_Name
	, [BILL_PROV_ID]
	, [BILL_PROV_NAME]
	, [PROV_FLAG]
	, c.MM
	, c.MM_risk
	, c.MM_ne
	, c.[HCC_COMMUNITY_RISK]
	, c.[AGE]
GO


/*

(6538 row(s) affected)

(14920 row(s) affected)

(6541 row(s) affected)

5 min

*/

