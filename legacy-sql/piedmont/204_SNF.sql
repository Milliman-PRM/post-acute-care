

IF OBJECT_ID('mi_ATM945_ACOI.dbo.A_tmp_SNF', 'U') IS NOT NULL
DROP TABLE mi_ATM945_ACOI.dbo.A_tmp_SNF;

SELECT
    a.INCURRED_YEAR_AND_MONTH
	, a.ELIG_STATUS
	, a.ASSN_STATUS
	, a.PCP_Name, a.area
	, a.[BILL_PROV_ID]
    , a.[BILL_PROV_NAME]
	, case when c.CCN is null then 0 else 1 end as PROV_Flag
	, a.CS_CLAIM_ID_KEY
	, max(a.admits) as admits
    , max(a.days) as days
	, max(a.paid) as paid
	, max(b.readmit) as readmit
INTO
	mi_ATM945_ACOI.dbo.A_tmp_SNF
FROM
	(SELECT
		a.INCURRED_YEAR_AND_MONTH
		, b.ELIG_STATUS
		, b.ASSN_STATUS
		, b.PCP_Name, b.area
		, a.[PRVDR_OSCAR_NUM] as [BILL_PROV_ID]
		, ltrim(rtrim([PRVDR_OSCAR_NUM])) + ' - ' + coalesce(ltrim(rtrim([PRVDR_OSCAR_NAME])),'')  as [BILL_PROV_NAME]
		, a.CS_CLAIM_ID_KEY
		, a.MEMBER_KEY
		, min(a.FROM_DATE) as from_date
		, max(a.TO_DATE) as to_date
		, sum(cast(a.MR_ADMITS_CASES_RAW as float)) as admits
		, sum(cast(a.mr_units_days_raw as float)) as days
		, sum(cast(a.amt_paid as float)) as paid
	FROM
		mi_ATM945_ACOI.dbo.dat_Claims as a
		right join mi_ATM945_ACOI.dbo.A_Enrollment_Key as b
			on b.[MEMBER_KEY] = a.[MEMBER_KEY]
			and b.[INCURRED_YEAR_AND_MONTH] = a.[INCURRED_YEAR_AND_MONTH]
	WHERE
		a.SV_STAT != 'D'
		and b.MM = 1
		and a.HCG_MR_LINE = 'I31'
	GROUP BY
		a.INCURRED_YEAR_AND_MONTH
		, b.ELIG_STATUS
		, b.ASSN_STATUS
		, b.PCP_Name, b.area
		, a.[PRVDR_OSCAR_NUM] 
		, ltrim(rtrim([PRVDR_OSCAR_NUM])) + ' - ' + coalesce(ltrim(rtrim([PRVDR_OSCAR_NAME])),'') 
		, a.CS_CLAIM_ID_KEY
		, a.MEMBER_KEY
	) as a
	left join
	(
	SELECT
		FROM_DATE
		, member_key
		, 1 as readmit
	FROM
		mi_ATM945_ACOI.dbo.dat_Claims as a
	WHERE
		a.SV_STAT = 'P'
		and a.HCG_MR_LINE in ('I11a','I11b','I12')
	GROUP BY
		FROM_DATE
		, member_key
	) as b
		on a.member_key = b.member_key
		and a.to_date <= b.from_date
		and DATEADD(day,2,a.to_date) > b.from_date
	left join mi_ATM945_ACOI.dbo.tmpref_ccn_flag as c
		on a.[BILL_PROV_ID] = c.CCN
		
GROUP BY
	a.INCURRED_YEAR_AND_MONTH
	, a.ELIG_STATUS
	, a.ASSN_STATUS
	, a.PCP_Name, a.area
	, a.[BILL_PROV_ID]
	, a.[BILL_PROV_NAME]
	, case when c.CCN is null then 0 else 1 end
	, a.CS_CLAIM_ID_KEY
GO



IF OBJECT_ID('mi_ATM945_ACOI.dbo.A_SNF', 'U') IS NOT NULL
DROP TABLE mi_ATM945_ACOI.dbo.A_SNF;

SELECT
	c.INCURRED_YEAR_AND_MONTH
	, c.[MEMBER_MONTH_START_DATE]
	, c.ELIG_STATUS
	, c.ASSN_STATUS
	, c.PCP_Name, c.area
	, c.MM
	, [BILL_PROV_ID]
    , [BILL_PROV_NAME]
	, PROV_flag
	, sum(admits) as admits
    , sum(days) as days
	, sum(paid) as paid
	, sum(case when readmit=1 then admits else 0 end) as readmit
INTO
	mi_ATM945_ACOI.dbo.A_SNF
FROM
	mi_ATM945_ACOI.dbo.A_tmp_SNF as a
	right join mi_ATM945_ACOI.dbo.A_Enrollment_Summary2 as c
		on c.[INCURRED_YEAR_AND_MONTH] = a.[INCURRED_YEAR_AND_MONTH]
		and c.[ELIG_STATUS] = a.[ELIG_STATUS]
		and c.[ASSN_STATUS] = a.[ASSN_STATUS]
		and c.[PCP_Name] = a.[PCP_Name]
		and c.[area] = a.[area]
GROUP BY
	c.INCURRED_YEAR_AND_MONTH
	, c.[MEMBER_MONTH_START_DATE]
	, c.ELIG_STATUS
	, c.ASSN_STATUS
	, c.PCP_Name, c.area
	, c.MM
	, a.[BILL_PROV_ID]
    , a.[BILL_PROV_NAME]
	, PROV_flag
;



/*

Warning: Null value is eliminated by an aggregate or other SET operation.

(10813 row(s) affected)
Warning: Null value is eliminated by an aggregate or other SET operation.

(5911 row(s) affected)

1 mins

*/