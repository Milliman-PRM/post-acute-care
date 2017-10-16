

IF OBJECT_ID('mi_ATM881_ACOI.demo.A_tmp_HH1', 'U') IS NOT NULL
DROP TABLE mi_ATM881_ACOI.demo.A_tmp_HH1;

SELECT
	a.INCURRED_YEAR_AND_MONTH
	, b.ELIG_STATUS
	, b.ASSN_STATUS
	, b.PCP_Name, b.area
	, a.[PRVDR_OSCAR_NUM] as [BILL_PROV_ID]
	, ltrim(rtrim([PRVDR_OSCAR_NUM])) + ' - ' + coalesce(ltrim(rtrim([PRVDR_OSCAR_NAME])),'')  as [BILL_PROV_NAME]
	, case when c.CCN is null then 0 else 1 end as PROV_Flag
	, a.CLAIM_ID
	, a.member_key
	, case when max(a.SV_STAT) = 'R' then -1 else 1 end as cases
    , sum(case
		when substring(REV_CODE,1,3) in ('042','043','044','055','056','057') and a.SV_STAT = 'P' then 1
		when substring(REV_CODE,1,3) in ('042','043','044','055','056','057') and a.SV_STAT = 'R' then -1
		else 0 end) as visits
	, sum(cast(a.amt_paid as float)) as paid
	, max(from_date) as last_visit_date
INTO
	mi_ATM881_ACOI.demo.A_tmp_HH1
FROM
	mi_ATM881_ACOI.demo.dat_Claims as a
	right join mi_ATM881_ACOI.demo.A_Enrollment_Key as b
		on b.[MEMBER_KEY] = a.[MEMBER_KEY]
		and b.[INCURRED_YEAR_AND_MONTH] = a.[INCURRED_YEAR_AND_MONTH]
	left join mi_ATM881_ACOI.demo.tmpref_ccn_flag as c
		on a.[PRVDR_OSCAR_NUM] = c.CCN
WHERE
	a.SV_STAT != 'D'
	and b.MM = 1
	and a.HCG_MR_LINE = 'P82a'
GROUP BY
    a.INCURRED_YEAR_AND_MONTH
	, b.ELIG_STATUS
	, b.ASSN_STATUS
	, b.PCP_Name, b.area
	, a.[PRVDR_OSCAR_NUM]
	, ltrim(rtrim([PRVDR_OSCAR_NUM])) + ' - ' + coalesce(ltrim(rtrim([PRVDR_OSCAR_NAME])),'') 
	, case when c.CCN is null then 0 else 1 end
	, a.CLAIM_ID
	, a.member_key
;



IF OBJECT_ID('mi_ATM881_ACOI.demo.A_tmp_HH2', 'U') IS NOT NULL
DROP TABLE mi_ATM881_ACOI.demo.A_tmp_HH2;

SELECT
	a.INCURRED_YEAR_AND_MONTH
	, a.ELIG_STATUS
	, a.ASSN_STATUS
	, a.PCP_Name, a.area
	, a.[BILL_PROV_ID]
    , a.[BILL_PROV_NAME]
	, PROV_FLAG
	, a.CLAIM_ID
	, a.cases
    , a.visits
	, a.paid
	, max(d.readmit) as readmit
	, max(e.er) as er
INTO
	mi_ATM881_ACOI.demo.A_tmp_HH2
FROM
	mi_ATM881_ACOI.demo.A_tmp_HH1 as a
	left join
		(
		SELECT
			FROM_DATE
			, member_key
			, 1 as readmit
		FROM
			mi_ATM881_ACOI.demo.dat_Claims as a
		WHERE
			a.SV_STAT = 'P'
			and a.HCG_MR_LINE in ('I11a','I11b','I12')
		GROUP BY
			FROM_DATE
			, member_key
		) as d
		on a.member_key = d.member_key
		and a.last_visit_date < d.from_date
		and DATEADD(day,14,a.last_visit_date) > d.from_date
	left join
		(
		SELECT
			FROM_DATE
			, member_key
			, 1 as er
		FROM
			mi_ATM881_ACOI.demo.dat_Claims as a
		WHERE
			a.SV_STAT = 'P'
			and a.HCG_MR_LINE in ('O11a')
		GROUP BY
			FROM_DATE
			, member_key
		) as e
		on a.member_key = e.member_key
		and a.last_visit_date < e.from_date
		and DATEADD(day,14,a.last_visit_date) > e.from_date
GROUP BY
	a.INCURRED_YEAR_AND_MONTH
	, a.ELIG_STATUS
	, a.ASSN_STATUS
	, a.PCP_Name, a.area
	, a.[BILL_PROV_ID]
    , a.[BILL_PROV_NAME]
	, PROV_FLAG
	, a.CLAIM_ID
	, a.cases
    , a.visits
	, a.paid
GO



IF OBJECT_ID('mi_ATM881_ACOI.demo.A_HH', 'U') IS NOT NULL
DROP TABLE mi_ATM881_ACOI.demo.A_HH;

SELECT
	c.INCURRED_YEAR_AND_MONTH
	, c.[MEMBER_MONTH_START_DATE]
	, c.ELIG_STATUS
	, c.ASSN_STATUS
	, c.PCP_Name, c.area
	, c.MM
	, a.[BILL_PROV_ID]
    , a.[BILL_PROV_NAME]
	, PROV_FLAG
	, sum(a.cases) as cases
    , sum(a.visits) as visits
	, sum(a.paid) as paid
	, sum(case when a.readmit=1 then a.cases else 0 end) as readmit
	, sum(case when a.er=1 then a.cases else 0 end) as er
INTO
	mi_ATM881_ACOI.demo.A_HH
FROM
	mi_ATM881_ACOI.demo.A_tmp_HH2 as a
	right join mi_ATM881_ACOI.demo.A_Enrollment_Summary2 as c
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
	, PROV_FLAG
;


/*

(151071 row(s) affected)
Warning: Null value is eliminated by an aggregate or other SET operation.

(151071 row(s) affected)
Warning: Null value is eliminated by an aggregate or other SET operation.

(27554 row(s) affected)

10mins


*/