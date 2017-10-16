

IF OBJECT_ID('mi_ATM881_ACOI.demo.A_Claim_Summary1', 'U') IS NOT NULL
DROP TABLE mi_ATM881_ACOI.demo.A_Claim_Summary1;

SELECT
	d.INCURRED_YEAR_AND_MONTH
	,d.MEMBER_MONTH_START_DATE
	,d.ELIG_STATUS
	,d.ASSN_STATUS
	,d.area, d.PCP_Name
	,d.Age
	,d.MM
	,d.MM_risk
	,d.HCC_COMMUNITY_RISK
	,c.HCG_SETTING
	,c.paid
INTO
	mi_ATM881_ACOI.demo.A_Claim_Summary1
FROM
	(SELECT
		b.INCURRED_YEAR_AND_MONTH
		,b.ELIG_STATUS
		,b.ASSN_STATUS
		,b.area, b.PCP_Name
		,x.HCG_SETTING_ as HCG_SETTING
		,sum(cast(a.amt_paid as float)) as paid
	FROM
		mi_ATM881_ACOI.demo.dat_Claims as a
		right join mi_ATM881_ACOI.demo.A_Enrollment_Key as b
			on b.[MEMBER_KEY] = a.[MEMBER_KEY]
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
		,x.HCG_SETTING_) as c
	right join mi_ATM881_ACOI.demo.A_Enrollment_Summary2b as d
		on d.[INCURRED_YEAR_AND_MONTH] = c.[INCURRED_YEAR_AND_MONTH]
		and d.[ELIG_STATUS] = c.[ELIG_STATUS]
		and d.[ASSN_STATUS] = c.[ASSN_STATUS]
		and d.area = c.area
		and d.PCP_Name = c.PCP_Name
GO



IF OBJECT_ID('mi_ATM881_ACOI.demo.A_Demo_Sum', 'U') IS NOT NULL
DROP TABLE mi_ATM881_ACOI.demo.A_Demo_Sum;

SELECT
	c.[INCURRED_YEAR_AND_MONTH]
	,c.[MEMBER_MONTH_START_DATE]
	,c.[ELIG_STATUS]
	,c.ASSN_STATUS
	,c.area, c.PCP_Name
	,c.AGE
	,c.mems
	,c.MM
	,c.MM_risk
	,c.MM_ne
	,c.HCC_COMMUNITY_RISK
	,sum(d.paid) as paid
INTO
	mi_ATM881_ACOI.demo.A_Demo_Sum
FROM
	(SELECT
		a.[INCURRED_YEAR_AND_MONTH]
		,a.[MEMBER_MONTH_START_DATE]
		,a.[ELIG_STATUS]
		,a.ASSN_STATUS
		,a.area, a.PCP_Name
		,count(distinct b.MEMBER_KEY) as mems
		,sum(b.MM) as MM
		,sum(b.MM_risk) as MM_risk
		,sum(b.MM_ne) as MM_ne
		,sum(b.HCC_COMMUNITY_RISK) as HCC_COMMUNITY_RISK
		,sum(b.AGE) as AGE
	FROM
		mi_ATM881_ACOI.demo.A_Enrollment_Summary2b as a
		right join mi_ATM881_ACOI.demo.A_Enrollment_Key as b
			on a.[ELIG_STATUS] = b.[ELIG_STATUS]
			and a.[ASSN_STATUS] = b.[ASSN_STATUS]
			and a.[area] = b.[area]
			and a.[PCP_Name] = b.[PCP_Name]
			and a.[MEMBER_MONTH_START_DATE] >= b.[MEMBER_MONTH_START_DATE]
			and a.[MEMBER_MONTH_START_DATE] <= DATEADD(month,11, b.[MEMBER_MONTH_START_DATE])
	GROUP BY
		a.[INCURRED_YEAR_AND_MONTH]
		,a.[MEMBER_MONTH_START_DATE]
		,a.[ELIG_STATUS]
		,a.ASSN_STATUS
		,a.area, a.PCP_Name
		) as c
	left join
		(SELECT 
			[MEMBER_MONTH_START_DATE]
			,ELIG_STATUS
			,ASSN_STATUS
			,area, PCP_Name
			,sum(paid) as paid
		FROM
			mi_ATM881_ACOI.demo.A_Claim_Summary1
		GROUP BY	
			[MEMBER_MONTH_START_DATE]
			,ELIG_STATUS
			,ASSN_STATUS
			,area, PCP_Name) as d
		on c.[ELIG_STATUS] = d.[ELIG_STATUS]
		and c.[ASSN_STATUS] = d.[ASSN_STATUS]
		and c.[area] = d.[area]
		and c.[PCP_Name] = d.[PCP_Name]
		and c.[MEMBER_MONTH_START_DATE] >= d.[MEMBER_MONTH_START_DATE]
		and c.[MEMBER_MONTH_START_DATE] <= DATEADD(month,11, d.[MEMBER_MONTH_START_DATE])
GROUP BY
	c.[INCURRED_YEAR_AND_MONTH]
	,c.[MEMBER_MONTH_START_DATE]
	,c.[ELIG_STATUS]
	,c.ASSN_STATUS
	,c.area, c.PCP_Name
	,c.AGE
	,c.mems
	,c.MM
	,c.MM_risk
	,c.MM_ne
	,c.HCC_COMMUNITY_RISK


/*

(13016 row(s) affected)
Warning: Null value is eliminated by an aggregate or other SET operation.

(2123 row(s) affected)


5mins



*/