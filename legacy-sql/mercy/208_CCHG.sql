

IF OBJECT_ID('mi_ATM881_ACOI.dbo.A_CCHG', 'U') IS NOT NULL
DROP TABLE mi_ATM881_ACOI.dbo.A_CCHG;

SELECT
	b.MEMBER_ID as MEMBER_KEY
	,b.INCURRED_YEAR_AND_MONTH
	,b.[MEMBER_MONTH_START_DATE]
	,b.ELIG_STATUS
	,b.ASSN_STATUS
	,b.area, b.PCP_Name
	,b.[CCHG_CAT_CODE_AND_DESC] as cchg_desc
	,b.MM
	,b.MM_Risk
	,b.HCC_COMMUNITY_RISK
	,b.AGE
	,b.Gender
	,coalesce(a.paid,0) as paid
INTO
	mi_ATM881_ACOI.dbo.A_CCHG
FROM
	(select MEMBER_key, INCURRED_YEAR_AND_MONTH, sum(cast(amt_paid as float)) as paid from mi_ATM881_ACOI.dbo.dat_Claims where substring(HCG_MR_LINE,1,3) not in ('P81') group by MEMBER_key, INCURRED_YEAR_AND_MONTH) as a
	right join mi_ATM881_ACOI.dbo.A_Enrollment_Key as b
		on b.[MEMBER_key] = a.[MEMBER_key]
		and b.[INCURRED_YEAR_AND_MONTH] = a.[INCURRED_YEAR_AND_MONTH]
GO


/*

(7151874 row(s) affected)

5min

*/