

IF OBJECT_ID('mi_ATM881_ACOI.dbo.tmpref_DRG', 'U') IS NOT NULL
DROP TABLE mi_ATM881_ACOI.dbo.tmpref_DRG;

SELECT [DRG_CODE], [DRG_DESC]
INTO mi_ATM881_ACOI.dbo.tmpref_DRG
FROM [mi_ATM881].[dbo].[RFT_DRG]
WHERE DRG_TYPE = 'MS'
GO


IF OBJECT_ID('mi_ATM881_ACOI.dbo.A_DRG', 'U') IS NOT NULL
DROP TABLE mi_ATM881_ACOI.dbo.A_DRG;

SELECT
    a.INCURRED_YEAR_AND_MONTH
	, b.ELIG_STATUS
	, b.ASSN_STATUS
	, b.area, b.PCP_Name
	, a.CLIENT_MS_DRG_CODE as DRG_CODE
	, sum(cast(a.MR_ADMITS_CASES_RAW as float)) as admits
    , sum(cast(a.mr_units_days_raw as float)) as days
	, sum(cast(a.amt_paid as float)) as paid
INTO
	mi_ATM881_ACOI.dbo.A_DRG
FROM
	mi_ATM881_ACOI.dbo.dat_Claims as a
	right join mi_ATM881_ACOI.dbo.A_Enrollment_Key as b
		on b.[MEMBER_KEY] = a.[MEMBER_KEY]
		and b.[INCURRED_YEAR_AND_MONTH] = a.[INCURRED_YEAR_AND_MONTH]
WHERE
	b.MM = 1
	and a.CLIENT_MS_DRG_CODE is not null
	and a.CLIENT_MS_DRG_CODE != 'SNF'
GROUP BY
	a.INCURRED_YEAR_AND_MONTH
	, b.ELIG_STATUS
	, b.ASSN_STATUS
	,b.area, b.PCP_Name
	, a.CLIENT_MS_DRG_CODE 
GO


IF OBJECT_ID('mi_ATM881_ACOI.dbo.A_DRG_wBnch', 'U') IS NOT NULL
DROP TABLE mi_ATM881_ACOI.dbo.A_DRG_wBnch;

SELECT
	c.INCURRED_YEAR_AND_MONTH
	, c.[MEMBER_MONTH_START_DATE]
	, c.ELIG_STATUS
	, c.ASSN_STATUS
	, c.area, c.PCP_Name
	, c.MM
	, COALESCE(a.[DRG_CODE],b.[DRG]) as DRG_CODE
	, x.DRG_DESC
	, a.admits
    , a.days
	, a.paid
	, b.LM_util as cases_LM
	, b.WM_util as cases_WM
	, b.LM_days as utils_LM
	, b.WM_days as utils_WM
INTO
	mi_ATM881_ACOI.dbo.A_DRG_wBnch
FROM
	mi_ATM881_ACOI.dbo.A_DRG as a
	full join mi_ATM881_ACOI.dbo.A_Bnchmrk_DRG as b
		on b.[INCURRED_YEAR_AND_MONTH] = a.[INCURRED_YEAR_AND_MONTH]
		and b.[ELIG_STATUS] = a.[ELIG_STATUS]
		and b.[ASSN_STATUS] = a.[ASSN_STATUS]
		and b.area = a.area
		and b.PCP_Name = a.PCP_Name
		and b.[DRG] = a.[DRG_CODE]
	left join mi_ATM881_ACOI.dbo.tmpref_DRG as x
		on x.DRG_CODE = COALESCE(a.[DRG_CODE],b.[DRG])
	right join mi_ATM881_ACOI.dbo.A_Enrollment_Summary2 as c
		on c.[INCURRED_YEAR_AND_MONTH] =  COALESCE(a.[INCURRED_YEAR_AND_MONTH],b.[INCURRED_YEAR_AND_MONTH])
		and c.[ELIG_STATUS] = COALESCE(a.[ELIG_STATUS],b.[ELIG_STATUS])
		and c.[ASSN_STATUS] = COALESCE(a.[ASSN_STATUS],b.[ASSN_STATUS])
		and c.[PCP_Name] = COALESCE(a.[PCP_Name],b.[PCP_Name])
		and c.[area] = COALESCE(a.[area],b.[area])
GO


IF OBJECT_ID('mi_ATM881_ACOI.dbo.A_DRG_Fam_wBnch', 'U') IS NOT NULL
DROP TABLE mi_ATM881_ACOI.dbo.A_DRG_Fam_wBnch;

SELECT
	a.*
	, b.[DRG Family]
	, b.[DRG Family List]
INTO
	mi_ATM881_ACOI.dbo.A_DRG_Fam_wBnch
FROM
	mi_ATM881_ACOI.dbo.A_DRG_wBnch as a
	left join ref_ACOI.dbo.ref_DRG_Family_Map as b
			on a.DRG_CODE = REPLACE(STR(b.DRG,3),' ','0')
GO

/*

(766 row(s) affected)

(101562 row(s) affected)

(1615160 row(s) affected)

(1615160 row(s) affected)


5mins

*/