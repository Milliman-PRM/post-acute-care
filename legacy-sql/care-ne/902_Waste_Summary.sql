

IF OBJECT_ID('MI_CareNE_ACOI.dbo.A_Waste', 'U') IS NOT NULL
DROP TABLE MI_CareNE_ACOI.dbo.A_Waste;

SELECT
	b.INCURRED_YEAR_AND_MONTH
	,b.[MEMBER_MONTH_START_DATE]
	,b.ELIG_STATUS
	,b.ASSN_STATUS
	,b.area, b.PCP_Name
	,a.[WASTE_MeasureFamilyRootCause]
	,a.[WASTE_HEADLINE]
    ,a.[WASTE_DESCRIPTION]
	,a.[WASTE_DEGREE_OF_CERTAINTY]
	,a.[WASTE_COSTCOUNT_FLAG]
	,1 as episode_count
	,sum(cast(a.amt_paid as float)) as paid
INTO
	MI_CareNE_ACOI.dbo.A_Waste
FROM
	MI_CareNE_ACOI.dbo.dat_Claims as a
	left join MI_CareNE_ACOI.dbo.A_Enrollment_Key as b
		on b.[MEMBER_KEY] = a.[MEMBER_KEY]
		and b.[INCURRED_YEAR_AND_MONTH] = a.[INCURRED_YEAR_AND_MONTH]
	left join ref_ACOI.dbo.ref_SvcCat_Mapping as x
		on a.HCG_MR_LINE = x.HCG_MR_LINE
WHERE
	b.MM =1
	and substring(a.HCG_MR_LINE,1,3) not in ('P81')
GROUP BY
	b.INCURRED_YEAR_AND_MONTH
	,b.[MEMBER_MONTH_START_DATE]
	,b.ELIG_STATUS
	,b.ASSN_STATUS
	,b.area, b.PCP_Name
	,a.[WASTE_MeasureFamilyRootCause]
	,a.[WASTE_HEADLINE]
    ,a.[WASTE_DESCRIPTION]
	,a.[WASTE_DEGREE_OF_CERTAINTY]
	,a.[WASTE_COSTCOUNT_FLAG]
GO

/*

(19882 row(s) affected)


6 mins

*/
