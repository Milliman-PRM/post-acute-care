

IF OBJECT_ID('mi_ATM881_ACOI.demo.A_IP_DIS', 'U') IS NOT NULL
DROP TABLE mi_ATM881_ACOI.demo.A_IP_DIS;

SELECT
	c.INCURRED_YEAR_AND_MONTH
	, c.[MEMBER_MONTH_START_DATE]
	, c.ELIG_STATUS
	, c.ASSN_STATUS
	,c.area, c.PCP_Name
	, c.MM
	, a.[BILL_PROV_ID]
	, a.[BILL_PROV_NAME]
	, 0 as PROV_FLAG
	, a.[DRG Family]
	, a.[DRG Family List]
	, coalesce(admits_dis_ip,0) as admits_dis_ip
	, coalesce(admits_dis_snf,0) as admits_dis_snf
	, coalesce(admits_dis_hha,0) as admits_dis_hha
	, coalesce(admits_dis_irf,0) as admits_dis_irf
	, coalesce(admits_dis_death,0) as admits_dis_death
	, coalesce(med_admits,0) as med_admits
	, coalesce(surg_admits,0) as surg_admits
	, coalesce(admits,0) as admits
INTO
	mi_ATM881_ACOI.demo.A_IP_DIS
FROM
	mi_ATM881_ACOI.demo.A_Enrollment_Summary2 as c
	left join 
		(SELECT
			a.INCURRED_YEAR_AND_MONTH
			, b.ELIG_STATUS
			, b.ASSN_STATUS
			,b.area, b.PCP_Name
			, a.[PRVDR_OSCAR_NUM] as [BILL_PROV_ID]
			,ltrim(rtrim([PRVDR_OSCAR_NUM])) + ' - ' + coalesce(ltrim(rtrim([PRVDR_OSCAR_NAME])),'') as [BILL_PROV_NAME]
			, x.[DRG Family]
			, x.[DRG Family List]
			, sum(case when [DIS_STAT] = '20' then a.MR_ADMITS_CASES_RAW else 0 end) as admits_dis_death
			, sum(case when a.HCG_MR_LINE in ('I11a','I11b') then a.MR_ADMITS_CASES_RAW else 0 end) as med_admits
			, sum(case when a.HCG_MR_LINE in ('I12') then a.MR_ADMITS_CASES_RAW else 0 end) as surg_admits
			, sum(a.MR_ADMITS_CASES_RAW) as admits
		FROM
			mi_ATM881_ACOI.demo.dat_Claims as a
			right join mi_ATM881_ACOI.demo.A_Enrollment_Key as b
				on b.[MEMBER_KEY] = a.[MEMBER_KEY]
				and b.[INCURRED_YEAR_AND_MONTH] = a.[INCURRED_YEAR_AND_MONTH]
			left join ref_ACOI.dbo.ref_DRG_Family_Map as x
				on a.CLIENT_MS_DRG_CODE = REPLACE(STR(x.DRG,3),' ','0')
		WHERE
			a.HCG_MR_LINE in ('I11a','I11b','I12')
			and b.MM = 1
		GROUP BY
			a.INCURRED_YEAR_AND_MONTH
			, b.ELIG_STATUS
			, b.ASSN_STATUS
			,b.area, b.PCP_Name
			, a.[PRVDR_OSCAR_NUM]
			,ltrim(rtrim([PRVDR_OSCAR_NUM])) + ' - ' + coalesce(ltrim(rtrim([PRVDR_OSCAR_NAME])),'') 
			, x.[DRG Family]
			, x.[DRG Family List]
		) as a
		on a.[INCURRED_YEAR_AND_MONTH] = c.[INCURRED_YEAR_AND_MONTH]
		and a.[ELIG_STATUS] = c.[ELIG_STATUS]
		and a.[ASSN_STATUS] = c.[ASSN_STATUS]
		and c.[area] = a.[area]
		and c.[PCP_Name] = a.[PCP_Name] 
	left join
		(SELECT
			a.INCURRED_YEAR_AND_MONTH
			, b.ELIG_STATUS
			, b.ASSN_STATUS
			, b.area, b.PCP_Name
			, c.[PRVDR_OSCAR_NUM] as [BILL_PROV_ID]
			,ltrim(rtrim([PRVDR_OSCAR_NUM])) + ' - ' + coalesce(ltrim(rtrim([PRVDR_OSCAR_NAME])),'') as [BILL_PROV_NAME]
			, a.[DRG Family]
			, a.[DRG Family List]
			,sum(a.[freq_readm]) as admits_dis_ip
			,sum(a.[freq_irf]) as admits_dis_irf
			,sum(a.[freq_snf]) as admits_dis_snf
			,sum(a.[freq_hha]) as admits_dis_hha
		FROM
			mi_ATM881_ACOI.demo.A_tmp_PAC4 as a
			right join mi_ATM881_ACOI.demo.A_Enrollment_Key as b
				on b.[MEMBER_KEY] = a.[MEMBER_KEY]
				and b.[INCURRED_YEAR_AND_MONTH] = a.[INCURRED_YEAR_AND_MONTH]
			left join (select distinct [PRVDR_OSCAR_NUM], [PRVDR_OSCAR_NAME] from mi_ATM881_ACOI.demo.tmpref_ccn) as c
				on c.[PRVDR_OSCAR_NUM] = a.[ATT_PROV_ID]
		GROUP BY
			a.INCURRED_YEAR_AND_MONTH
			, b.ELIG_STATUS
			, b.ASSN_STATUS
			, b.area, b.PCP_Name
			, c.[PRVDR_OSCAR_NUM]
			,ltrim(rtrim([PRVDR_OSCAR_NUM])) + ' - ' + coalesce(ltrim(rtrim([PRVDR_OSCAR_NAME])),'') 
			, a.[DRG Family]
			, a.[DRG Family List]
		) as b
		on b.[INCURRED_YEAR_AND_MONTH] = c.[INCURRED_YEAR_AND_MONTH]
		and b.[ELIG_STATUS] = c.[ELIG_STATUS]
		and b.[ASSN_STATUS] = c.[ASSN_STATUS]
		and c.[area] = b.[area]
		and c.[PCP_Name] = b.[PCP_Name] 
		and b.[BILL_PROV_ID]= a.[BILL_PROV_ID]
		and	b.[BILL_PROV_NAME] = a.[BILL_PROV_NAME]
		and b.[DRG Family] = a.[DRG Family]
		and	b.[DRG Family List] = a.[DRG Family List]
		and b.[DRG Family] = a.[DRG Family]
;

/*

(60254 row(s) affected)

5min

*/
