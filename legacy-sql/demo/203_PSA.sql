

IF OBJECT_ID('mi_ATM881_ACOI.demo.A_tmp_PSA1', 'U') IS NOT NULL
DROP TABLE mi_ATM881_ACOI.demo.A_tmp_PSA1;

SELECT
	[MEMBER_KEY]
	,INCURRED_YEAR_AND_MONTH
	,[CS_CLAIM_ID_KEY]
	,[PRVDR_OSCAR_NUM] as [BILL_PROV_ID]
	,ltrim(rtrim([PRVDR_OSCAR_NUM])) + ' - ' +  ltrim(rtrim([PRVDR_OSCAR_NAME])) as [BILL_PROV_NAME]
	,0 as [PROV_FLAG]
	,MR_ADMITS_CASES_RAW
	,amt_paid
	,[PROC_CODE]
	,[CLIENT_MS_DRG_CODE]
	,[ICD_DIAG_01_PRIMARY]
	,[ICD_DIAG_02]
	,[ICD_DIAG_03]
	,[ICD_DIAG_04]
	,[ICD_DIAG_05]
	,[ICD_DIAG_06]
	,[ICD_DIAG_07]
	,[ICD_DIAG_08]
	,[ICD_DIAG_09]
	,[ICD_DIAG_10]
	,[ICD_DIAG_11]
	,[ICD_DIAG_12]
	,[ICD_DIAG_13]
	,[ICD_DIAG_14]
	,[ICD_DIAG_15]
	,[ICD_DIAG_16]
	,[ICD_DIAG_17]
	,[ICD_DIAG_18]
	,[ICD_DIAG_19]
	,[ICD_DIAG_20]
	,[ICD_DIAG_21]
	,[ICD_DIAG_22]
	,[ICD_DIAG_23]
	,[ICD_DIAG_24]
	,[ICD_DIAG_25]
	,[ICD_DIAG_26]
	,[ICD_DIAG_27]
	,[ICD_DIAG_28]
	,[ICD_DIAG_29]
	,[ICD_DIAG_30]
	,[ICD_PROC_01_PRINCIPLE]
	,[ICD_PROC_02]
	,[ICD_PROC_03]
	,[ICD_PROC_04]
	,[ICD_PROC_05]
	,[ICD_PROC_06]
	,[ICD_PROC_07]
	,[ICD_PROC_08]
	,[ICD_PROC_09]
	,[ICD_PROC_10]
	,[ICD_PROC_11]
	,[ICD_PROC_12]
	,[ICD_PROC_13]
	,[ICD_PROC_14]
	,[ICD_PROC_15]
	,[ICD_PROC_16]
	,[ICD_PROC_17]
	,[ICD_PROC_18]
	,[ICD_PROC_19]
	,[ICD_PROC_20]
	,[ICD_PROC_21]
	,[ICD_PROC_22]
	,[ICD_PROC_23]
	,[ICD_PROC_24]
	,[ICD_PROC_25]
	,[ICD_PROC_26]
	,[ICD_PROC_27]
	,[ICD_PROC_28]
	,[ICD_PROC_29]
	,[ICD_PROC_30]
into
	mi_ATM881_ACOI.demo.A_tmp_PSA1
from
	mi_ATM881_ACOI.demo.dat_Claims
where
	HCG_MR_LINE in ('I11a','I11b','I12')
GO



IF OBJECT_ID('mi_ATM881_ACOI.demo.A_tmp_PSA2', 'U') IS NOT NULL
DROP TABLE mi_ATM881_ACOI.demo.A_tmp_PSA2;

SELECT
	a.[MEMBER_KEY]
	,a.INCURRED_YEAR_AND_MONTH
	,a.[CS_CLAIM_ID_KEY]
	,a.[BILL_PROV_ID]
	,a.[BILL_PROV_NAME]
	,a.[PROV_FLAG]
	,b.psa_cat
	,b.excl
into
	mi_ATM881_ACOI.demo.A_tmp_PSA2
from
	mi_ATM881_ACOI.demo.A_tmp_PSA1 as a
	cross join ref_ACOI.dbo.ref_PSA_code_set as b
where
	1 = (case
		when b.code_type in ('HCPCS') and a.[PROC_CODE]=b.code then 1

		when b.code_type in ('MS_DRG') and a.[CLIENT_MS_DRG_CODE]=b.code then 1

		when b.code_type in ('icd9_diag','icd10_diag') and a.[ICD_DIAG_01_PRIMARY]=b.code then 1
		when b.code_type in ('icd9_diag','icd10_diag') and a.[ICD_DIAG_02]=b.code then 1
		when b.code_type in ('icd9_diag','icd10_diag') and a.[ICD_DIAG_03]=b.code then 1
		when b.code_type in ('icd9_diag','icd10_diag') and a.[ICD_DIAG_04]=b.code then 1
		when b.code_type in ('icd9_diag','icd10_diag') and a.[ICD_DIAG_05]=b.code then 1
		when b.code_type in ('icd9_diag','icd10_diag') and a.[ICD_DIAG_06]=b.code then 1
		when b.code_type in ('icd9_diag','icd10_diag') and a.[ICD_DIAG_07]=b.code then 1
		when b.code_type in ('icd9_diag','icd10_diag') and a.[ICD_DIAG_08]=b.code then 1
		when b.code_type in ('icd9_diag','icd10_diag') and a.[ICD_DIAG_09]=b.code then 1
		when b.code_type in ('icd9_diag','icd10_diag') and a.[ICD_DIAG_10]=b.code then 1
		when b.code_type in ('icd9_diag','icd10_diag') and a.[ICD_DIAG_11]=b.code then 1
		when b.code_type in ('icd9_diag','icd10_diag') and a.[ICD_DIAG_12]=b.code then 1
		when b.code_type in ('icd9_diag','icd10_diag') and a.[ICD_DIAG_13]=b.code then 1
		when b.code_type in ('icd9_diag','icd10_diag') and a.[ICD_DIAG_14]=b.code then 1
		when b.code_type in ('icd9_diag','icd10_diag') and a.[ICD_DIAG_15]=b.code then 1
		when b.code_type in ('icd9_diag','icd10_diag') and a.[ICD_DIAG_16]=b.code then 1
		when b.code_type in ('icd9_diag','icd10_diag') and a.[ICD_DIAG_17]=b.code then 1
		when b.code_type in ('icd9_diag','icd10_diag') and a.[ICD_DIAG_18]=b.code then 1
		when b.code_type in ('icd9_diag','icd10_diag') and a.[ICD_DIAG_19]=b.code then 1
		when b.code_type in ('icd9_diag','icd10_diag') and a.[ICD_DIAG_20]=b.code then 1
		when b.code_type in ('icd9_diag','icd10_diag') and a.[ICD_DIAG_21]=b.code then 1
		when b.code_type in ('icd9_diag','icd10_diag') and a.[ICD_DIAG_22]=b.code then 1
		when b.code_type in ('icd9_diag','icd10_diag') and a.[ICD_DIAG_23]=b.code then 1
		when b.code_type in ('icd9_diag','icd10_diag') and a.[ICD_DIAG_24]=b.code then 1
		when b.code_type in ('icd9_diag','icd10_diag') and a.[ICD_DIAG_25]=b.code then 1
		when b.code_type in ('icd9_diag','icd10_diag') and a.[ICD_DIAG_26]=b.code then 1
		when b.code_type in ('icd9_diag','icd10_diag') and a.[ICD_DIAG_27]=b.code then 1
		when b.code_type in ('icd9_diag','icd10_diag') and a.[ICD_DIAG_28]=b.code then 1
		when b.code_type in ('icd9_diag','icd10_diag') and a.[ICD_DIAG_29]=b.code then 1
		when b.code_type in ('icd9_diag','icd10_diag') and a.[ICD_DIAG_30]=b.code then 1

		when b.code_type in ('icd9_proc','icd10_proc') and a.[ICD_PROC_01_PRINCIPLE]=b.code then 1
		when b.code_type in ('icd9_proc','icd10_proc') and a.[ICD_PROC_02]=b.code then 1
		when b.code_type in ('icd9_proc','icd10_proc') and a.[ICD_PROC_03]=b.code then 1
		when b.code_type in ('icd9_proc','icd10_proc') and a.[ICD_PROC_04]=b.code then 1
		when b.code_type in ('icd9_proc','icd10_proc') and a.[ICD_PROC_05]=b.code then 1
		when b.code_type in ('icd9_proc','icd10_proc') and a.[ICD_PROC_06]=b.code then 1
		when b.code_type in ('icd9_proc','icd10_proc') and a.[ICD_PROC_07]=b.code then 1
		when b.code_type in ('icd9_proc','icd10_proc') and a.[ICD_PROC_08]=b.code then 1
		when b.code_type in ('icd9_proc','icd10_proc') and a.[ICD_PROC_09]=b.code then 1
		when b.code_type in ('icd9_proc','icd10_proc') and a.[ICD_PROC_10]=b.code then 1
		when b.code_type in ('icd9_proc','icd10_proc') and a.[ICD_PROC_11]=b.code then 1
		when b.code_type in ('icd9_proc','icd10_proc') and a.[ICD_PROC_12]=b.code then 1
		when b.code_type in ('icd9_proc','icd10_proc') and a.[ICD_PROC_13]=b.code then 1
		when b.code_type in ('icd9_proc','icd10_proc') and a.[ICD_PROC_14]=b.code then 1
		when b.code_type in ('icd9_proc','icd10_proc') and a.[ICD_PROC_15]=b.code then 1
		when b.code_type in ('icd9_proc','icd10_proc') and a.[ICD_PROC_16]=b.code then 1
		when b.code_type in ('icd9_proc','icd10_proc') and a.[ICD_PROC_17]=b.code then 1
		when b.code_type in ('icd9_proc','icd10_proc') and a.[ICD_PROC_18]=b.code then 1
		when b.code_type in ('icd9_proc','icd10_proc') and a.[ICD_PROC_19]=b.code then 1
		when b.code_type in ('icd9_proc','icd10_proc') and a.[ICD_PROC_20]=b.code then 1
		when b.code_type in ('icd9_proc','icd10_proc') and a.[ICD_PROC_21]=b.code then 1
		when b.code_type in ('icd9_proc','icd10_proc') and a.[ICD_PROC_22]=b.code then 1
		when b.code_type in ('icd9_proc','icd10_proc') and a.[ICD_PROC_23]=b.code then 1
		when b.code_type in ('icd9_proc','icd10_proc') and a.[ICD_PROC_24]=b.code then 1
		when b.code_type in ('icd9_proc','icd10_proc') and a.[ICD_PROC_25]=b.code then 1
		when b.code_type in ('icd9_proc','icd10_proc') and a.[ICD_PROC_26]=b.code then 1
		when b.code_type in ('icd9_proc','icd10_proc') and a.[ICD_PROC_27]=b.code then 1
		when b.code_type in ('icd9_proc','icd10_proc') and a.[ICD_PROC_28]=b.code then 1
		when b.code_type in ('icd9_proc','icd10_proc') and a.[ICD_PROC_29]=b.code then 1
		when b.code_type in ('icd9_proc','icd10_proc') and a.[ICD_PROC_30]=b.code then 1
		else 0 end)
GO



IF OBJECT_ID('mi_ATM881_ACOI.demo.A_tmp_PSA3', 'U') IS NOT NULL
DROP TABLE mi_ATM881_ACOI.demo.A_tmp_PSA3;

SELECT
	a.[MEMBER_KEY]
	,a.INCURRED_YEAR_AND_MONTH
	,a.[CS_CLAIM_ID_KEY]
	, a.[BILL_PROV_ID]
	, a.[BILL_PROV_NAME]
	, a.[PROV_FLAG]
	,MR_ADMITS_CASES_RAW
	,amt_paid
	,sum(case when psa_cat = 'Bariatric' and excl < 1 and incl > 0 then MR_ADMITS_CASES_RAW else 0 end) as PSA_Bariatric
	,sum(case when psa_cat = 'CABG_PTCA' and excl < 1 and incl > 0 then MR_ADMITS_CASES_RAW else 0 end) as PSA_CABG_PTCA
	,sum(case when psa_cat = 'TURP' and excl > 0 and incl > 0 then MR_ADMITS_CASES_RAW else 0 end) as PSA_TURP
	,sum(case when psa_cat = 'Hysterectomy ' and excl < 1 and incl > 0 then MR_ADMITS_CASES_RAW else 0 end) as PSA_Hysterectomy 
	,sum(case when psa_cat = 'Hip_Knee_Replacement' and excl > 0 and incl > 0 then MR_ADMITS_CASES_RAW else 0 end) as PSA_Hip_Knee_Replacement
	,sum(case when psa_cat = 'Spinal_Fusion' and excl < 1 and incl > 0 then MR_ADMITS_CASES_RAW else 0 end) as PSA_Spinal_Fusion
INTO
	mi_ATM881_ACOI.demo.A_tmp_PSA3
FROM
	(SELECT
		[MEMBER_KEY]
		,INCURRED_YEAR_AND_MONTH
		,[CS_CLAIM_ID_KEY]
		, [BILL_PROV_ID]
		, [BILL_PROV_NAME]
		, [PROV_FLAG]
		,sum(MR_ADMITS_CASES_RAW) as MR_ADMITS_CASES_RAW
		,sum(amt_paid) as amt_paid
	FROM
		mi_ATM881_ACOI.demo.A_tmp_PSA1
	GROUP BY
		[MEMBER_KEY]
		,INCURRED_YEAR_AND_MONTH
		,[CS_CLAIM_ID_KEY]
		, [BILL_PROV_ID]
		, [BILL_PROV_NAME]
		, [PROV_FLAG]) as a
	left join (
		SELECT
			[MEMBER_KEY]
			,INCURRED_YEAR_AND_MONTH
			,[CS_CLAIM_ID_KEY]
			,psa_cat
			,sum(excl) as excl
			,sum(1-excl) as incl 
		FROM
			mi_ATM881_ACOI.demo.A_tmp_PSA2
		GROUP BY
			[MEMBER_KEY]
			,INCURRED_YEAR_AND_MONTH
			,[CS_CLAIM_ID_KEY]
			,psa_cat) as b
		on a.MEMBER_KEY = b.MEMBER_KEY
		and a.INCURRED_YEAR_AND_MONTH = b.INCURRED_YEAR_AND_MONTH
		and a.CS_CLAIM_ID_KEY = b.CS_CLAIM_ID_KEY
GROUP BY
	a.[MEMBER_KEY]
	,a.INCURRED_YEAR_AND_MONTH
	,a.[CS_CLAIM_ID_KEY]
	, a.[BILL_PROV_ID]
	, a.[BILL_PROV_NAME]
	, a.[PROV_FLAG]
	,MR_ADMITS_CASES_RAW
	,amt_paid
GO



IF OBJECT_ID('mi_ATM881_ACOI.demo.A_PSA', 'U') IS NOT NULL
DROP TABLE mi_ATM881_ACOI.demo.A_PSA;

SELECT
	c.INCURRED_YEAR_AND_MONTH
	, c.[MEMBER_MONTH_START_DATE]
	, c.ELIG_STATUS
	, c.ASSN_STATUS
	, c.area, c.PCP_Name
	, [BILL_PROV_ID]
	, case when [BILL_PROV_NAME] is null and [BILL_PROV_ID] is not null then ltrim(rtrim([BILL_PROV_ID])) + ' - ' else [BILL_PROV_NAME] end as [BILL_PROV_NAME]
	, [PROV_FLAG]
	, c.MM
	, c.MM_risk
	, c.MM_ne
	, c.[HCC_COMMUNITY_RISK]
	, c.[AGE]
	, sum(coalesce(a.MR_ADMITS_CASES_RAW,0)) as admits
	, sum(coalesce(a.amt_paid,0)) as paid
	, sum(case
			when a.PSA_Bariatric > 0 then a.PSA_Bariatric
			when a.PSA_CABG_PTCA > 0 then a.PSA_CABG_PTCA
			when a.PSA_TURP > 0 then a.PSA_TURP
			when a.PSA_Hysterectomy > 0 then a.PSA_Hysterectomy
			when a.PSA_Hip_Knee_Replacement > 0 then a.PSA_Hip_Knee_Replacement
			when a.PSA_Spinal_Fusion > 0 then a.PSA_Spinal_Fusion
			else 0 end) as PSA_Total
	, sum(case
			when a.PSA_Bariatric > 0 then a.amt_paid
			when a.PSA_CABG_PTCA > 0 then a.amt_paid
			when a.PSA_TURP > 0 then a.amt_paid
			when a.PSA_Hysterectomy > 0 then a.amt_paid
			when a.PSA_Hip_Knee_Replacement > 0 then a.amt_paid
			when a.PSA_Spinal_Fusion > 0 then a.amt_paid
			else 0 end) as PSA_Total_Paid
	, sum(coalesce(a.PSA_Bariatric,0)) as PSA_Bariatric
	, sum(coalesce(a.PSA_CABG_PTCA,0)) as PSA_CABG_PTCA
	, sum(coalesce(a.PSA_TURP,0)) as PSA_TURP
	, sum(coalesce(a.PSA_Hysterectomy,0)) as PSA_Hysterectomy 
	, sum(coalesce(a.PSA_Hip_Knee_Replacement,0)) as PSA_Hip_Knee_Replacement
	, sum(coalesce(a.PSA_Spinal_Fusion,0)) as PSA_Spinal_Fusion
INTO
	mi_ATM881_ACOI.demo.A_PSA
FROM
	mi_ATM881_ACOI.demo.A_tmp_PSA3 as a
	right join mi_ATM881_ACOI.demo.A_Enrollment_Key as b
		on b.[MEMBER_KEY] = a.[MEMBER_KEY]
		and b.[INCURRED_YEAR_AND_MONTH] = a.[INCURRED_YEAR_AND_MONTH]
	right join mi_ATM881_ACOI.demo.A_Enrollment_Summary2 as c
		on b.[INCURRED_YEAR_AND_MONTH] = c.[INCURRED_YEAR_AND_MONTH]
		and b.[ELIG_STATUS] = c.[ELIG_STATUS]
		and  b.ASSN_STATUS = c.ASSN_STATUS
		and b.area = c.area
		and b.PCP_Name = c.PCP_Name
WHERE
	b.MM = 1
GROUP BY
	c.INCURRED_YEAR_AND_MONTH
	, c.[MEMBER_MONTH_START_DATE]
	, c.ELIG_STATUS
	, c.ASSN_STATUS
	, c.area, c.PCP_Name
	, [BILL_PROV_ID]
	, case when [BILL_PROV_NAME] is null and [BILL_PROV_ID] is not null then ltrim(rtrim([BILL_PROV_ID])) + ' - ' else [BILL_PROV_NAME] end
	, [PROV_FLAG]
	, c.MM
	, c.MM_risk
	, c.MM_ne
	, c.[HCC_COMMUNITY_RISK]
	, c.[AGE]
;



/*

(1306088 row(s) affected)

(738508 row(s) affected)

(43582 row(s) affected)

(25511 row(s) affected)


2 hours

*/
