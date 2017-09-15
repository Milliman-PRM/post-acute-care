

IF OBJECT_ID('mi_ATM881_ACOI.demo.A_tmp_CPD1', 'U') IS NOT NULL
DROP TABLE mi_ATM881_ACOI.demo.A_tmp_CPD1;

SELECT
	b.MEMBER_ID
	,b.INCURRED_YEAR_AND_MONTH
	,b.[MEMBER_MONTH_START_DATE]
	,b.ELIG_STATUS
	,b.ASSN_STATUS
	,b.area, b.PCP_Name
	,coalesce(sum((case when substring(a.HCG_MR_LINE,1,3) in ('P81') then 0 else a.amt_paid end)),0) as paid
INTO
	mi_ATM881_ACOI.demo.A_tmp_CPD1
FROM
	mi_ATM881_ACOI.demo.dat_Claims as a
	right join mi_ATM881_ACOI.demo.A_Enrollment_Key as b
		on b.[MEMBER_ID] = a.[MEMBER_ID]
		and b.[INCURRED_YEAR_AND_MONTH] = a.[INCURRED_YEAR_AND_MONTH]
GROUP BY
	b.MEMBER_ID
	,b.INCURRED_YEAR_AND_MONTH
	,b.[MEMBER_MONTH_START_DATE]
	,b.ELIG_STATUS
	,b.ASSN_STATUS
	,b.area, b.PCP_Name
GO


IF OBJECT_ID('mi_ATM881_ACOI.demo.A_tmp_CPD2', 'U') IS NOT NULL
DROP TABLE mi_ATM881_ACOI.demo.A_tmp_CPD2;

SELECT
	a.MEMBER_ID
	,a.[INCURRED_YEAR_AND_MONTH]
	,a.[MEMBER_MONTH_START_DATE]
	,b.[MEMBER_MONTH_START_DATE] as [MEMBER_MONTH_START_DATE_2]
	,b.[ELIG_STATUS]
	,b.ASSN_STATUS
	,b.area, b.PCP_Name
	,b.MM
	,b.MM_Risk
	,b.HCC_COMMUNITY_RISK
	,b.AGE
	,b.Gender
INTO
	mi_ATM881_ACOI.demo.A_tmp_CPD2
FROM
	(select *
	from 
		(SELECT DISTINCT [INCURRED_YEAR_AND_MONTH],[MEMBER_MONTH_START_DATE]
		FROM mi_ATM881_ACOI.demo.A_Enrollment_Key) as x
		cross join
		(SELECT DISTINCT MEMBER_ID, ASSN_STATUS
		FROM mi_ATM881_ACOI.demo.A_Enrollment_Key) as y) as a
	right join mi_ATM881_ACOI.demo.A_Enrollment_Key as b
		on a.MEMBER_ID = b.MEMBER_ID
		and a.ASSN_STATUS = b.ASSN_STATUS
		and a.[MEMBER_MONTH_START_DATE] >= b.[MEMBER_MONTH_START_DATE]
		and a.[MEMBER_MONTH_START_DATE] <= DATEADD(month,11, b.[MEMBER_MONTH_START_DATE])
GO


IF OBJECT_ID('mi_ATM881_ACOI.demo.A_tmp_CPD3', 'U') IS NOT NULL
DROP TABLE mi_ATM881_ACOI.demo.A_tmp_CPD3;

SELECT
	a.MEMBER_ID
	,a.[INCURRED_YEAR_AND_MONTH]
	,a.[MEMBER_MONTH_START_DATE]
	,a.[ELIG_STATUS]
	,a.ASSN_STATUS
	,a.area, a.PCP_Name
	,sum(a.MM) as MM
	,sum(a.MM_Risk)  as MM_Risk
	,sum(a.HCC_COMMUNITY_RISK) as HCC_COMMUNITY_RISK
	,sum(a.AGE) as AGE
	,max(a.gender) as Gender
	,sum(b.paid) as paid
INTO
	mi_ATM881_ACOI.demo.A_tmp_CPD3
FROM
	mi_ATM881_ACOI.demo.A_tmp_CPD2 as a
	left join mi_ATM881_ACOI.demo.A_tmp_CPD1 as b
		on a.[MEMBER_ID] = b.[MEMBER_ID]
		and a.ASSN_STATUS = b.ASSN_STATUS
		and b.[MEMBER_MONTH_START_DATE] = a.[MEMBER_MONTH_START_DATE_2]
GROUP BY
	a.MEMBER_ID
	,a.[INCURRED_YEAR_AND_MONTH]
	,a.[MEMBER_MONTH_START_DATE]
	,a.[ELIG_STATUS]
	,a.ASSN_STATUS
	,a.area, a.PCP_Name
GO


IF OBJECT_ID('mi_ATM881_ACOI.demo.A_CPD2', 'U') IS NOT NULL
DROP TABLE mi_ATM881_ACOI.demo.A_CPD2;

SELECT
	MEMBER_ID as MEMBER_KEY
	,[INCURRED_YEAR_AND_MONTH]
	,[MEMBER_MONTH_START_DATE]
	,[ELIG_STATUS]
	,ASSN_STATUS
	,area, PCP_Name
	,1 as mems
	,MM
	,MM_Risk
	,HCC_COMMUNITY_RISK
	,Age
	,Gender
	,paid
	,case
		when paid is null or paid = 0 then '$0'
		when paid/mm*12 <500 then '$1-500'
		when paid/mm*12 <1000 then '$499-1,000'
		when paid/mm*12 <1750 then '$1,001-1,750'
		when paid/mm*12 <2500 then '$1,751-2,500'
		when paid/mm*12 <3500 then '$2,501-3,500'
		when paid/mm*12 <5000 then '$3,501-5,000'
		when paid/mm*12 <7500 then '$5,001-7,500'
		when paid/mm*12 <15000 then '$7,501-15,000'
		when paid/mm*12 <25000 then '$15,001-25,000'
		when paid/mm*12 <50000 then '$25,001-50,000'
		when paid/mm*12 <100000 then '$50,001-100,000'
		when paid/mm*12 <200000 then '$100,001-200,000'
		when paid/mm*12 >= 200000 then '$200,001 +'
		else 'ERR' end as cpd_cat
INTO
	mi_ATM881_ACOI.demo.A_CPD2
FROM
	mi_ATM881_ACOI.demo.A_tmp_CPD3
GO


IF OBJECT_ID('mi_ATM881_ACOI.demo.A_tmp_CPD4', 'U') IS NOT NULL
DROP TABLE mi_ATM881_ACOI.demo.A_tmp_CPD4;

SELECT
	b.MEMBER_ID
	,b.ASSN_STATUS
	,b.INCURRED_YEAR_AND_MONTH
	,b.[MEMBER_MONTH_START_DATE]
	,x.HCG_SUBCAT1_ as HCG_SUBCAT1
	,coalesce(sum((case when substring(a.HCG_MR_LINE,1,3) in ('P81') then 0 else a.amt_paid end)),0) as paid
INTO
	mi_ATM881_ACOI.demo.A_tmp_CPD4
FROM
	mi_ATM881_ACOI.demo.dat_Claims as a
	right join mi_ATM881_ACOI.demo.A_Enrollment_Key as b
		on b.[MEMBER_ID] = a.[MEMBER_ID]
		and b.[INCURRED_YEAR_AND_MONTH] = a.[INCURRED_YEAR_AND_MONTH]
	left join ref_ACOI.dbo.ref_SvcCat_Mapping as x
		on a.HCG_MR_LINE = x.HCG_MR_LINE
GROUP BY
	b.MEMBER_ID
	,b.ASSN_STATUS
	,b.INCURRED_YEAR_AND_MONTH
	,b.[MEMBER_MONTH_START_DATE]
	,x.HCG_SUBCAT1_
GO


IF OBJECT_ID('mi_ATM881_ACOI.demo.A_tmp_CPD5', 'U') IS NOT NULL
DROP TABLE mi_ATM881_ACOI.demo.A_tmp_CPD5;

SELECT
	a.MEMBER_ID
	,a.[INCURRED_YEAR_AND_MONTH]
	,a.[MEMBER_MONTH_START_DATE]
	,a.[ELIG_STATUS]
	,a.ASSN_STATUS
	,a.area, a.PCP_Name
	,b.HCG_SUBCAT1
	,sum(a.MM) as MM
	,sum(a.MM_Risk)  as MM_Risk
	,sum(a.HCC_COMMUNITY_RISK) as HCC_COMMUNITY_RISK
	,sum(a.AGE) as AGE
	,max(a.gender) as Gender
	,sum(b.paid) as paid
INTO
	mi_ATM881_ACOI.demo.A_tmp_CPD5
FROM
	mi_ATM881_ACOI.demo.A_tmp_CPD2 as a
	left join mi_ATM881_ACOI.demo.A_tmp_CPD4 as b
		on a.[MEMBER_ID] = b.[MEMBER_ID]
		and a.ASSN_STATUS = b.ASSN_STATUS
		and b.[MEMBER_MONTH_START_DATE] = a.[MEMBER_MONTH_START_DATE_2]
GROUP BY
	a.MEMBER_ID
	,a.[INCURRED_YEAR_AND_MONTH]
	,a.[MEMBER_MONTH_START_DATE]
	,a.[ELIG_STATUS]
	,a.ASSN_STATUS
	,a.area, a.PCP_Name
	,b.HCG_SUBCAT1
GO




/*

Warning: Null value is eliminated by an aggregate or other SET operation.

(2376745 row(s) affected)

(25484282 row(s) affected)
Warning: Null value is eliminated by an aggregate or other SET operation.

(3066976 row(s) affected)

(3066976 row(s) affected)
Warning: Null value is eliminated by an aggregate or other SET operation.

(7761922 row(s) affected)
Warning: Null value is eliminated by an aggregate or other SET operation.

(26546531 row(s) affected)

30mins

*/