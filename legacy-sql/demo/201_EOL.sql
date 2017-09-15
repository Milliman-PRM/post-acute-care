
IF OBJECT_ID('mi_ATM881_ACOI.demo.A_tmp_EOL0a', 'U') IS NOT NULL
DROP TABLE mi_ATM881_ACOI.demo.A_tmp_EOL0a;

SELECT DISTINCT
	MEMBER_KEY
	,coalesce([ADM_DATE],FROM_DATE) as [ADM_DATE]
	,coalesce([DIS_DATE],FROM_DATE) as [DIS_DATE]
INTO
	mi_ATM881_ACOI.demo.A_tmp_EOL0a
FROM
	mi_ATM881_ACOI.demo.dat_claims
WHERE
	HCG_MR_LINE = 'P82b'


IF OBJECT_ID('mi_ATM881_ACOI.demo.A_tmp_EOL0b', 'U') IS NOT NULL
DROP TABLE mi_ATM881_ACOI.demo.A_tmp_EOL0b;

SELECT DISTINCT
	MEMBER_KEY
	,FROM_DATE
INTO
	mi_ATM881_ACOI.demo.A_tmp_EOL0b
FROM
	mi_ATM881_ACOI.demo.dat_claims
WHERE
	HCG_MR_LINE = 'P82b'
	

IF OBJECT_ID('mi_ATM881_ACOI.demo.A_tmp_EOL0c', 'U') IS NOT NULL
DROP TABLE mi_ATM881_ACOI.demo.A_tmp_EOL0c;

SELECT
	a.MEMBER_KEY
	,[ADM_DATE]
    ,[DIS_DATE]
	,case when count(distinct FROM_DATE) > coalesce(datediff(day,[ADM_DATE],[DIS_DATE])+1,0) then count(distinct FROM_DATE) else coalesce(datediff(day,[ADM_DATE],[DIS_DATE])+1,0) end as hospice_days
INTO
	mi_ATM881_ACOI.demo.A_tmp_EOL0c
FROM
	mi_ATM881_ACOI.demo.A_tmp_EOL0a as a
	left join mi_ATM881_ACOI.demo.A_tmp_EOL0b as b
		on b.MEMBER_KEY = a.MEMBER_KEY
		and b.FROM_DATE >= a.[ADM_DATE]
		and b.FROM_DATE <= a.[DIS_DATE]
GROUP BY
	a.MEMBER_KEY
	,[ADM_DATE]
    ,[DIS_DATE]
GO


IF OBJECT_ID('mi_ATM881_ACOI.demo.A_tmp_EOL0d', 'U') IS NOT NULL
DROP TABLE mi_ATM881_ACOI.demo.A_tmp_EOL0d;

SELECT
	MEMBER_KEY
	,sum(hospice_days) as hospice_days
INTO
	mi_ATM881_ACOI.demo.A_tmp_EOL0d
FROM
	mi_ATM881_ACOI.demo.A_tmp_EOL0c
GROUP BY
	MEMBER_KEY
GO


IF OBJECT_ID('mi_ATM881_ACOI.demo.A_tmp_EOL1', 'U') IS NOT NULL
DROP TABLE mi_ATM881_ACOI.demo.A_tmp_EOL1;

SELECT
	a.MEMBER_KEY
	,max(b.[MEM_DOD]) as [MEM_DOD]
INTO
	mi_ATM881_ACOI.demo.A_tmp_EOL1
FROM
	mi_ATM881_ACOI.demo.dat_MEMBMTHS as a
	left join mi_ATM881_ACOI.dbo.tmpref_STAGING_MEMBER as b
		on a.[MEMBER_ID] = b.[MEMBER_ID]
GROUP BY
	a.MEMBER_KEY
GO


IF OBJECT_ID('mi_ATM881_ACOI.demo.A_tmp_EOL2', 'U') IS NOT NULL
DROP TABLE mi_ATM881_ACOI.demo.A_tmp_EOL2;

SELECT
	a.INCURRED_YEAR_AND_MONTH
	,a.[MEMBER_MONTH_START_DATE]
	,a.ELIG_STATUS
	,a.ASSN_STATUS
	,a.area, a.PCP_Name
	,a.MEMBER_KEY
	,coalesce(b.[MEM_DOD],c.death_dis_date) as MEM_DOD
	,case when coalesce(b.[MEM_DOD],c.death_dis_date) < DATEADD(month,1, a.[MEMBER_MONTH_START_DATE]) and coalesce(b.[MEM_DOD],c.death_dis_date) >= a.[MEMBER_MONTH_START_DATE] then 1 else 0 end as died
	,case when coalesce(b.[MEM_DOD],c.death_dis_date) < DATEADD(month,1, a.[MEMBER_MONTH_START_DATE]) and coalesce(b.[MEM_DOD],c.death_dis_date) >= a.[MEMBER_MONTH_START_DATE] then month(coalesce(b.[MEM_DOD],c.death_dis_date)) else 0 end as died_m
	,coalesce(c.death_in_hosp,0) as died_in_hosp
	,case when coalesce(b.[MEM_DOD],c.death_dis_date) < DATEADD(month,1, a.[MEMBER_MONTH_START_DATE]) and coalesce(b.[MEM_DOD],c.death_dis_date) >= a.[MEMBER_MONTH_START_DATE] then coalesce(e.hospice_days,0) else 0 end as hospice_days
INTO
	mi_ATM881_ACOI.demo.A_tmp_EOL2
FROM
	mi_ATM881_ACOI.demo.A_Enrollment_Key as a
	left join mi_ATM881_ACOI.demo.A_tmp_EOL1 as b
		on a.MEMBER_KEY = b.MEMBER_KEY
	left join (
		SELECT
			INCURRED_YEAR_AND_MONTH
			,MEMBER_KEY
			,max(case when substring(HCG_MR_LINE,1,1)='I' and HCG_MR_LINE != 'I31' then 1 else 0 end) as death_in_hosp
			,max(TO_DATE) as death_dis_date
		FROM
			mi_ATM881_ACOI.demo.dat_claims
		WHERE
			DIS_STAT = '20'
		GROUP BY
			INCURRED_YEAR_AND_MONTH
			,MEMBER_KEY		
		) as c
			on a.MEMBER_KEY = c.MEMBER_KEY
			and a.INCURRED_YEAR_AND_MONTH = c.INCURRED_YEAR_AND_MONTH
	left join mi_ATM881_ACOI.demo.A_tmp_EOL0d as e
		on a.MEMBER_KEY = e.MEMBER_KEY
GO

IF OBJECT_ID('mi_ATM881_ACOI.demo.A_tmp_EOL3', 'U') IS NOT NULL
DROP TABLE mi_ATM881_ACOI.demo.A_tmp_EOL3;

SELECT
	a.MEMBER_KEY
	,b.INCURRED_YEAR_AND_MONTH
	,sum(a.[AMT_PAID]) as paid
	,sum(case when HCG_MR_LINE in ('O11a','O11b') then a.[AMT_PAID] else 0 end) as er_paid
INTO
	mi_ATM881_ACOI.demo.A_tmp_EOL3
FROM
	mi_ATM881_ACOI.demo.dat_Claims as a
	right join (
		SELECT DISTINCT
			MEMBER_KEY
			,INCURRED_YEAR_AND_MONTH
			,MEM_DOD
		FROM
			mi_ATM881_ACOI.demo.A_tmp_EOL2
		WHERE
			died = 1	
	) as b
		on b.MEMBER_KEY = a.MEMBER_KEY
		and b.[MEM_DOD] >= a.FROM_DATE
		and b.[MEM_DOD] <= DATEADD(day,30,a.FROM_DATE)
GROUP BY
	a.MEMBER_KEY
	,b.INCURRED_YEAR_AND_MONTH
GO


IF OBJECT_ID('mi_ATM881_ACOI.demo.A_EOL', 'U') IS NOT NULL
DROP TABLE mi_ATM881_ACOI.demo.A_EOL;

SELECT
	c.[INCURRED_YEAR_AND_MONTH]
	,c.[MEMBER_MONTH_START_DATE]
    ,c.[ELIG_STATUS]
    ,c.[ASSN_STATUS]
    ,c.[area]
    ,c.[PCP_Name]
	,c.MM
	,sum(a.died) as died
	,sum(a.died_m) as died_m
	,sum(a.died_in_hosp) as died_in_hosp
	,sum(case when a.hospice_days > 0 then 1 else 0 end) as hospice
	,sum(case when a.hospice_days > 3 then 1 else 0 end) as hospice3
	,sum(case when b.er_paid > 0 then 1 else 0 end) as er_30priordeath
	,sum(coalesce(b.paid,0)) as paid_30priordeath
INTO
	mi_ATM881_ACOI.demo.A_EOL
FROM
	mi_ATM881_ACOI.demo.A_tmp_EOL2 as a
	left join mi_ATM881_ACOI.demo.A_tmp_EOL3 as b
		on b.MEMBER_KEY = a.MEMBER_KEY
		and b.INCURRED_YEAR_AND_MONTH = a.INCURRED_YEAR_AND_MONTH
	right join mi_ATM881_ACOI.demo.A_Enrollment_Summary2 as c
		on a.[INCURRED_YEAR_AND_MONTH] = c.[INCURRED_YEAR_AND_MONTH]
		and a.[ELIG_STATUS] = c.[ELIG_STATUS]
		and a.[ASSN_STATUS] = c.[ASSN_STATUS]
		and a.[PCP_Name] = c.[PCP_Name]
		and a.[area] = c.[area]
GROUP BY
	c.[INCURRED_YEAR_AND_MONTH]
	,c.[MEMBER_MONTH_START_DATE]
    ,c.[ELIG_STATUS]
    ,c.[ASSN_STATUS]
    ,c.[area]
    ,c.[PCP_Name]
	,c.MM
GO


/*


(95817 row(s) affected)

(95975 row(s) affected)

(95817 row(s) affected)

(2662 row(s) affected)
Warning: Null value is eliminated by an aggregate or other SET operation.

(39687 row(s) affected)

(2376745 row(s) affected)
Warning: Null value is eliminated by an aggregate or other SET operation.

(3779 row(s) affected)

(2109 row(s) affected)


10 mins

*/