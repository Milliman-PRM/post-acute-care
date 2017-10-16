

/*
IF OBJECT_ID('mi_ATM881_ACOI.demo.dat_Claims_p', 'U') IS NOT NULL
DROP TABLE mi_ATM881_ACOI.demo.dat_Claims_p;
*/

sp_rename 'mi_ATM881_ACOI.demo.dat_Claims', 'dat_Claims_p'
GO

sp_rename 'mi_ATM881_ACOI.demo.dat_Claims_p.PRVDR_OSCAR_NUM', 'PRVDR_OSCAR_NUM_2', 'COLUMN'
GO
sp_rename 'mi_ATM881_ACOI.demo.dat_Claims_p.PRVDR_OSCAR_NAME', 'PRVDR_OSCAR_NAME_2', 'COLUMN'
GO



IF OBJECT_ID('mi_ATM881_ACOI.demo.deid_PRVDR', 'U') IS NOT NULL
DROP TABLE mi_ATM881_ACOI.demo.deid_PRVDR;

SELECT
	[PRVDR_OSCAR_NUM_2]
	,'ID '+cast((ROW_NUMBER() over (order by [PRVDR_OSCAR_NUM_2])) as varchar(255)) as [PRVDR_OSCAR_NUM]
	,'Provider '+cast((ROW_NUMBER() over (order by [PRVDR_OSCAR_NUM_2])) as varchar(255)) as PRVDR_OSCAR_NAME
INTO
	mi_ATM881_ACOI.demo.deid_PRVDR
FROM
	(SELECT DISTINCT [PRVDR_OSCAR_NUM_2]
	FROM mi_ATM881_ACOI.demo.dat_Claims_p
	) as a
GO


IF OBJECT_ID('mi_ATM881_ACOI.demo.dat_Claims', 'U') IS NOT NULL
DROP TABLE mi_ATM881_ACOI.demo.dat_Claims;

SELECT
	a.*
	,[PRVDR_OSCAR_NUM]
    ,PRVDR_OSCAR_NAME
INTO
	mi_ATM881_ACOI.demo.dat_Claims
FROM
	mi_ATM881_ACOI.demo.dat_Claims_p as a
	left join mi_ATM881_ACOI.demo.deid_PRVDR as b
		on a.[PRVDR_OSCAR_NUM_2] = b.[PRVDR_OSCAR_NUM_2]



IF OBJECT_ID('MI_DEMO_ACOI.dbo.tmpref_ccn_flag', 'U') IS NOT NULL
DROP TABLE MI_DEMO_ACOI.dbo.tmpref_ccn_flag;

IF OBJECT_ID('MI_DEMO_ACOI.dbo.tmpref_npi_flag', 'U') IS NOT NULL
DROP TABLE MI_DEMO_ACOI.dbo.tmpref_npi_flag;


SELECT DISTINCT
	[PRVDR_OSCAR_NUM] as CCN
INTO
	MI_DEMO_ACOI.dbo.tmpref_ccn_flag
FROM
	mi_ATM881_ACOI.dbo.dat_Claims
WHERE
	[SERVICES_KEY] % 3 = 0


SELECT DISTINCT
	[BILL_PROV_ID] as NPI
INTO
	MI_DEMO_ACOI.dbo.tmpref_npi_flag
FROM
	MI_DEMO_ACOI.dbo.dat_Claims
WHERE
	[SERVICES_KEY] % 3 = 0

/*

Caution: Changing any part of an object name could break scripts and stored procedures.
Caution: Changing any part of an object name could break scripts and stored procedures.
Caution: Changing any part of an object name could break scripts and stored procedures.

(4327 row(s) affected)

(21326916 row(s) affected)

5min

*/


