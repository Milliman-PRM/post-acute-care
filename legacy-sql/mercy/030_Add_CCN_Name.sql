

IF OBJECT_ID('mi_ATM881_ACOI.dbo.dat_Claims_p', 'U') IS NOT NULL
DROP TABLE mi_ATM881_ACOI.dbo.dat_Claims_p;

use mi_ATM881_ACOI
go
exec sp_rename 'dbo.dat_Claims', 'dat_Claims_p';  



IF OBJECT_ID('mi_ATM881_ACOI.dbo.tmpref_ccn_by_claim_id', 'U') IS NOT NULL
DROP TABLE mi_ATM881_ACOI.dbo.tmpref_ccn_by_claim_id;

SELECT
	[CUR_CLM_UNIQ_ID]
    ,max([PRVDR_OSCAR_NUM]) as [_CLAIM_UDF_01_]
INTO
	mi_ATM881_ACOI.dbo.tmpref_ccn_by_claim_id
FROM
	mi_ATM881_SOURCEDATA.dbo.CCLF1_CLAIMHEADER_PASSED
GROUP BY
	[CUR_CLM_UNIQ_ID]

	

IF OBJECT_ID('mi_ATM881_ACOI.dbo.tmpref_ccn', 'U') IS NOT NULL
DROP TABLE mi_ATM881_ACOI.dbo.tmpref_ccn;

select 
	b.[CUR_CLM_UNIQ_ID]
	, b.[_CLAIM_UDF_01_] as [PRVDR_OSCAR_NUM]
	, coalesce(
		z0.[FAC_NAME]
		,'Psychiatric unit of '+z1.[FAC_NAME]
		,'Rehabilitation unit of '+z2.[FAC_NAME]
		,'Swing bed of '+z3.[FAC_NAME]
		,'Rehabilitation unit of '+z4.[FAC_NAME]
		,'Psychiatric unit of '+z5.[FAC_NAME]
		,'Swing-bed of '+z6.[FAC_NAME]
		) as [PRVDR_OSCAR_NAME]
INTO
	mi_ATM881_ACOI.dbo.tmpref_ccn
FROM
	mi_ATM881_ACOI.dbo.tmpref_ccn_by_claim_id as b
	left join ref_ACOI.dbo.ref_ccn as z0
		on b.[_CLAIM_UDF_01_] = z0.CCN
	left join ref_ACOI.dbo.ref_ccn as z1
		on substring(b.[_CLAIM_UDF_01_],3,1) = 'S'
		and substring(b.[_CLAIM_UDF_01_],1,2)+'0'+substring(b.[_CLAIM_UDF_01_],4,3) = z1.CCN
	left join ref_ACOI.dbo.ref_ccn as z2
		on substring(b.[_CLAIM_UDF_01_],3,1) = 'T'
		and substring(b.[_CLAIM_UDF_01_],1,2)+'0'+substring(b.[_CLAIM_UDF_01_],4,3) = z2.CCN
	left join ref_ACOI.dbo.ref_ccn as z3
		on substring(b.[_CLAIM_UDF_01_],3,1) = 'Z'
		and substring(b.[_CLAIM_UDF_01_],1,2)+'1'+substring(b.[_CLAIM_UDF_01_],4,3) = z3.CCN
	left join ref_ACOI.dbo.ref_ccn as z4
		on substring(b.[_CLAIM_UDF_01_],3,1) = 'R'
		and substring(b.[_CLAIM_UDF_01_],1,2)+'1'+substring(b.[_CLAIM_UDF_01_],4,3) = z4.CCN
	left join ref_ACOI.dbo.ref_ccn as z5
		on substring(b.[_CLAIM_UDF_01_],3,1) = 'M'
		and substring(b.[_CLAIM_UDF_01_],1,2)+'1'+substring(b.[_CLAIM_UDF_01_],4,3) = z5.CCN
	left join ref_ACOI.dbo.ref_ccn as z6
		on substring(b.[_CLAIM_UDF_01_],3,1) = 'U'
		and substring(b.[_CLAIM_UDF_01_],1,2)+'0'+substring(b.[_CLAIM_UDF_01_],4,3) = z6.CCN



SELECT
	a.*
	, b.PRVDR_OSCAR_NUM
	, b.PRVDR_OSCAR_NAME
INTO
	mi_ATM881_ACOI.dbo.dat_Claims
FROM
	mi_ATM881_ACOI.dbo.dat_Claims_p as a
	left join mi_ATM881_ACOI.dbo.tmpref_ccn as b
		on a.[CLAIM_ID] = b.[CUR_CLM_UNIQ_ID]
GO



/*
Caution: Changing any part of an object name could break scripts and stored procedures.

(3830397 row(s) affected)

(3830397 row(s) affected)

(64016119 row(s) affected)

10 min

*/


USE mi_ATM881_ACOI
GO
IF (OBJECT_ID('mi_ATM881_ACOI.dbo.dat_Claims_p', 'U') IS NOT NULL
	AND (SELECT SUM (row_count) FROM sys.dm_db_partition_stats WHERE object_id=OBJECT_ID('dat_Claims_p') AND (index_id=0 or index_id=1)) = (SELECT SUM (row_count) FROM sys.dm_db_partition_stats WHERE object_id=OBJECT_ID('dat_Claims') AND (index_id=0 or index_id=1))
	AND (SELECT SUM (row_count) FROM sys.dm_db_partition_stats WHERE object_id=OBJECT_ID('dat_Claims') AND (index_id=0 or index_id=1)) <> 0
	)
DROP TABLE mi_ATM881_ACOI.dbo.dat_Claims_p;

