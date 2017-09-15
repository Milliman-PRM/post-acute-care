
IF OBJECT_ID('mi_ATM945_ACOI.dbo.tmpref_MEM_ASSN', 'U') IS NOT NULL
DROP TABLE mi_ATM945_ACOI.dbo.tmpref_MEM_ASSN;


CREATE TABLE mi_ATM945_ACOI.dbo.tmpref_MEM_ASSN
(
start_dt date
,end_dt date
,member_id nvarchar(255)
,subpop1 nvarchar(255)
)
GO

BULK
INSERT mi_ATM945_ACOI.dbo.tmpref_MEM_ASSN
FROM 'E:\_ACO_Insight\03_Piedmont\_support\Mem_ASSN.txt'
WITH
(
FIRSTROW=2
,FIELDTERMINATOR = '\t'
,ROWTERMINATOR = '\n'
)
GO

/*

(34827 row(s) affected)

1min

*/
