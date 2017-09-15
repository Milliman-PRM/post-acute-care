
IF OBJECT_ID('mi_ATM881_ACOI.dbo.tmpref_MEM_ASSN', 'U') IS NOT NULL
DROP TABLE mi_ATM881_ACOI.dbo.tmpref_MEM_ASSN;


CREATE TABLE mi_ATM881_ACOI.dbo.tmpref_MEM_ASSN
(
start_dt date
,end_dt date
,member_id nvarchar(255)
,subpop1 nvarchar(255)
)
GO

BULK
INSERT mi_ATM881_ACOI.dbo.tmpref_MEM_ASSN
FROM 'E:\_ACO_Insight\01_MercyACO\_support\Mem_ASSN.csv'
WITH
(
FIRSTROW=2
,FIELDTERMINATOR = ','
,ROWTERMINATOR = '\n'
)
GO

/*

(371909 row(s) affected)

1min

*/
