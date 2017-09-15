
IF OBJECT_ID('mi_ATM945_ACOI.dbo.tmpref_CCN_Flag', 'U') IS NOT NULL
DROP TABLE mi_ATM945_ACOI.dbo.tmpref_CCN_Flag;


CREATE TABLE mi_ATM945_ACOI.dbo.tmpref_CCN_Flag
(
CCN nvarchar(255)
)
GO

BULK
INSERT mi_ATM945_ACOI.dbo.tmpref_CCN_Flag
FROM 'E:\_ACO_Insight\03_Piedmont\_support\CCN_Flag.csv'
WITH
(
FIRSTROW=2
,FIELDTERMINATOR = ','
,ROWTERMINATOR = '\n'
)
GO


IF OBJECT_ID('mi_ATM945_ACOI.dbo.tmpref_NPI_Flag', 'U') IS NOT NULL
DROP TABLE mi_ATM945_ACOI.dbo.tmpref_NPI_Flag;


CREATE TABLE mi_ATM945_ACOI.dbo.tmpref_NPI_Flag
(
NPI nvarchar(255)
)
GO

BULK
INSERT mi_ATM945_ACOI.dbo.tmpref_NPI_Flag
FROM 'E:\_ACO_Insight\03_Piedmont\_support\NPI_Flag.csv'
WITH
(
FIRSTROW=2
,FIELDTERMINATOR = ','
,ROWTERMINATOR = '\n'
)
GO


/*

(7 row(s) affected)

(1687 row(s) affected)

*/