
IF OBJECT_ID('mi_ATM881_ACOI.dbo.tmpref_CCN_Flag', 'U') IS NOT NULL
DROP TABLE mi_ATM881_ACOI.dbo.tmpref_CCN_Flag;


CREATE TABLE mi_ATM881_ACOI.dbo.tmpref_CCN_Flag
(
CCN nvarchar(255)
)
GO

BULK
INSERT mi_ATM881_ACOI.dbo.tmpref_CCN_Flag
FROM 'E:\_ACO_Insight\01_MercyACO\_support\CCN_Flag.csv'
WITH
(
FIRSTROW=2
,FIELDTERMINATOR = ','
,ROWTERMINATOR = '\n'
)
GO


/*

(404 row(s) affected)

*/