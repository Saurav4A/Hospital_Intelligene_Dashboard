CREATE OR ALTER PROCEDURE dbo.usp_CountNewPharmacyIndent
AS
DECLARE @Back2Day AS DATETIME
SET @Back2Day = DATEADD(day, -2, GETDATE())
BEGIN
    SELECT COUNT(dbo.IVPatientIndentMst.IndentID)
    FROM dbo.IVPatientIndentMst
    LEFT OUTER JOIN dbo.Visit
        ON dbo.IVPatientIndentMst.VisitID = dbo.Visit.Visit_ID
    LEFT OUTER JOIN dbo.IvPatientIssueMst
        ON dbo.IVPatientIndentMst.IndentID = dbo.IvPatientIssueMst.IndentId
    WHERE ISNULL(dbo.IvPatientIssueMst.IssueId, 0) = 0
      AND dbo.Visit.DischargeType <> 2
      AND dbo.IVPatientIndentMst.IndentDate > @Back2Day
END
GO

CREATE OR ALTER PROC dbo.usp_GetLatestItemConsumptionLast30Days
AS
BEGIN
    SET NOCOUNT ON;

    SELECT
        s.ItemID,
        i.Name AS ItemName,
        SUM(s.Qty) AS TotalQtyConsumedLast30Days
    FROM dbo.IVStockRegister AS s
    INNER JOIN dbo.IVItem AS i
        ON i.ID = s.ItemID
    WHERE s.DocType IN ('PIS', 'ISD', 'ROT', '')
      AND s.DocDate >= DATEADD(DAY, -30, CONVERT(date, GETDATE()))
    GROUP BY
        s.ItemID,
        i.Name
    ORDER BY
        i.Name;
END;
GO
