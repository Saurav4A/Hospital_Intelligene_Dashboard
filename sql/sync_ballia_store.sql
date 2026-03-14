USE [BalliaStore];
GO

IF COL_LENGTH('dbo.IVItem', 'TechnicalSpecs') IS NULL
BEGIN
    ALTER TABLE dbo.IVItem ADD TechnicalSpecs NVARCHAR(MAX) NULL;
END;
GO

IF COL_LENGTH('dbo.IVPoMst', 'Dept') IS NULL
BEGIN
    ALTER TABLE dbo.IVPoMst ADD Dept NCHAR(10) NULL;
END;
GO

IF COL_LENGTH('dbo.IVPoMst', 'PurchasingDeptId') IS NULL
BEGIN
    ALTER TABLE dbo.IVPoMst ADD PurchasingDeptId INT NULL;
END;
GO

IF COL_LENGTH('dbo.IVPoMst', 'SpecialNotes') IS NULL
BEGIN
    ALTER TABLE dbo.IVPoMst ADD SpecialNotes NVARCHAR(1000) NULL;
END;
GO

IF COL_LENGTH('dbo.IVPoMst', 'SeniorApprovalAuthorityName') IS NULL
BEGIN
    ALTER TABLE dbo.IVPoMst ADD SeniorApprovalAuthorityName NVARCHAR(160) NULL;
END;
GO

IF COL_LENGTH('dbo.IVPoMst', 'SeniorApprovalAuthorityDesignation') IS NULL
BEGIN
    ALTER TABLE dbo.IVPoMst ADD SeniorApprovalAuthorityDesignation NVARCHAR(120) NULL;
END;
GO

CREATE OR ALTER PROC dbo.usp_GetLatestItemConsumptionLast30Days
AS
BEGIN
    SET NOCOUNT ON;

    SELECT
          s.ItemID
        , i.Name AS ItemName
        , SUM(s.Qty) AS TotalQtyConsumedLast30Days
    FROM dbo.IVStockRegister AS s
    INNER JOIN dbo.IVItem AS i
        ON i.ID = s.ItemID
    WHERE
        s.DocType IN ('PIS', 'ISD', 'ROT')
        AND s.DocDate >= DATEADD(DAY, -30, CONVERT(date, GETDATE()))
    GROUP BY
        s.ItemID,
        i.Name
    ORDER BY
        i.Name;
END;
GO
