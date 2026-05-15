/*
    Purchase Order Approval Date migration

    Adds nullable dbo.IVPoMst.POApprovalDate to every unit database used by the
    Purchase Order module. The column is intentionally nullable so existing POs,
    reports, exports, and old PDFs continue to work without backfill.
*/

SET NOCOUNT ON;

IF DB_ID(N'Prodoc2021') IS NOT NULL AND OBJECT_ID(N'[Prodoc2021].dbo.IVPoMst') IS NOT NULL
BEGIN
    EXEC(N'
USE [Prodoc2021];
IF COL_LENGTH(''dbo.IVPoMst'', ''POApprovalDate'') IS NULL
BEGIN
    ALTER TABLE dbo.IVPoMst
    ADD POApprovalDate DATETIME NULL;
END;
');
END;

IF DB_ID(N'ACI') IS NOT NULL AND OBJECT_ID(N'[ACI].dbo.IVPoMst') IS NOT NULL
BEGIN
    EXEC(N'
USE [ACI];
IF COL_LENGTH(''dbo.IVPoMst'', ''POApprovalDate'') IS NULL
BEGIN
    ALTER TABLE dbo.IVPoMst
    ADD POApprovalDate DATETIME NULL;
END;
');
END;

IF DB_ID(N'Prodoc2022') IS NOT NULL AND OBJECT_ID(N'[Prodoc2022].dbo.IVPoMst') IS NOT NULL
BEGIN
    EXEC(N'
USE [Prodoc2022];
IF COL_LENGTH(''dbo.IVPoMst'', ''POApprovalDate'') IS NULL
BEGIN
    ALTER TABLE dbo.IVPoMst
    ADD POApprovalDate DATETIME NULL;
END;
');
END;

IF DB_ID(N'Nayanshree') IS NOT NULL AND OBJECT_ID(N'[Nayanshree].dbo.IVPoMst') IS NOT NULL
BEGIN
    EXEC(N'
USE [Nayanshree];
IF COL_LENGTH(''dbo.IVPoMst'', ''POApprovalDate'') IS NULL
BEGIN
    ALTER TABLE dbo.IVPoMst
    ADD POApprovalDate DATETIME NULL;
END;
');
END;

IF DB_ID(N'AHLStore') IS NOT NULL AND OBJECT_ID(N'[AHLStore].dbo.IVPoMst') IS NOT NULL
BEGIN
    EXEC(N'
USE [AHLStore];
IF COL_LENGTH(''dbo.IVPoMst'', ''POApprovalDate'') IS NULL
BEGIN
    ALTER TABLE dbo.IVPoMst
    ADD POApprovalDate DATETIME NULL;
END;
');
END;

IF DB_ID(N'CancerUnitStore') IS NOT NULL AND OBJECT_ID(N'[CancerUnitStore].dbo.IVPoMst') IS NOT NULL
BEGIN
    EXEC(N'
USE [CancerUnitStore];
IF COL_LENGTH(''dbo.IVPoMst'', ''POApprovalDate'') IS NULL
BEGIN
    ALTER TABLE dbo.IVPoMst
    ADD POApprovalDate DATETIME NULL;
END;
');
END;

IF DB_ID(N'BalliaStore') IS NOT NULL AND OBJECT_ID(N'[BalliaStore].dbo.IVPoMst') IS NOT NULL
BEGIN
    EXEC(N'
USE [BalliaStore];
IF COL_LENGTH(''dbo.IVPoMst'', ''POApprovalDate'') IS NULL
BEGIN
    ALTER TABLE dbo.IVPoMst
    ADD POApprovalDate DATETIME NULL;
END;
');
END;
