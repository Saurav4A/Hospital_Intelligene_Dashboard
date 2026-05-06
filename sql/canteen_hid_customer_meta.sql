/*
Run this script in each legacy canteen database:
  1. EmpAtten20   (AHL)
  2. CanteenACI   (ACI)

Purpose:
  Add a non-breaking HID overlay table for permanent-customer ledger metadata.
  Legacy reports and legacy procedures remain unchanged.
*/

IF OBJECT_ID(N'dbo.HID_CanteenCustomerMeta', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.HID_CanteenCustomerMeta (
        TmpPatientID INT NOT NULL PRIMARY KEY,
        LedgerCode VARCHAR(60) NULL,
        LedgerName VARCHAR(300) NULL,
        DepartmentName VARCHAR(120) NULL,
        CardReference VARCHAR(60) NULL,
        WorkflowContext VARCHAR(60) NULL,
        ReferenceLabel VARCHAR(120) NULL,
        MobilePrimary VARCHAR(20) NULL,
        MobileSecondary VARCHAR(20) NULL,
        Notes VARCHAR(500) NULL,
        IsActive BIT NOT NULL CONSTRAINT DF_HID_CanteenCustomerMeta_IsActive DEFAULT ((1)),
        CreatedOn DATETIME NOT NULL CONSTRAINT DF_HID_CanteenCustomerMeta_CreatedOn DEFAULT (GETDATE()),
        UpdatedOn DATETIME NOT NULL CONSTRAINT DF_HID_CanteenCustomerMeta_UpdatedOn DEFAULT (GETDATE())
    );
END
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'dbo.HID_CanteenCustomerMeta')
      AND name = N'IX_HID_CanteenCustomerMeta_LedgerCode'
)
BEGIN
    CREATE NONCLUSTERED INDEX IX_HID_CanteenCustomerMeta_LedgerCode
    ON dbo.HID_CanteenCustomerMeta (LedgerCode);
END
GO
