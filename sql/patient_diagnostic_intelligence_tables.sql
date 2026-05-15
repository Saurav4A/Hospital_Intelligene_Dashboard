/* Patient Diagnostic Intelligence optional HID-side tables and safe helper indexes.
   Review in the target unit/HID database before applying. No HMIS transaction table is altered destructively. */

IF OBJECT_ID('dbo.PatientDiagnostic_Module_Settings', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.PatientDiagnostic_Module_Settings (
        SettingID INT IDENTITY(1,1) PRIMARY KEY,
        SettingKey NVARCHAR(100) NOT NULL,
        SettingValue NVARCHAR(MAX) NULL,
        IsActive BIT NOT NULL CONSTRAINT DF_PDI_Settings_IsActive DEFAULT (1),
        CreatedAt DATETIME NOT NULL CONSTRAINT DF_PDI_Settings_CreatedAt DEFAULT (GETDATE()),
        UpdatedAt DATETIME NULL
    );
END;

IF OBJECT_ID('dbo.PatientDiagnostic_Followup_Rules', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.PatientDiagnostic_Followup_Rules (
        RuleID INT IDENTITY(1,1) PRIMARY KEY,
        RuleName NVARCHAR(200) NOT NULL,
        TestID INT NULL,
        ServiceID INT NULL,
        ParameterID INT NULL,
        FollowupGapDays INT NOT NULL CONSTRAINT DF_PDI_Rules_Gap DEFAULT (90),
        AbnormalOnly BIT NOT NULL CONSTRAINT DF_PDI_Rules_AbnormalOnly DEFAULT (1),
        IsActive BIT NOT NULL CONSTRAINT DF_PDI_Rules_IsActive DEFAULT (1),
        CreatedBy NVARCHAR(100) NULL,
        CreatedAt DATETIME NOT NULL CONSTRAINT DF_PDI_Rules_CreatedAt DEFAULT (GETDATE()),
        UpdatedAt DATETIME NULL
    );
END;

IF OBJECT_ID('dbo.PatientDiagnostic_Campaign_Candidates', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.PatientDiagnostic_Campaign_Candidates (
        CandidateID INT IDENTITY(1,1) PRIMARY KEY,
        PatientID INT NOT NULL,
        VisitID INT NULL,
        TestID INT NULL,
        ParameterID INT NULL,
        LastResult NVARCHAR(200) NULL,
        LastResultStatus NVARCHAR(50) NULL,
        LastTestDate DATETIME NULL,
        SuggestedReason NVARCHAR(500) NULL,
        RuleID INT NULL,
        CandidateStatus NVARCHAR(50) NOT NULL CONSTRAINT DF_PDI_Candidate_Status DEFAULT ('NEW'),
        CreatedAt DATETIME NOT NULL CONSTRAINT DF_PDI_Candidate_CreatedAt DEFAULT (GETDATE()),
        UpdatedAt DATETIME NULL
    );
END;

IF OBJECT_ID('dbo.PatientDiagnostic_Report_Audit', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.PatientDiagnostic_Report_Audit (
        AuditID INT IDENTITY(1,1) PRIMARY KEY,
        ReportType NVARCHAR(100) NULL,
        ExportType NVARCHAR(20) NULL,
        GeneratedBy NVARCHAR(100) NULL,
        GeneratedAt DATETIME NOT NULL CONSTRAINT DF_PDI_Audit_GeneratedAt DEFAULT (GETDATE()),
        FromDate DATE NULL,
        ToDate DATE NULL,
        SelectedTests NVARCHAR(MAX) NULL,
        FilterJson NVARCHAR(MAX) NULL,
        FileName NVARCHAR(300) NULL,
        RowCount INT NULL,
        Status NVARCHAR(50) NOT NULL CONSTRAINT DF_PDI_Audit_Status DEFAULT ('SUCCESS'),
        ErrorMessage NVARCHAR(MAX) NULL
    );
END;

IF OBJECT_ID('dbo.LABTestResultup', 'U') IS NOT NULL
AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID('dbo.LABTestResultup') AND name = 'IX_PDI_LABTestResultup_Patient_Test_ResultAuth')
BEGIN
    CREATE NONCLUSTERED INDEX IX_PDI_LABTestResultup_Patient_Test_ResultAuth
    ON dbo.LABTestResultup (PatientID, PatientVisitID, TestID, ParamID, ResultAuthDtTime)
    INCLUDE (Result, SampleID, OrderID, OrderDtlID, AbnormalFlag, ResultAuthFlag);
END;

IF OBJECT_ID('dbo.LABTestResultup', 'U') IS NOT NULL
AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID('dbo.LABTestResultup') AND name = 'IX_PDI_LABTestResultup_Test_Order')
BEGIN
    CREATE NONCLUSTERED INDEX IX_PDI_LABTestResultup_Test_Order
    ON dbo.LABTestResultup (TestID, ParamID, SampleID, OrderID, OrderDtlID);
END;

IF OBJECT_ID('dbo.LABSampleup', 'U') IS NOT NULL
AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID('dbo.LABSampleup') AND name = 'IX_PDI_LABSampleup_Patient_Visit_Order')
BEGIN
    CREATE NONCLUSTERED INDEX IX_PDI_LABSampleup_Patient_Visit_Order
    ON dbo.LABSampleup (SampleID, PatientID, VisitID, OrderID, SmpGenDateTime);
END;

IF OBJECT_ID('dbo.LABSampleDtlup', 'U') IS NOT NULL
AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID('dbo.LABSampleDtlup') AND name = 'IX_PDI_LABSampleDtlup_Sample_Test_OrderDtl')
BEGIN
    CREATE NONCLUSTERED INDEX IX_PDI_LABSampleDtlup_Sample_Test_OrderDtl
    ON dbo.LABSampleDtlup (SmpID, TestID, OrderDtlID);
END;
