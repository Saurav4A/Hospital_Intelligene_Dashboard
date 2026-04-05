SET NOCOUNT ON;

IF OBJECT_ID(N'dbo.Visit_Duplicate', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.Visit_Duplicate
    (
        visitId INT IDENTITY(1,1) NOT NULL,
        patientId INT NULL,
        visitDate DATETIME NULL,
        dischargeDate DATETIME NULL,
        patientSubTypeId INT NULL,
        docId INT NULL,
        dischargeTypeId INT NULL,
        visitTypeId INT NULL,
        visitStatus NVARCHAR(5) NULL,
        insertedBy INT NULL,
        insertedOn DATETIME NULL,
        CBillId INT NULL,
        patientTypeId INT NULL,
        payType INT NULL,
        sourceVisitId INT NULL,
        sourceVisitNo VARCHAR(50) NULL,
        sourceAdmissionNo VARCHAR(50) NULL,
        referralNo VARCHAR(100) NULL,
        referralDate DATE NULL,
        payerTpaName NVARCHAR(200) NULL,
        verifiedByUserName NVARCHAR(100) NULL,
        settlementMode NVARCHAR(30) NULL,
        updatedBy INT NULL,
        updatedOn DATETIME NULL,
        CONSTRAINT PK_Visit_Duplicate PRIMARY KEY CLUSTERED (visitId)
    );
END;

IF OBJECT_ID(N'dbo.Visit_Duplicate', N'U') IS NOT NULL
BEGIN
    IF COL_LENGTH(N'dbo.Visit_Duplicate', N'patientTypeId') IS NULL
        ALTER TABLE dbo.Visit_Duplicate ADD patientTypeId INT NULL;

    IF COL_LENGTH(N'dbo.Visit_Duplicate', N'payType') IS NULL
        ALTER TABLE dbo.Visit_Duplicate ADD payType INT NULL;

    IF COL_LENGTH(N'dbo.Visit_Duplicate', N'sourceVisitId') IS NULL
        ALTER TABLE dbo.Visit_Duplicate ADD sourceVisitId INT NULL;

    IF COL_LENGTH(N'dbo.Visit_Duplicate', N'sourceVisitNo') IS NULL
        ALTER TABLE dbo.Visit_Duplicate ADD sourceVisitNo VARCHAR(50) NULL;

    IF COL_LENGTH(N'dbo.Visit_Duplicate', N'sourceAdmissionNo') IS NULL
        ALTER TABLE dbo.Visit_Duplicate ADD sourceAdmissionNo VARCHAR(50) NULL;

    IF COL_LENGTH(N'dbo.Visit_Duplicate', N'referralNo') IS NULL
        ALTER TABLE dbo.Visit_Duplicate ADD referralNo VARCHAR(100) NULL;

    IF COL_LENGTH(N'dbo.Visit_Duplicate', N'referralDate') IS NULL
        ALTER TABLE dbo.Visit_Duplicate ADD referralDate DATE NULL;

    IF COL_LENGTH(N'dbo.Visit_Duplicate', N'payerTpaName') IS NULL
        ALTER TABLE dbo.Visit_Duplicate ADD payerTpaName NVARCHAR(200) NULL;

    IF COL_LENGTH(N'dbo.Visit_Duplicate', N'verifiedByUserName') IS NULL
        ALTER TABLE dbo.Visit_Duplicate ADD verifiedByUserName NVARCHAR(100) NULL;

    IF COL_LENGTH(N'dbo.Visit_Duplicate', N'settlementMode') IS NULL
        ALTER TABLE dbo.Visit_Duplicate ADD settlementMode NVARCHAR(30) NULL;

    IF COL_LENGTH(N'dbo.Visit_Duplicate', N'updatedBy') IS NULL
        ALTER TABLE dbo.Visit_Duplicate ADD updatedBy INT NULL;

    IF COL_LENGTH(N'dbo.Visit_Duplicate', N'updatedOn') IS NULL
        ALTER TABLE dbo.Visit_Duplicate ADD updatedOn DATETIME NULL;
END;

IF OBJECT_ID(N'dbo.Visit', N'U') IS NOT NULL
BEGIN
    IF COL_LENGTH(N'dbo.Visit', N'HasVirtualVisitDuplicate') IS NULL
    BEGIN
        ALTER TABLE dbo.Visit
        ADD HasVirtualVisitDuplicate BIT NOT NULL
            CONSTRAINT DF_Visit_HasVirtualVisitDuplicate DEFAULT (0);
    END;

    IF COL_LENGTH(N'dbo.Visit', N'VirtualVisitDuplicateId') IS NULL
        ALTER TABLE dbo.Visit ADD VirtualVisitDuplicateId INT NULL;

    IF COL_LENGTH(N'dbo.Visit', N'VirtualVisitUpdatedBy') IS NULL
        ALTER TABLE dbo.Visit ADD VirtualVisitUpdatedBy INT NULL;

    IF COL_LENGTH(N'dbo.Visit', N'VirtualVisitUpdatedOn') IS NULL
        ALTER TABLE dbo.Visit ADD VirtualVisitUpdatedOn DATETIME NULL;
END;

IF OBJECT_ID(N'dbo.Corp_Bill_Mst', N'U') IS NOT NULL
BEGIN
    IF COL_LENGTH(N'dbo.Corp_Bill_Mst', N'duplicateVisitId') IS NULL
        ALTER TABLE dbo.Corp_Bill_Mst ADD duplicateVisitId INT NULL;
END;

IF OBJECT_ID(N'dbo.Visit_Duplicate', N'U') IS NOT NULL
BEGIN
    EXEC(N'
        UPDATE vd
        SET
            patientTypeId = COALESCE(vd.patientTypeId, pst.PatientType_ID, v.PatientType_ID),
            payType = CASE
                WHEN vd.payType IS NOT NULL THEN vd.payType
                WHEN vd.sourceVisitId IS NOT NULL THEN v.payType
                ELSE vd.payType
            END,
            sourceVisitNo = CASE
                WHEN ISNULL(LTRIM(RTRIM(vd.sourceVisitNo)), '''') <> '''' THEN vd.sourceVisitNo
                WHEN vd.sourceVisitId IS NOT NULL THEN v.VisitNo
                ELSE vd.sourceVisitNo
            END,
            sourceAdmissionNo = CASE
                WHEN ISNULL(LTRIM(RTRIM(vd.sourceAdmissionNo)), '''') <> '''' THEN vd.sourceAdmissionNo
                WHEN vd.sourceVisitId IS NOT NULL THEN v.AdmissionNo
                ELSE vd.sourceAdmissionNo
            END
        FROM dbo.Visit_Duplicate vd
        LEFT JOIN dbo.PatientSubType_Mst pst WITH (NOLOCK)
            ON pst.PatientSubType_ID = vd.patientSubTypeId
        LEFT JOIN dbo.Visit v WITH (NOLOCK)
            ON v.Visit_ID = vd.sourceVisitId
        WHERE
            vd.patientTypeId IS NULL
            OR (vd.payType IS NULL AND vd.sourceVisitId IS NOT NULL)
            OR (ISNULL(LTRIM(RTRIM(vd.sourceVisitNo)), '''') = '''' AND vd.sourceVisitId IS NOT NULL)
            OR (ISNULL(LTRIM(RTRIM(vd.sourceAdmissionNo)), '''') = '''' AND vd.sourceVisitId IS NOT NULL);

        ;WITH LatestLinkedDuplicate AS
        (
            SELECT
                vd.sourceVisitId,
                vd.visitId,
                COALESCE(vd.updatedBy, vd.insertedBy) AS auditBy,
                COALESCE(vd.updatedOn, vd.insertedOn) AS auditOn,
                ROW_NUMBER() OVER (PARTITION BY vd.sourceVisitId ORDER BY vd.visitId DESC) AS rn
            FROM dbo.Visit_Duplicate vd WITH (NOLOCK)
            WHERE vd.sourceVisitId IS NOT NULL
        )
        UPDATE v
        SET
            HasVirtualVisitDuplicate = 1,
            VirtualVisitDuplicateId = l.visitId,
            VirtualVisitUpdatedBy = l.auditBy,
            VirtualVisitUpdatedOn = l.auditOn
        FROM dbo.Visit v
        INNER JOIN LatestLinkedDuplicate l
            ON l.sourceVisitId = v.Visit_ID
           AND l.rn = 1;
    ');
END;

IF OBJECT_ID(N'dbo.Visit_Duplicate', N'U') IS NOT NULL
BEGIN
    IF NOT EXISTS
    (
        SELECT 1
        FROM sys.indexes
        WHERE object_id = OBJECT_ID(N'dbo.Visit_Duplicate')
          AND name = N'UX_Visit_Duplicate_SourceVisitId'
    )
    BEGIN
        EXEC(N'
            CREATE UNIQUE NONCLUSTERED INDEX UX_Visit_Duplicate_SourceVisitId
            ON dbo.Visit_Duplicate (sourceVisitId)
            WHERE sourceVisitId IS NOT NULL;
        ');
    END;

    IF NOT EXISTS
    (
        SELECT 1
        FROM sys.indexes
        WHERE object_id = OBJECT_ID(N'dbo.Visit_Duplicate')
          AND name = N'IX_Visit_Duplicate_BillingLookup'
    )
    BEGIN
        EXEC(N'
            CREATE NONCLUSTERED INDEX IX_Visit_Duplicate_BillingLookup
            ON dbo.Visit_Duplicate
            (
                visitTypeId,
                payType,
                patientTypeId,
                patientSubTypeId,
                dischargeTypeId
            )
            INCLUDE
            (
                visitId,
                patientId,
                visitDate,
                dischargeDate,
                sourceVisitId,
                sourceVisitNo,
                insertedOn
            );
        ');
    END;
END;

IF OBJECT_ID(N'dbo.Usp_CorpPatientListforDuplicateBilling', N'P') IS NULL
BEGIN
    EXEC(N'
        CREATE PROCEDURE dbo.Usp_CorpPatientListforDuplicateBilling
        AS
        BEGIN
            SET NOCOUNT ON;
        END
    ');
END;

EXEC(N'
ALTER PROCEDURE dbo.Usp_CorpPatientListforDuplicateBilling
    @visitypeid INT,
    @ptypeid INT,
    @pSubtypeid INT,
    @payType INT = 0
AS
BEGIN
    SET NOCOUNT ON;

    SELECT
        vd.visitId,
        vd.patientId,
        p.Registration_No AS RegNo,
        dbo.fn_PatientFullName(vd.patientId) AS PatientName,
        vd.visitDate,
        vd.dischargeDate,
        ISNULL(cb.CBill_ID, 0) AS CBill_ID,
        ISNULL(cb.CBill_NO, '''') AS CBill_No,
        ISNULL(cb.CBill_Date, 0) AS CBill_Date,
        ISNULL(cb.CAmount, 0) AS cAmount,
        ISNULL(cb.Status, '''') AS status,
        ISNULL(cb.Submit_Date, '''') AS Submit_Date,
        pst.PatientSubType_Desc,
        ISNULL(vd.payType, 0) AS payType,
        CASE ISNULL(vd.payType, 0)
            WHEN 1 THEN ''CASH''
            WHEN 2 THEN ''CASHLESS''
            ELSE ''''
        END AS PatientCategory,
        ISNULL(vd.patientTypeId, pst.PatientType_ID) AS PatientType_ID,
        ISNULL(pt.PatientType, '''') AS PatientType,
        vd.sourceVisitId,
        ISNULL(vd.sourceVisitNo, '''') AS sourceVisitNo
    FROM dbo.Visit_Duplicate vd WITH (NOLOCK)
    LEFT JOIN dbo.Patient p WITH (NOLOCK)
        ON p.PatientId = vd.patientId
    LEFT JOIN dbo.PatientSubType_Mst pst WITH (NOLOCK)
        ON pst.PatientSubType_ID = vd.patientSubTypeId
    LEFT JOIN dbo.PatientType_mst pt WITH (NOLOCK)
        ON pt.PatientType_ID = ISNULL(vd.patientTypeId, pst.PatientType_ID)
    LEFT JOIN dbo.Corp_Bill_Mst cb WITH (NOLOCK)
        ON cb.duplicateVisitId = vd.visitId
    WHERE
        vd.visitTypeId = @visitypeid
        AND (@pSubtypeid IS NULL OR @pSubtypeid = 0 OR vd.patientSubTypeId = @pSubtypeid)
        AND (@ptypeid IS NULL OR @ptypeid = 0 OR ISNULL(vd.patientTypeId, pst.PatientType_ID) = @ptypeid)
        AND (@payType IS NULL OR @payType = 0 OR ISNULL(vd.payType, 0) = @payType)
        AND (@visitypeid <> 1 OR ISNULL(vd.dischargeTypeId, 0) = 2)
    ORDER BY
        vd.dischargeDate,
        vd.visitId;
END
');

-- Final override: only hide source Visit rows when an eligible linked duplicate exists.
EXEC(N'
ALTER PROCEDURE dbo.Usp_CorpPatientListforBilling
    @visitypeid INT,
    @ptypeid INT,
    @pSubtypeid INT
AS
BEGIN
    SET NOCOUNT ON;

    CREATE TABLE #EligibleDuplicateRows
    (
        Bill_ID BIGINT NULL,
        VisitNo NVARCHAR(100) COLLATE DATABASE_DEFAULT NULL,
        Registration_No NVARCHAR(100) COLLATE DATABASE_DEFAULT NULL,
        PatientName NVARCHAR(300) COLLATE DATABASE_DEFAULT NULL,
        VisitDate DATETIME NULL,
        DischargeDate DATETIME NULL,
        Bill_No NVARCHAR(100) COLLATE DATABASE_DEFAULT NULL,
        BillDate DATETIME NULL,
        NetAmount DECIMAL(19,4) NULL,
        Visit_ID BIGINT NULL,
        PatientID BIGINT NULL,
        Submit_Date DATETIME NULL,
        CBill_ID BIGINT NULL,
        CAmount DECIMAL(19,4) NULL,
        Status NVARCHAR(100) COLLATE DATABASE_DEFAULT NULL,
        Doc_nm NVARCHAR(300) COLLATE DATABASE_DEFAULT NULL,
        cmbillNo NVARCHAR(100) COLLATE DATABASE_DEFAULT NULL,
        PatientSubType_Desc NVARCHAR(300) COLLATE DATABASE_DEFAULT NULL,
        SourceVisitId BIGINT NULL
    );

    INSERT INTO #EligibleDuplicateRows
    (
        Bill_ID,
        VisitNo,
        Registration_No,
        PatientName,
        VisitDate,
        DischargeDate,
        Bill_No,
        BillDate,
        NetAmount,
        Visit_ID,
        PatientID,
        Submit_Date,
        CBill_ID,
        CAmount,
        Status,
        Doc_nm,
        cmbillNo,
        PatientSubType_Desc,
        SourceVisitId
    )
    SELECT
        bm.Bill_ID,
        ISNULL(vd.sourceVisitNo, src.VisitNo) AS VisitNo,
        p.Registration_No,
        dbo.fn_PatientFullName(vd.patientId) AS PatientName,
        vd.visitDate,
        vd.dischargeDate,
        CASE
            WHEN @visitypeid IN (2, 3) THEN
                ISNULL(
                    (
                        CASE
                            WHEN ISNULL(cb.CBill_NO, '''') = ''''
                                THEN dbo.fn_GetCorpBillNo(bm.Bill_ID, src.Visit_ID)
                            ELSE ISNULL(cb.CBill_NO, '''')
                        END
                    ),
                    ''''
                )
            ELSE ISNULL(cb.CBill_NO, '''')
        END AS Bill_No,
        bm.BillDate,
        bm.NetAmount,
        vd.visitId AS Visit_ID,
        vd.patientId AS PatientID,
        ISNULL(cb.Submit_Date, 0) AS Submit_Date,
        ISNULL(cb.CBill_ID, 0) AS CBill_ID,
        ISNULL(cb.CAmount, 0) AS CAmount,
        CASE
            WHEN @visitypeid IN (2, 3) THEN
                ISNULL(
                    (
                        CASE
                            WHEN ISNULL(cb.Status, '''') = ''''
                                THEN dbo.fn_GetCorpBillStatus(bm.Bill_ID, src.Visit_ID)
                            ELSE ISNULL(cb.Status, '''')
                        END
                    ),
                    ''''
                )
            ELSE ISNULL(cb.Status, '''')
        END AS Status,
        cb.Doc_nm,
        cb.Bill_No AS cmbillNo,
        pst.PatientSubType_Desc,
        src.Visit_ID AS SourceVisitId
    FROM dbo.Visit_Duplicate vd
    INNER JOIN dbo.Visit src
        ON src.Visit_ID = vd.sourceVisitId
    INNER JOIN dbo.Billing_Mst bm
        ON src.Visit_ID = bm.Visit_ID
    LEFT OUTER JOIN dbo.PatientSubType_Mst pst
        ON vd.patientSubTypeId = pst.PatientSubType_ID
    LEFT OUTER JOIN dbo.Patient p
        ON vd.patientId = p.PatientId
    LEFT OUTER JOIN dbo.Corp_Bill_Mst cb
        ON cb.duplicateVisitId = vd.visitId
    WHERE
        vd.visitTypeId = @visitypeid
        AND ISNULL(vd.patientTypeId, pst.PatientType_ID) = @ptypeid
        AND vd.patientSubTypeId = @pSubtypeid
        AND bm.BillType = ''P''
        AND ISNULL(bm.CancelStatus, 0) = 0
        AND src.DepartmentID <> 7
        AND
        (
            (@visitypeid = 1 AND @ptypeid = 65 AND ISNULL(bm.submitted, 0) = 1)
            OR (@visitypeid = 1 AND @ptypeid <> 65 AND ISNULL(vd.dischargeTypeId, 0) = 2)
            OR (@visitypeid IN (2, 3, 6))
        );

    IF @visitypeid = 1
    BEGIN
        IF @ptypeid = 65
        BEGIN
            SELECT *
            FROM
            (
                SELECT
                    dbo.Billing_Mst.Bill_ID,
                    dbo.Visit.VisitNo,
                    dbo.Patient.Registration_No,
                    dbo.fn_PatientFullName(dbo.Visit.PatientID) AS PatientName,
                    dbo.Visit.VisitDate,
                    dbo.Visit.DischargeDate,
                    ISNULL(dbo.Corp_Bill_Mst.CBill_NO, '''') AS Bill_No,
                    dbo.Billing_Mst.BillDate,
                    dbo.Billing_Mst.NetAmount,
                    dbo.Visit.Visit_ID,
                    dbo.Visit.PatientID,
                    ISNULL(dbo.Corp_Bill_Mst.Submit_Date, 0) AS Submit_Date,
                    ISNULL(dbo.Corp_Bill_Mst.CBill_ID, 0) AS CBill_ID,
                    ISNULL(dbo.Corp_Bill_Mst.CAmount, 0) AS CAmount,
                    ISNULL(dbo.Corp_Bill_Mst.Status, '''') AS Status,
                    dbo.Corp_Bill_Mst.Doc_nm,
                    dbo.Corp_Bill_Mst.Bill_No AS cmbillNo,
                    dbo.PatientSubType_Mst.PatientSubType_Desc
                FROM dbo.Visit
                LEFT OUTER JOIN dbo.PatientSubType_Mst
                    ON dbo.Visit.PatientSubType_ID = dbo.PatientSubType_Mst.PatientSubType_ID
                LEFT OUTER JOIN dbo.PatientType_mst
                    ON dbo.Visit.PatientType_ID = dbo.PatientType_mst.PatientType_ID
                LEFT OUTER JOIN dbo.Patient
                    ON dbo.Visit.PatientID = dbo.Patient.PatientId
                RIGHT OUTER JOIN dbo.Billing_Mst
                    ON dbo.Visit.Visit_ID = dbo.Billing_Mst.Visit_ID
                LEFT OUTER JOIN dbo.Corp_Bill_Mst
                    ON dbo.Billing_Mst.Bill_ID = dbo.Corp_Bill_Mst.Bill_ID
                WHERE
                    dbo.Visit.VisitTypeID = 1
                    AND dbo.Billing_Mst.BillType = ''P''
                    AND dbo.Visit.PatientType_ID = @ptypeid
                    AND dbo.Billing_Mst.submitted = 1
                    AND dbo.Visit.PatientSubType_ID = @pSubtypeid
                    AND dbo.Visit.DepartmentID <> 7
                    AND ISNULL(dbo.Billing_Mst.CancelStatus, 0) = 0
                    AND NOT EXISTS
                    (
                        SELECT 1
                        FROM #EligibleDuplicateRows ed
                        WHERE ed.SourceVisitId = dbo.Visit.Visit_ID
                    )

                UNION ALL

                SELECT
                    Bill_ID,
                    VisitNo,
                    Registration_No,
                    PatientName,
                    VisitDate,
                    DischargeDate,
                    Bill_No,
                    BillDate,
                    NetAmount,
                    Visit_ID,
                    PatientID,
                    Submit_Date,
                    CBill_ID,
                    CAmount,
                    Status,
                    Doc_nm,
                    cmbillNo,
                    PatientSubType_Desc
                FROM #EligibleDuplicateRows
            ) AS BillingRows
            ORDER BY DischargeDate, Visit_ID;
        END
        ELSE
        BEGIN
            SELECT *
            FROM
            (
                SELECT
                    dbo.Billing_Mst.Bill_ID,
                    dbo.Visit.VisitNo,
                    dbo.Patient.Registration_No,
                    dbo.fn_PatientFullName(dbo.Visit.PatientID) AS PatientName,
                    dbo.Visit.VisitDate,
                    dbo.Visit.DischargeDate,
                    ISNULL(dbo.Corp_Bill_Mst.CBill_NO, '''') AS Bill_No,
                    dbo.Billing_Mst.BillDate,
                    dbo.Billing_Mst.NetAmount,
                    dbo.Visit.Visit_ID,
                    dbo.Visit.PatientID,
                    ISNULL(dbo.Corp_Bill_Mst.Submit_Date, 0) AS Submit_Date,
                    ISNULL(dbo.Corp_Bill_Mst.CBill_ID, 0) AS CBill_ID,
                    ISNULL(dbo.Corp_Bill_Mst.CAmount, 0) AS CAmount,
                    ISNULL(dbo.Corp_Bill_Mst.Status, '''') AS Status,
                    dbo.Corp_Bill_Mst.Doc_nm,
                    dbo.Corp_Bill_Mst.Bill_No AS cmbillNo,
                    dbo.PatientSubType_Mst.PatientSubType_Desc
                FROM dbo.Visit
                LEFT OUTER JOIN dbo.PatientSubType_Mst
                    ON dbo.Visit.PatientSubType_ID = dbo.PatientSubType_Mst.PatientSubType_ID
                LEFT OUTER JOIN dbo.PatientType_mst
                    ON dbo.Visit.PatientType_ID = dbo.PatientType_mst.PatientType_ID
                LEFT OUTER JOIN dbo.Patient
                    ON dbo.Visit.PatientID = dbo.Patient.PatientId
                RIGHT OUTER JOIN dbo.Billing_Mst
                    ON dbo.Visit.Visit_ID = dbo.Billing_Mst.Visit_ID
                LEFT OUTER JOIN dbo.Corp_Bill_Mst
                    ON dbo.Billing_Mst.Bill_ID = dbo.Corp_Bill_Mst.Bill_ID
                WHERE
                    dbo.Visit.VisitTypeID = 1
                    AND dbo.Billing_Mst.BillType = ''P''
                    AND dbo.Visit.PatientType_ID = @ptypeid
                    AND dbo.Visit.DischargeType = 2
                    AND dbo.Visit.PatientSubType_ID = @pSubtypeid
                    AND dbo.Visit.DepartmentID <> 7
                    AND ISNULL(dbo.Billing_Mst.CancelStatus, 0) = 0
                    AND NOT EXISTS
                    (
                        SELECT 1
                        FROM #EligibleDuplicateRows ed
                        WHERE ed.SourceVisitId = dbo.Visit.Visit_ID
                    )

                UNION ALL

                SELECT
                    Bill_ID,
                    VisitNo,
                    Registration_No,
                    PatientName,
                    VisitDate,
                    DischargeDate,
                    Bill_No,
                    BillDate,
                    NetAmount,
                    Visit_ID,
                    PatientID,
                    Submit_Date,
                    CBill_ID,
                    CAmount,
                    Status,
                    Doc_nm,
                    cmbillNo,
                    PatientSubType_Desc
                FROM #EligibleDuplicateRows
            ) AS BillingRows
            ORDER BY DischargeDate, Visit_ID;
        END
    END
    ELSE IF @visitypeid = 2
    BEGIN
        SELECT *
        FROM
        (
            SELECT
                dbo.Billing_Mst.Bill_ID,
                dbo.Visit.VisitNo,
                dbo.Patient.Registration_No,
                dbo.fn_PatientFullName(dbo.Visit.PatientID) AS PatientName,
                dbo.Visit.VisitDate,
                dbo.Visit.DischargeDate,
                ISNULL(
                    (
                        CASE
                            WHEN ISNULL(dbo.Corp_Bill_Mst.CBill_NO, '''') = ''''
                                THEN dbo.fn_GetCorpBillNo(dbo.Billing_Mst.Bill_ID, dbo.Billing_Mst.visit_id)
                            ELSE ISNULL(dbo.Corp_Bill_Mst.CBill_NO, '''')
                        END
                    ),
                    ''''
                ) AS Bill_No,
                dbo.Billing_Mst.BillDate,
                dbo.Billing_Mst.NetAmount,
                dbo.Visit.Visit_ID,
                dbo.Visit.PatientID,
                ISNULL(dbo.Corp_Bill_Mst.Submit_Date, 0) AS Submit_Date,
                ISNULL(dbo.Corp_Bill_Mst.CBill_ID, 0) AS CBill_ID,
                ISNULL(dbo.Corp_Bill_Mst.CAmount, 0) AS CAmount,
                ISNULL(
                    (
                        CASE
                            WHEN ISNULL(dbo.Corp_Bill_Mst.Status, '''') = ''''
                                THEN dbo.fn_GetCorpBillStatus(dbo.Billing_Mst.Bill_ID, dbo.Billing_Mst.visit_id)
                            ELSE ISNULL(dbo.Corp_Bill_Mst.Status, '''')
                        END
                    ),
                    ''''
                ) AS Status,
                dbo.Corp_Bill_Mst.Doc_nm,
                dbo.Corp_Bill_Mst.Bill_No AS cmbillNo,
                dbo.PatientSubType_Mst.PatientSubType_Desc
            FROM dbo.Visit
            LEFT OUTER JOIN dbo.PatientSubType_Mst
                ON dbo.Visit.PatientSubType_ID = dbo.PatientSubType_Mst.PatientSubType_ID
            LEFT OUTER JOIN dbo.PatientType_mst
                ON dbo.Visit.PatientType_ID = dbo.PatientType_mst.PatientType_ID
            LEFT OUTER JOIN dbo.Patient
                ON dbo.Visit.PatientID = dbo.Patient.PatientId
            RIGHT OUTER JOIN dbo.Billing_Mst
                ON dbo.Visit.Visit_ID = dbo.Billing_Mst.Visit_ID
            LEFT OUTER JOIN dbo.Corp_Bill_Mst
                ON dbo.Billing_Mst.Bill_ID = dbo.Corp_Bill_Mst.Bill_ID
            WHERE
                dbo.Visit.VisitTypeID = 2
                AND dbo.Billing_Mst.BillType = ''P''
                AND dbo.Visit.PatientType_ID = @ptypeid
                AND dbo.Visit.PatientSubType_ID = @pSubtypeid
                AND dbo.Visit.DepartmentID <> 7
                AND ISNULL(dbo.Billing_Mst.CancelStatus, 0) = 0
                AND NOT EXISTS
                (
                    SELECT 1
                    FROM #EligibleDuplicateRows ed
                    WHERE ed.SourceVisitId = dbo.Visit.Visit_ID
                )

            UNION ALL

            SELECT
                Bill_ID,
                VisitNo,
                Registration_No,
                PatientName,
                VisitDate,
                DischargeDate,
                Bill_No,
                BillDate,
                NetAmount,
                Visit_ID,
                PatientID,
                Submit_Date,
                CBill_ID,
                CAmount,
                Status,
                Doc_nm,
                cmbillNo,
                PatientSubType_Desc
            FROM #EligibleDuplicateRows
        ) AS BillingRows
        ORDER BY VisitDate, Visit_ID;
    END
    ELSE IF @visitypeid = 3
    BEGIN
        SELECT *
        FROM
        (
            SELECT
                dbo.Billing_Mst.Bill_ID,
                dbo.Visit.VisitNo,
                dbo.Patient.Registration_No,
                dbo.fn_PatientFullName(dbo.Visit.PatientID) AS PatientName,
                dbo.Visit.VisitDate,
                dbo.Visit.DischargeDate,
                ISNULL(
                    (
                        CASE
                            WHEN ISNULL(dbo.Corp_Bill_Mst.CBill_NO, '''') = ''''
                                THEN dbo.fn_GetCorpBillNo(dbo.Billing_Mst.Bill_ID, dbo.Billing_Mst.visit_id)
                            ELSE ISNULL(dbo.Corp_Bill_Mst.CBill_NO, '''')
                        END
                    ),
                    ''''
                ) AS Bill_No,
                dbo.Billing_Mst.BillDate,
                dbo.Billing_Mst.NetAmount,
                dbo.Visit.Visit_ID,
                dbo.Visit.PatientID,
                ISNULL(dbo.Corp_Bill_Mst.Submit_Date, 0) AS Submit_Date,
                ISNULL(dbo.Corp_Bill_Mst.CBill_ID, 0) AS CBill_ID,
                ISNULL(dbo.Corp_Bill_Mst.CAmount, 0) AS CAmount,
                ISNULL(
                    (
                        CASE
                            WHEN ISNULL(dbo.Corp_Bill_Mst.Status, '''') = ''''
                                THEN dbo.fn_GetCorpBillStatus(dbo.Billing_Mst.Bill_ID, dbo.Billing_Mst.visit_id)
                            ELSE ISNULL(dbo.Corp_Bill_Mst.Status, '''')
                        END
                    ),
                    ''''
                ) AS Status,
                dbo.Corp_Bill_Mst.Doc_nm,
                dbo.Corp_Bill_Mst.Bill_No AS cmbillNo,
                dbo.PatientSubType_Mst.PatientSubType_Desc
            FROM dbo.Visit
            LEFT OUTER JOIN dbo.PatientSubType_Mst
                ON dbo.Visit.PatientSubType_ID = dbo.PatientSubType_Mst.PatientSubType_ID
            LEFT OUTER JOIN dbo.PatientType_mst
                ON dbo.Visit.PatientType_ID = dbo.PatientType_mst.PatientType_ID
            LEFT OUTER JOIN dbo.Patient
                ON dbo.Visit.PatientID = dbo.Patient.PatientId
            RIGHT OUTER JOIN dbo.Billing_Mst
                ON dbo.Visit.Visit_ID = dbo.Billing_Mst.Visit_ID
            LEFT OUTER JOIN dbo.Corp_Bill_Mst
                ON dbo.Billing_Mst.Bill_ID = dbo.Corp_Bill_Mst.Bill_ID
            WHERE
                dbo.Visit.VisitTypeID = 3
                AND dbo.Billing_Mst.BillType = ''P''
                AND dbo.Visit.PatientType_ID = @ptypeid
                AND dbo.Visit.PatientSubType_ID = @pSubtypeid
                AND dbo.Visit.DepartmentID <> 7
                AND ISNULL(dbo.Billing_Mst.CancelStatus, 0) = 0
                AND NOT EXISTS
                (
                    SELECT 1
                    FROM #EligibleDuplicateRows ed
                    WHERE ed.SourceVisitId = dbo.Visit.Visit_ID
                )

            UNION ALL

            SELECT
                Bill_ID,
                VisitNo,
                Registration_No,
                PatientName,
                VisitDate,
                DischargeDate,
                Bill_No,
                BillDate,
                NetAmount,
                Visit_ID,
                PatientID,
                Submit_Date,
                CBill_ID,
                CAmount,
                Status,
                Doc_nm,
                cmbillNo,
                PatientSubType_Desc
            FROM #EligibleDuplicateRows
        ) AS BillingRows
        ORDER BY VisitDate, Visit_ID;
    END
    ELSE IF @visitypeid = 6
    BEGIN
        SELECT *
        FROM
        (
            SELECT
                dbo.Billing_Mst.Bill_ID,
                dbo.Visit.VisitNo,
                dbo.Patient.Registration_No,
                dbo.fn_PatientFullName(dbo.Visit.PatientID) AS PatientName,
                dbo.Visit.VisitDate,
                dbo.Visit.DischargeDate,
                ISNULL(dbo.Corp_Bill_Mst.CBill_NO, '''') AS Bill_No,
                dbo.Billing_Mst.BillDate,
                dbo.Billing_Mst.NetAmount,
                dbo.Visit.Visit_ID,
                dbo.Visit.PatientID,
                ISNULL(dbo.Corp_Bill_Mst.Submit_Date, 0) AS Submit_Date,
                ISNULL(dbo.Corp_Bill_Mst.CBill_ID, 0) AS CBill_ID,
                ISNULL(dbo.Corp_Bill_Mst.CAmount, 0) AS CAmount,
                ISNULL(dbo.Corp_Bill_Mst.Status, '''') AS Status,
                dbo.Corp_Bill_Mst.Doc_nm,
                dbo.Corp_Bill_Mst.Bill_No AS cmbillNo,
                dbo.PatientSubType_Mst.PatientSubType_Desc
            FROM dbo.Visit
            LEFT OUTER JOIN dbo.PatientSubType_Mst
                ON dbo.Visit.PatientSubType_ID = dbo.PatientSubType_Mst.PatientSubType_ID
            LEFT OUTER JOIN dbo.PatientType_mst
                ON dbo.Visit.PatientType_ID = dbo.PatientType_mst.PatientType_ID
            LEFT OUTER JOIN dbo.Patient
                ON dbo.Visit.PatientID = dbo.Patient.PatientId
            RIGHT OUTER JOIN dbo.Billing_Mst
                ON dbo.Visit.Visit_ID = dbo.Billing_Mst.Visit_ID
            LEFT OUTER JOIN dbo.Corp_Bill_Mst
                ON dbo.Billing_Mst.Bill_ID = dbo.Corp_Bill_Mst.Bill_ID
            WHERE
                dbo.Visit.VisitTypeID = 6
                AND dbo.Billing_Mst.BillType = ''P''
                AND dbo.Visit.PatientType_ID = @ptypeid
                AND dbo.Visit.PatientSubType_ID = @pSubtypeid
                AND dbo.Visit.DepartmentID <> 7
                AND ISNULL(dbo.Billing_Mst.CancelStatus, 0) = 0
                AND NOT EXISTS
                (
                    SELECT 1
                    FROM #EligibleDuplicateRows ed
                    WHERE ed.SourceVisitId = dbo.Visit.Visit_ID
                )

            UNION ALL

            SELECT
                Bill_ID,
                VisitNo,
                Registration_No,
                PatientName,
                VisitDate,
                DischargeDate,
                Bill_No,
                BillDate,
                NetAmount,
                Visit_ID,
                PatientID,
                Submit_Date,
                CBill_ID,
                CAmount,
                Status,
                Doc_nm,
                cmbillNo,
                PatientSubType_Desc
            FROM #EligibleDuplicateRows
        ) AS BillingRows
        ORDER BY DischargeDate, Visit_ID;
    END
END
');

IF OBJECT_ID(N'dbo.Usp_CorpPatientListforBilling', N'P') IS NULL
BEGIN
    EXEC(N'
        CREATE PROCEDURE dbo.Usp_CorpPatientListforBilling
        AS
        BEGIN
            SET NOCOUNT ON;
        END
    ');
END;

EXEC(N'
ALTER PROCEDURE dbo.Usp_CorpPatientListforBilling
    @visitypeid INT,
    @ptypeid INT,
    @pSubtypeid INT
AS
BEGIN
    SET NOCOUNT ON;

    IF @visitypeid = 1
    BEGIN
        IF @ptypeid = 65
        BEGIN
            SELECT *
            FROM
            (
                SELECT
                    dbo.Billing_Mst.Bill_ID,
                    dbo.Visit.VisitNo,
                    dbo.Patient.Registration_No,
                    dbo.fn_PatientFullName(dbo.Visit.PatientID) AS PatientName,
                    dbo.Visit.VisitDate,
                    dbo.Visit.DischargeDate,
                    ISNULL(dbo.Corp_Bill_Mst.CBill_NO, '''') AS Bill_No,
                    dbo.Billing_Mst.BillDate,
                    dbo.Billing_Mst.NetAmount,
                    dbo.Visit.Visit_ID,
                    dbo.Visit.PatientID,
                    ISNULL(dbo.Corp_Bill_Mst.Submit_Date, 0) AS Submit_Date,
                    ISNULL(dbo.Corp_Bill_Mst.CBill_ID, 0) AS CBill_ID,
                    ISNULL(dbo.Corp_Bill_Mst.CAmount, 0) AS CAmount,
                    ISNULL(dbo.Corp_Bill_Mst.Status, '''') AS Status,
                    dbo.Corp_Bill_Mst.Doc_nm,
                    dbo.Corp_Bill_Mst.Bill_No AS cmbillNo,
                    dbo.PatientSubType_Mst.PatientSubType_Desc
                FROM dbo.Visit
                LEFT OUTER JOIN dbo.PatientSubType_Mst
                    ON dbo.Visit.PatientSubType_ID = dbo.PatientSubType_Mst.PatientSubType_ID
                LEFT OUTER JOIN dbo.PatientType_mst
                    ON dbo.Visit.PatientType_ID = dbo.PatientType_mst.PatientType_ID
                LEFT OUTER JOIN dbo.Patient
                    ON dbo.Visit.PatientID = dbo.Patient.PatientId
                RIGHT OUTER JOIN dbo.Billing_Mst
                    ON dbo.Visit.Visit_ID = dbo.Billing_Mst.Visit_ID
                LEFT OUTER JOIN dbo.Corp_Bill_Mst
                    ON dbo.Billing_Mst.Bill_ID = dbo.Corp_Bill_Mst.Bill_ID
                WHERE
                    dbo.Visit.VisitTypeID = 1
                    AND dbo.Billing_Mst.BillType = ''P''
                    AND dbo.Visit.PatientType_ID = @ptypeid
                    AND dbo.Billing_Mst.submitted = 1
                    AND dbo.Visit.PatientSubType_ID = @pSubtypeid
                    AND dbo.Visit.DepartmentID <> 7
                    AND ISNULL(dbo.Billing_Mst.CancelStatus, 0) = 0
                    AND NOT EXISTS
                    (
                        SELECT 1
                        FROM dbo.Visit_Duplicate vd2 WITH (NOLOCK)
                        WHERE vd2.sourceVisitId = dbo.Visit.Visit_ID
                    )

                UNION ALL

                SELECT
                    bm.Bill_ID,
                    ISNULL(vd.sourceVisitNo, src.VisitNo) AS VisitNo,
                    p.Registration_No,
                    dbo.fn_PatientFullName(vd.patientId) AS PatientName,
                    vd.visitDate,
                    vd.dischargeDate,
                    ISNULL(cb.CBill_NO, '''') AS Bill_No,
                    bm.BillDate,
                    bm.NetAmount,
                    vd.visitId AS Visit_ID,
                    vd.patientId AS PatientID,
                    ISNULL(cb.Submit_Date, 0) AS Submit_Date,
                    ISNULL(cb.CBill_ID, 0) AS CBill_ID,
                    ISNULL(cb.CAmount, 0) AS CAmount,
                    ISNULL(cb.Status, '''') AS Status,
                    cb.Doc_nm,
                    cb.Bill_No AS cmbillNo,
                    pst.PatientSubType_Desc
                FROM dbo.Visit_Duplicate vd
                INNER JOIN dbo.Visit src
                    ON src.Visit_ID = vd.sourceVisitId
                INNER JOIN dbo.Billing_Mst bm
                    ON src.Visit_ID = bm.Visit_ID
                LEFT OUTER JOIN dbo.PatientSubType_Mst pst
                    ON vd.patientSubTypeId = pst.PatientSubType_ID
                LEFT OUTER JOIN dbo.PatientType_mst pt
                    ON pt.PatientType_ID = ISNULL(vd.patientTypeId, pst.PatientType_ID)
                LEFT OUTER JOIN dbo.Patient p
                    ON vd.patientId = p.PatientId
                LEFT OUTER JOIN dbo.Corp_Bill_Mst cb
                    ON cb.duplicateVisitId = vd.visitId
                WHERE
                    vd.visitTypeId = 1
                    AND bm.BillType = ''P''
                    AND ISNULL(vd.patientTypeId, pst.PatientType_ID) = @ptypeid
                    AND bm.submitted = 1
                    AND vd.patientSubTypeId = @pSubtypeid
                    AND src.DepartmentID <> 7
                    AND ISNULL(bm.CancelStatus, 0) = 0
            ) AS BillingRows
            ORDER BY DischargeDate, Visit_ID;
        END
        ELSE
        BEGIN
            SELECT *
            FROM
            (
                SELECT
                    dbo.Billing_Mst.Bill_ID,
                    dbo.Visit.VisitNo,
                    dbo.Patient.Registration_No,
                    dbo.fn_PatientFullName(dbo.Visit.PatientID) AS PatientName,
                    dbo.Visit.VisitDate,
                    dbo.Visit.DischargeDate,
                    ISNULL(dbo.Corp_Bill_Mst.CBill_NO, '''') AS Bill_No,
                    dbo.Billing_Mst.BillDate,
                    dbo.Billing_Mst.NetAmount,
                    dbo.Visit.Visit_ID,
                    dbo.Visit.PatientID,
                    ISNULL(dbo.Corp_Bill_Mst.Submit_Date, 0) AS Submit_Date,
                    ISNULL(dbo.Corp_Bill_Mst.CBill_ID, 0) AS CBill_ID,
                    ISNULL(dbo.Corp_Bill_Mst.CAmount, 0) AS CAmount,
                    ISNULL(dbo.Corp_Bill_Mst.Status, '''') AS Status,
                    dbo.Corp_Bill_Mst.Doc_nm,
                    dbo.Corp_Bill_Mst.Bill_No AS cmbillNo,
                    dbo.PatientSubType_Mst.PatientSubType_Desc
                FROM dbo.Visit
                LEFT OUTER JOIN dbo.PatientSubType_Mst
                    ON dbo.Visit.PatientSubType_ID = dbo.PatientSubType_Mst.PatientSubType_ID
                LEFT OUTER JOIN dbo.PatientType_mst
                    ON dbo.Visit.PatientType_ID = dbo.PatientType_mst.PatientType_ID
                LEFT OUTER JOIN dbo.Patient
                    ON dbo.Visit.PatientID = dbo.Patient.PatientId
                RIGHT OUTER JOIN dbo.Billing_Mst
                    ON dbo.Visit.Visit_ID = dbo.Billing_Mst.Visit_ID
                LEFT OUTER JOIN dbo.Corp_Bill_Mst
                    ON dbo.Billing_Mst.Bill_ID = dbo.Corp_Bill_Mst.Bill_ID
                WHERE
                    dbo.Visit.VisitTypeID = 1
                    AND dbo.Billing_Mst.BillType = ''P''
                    AND dbo.Visit.PatientType_ID = @ptypeid
                    AND dbo.Visit.DischargeType = 2
                    AND dbo.Visit.PatientSubType_ID = @pSubtypeid
                    AND dbo.Visit.DepartmentID <> 7
                    AND ISNULL(dbo.Billing_Mst.CancelStatus, 0) = 0
                    AND NOT EXISTS
                    (
                        SELECT 1
                        FROM dbo.Visit_Duplicate vd2 WITH (NOLOCK)
                        WHERE vd2.sourceVisitId = dbo.Visit.Visit_ID
                    )

                UNION ALL

                SELECT
                    bm.Bill_ID,
                    ISNULL(vd.sourceVisitNo, src.VisitNo) AS VisitNo,
                    p.Registration_No,
                    dbo.fn_PatientFullName(vd.patientId) AS PatientName,
                    vd.visitDate,
                    vd.dischargeDate,
                    ISNULL(cb.CBill_NO, '''') AS Bill_No,
                    bm.BillDate,
                    bm.NetAmount,
                    vd.visitId AS Visit_ID,
                    vd.patientId AS PatientID,
                    ISNULL(cb.Submit_Date, 0) AS Submit_Date,
                    ISNULL(cb.CBill_ID, 0) AS CBill_ID,
                    ISNULL(cb.CAmount, 0) AS CAmount,
                    ISNULL(cb.Status, '''') AS Status,
                    cb.Doc_nm,
                    cb.Bill_No AS cmbillNo,
                    pst.PatientSubType_Desc
                FROM dbo.Visit_Duplicate vd
                INNER JOIN dbo.Visit src
                    ON src.Visit_ID = vd.sourceVisitId
                INNER JOIN dbo.Billing_Mst bm
                    ON src.Visit_ID = bm.Visit_ID
                LEFT OUTER JOIN dbo.PatientSubType_Mst pst
                    ON vd.patientSubTypeId = pst.PatientSubType_ID
                LEFT OUTER JOIN dbo.PatientType_mst pt
                    ON pt.PatientType_ID = ISNULL(vd.patientTypeId, pst.PatientType_ID)
                LEFT OUTER JOIN dbo.Patient p
                    ON vd.patientId = p.PatientId
                LEFT OUTER JOIN dbo.Corp_Bill_Mst cb
                    ON cb.duplicateVisitId = vd.visitId
                WHERE
                    vd.visitTypeId = 1
                    AND bm.BillType = ''P''
                    AND ISNULL(vd.patientTypeId, pst.PatientType_ID) = @ptypeid
                    AND ISNULL(vd.dischargeTypeId, 0) = 2
                    AND vd.patientSubTypeId = @pSubtypeid
                    AND src.DepartmentID <> 7
                    AND ISNULL(bm.CancelStatus, 0) = 0
            ) AS BillingRows
            ORDER BY DischargeDate, Visit_ID;
        END
    END
    ELSE IF @visitypeid = 2
    BEGIN
        SELECT *
        FROM
        (
            SELECT
                dbo.Billing_Mst.Bill_ID,
                dbo.Visit.VisitNo,
                dbo.Patient.Registration_No,
                dbo.fn_PatientFullName(dbo.Visit.PatientID) AS PatientName,
                dbo.Visit.VisitDate,
                dbo.Visit.DischargeDate,
                ISNULL((CASE
                    WHEN ISNULL(dbo.Corp_Bill_Mst.CBill_NO, '''') = ''''
                        THEN dbo.fn_GetCorpBillNo(dbo.Billing_Mst.Bill_ID, dbo.Billing_Mst.visit_id)
                    ELSE ISNULL(dbo.Corp_Bill_Mst.CBill_NO, '''')
                END), '''') AS Bill_No,
                dbo.Billing_Mst.BillDate,
                dbo.Billing_Mst.NetAmount,
                dbo.Visit.Visit_ID,
                dbo.Visit.PatientID,
                ISNULL(dbo.Corp_Bill_Mst.Submit_Date, 0) AS Submit_Date,
                ISNULL(dbo.Corp_Bill_Mst.CBill_ID, 0) AS CBill_ID,
                ISNULL(dbo.Corp_Bill_Mst.CAmount, 0) AS CAmount,
                ISNULL((CASE
                    WHEN ISNULL(dbo.Corp_Bill_Mst.Status, '''') = ''''
                        THEN dbo.fn_GetCorpBillStatus(dbo.Billing_Mst.Bill_ID, dbo.Billing_Mst.visit_id)
                    ELSE ISNULL(dbo.Corp_Bill_Mst.Status, '''')
                END), '''') AS Status,
                dbo.Corp_Bill_Mst.Doc_nm,
                dbo.Corp_Bill_Mst.Bill_No AS cmbillNo,
                dbo.PatientSubType_Mst.PatientSubType_Desc
            FROM dbo.Visit
            LEFT OUTER JOIN dbo.PatientSubType_Mst
                ON dbo.Visit.PatientSubType_ID = dbo.PatientSubType_Mst.PatientSubType_ID
            LEFT OUTER JOIN dbo.PatientType_mst
                ON dbo.Visit.PatientType_ID = dbo.PatientType_mst.PatientType_ID
            LEFT OUTER JOIN dbo.Patient
                ON dbo.Visit.PatientID = dbo.Patient.PatientId
            RIGHT OUTER JOIN dbo.Billing_Mst
                ON dbo.Visit.Visit_ID = dbo.Billing_Mst.Visit_ID
            LEFT OUTER JOIN dbo.Corp_Bill_Mst
                ON dbo.Billing_Mst.Bill_ID = dbo.Corp_Bill_Mst.Bill_ID
            WHERE
                dbo.Visit.VisitTypeID = 2
                AND dbo.Billing_Mst.BillType = ''P''
                AND dbo.Visit.PatientType_ID = @ptypeid
                AND dbo.Visit.PatientSubType_ID = @pSubtypeid
                AND dbo.Visit.DepartmentID <> 7
                AND ISNULL(dbo.Billing_Mst.CancelStatus, 0) = 0
                AND NOT EXISTS
                (
                    SELECT 1
                    FROM dbo.Visit_Duplicate vd2 WITH (NOLOCK)
                    WHERE vd2.sourceVisitId = dbo.Visit.Visit_ID
                )

            UNION ALL

            SELECT
                bm.Bill_ID,
                ISNULL(vd.sourceVisitNo, src.VisitNo) AS VisitNo,
                p.Registration_No,
                dbo.fn_PatientFullName(vd.patientId) AS PatientName,
                vd.visitDate,
                vd.dischargeDate,
                ISNULL((CASE
                    WHEN ISNULL(cb.CBill_NO, '''') = ''''
                        THEN dbo.fn_GetCorpBillNo(bm.Bill_ID, vd.visitId)
                    ELSE ISNULL(cb.CBill_NO, '''')
                END), '''') AS Bill_No,
                bm.BillDate,
                bm.NetAmount,
                vd.visitId AS Visit_ID,
                vd.patientId AS PatientID,
                ISNULL(cb.Submit_Date, 0) AS Submit_Date,
                ISNULL(cb.CBill_ID, 0) AS CBill_ID,
                ISNULL(cb.CAmount, 0) AS CAmount,
                ISNULL((CASE
                    WHEN ISNULL(cb.Status, '''') = ''''
                        THEN dbo.fn_GetCorpBillStatus(bm.Bill_ID, vd.visitId)
                    ELSE ISNULL(cb.Status, '''')
                END), '''') AS Status,
                cb.Doc_nm,
                cb.Bill_No AS cmbillNo,
                pst.PatientSubType_Desc
            FROM dbo.Visit_Duplicate vd
            INNER JOIN dbo.Visit src
                ON src.Visit_ID = vd.sourceVisitId
            INNER JOIN dbo.Billing_Mst bm
                ON src.Visit_ID = bm.Visit_ID
            LEFT OUTER JOIN dbo.PatientSubType_Mst pst
                ON vd.patientSubTypeId = pst.PatientSubType_ID
            LEFT OUTER JOIN dbo.PatientType_mst pt
                ON pt.PatientType_ID = ISNULL(vd.patientTypeId, pst.PatientType_ID)
            LEFT OUTER JOIN dbo.Patient p
                ON vd.patientId = p.PatientId
            LEFT OUTER JOIN dbo.Corp_Bill_Mst cb
                ON cb.duplicateVisitId = vd.visitId
            WHERE
                vd.visitTypeId = 2
                AND bm.BillType = ''P''
                AND ISNULL(vd.patientTypeId, pst.PatientType_ID) = @ptypeid
                AND vd.patientSubTypeId = @pSubtypeid
                AND src.DepartmentID <> 7
                AND ISNULL(bm.CancelStatus, 0) = 0
        ) AS BillingRows
        ORDER BY VisitDate, Visit_ID;
    END
    ELSE IF @visitypeid = 3
    BEGIN
        SELECT *
        FROM
        (
            SELECT
                dbo.Billing_Mst.Bill_ID,
                dbo.Visit.VisitNo,
                dbo.Patient.Registration_No,
                dbo.fn_PatientFullName(dbo.Visit.PatientID) AS PatientName,
                dbo.Visit.VisitDate,
                dbo.Visit.DischargeDate,
                ISNULL((CASE
                    WHEN ISNULL(dbo.Corp_Bill_Mst.CBill_NO, '''') = ''''
                        THEN dbo.fn_GetCorpBillNo(dbo.Billing_Mst.Bill_ID, dbo.Billing_Mst.visit_id)
                    ELSE ISNULL(dbo.Corp_Bill_Mst.CBill_NO, '''')
                END), '''') AS Bill_No,
                dbo.Billing_Mst.BillDate,
                dbo.Billing_Mst.NetAmount,
                dbo.Visit.Visit_ID,
                dbo.Visit.PatientID,
                ISNULL(dbo.Corp_Bill_Mst.Submit_Date, 0) AS Submit_Date,
                ISNULL(dbo.Corp_Bill_Mst.CBill_ID, 0) AS CBill_ID,
                ISNULL(dbo.Corp_Bill_Mst.CAmount, 0) AS CAmount,
                ISNULL((CASE
                    WHEN ISNULL(dbo.Corp_Bill_Mst.Status, '''') = ''''
                        THEN dbo.fn_GetCorpBillStatus(dbo.Billing_Mst.Bill_ID, dbo.Billing_Mst.visit_id)
                    ELSE ISNULL(dbo.Corp_Bill_Mst.Status, '''')
                END), '''') AS Status,
                dbo.Corp_Bill_Mst.Doc_nm,
                dbo.Corp_Bill_Mst.Bill_No AS cmbillNo,
                dbo.PatientSubType_Mst.PatientSubType_Desc
            FROM dbo.Visit
            LEFT OUTER JOIN dbo.PatientSubType_Mst
                ON dbo.Visit.PatientSubType_ID = dbo.PatientSubType_Mst.PatientSubType_ID
            LEFT OUTER JOIN dbo.PatientType_mst
                ON dbo.Visit.PatientType_ID = dbo.PatientType_mst.PatientType_ID
            LEFT OUTER JOIN dbo.Patient
                ON dbo.Visit.PatientID = dbo.Patient.PatientId
            RIGHT OUTER JOIN dbo.Billing_Mst
                ON dbo.Visit.Visit_ID = dbo.Billing_Mst.Visit_ID
            LEFT OUTER JOIN dbo.Corp_Bill_Mst
                ON dbo.Billing_Mst.Bill_ID = dbo.Corp_Bill_Mst.Bill_ID
            WHERE
                dbo.Visit.VisitTypeID = 3
                AND dbo.Billing_Mst.BillType = ''P''
                AND dbo.Visit.PatientType_ID = @ptypeid
                AND dbo.Visit.PatientSubType_ID = @pSubtypeid
                AND dbo.Visit.DepartmentID <> 7
                AND ISNULL(dbo.Billing_Mst.CancelStatus, 0) = 0
                AND NOT EXISTS
                (
                    SELECT 1
                    FROM dbo.Visit_Duplicate vd2 WITH (NOLOCK)
                    WHERE vd2.sourceVisitId = dbo.Visit.Visit_ID
                )

            UNION ALL

            SELECT
                bm.Bill_ID,
                ISNULL(vd.sourceVisitNo, src.VisitNo) AS VisitNo,
                p.Registration_No,
                dbo.fn_PatientFullName(vd.patientId) AS PatientName,
                vd.visitDate,
                vd.dischargeDate,
                ISNULL((CASE
                    WHEN ISNULL(cb.CBill_NO, '''') = ''''
                        THEN dbo.fn_GetCorpBillNo(bm.Bill_ID, vd.visitId)
                    ELSE ISNULL(cb.CBill_NO, '''')
                END), '''') AS Bill_No,
                bm.BillDate,
                bm.NetAmount,
                vd.visitId AS Visit_ID,
                vd.patientId AS PatientID,
                ISNULL(cb.Submit_Date, 0) AS Submit_Date,
                ISNULL(cb.CBill_ID, 0) AS CBill_ID,
                ISNULL(cb.CAmount, 0) AS CAmount,
                ISNULL((CASE
                    WHEN ISNULL(cb.Status, '''') = ''''
                        THEN dbo.fn_GetCorpBillStatus(bm.Bill_ID, vd.visitId)
                    ELSE ISNULL(cb.Status, '''')
                END), '''') AS Status,
                cb.Doc_nm,
                cb.Bill_No AS cmbillNo,
                pst.PatientSubType_Desc
            FROM dbo.Visit_Duplicate vd
            INNER JOIN dbo.Visit src
                ON src.Visit_ID = vd.sourceVisitId
            INNER JOIN dbo.Billing_Mst bm
                ON src.Visit_ID = bm.Visit_ID
            LEFT OUTER JOIN dbo.PatientSubType_Mst pst
                ON vd.patientSubTypeId = pst.PatientSubType_ID
            LEFT OUTER JOIN dbo.PatientType_mst pt
                ON pt.PatientType_ID = ISNULL(vd.patientTypeId, pst.PatientType_ID)
            LEFT OUTER JOIN dbo.Patient p
                ON vd.patientId = p.PatientId
            LEFT OUTER JOIN dbo.Corp_Bill_Mst cb
                ON cb.duplicateVisitId = vd.visitId
            WHERE
                vd.visitTypeId = 3
                AND bm.BillType = ''P''
                AND ISNULL(vd.patientTypeId, pst.PatientType_ID) = @ptypeid
                AND vd.patientSubTypeId = @pSubtypeid
                AND src.DepartmentID <> 7
                AND ISNULL(bm.CancelStatus, 0) = 0
        ) AS BillingRows
        ORDER BY VisitDate, Visit_ID;
    END
    ELSE IF @visitypeid = 6
    BEGIN
        SELECT *
        FROM
        (
            SELECT
                dbo.Billing_Mst.Bill_ID,
                dbo.Visit.VisitNo,
                dbo.Patient.Registration_No,
                dbo.fn_PatientFullName(dbo.Visit.PatientID) AS PatientName,
                dbo.Visit.VisitDate,
                dbo.Visit.DischargeDate,
                ISNULL(dbo.Corp_Bill_Mst.CBill_NO, '''') AS Bill_No,
                dbo.Billing_Mst.BillDate,
                dbo.Billing_Mst.NetAmount,
                dbo.Visit.Visit_ID,
                dbo.Visit.PatientID,
                ISNULL(dbo.Corp_Bill_Mst.Submit_Date, 0) AS Submit_Date,
                ISNULL(dbo.Corp_Bill_Mst.CBill_ID, 0) AS CBill_ID,
                ISNULL(dbo.Corp_Bill_Mst.CAmount, 0) AS CAmount,
                ISNULL(dbo.Corp_Bill_Mst.Status, '''') AS Status,
                dbo.Corp_Bill_Mst.Doc_nm,
                dbo.Corp_Bill_Mst.Bill_No AS cmbillNo,
                dbo.PatientSubType_Mst.PatientSubType_Desc
            FROM dbo.Visit
            LEFT OUTER JOIN dbo.PatientSubType_Mst
                ON dbo.Visit.PatientSubType_ID = dbo.PatientSubType_Mst.PatientSubType_ID
            LEFT OUTER JOIN dbo.PatientType_mst
                ON dbo.Visit.PatientType_ID = dbo.PatientType_mst.PatientType_ID
            LEFT OUTER JOIN dbo.Patient
                ON dbo.Visit.PatientID = dbo.Patient.PatientId
            RIGHT OUTER JOIN dbo.Billing_Mst
                ON dbo.Visit.Visit_ID = dbo.Billing_Mst.Visit_ID
            LEFT OUTER JOIN dbo.Corp_Bill_Mst
                ON dbo.Billing_Mst.Bill_ID = dbo.Corp_Bill_Mst.Bill_ID
            WHERE
                dbo.Visit.VisitTypeID = 6
                AND dbo.Billing_Mst.BillType = ''P''
                AND dbo.Visit.PatientType_ID = @ptypeid
                AND dbo.Visit.PatientSubType_ID = @pSubtypeid
                AND dbo.Visit.DepartmentID <> 7
                AND ISNULL(dbo.Billing_Mst.CancelStatus, 0) = 0
                AND NOT EXISTS
                (
                    SELECT 1
                    FROM dbo.Visit_Duplicate vd2 WITH (NOLOCK)
                    WHERE vd2.sourceVisitId = dbo.Visit.Visit_ID
                )

            UNION ALL

            SELECT
                bm.Bill_ID,
                ISNULL(vd.sourceVisitNo, src.VisitNo) AS VisitNo,
                p.Registration_No,
                dbo.fn_PatientFullName(vd.patientId) AS PatientName,
                vd.visitDate,
                vd.dischargeDate,
                ISNULL(cb.CBill_NO, '''') AS Bill_No,
                bm.BillDate,
                bm.NetAmount,
                vd.visitId AS Visit_ID,
                vd.patientId AS PatientID,
                ISNULL(cb.Submit_Date, 0) AS Submit_Date,
                ISNULL(cb.CBill_ID, 0) AS CBill_ID,
                ISNULL(cb.CAmount, 0) AS CAmount,
                ISNULL(cb.Status, '''') AS Status,
                cb.Doc_nm,
                cb.Bill_No AS cmbillNo,
                pst.PatientSubType_Desc
            FROM dbo.Visit_Duplicate vd
            INNER JOIN dbo.Visit src
                ON src.Visit_ID = vd.sourceVisitId
            INNER JOIN dbo.Billing_Mst bm
                ON src.Visit_ID = bm.Visit_ID
            LEFT OUTER JOIN dbo.PatientSubType_Mst pst
                ON vd.patientSubTypeId = pst.PatientSubType_ID
            LEFT OUTER JOIN dbo.PatientType_mst pt
                ON pt.PatientType_ID = ISNULL(vd.patientTypeId, pst.PatientType_ID)
            LEFT OUTER JOIN dbo.Patient p
                ON vd.patientId = p.PatientId
            LEFT OUTER JOIN dbo.Corp_Bill_Mst cb
                ON cb.duplicateVisitId = vd.visitId
            WHERE
                vd.visitTypeId = 6
                AND bm.BillType = ''P''
                AND ISNULL(vd.patientTypeId, pst.PatientType_ID) = @ptypeid
                AND vd.patientSubTypeId = @pSubtypeid
                AND src.DepartmentID <> 7
                AND ISNULL(bm.CancelStatus, 0) = 0
        ) AS BillingRows
        ORDER BY DischargeDate, Visit_ID;
    END
END
');
