/* Corporate Bill Summary paging SP (ACI/AHL) */
CREATE OR ALTER PROCEDURE dbo.usp_CorpBillSummary_Page
 @FromDate DATE = NULL,
 @ToDate DATE = NULL,
 @VisitType INT = 0,
 @StatusFilter NVARCHAR(20) = N'all',
 @PatientSubtype NVARCHAR(200) = N'',
 @SearchQuery NVARCHAR(200) = N'',
 @Page INT = 1,
 @PageSize INT = 25
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @PageSafe INT = CASE WHEN ISNULL(@Page, 1) < 1 THEN 1 ELSE @Page END;
    DECLARE @PageSizeSafe INT = CASE
        WHEN ISNULL(@PageSize, 25) < 10 THEN 10
        WHEN @PageSize > 200 THEN 200
        ELSE @PageSize
    END;
    DECLARE @StatusNorm NVARCHAR(20) = LOWER(LTRIM(RTRIM(ISNULL(@StatusFilter, N'all'))));
    DECLARE @SubtypeNorm NVARCHAR(200) = LOWER(LTRIM(RTRIM(ISNULL(@PatientSubtype, N''))));
    DECLARE @SearchLower NVARCHAR(200) = LOWER(LTRIM(RTRIM(ISNULL(@SearchQuery, N''))));
    DECLARE @SearchLike NVARCHAR(220) = N'%' + @SearchLower + N'%';

    ;WITH base AS (
        SELECT
            CAST(ISNULL(cb.CBill_ID, 0) AS INT) AS CBill_ID,
            CAST(NULLIF(COALESCE(v.Visit_ID, bm.Visit_ID, cb.Visit_ID), 0) AS INT) AS Visit_ID,
            CAST(NULLIF(COALESCE(v.PatientID, cb.PatientID), 0) AS INT) AS PatientID,
            CAST(ISNULL(bm.Bill_ID, 0) AS INT) AS Bill_ID,
            CAST(bm.BillDate AS DATETIME) AS BillDate,
            CAST(cb.CBill_Date AS DATETIME) AS CBill_Date,
            CAST(cb.Submit_Date AS DATETIME) AS Submit_Date,
            CAST(ISNULL(cb.CAmount, 0) AS FLOAT) AS CAmount,
            CAST(ISNULL(cb.Due_Amt, ISNULL(cb.dueAmount, 0)) AS FLOAT) AS DueAmount,
            CAST(ISNULL(cb.Old_Bill_Amt, 0) AS FLOAT) AS Old_Bill_Amt,
            CAST(cb.Old_Bill_Date AS DATETIME) AS Old_Bill_Date,
            LTRIM(RTRIM(ISNULL(CONVERT(NVARCHAR(100), cb.Status), N''))) AS StatusRaw,
            CASE
                WHEN UPPER(LTRIM(RTRIM(ISNULL(CONVERT(NVARCHAR(100), cb.Status), N'')))) IN (N'Y', N'1', N'TRUE', N'YES', N'FINAL', N'FINAL SUBMITTED')
                     OR UPPER(LTRIM(RTRIM(ISNULL(CONVERT(NVARCHAR(100), cb.Status), N'')))) LIKE N'%FINAL%'
                    THEN 1
                ELSE 0
            END AS IsFinalStatus,
            ISNULL(
                NULLIF(CONVERT(NVARCHAR(80), cb.CBill_NO), N''),
                ISNULL(NULLIF(CONVERT(NVARCHAR(80), cb.Bill_No), N''), CONVERT(NVARCHAR(80), bm.Bill_ID))
            ) AS BillNo,
            CAST(ISNULL(v.VisitTypeID, 0) AS INT) AS VisitTypeID,
            ISNULL(CONVERT(NVARCHAR(120), v.TypeOfVisit), N'') AS TypeOfVisit,
            v.VisitDate,
            v.DischargeDate,
            CAST(NULLIF(v.PatientID, 0) AS INT) AS VisitPatientID,
            CAST(NULLIF(v.PatientType_ID, 0) AS INT) AS VisitPatientTypeID,
            CAST(NULLIF(v.PatientSubType_ID, 0) AS INT) AS VisitPatientSubTypeID,
            CAST(NULLIF(v.DocInCharge, 0) AS INT) AS DocInChargeID,
            CAST(NULLIF(v.DepartmentID, 0) AS INT) AS DepartmentID,
            CAST(ISNULL(v.DischargeType, 0) AS INT) AS DischargeTypeID,
            CASE
                WHEN UPPER(LTRIM(RTRIM(CONVERT(NVARCHAR(20), ISNULL(bm.CancelStatus, N''))))) IN (N'1', N'TRUE', N'YES', N'Y')
                    THEN 1
                ELSE 0
            END AS CancelStatusNorm,
            CASE
                WHEN ISNULL(v.VisitTypeID, 0) = 1 OR UPPER(ISNULL(v.TypeOfVisit, N'')) LIKE N'%IPD%' THEN 1
                ELSE 0
            END AS IsIPDLike,
            CASE
                WHEN ISNULL(v.VisitTypeID, 0) = 2 OR UPPER(ISNULL(v.TypeOfVisit, N'')) LIKE N'%OPD%' THEN 1
                ELSE 0
            END AS IsOPDLike,
            CASE
                WHEN ISNULL(v.VisitTypeID, 0) = 3
                     OR UPPER(ISNULL(v.TypeOfVisit, N'')) LIKE N'%DPV%'
                     OR UPPER(ISNULL(v.TypeOfVisit, N'')) LIKE N'%DAY%'
                    THEN 1
                ELSE 0
            END AS IsDPVLike,
            CASE
                WHEN v.PatientID IS NULL THEN N''
                ELSE ISNULL(dbo.fn_regno(v.PatientID), N'')
            END AS Registration_No,
            CASE
                WHEN v.PatientSubType_ID IS NULL THEN N''
                ELSE ISNULL(dbo.fn_patsub_type(v.PatientSubType_ID), N'')
            END AS PatientSubTypeName
        FROM dbo.Billing_Mst bm WITH (NOLOCK)
        LEFT JOIN dbo.Visit v WITH (NOLOCK)
            ON v.Visit_ID = bm.Visit_ID
        LEFT JOIN dbo.Corp_Bill_Mst cb WITH (NOLOCK)
            ON cb.Bill_ID = bm.Bill_ID
        WHERE
            UPPER(LTRIM(RTRIM(ISNULL(CONVERT(NVARCHAR(30), bm.BillType), N'')))) = N'P'
            AND v.Visit_ID IS NOT NULL
            AND ISNULL(v.DepartmentID, 0) <> 7
    )
    SELECT
        b.CBill_ID,
        b.Visit_ID,
        b.PatientID,
        b.Bill_ID,
        b.BillDate,
        b.CBill_Date,
        b.Submit_Date,
        b.CAmount,
        b.DueAmount,
        b.Old_Bill_Amt,
        b.Old_Bill_Date,
        b.StatusRaw,
        b.IsFinalStatus,
        b.BillNo,
        b.VisitTypeID,
        b.TypeOfVisit,
        b.VisitDate,
        b.DischargeDate,
        b.VisitPatientID,
        b.VisitPatientTypeID,
        b.VisitPatientSubTypeID,
        b.DocInChargeID,
        b.DepartmentID,
        b.Registration_No,
        b.PatientSubTypeName
    INTO #corp_bill_filtered
    FROM base b
    WHERE
        b.BillDate IS NOT NULL
        AND (@FromDate IS NULL OR CAST(b.BillDate AS DATE) >= @FromDate)
        AND (@ToDate IS NULL OR CAST(b.BillDate AS DATE) <= @ToDate)
        AND (
            (@VisitType = 0 AND (
                (ISNULL(b.IsIPDLike, 0) = 1 AND ISNULL(b.DischargeTypeID, 0) = 2 AND ISNULL(b.CancelStatusNorm, 0) = 0)
                OR (ISNULL(b.IsOPDLike, 0) = 1 AND ISNULL(b.CancelStatusNorm, 0) = 0)
                OR (ISNULL(b.IsDPVLike, 0) = 1)
            ))
            OR (@VisitType = 1 AND ISNULL(b.IsIPDLike, 0) = 1 AND ISNULL(b.DischargeTypeID, 0) = 2 AND ISNULL(b.CancelStatusNorm, 0) = 0)
            OR (@VisitType = 2 AND ISNULL(b.IsOPDLike, 0) = 1 AND ISNULL(b.CancelStatusNorm, 0) = 0)
            OR (@VisitType = 3 AND ISNULL(b.IsDPVLike, 0) = 1)
        )
        AND (
            @StatusNorm = N'all'
            OR (@StatusNorm = N'final' AND ISNULL(b.IsFinalStatus, 0) = 1)
            OR (@StatusNorm = N'nonfinal' AND ISNULL(b.IsFinalStatus, 0) = 0)
        )
        AND (
            @SubtypeNorm = N''
            OR LOWER(ISNULL(b.PatientSubTypeName, N'')) = @SubtypeNorm
        )
        AND (
            @SearchLower = N''
            OR LOWER(ISNULL(b.BillNo, N'')) LIKE @SearchLike
            OR LOWER(ISNULL(b.Registration_No, N'')) LIKE @SearchLike
            OR LOWER(ISNULL(b.TypeOfVisit, N'')) LIKE @SearchLike
            OR LOWER(ISNULL(b.PatientSubTypeName, N'')) LIKE @SearchLike
            OR LOWER(CONVERT(NVARCHAR(40), ISNULL(b.Bill_ID, 0))) LIKE @SearchLike
            OR LOWER(CONVERT(NVARCHAR(40), ISNULL(b.CBill_ID, 0))) LIKE @SearchLike
            OR LOWER(CONVERT(NVARCHAR(40), ISNULL(b.Visit_ID, 0))) LIKE @SearchLike
            OR LOWER(CONVERT(NVARCHAR(40), ISNULL(b.PatientID, 0))) LIKE @SearchLike
            OR LOWER(
                CASE
                    WHEN COALESCE(b.VisitPatientID, b.PatientID) IS NULL THEN N''
                    ELSE ISNULL(dbo.fn_patientfullname(COALESCE(b.VisitPatientID, b.PatientID)), N'')
                END
            ) LIKE @SearchLike
        );

    DECLARE @TotalRows INT = (SELECT COUNT(1) FROM #corp_bill_filtered);
    DECLARE @TotalPages INT = CASE WHEN @TotalRows <= 0 THEN 1 ELSE CEILING(@TotalRows * 1.0 / @PageSizeSafe) END;
    IF @PageSafe > @TotalPages SET @PageSafe = @TotalPages;

    ;WITH numbered AS (
        SELECT
            f.*,
            ROW_NUMBER() OVER (ORDER BY f.BillDate DESC, f.CBill_ID DESC, f.Bill_ID DESC) AS rn
        FROM #corp_bill_filtered f
    )
    SELECT
        n.CBill_ID,
        n.Visit_ID,
        n.PatientID,
        ISNULL(NULLIF(n.Registration_No, N''), N'Unknown') AS Registration_No,
        ISNULL(NULLIF(n.BillNo, N''), CONVERT(NVARCHAR(80), ISNULL(NULLIF(n.Bill_ID, 0), n.CBill_ID))) AS BillNo,
        CAST(ISNULL(n.CAmount, 0) AS FLOAT) AS CAmount,
        CAST(ISNULL(n.DueAmount, 0) AS FLOAT) AS DueAmount,
        CAST(ISNULL(n.Old_Bill_Amt, 0) AS FLOAT) AS Old_Bill_Amt,
        n.Old_Bill_Date,
        CASE
            WHEN ISNULL(n.IsFinalStatus, 0) = 1 THEN N'Final Submitted'
            WHEN UPPER(LTRIM(RTRIM(ISNULL(n.StatusRaw, N'')))) = N'N' THEN N'Submission Pending'
            WHEN LTRIM(RTRIM(ISNULL(n.StatusRaw, N''))) = N'' THEN N'Not Worked'
            ELSE N'Submission Pending'
        END AS [Status],
        n.BillDate,
        n.CBill_Date,
        n.Submit_Date,
        n.VisitDate,
        n.DischargeDate,
        CASE
            WHEN COALESCE(n.VisitPatientID, n.PatientID) IS NULL THEN N''
            ELSE ISNULL(dbo.fn_patientfullname(COALESCE(n.VisitPatientID, n.PatientID)), N'')
        END AS PatientName,
        ISNULL(n.TypeOfVisit, N'') AS TypeOfVisit,
        CASE
            WHEN n.VisitPatientTypeID IS NULL THEN N''
            ELSE ISNULL(dbo.fn_pat_type(n.VisitPatientTypeID), N'')
        END AS PatientType,
        ISNULL(n.PatientSubTypeName, N'') AS PatientSubType,
        CASE
            WHEN n.DocInChargeID IS NULL THEN N''
            ELSE ISNULL(dbo.fn_doctorfirstname(n.DocInChargeID), N'')
        END AS DocInCharge,
        CASE
            WHEN n.DepartmentID IS NULL THEN N''
            ELSE ISNULL(dbo.fn_dept(n.DepartmentID), N'')
        END AS Dept
    FROM numbered n
    WHERE n.rn BETWEEN ((@PageSafe - 1) * @PageSizeSafe + 1) AND (@PageSafe * @PageSizeSafe)
    ORDER BY n.rn;

    SELECT
        @PageSafe AS page,
        @PageSizeSafe AS page_size,
        @TotalRows AS total_rows,
        @TotalPages AS total_pages,
        CAST(N'sql_sp' AS NVARCHAR(30)) AS query_engine;

    SELECT DISTINCT
        LTRIM(RTRIM(ISNULL(PatientSubTypeName, N''))) AS patient_subtype
    FROM #corp_bill_filtered
    WHERE LTRIM(RTRIM(ISNULL(PatientSubTypeName, N''))) <> N''
    ORDER BY patient_subtype;
END;
GO
