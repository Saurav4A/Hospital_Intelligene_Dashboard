/* Corporate reconciliation receipt action desk paging SP */
IF OBJECT_ID(N'dbo.usp_CorpRecon_ReceiptAction_Page', N'P') IS NULL
    EXEC(N'CREATE PROCEDURE dbo.usp_CorpRecon_ReceiptAction_Page AS BEGIN SET NOCOUNT ON; END;');
GO
ALTER PROCEDURE dbo.usp_CorpRecon_ReceiptAction_Page
    @CutoffDate DATE = '2025-03-31',
    @Q NVARCHAR(200) = N'',
    @BillSource NVARCHAR(120) = N'',
    @Page INT = 1,
    @PageSize INT = 25
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @PageSafe INT = CASE WHEN ISNULL(@Page, 1) < 1 THEN 1 ELSE @Page END;
    DECLARE @PageSizeSafe INT = CASE WHEN ISNULL(@PageSize, 25) < 10 THEN 10 WHEN @PageSize > 500 THEN 500 ELSE @PageSize END;
    DECLARE @QSafe NVARCHAR(200) = LOWER(LTRIM(RTRIM(ISNULL(@Q, N''))));
    DECLARE @BillSourceSafe NVARCHAR(120) = LOWER(LTRIM(RTRIM(ISNULL(@BillSource, N''))));

    CREATE TABLE #q_terms (
        seq INT NOT NULL,
        term NVARCHAR(200) NOT NULL PRIMARY KEY
    );

    DECLARE @QWork NVARCHAR(400) = REPLACE(REPLACE(REPLACE(@QSafe, CHAR(9), N','), CHAR(10), N','), CHAR(13), N',');
    DECLARE @CommaPos INT;
    DECLARE @Part NVARCHAR(200);
    DECLARE @Seq INT = 0;

    IF CHARINDEX(N',', @QWork) > 0
    BEGIN
        SET @QWork = @QWork + N',';
        WHILE LEN(@QWork) > 0
        BEGIN
            SET @CommaPos = CHARINDEX(N',', @QWork);
            IF @CommaPos <= 0 BREAK;
            SET @Part = LTRIM(RTRIM(LEFT(@QWork, @CommaPos - 1)));
            IF @Part <> N'' AND NOT EXISTS (SELECT 1 FROM #q_terms WHERE term = @Part)
            BEGIN
                SET @Seq = @Seq + 1;
                INSERT INTO #q_terms(seq, term) VALUES (@Seq, @Part);
            END;
            SET @QWork = SUBSTRING(@QWork, @CommaPos + 1, LEN(@QWork));
        END;
    END;

    DECLARE @HasTermList BIT = CASE WHEN EXISTS (SELECT 1 FROM #q_terms) THEN 1 ELSE 0 END;
    DECLARE @QCompact NVARCHAR(200) = REPLACE(@QSafe, N' ', N'');
    DECLARE @IsSingleTokenSearch BIT = CASE WHEN @QSafe <> N'' AND @HasTermList = 0 AND CHARINDEX(N' ', @QSafe) = 0 THEN 1 ELSE 0 END;
    DECLARE @IsNumericSingleSearch BIT = CASE WHEN @IsSingleTokenSearch = 1 AND @QSafe NOT LIKE N'%[^0-9]%' THEN 1 ELSE 0 END;
    DECLARE @IsTargetedTokenSearch BIT = CASE WHEN @IsSingleTokenSearch = 1 AND (@IsNumericSingleSearch = 1 OR PATINDEX(N'%[0-9]%', @QSafe) > 0 OR CHARINDEX(N'-', @QSafe) > 0 OR CHARINDEX(N'/', @QSafe) > 0) THEN 1 ELSE 0 END;
    DECLARE @QInt INT = CASE WHEN @IsNumericSingleSearch = 1 AND LEN(@QSafe) <= 10 THEN CAST(@QSafe AS INT) ELSE NULL END;

    ;WITH bill_mst AS (
        SELECT
            CAST(b.CBill_ID AS INT) AS BillId,
            N'BILL_MST_POST' AS BillSourceKey,
            N'Corporate Bill' AS BillSource,
            CAST(COALESCE(b.Submit_Date, b.CBill_Date) AS DATETIME) AS BillDate,
            CAST(b.Submit_Date AS DATETIME) AS SubmitDateRaw,
            CAST(b.CBill_Date AS DATETIME) AS CBillDateRaw,
            CAST(NULL AS DATETIME) AS DueDate,
            CAST(ISNULL(b.CAmount, 0) AS FLOAT) AS BillAmount,
            CAST(
                ISNULL(
                    NULLIF(CONVERT(NVARCHAR(80), b.CBill_NO), N''),
                    NULLIF(CONVERT(NVARCHAR(80), b.Bill_No), N'')
                ) AS NVARCHAR(80)
            ) AS BillNo,
            CAST(NULLIF(b.PatientID, 0) AS INT) AS PatientId,
            CAST(NULLIF(b.Visit_ID, 0) AS INT) AS VisitId,
            CAST(NULLIF(b.PatientTypeId, 0) AS INT) AS PatientTypeId,
            CAST(NULLIF(b.PatientTypeIdSrNo, 0) AS INT) AS PatientSubTypeId,
            CAST(N'' AS NVARCHAR(255)) AS SourcePatientName,
            CAST(ISNULL(b.Status, N'') AS NVARCHAR(80)) AS BillStatusRaw,
            CAST(ISNULL(b.Due_Amt, ISNULL(b.dueAmount, 0)) AS FLOAT) AS BillDueAmountRaw,
            CAST(NULL AS INT) AS BillUpdatedById,
            CAST(NULL AS DATETIME) AS BillUpdatedOnRaw,
            CAST(0 AS FLOAT) AS OpeningWriteOffAmt
        FROM dbo.Corp_Bill_Mst b WITH (NOLOCK)
        WHERE COALESCE(b.Submit_Date, b.CBill_Date) > @CutoffDate
    ),
    opening AS (
        SELECT
            CAST(o.OPId AS INT) AS BillId,
            N'OPENING' AS BillSourceKey,
            N'Opening Balance' AS BillSource,
            CAST(o.DueDate AS DATETIME) AS BillDate,
            CAST(NULL AS DATETIME) AS SubmitDateRaw,
            CAST(NULL AS DATETIME) AS CBillDateRaw,
            CAST(o.DueDate AS DATETIME) AS DueDate,
            CAST(ISNULL(o.DueAmount, 0) AS FLOAT) AS BillAmount,
            CAST(ISNULL(o.RefNo, N'') AS NVARCHAR(80)) AS BillNo,
            CAST(NULLIF(o.PatientId, 0) AS INT) AS PatientId,
            CAST(NULL AS INT) AS VisitId,
            CAST(NULLIF(o.PatientTypeId, 0) AS INT) AS PatientTypeId,
            CAST(NULLIF(o.PatientSubTypeId, 0) AS INT) AS PatientSubTypeId,
            CAST(ISNULL(o.PatientName, N'') AS NVARCHAR(255)) AS SourcePatientName,
            CAST(N'' AS NVARCHAR(80)) AS BillStatusRaw,
            CAST(ISNULL(o.DueAmount, 0) AS FLOAT) AS BillDueAmountRaw,
            CAST(NULL AS INT) AS BillUpdatedById,
            CAST(NULL AS DATETIME) AS BillUpdatedOnRaw,
            CAST(ISNULL(o.WriteOffAmt, 0) AS FLOAT) AS OpeningWriteOffAmt
        FROM dbo.CorpOpening o WITH (NOLOCK)
    ),
    canonical AS (
        SELECT * FROM bill_mst
        UNION ALL
        SELECT * FROM opening
    )
    SELECT
        c.*,
        LOWER(LTRIM(RTRIM(ISNULL(c.BillNo, N'')))) AS BillNoKey,
        LOWER(REPLACE(LTRIM(RTRIM(ISNULL(c.BillNo, N''))), N' ', N'')) AS BillNoCompactKey
    INTO #canonical_base
    FROM canonical c;

    CREATE CLUSTERED INDEX IX_canonical_base_key ON #canonical_base(BillSourceKey, BillId);
    CREATE NONCLUSTERED INDEX IX_canonical_base_billno_key ON #canonical_base(BillNoKey) INCLUDE (BillId, BillSourceKey, PatientId, VisitId, BillNoCompactKey);
    CREATE NONCLUSTERED INDEX IX_canonical_base_billno_compact ON #canonical_base(BillNoCompactKey) INCLUDE (BillId, BillSourceKey, PatientId, VisitId);
    CREATE NONCLUSTERED INDEX IX_canonical_base_patient ON #canonical_base(PatientId) INCLUDE (BillId, BillSourceKey, BillNo, VisitId);

    CREATE TABLE #scope_seed (
        BillSourceKey NVARCHAR(40) NOT NULL,
        BillId INT NOT NULL,
        PRIMARY KEY (BillSourceKey, BillId)
    );

    IF @HasTermList = 1
    BEGIN
        INSERT INTO #scope_seed(BillSourceKey, BillId)
        SELECT DISTINCT c.BillSourceKey, c.BillId
        FROM #canonical_base c
        INNER JOIN #q_terms qt
            ON qt.term = LOWER(CASE WHEN c.PatientId IS NULL THEN N'' ELSE CONVERT(NVARCHAR(40), c.PatientId) END);
    END
    ELSE IF @IsSingleTokenSearch = 1
    BEGIN
        INSERT INTO #scope_seed(BillSourceKey, BillId)
        SELECT c.BillSourceKey, c.BillId
        FROM #canonical_base c
        WHERE c.BillNoKey = @QSafe;

        IF @QCompact <> @QSafe
        BEGIN
            INSERT INTO #scope_seed(BillSourceKey, BillId)
            SELECT c.BillSourceKey, c.BillId
            FROM #canonical_base c
            WHERE c.BillNoCompactKey = @QCompact
              AND NOT EXISTS (
                    SELECT 1
                    FROM #scope_seed s
                    WHERE s.BillSourceKey = c.BillSourceKey
                      AND s.BillId = c.BillId
              );
        END;

        IF @IsNumericSingleSearch = 1 AND @QInt IS NOT NULL
        BEGIN
            INSERT INTO #scope_seed(BillSourceKey, BillId)
            SELECT c.BillSourceKey, c.BillId
            FROM #canonical_base c
            WHERE (c.BillId = @QInt OR c.PatientId = @QInt OR c.VisitId = @QInt)
              AND NOT EXISTS (
                    SELECT 1
                    FROM #scope_seed s
                    WHERE s.BillSourceKey = c.BillSourceKey
                      AND s.BillId = c.BillId
              );
        END;
    END;

    DECLARE @HasScopeSeed BIT = CASE WHEN EXISTS (SELECT 1 FROM #scope_seed) THEN 1 ELSE 0 END;

    SELECT TOP 0 *
    INTO #scope_base
    FROM #canonical_base;

    IF @HasScopeSeed = 1
    BEGIN
        INSERT INTO #scope_base
        SELECT c.*
        FROM #canonical_base c
        INNER JOIN #scope_seed s
            ON s.BillSourceKey = c.BillSourceKey
           AND s.BillId = c.BillId;
    END
    ELSE IF @HasTermList = 0 AND @IsTargetedTokenSearch = 0
    BEGIN
        INSERT INTO #scope_base
        SELECT *
        FROM #canonical_base;
    END;

    CREATE CLUSTERED INDEX IX_scope_base_key ON #scope_base(BillSourceKey, BillId);

    SELECT
        c.*,
        ISNULL(v.TypeOfVisit, N'') AS TypeOfVisit,
        v.VisitDate,
        v.DischargeDate,
        CASE
            WHEN v.PatientID IS NOT NULL THEN ISNULL(dbo.fn_regno(v.PatientID), N'')
            WHEN c.PatientId IS NOT NULL THEN ISNULL(dbo.fn_regno(c.PatientId), N'')
            ELSE N''
        END AS Registration_No,
        CASE
            WHEN v.PatientID IS NOT NULL THEN ISNULL(dbo.fn_patientfullname(v.PatientID), N'')
            WHEN c.PatientId IS NOT NULL THEN ISNULL(dbo.fn_patientfullname(c.PatientId), N'')
            ELSE ISNULL(c.SourcePatientName, N'')
        END AS PatientName,
        CASE
            WHEN v.PatientType_ID IS NOT NULL THEN ISNULL(dbo.fn_pat_type(v.PatientType_ID), N'')
            WHEN c.PatientTypeId IS NOT NULL THEN ISNULL(dbo.fn_pat_type(c.PatientTypeId), N'')
            ELSE N''
        END AS PatientType,
        CASE
            WHEN v.PatientSubType_ID IS NOT NULL THEN ISNULL(dbo.fn_patsub_type(v.PatientSubType_ID), N'')
            WHEN c.PatientSubTypeId IS NOT NULL THEN ISNULL(dbo.fn_patsub_type(c.PatientSubTypeId), N'')
            ELSE N''
        END AS PatientSubType,
        CASE WHEN v.DepartmentID IS NULL THEN N'' ELSE ISNULL(dbo.fn_dept(v.DepartmentID), N'') END AS Dept,
        CASE WHEN v.UnitID IS NULL THEN N'' ELSE ISNULL(dbo.Fn_subDept(v.UnitID), N'') END AS SubDept,
        CAST(N'' AS NVARCHAR(120)) AS BillUpdatedByName
    INTO #scope
    FROM #scope_base c
    LEFT JOIN dbo.Visit v WITH (NOLOCK) ON v.Visit_ID = c.VisitId;

    CREATE CLUSTERED INDEX IX_scope_key ON #scope(BillId, BillSourceKey);
    CREATE NONCLUSTERED INDEX IX_scope_patient ON #scope(PatientId) INCLUDE (BillSourceKey, BillNo, VisitId, PatientName, Registration_No);

    SELECT TOP 0
        CAST(d.recDtlId AS INT) AS ReceiptDetailId,
        CAST(d.receiptId AS INT) AS ReceiptId,
        CAST(d.billId AS INT) AS BillId,
        CAST(UPPER(ISNULL(NULLIF(LTRIM(RTRIM(CONVERT(NVARCHAR(40), d.BillSourceKey))), N''), N'UNKNOWN')) AS NVARCHAR(40)) AS ReceiptBillSourceKey,
        CAST(ISNULL(d.receiptAmt, 0) AS FLOAT) AS ReceiptAmtDtl,
        CAST(m.Receipt_Date AS DATETIME) AS ReceiptDateNorm,
        CAST(ISNULL(m.Cancelstatus, 0) AS INT) AS CancelStatus,
        CAST(ISNULL(m.rebateDiscountAmt, 0) AS FLOAT) AS RebateDiscountAmt,
        CAST(ISNULL(m.TDSAmt, 0) AS FLOAT) AS TDSAmt,
        CAST(ISNULL(m.WriteOffAmt, 0) AS FLOAT) AS WriteOffAmt
    INTO #receipt_raw
    FROM dbo.Corp_Receipt_Dtl d WITH (NOLOCK)
    LEFT JOIN dbo.Corp_Receipt_Mst m WITH (NOLOCK) ON d.receiptId = m.Receipt_ID
    WHERE 1 = 0;

    IF EXISTS (SELECT 1 FROM #scope)
    BEGIN
        INSERT INTO #receipt_raw
        SELECT
            CAST(d.recDtlId AS INT) AS ReceiptDetailId,
            CAST(d.receiptId AS INT) AS ReceiptId,
            CAST(d.billId AS INT) AS BillId,
            CAST(UPPER(ISNULL(NULLIF(LTRIM(RTRIM(CONVERT(NVARCHAR(40), d.BillSourceKey))), N''), N'UNKNOWN')) AS NVARCHAR(40)) AS ReceiptBillSourceKey,
            CAST(ISNULL(d.receiptAmt, 0) AS FLOAT) AS ReceiptAmtDtl,
            CAST(m.Receipt_Date AS DATETIME) AS ReceiptDateNorm,
            CAST(ISNULL(m.Cancelstatus, 0) AS INT) AS CancelStatus,
            CAST(ISNULL(m.rebateDiscountAmt, 0) AS FLOAT) AS RebateDiscountAmt,
            CAST(ISNULL(m.TDSAmt, 0) AS FLOAT) AS TDSAmt,
            CAST(ISNULL(m.WriteOffAmt, 0) AS FLOAT) AS WriteOffAmt
        FROM dbo.Corp_Receipt_Dtl d WITH (NOLOCK)
        LEFT JOIN dbo.Corp_Receipt_Mst m WITH (NOLOCK) ON d.receiptId = m.Receipt_ID
        INNER JOIN #scope s
            ON s.BillId = CAST(d.billId AS INT)
           AND UPPER(ISNULL(s.BillSourceKey, N'')) = UPPER(ISNULL(NULLIF(LTRIM(RTRIM(CONVERT(NVARCHAR(40), d.BillSourceKey))), N''), N'UNKNOWN'));
    END;

    CREATE CLUSTERED INDEX IX_receipt_raw_key ON #receipt_raw(BillId, ReceiptBillSourceKey, ReceiptId, ReceiptDetailId);

    SELECT *
    INTO #receipt_scope
    FROM (
        SELECT
            rr.*,
            CAST(
                CASE
                    WHEN SUM(rr.ReceiptAmtDtl) OVER (PARTITION BY rr.ReceiptId) > 0
                        THEN rr.RebateDiscountAmt * (rr.ReceiptAmtDtl / NULLIF(SUM(rr.ReceiptAmtDtl) OVER (PARTITION BY rr.ReceiptId), 0))
                    WHEN COUNT(1) OVER (PARTITION BY rr.ReceiptId) > 0
                        THEN rr.RebateDiscountAmt / NULLIF(COUNT(1) OVER (PARTITION BY rr.ReceiptId), 0)
                    ELSE 0
                END AS FLOAT
            ) AS RebateAllocated,
            CAST(
                CASE
                    WHEN SUM(rr.ReceiptAmtDtl) OVER (PARTITION BY rr.ReceiptId) > 0
                        THEN rr.TDSAmt * (rr.ReceiptAmtDtl / NULLIF(SUM(rr.ReceiptAmtDtl) OVER (PARTITION BY rr.ReceiptId), 0))
                    WHEN COUNT(1) OVER (PARTITION BY rr.ReceiptId) > 0
                        THEN rr.TDSAmt / NULLIF(COUNT(1) OVER (PARTITION BY rr.ReceiptId), 0)
                    ELSE 0
                END AS FLOAT
            ) AS TDSAllocated,
            CAST(
                CASE
                    WHEN SUM(rr.ReceiptAmtDtl) OVER (PARTITION BY rr.ReceiptId) > 0
                        THEN rr.WriteOffAmt * (rr.ReceiptAmtDtl / NULLIF(SUM(rr.ReceiptAmtDtl) OVER (PARTITION BY rr.ReceiptId), 0))
                    WHEN COUNT(1) OVER (PARTITION BY rr.ReceiptId) > 0
                        THEN rr.WriteOffAmt / NULLIF(COUNT(1) OVER (PARTITION BY rr.ReceiptId), 0)
                    ELSE 0
                END AS FLOAT
            ) AS WriteOffAllocated
        FROM #receipt_raw rr
        WHERE ISNULL(rr.CancelStatus, 0) <> 1
    ) x;

    SELECT
        ReceiptBillSourceKey,
        BillId,
        SUM(ReceiptAmtDtl) AS receipt_total_all_time,
        SUM(TDSAllocated) AS tds_total_all_time,
        SUM(RebateAllocated) AS rebate_discount_all_time,
        SUM(WriteOffAllocated) AS writeoff_total_all_time,
        COUNT(1) AS receipt_count_all_time,
        MAX(ReceiptDateNorm) AS last_receipt_date_all_time_dt
    INTO #agg_all
    FROM #receipt_scope
    GROUP BY ReceiptBillSourceKey, BillId;

    SELECT
        s.*,
        ISNULL(a.receipt_total_all_time, 0) AS receipt_total_all_time,
        ISNULL(a.tds_total_all_time, 0) AS tds_total_all_time,
        ISNULL(a.rebate_discount_all_time, 0) AS rebate_discount_all_time,
        ISNULL(a.writeoff_total_all_time, 0) + CASE WHEN s.BillSourceKey = N'OPENING' THEN ISNULL(s.OpeningWriteOffAmt, 0) ELSE 0 END AS writeoff_total_all_time,
        ISNULL(a.receipt_count_all_time, 0) AS receipt_count_all_time,
        a.last_receipt_date_all_time_dt
    INTO #rows_all
    FROM #scope s
    LEFT JOIN #agg_all a
        ON a.BillId = s.BillId
       AND UPPER(ISNULL(a.ReceiptBillSourceKey, N'')) = UPPER(ISNULL(s.BillSourceKey, N''));

    ALTER TABLE #rows_all ADD settled_total_all_time FLOAT NULL, balance_all_time FLOAT NULL, status_all_time NVARCHAR(20) NULL;

    UPDATE #rows_all
    SET settled_total_all_time = ISNULL(receipt_total_all_time, 0) + ISNULL(tds_total_all_time, 0) + ISNULL(rebate_discount_all_time, 0) + ISNULL(writeoff_total_all_time, 0),
        balance_all_time = ISNULL(BillAmount, 0) - (ISNULL(receipt_total_all_time, 0) + ISNULL(tds_total_all_time, 0) + ISNULL(rebate_discount_all_time, 0) + ISNULL(writeoff_total_all_time, 0));

    UPDATE #rows_all
    SET status_all_time = CASE
        WHEN balance_all_time < -1 THEN N'Overpaid'
        WHEN ABS(balance_all_time) <= 1 THEN N'Settled'
        WHEN settled_total_all_time > 0 THEN N'Partial'
        ELSE N'Unpaid'
    END;

    SELECT
        r.*,
        CASE
            WHEN @HasTermList = 1 THEN (
                SELECT MIN(qt.seq)
                FROM #q_terms qt
                WHERE qt.term = LOWER(CASE WHEN r.PatientId IS NULL THEN N'' ELSE CONVERT(NVARCHAR(40), r.PatientId) END)
            )
            ELSE NULL
        END AS PatientSearchOrder
    INTO #rows_filtered
    FROM #rows_all r
    WHERE (@BillSourceSafe = N'' OR LOWER(ISNULL(r.BillSource, N'')) = @BillSourceSafe)
      AND (
            UPPER(ISNULL(r.BillSourceKey, N'')) = N'OPENING'
            OR (
                UPPER(ISNULL(r.BillSourceKey, N'')) = N'BILL_MST_POST'
                AND UPPER(ISNULL(r.BillStatusRaw, N'')) IN (N'', N'Y')
            )
      )
      AND (
            @QSafe = N''
            OR (
                @HasTermList = 1
                AND EXISTS (
                    SELECT 1
                    FROM #q_terms qt
                    WHERE qt.term = LOWER(CASE WHEN r.PatientId IS NULL THEN N'' ELSE CONVERT(NVARCHAR(40), r.PatientId) END)
                )
            )
            OR (
                @HasTermList = 0
                AND LOWER(CONCAT(
                    ISNULL(r.BillNo, N''), N' ',
                    ISNULL(r.Registration_No, N''), N' ',
                    ISNULL(r.PatientName, N''), N' ',
                    ISNULL(r.BillSource, N''), N' ',
                    ISNULL(r.TypeOfVisit, N''), N' ',
                    ISNULL(r.PatientType, N''), N' ',
                    ISNULL(r.PatientSubType, N''), N' ',
                    ISNULL(r.Dept, N''), N' ',
                    ISNULL(r.SubDept, N''), N' ',
                    CONVERT(NVARCHAR(30), ISNULL(r.BillId, 0)), N' ',
                    CONVERT(NVARCHAR(30), ISNULL(r.PatientId, 0)), N' ',
                    CONVERT(NVARCHAR(30), ISNULL(r.VisitId, 0))
                )) LIKE N'%' + @QSafe + N'%'
            )
      );

    DECLARE @TotalRows INT = (SELECT COUNT(1) FROM #rows_filtered);
    DECLARE @TotalPages INT = CASE WHEN @TotalRows <= 0 THEN 1 ELSE CEILING(@TotalRows * 1.0 / @PageSizeSafe) END;
    IF @PageSafe > @TotalPages SET @PageSafe = @TotalPages;

    ;WITH ordered AS (
        SELECT
            r.*,
            ROW_NUMBER() OVER (
                ORDER BY
                    CASE WHEN @HasTermList = 1 THEN ISNULL(r.PatientSearchOrder, 2147483647) ELSE 0 END ASC,
                    r.BillDate DESC,
                    r.BillId DESC
            ) AS rn
        FROM #rows_filtered r
    )
    SELECT
        CONCAT(N'BILL-', ISNULL(NULLIF(o.BillSourceKey, N''), N'UNKNOWN'), N'-', CONVERT(NVARCHAR(30), o.BillId)) AS bill_key,
        o.BillId AS bill_id,
        o.BillSourceKey AS bill_source_key,
        o.BillSource AS bill_source,
        o.BillNo AS bill_no,
        o.Registration_No AS registration_no,
        CONVERT(NVARCHAR(10), o.BillDate, 23) AS bill_date,
        CONVERT(NVARCHAR(10), o.SubmitDateRaw, 23) AS submit_date_raw,
        CONVERT(NVARCHAR(10), o.CBillDateRaw, 23) AS c_bill_date_raw,
        CONVERT(NVARCHAR(10), o.DueDate, 23) AS due_date,
        o.PatientId AS patient_id,
        o.PatientName AS patient_name,
        o.PatientType AS patient_type,
        o.PatientSubType AS patient_subtype,
        o.TypeOfVisit AS type_of_visit,
        o.VisitId AS visit_id,
        CONVERT(NVARCHAR(10), o.VisitDate, 23) AS visit_date,
        CONVERT(NVARCHAR(10), o.DischargeDate, 23) AS discharge_date,
        o.Dept AS dept,
        o.SubDept AS sub_dept,
        o.BillAmount AS bill_amount,
        o.receipt_total_all_time,
        o.tds_total_all_time,
        o.rebate_discount_all_time,
        o.writeoff_total_all_time,
        o.settled_total_all_time,
        o.balance_all_time,
        o.status_all_time,
        o.receipt_count_all_time,
        CONVERT(NVARCHAR(10), o.last_receipt_date_all_time_dt, 23) AS last_receipt_date_all_time,
        o.SourcePatientName AS source_patient_name,
        o.BillStatusRaw AS bill_status_raw
    FROM ordered o
    WHERE o.rn BETWEEN ((@PageSafe - 1) * @PageSizeSafe) + 1 AND (@PageSafe * @PageSizeSafe)
    ORDER BY o.rn;

    SELECT
        @PageSafe AS page,
        @PageSizeSafe AS page_size,
        @TotalRows AS total_rows,
        @TotalPages AS total_pages,
        NULLIF(@QSafe, N'') AS q,
        NULLIF(@BillSourceSafe, N'') AS bill_source;

    SELECT DISTINCT LTRIM(RTRIM(ISNULL(BillSource, N''))) AS bill_source
    FROM #rows_filtered
    WHERE LTRIM(RTRIM(ISNULL(BillSource, N''))) <> N''
    ORDER BY bill_source;
END;
GO
