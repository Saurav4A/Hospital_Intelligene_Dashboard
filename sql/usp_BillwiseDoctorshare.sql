CREATE OR ALTER PROCEDURE [dbo].[usp_BillwiseDoctorshare]
(
      @FromDate DATETIME
    , @ToDate   DATETIME
    , @DocId    INT
    , @vtype    INT
)
AS
BEGIN
    SET NOCOUNT ON;
    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

    IF (@FromDate > @ToDate)
    BEGIN
        DECLARE @tmp DATETIME = @FromDate;
        SET @FromDate = @ToDate;
        SET @ToDate   = @tmp;
    END;

    DECLARE @ToDateNextDay DATETIME = DATEADD(DAY, 1, CONVERT(DATE, @ToDate));

    IF (@vtype = 1)
    BEGIN
        ;WITH Base AS
        (
            SELECT
                  v.TypeOfVisit
                , bm.Registration_No
                , bm.BillNo
                , bm.BillDate
                , s.Service_Name
                , bd.Amount
                , PatientID = v.PatientID
                , vtype = v.VisitTypeID
                , pt.PatientType
                , BillQty = bd.Quantity
                , Rate = bd.Rate
                , v.VisitDate
                , v.DischargeDate
                , subdocid = v.subdocid
                , docid = CASE ISNULL(od.DocID, 0) WHEN 0 THEN om.OrdByDocID ELSE od.DocID END
            FROM dbo.Billing_Mst bm
            INNER JOIN dbo.Visit v ON v.Visit_ID = bm.Visit_ID
            INNER JOIN dbo.BillingDetails bd ON bd.Bill_ID = bm.Bill_ID
            INNER JOIN dbo.Service_Mst s ON s.Service_ID = bd.ServiceID AND s.Category_Id IN (1,36,67)
            LEFT JOIN dbo.OrderDtl od ON od.OrdDtlID = bd.OrderDtlId
            LEFT JOIN dbo.OrderMst om ON om.OrdId = od.OrdID
            LEFT JOIN dbo.PatientType_mst pt ON pt.PatientType_ID = v.PatientType_ID
            WHERE bm.CancelStatus = 'false'
              AND bm.BillType = 'P'
              AND bm.BillDate >= @FromDate
              AND bm.BillDate < @ToDateNextDay
              AND ISNULL(bd.Amount,0) > 0
              AND v.VisitTypeID = @vtype
              AND (
                    @DocId = 0
                    OR od.DocID = @DocId
                    OR (ISNULL(od.DocID,0) = 0 AND om.OrdByDocID = @DocId)
                  )
        )
        SELECT
              TypeOfVisit
            , Registration_No
            , BillNo
            , BillDate
            , Service_Name
            , Amount
            , dbo.fn_DoctorFirstName(docid) AS Doctorname
            , dbo.fn_PatientFullName(PatientID) AS Patientname
            , vtype
            , Patienttype
            , BillQty
            , Rate
            , VisitDate
            , DischargeDate
            , dbo.fn_DoctorFirstName(subdocid) AS SecondaryDoc
        FROM Base
        ORDER BY BillDate, BillNo, Registration_No
        OPTION (RECOMPILE);
        RETURN;
    END;

    IF (@vtype IN (2,6))
    BEGIN
        ;WITH Base AS
        (
            SELECT
                  v.TypeOfVisit
                , bm.Registration_No
                , bm.BillNo
                , bm.BillDate
                , s.Service_Name
                , bd.Amount
                , PatientID = v.PatientID
                , vtype = v.VisitTypeID
                , pt.PatientType
                , BillQty = bd.Quantity
                , Rate = bd.Rate
                , v.VisitDate
                , v.DischargeDate
                , subdocid = v.subdocid
                , docid = CASE ISNULL(od.DocID, 0) WHEN 0 THEN v.DocInCharge ELSE od.DocID END
            FROM dbo.Billing_Mst bm
            INNER JOIN dbo.Visit v ON v.Visit_ID = bm.Visit_ID
            INNER JOIN dbo.BillingDetails bd ON bd.Bill_ID = bm.Bill_ID
            INNER JOIN dbo.Service_Mst s ON s.Service_ID = bd.ServiceID AND s.Category_Id IN (1,36,67)
            LEFT JOIN dbo.OrderDtl od ON od.OrdDtlID = bd.OrderDtlId
            LEFT JOIN dbo.PatientType_mst pt ON pt.PatientType_ID = v.PatientType_ID
            WHERE bm.CancelStatus = 'false'
              AND bm.BillType = 'P'
              AND bm.BillDate >= @FromDate
              AND bm.BillDate < @ToDateNextDay
              AND ISNULL(bd.Amount,0) > 0
              AND v.VisitTypeID = @vtype
              AND (
                    @DocId = 0
                    OR od.DocID = @DocId
                    OR (ISNULL(od.DocID,0) = 0 AND v.DocInCharge = @DocId)
                  )
        )
        SELECT
              TypeOfVisit
            , Registration_No
            , BillNo
            , BillDate
            , Service_Name
            , Amount
            , dbo.fn_DoctorFirstName(docid) AS Doctorname
            , dbo.fn_PatientFullName(PatientID) AS Patientname
            , vtype
            , Patienttype
            , BillQty
            , Rate
            , VisitDate
            , DischargeDate
            , dbo.fn_DoctorFirstName(subdocid) AS SecondaryDoc
        FROM Base
        ORDER BY BillDate, BillNo, Registration_No
        OPTION (RECOMPILE);
        RETURN;
    END;

    IF (@vtype = 3)
    BEGIN
        ;WITH Base AS
        (
            SELECT
                  v.TypeOfVisit
                , bm.Registration_No
                , bm.BillNo
                , bm.BillDate
                , s.Service_Name
                , bd.Amount
                , PatientID = v.PatientID
                , vtype = v.VisitTypeID
                , pt.PatientType
                , BillQty = bd.Quantity
                , Rate = bd.Rate
                , v.VisitDate
                , v.DischargeDate
                , subdocid = v.subdocid
                , docid = CASE ISNULL(od.DocID, 0) WHEN 0 THEN v.DocInCharge ELSE od.DocID END
            FROM dbo.Billing_Mst bm
            INNER JOIN dbo.Visit v ON v.Visit_ID = bm.Visit_ID
            INNER JOIN dbo.BillingDetails bd ON bd.Bill_ID = bm.Bill_ID
            INNER JOIN dbo.Service_Mst s ON s.Service_ID = bd.ServiceID
            LEFT JOIN dbo.OrderDtl od ON od.OrdDtlID = bd.OrderDtlId
            LEFT JOIN dbo.PatientType_mst pt ON pt.PatientType_ID = v.PatientType_ID
            WHERE bm.CancelStatus = 'false'
              AND bm.BillType = 'P'
              AND bm.BillDate >= @FromDate
              AND bm.BillDate < @ToDateNextDay
              AND ISNULL(bd.Amount,0) > 0
              AND v.VisitTypeID = @vtype
              AND (
                    @DocId = 0
                    OR od.DocID = @DocId
                    OR (ISNULL(od.DocID,0) = 0 AND v.DocInCharge = @DocId)
                  )
        )
        SELECT
              TypeOfVisit
            , Registration_No
            , BillNo
            , BillDate
            , Service_Name
            , Amount
            , dbo.fn_DoctorFirstName(docid) AS Doctorname
            , dbo.fn_PatientFullName(PatientID) AS Patientname
            , vtype
            , Patienttype
            , BillQty
            , Rate
            , VisitDate
            , DischargeDate
            , dbo.fn_DoctorFirstName(subdocid) AS SecondaryDoc
        FROM Base
        ORDER BY BillDate, BillNo, Registration_No
        OPTION (RECOMPILE);
        RETURN;
    END;

    ;WITH Base AS
    (
        SELECT
              v.TypeOfVisit
            , bm.Registration_No
            , bm.BillNo
            , bm.BillDate
            , s.Service_Name
            , bd.Amount
            , PatientID = v.PatientID
            , vtype = v.VisitTypeID
            , pt.PatientType
            , BillQty = bd.Quantity
            , Rate = bd.Rate
            , v.VisitDate
            , v.DischargeDate
            , subdocid = v.subdocid
            , docid = CASE ISNULL(od.DocID, 0) WHEN 0 THEN om.OrdByDocID ELSE od.DocID END
        FROM dbo.Billing_Mst bm
        INNER JOIN dbo.Visit v ON v.Visit_ID = bm.Visit_ID
        INNER JOIN dbo.BillingDetails bd ON bd.Bill_ID = bm.Bill_ID
        INNER JOIN dbo.Service_Mst s ON s.Service_ID = bd.ServiceID AND s.Category_Id IN (1,36,67)
        LEFT JOIN dbo.OrderDtl od ON od.OrdDtlID = bd.OrderDtlId
        LEFT JOIN dbo.OrderMst om ON om.OrdId = od.OrdID
        LEFT JOIN dbo.PatientType_mst pt ON pt.PatientType_ID = v.PatientType_ID
        WHERE bm.CancelStatus = 'false'
          AND bm.BillType = 'P'
          AND bm.BillDate >= @FromDate
          AND bm.BillDate < @ToDateNextDay
          AND ISNULL(bd.Amount,0) > 0
          AND v.VisitTypeID = 1
          AND (
                @DocId = 0
                OR od.DocID = @DocId
                OR (ISNULL(od.DocID,0) = 0 AND om.OrdByDocID = @DocId)
              )

        UNION ALL

        SELECT
              v.TypeOfVisit
            , bm.Registration_No
            , bm.BillNo
            , bm.BillDate
            , s.Service_Name
            , bd.Amount
            , PatientID = v.PatientID
            , vtype = v.VisitTypeID
            , pt.PatientType
            , BillQty = bd.Quantity
            , Rate = bd.Rate
            , v.VisitDate
            , v.DischargeDate
            , subdocid = v.subdocid
            , docid = CASE ISNULL(od.DocID, 0) WHEN 0 THEN v.DocInCharge ELSE od.DocID END
        FROM dbo.Billing_Mst bm
        INNER JOIN dbo.Visit v ON v.Visit_ID = bm.Visit_ID
        INNER JOIN dbo.BillingDetails bd ON bd.Bill_ID = bm.Bill_ID
        INNER JOIN dbo.Service_Mst s ON s.Service_ID = bd.ServiceID AND s.Category_Id IN (1,36,67)
        LEFT JOIN dbo.OrderDtl od ON od.OrdDtlID = bd.OrderDtlId
        LEFT JOIN dbo.PatientType_mst pt ON pt.PatientType_ID = v.PatientType_ID
        WHERE bm.CancelStatus = 'false'
          AND bm.BillType = 'P'
          AND bm.BillDate >= @FromDate
          AND bm.BillDate < @ToDateNextDay
          AND ISNULL(bd.Amount,0) > 0
          AND v.VisitTypeID IN (2,6)
          AND (
                @DocId = 0
                OR od.DocID = @DocId
                OR (ISNULL(od.DocID,0) = 0 AND v.DocInCharge = @DocId)
              )

        UNION ALL

        SELECT
              v.TypeOfVisit
            , bm.Registration_No
            , bm.BillNo
            , bm.BillDate
            , s.Service_Name
            , bd.Amount
            , PatientID = v.PatientID
            , vtype = v.VisitTypeID
            , pt.PatientType
            , BillQty = bd.Quantity
            , Rate = bd.Rate
            , v.VisitDate
            , v.DischargeDate
            , subdocid = v.subdocid
            , docid = CASE ISNULL(od.DocID, 0) WHEN 0 THEN v.DocInCharge ELSE od.DocID END
        FROM dbo.Billing_Mst bm
        INNER JOIN dbo.Visit v ON v.Visit_ID = bm.Visit_ID
        INNER JOIN dbo.BillingDetails bd ON bd.Bill_ID = bm.Bill_ID
        INNER JOIN dbo.Service_Mst s ON s.Service_ID = bd.ServiceID
        LEFT JOIN dbo.OrderDtl od ON od.OrdDtlID = bd.OrderDtlId
        LEFT JOIN dbo.PatientType_mst pt ON pt.PatientType_ID = v.PatientType_ID
        WHERE bm.CancelStatus = 'false'
          AND bm.BillType = 'P'
          AND bm.BillDate >= @FromDate
          AND bm.BillDate < @ToDateNextDay
          AND ISNULL(bd.Amount,0) > 0
          AND v.VisitTypeID = 3
          AND (
                @DocId = 0
                OR od.DocID = @DocId
                OR (ISNULL(od.DocID,0) = 0 AND v.DocInCharge = @DocId)
              )
    )
    SELECT
          TypeOfVisit
        , Registration_No
        , BillNo
        , BillDate
        , Service_Name
        , Amount
        , dbo.fn_DoctorFirstName(docid) AS Doctorname
        , dbo.fn_PatientFullName(PatientID) AS Patientname
        , vtype
        , Patienttype
        , BillQty
        , Rate
        , VisitDate
        , DischargeDate
        , dbo.fn_DoctorFirstName(subdocid) AS SecondaryDoc
    FROM Base
    ORDER BY BillDate, BillNo, Registration_No
    OPTION (RECOMPILE);
END
