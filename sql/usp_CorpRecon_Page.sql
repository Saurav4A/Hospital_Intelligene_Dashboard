/* Corporate Reconciliation paging SP (hardened, compact) */
IF OBJECT_ID(N'dbo.usp_CorpRecon_Page', N'P') IS NULL
    EXEC(N'CREATE PROCEDURE dbo.usp_CorpRecon_Page AS BEGIN SET NOCOUNT ON; END;');
GO
ALTER PROCEDURE dbo.usp_CorpRecon_Page
 @CutoffDate DATE='2025-03-31',
 @BillFrom DATE=NULL,@BillTo DATE=NULL,@ReceiptFrom DATE=NULL,@ReceiptTo DATE=NULL,
 @IncludeCancelled BIT=0,@Q NVARCHAR(200)=N'',@BillSource NVARCHAR(120)=N'',@PatientSubtype NVARCHAR(200)=N'',@KpiFilter NVARCHAR(64)=N'',
 @SortBy NVARCHAR(64)=N'balance_all_time',@SortDir NVARCHAR(4)=N'desc',@Page INT=1,@PageSize INT=25,
 @ReceiptWriteoffColumn SYSNAME=NULL,@OpeningWriteoffColumn SYSNAME=NULL,@BillAuditedColumn SYSNAME=NULL,
 @BillUpdatedByColumn SYSNAME=NULL,@BillUpdatedOnColumn SYSNAME=NULL,@DtlReceiptDateColumn SYSNAME=NULL,
 @DtlInsertedByColumn SYSNAME=NULL,@RebateDiscountColumn SYSNAME=NULL,@TdsAmountColumn SYSNAME=NULL
AS
BEGIN
 SET NOCOUNT ON;
 DECLARE @PageSafe INT=CASE WHEN ISNULL(@Page,1)<1 THEN 1 ELSE @Page END;
 DECLARE @PageSizeSafe INT=CASE WHEN ISNULL(@PageSize,25)<10 THEN 10 WHEN @PageSize>500 THEN 500 ELSE @PageSize END;
 DECLARE @SortBySafe NVARCHAR(64)=LOWER(LTRIM(RTRIM(ISNULL(@SortBy,N'balance_all_time'))));
 DECLARE @SortDirSafe NVARCHAR(4)=CASE WHEN LOWER(LTRIM(RTRIM(ISNULL(@SortDir,N'desc'))))=N'asc' THEN N'asc' ELSE N'desc' END;
 DECLARE @QSafe NVARCHAR(200)=LOWER(LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(ISNULL(@Q,N''),CHAR(9),N','),CHAR(10),N','),CHAR(13),N','))));
 DECLARE @BillSourceSafe NVARCHAR(120)=LOWER(LTRIM(RTRIM(ISNULL(@BillSource,N''))));
 DECLARE @SubtypeSafe NVARCHAR(200)=LOWER(LTRIM(RTRIM(ISNULL(@PatientSubtype,N''))));
 DECLARE @KpiFilterSafe NVARCHAR(64)=LOWER(LTRIM(RTRIM(ISNULL(@KpiFilter,N''))));
 DECLARE @SubtypeKey NVARCHAR(200)=UPPER(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(ISNULL(@PatientSubtype,N''),N' ',N''),N'-',N''),N'/',N''),N'(',N''),N')',N''),N',',N''),N'.',N''),N'_',N''));
 DECLARE @HasReceiptWriteoff BIT=CASE WHEN NULLIF(LTRIM(RTRIM(ISNULL(@ReceiptWriteoffColumn,N''))),N'') IS NULL THEN 0 ELSE 1 END;
 DECLARE @HasOpeningWriteoff BIT=CASE WHEN NULLIF(LTRIM(RTRIM(ISNULL(@OpeningWriteoffColumn,N''))),N'') IS NULL THEN 0 ELSE 1 END;
 DECLARE @HasBillAudited BIT=CASE WHEN NULLIF(LTRIM(RTRIM(ISNULL(@BillAuditedColumn,N''))),N'') IS NULL THEN 0 ELSE 1 END;
 DECLARE @SettleTolerance FLOAT=1.0;

 CREATE TABLE #q_reg_terms(seq INT NOT NULL,term NVARCHAR(200) NOT NULL PRIMARY KEY);
 DECLARE @QWork NVARCHAR(400)=@QSafe;
 DECLARE @QCommaPos INT;
 DECLARE @QPart NVARCHAR(200);
 DECLARE @QSeq INT=0;
 IF CHARINDEX(N',',@QWork)>0
 BEGIN
  SET @QWork=@QWork+N',';
  WHILE LEN(@QWork)>0
  BEGIN
   SET @QCommaPos=CHARINDEX(N',',@QWork);
   IF @QCommaPos<=0 BREAK;
   SET @QPart=LTRIM(RTRIM(LEFT(@QWork,@QCommaPos-1)));
   IF @QPart<>N'' AND NOT EXISTS (SELECT 1 FROM #q_reg_terms WHERE term=@QPart)
   BEGIN
    SET @QSeq=@QSeq+1;
    INSERT INTO #q_reg_terms(seq,term) VALUES (@QSeq,@QPart);
   END
   SET @QWork=SUBSTRING(@QWork,@QCommaPos+1,LEN(@QWork));
  END
 END;
 DECLARE @HasRegTermList BIT=CASE WHEN EXISTS(SELECT 1 FROM #q_reg_terms) THEN 1 ELSE 0 END;
 DECLARE @QCompact NVARCHAR(200)=REPLACE(@QSafe,N' ',N'');
 DECLARE @IsSingleTokenSearch BIT=CASE WHEN @QSafe<>N'' AND @HasRegTermList=0 AND CHARINDEX(N' ',@QSafe)=0 THEN 1 ELSE 0 END;
 DECLARE @IsNumericSingleSearch BIT=CASE WHEN @IsSingleTokenSearch=1 AND @QSafe NOT LIKE N'%[^0-9]%' THEN 1 ELSE 0 END;
 DECLARE @IsTargetedTokenSearch BIT=CASE WHEN @IsSingleTokenSearch=1 AND (@IsNumericSingleSearch=1 OR PATINDEX(N'%[0-9]%',@QSafe)>0 OR CHARINDEX(N'-',@QSafe)>0 OR CHARINDEX(N'/',@QSafe)>0) THEN 1 ELSE 0 END;
 DECLARE @QInt INT=CASE WHEN @IsNumericSingleSearch=1 AND LEN(@QSafe)<=10 THEN CAST(@QSafe AS INT) ELSE NULL END;

 ;WITH bill_mst AS (
  SELECT CAST(b.CBill_ID AS INT) BillId,N'BILL_MST_POST' BillSourceKey,N'Corporate Bill' BillSource,
         CAST(COALESCE(b.Submit_Date,b.CBill_Date) AS DATETIME) BillDate,CAST(b.Submit_Date AS DATETIME) SubmitDateRaw,CAST(b.CBill_Date AS DATETIME) CBillDateRaw,
         CAST(NULL AS DATETIME) DueDate,CAST(ISNULL(b.CAmount,0) AS FLOAT) BillAmount,
         CAST(ISNULL(NULLIF(CONVERT(NVARCHAR(80),b.CBill_NO),''),NULLIF(CONVERT(NVARCHAR(80),b.Bill_No),'')) AS NVARCHAR(80)) BillNo,
         CAST(NULLIF(b.PatientID,0) AS INT) PatientId,CAST(NULLIF(b.Visit_ID,0) AS INT) VisitId,
         CAST(NULLIF(b.PatientTypeId,0) AS INT) PatientTypeId,CAST(NULLIF(b.PatientTypeIdSrNo,0) AS INT) PatientSubTypeId,
         CAST(N'' AS NVARCHAR(255)) SourcePatientName,CAST(ISNULL(b.Status,'') AS NVARCHAR(80)) BillStatusRaw,
         CAST(ISNULL(b.Due_Amt,ISNULL(b.dueAmount,0)) AS FLOAT) BillDueAmountRaw,
         CAST(CASE WHEN LTRIM(RTRIM(CONVERT(NVARCHAR(20),ISNULL(b.Audited,0)))) IN ('1','Y','y','YES','Yes','TRUE','True','true') THEN 1 ELSE 0 END AS INT) BillAuditedFlag,
         CAST(0 AS FLOAT) OpeningWriteOffAmt
  FROM dbo.Corp_Bill_Mst b WITH (NOLOCK)
  WHERE COALESCE(b.Submit_Date,b.CBill_Date) > @CutoffDate
 ), opening AS (
  SELECT CAST(o.OPId AS INT) BillId,N'OPENING' BillSourceKey,N'Opening Balance' BillSource,
         CAST(o.DueDate AS DATETIME) BillDate,CAST(NULL AS DATETIME) SubmitDateRaw,CAST(NULL AS DATETIME) CBillDateRaw,
         CAST(o.DueDate AS DATETIME) DueDate,CAST(ISNULL(o.DueAmount,0) AS FLOAT) BillAmount,CAST(ISNULL(o.RefNo,'') AS NVARCHAR(80)) BillNo,
         CAST(NULLIF(o.PatientId,0) AS INT) PatientId,CAST(NULL AS INT) VisitId,
         CAST(NULLIF(o.PatientTypeId,0) AS INT) PatientTypeId,CAST(NULLIF(o.PatientSubTypeId,0) AS INT) PatientSubTypeId,
         CAST(ISNULL(o.PatientName,'') AS NVARCHAR(255)) SourcePatientName,CAST(N'' AS NVARCHAR(80)) BillStatusRaw,
         CAST(ISNULL(o.DueAmount,0) AS FLOAT) BillDueAmountRaw,CAST(0 AS INT) BillAuditedFlag,
         CAST(ISNULL(o.WriteOffAmt,0) AS FLOAT) OpeningWriteOffAmt
 FROM dbo.CorpOpening o WITH (NOLOCK)
 ), canonical AS (
  SELECT * FROM bill_mst
  UNION ALL
  SELECT * FROM opening
 )
 SELECT c.*,
        LOWER(LTRIM(RTRIM(ISNULL(c.BillNo,N'')))) BillNoKey,
        LOWER(REPLACE(LTRIM(RTRIM(ISNULL(c.BillNo,N''))),N' ',N'')) BillNoCompactKey
 INTO #canonical_base
 FROM canonical c;

 CREATE CLUSTERED INDEX IX_canonical_base_key ON #canonical_base(BillSourceKey,BillId);
 CREATE NONCLUSTERED INDEX IX_canonical_base_billno_key ON #canonical_base(BillNoKey) INCLUDE (BillId,BillSourceKey,PatientId,VisitId,BillNoCompactKey);
 CREATE NONCLUSTERED INDEX IX_canonical_base_billno_compact ON #canonical_base(BillNoCompactKey) INCLUDE (BillId,BillSourceKey,PatientId,VisitId);
 CREATE NONCLUSTERED INDEX IX_canonical_base_patient ON #canonical_base(PatientId) INCLUDE (BillId,BillSourceKey,BillNo,VisitId);

 CREATE TABLE #scope_seed(
  BillSourceKey NVARCHAR(40) NOT NULL,
  BillId INT NOT NULL,
  PRIMARY KEY(BillSourceKey,BillId)
 );

 IF @HasRegTermList=1
 BEGIN
  INSERT INTO #scope_seed(BillSourceKey,BillId)
  SELECT DISTINCT c.BillSourceKey,c.BillId
  FROM #canonical_base c
  INNER JOIN #q_reg_terms qt
   ON qt.term=LOWER(CASE WHEN c.PatientId IS NULL THEN N'' ELSE CONVERT(NVARCHAR(40),c.PatientId) END);
 END
 ELSE IF @IsSingleTokenSearch=1
 BEGIN
  INSERT INTO #scope_seed(BillSourceKey,BillId)
  SELECT c.BillSourceKey,c.BillId
  FROM #canonical_base c
  WHERE c.BillNoKey=@QSafe;

  IF @QCompact<>@QSafe
  BEGIN
   INSERT INTO #scope_seed(BillSourceKey,BillId)
   SELECT c.BillSourceKey,c.BillId
   FROM #canonical_base c
   WHERE c.BillNoCompactKey=@QCompact
     AND NOT EXISTS (
      SELECT 1
      FROM #scope_seed s
      WHERE s.BillSourceKey=c.BillSourceKey AND s.BillId=c.BillId
     );
  END;

  IF @IsNumericSingleSearch=1 AND @QInt IS NOT NULL
  BEGIN
   INSERT INTO #scope_seed(BillSourceKey,BillId)
   SELECT c.BillSourceKey,c.BillId
   FROM #canonical_base c
   WHERE (c.BillId=@QInt OR c.PatientId=@QInt OR c.VisitId=@QInt)
     AND NOT EXISTS (
      SELECT 1
      FROM #scope_seed s
      WHERE s.BillSourceKey=c.BillSourceKey AND s.BillId=c.BillId
     );
  END;
 END;

 DECLARE @HasScopeSeed BIT=CASE WHEN EXISTS(SELECT 1 FROM #scope_seed) THEN 1 ELSE 0 END;

 SELECT TOP 0 *
 INTO #scope_base
 FROM #canonical_base;

 IF @HasScopeSeed=1
 BEGIN
  INSERT INTO #scope_base
  SELECT c.*
  FROM #canonical_base c
  INNER JOIN #scope_seed s
   ON s.BillSourceKey=c.BillSourceKey
  AND s.BillId=c.BillId;
 END
 ELSE IF @HasRegTermList=0 AND @IsTargetedTokenSearch=0
 BEGIN
  INSERT INTO #scope_base
  SELECT *
  FROM #canonical_base;
 END;

 CREATE CLUSTERED INDEX IX_scope_base_key ON #scope_base(BillSourceKey,BillId);

 SELECT c.*,ISNULL(v.TypeOfVisit,'') TypeOfVisit,v.VisitDate,v.DischargeDate,
        CASE WHEN v.PatientID IS NOT NULL THEN ISNULL(dbo.fn_regno(v.PatientID),'') WHEN c.PatientId IS NOT NULL THEN ISNULL(dbo.fn_regno(c.PatientId),'') ELSE N'' END Registration_No,
        CASE WHEN v.PatientID IS NOT NULL THEN ISNULL(dbo.fn_patientfullname(v.PatientID),'') WHEN c.PatientId IS NOT NULL THEN ISNULL(dbo.fn_patientfullname(c.PatientId),'') ELSE ISNULL(c.SourcePatientName,'') END PatientName,
        CASE WHEN v.PatientType_ID IS NOT NULL THEN ISNULL(dbo.fn_pat_type(v.PatientType_ID),'') WHEN c.PatientTypeId IS NOT NULL THEN ISNULL(dbo.fn_pat_type(c.PatientTypeId),'') ELSE '' END PatientType,
        CASE WHEN v.PatientSubType_ID IS NOT NULL THEN ISNULL(dbo.fn_patsub_type(v.PatientSubType_ID),'') WHEN c.PatientSubTypeId IS NOT NULL THEN ISNULL(dbo.fn_patsub_type(c.PatientSubTypeId),'') ELSE '' END PatientSubType,
        CASE WHEN v.DepartmentID IS NULL THEN '' ELSE ISNULL(dbo.fn_dept(v.DepartmentID),'') END Dept,
        CASE WHEN v.UnitID IS NULL THEN '' ELSE ISNULL(dbo.Fn_subDept(v.UnitID),'') END SubDept,
        CAST(NULL AS INT) BillUpdatedById,CAST('' AS NVARCHAR(120)) BillUpdatedByName,CAST(NULL AS DATETIME) BillUpdatedOnRaw
 INTO #bill_scope
 FROM #scope_base c
 LEFT JOIN dbo.Visit v WITH (NOLOCK) ON v.Visit_ID = c.VisitId;

 CREATE CLUSTERED INDEX IX_bill_scope_key ON #bill_scope(BillId,BillSourceKey);
 CREATE NONCLUSTERED INDEX IX_bill_scope_patient ON #bill_scope(PatientId) INCLUDE (BillSourceKey,BillNo,VisitId,PatientName,Registration_No);

 SELECT BillId,BillSourceKey INTO #suspense_ids
 FROM #bill_scope
 WHERE BillSourceKey=N'BILL_MST_POST' AND UPPER(ISNULL(BillStatusRaw,N''))=N'Y' AND SubmitDateRaw>@CutoffDate AND CBillDateRaw<=@CutoffDate AND ISNULL(BillAuditedFlag,0)<=0;
 CREATE CLUSTERED INDEX IX_suspense_ids_key ON #suspense_ids(BillId,BillSourceKey);
 DECLARE @SuspenseCount INT=(SELECT COUNT(1) FROM #suspense_ids);

 SELECT TOP 0
  CAST(d.recDtlId AS INT) ReceiptDetailId,CAST(d.receiptId AS INT) ReceiptId,CAST(d.billId AS INT) BillId,
  CAST(UPPER(ISNULL(NULLIF(LTRIM(RTRIM(CONVERT(NVARCHAR(40),d.BillSourceKey))),N''),N'UNKNOWN')) AS NVARCHAR(40)) ReceiptBillSourceKey,
  CAST(ISNULL(d.billAmt,0) AS FLOAT) BillAmtDtl,CAST(ISNULL(d.receiptAmt,0) AS FLOAT) ReceiptAmtDtl,CAST(ISNULL(d.dueAmt,0) AS FLOAT) DueAmtDtl,
  CAST(NULLIF(d.visitId,0) AS INT) DtlVisitId,CAST(NULLIF(d.PatientId,0) AS INT) DtlPatientId,
  CAST(NULLIF(m.VisitID,0) AS INT) MstVisitId,CAST(NULLIF(m.PatientID,0) AS INT) MstPatientId,
  CAST(ISNULL(m.Cancelstatus,0) AS INT) CancelStatus,CAST(NULL AS INT) InsertedById,
  CAST(m.Receipt_Date AS DATETIME) ReceiptDateNorm,ISNULL(CONVERT(NVARCHAR(80),m.CReceipt_No),N'') ReceiptNo,
  ISNULL(CONVERT(NVARCHAR(120),m.UTRNo),N'') UTRNo,CAST(ISNULL(m.CPayment_Mode,0) AS INT) PaymentModeId,
  LTRIM(RTRIM(CONVERT(NVARCHAR(120),ISNULL(m.CPayment_Mode,0)))) PaymentMode,
  CAST(ISNULL(m.rebateDiscountAmt,0) AS FLOAT) RebateDiscountAmt,CAST(ISNULL(m.TDSAmt,0) AS FLOAT) TDSAmt,CAST(ISNULL(m.WriteOffAmt,0) AS FLOAT) WriteOffAmt
 INTO #receipt_raw
 FROM dbo.Corp_Receipt_Dtl d WITH (NOLOCK)
 LEFT JOIN dbo.Corp_Receipt_Mst m WITH (NOLOCK) ON d.receiptId=m.Receipt_ID
 WHERE 1=0;

 IF EXISTS (SELECT 1 FROM #bill_scope)
 BEGIN
  INSERT INTO #receipt_raw
  SELECT
   CAST(d.recDtlId AS INT) ReceiptDetailId,CAST(d.receiptId AS INT) ReceiptId,CAST(d.billId AS INT) BillId,
   CAST(UPPER(ISNULL(NULLIF(LTRIM(RTRIM(CONVERT(NVARCHAR(40),d.BillSourceKey))),N''),N'UNKNOWN')) AS NVARCHAR(40)) ReceiptBillSourceKey,
   CAST(ISNULL(d.billAmt,0) AS FLOAT) BillAmtDtl,CAST(ISNULL(d.receiptAmt,0) AS FLOAT) ReceiptAmtDtl,CAST(ISNULL(d.dueAmt,0) AS FLOAT) DueAmtDtl,
   CAST(NULLIF(d.visitId,0) AS INT) DtlVisitId,CAST(NULLIF(d.PatientId,0) AS INT) DtlPatientId,
   CAST(NULLIF(m.VisitID,0) AS INT) MstVisitId,CAST(NULLIF(m.PatientID,0) AS INT) MstPatientId,
   CAST(ISNULL(m.Cancelstatus,0) AS INT) CancelStatus,CAST(NULL AS INT) InsertedById,
   CAST(m.Receipt_Date AS DATETIME) ReceiptDateNorm,ISNULL(CONVERT(NVARCHAR(80),m.CReceipt_No),N'') ReceiptNo,
   ISNULL(CONVERT(NVARCHAR(120),m.UTRNo),N'') UTRNo,CAST(ISNULL(m.CPayment_Mode,0) AS INT) PaymentModeId,
   LTRIM(RTRIM(CONVERT(NVARCHAR(120),ISNULL(m.CPayment_Mode,0)))) PaymentMode,
   CAST(ISNULL(m.rebateDiscountAmt,0) AS FLOAT) RebateDiscountAmt,CAST(ISNULL(m.TDSAmt,0) AS FLOAT) TDSAmt,CAST(ISNULL(m.WriteOffAmt,0) AS FLOAT) WriteOffAmt
  FROM dbo.Corp_Receipt_Dtl d WITH (NOLOCK)
  LEFT JOIN dbo.Corp_Receipt_Mst m WITH (NOLOCK) ON d.receiptId=m.Receipt_ID
  INNER JOIN #bill_scope bs
   ON bs.BillId=CAST(d.billId AS INT)
  AND UPPER(ISNULL(bs.BillSourceKey,N''))=UPPER(ISNULL(NULLIF(LTRIM(RTRIM(CONVERT(NVARCHAR(40),d.BillSourceKey))),N''),N'UNKNOWN'))
  WHERE NOT EXISTS (
   SELECT 1
   FROM #suspense_ids s
   WHERE s.BillId=bs.BillId AND s.BillSourceKey=bs.BillSourceKey
  );
 END;
 CREATE CLUSTERED INDEX IX_receipt_raw_key ON #receipt_raw(BillId,ReceiptBillSourceKey,ReceiptId,ReceiptDetailId);

;WITH trg AS (
 SELECT ReceiptBillSourceKey,BillId,ReceiptId,ROW_NUMBER() OVER (PARTITION BY ReceiptBillSourceKey,BillId ORDER BY ISNULL(CancelStatus,0) ASC,ReceiptDateNorm DESC,ReceiptId DESC) rn
 FROM #receipt_raw WHERE ReceiptId IS NOT NULL AND ISNULL(CancelStatus,0)<>1
) SELECT ReceiptBillSourceKey,BillId,ReceiptId WriteOffTargetReceiptId INTO #target_receipt FROM trg WHERE rn=1;

 SELECT * INTO #receipt_scope FROM (
  SELECT alloc.*,
   CAST(CASE WHEN net.NetDueRounded<=0 THEN 0 ELSE net.NetDueRounded END AS FLOAT) NetDueAmtDtl
  FROM (
   SELECT rr.*,
    CAST(CASE WHEN SUM(rr.ReceiptAmtDtl) OVER (PARTITION BY rr.ReceiptId)>0 THEN rr.RebateDiscountAmt*(rr.ReceiptAmtDtl/NULLIF(SUM(rr.ReceiptAmtDtl) OVER (PARTITION BY rr.ReceiptId),0))
              WHEN COUNT(1) OVER (PARTITION BY rr.ReceiptId)>0 THEN rr.RebateDiscountAmt/NULLIF(COUNT(1) OVER (PARTITION BY rr.ReceiptId),0) ELSE 0 END AS FLOAT) RebateAllocated,
    CAST(CASE WHEN SUM(rr.ReceiptAmtDtl) OVER (PARTITION BY rr.ReceiptId)>0 THEN rr.TDSAmt*(rr.ReceiptAmtDtl/NULLIF(SUM(rr.ReceiptAmtDtl) OVER (PARTITION BY rr.ReceiptId),0))
              WHEN COUNT(1) OVER (PARTITION BY rr.ReceiptId)>0 THEN rr.TDSAmt/NULLIF(COUNT(1) OVER (PARTITION BY rr.ReceiptId),0) ELSE 0 END AS FLOAT) TDSAllocated,
    CAST(CASE WHEN SUM(rr.ReceiptAmtDtl) OVER (PARTITION BY rr.ReceiptId)>0 THEN rr.WriteOffAmt*(rr.ReceiptAmtDtl/NULLIF(SUM(rr.ReceiptAmtDtl) OVER (PARTITION BY rr.ReceiptId),0))
              WHEN COUNT(1) OVER (PARTITION BY rr.ReceiptId)>0 THEN rr.WriteOffAmt/NULLIF(COUNT(1) OVER (PARTITION BY rr.ReceiptId),0) ELSE 0 END AS FLOAT) WriteOffAllocated
   FROM #receipt_raw rr
   WHERE (@IncludeCancelled=1 OR ISNULL(rr.CancelStatus,0)<>1)
  ) alloc
  CROSS APPLY (
   SELECT ROUND(ISNULL(alloc.DueAmtDtl,0)-ISNULL(alloc.TDSAllocated,0)-ISNULL(alloc.RebateAllocated,0)-ISNULL(alloc.WriteOffAllocated,0),2) NetDueRounded
  ) net
 )x;

 SELECT ReceiptBillSourceKey,BillId,SUM(ReceiptAmtDtl) receipt_total_all_time,SUM(TDSAllocated) tds_total_all_time,SUM(RebateAllocated) rebate_discount_all_time,SUM(WriteOffAllocated) writeoff_total_all_time,
        COUNT(1) receipt_count_all_time,MAX(ReceiptDateNorm) last_receipt_date_all_time_dt
 INTO #agg_all FROM #receipt_scope GROUP BY ReceiptBillSourceKey,BillId;

 SELECT ReceiptBillSourceKey,BillId,SUM(ReceiptAmtDtl) receipt_total_window,SUM(TDSAllocated) tds_total_window,SUM(RebateAllocated) rebate_discount_window,SUM(WriteOffAllocated) writeoff_total_window,
        COUNT(1) receipt_count_window,MAX(ReceiptDateNorm) last_receipt_date_window_dt
 INTO #agg_window FROM #receipt_scope
 WHERE (@ReceiptFrom IS NULL OR CAST(ReceiptDateNorm AS DATE)>=@ReceiptFrom) AND (@ReceiptTo IS NULL OR CAST(ReceiptDateNorm AS DATE)<=@ReceiptTo)
 GROUP BY ReceiptBillSourceKey,BillId;

 SELECT b.*,ISNULL(a.receipt_total_all_time,0) receipt_total_all_time,ISNULL(a.tds_total_all_time,0) tds_total_all_time,ISNULL(a.rebate_discount_all_time,0) rebate_discount_all_time,
        ISNULL(a.writeoff_total_all_time,0)+CASE WHEN b.BillSourceKey=N'OPENING' THEN ISNULL(b.OpeningWriteOffAmt,0) ELSE 0 END writeoff_total_all_time,
        ISNULL(a.receipt_count_all_time,0) receipt_count_all_time,a.last_receipt_date_all_time_dt,
        ISNULL(w.receipt_total_window,0) receipt_total_window,ISNULL(w.tds_total_window,0) tds_total_window,ISNULL(w.rebate_discount_window,0) rebate_discount_window,
        ISNULL(w.writeoff_total_window,0)+CASE WHEN b.BillSourceKey=N'OPENING' THEN ISNULL(b.OpeningWriteOffAmt,0) ELSE 0 END writeoff_total_window,
        ISNULL(w.receipt_count_window,0) receipt_count_window,w.last_receipt_date_window_dt,tr.WriteOffTargetReceiptId
 INTO #rows_all
 FROM #bill_scope b
 LEFT JOIN #agg_all a ON a.BillId=b.BillId AND UPPER(ISNULL(a.ReceiptBillSourceKey,N''))=UPPER(ISNULL(b.BillSourceKey,N''))
 LEFT JOIN #agg_window w ON w.BillId=b.BillId AND UPPER(ISNULL(w.ReceiptBillSourceKey,N''))=UPPER(ISNULL(b.BillSourceKey,N''))
 LEFT JOIN #target_receipt tr ON tr.BillId=b.BillId AND UPPER(ISNULL(tr.ReceiptBillSourceKey,N''))=UPPER(ISNULL(b.BillSourceKey,N''))
 WHERE NOT EXISTS (
  SELECT 1
  FROM #suspense_ids s
  WHERE s.BillId=b.BillId AND s.BillSourceKey=b.BillSourceKey
 );

 ALTER TABLE #rows_all ADD closure_delta_all_time FLOAT NULL,settled_total_all_time FLOAT NULL,settled_total_window FLOAT NULL,balance_all_time FLOAT NULL,balance_window FLOAT NULL,status_all_time NVARCHAR(20) NULL,status_window NVARCHAR(20) NULL;
 UPDATE #rows_all SET
  closure_delta_all_time=ISNULL(BillAmount,0)-(ISNULL(receipt_total_all_time,0)+ISNULL(tds_total_all_time,0)+ISNULL(rebate_discount_all_time,0)+ISNULL(writeoff_total_all_time,0));
 -- Reconciliation policy: once a bill has any receipt, force it to close by moving the
 -- remaining delta into the write-off bucket. Negative delta reverses the same bucket so
 -- overpaid bills also normalize to settled.
 UPDATE #rows_all SET
  writeoff_total_all_time=ISNULL(writeoff_total_all_time,0)+CASE WHEN ISNULL(receipt_total_all_time,0)>@SettleTolerance AND ABS(ISNULL(closure_delta_all_time,0))>@SettleTolerance THEN ISNULL(closure_delta_all_time,0) ELSE 0 END,
  writeoff_total_window=ISNULL(writeoff_total_window,0)+CASE WHEN ISNULL(receipt_total_window,0)>@SettleTolerance AND ABS(ISNULL(closure_delta_all_time,0))>@SettleTolerance THEN ISNULL(closure_delta_all_time,0) ELSE 0 END;
 UPDATE #rows_all SET
  settled_total_all_time=ISNULL(receipt_total_all_time,0)+ISNULL(tds_total_all_time,0)+ISNULL(rebate_discount_all_time,0)+ISNULL(writeoff_total_all_time,0),
  settled_total_window=ISNULL(receipt_total_window,0)+ISNULL(tds_total_window,0)+ISNULL(rebate_discount_window,0)+ISNULL(writeoff_total_window,0),
  balance_all_time=ISNULL(BillAmount,0)-(ISNULL(receipt_total_all_time,0)+ISNULL(tds_total_all_time,0)+ISNULL(rebate_discount_all_time,0)+ISNULL(writeoff_total_all_time,0));
 UPDATE #rows_all SET
  balance_window=CASE
   WHEN ISNULL(receipt_total_window,0)>@SettleTolerance THEN ISNULL(balance_all_time,0)
   ELSE ISNULL(BillAmount,0)-(ISNULL(receipt_total_window,0)+ISNULL(tds_total_window,0)+ISNULL(rebate_discount_window,0)+ISNULL(writeoff_total_window,0))
  END;
 UPDATE #rows_all SET
  status_all_time=CASE WHEN balance_all_time<-1 THEN N'Overpaid' WHEN ABS(balance_all_time)<=1 THEN N'Settled' WHEN settled_total_all_time>0 THEN N'Partial' ELSE N'Unpaid' END,
  status_window=CASE WHEN balance_window<-1 THEN N'Overpaid' WHEN ABS(balance_window)<=1 THEN N'Settled' WHEN settled_total_window>0 THEN N'Partial' ELSE N'Unpaid' END;

 SELECT
  r.*,
  CASE
   WHEN @HasRegTermList=1 THEN (
    SELECT MIN(qt.seq)
    FROM #q_reg_terms qt
    WHERE qt.term=LOWER(CASE WHEN r.PatientId IS NULL THEN N'' ELSE CONVERT(NVARCHAR(40),r.PatientId) END)
   )
   ELSE NULL
  END AS PatientSearchOrder
 INTO #rows_filtered
 FROM #rows_all r
 WHERE (
      (@BillFrom IS NULL AND @BillTo IS NULL)
      OR (
           (
            UPPER(ISNULL(r.BillSourceKey,N''))=N'OPENING'
            AND (
                 ISNULL(r.settled_total_window,0)>@SettleTolerance
                 OR ISNULL(r.balance_all_time,0)>@SettleTolerance
                )
           )
           OR (
            UPPER(ISNULL(r.BillSourceKey,N''))<>N'OPENING'
            AND (
                 ((@BillFrom IS NULL OR CAST(r.BillDate AS DATE)>=@BillFrom) AND (@BillTo IS NULL OR CAST(r.BillDate AS DATE)<=@BillTo))
                 OR ISNULL(r.settled_total_window,0)>@SettleTolerance
                )
           )
         )
   )
 AND (@BillSourceSafe=N'' OR LOWER(ISNULL(r.BillSource,N''))=@BillSourceSafe)
   AND (
        UPPER(ISNULL(r.BillSourceKey,N''))=N'OPENING'
        OR UPPER(ISNULL(r.BillStatusRaw,N''))=N'Y'
   )
   AND (@SubtypeKey=N'' OR UPPER(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(ISNULL(r.PatientSubType,N''),N' ',N''),N'-',N''),N'/',N''),N'(',N''),N')',N''),N',',N''),N'.',N''),N'_',N''))=@SubtypeKey)
   AND (
        @QSafe=N''
        OR (
             @HasRegTermList=1
             AND EXISTS (
                  SELECT 1
                  FROM #q_reg_terms qt
                  WHERE qt.term=LOWER(CASE WHEN r.PatientId IS NULL THEN N'' ELSE CONVERT(NVARCHAR(40),r.PatientId) END)
             )
        )
        OR (
             @HasRegTermList=0
             AND LOWER(CONCAT(
                  ISNULL(r.BillNo,N''),N' ',
                  ISNULL(r.Registration_No,N''),N' ',
                  ISNULL(r.PatientName,N''),N' ',
                  ISNULL(r.BillSource,N''),N' ',
                  ISNULL(r.TypeOfVisit,N''),N' ',
                  ISNULL(r.PatientType,N''),N' ',
                  ISNULL(r.PatientSubType,N''),N' ',
                  ISNULL(r.Dept,N''),N' ',
                  ISNULL(r.SubDept,N''),N' ',
                  CONVERT(NVARCHAR(30),ISNULL(r.BillId,0)),N' ',
                  CONVERT(NVARCHAR(30),ISNULL(r.PatientId,0)),N' ',
                  CONVERT(NVARCHAR(30),ISNULL(r.VisitId,0))
             )) LIKE N'%'+@QSafe+N'%'
        )
   )
   AND (
        @KpiFilterSafe=N'' OR @KpiFilterSafe=N'bill_count'
        OR (@KpiFilterSafe=N'total_bill_amount' AND ISNULL(r.BillAmount,0)>0)
        OR (@KpiFilterSafe=N'receipt_all_time' AND ISNULL(r.receipt_total_all_time,0)>0)
        OR (@KpiFilterSafe=N'receipt_in_window' AND ISNULL(r.receipt_total_window,0)>0)
        OR (@KpiFilterSafe=N'balance_all_time' AND ABS(ISNULL(r.balance_all_time,0))>1)
        OR (@KpiFilterSafe=N'balance_in_window' AND ABS(ISNULL(r.balance_window,0))>1)
        OR (@KpiFilterSafe=N'settled_count' AND ISNULL(r.status_all_time,N'')=N'Settled')
        OR (@KpiFilterSafe=N'partial_count' AND ISNULL(r.status_all_time,N'')=N'Partial')
        OR (@KpiFilterSafe=N'unpaid_count' AND ISNULL(r.status_all_time,N'')=N'Unpaid')
        OR (@KpiFilterSafe=N'overpaid_count' AND ISNULL(r.status_all_time,N'')=N'Overpaid')
   );

 DECLARE @TotalRows INT=(SELECT COUNT(1) FROM #rows_filtered);
 DECLARE @TotalPages INT=CASE WHEN @TotalRows<=0 THEN 1 ELSE CEILING(@TotalRows*1.0/@PageSizeSafe) END;
 IF @PageSafe>@TotalPages SET @PageSafe=@TotalPages;
 DECLARE @StartRow INT=((@PageSafe-1)*@PageSizeSafe)+1;
 DECLARE @EndRow INT=@PageSafe*@PageSizeSafe;

 DECLARE @OrderCol SYSNAME=CASE @SortBySafe
  WHEN N'bill_id' THEN N'BillId' WHEN N'bill_no' THEN N'BillNo' WHEN N'bill_source' THEN N'BillSource' WHEN N'bill_date' THEN N'BillDate'
  WHEN N'due_date' THEN N'DueDate' WHEN N'patient_name' THEN N'PatientName' WHEN N'visit_id' THEN N'VisitId' WHEN N'bill_amount' THEN N'BillAmount'
  WHEN N'receipt_total_all_time' THEN N'receipt_total_all_time' WHEN N'tds_total_all_time' THEN N'tds_total_all_time'
  WHEN N'rebate_discount_all_time' THEN N'rebate_discount_all_time' WHEN N'writeoff_total_all_time' THEN N'writeoff_total_all_time'
  WHEN N'settled_total_all_time' THEN N'settled_total_all_time' WHEN N'receipt_total_window' THEN N'receipt_total_window'
  WHEN N'tds_total_window' THEN N'tds_total_window' WHEN N'rebate_discount_window' THEN N'rebate_discount_window'
  WHEN N'writeoff_total_window' THEN N'writeoff_total_window' WHEN N'settled_total_window' THEN N'settled_total_window'
  WHEN N'balance_window' THEN N'balance_window' WHEN N'status_all_time' THEN N'status_all_time' WHEN N'status_window' THEN N'status_window'
  WHEN N'receipt_count_all_time' THEN N'receipt_count_all_time' WHEN N'receipt_count_window' THEN N'receipt_count_window'
  WHEN N'last_receipt_date_all_time' THEN N'last_receipt_date_all_time_dt' WHEN N'last_receipt_date_window' THEN N'last_receipt_date_window_dt'
 ELSE N'balance_all_time' END;
 DECLARE @OrderDir NVARCHAR(4)=CASE WHEN @SortDirSafe=N'asc' THEN N'ASC' ELSE N'DESC' END;
 DECLARE @OrderPrefix NVARCHAR(MAX)=CASE WHEN @HasRegTermList=1 THEN N'ISNULL(PatientSearchOrder,2147483647) ASC, ' ELSE N'' END;

 SELECT TOP 0 *,CAST(0 AS INT) row_num INTO #page_rows FROM #rows_filtered;
 DECLARE @PageSql NVARCHAR(MAX)=N'
 ;WITH ordered AS (
  SELECT r.*,ROW_NUMBER() OVER (ORDER BY '+@OrderPrefix+QUOTENAME(@OrderCol)+N' '+@OrderDir+N', BillSourceKey DESC, BillId DESC) row_num
  FROM #rows_filtered r
 )
 INSERT INTO #page_rows
 SELECT *
 FROM ordered
 WHERE row_num BETWEEN @start_row AND @end_row
 ORDER BY row_num';
 EXEC sp_executesql @PageSql,N'@start_row INT,@end_row INT',@start_row=@StartRow,@end_row=@EndRow;

 -- Result set 1: page rows
 DECLARE @RowsSql NVARCHAR(MAX)=N'
 SELECT
   CONCAT(N''BILL-'',ISNULL(NULLIF(p.BillSourceKey,N''''),N''UNKNOWN''),N''-'',CONVERT(NVARCHAR(30),p.BillId)) bill_key,p.BillId bill_id,p.BillSourceKey bill_source_key,p.BillSource bill_source,p.BillNo bill_no,p.Registration_No registration_no,
   CONVERT(NVARCHAR(10),p.BillDate,23) bill_date,CONVERT(NVARCHAR(10),p.SubmitDateRaw,23) submit_date_raw,CONVERT(NVARCHAR(10),p.CBillDateRaw,23) c_bill_date_raw,
   CONVERT(NVARCHAR(10),p.DueDate,23) due_date,p.PatientId patient_id,p.PatientName patient_name,p.PatientType patient_type,p.PatientSubType patient_subtype,p.TypeOfVisit type_of_visit,
   p.VisitId visit_id,CONVERT(NVARCHAR(10),p.VisitDate,23) visit_date,CONVERT(NVARCHAR(10),p.DischargeDate,23) discharge_date,p.Dept dept,p.SubDept sub_dept,p.BillAmount bill_amount,
   p.receipt_total_all_time,p.tds_total_all_time,p.rebate_discount_all_time,p.writeoff_total_all_time,p.settled_total_all_time,p.balance_all_time,p.status_all_time,p.receipt_count_all_time,
   CONVERT(NVARCHAR(10),p.last_receipt_date_all_time_dt,23) last_receipt_date_all_time,
   p.SourcePatientName source_patient_name,p.BillStatusRaw bill_status_raw,p.BillDueAmountRaw bill_due_amount_raw,p.BillAuditedFlag bill_audited_flag,p.OpeningWriteOffAmt opening_writeoff_amount,
   p.WriteOffTargetReceiptId writeoff_target_receipt_id,p.BillUpdatedById bill_updated_by_id,p.BillUpdatedByName bill_updated_by,CONVERT(NVARCHAR(19),p.BillUpdatedOnRaw,120) bill_updated_on,
   CAST(0 AS BIT) is_suspense_date_anomaly,CAST(N'''' AS NVARCHAR(250)) suspense_reason,CAST(0 AS INT) suspense_days_gap
 FROM #page_rows p ORDER BY p.row_num';
 EXEC sp_executesql @RowsSql;

 -- Result set 2: page receipt details
 SELECT
  CONCAT(N'BILL-',ISNULL(NULLIF(d.ReceiptBillSourceKey,N''),N'UNKNOWN'),N'-',CONVERT(NVARCHAR(30),d.BillId)) bill_key,
  d.BillId bill_id,d.ReceiptBillSourceKey receipt_bill_source_key,
  CASE WHEN UPPER(ISNULL(d.ReceiptBillSourceKey,N''))=N'OPENING' THEN N'Opening' ELSE N'Bill' END receipt_against_source,
  d.ReceiptDetailId receipt_detail_id,d.ReceiptId receipt_id,d.ReceiptNo receipt_no,
  CONVERT(NVARCHAR(10),d.ReceiptDateNorm,23) receipt_date,d.ReceiptAmtDtl receipt_amount,d.BillAmtDtl bill_amount,d.NetDueAmtDtl due_amount,
  d.TDSAllocated tds_amount,d.RebateAllocated rebate_discount_amount,d.WriteOffAllocated writeoff_amount,
  d.CancelStatus cancel_status,d.PaymentModeId payment_mode_id,
  CASE WHEN LTRIM(RTRIM(ISNULL(d.PaymentMode,N'')))=N'' THEN CONVERT(NVARCHAR(20),ISNULL(d.PaymentModeId,0)) ELSE d.PaymentMode END payment_mode,
  d.UTRNo utr_no,d.InsertedById inserted_by_id,
  CASE WHEN d.InsertedById IS NULL THEN N'' ELSE CONVERT(NVARCHAR(100),d.InsertedById) END inserted_by,
  COALESCE(d.DtlVisitId,d.MstVisitId) visit_id,COALESCE(d.DtlPatientId,d.MstPatientId) patient_id,
  CAST(CASE WHEN (@ReceiptFrom IS NULL OR CAST(d.ReceiptDateNorm AS DATE)>=@ReceiptFrom) AND (@ReceiptTo IS NULL OR CAST(d.ReceiptDateNorm AS DATE)<=@ReceiptTo) THEN 1 ELSE 0 END AS BIT) in_window
 FROM #receipt_scope d
 INNER JOIN #page_rows p ON p.BillId=d.BillId AND UPPER(ISNULL(p.BillSourceKey,N''))=UPPER(ISNULL(d.ReceiptBillSourceKey,N''))
 ORDER BY d.ReceiptDateNorm DESC,d.ReceiptId DESC,d.ReceiptDetailId DESC;

 -- Result set 3: KPIs
 SELECT
  ISNULL(SUM(BillAmount),0) total_bill_amount,ISNULL(SUM(receipt_total_all_time),0) receipt_all_time,
  ISNULL(SUM(rebate_discount_all_time),0) rebate_discount_all_time,ISNULL(SUM(writeoff_total_all_time),0) writeoff_all_time,ISNULL(SUM(settled_total_all_time),0) settled_total_all_time,
  ISNULL(SUM(balance_all_time),0) balance_all_time,
  COUNT(1) bill_count,SUM(CASE WHEN status_all_time=N'Settled' THEN 1 ELSE 0 END) settled_count,SUM(CASE WHEN status_all_time=N'Partial' THEN 1 ELSE 0 END) partial_count,
  SUM(CASE WHEN status_all_time=N'Unpaid' THEN 1 ELSE 0 END) unpaid_count,SUM(CASE WHEN status_all_time=N'Overpaid' THEN 1 ELSE 0 END) overpaid_count
 FROM #rows_filtered;

 -- Result set 4: Meta
 SELECT
 @PageSafe page,@PageSizeSafe page_size,@TotalRows total_rows,@TotalPages total_pages,@SortBySafe sort_by,@SortDirSafe sort_dir,
  @QSafe q,NULLIF(@BillSourceSafe,N'') bill_source,NULLIF(@SubtypeSafe,N'') patient_subtype,NULLIF(@KpiFilterSafe,N'') kpi_filter,
  CASE
   WHEN @BillFrom IS NULL AND @BillTo IS NULL THEN NULL
   ELSE N'Includes bills in submit date range plus older bills with settlement activity in the receipt window; openings remain when they have in-window settlement or open balance'
  END scope_rule,
  CASE @KpiFilterSafe
   WHEN N'bill_count' THEN N'All Bills'
   WHEN N'total_bill_amount' THEN N'Bill Amount > 0'
   WHEN N'receipt_all_time' THEN N'Receipt > 0'
   WHEN N'receipt_in_window' THEN N'Receipt In Window > 0'
   WHEN N'balance_all_time' THEN N'Open Balance'
   WHEN N'balance_in_window' THEN N'Open Balance (Window)'
   WHEN N'settled_count' THEN N'Settled'
   WHEN N'partial_count' THEN N'Partial'
   WHEN N'unpaid_count' THEN N'Unpaid'
   WHEN N'overpaid_count' THEN N'Overpaid'
   ELSE NULL
  END kpi_filter_label,
  @IncludeCancelled include_cancelled,
  @BillFrom bill_from,@BillTo bill_to,@ReceiptFrom receipt_from,@ReceiptTo receipt_to,@SuspenseCount suspense_count,
  @HasReceiptWriteoff has_receipt_writeoff_column,@HasOpeningWriteoff has_opening_writeoff_column,@HasBillAudited has_bill_audited_column,
  CAST(0 AS BIT) cache_used,CAST(0 AS BIT) cache_hit,CAST(NULL AS INT) cache_age_sec,CAST(NULL AS NVARCHAR(19)) cached_at;

 -- Result set 5: available sources
 SELECT DISTINCT LTRIM(RTRIM(ISNULL(BillSource,N''))) bill_source
 FROM #rows_filtered
 WHERE LTRIM(RTRIM(ISNULL(BillSource,N'')))<>N''
 ORDER BY bill_source;

 -- Result set 6: available subtypes
 SELECT DISTINCT LTRIM(RTRIM(ISNULL(PatientSubType,N''))) patient_subtype
 FROM #rows_filtered
 WHERE LTRIM(RTRIM(ISNULL(PatientSubType,N'')))<>N''
 ORDER BY patient_subtype;

 -- Result set 7: subtype settlement summary (full filtered scope, all pages)
SELECT
 CASE
  WHEN LTRIM(RTRIM(ISNULL(PatientSubType,N'')))<>N'' THEN LTRIM(RTRIM(ISNULL(PatientSubType,N'')))
  WHEN UPPER(ISNULL(BillSourceKey,N''))=N'OPENING' THEN N'Opening (Unmapped)'
  ELSE N'Unspecified'
 END subtype,
 COUNT(1) bills,
 ISNULL(SUM(BillAmount),0) bill_amount,
 SUM(CASE WHEN UPPER(ISNULL(BillSourceKey,N''))=N'OPENING' THEN 0 ELSE 1 END) corporate_bills,
 SUM(CASE WHEN UPPER(ISNULL(BillSourceKey,N''))=N'OPENING' THEN 1 ELSE 0 END) opening_bills,
 ISNULL(SUM(receipt_total_all_time),0) receipt_all_time,
 ISNULL(SUM(tds_total_all_time),0) tds_all_time,
 ISNULL(SUM(rebate_discount_all_time),0) rebate_discount_all_time,
 ISNULL(SUM(writeoff_total_all_time),0) writeoff_all_time,
 ISNULL(SUM(settled_total_all_time),0) settled_total_all_time,
 SUM(CASE WHEN status_all_time=N'Settled' THEN 1 ELSE 0 END) settled_count,
 SUM(CASE WHEN status_all_time=N'Settled' AND UPPER(ISNULL(BillSourceKey,N''))<>N'OPENING' THEN 1 ELSE 0 END) settled_corporate_count,
 SUM(CASE WHEN status_all_time=N'Settled' AND UPPER(ISNULL(BillSourceKey,N''))=N'OPENING' THEN 1 ELSE 0 END) settled_opening_count,
 SUM(CASE WHEN status_all_time=N'Partial' THEN 1 ELSE 0 END) partial_count,
 SUM(CASE WHEN status_all_time=N'Partial' AND UPPER(ISNULL(BillSourceKey,N''))<>N'OPENING' THEN 1 ELSE 0 END) partial_corporate_count,
 SUM(CASE WHEN status_all_time=N'Partial' AND UPPER(ISNULL(BillSourceKey,N''))=N'OPENING' THEN 1 ELSE 0 END) partial_opening_count,
 SUM(CASE WHEN status_all_time=N'Unpaid' THEN 1 ELSE 0 END) unpaid_count,
 SUM(CASE WHEN status_all_time=N'Unpaid' AND UPPER(ISNULL(BillSourceKey,N''))<>N'OPENING' THEN 1 ELSE 0 END) unpaid_corporate_count,
 SUM(CASE WHEN status_all_time=N'Unpaid' AND UPPER(ISNULL(BillSourceKey,N''))=N'OPENING' THEN 1 ELSE 0 END) unpaid_opening_count,
 SUM(CASE WHEN status_all_time=N'Overpaid' THEN 1 ELSE 0 END) overpaid_count,
 SUM(CASE WHEN status_all_time=N'Overpaid' AND UPPER(ISNULL(BillSourceKey,N''))<>N'OPENING' THEN 1 ELSE 0 END) overpaid_corporate_count,
 SUM(CASE WHEN status_all_time=N'Overpaid' AND UPPER(ISNULL(BillSourceKey,N''))=N'OPENING' THEN 1 ELSE 0 END) overpaid_opening_count,
  SUM(CASE WHEN status_all_time IN (N'Partial',N'Unpaid',N'Overpaid') THEN 1 ELSE 0 END) closing_qty,
  ISNULL(SUM(balance_all_time),0) closing_balance
FROM #rows_filtered
 GROUP BY
  CASE
   WHEN LTRIM(RTRIM(ISNULL(PatientSubType,N'')))<>N'' THEN LTRIM(RTRIM(ISNULL(PatientSubType,N'')))
   WHEN UPPER(ISNULL(BillSourceKey,N''))=N'OPENING' THEN N'Opening (Unmapped)'
   ELSE N'Unspecified'
  END
 ORDER BY COUNT(1) DESC, ABS(ISNULL(SUM(balance_all_time),0)) DESC, subtype;

 -- Result set 8: suspense meta
 SELECT @SuspenseCount suspense_count,N'Submit Date post-cutoff but Bill Date pre-cutoff.' suspense_reason;
END;
GO
