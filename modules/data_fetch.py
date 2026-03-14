from modules.db_connection import get_sql_connection
import pandas as pd
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import json
import pyodbc
from decimal import Decimal
import re
import time
from threading import Lock

# Keep IST for consistent formatting
LOCAL_TZ = ZoneInfo("Asia/Kolkata")

def fetch_revenue(unit, from_date, to_date):
    """
    Fetches revenue summary from a specific unit (AHL/ACI/BALLIA)
    using stored procedure Usp_CorpsubmitBillSummaryupdate.
    (Unchanged from your version)
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"âš ï¸ Could not connect to {unit}")
        return None

    try:
        query = f"""
        EXEC Usp_CorpsubmitBillSummaryupdate
            @FromDate = '{from_date}',
            @ToDate = '{to_date}'
        """
        df = pd.read_sql(query, conn)
        df['Unit'] = unit
        return df
    except Exception as e:
        print(f"âŒ Error fetching revenue from {unit}: {e}")
        return None
    finally:
        conn.close()


# ===================== Corporate Bill Summary (Ballia) =====================
def fetch_corporate_bill_summary(unit: str, from_date: str, to_date: str, visit_type: int = 0):
    """
    Calls dbo.Usp_Get_Corp_Bill_Summary_ByType for a given unit (Ballia expected).
    visit_type: 0=All, 1=IPD, 2=OPD, 3=DPV
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"ï¿½sï¿½ï¿½,? Could not connect to {unit} for corporate bill summary")
        return None
    try:
        sql = "EXEC dbo.Usp_Get_Corp_Bill_Summary_ByType ?, ?, ?"
        df = pd.read_sql(sql, conn, params=[from_date, to_date, visit_type])
        if df is None or df.empty:
            return df
        df.columns = [c.strip() for c in df.columns]
        df["Unit"] = (unit or "").upper()
        return df
    except Exception as e:
        print(f"ï¿½?O Error fetching corporate summary ({unit}): {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


# ===================== Collections / Receipts Summary =====================
def fetch_receipt_summary(unit: str, from_date: str, to_date: str):
    """
    Executes dbo.Usp_ReceiptSummary for the given unit and date range.
    Returns raw dataframe with columns stripped + Unit column stamped.
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"âš ï¸ Could not connect to {unit} for receipt summary")
        return None
    try:
        sql = "EXEC dbo.Usp_ReceiptSummary ?, ?"
        df = pd.read_sql(sql, conn, params=[from_date, to_date])
        if df is None or df.empty:
            return df
        df.columns = [c.strip() for c in df.columns]
        df["Unit"] = (unit or "").upper()
        return df
    except Exception as e:
        print(f"âš ï¸ Error fetching receipt summary ({unit}): {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _corp_bill_summary_parse_int(raw_value, default: int, minimum: int | None = None, maximum: int | None = None) -> int:
    try:
        value = int(float(raw_value))
    except Exception:
        value = int(default)
    if minimum is not None:
        value = max(minimum, value)
    if maximum is not None:
        value = min(maximum, value)
    return value


def _corp_bill_summary_norm_status_filter(status_filter: str) -> str:
    key = str(status_filter or "").strip().lower()
    if key in {"final", "nonfinal"}:
        return key
    return "all"


def _corp_bill_summary_parse_date_or_none(raw_value):
    txt = str(raw_value or "").strip()
    if not txt:
        return None
    txt = txt[:10]
    try:
        return datetime.strptime(txt, "%Y-%m-%d").date().isoformat()
    except Exception:
        return None


def _corp_bill_summary_dict_rows_from_cursor(cursor):
    if not cursor or not cursor.description:
        return []
    cols = [str(c[0]).strip() for c in cursor.description]
    rows = cursor.fetchall()
    out = []
    for row in rows:
        out.append({cols[i]: row[i] for i in range(len(cols))})
    return out


def _fetch_corporate_bill_summary_page_sp(
    conn,
    *,
    from_date: str | None,
    to_date: str | None,
    visit_type: int,
    status_filter: str,
    patient_subtype: str,
    search_query: str,
    page: int,
    page_size: int,
):
    if not conn:
        return None

    started = time.perf_counter()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            EXEC dbo.usp_CorpBillSummary_Page
                 @FromDate = ?,
                 @ToDate = ?,
                 @VisitType = ?,
                 @StatusFilter = ?,
                 @PatientSubtype = ?,
                 @SearchQuery = ?,
                 @Page = ?,
                 @PageSize = ?
            """,
            [
                from_date,
                to_date,
                int(visit_type),
                str(status_filter or "all"),
                str(patient_subtype or ""),
                str(search_query or ""),
                int(page),
                int(page_size),
            ],
        )

        result_sets = []
        while True:
            if cur.description:
                result_sets.append(_corp_bill_summary_dict_rows_from_cursor(cur))
            if not cur.nextset():
                break

        if not result_sets:
            return None

        rows = list(result_sets[0] or [])
        meta = {}
        if len(result_sets) > 1 and result_sets[1]:
            meta = dict(result_sets[1][0] or {})
        elif rows and isinstance(rows[0], dict):
            first = rows[0]
            if "total_rows" in first:
                try:
                    meta["total_rows"] = int(first.get("total_rows") or 0)
                except Exception:
                    meta["total_rows"] = 0
        available_subtypes = []
        if len(result_sets) > 2 and isinstance(result_sets[2], list):
            for rec in result_sets[2]:
                if not isinstance(rec, dict):
                    continue
                val = str(rec.get("patient_subtype") or rec.get("PatientSubType") or "").strip()
                if val:
                    available_subtypes.append(val)
        if available_subtypes:
            dedup = []
            seen = set()
            for val in available_subtypes:
                key = val.lower()
                if key in seen:
                    continue
                seen.add(key)
                dedup.append(val)
            available_subtypes = dedup

        total_rows = _corp_bill_summary_parse_int(meta.get("total_rows"), len(rows), 0, None)
        total_pages = _corp_bill_summary_parse_int(
            meta.get("total_pages"),
            max(1, (total_rows + max(1, int(page_size)) - 1) // max(1, int(page_size))),
            1,
            None,
        )
        db_ms = round((time.perf_counter() - started) * 1000.0, 1)

        out_meta = {
            "page": _corp_bill_summary_parse_int(meta.get("page"), page, 1, None),
            "page_size": _corp_bill_summary_parse_int(meta.get("page_size"), page_size, 1, 5000),
            "total_rows": int(total_rows),
            "total_pages": int(total_pages),
            "query_engine": "sql_sp",
            "available_patient_subtypes": available_subtypes,
            "timings": {"db_ms": float(db_ms), "total_ms": float(db_ms)},
        }
        return {"status": "success", "rows": rows, "meta": out_meta}
    except Exception:
        return None


def _fetch_corporate_bill_summary_page_sql(
    conn,
    *,
    from_date: str | None,
    to_date: str | None,
    visit_type: int,
    status_filter: str,
    patient_subtype: str,
    search_query: str,
    page: int,
    page_size: int,
):
    if not conn:
        return {"status": "error", "message": "Database connection unavailable"}

    started = time.perf_counter()
    try:
        sql = """
        SET NOCOUNT ON;
        DECLARE @FromDate DATE = ?;
        DECLARE @ToDate DATE = ?;
        DECLARE @VisitType INT = ?;
        DECLARE @StatusFilter NVARCHAR(20) = ?;
        DECLARE @PatientSubtype NVARCHAR(200) = ?;
        DECLARE @SearchQuery NVARCHAR(200) = ?;
        DECLARE @Page INT = ?;
        DECLARE @PageSize INT = ?;

        IF @Page < 1 SET @Page = 1;
        IF @PageSize < 1 SET @PageSize = 25;

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
                CAST(
                    CASE
                        WHEN ISNULL(cb.Old_Bill_Amt, 0) <> 0 THEN ISNULL(cb.Old_Bill_Amt, 0)
                        ELSE ISNULL(bm.NetAmount, 0)
                    END
                AS FLOAT) AS Old_Bill_Amt,
                CAST(COALESCE(cb.Old_Bill_Date, bm.BillDate) AS DATETIME) AS Old_Bill_Date,
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
                OR LOWER(CONVERT(NVARCHAR(10), CAST(b.BillDate AS DATE), 23)) LIKE @SearchLike
                OR LOWER(CONVERT(NVARCHAR(10), CAST(b.BillDate AS DATE), 105)) LIKE @SearchLike
                OR LOWER(CONVERT(NVARCHAR(10), CAST(b.BillDate AS DATE), 103)) LIKE @SearchLike
                OR LOWER(REPLACE(CONVERT(NVARCHAR(11), CAST(b.BillDate AS DATE), 106), N' ', N'-')) LIKE @SearchLike
                OR LOWER(CONVERT(NVARCHAR(19), CAST(b.BillDate AS DATETIME), 120)) LIKE @SearchLike
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

        SELECT COUNT(1) AS total_rows FROM #corp_bill_filtered;

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
            CASE
                WHEN n.VisitPatientSubTypeID IS NULL THEN N''
                ELSE ISNULL(n.PatientSubTypeName, N'')
            END AS PatientSubType,
            CASE
                WHEN n.DocInChargeID IS NULL THEN N''
                ELSE ISNULL(dbo.fn_doctorfirstname(n.DocInChargeID), N'')
            END AS DocInCharge,
            CASE
                WHEN n.DepartmentID IS NULL THEN N''
                ELSE ISNULL(dbo.fn_dept(n.DepartmentID), N'')
            END AS Dept
        FROM numbered n
        WHERE n.rn BETWEEN ((@Page - 1) * @PageSize + 1) AND (@Page * @PageSize)
        ORDER BY n.rn;

        SELECT DISTINCT
            LTRIM(RTRIM(ISNULL(PatientSubTypeName, N''))) AS patient_subtype
        FROM #corp_bill_filtered
        WHERE LTRIM(RTRIM(ISNULL(PatientSubTypeName, N''))) <> N''
        ORDER BY patient_subtype;
        """

        cur = conn.cursor()
        cur.execute(
            sql,
            [
                from_date,
                to_date,
                int(visit_type),
                str(status_filter or "all"),
                str(patient_subtype or ""),
                str(search_query or ""),
                int(page),
                int(page_size),
            ],
        )

        total_rows = 0
        page_rows = []
        available_subtypes = []
        set_index = 0
        while True:
            if cur.description:
                rows = _corp_bill_summary_dict_rows_from_cursor(cur)
                if set_index == 0:
                    if rows:
                        total_rows = _corp_bill_summary_parse_int(rows[0].get("total_rows"), 0, 0, None)
                elif set_index == 1:
                    page_rows = rows
                elif set_index == 2:
                    for rec in rows:
                        val = str(rec.get("patient_subtype") or "").strip()
                        if val:
                            available_subtypes.append(val)
                set_index += 1
            if not cur.nextset():
                break

        if available_subtypes:
            dedup = []
            seen = set()
            for val in available_subtypes:
                key = val.lower()
                if key in seen:
                    continue
                seen.add(key)
                dedup.append(val)
            available_subtypes = dedup

        total_pages = max(1, (int(total_rows) + int(page_size) - 1) // int(page_size)) if int(page_size) > 0 else 1
        db_ms = round((time.perf_counter() - started) * 1000.0, 1)
        return {
            "status": "success",
            "rows": page_rows,
            "meta": {
                "page": int(page),
                "page_size": int(page_size),
                "total_rows": int(total_rows),
                "total_pages": int(total_pages),
                "query_engine": "python_sql",
                "available_patient_subtypes": available_subtypes,
                "timings": {"db_ms": float(db_ms), "total_ms": float(db_ms)},
            },
        }
    except Exception as e:
        return {"status": "error", "message": f"Failed to fetch corporate bill summary page: {e}"}


def fetch_corporate_bill_summary_page(
    unit: str,
    from_date: str | None = None,
    to_date: str | None = None,
    visit_type: int = 0,
    status_filter: str = "all",
    patient_subtype: str = "",
    search_query: str = "",
    page: int = 1,
    page_size: int = 25,
    prefer_sp: bool = True,
):
    unit_key = str(unit or "").strip().upper()
    if unit_key not in {"AHL", "ACI"}:
        return {"status": "error", "message": "Unit not supported for paged corporate bill summary"}

    vt = _corp_bill_summary_parse_int(visit_type, 0, 0, 3)
    pg = _corp_bill_summary_parse_int(page, 1, 1, None)
    pg_size = _corp_bill_summary_parse_int(page_size, 25, 10, 200)
    sf = _corp_bill_summary_norm_status_filter(status_filter)
    subtype = str(patient_subtype or "").strip()
    q = str(search_query or "").strip()
    from_iso = _corp_bill_summary_parse_date_or_none(from_date)
    to_iso = _corp_bill_summary_parse_date_or_none(to_date)

    conn = get_sql_connection(unit_key)
    if not conn:
        return {"status": "error", "message": f"Could not connect to {unit_key} for corporate bill summary"}
    try:
        if bool(prefer_sp):
            sp_payload = _fetch_corporate_bill_summary_page_sp(
                conn,
                from_date=from_iso,
                to_date=to_iso,
                visit_type=vt,
                status_filter=sf,
                patient_subtype=subtype,
                search_query=q,
                page=pg,
                page_size=pg_size,
            )
            if isinstance(sp_payload, dict) and str(sp_payload.get("status") or "").lower() == "success":
                return sp_payload

        return _fetch_corporate_bill_summary_page_sql(
            conn,
            from_date=from_iso,
            to_date=to_iso,
            visit_type=vt,
            status_filter=sf,
            patient_subtype=subtype,
            search_query=q,
            page=pg,
            page_size=pg_size,
        )
    finally:
        try:
            conn.close()
        except Exception:
            pass


def fetch_corporate_bill_summary_rows_for_export(
    unit: str,
    from_date: str | None = None,
    to_date: str | None = None,
    visit_type: int = 0,
    status_filter: str = "all",
    patient_subtype: str = "",
    search_query: str = "",
    prefer_sp: bool = True,
):
    unit_key = str(unit or "").strip().upper()
    if unit_key not in {"AHL", "ACI"}:
        return {"status": "error", "message": "Unit not supported for this export flow"}

    all_rows = []
    page = 1
    page_size = 200
    total_pages = 1
    aggregate_meta = {}

    while page <= total_pages and page <= 20000:
        payload = fetch_corporate_bill_summary_page(
            unit=unit_key,
            from_date=from_date,
            to_date=to_date,
            visit_type=visit_type,
            status_filter=status_filter,
            patient_subtype=patient_subtype,
            search_query=search_query,
            page=page,
            page_size=page_size,
            prefer_sp=prefer_sp,
        )
        if not isinstance(payload, dict) or str(payload.get("status") or "").lower() != "success":
            return payload if isinstance(payload, dict) else {"status": "error", "message": "Failed to fetch paged rows for export"}

        rows = payload.get("rows")
        rows = rows if isinstance(rows, list) else []
        meta = payload.get("meta") if isinstance(payload.get("meta"), dict) else {}
        aggregate_meta = meta
        meta_total_pages = _corp_bill_summary_parse_int(meta.get("total_pages"), 0, 0, None)
        if meta_total_pages > 0:
            total_pages = meta_total_pages
        elif len(rows) >= int(page_size):
            total_pages = max(total_pages, page + 1)
        else:
            total_pages = max(total_pages, page)
        all_rows.extend(rows)

        if not rows:
            break
        page += 1

    return {
        "status": "success",
        "rows": all_rows,
        "meta": {
            "total_rows": int(len(all_rows)),
            "total_pages_fetched": int(max(1, page - 1)),
            "query_engine": str(aggregate_meta.get("query_engine") or ""),
        },
    }


# ===================== Corporate Receipt Reconciliation =====================
CORP_RECON_WRITEOFF_CANDIDATES = [
    "WriteOffAmt",
    "WriteOffAmount",
    "WriteOff_Amt",
    "Write_Off_Amt",
    "WriteOff",
    "writeoffAmt",
    "writeoffamount",
]
CORP_RECON_AUDITED_CANDIDATES = [
    "Audited",
    "IsAudited",
    "AuditOk",
    "AuditFlag",
]
_CORP_RECON_SCHEMA_READY_UNITS = set()
_CORP_RECON_SCHEMA_READY_META = {}
_CORP_RECON_SCHEMA_READY_LOCK = Lock()


def _ensure_corporate_writeoff_columns_conn(conn) -> dict:
    """
    Ensure write-off amount column exists in:
      - dbo.Corp_Receipt_Mst
      - dbo.CorpOpening
    Returns discovered column names and whether a new column was added.
    """
    result = {
        "receipt_column": None,
        "opening_column": None,
        "receipt_added": False,
        "opening_added": False,
    }
    if not conn:
        return result

    def _table_cols_map(table_name: str) -> dict:
        try:
            preview_df = pd.read_sql(f"SELECT TOP 0 * FROM {table_name}", conn)
            return {str(c).strip().lower(): str(c).strip() for c in preview_df.columns}
        except Exception:
            return {}

    def _pick_col(cols_map: dict) -> str | None:
        for cand in CORP_RECON_WRITEOFF_CANDIDATES:
            key = str(cand).strip().lower()
            if key in cols_map:
                return cols_map[key]
        return None

    def _ensure_for_table(table_name: str, result_col_key: str, result_added_key: str):
        cols_map = _table_cols_map(table_name)
        col_name = _pick_col(cols_map)
        if not col_name:
            try:
                cur = conn.cursor()
                cur.execute(f"ALTER TABLE {table_name} ADD [WriteOffAmt] FLOAT NULL")
                conn.commit()
                result[result_added_key] = True
            except Exception:
                try:
                    conn.rollback()
                except Exception:
                    pass
            cols_map = _table_cols_map(table_name)
            col_name = _pick_col(cols_map)
        result[result_col_key] = col_name

    _ensure_for_table("dbo.Corp_Receipt_Mst", "receipt_column", "receipt_added")
    _ensure_for_table("dbo.CorpOpening", "opening_column", "opening_added")
    return result


def _ensure_corporate_bill_audited_column_conn(conn) -> dict:
    """
    Ensure audit approval flag column exists in dbo.Corp_Bill_Mst.
    Returns discovered column name and whether a new column was added.
    """
    result = {
        "bill_audited_column": None,
        "bill_audited_added": False,
    }
    if not conn:
        return result

    def _table_cols_map(table_name: str) -> dict:
        try:
            preview_df = pd.read_sql(f"SELECT TOP 0 * FROM {table_name}", conn)
            return {str(c).strip().lower(): str(c).strip() for c in preview_df.columns}
        except Exception:
            return {}

    def _pick_col(cols_map: dict) -> str | None:
        for cand in CORP_RECON_AUDITED_CANDIDATES:
            key = str(cand or "").strip().lower()
            if key and key in cols_map:
                return cols_map[key]
        return None

    cols_map = _table_cols_map("dbo.Corp_Bill_Mst")
    col_name = _pick_col(cols_map)
    if not col_name:
        try:
            cur = conn.cursor()
            cur.execute("ALTER TABLE dbo.Corp_Bill_Mst ADD [Audited] BIT NULL")
            conn.commit()
            result["bill_audited_added"] = True
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
        cols_map = _table_cols_map("dbo.Corp_Bill_Mst")
        col_name = _pick_col(cols_map)
    result["bill_audited_column"] = col_name
    return result


def _ensure_corporate_recon_schema_once(conn, unit: str):
    unit_key = str(unit or "").strip().upper()
    if not unit_key:
        unit_key = "DEFAULT"

    with _CORP_RECON_SCHEMA_READY_LOCK:
        if unit_key in _CORP_RECON_SCHEMA_READY_UNITS:
            cached = _CORP_RECON_SCHEMA_READY_META.get(unit_key)
            if isinstance(cached, dict):
                return {
                    "writeoff": dict((cached.get("writeoff") or {})),
                    "audited": dict((cached.get("audited") or {})),
                }
            return None

    writeoff_info = _ensure_corporate_writeoff_columns_conn(conn)
    audited_info = _ensure_corporate_bill_audited_column_conn(conn)
    schema_info = {
        "writeoff": dict(writeoff_info or {}),
        "audited": dict(audited_info or {}),
    }

    with _CORP_RECON_SCHEMA_READY_LOCK:
        _CORP_RECON_SCHEMA_READY_UNITS.add(unit_key)
        _CORP_RECON_SCHEMA_READY_META[unit_key] = schema_info

    return {
        "writeoff": dict((schema_info.get("writeoff") or {})),
        "audited": dict((schema_info.get("audited") or {})),
    }


def fetch_corporate_reconciliation_page(
    unit: str,
    *,
    cutoff_date: str = "2025-03-31",
    bill_from: str | None = None,
    bill_to: str | None = None,
    receipt_from: str | None = None,
    receipt_to: str | None = None,
    include_cancelled: bool = False,
    q: str = "",
    kpi_filter: str = "",
    bill_source: str = "",
    patient_subtype: str = "",
    sort_by: str = "balance_all_time",
    sort_dir: str = "desc",
    page: int = 1,
    page_size: int = 25,
):
    """
    Preferred fast path for corporate reconciliation.
    Calls SQL stored procedure dbo.usp_CorpRecon_Page when available.

    Expected proc contract: returns one row with JSON columns:
      rows_json, receipt_details_json, kpis_json, meta_json, suspense_json (optional)
    """
    conn = get_sql_connection(unit)
    if not conn:
        return None

    try:
        def _table_cols_map(table_name: str) -> dict:
            if not table_name:
                return {}
            try:
                preview_df = pd.read_sql(f"SELECT TOP 0 * FROM {table_name}", conn)
                return {str(c).strip().lower(): str(c).strip() for c in preview_df.columns}
            except Exception:
                return {}

        def _pick_col(cols_map: dict, candidates: list[str]) -> str | None:
            for cand in candidates:
                key = str(cand).strip().lower()
                if key in cols_map:
                    return cols_map[key]
            return None

        schema_info = _ensure_corporate_recon_schema_once(conn, unit) or {}
        writeoff_info = dict((schema_info or {}).get("writeoff") or {})
        audited_info = dict((schema_info or {}).get("audited") or {})

        bill_cols_map = _table_cols_map("dbo.Corp_Bill_Mst")
        mst_cols_map = _table_cols_map("dbo.Corp_Receipt_Mst")
        dtl_cols_map = _table_cols_map("dbo.Corp_Receipt_Dtl")

        receipt_writeoff_col = (writeoff_info.get("receipt_column") or "").strip() or None
        opening_writeoff_col = (writeoff_info.get("opening_column") or "").strip() or None
        bill_audited_col = (audited_info.get("bill_audited_column") or "").strip() or None
        bill_updated_by_col = _pick_col(bill_cols_map, ["Updated_By", "UpdatedBy", "updated_by"])
        bill_updated_on_col = _pick_col(
            bill_cols_map,
            ["Updated_On", "UpdatedOn", "ModifiedOn", "accUpdatedDate", "AccUpdatedDate"],
        )
        dtl_receipt_date_col = _pick_col(dtl_cols_map, ["ReceiptDate"])
        dtl_inserted_by_col = _pick_col(dtl_cols_map, ["insertedBy", "InsertedBy", "Inserted_By"])
        rebate_col = _pick_col(mst_cols_map, ["rebateDiscountAmt", "RebateDiscountAmt"])
        tds_col = _pick_col(mst_cols_map, ["TDSAmt", "TdsAmt", "tdsAmt", "TDS_AMT"])

        # The SQL proc currently targets canonical column names for the hot path.
        # If the unit uses a schema variant, safely fall back to the Python path.
        canonical_required = {
            "receipt_writeoff_col": (receipt_writeoff_col or "").strip().lower() == "writeoffamt",
            "opening_writeoff_col": (opening_writeoff_col or "").strip().lower() == "writeoffamt",
            "bill_audited_col": (bill_audited_col or "").strip().lower() == "audited",
            "rebate_col": (rebate_col or "").strip().lower() == "rebatediscountamt",
            "tds_col": (tds_col or "").strip().lower() == "tdsamt",
        }
        if not all(canonical_required.values()):
            return None

        kpi_filter_key = str(kpi_filter or "").strip().lower()

        sql = """
        EXEC dbo.usp_CorpRecon_Page
             @CutoffDate = ?,
             @BillFrom = ?,
             @BillTo = ?,
             @ReceiptFrom = ?,
             @ReceiptTo = ?,
             @IncludeCancelled = ?,
             @Q = ?,
             @BillSource = ?,
             @PatientSubtype = ?,
             @SortBy = ?,
             @SortDir = ?,
             @Page = ?,
             @PageSize = ?,
             @ReceiptWriteoffColumn = ?,
             @OpeningWriteoffColumn = ?,
             @BillAuditedColumn = ?,
             @BillUpdatedByColumn = ?,
             @BillUpdatedOnColumn = ?,
             @DtlReceiptDateColumn = ?,
             @DtlInsertedByColumn = ?,
             @RebateDiscountColumn = ?,
             @TdsAmountColumn = ?
        """
        params = [
            cutoff_date,
            bill_from or None,
            bill_to or None,
            receipt_from or None,
            receipt_to or None,
            1 if include_cancelled else 0,
            str(q or ""),
            str(bill_source or ""),
            str(patient_subtype or ""),
            str(sort_by or "balance_all_time"),
            str(sort_dir or "desc"),
            int(page or 1),
            int(page_size or 25),
            receipt_writeoff_col,
            opening_writeoff_col,
            bill_audited_col,
            bill_updated_by_col,
            bill_updated_on_col,
            dtl_receipt_date_col,
            dtl_inserted_by_col,
            rebate_col,
            tds_col,
        ]
        if kpi_filter_key:
            sql += ",\n             @KpiFilter = ?"
            params.append(kpi_filter_key)

        cur = conn.cursor()
        cur.execute(sql, params)

        result_sets = []
        while True:
            if cur.description:
                cols = [str(c[0]).strip() for c in cur.description]
                rows = cur.fetchall()
                result_sets.append(
                    [
                        {cols[i]: row[i] for i in range(len(cols))}
                        for row in rows
                    ]
                )
            if not cur.nextset():
                break

        if not result_sets:
            return None

        first_set = result_sets[0]
        first_cols = set(first_set[0].keys()) if first_set and isinstance(first_set[0], dict) else set()
        json_contract = {"rows_json", "receipt_details_json", "kpis_json", "meta_json"}
        if json_contract.issubset(first_cols):
            row = first_set[0] if first_set else {}

            def _load_json(name: str, default):
                raw = row.get(name)
                if raw in (None, ""):
                    return default
                try:
                    return json.loads(raw)
                except Exception:
                    return default

            def _unwrap_obj(value, default):
                if isinstance(value, dict):
                    return value
                if isinstance(value, list) and value and isinstance(value[0], dict):
                    return value[0]
                return default

            rows_payload = _load_json("rows_json", [])
            if not isinstance(rows_payload, list):
                rows_payload = []

            details_payload = _load_json("receipt_details_json", {})
            if isinstance(details_payload, list):
                details_map = {}
                for rec in details_payload:
                    if not isinstance(rec, dict):
                        continue
                    bill_key = str(rec.get("bill_key") or "").strip()
                    if not bill_key:
                        bill_id_val = rec.get("bill_id")
                        try:
                            bill_key = f"BILL-{int(bill_id_val)}"
                        except Exception:
                            bill_key = ""
                    if not bill_key:
                        continue
                    rec_copy = dict(rec)
                    rec_copy.pop("bill_key", None)
                    details_map.setdefault(bill_key, []).append(rec_copy)
                details_payload = details_map
            elif not isinstance(details_payload, dict):
                details_payload = {}

            kpis_payload = _unwrap_obj(_load_json("kpis_json", {}), {})

            meta_payload = _unwrap_obj(_load_json("meta_json", {}), {})
            if not isinstance(meta_payload, dict):
                meta_payload = {}
            for arr_key in ("available_sources", "available_patient_subtypes"):
                arr_val = meta_payload.get(arr_key)
                if isinstance(arr_val, str):
                    try:
                        parsed_arr = json.loads(arr_val)
                        if isinstance(parsed_arr, list):
                            meta_payload[arr_key] = parsed_arr
                    except Exception:
                        pass
            if not isinstance(meta_payload.get("available_sources"), list):
                meta_payload["available_sources"] = []
            if not isinstance(meta_payload.get("available_patient_subtypes"), list):
                meta_payload["available_patient_subtypes"] = []
            meta_payload.setdefault("has_receipt_writeoff_column", bool(receipt_writeoff_col))
            meta_payload.setdefault("has_opening_writeoff_column", bool(opening_writeoff_col))
            meta_payload.setdefault("has_bill_audited_column", bool(bill_audited_col))
            meta_payload.setdefault("used_sql_sp", True)

            out = {
                "status": "success",
                "unit": str(unit or "").upper(),
                "cutoff_date": cutoff_date,
                "rows": rows_payload,
                "receipt_details": details_payload,
                "kpis": kpis_payload,
                "meta": meta_payload,
            }
            suspense_obj = _unwrap_obj(_load_json("suspense_json", None), None)
            if isinstance(suspense_obj, dict):
                out["suspense"] = suspense_obj
            return out

        rows_payload = first_set if isinstance(first_set, list) else []
        details_rows = result_sets[1] if len(result_sets) > 1 and isinstance(result_sets[1], list) else []
        kpis_payload = result_sets[2][0] if len(result_sets) > 2 and result_sets[2] else {}
        meta_payload = result_sets[3][0] if len(result_sets) > 3 and result_sets[3] else {}
        sources_rows = result_sets[4] if len(result_sets) > 4 and isinstance(result_sets[4], list) else []
        subtypes_rows = result_sets[5] if len(result_sets) > 5 and isinstance(result_sets[5], list) else []
        subtype_summary_rows = []
        suspense_row = {}
        if len(result_sets) > 6 and isinstance(result_sets[6], list):
            rs7 = result_sets[6]
            rs7_first = rs7[0] if rs7 else {}
            if isinstance(rs7_first, dict) and "suspense_count" in rs7_first:
                suspense_row = rs7_first
            else:
                subtype_summary_rows = rs7
                if len(result_sets) > 7 and isinstance(result_sets[7], list) and result_sets[7]:
                    rs8_first = result_sets[7][0]
                    if isinstance(rs8_first, dict):
                        suspense_row = rs8_first

        details_payload = {}
        for rec in details_rows:
            if not isinstance(rec, dict):
                continue
            bill_key = str(rec.get("bill_key") or "").strip()
            if not bill_key:
                bill_id_val = rec.get("bill_id")
                try:
                    bill_key = f"BILL-{int(bill_id_val)}"
                except Exception:
                    bill_key = ""
            if not bill_key:
                continue
            rec_copy = dict(rec)
            rec_copy.pop("bill_key", None)
            details_payload.setdefault(bill_key, []).append(rec_copy)

        if not isinstance(kpis_payload, dict):
            kpis_payload = {}
        if not isinstance(meta_payload, dict):
            meta_payload = {}

        meta_payload["available_sources"] = [
            str((r or {}).get("bill_source") or "").strip()
            for r in sources_rows
            if str((r or {}).get("bill_source") or "").strip()
        ]
        meta_payload["available_patient_subtypes"] = [
            str((r or {}).get("patient_subtype") or "").strip()
            for r in subtypes_rows
            if str((r or {}).get("patient_subtype") or "").strip()
        ]
        normalized_subtype_summary = []
        for row in (subtype_summary_rows or []):
            if not isinstance(row, dict):
                continue
            subtype_name = str((row or {}).get("subtype") or "").strip()
            if not subtype_name:
                continue
            try:
                bills_val = int((row or {}).get("bills") or 0)
            except Exception:
                bills_val = 0
            has_corporate_col = "corporate_bills" in row
            has_opening_col = "opening_bills" in row
            try:
                corporate_val = int((row or {}).get("corporate_bills") or 0)
            except Exception:
                corporate_val = 0
            try:
                opening_val = int((row or {}).get("opening_bills") or 0)
            except Exception:
                opening_val = 0
            if bills_val > 0 and not has_corporate_col and not has_opening_col:
                # Backward-compatible default for older proc versions.
                corporate_val = bills_val
                opening_val = 0
            try:
                settled_val = int((row or {}).get("settled_count") or 0)
            except Exception:
                settled_val = 0
            has_settled_corp_col = "settled_corporate_count" in row
            has_settled_open_col = "settled_opening_count" in row
            try:
                settled_corporate_val = int((row or {}).get("settled_corporate_count") or 0)
            except Exception:
                settled_corporate_val = 0
            try:
                settled_opening_val = int((row or {}).get("settled_opening_count") or 0)
            except Exception:
                settled_opening_val = 0
            if settled_val > 0 and not has_settled_corp_col and not has_settled_open_col:
                settled_corporate_val = settled_val
                settled_opening_val = 0
            try:
                partial_val = int((row or {}).get("partial_count") or 0)
            except Exception:
                partial_val = 0
            has_partial_corp_col = "partial_corporate_count" in row
            has_partial_open_col = "partial_opening_count" in row
            try:
                partial_corporate_val = int((row or {}).get("partial_corporate_count") or 0)
            except Exception:
                partial_corporate_val = 0
            try:
                partial_opening_val = int((row or {}).get("partial_opening_count") or 0)
            except Exception:
                partial_opening_val = 0
            if partial_val > 0 and not has_partial_corp_col and not has_partial_open_col:
                partial_corporate_val = partial_val
                partial_opening_val = 0
            try:
                unpaid_val = int((row or {}).get("unpaid_count") or 0)
            except Exception:
                unpaid_val = 0
            has_unpaid_corp_col = "unpaid_corporate_count" in row
            has_unpaid_open_col = "unpaid_opening_count" in row
            try:
                unpaid_corporate_val = int((row or {}).get("unpaid_corporate_count") or 0)
            except Exception:
                unpaid_corporate_val = 0
            try:
                unpaid_opening_val = int((row or {}).get("unpaid_opening_count") or 0)
            except Exception:
                unpaid_opening_val = 0
            if unpaid_val > 0 and not has_unpaid_corp_col and not has_unpaid_open_col:
                unpaid_corporate_val = unpaid_val
                unpaid_opening_val = 0
            try:
                overpaid_val = int((row or {}).get("overpaid_count") or 0)
            except Exception:
                overpaid_val = 0
            has_overpaid_corp_col = "overpaid_corporate_count" in row
            has_overpaid_open_col = "overpaid_opening_count" in row
            try:
                overpaid_corporate_val = int((row or {}).get("overpaid_corporate_count") or 0)
            except Exception:
                overpaid_corporate_val = 0
            try:
                overpaid_opening_val = int((row or {}).get("overpaid_opening_count") or 0)
            except Exception:
                overpaid_opening_val = 0
            if overpaid_val > 0 and not has_overpaid_corp_col and not has_overpaid_open_col:
                overpaid_corporate_val = overpaid_val
                overpaid_opening_val = 0
            try:
                closing_val = float((row or {}).get("closing_balance") or 0.0)
            except Exception:
                closing_val = 0.0
            normalized_subtype_summary.append(
                {
                    "subtype": subtype_name,
                    "bills": bills_val,
                    "corporate_bills": corporate_val,
                    "opening_bills": opening_val,
                    "settled_count": settled_val,
                    "settled_corporate_count": settled_corporate_val,
                    "settled_opening_count": settled_opening_val,
                    "partial_count": partial_val,
                    "partial_corporate_count": partial_corporate_val,
                    "partial_opening_count": partial_opening_val,
                    "unpaid_count": unpaid_val,
                    "unpaid_corporate_count": unpaid_corporate_val,
                    "unpaid_opening_count": unpaid_opening_val,
                    "overpaid_count": overpaid_val,
                    "overpaid_corporate_count": overpaid_corporate_val,
                    "overpaid_opening_count": overpaid_opening_val,
                    "closing_balance": closing_val,
                }
            )
        meta_payload["subtype_closing_summary"] = normalized_subtype_summary
        meta_payload["subtype_closing_scope"] = "full_filter" if normalized_subtype_summary else "page"
        meta_payload.setdefault("has_receipt_writeoff_column", bool(receipt_writeoff_col))
        meta_payload.setdefault("has_opening_writeoff_column", bool(opening_writeoff_col))
        meta_payload.setdefault("has_bill_audited_column", bool(bill_audited_col))
        meta_payload.setdefault("used_sql_sp", True)
        meta_payload.setdefault("suspense_count", int((suspense_row or {}).get("suspense_count") or 0))

        out = {
            "status": "success",
            "unit": str(unit or "").upper(),
            "cutoff_date": cutoff_date,
            "rows": rows_payload,
            "receipt_details": details_payload,
            "kpis": kpis_payload,
            "meta": meta_payload,
        }
        if suspense_row:
            out["suspense"] = {
                "meta": {
                    "count": int((suspense_row or {}).get("suspense_count") or 0),
                    "reason": str((suspense_row or {}).get("suspense_reason") or ""),
                }
            }
        return out
    except Exception:
        # Silent fallback to python path when proc is absent or incompatible.
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def fetch_corporate_reconciliation_raw(unit: str, cutoff_date: str = "2025-03-31", bill_ids=None):
    """
    Fetch canonical corporate bill universe + linked corporate receipt lines.
    bill_ids (optional): limit to specific bill ids for targeted fetch.
    Returns a dict with:
      - bills: one row per canonical bill
      - receipts: one row per receipt detail linked to billId
      - meta: compatibility flags used by callers
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Could not connect to {unit} for corporate reconciliation")
        return None
    try:
        schema_ensure_info = _ensure_corporate_recon_schema_once(conn, unit) or {}
        audited_schema_info = (schema_ensure_info or {}).get("audited") or {}

        normalized_bill_ids = []
        if bill_ids:
            try:
                normalized_bill_ids = sorted(
                    {
                        int(v)
                        for v in list(bill_ids)
                        if str(v).strip().lstrip("-").isdigit() and int(v) > 0
                    }
                )
            except Exception:
                normalized_bill_ids = []

        def _pick_col(cols_map: dict, candidates: list[str]):
            for cand in candidates:
                key = cand.strip().lower()
                if key in cols_map:
                    return cols_map[key]
            return None

        def _first_existing_table(candidates: list[str]):
            for table_name in candidates:
                try:
                    pd.read_sql(f"SELECT TOP 0 * FROM {table_name}", conn)
                    return table_name
                except Exception:
                    continue
            return None

        def _table_cols_map(table_name: str) -> dict:
            if not table_name:
                return {}
            try:
                preview_df = pd.read_sql(f"SELECT TOP 0 * FROM {table_name}", conn)
                return {str(c).strip().lower(): str(c).strip() for c in preview_df.columns}
            except Exception:
                return {}

        dtl_cols_map = _table_cols_map("dbo.Corp_Receipt_Dtl")
        mst_cols_map = _table_cols_map("dbo.Corp_Receipt_Mst")
        bill_cols_map = _table_cols_map("dbo.Corp_Bill_Mst")
        open_cols_map = _table_cols_map("dbo.CorpOpening")
        dtl_cols = set(dtl_cols_map.keys())
        mst_cols = set(mst_cols_map.keys())

        dtl_receipt_date_col = _pick_col(dtl_cols_map, ["ReceiptDate"])
        rebate_discount_col = _pick_col(mst_cols_map, ["rebateDiscountAmt", "RebateDiscountAmt"])
        tds_amt_col = _pick_col(mst_cols_map, ["TDSAmt", "TdsAmt", "tdsAmt", "TDS_AMT"])
        mst_writeoff_col = _pick_col(mst_cols_map, CORP_RECON_WRITEOFF_CANDIDATES)
        opening_writeoff_col = _pick_col(open_cols_map, CORP_RECON_WRITEOFF_CANDIDATES)
        bill_audited_col = _pick_col(bill_cols_map, CORP_RECON_AUDITED_CANDIDATES) or (
            (audited_schema_info or {}).get("bill_audited_column")
        )
        dtl_inserted_by_col = _pick_col(dtl_cols_map, ["insertedBy", "InsertedBy", "Inserted_By"])
        bill_updated_by_col = _pick_col(bill_cols_map, ["Updated_By", "UpdatedBy", "updated_by"])
        bill_updated_on_col = _pick_col(
            bill_cols_map,
            ["Updated_On", "UpdatedOn", "ModifiedOn", "accUpdatedDate", "AccUpdatedDate"],
        )

        has_dtl_receipt_date = bool(dtl_receipt_date_col)
        has_rebate_discount_amt = bool(rebate_discount_col)
        has_tds_amt = bool(tds_amt_col)
        has_mst_writeoff_amt = bool(mst_writeoff_col)
        has_opening_writeoff_amt = bool(opening_writeoff_col)
        has_bill_audited_flag = bool(bill_audited_col)
        receipt_date_expr = f"d.[{dtl_receipt_date_col}]" if dtl_receipt_date_col else "m.Receipt_Date"
        rebate_discount_expr = (
            f"CAST(ISNULL(m.[{rebate_discount_col}], 0) AS FLOAT)"
            if rebate_discount_col
            else "CAST(0 AS FLOAT)"
        )
        tds_amt_expr = (
            f"CAST(ISNULL(m.[{tds_amt_col}], 0) AS FLOAT)"
            if tds_amt_col
            else "CAST(0 AS FLOAT)"
        )
        mst_writeoff_expr = (
            f"CAST(ISNULL(m.[{mst_writeoff_col}], 0) AS FLOAT)"
            if mst_writeoff_col
            else "CAST(0 AS FLOAT)"
        )
        opening_writeoff_expr = (
            f"CAST(ISNULL(o.[{opening_writeoff_col}], 0) AS FLOAT)"
            if opening_writeoff_col
            else "CAST(0 AS FLOAT)"
        )
        bill_audited_expr = (
            "CAST(CASE "
            f"WHEN LTRIM(RTRIM(CONVERT(NVARCHAR(20), ISNULL(b.[{bill_audited_col}], 0)))) "
            "IN ('1','Y','y','YES','Yes','TRUE','True','true') THEN 1 ELSE 0 END AS INT)"
            if bill_audited_col
            else "CAST(0 AS INT)"
        )
        def _safe_int_expr(raw_expr: str) -> str:
            expr = str(raw_expr or "").strip()
            if not expr:
                return "CAST(NULL AS INT)"
            txt = f"LTRIM(RTRIM(CONVERT(NVARCHAR(100), {expr})))"
            return (
                "CASE "
                f"WHEN {txt} = '' THEN NULL "
                f"WHEN {txt} LIKE '%[^0-9]%' THEN NULL "
                f"ELSE CAST({expr} AS INT) END"
            )

        bill_updated_on_expr = (
            "CAST(CASE "
            f"WHEN ISDATE(CONVERT(NVARCHAR(50), b.[{bill_updated_on_col}])) = 1 "
            f"THEN CONVERT(DATETIME, b.[{bill_updated_on_col}]) "
            "ELSE NULL END AS DATETIME)"
            if bill_updated_on_col
            else "CAST(NULL AS DATETIME)"
        )
        bill_updated_by_id_expr = (
            _safe_int_expr(f"b.[{bill_updated_by_col}]")
            if bill_updated_by_col
            else "CAST(NULL AS INT)"
        )
        dtl_inserted_by_id_expr = (
            _safe_int_expr(f"d.[{dtl_inserted_by_col}]")
            if dtl_inserted_by_col
            else "CAST(NULL AS INT)"
        )

        pm_table = _first_existing_table([
            "dbo.PaymentMode_Mst",
            "dbo.Paymentmode_mst",
            "dbo.PaymentModeMst",
        ])
        pm_cols = _table_cols_map(pm_table) if pm_table else {}
        pm_id_col = _pick_col(pm_cols, ["PModeID", "PaymentModeID", "Id"]) if pm_cols else None
        pm_name_col = _pick_col(pm_cols, ["PModeName", "PaymentMode", "ModeName", "PaymentModeName"]) if pm_cols else None
        payment_mode_expr = "CONVERT(NVARCHAR(100), m.CPayment_Mode)"
        payment_mode_join = ""
        if pm_table and pm_id_col and pm_name_col:
            payment_mode_join = f"LEFT JOIN {pm_table} pm WITH (NOLOCK) ON pm.[{pm_id_col}] = m.CPayment_Mode"
            payment_mode_expr = (
                f"LTRIM(RTRIM(ISNULL(CONVERT(NVARCHAR(200), pm.[{pm_name_col}]), "
                f"CONVERT(NVARCHAR(100), m.CPayment_Mode))))"
            )

        user_table = _first_existing_table([
            "dbo.User_Mst",
            "dbo.user_mst",
            "dbo.UserMst",
        ])
        user_cols = _table_cols_map(user_table) if user_table else {}
        user_id_col = _pick_col(user_cols, ["UserId", "Userid", "User_ID", "Id", "AccountId"]) if user_cols else None
        user_name_col = _pick_col(user_cols, ["UserName", "User_Name", "Username", "Name", "EmpName"]) if user_cols else None

        bill_user_join = ""
        bill_updated_by_name_expr = "CAST('' AS NVARCHAR(200))"
        if user_table and user_id_col and user_name_col:
            bill_user_join = f"LEFT JOIN {user_table} u_bill WITH (NOLOCK) ON u_bill.[{user_id_col}] = c.BillUpdatedById"
            bill_updated_by_name_expr = (
                "CASE WHEN c.BillUpdatedById IS NULL THEN '' ELSE "
                f"LTRIM(RTRIM(ISNULL(CONVERT(NVARCHAR(200), u_bill.[{user_name_col}]), "
                "CONVERT(NVARCHAR(100), c.BillUpdatedById)))) END"
            )
        elif bill_updated_by_col:
            bill_updated_by_name_expr = (
                "CASE WHEN c.BillUpdatedById IS NULL THEN '' ELSE "
                "CONVERT(NVARCHAR(100), c.BillUpdatedById) END"
            )

        receipt_user_join = ""
        inserted_by_name_expr = "CAST('' AS NVARCHAR(200))"
        if user_table and user_id_col and user_name_col and dtl_inserted_by_col:
            receipt_user_join = (
                f"LEFT JOIN {user_table} u_dtl WITH (NOLOCK) "
                f"ON u_dtl.[{user_id_col}] = {dtl_inserted_by_id_expr}"
            )
            inserted_by_name_expr = (
                f"LTRIM(RTRIM(ISNULL(CONVERT(NVARCHAR(200), u_dtl.[{user_name_col}]), "
                f"CONVERT(NVARCHAR(100), {dtl_inserted_by_id_expr}))))"
            )
        elif dtl_inserted_by_col:
            inserted_by_name_expr = f"CONVERT(NVARCHAR(100), {dtl_inserted_by_id_expr})"

        bill_scope_clause = ""
        bill_scope_params = []
        if normalized_bill_ids:
            placeholders = ",".join(["?"] * len(normalized_bill_ids))
            bill_scope_clause = f" AND c.BillId IN ({placeholders})"
            bill_scope_params = list(normalized_bill_ids)

        bills_sql = f"""
            ;WITH bill_mst AS (
                SELECT
                    CAST(b.CBill_ID AS INT) AS BillId,
                    CAST(NULLIF(b.Visit_ID, 0) AS INT) AS BillVisitId,
                    CAST(NULLIF(b.PatientID, 0) AS INT) AS BillPatientId,
                    CAST(b.Submit_Date AS DATETIME) AS SubmitDateMst,
                    CAST(b.CBill_Date AS DATETIME) AS CBillDateMst,
                    CAST(COALESCE(b.Submit_Date, b.CBill_Date) AS DATETIME) AS BillDateMst,
                    CAST(ISNULL(b.CAmount, 0) AS FLOAT) AS BillAmountMst,
                    CAST(
                        ISNULL(
                            NULLIF(CONVERT(NVARCHAR(80), b.CBill_NO), ''),
                            NULLIF(CONVERT(NVARCHAR(80), b.Bill_No), '')
                        ) AS NVARCHAR(80)
                    ) AS BillNoMst,
                    CAST(NULLIF(b.PatientTypeId, 0) AS INT) AS BillPatientTypeId,
                    CAST(NULLIF(b.PatientTypeIdSrNo, 0) AS INT) AS BillPatientSubTypeId,
                    CAST(ISNULL(b.Due_Amt, ISNULL(b.dueAmount, 0)) AS FLOAT) AS DueAmountMst,
                    CAST(ISNULL(b.Status, '') AS NVARCHAR(80)) AS BillStatusMst,
                    {bill_audited_expr} AS BillAuditedFlag,
                    {bill_updated_on_expr} AS BillUpdatedOnRaw,
                    {bill_updated_by_id_expr} AS BillUpdatedById
                FROM dbo.Corp_Bill_Mst b WITH (NOLOCK)
            ),
            opening AS (
                SELECT
                    CAST(o.OPId AS INT) AS BillId,
                    CAST(NULLIF(o.PatientId, 0) AS INT) AS OpenPatientId,
                    CAST(o.DueDate AS DATETIME) AS BillDateOpening,
                    CAST(o.DueDate AS DATETIME) AS DueDateOpening,
                    CAST(ISNULL(o.DueAmount, 0) AS FLOAT) AS BillAmountOpening,
                    CAST(ISNULL(o.RefNo, '') AS NVARCHAR(80)) AS BillNoOpening,
                    CAST(ISNULL(o.PatientName, '') AS NVARCHAR(255)) AS OpenPatientName,
                    CAST(NULLIF(o.PatientTypeId, 0) AS INT) AS OpenPatientTypeId,
                    CAST(NULLIF(o.PatientSubTypeId, 0) AS INT) AS OpenPatientSubTypeId,
                    CAST(ISNULL(o.ReceiptAmt, 0) AS FLOAT) AS OpeningReceiptAmt,
                    CAST(ISNULL(o.ReceiptId, 0) AS INT) AS OpeningReceiptId,
                    {opening_writeoff_expr} AS OpeningWriteOffAmt
                FROM dbo.CorpOpening o WITH (NOLOCK)
            ),
            bill_mst_post AS (
                SELECT
                    b.BillId,
                    b.BillVisitId,
                    b.BillPatientId,
                    b.SubmitDateMst,
                    b.CBillDateMst,
                    b.BillDateMst,
                    b.BillAmountMst,
                    b.BillNoMst,
                    b.BillPatientTypeId,
                    b.BillPatientSubTypeId,
                    b.DueAmountMst,
                    b.BillStatusMst,
                    b.BillAuditedFlag,
                    b.BillUpdatedOnRaw,
                    b.BillUpdatedById
                FROM bill_mst b
                WHERE b.BillDateMst > CAST(? AS DATETIME)
            ),
            canonical AS (
                SELECT
                    COALESCE(b.BillId, o.BillId) AS BillId,
                    CASE WHEN b.BillId IS NOT NULL THEN 'BILL_MST_POST' ELSE 'OPENING' END AS BillSource,
                    CASE WHEN b.BillId IS NOT NULL THEN b.BillDateMst ELSE o.BillDateOpening END AS BillDate,
                    CASE WHEN b.BillId IS NOT NULL THEN b.BillAmountMst ELSE o.BillAmountOpening END AS BillAmount,
                    CASE WHEN b.BillId IS NOT NULL THEN b.BillNoMst ELSE o.BillNoOpening END AS BillNo,
                    CASE WHEN b.BillId IS NOT NULL THEN b.BillPatientId ELSE o.OpenPatientId END AS PatientId,
                    CASE WHEN b.BillId IS NOT NULL THEN b.BillPatientTypeId ELSE o.OpenPatientTypeId END AS PatientTypeId,
                    CASE WHEN b.BillId IS NOT NULL THEN b.BillPatientSubTypeId ELSE o.OpenPatientSubTypeId END AS PatientSubTypeId,
                    CASE WHEN b.BillId IS NOT NULL THEN b.BillVisitId ELSE CAST(NULL AS INT) END AS VisitId,
                    CASE WHEN b.BillId IS NOT NULL THEN b.SubmitDateMst ELSE CAST(NULL AS DATETIME) END AS SubmitDateRaw,
                    CASE WHEN b.BillId IS NOT NULL THEN b.CBillDateMst ELSE CAST(NULL AS DATETIME) END AS CBillDateRaw,
                    CASE
                        WHEN b.BillId IS NOT NULL THEN CAST(NULL AS DATETIME)
                        ELSE o.DueDateOpening
                    END AS DueDate,
                    ISNULL(o.OpenPatientName, '') AS SourcePatientName,
                    CASE WHEN b.BillId IS NOT NULL THEN ISNULL(b.BillStatusMst, '') ELSE '' END AS BillStatusRaw,
                    CASE WHEN b.BillId IS NOT NULL THEN ISNULL(b.DueAmountMst, 0) ELSE 0 END AS BillDueAmountRaw,
                    CASE WHEN b.BillId IS NOT NULL THEN ISNULL(b.BillAuditedFlag, 0) ELSE 0 END AS BillAuditedFlag,
                    CASE WHEN b.BillId IS NOT NULL THEN b.BillUpdatedOnRaw ELSE CAST(NULL AS DATETIME) END AS BillUpdatedOnRaw,
                    CASE WHEN b.BillId IS NOT NULL THEN b.BillUpdatedById ELSE CAST(NULL AS INT) END AS BillUpdatedById,
                    ISNULL(o.OpeningReceiptAmt, 0) AS OpeningReceiptAmt,
                    ISNULL(o.OpeningReceiptId, 0) AS OpeningReceiptId,
                    ISNULL(o.OpeningWriteOffAmt, 0) AS OpeningWriteOffAmt
                FROM bill_mst_post b
                FULL OUTER JOIN opening o
                    ON b.BillId = o.BillId
                WHERE b.BillId IS NOT NULL OR o.BillId IS NOT NULL
            ),
            receipt_visit AS (
                SELECT
                    CAST(d.billId AS INT) AS BillId,
                    MAX(CASE WHEN ISNULL(d.visitId, 0) > 0 THEN CAST(d.visitId AS INT) ELSE CAST(ISNULL(m.VisitID, 0) AS INT) END) AS BestVisitId,
                    MAX(CASE WHEN ISNULL(d.PatientId, 0) > 0 THEN CAST(d.PatientId AS INT) ELSE CAST(ISNULL(m.PatientID, 0) AS INT) END) AS BestPatientId
                FROM dbo.Corp_Receipt_Dtl d WITH (NOLOCK)
                LEFT JOIN dbo.Corp_Receipt_Mst m WITH (NOLOCK)
                    ON d.receiptId = m.Receipt_ID
                GROUP BY d.billId
            ),
            canonical_enriched AS (
                SELECT
                    c.BillId,
                    c.BillSource,
                    c.BillDate,
                    c.BillAmount,
                    c.BillNo,
                    CAST(NULLIF(COALESCE(c.PatientId, rv.BestPatientId), 0) AS INT) AS PatientId,
                    CAST(NULLIF(COALESCE(c.VisitId, rv.BestVisitId), 0) AS INT) AS VisitId,
                    c.PatientTypeId,
                    c.PatientSubTypeId,
                    c.SubmitDateRaw,
                    c.CBillDateRaw,
                    c.DueDate,
                    c.SourcePatientName,
                    c.BillStatusRaw,
                    c.BillDueAmountRaw,
                    c.BillAuditedFlag,
                    c.BillUpdatedOnRaw,
                    c.BillUpdatedById,
                    c.OpeningReceiptAmt,
                    c.OpeningReceiptId,
                    c.OpeningWriteOffAmt
                FROM canonical c
                LEFT JOIN receipt_visit rv
                    ON rv.BillId = c.BillId
            )
            SELECT
                c.BillId,
                c.BillSource,
                c.BillDate,
                c.BillAmount,
                c.BillNo,
                c.PatientId,
                c.VisitId,
                c.SubmitDateRaw,
                c.CBillDateRaw,
                c.DueDate,
                c.SourcePatientName,
                c.BillStatusRaw,
                c.BillDueAmountRaw,
                c.BillAuditedFlag,
                c.BillUpdatedOnRaw,
                c.BillUpdatedById,
                {bill_updated_by_name_expr} AS BillUpdatedByName,
                c.OpeningReceiptAmt,
                c.OpeningReceiptId,
                c.OpeningWriteOffAmt,
                ISNULL(v.TypeOfVisit, '') AS TypeOfVisit,
                v.VisitDate,
                v.DischargeDate,
                CASE
                    WHEN c.BillSource = 'OPENING' AND ISNULL(c.SourcePatientName, '') <> '' THEN ISNULL(c.SourcePatientName, '')
                    WHEN c.BillSource = 'OPENING' AND c.PatientId IS NOT NULL THEN ISNULL(dbo.fn_patientfullname(c.PatientId), '')
                    WHEN v.PatientID IS NOT NULL THEN ISNULL(dbo.fn_patientfullname(v.PatientID), '')
                    WHEN c.PatientId IS NOT NULL THEN ISNULL(dbo.fn_patientfullname(c.PatientId), '')
                    ELSE ISNULL(c.SourcePatientName, '')
                END AS PatientName,
                CASE
                    WHEN c.BillSource = 'OPENING' AND c.PatientTypeId IS NOT NULL THEN ISNULL(dbo.fn_pat_type(c.PatientTypeId), '')
                    WHEN v.PatientType_ID IS NOT NULL THEN ISNULL(dbo.fn_pat_type(v.PatientType_ID), '')
                    WHEN c.PatientTypeId IS NOT NULL THEN ISNULL(dbo.fn_pat_type(c.PatientTypeId), '')
                    ELSE ''
                END AS PatientType,
                CASE
                    WHEN c.BillSource = 'OPENING' AND c.PatientSubTypeId IS NOT NULL THEN ISNULL(dbo.fn_patsub_type(c.PatientSubTypeId), '')
                    WHEN v.PatientSubType_ID IS NOT NULL THEN ISNULL(dbo.fn_patsub_type(v.PatientSubType_ID), '')
                    WHEN c.PatientSubTypeId IS NOT NULL THEN ISNULL(dbo.fn_patsub_type(c.PatientSubTypeId), '')
                    ELSE ''
                END AS PatientSubType,
                CASE
                    WHEN v.DepartmentID IS NULL THEN ''
                    ELSE ISNULL(dbo.fn_dept(v.DepartmentID), '')
                END AS Dept,
                CASE
                    WHEN v.UnitID IS NULL THEN ''
                    ELSE ISNULL(dbo.Fn_subDept(v.UnitID), '')
                END AS SubDept
            FROM canonical_enriched c
            LEFT JOIN dbo.Visit v WITH (NOLOCK)
                ON v.Visit_ID = c.VisitId
            {bill_user_join}
            WHERE c.BillId IS NOT NULL
            {bill_scope_clause}
        """
        bills_df = pd.read_sql(bills_sql, conn, params=[cutoff_date] + bill_scope_params)

        receipt_scope_clause = ""
        receipt_scope_params = []
        if normalized_bill_ids:
            placeholders = ",".join(["?"] * len(normalized_bill_ids))
            receipt_scope_clause = f"WHERE d.billId IN ({placeholders})"
            receipt_scope_params = list(normalized_bill_ids)

        receipts_sql = f"""
            SELECT
                CAST(d.recDtlId AS INT) AS ReceiptDetailId,
                CAST(d.receiptId AS INT) AS ReceiptId,
                CAST(d.billId AS INT) AS BillId,
                CAST(ISNULL(d.billAmt, 0) AS FLOAT) AS BillAmtDtl,
                CAST(ISNULL(d.receiptAmt, 0) AS FLOAT) AS ReceiptAmtDtl,
                CAST(ISNULL(d.dueAmt, 0) AS FLOAT) AS DueAmtDtl,
                CAST(NULLIF(d.visitId, 0) AS INT) AS DtlVisitId,
                CAST(NULLIF(d.PatientId, 0) AS INT) AS DtlPatientId,
                d.insertedOn AS InsertedOn,
                {dtl_inserted_by_id_expr} AS InsertedById,
                {inserted_by_name_expr} AS InsertedByName,
                {receipt_date_expr} AS ReceiptDate,
                m.Receipt_Date AS ReceiptDateMst,
                ISNULL(CONVERT(NVARCHAR(80), m.CReceipt_No), '') AS ReceiptNo,
                CAST(ISNULL(m.Amount, 0) AS FLOAT) AS ReceiptMstAmount,
                CAST(ISNULL(m.NetAmt, ISNULL(m.Amount, 0)) AS FLOAT) AS ReceiptNetAmt,
                CAST(ISNULL(m.GrossAmt, ISNULL(m.Amount, 0)) AS FLOAT) AS ReceiptGrossAmt,
                {tds_amt_expr} AS TDSAmt,
                {mst_writeoff_expr} AS WriteOffAmt,
                CAST(ISNULL(m.Cancelstatus, 0) AS INT) AS CancelStatus,
                ISNULL(CONVERT(NVARCHAR(120), m.UTRNo), '') AS UTRNo,
                CAST(ISNULL(m.CPayment_Mode, 0) AS INT) AS PaymentModeId,
                {payment_mode_expr} AS PaymentMode,
                {rebate_discount_expr} AS RebateDiscountAmt,
                CAST(NULLIF(m.VisitID, 0) AS INT) AS MstVisitId,
                CAST(NULLIF(m.PatientID, 0) AS INT) AS MstPatientId
            FROM dbo.Corp_Receipt_Dtl d WITH (NOLOCK)
            LEFT JOIN dbo.Corp_Receipt_Mst m WITH (NOLOCK)
                ON d.receiptId = m.Receipt_ID
            {payment_mode_join}
            {receipt_user_join}
            {receipt_scope_clause}
        """
        if receipt_scope_params:
            receipts_df = pd.read_sql(receipts_sql, conn, params=receipt_scope_params)
        else:
            receipts_df = pd.read_sql(receipts_sql, conn)

        if bills_df is None:
            bills_df = pd.DataFrame()
        if receipts_df is None:
            receipts_df = pd.DataFrame()

        if not bills_df.empty:
            bills_df.columns = [c.strip() for c in bills_df.columns]
            bills_df["Unit"] = (unit or "").upper()
        if not receipts_df.empty:
            receipts_df.columns = [c.strip() for c in receipts_df.columns]
            receipts_df["Unit"] = (unit or "").upper()

        return {
            "bills": bills_df,
            "receipts": receipts_df,
            "meta": {
                "has_dtl_receipt_date": bool(has_dtl_receipt_date),
                "has_rebate_discount_amt": bool(has_rebate_discount_amt),
                "has_tds_amt": bool(has_tds_amt),
                "has_receipt_writeoff_amt": bool(has_mst_writeoff_amt),
                "has_opening_writeoff_amt": bool(has_opening_writeoff_amt),
                "has_bill_audited_flag": bool(has_bill_audited_flag),
                "receipt_writeoff_column": mst_writeoff_col,
                "opening_writeoff_column": opening_writeoff_col,
                "bill_audited_column": bill_audited_col,
                "cutoff_date": cutoff_date,
            },
        }
    except Exception as e:
        print(f"Error fetching corporate reconciliation raw ({unit}): {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def apply_corporate_reconciliation_writeoff(
    unit: str,
    *,
    source_key: str,
    bill_id: int,
    writeoff_amount: float,
    target_receipt_id: int | None = None,
):
    """
    Persist write-off amount entry for a corporate reconciliation bill.
    Notes:
      - Updates only write-off column.
      - Does NOT update any due column.
    """
    conn = get_sql_connection(unit)
    if not conn:
        return {"status": "error", "message": f"Could not connect to {unit}"}

    try:
        def _table_cols_map(table_name: str) -> dict:
            if not table_name:
                return {}
            try:
                preview_df = pd.read_sql(f"SELECT TOP 0 * FROM {table_name}", conn)
                return {str(c).strip().lower(): str(c).strip() for c in preview_df.columns}
            except Exception:
                return {}

        def _pick_col(cols_map: dict, candidates: list[str]):
            for cand in candidates:
                key = str(cand or "").strip().lower()
                if key and key in cols_map:
                    return cols_map[key]
            return None

        def _to_pos_int(value):
            try:
                parsed = int(str(value).strip())
            except Exception:
                return None
            return parsed if parsed > 0 else None

        source_norm = str(source_key or "").strip().upper()
        bill_id_int = _to_pos_int(bill_id)
        if not bill_id_int:
            return {"status": "error", "message": "Invalid bill id"}

        try:
            amount = float(writeoff_amount)
        except Exception:
            amount = 0.0
        if amount <= 0:
            return {"status": "error", "message": "Write-off amount must be greater than zero"}

        schema_info = _ensure_corporate_writeoff_columns_conn(conn)

        cursor = conn.cursor()

        if source_norm == "OPENING":
            opening_cols = _table_cols_map("dbo.CorpOpening")
            opening_writeoff_col = _pick_col(opening_cols, CORP_RECON_WRITEOFF_CANDIDATES) or schema_info.get("opening_column")
            opening_updated_on_col = _pick_col(
                opening_cols,
                ["UpdatedOn", "Updated_On", "ModifiedOn", "LastUpdatedOn", "AccUpdatedDate", "accUpdatedDate"],
            )
            if not opening_writeoff_col:
                return {
                    "status": "error",
                    "message": "Write-off column not found in CorpOpening. Add a write-off amount column first.",
                }

            cursor.execute("SELECT TOP 1 1 FROM dbo.CorpOpening WITH (NOLOCK) WHERE OPId = ?", (bill_id_int,))
            if not cursor.fetchone():
                return {"status": "error", "message": f"Opening bill {bill_id_int} not found"}

            opening_set_sql = (
                f"SET [{opening_writeoff_col}] = CAST(ISNULL([{opening_writeoff_col}], 0) AS FLOAT) + ?"
            )
            if opening_updated_on_col:
                opening_set_sql += f", [{opening_updated_on_col}] = GETDATE()"
            cursor.execute(
                f"UPDATE dbo.CorpOpening "
                f"{opening_set_sql} "
                f"WHERE OPId = ?",
                (amount, bill_id_int),
            )
            try:
                conn.commit()
            except Exception:
                pass

            cursor.execute(
                f"SELECT CAST(ISNULL([{opening_writeoff_col}], 0) AS FLOAT) AS WriteOffTotal "
                f"FROM dbo.CorpOpening WITH (NOLOCK) WHERE OPId = ?",
                (bill_id_int,),
            )
            rec = cursor.fetchone()
            new_total = float(rec[0]) if rec and rec[0] is not None else 0.0
            return {
                "status": "success",
                "source_key": source_norm,
                "bill_id": bill_id_int,
                "writeoff_added": float(amount),
                "writeoff_total": new_total,
                "target_receipt_id": None,
                "writeoff_column": opening_writeoff_col,
            }

        if source_norm == "BILL_MST_POST":
            mst_cols = _table_cols_map("dbo.Corp_Receipt_Mst")
            dtl_cols = _table_cols_map("dbo.Corp_Receipt_Dtl")
            mst_writeoff_col = _pick_col(mst_cols, CORP_RECON_WRITEOFF_CANDIDATES) or schema_info.get("receipt_column")
            mst_updated_on_col = _pick_col(
                mst_cols,
                ["Updated_On", "UpdatedOn", "ModifiedOn", "LastUpdatedOn", "accUpdatedDate", "AccUpdatedDate"],
            )
            dtl_receipt_date_col = _pick_col(dtl_cols, ["ReceiptDate"])
            if not mst_writeoff_col:
                return {
                    "status": "error",
                    "message": "Write-off column not found in Corp_Receipt_Mst. Add a write-off amount column first.",
                }

            receipt_id_int = _to_pos_int(target_receipt_id)
            if receipt_id_int:
                cursor.execute(
                    "SELECT TOP 1 1 FROM dbo.Corp_Receipt_Dtl WITH (NOLOCK) WHERE billId = ? AND receiptId = ?",
                    (bill_id_int, receipt_id_int),
                )
                if not cursor.fetchone():
                    return {
                        "status": "error",
                        "message": f"Receipt {receipt_id_int} is not linked to bill {bill_id_int}",
                    }
            else:
                receipt_date_expr = f"d.[{dtl_receipt_date_col}]" if dtl_receipt_date_col else "m.Receipt_Date"
                cursor.execute(
                    f"""
                    SELECT TOP 1 CAST(d.receiptId AS INT) AS ReceiptId
                    FROM dbo.Corp_Receipt_Dtl d WITH (NOLOCK)
                    LEFT JOIN dbo.Corp_Receipt_Mst m WITH (NOLOCK)
                        ON d.receiptId = m.Receipt_ID
                    WHERE d.billId = ?
                    ORDER BY
                        CASE WHEN ISNULL(m.Cancelstatus, 0) = 1 THEN 1 ELSE 0 END ASC,
                        ISNULL({receipt_date_expr}, m.Receipt_Date) DESC,
                        d.receiptId DESC
                    """,
                    (bill_id_int,),
                )
                row = cursor.fetchone()
                if not row or row[0] in (None, ""):
                    return {
                        "status": "error",
                        "message": "No linked corporate receipt found for this bill. Write-off cannot be posted.",
                    }
                receipt_id_int = _to_pos_int(row[0])
                if not receipt_id_int:
                    return {
                        "status": "error",
                        "message": "Unable to resolve receipt target for write-off.",
                    }

            mst_set_sql = (
                f"SET [{mst_writeoff_col}] = CAST(ISNULL([{mst_writeoff_col}], 0) AS FLOAT) + ?"
            )
            if mst_updated_on_col:
                mst_set_sql += f", [{mst_updated_on_col}] = GETDATE()"
            cursor.execute(
                f"UPDATE dbo.Corp_Receipt_Mst "
                f"{mst_set_sql} "
                f"WHERE Receipt_ID = ?",
                (amount, receipt_id_int),
            )
            try:
                conn.commit()
            except Exception:
                pass

            cursor.execute(
                f"SELECT CAST(ISNULL([{mst_writeoff_col}], 0) AS FLOAT) AS WriteOffTotal "
                f"FROM dbo.Corp_Receipt_Mst WITH (NOLOCK) WHERE Receipt_ID = ?",
                (receipt_id_int,),
            )
            rec = cursor.fetchone()
            new_total = float(rec[0]) if rec and rec[0] is not None else 0.0

            return {
                "status": "success",
                "source_key": source_norm,
                "bill_id": bill_id_int,
                "writeoff_added": float(amount),
                "writeoff_total": new_total,
                "target_receipt_id": int(receipt_id_int),
                "writeoff_column": mst_writeoff_col,
            }

        return {
            "status": "error",
            "message": "Write-off is supported only for Opening and Corporate Bill (post-cutoff) sources.",
        }

    except Exception as e:
        return {"status": "error", "message": f"Failed to post write-off: {e}"}
    finally:
        try:
            conn.close()
        except Exception:
            pass


def apply_corporate_reconciliation_suspense_audit(
    unit: str,
    *,
    bill_ids,
    audited: bool = True,
    updated_by_id: int | None = None,
):
    """
    Mark/unmark suspense anomaly bills as audited in dbo.Corp_Bill_Mst.
    """
    conn = get_sql_connection(unit)
    if not conn:
        return {"status": "error", "message": f"Could not connect to {unit}"}

    try:
        normalized_bill_ids = sorted(
            {
                int(v)
                for v in (bill_ids or [])
                if str(v).strip().lstrip("-").isdigit() and int(v) > 0
            }
        )
        if not normalized_bill_ids:
            return {"status": "error", "message": "No valid bill ids supplied"}

        schema_info = _ensure_corporate_bill_audited_column_conn(conn)
        audited_col = (schema_info or {}).get("bill_audited_column")
        if not audited_col:
            return {"status": "error", "message": "Audited column not found in Corp_Bill_Mst."}

        def _table_cols_map(table_name: str) -> dict:
            try:
                preview_df = pd.read_sql(f"SELECT TOP 0 * FROM {table_name}", conn)
                return {str(c).strip().lower(): str(c).strip() for c in preview_df.columns}
            except Exception:
                return {}

        bill_cols = _table_cols_map("dbo.Corp_Bill_Mst")
        updated_on_col = None
        for cand in ["Updated_On", "UpdatedOn", "ModifiedOn", "accUpdatedDate", "AccUpdatedDate"]:
            key = str(cand or "").strip().lower()
            if key in bill_cols:
                updated_on_col = bill_cols[key]
                break
        updated_by_col = None
        for cand in ["Updated_By", "UpdatedBy", "updated_by"]:
            key = str(cand or "").strip().lower()
            if key in bill_cols:
                updated_by_col = bill_cols[key]
                break

        cursor = conn.cursor()
        id_placeholders = ",".join(["?"] * len(normalized_bill_ids))
        cursor.execute(
            f"SELECT CAST(CBill_ID AS INT) AS BillId FROM dbo.Corp_Bill_Mst WITH (NOLOCK) "
            f"WHERE CBill_ID IN ({id_placeholders})",
            tuple(normalized_bill_ids),
        )
        found_rows = cursor.fetchall() or []
        found_ids = sorted({int(r[0]) for r in found_rows if r and r[0] is not None})
        if not found_ids:
            return {
                "status": "success",
                "updated_count": 0,
                "requested_count": len(normalized_bill_ids),
                "matched_bill_ids": [],
                "missing_bill_ids": normalized_bill_ids,
                "audited_value": 1 if audited else 0,
                "audited_column": audited_col,
            }

        set_sql = f"SET [{audited_col}] = ?"
        update_params = [1 if audited else 0]
        if updated_on_col:
            set_sql += f", [{updated_on_col}] = GETDATE()"
        if updated_by_col and updated_by_id is not None and int(updated_by_id) > 0:
            set_sql += f", [{updated_by_col}] = ?"
            update_params.append(int(updated_by_id))

        update_placeholders = ",".join(["?"] * len(found_ids))
        update_params.extend(found_ids)
        cursor.execute(
            f"UPDATE dbo.Corp_Bill_Mst {set_sql} WHERE CBill_ID IN ({update_placeholders})",
            tuple(update_params),
        )
        try:
            conn.commit()
        except Exception:
            pass

        missing_ids = [bid for bid in normalized_bill_ids if bid not in set(found_ids)]
        return {
            "status": "success",
            "updated_count": int(len(found_ids)),
            "requested_count": int(len(normalized_bill_ids)),
            "matched_bill_ids": found_ids,
            "missing_bill_ids": missing_ids,
            "audited_value": 1 if audited else 0,
            "audited_column": audited_col,
        }
    except Exception as e:
        return {"status": "error", "message": f"Failed to update audited flag: {e}"}
    finally:
        try:
            conn.close()
        except Exception:
            pass


# ===================== Corporate Updates Feed =====================
def fetch_corporate_updates_raw(unit: str, lookback_hours: int = 24, limit: int = 1200):
    """
    Fetch recent additions/updates from key corporate tables.
    Returns:
      {
        "events": pd.DataFrame([...]),
        "meta": {"from_ts": "...", "as_of": "...", "lookback_hours": int, "limit": int}
      }
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Could not connect to {unit} for corporate updates")
        return None
    try:
        def _cols_map(table_name: str) -> dict:
            if not table_name:
                return {}
            try:
                cols_df = pd.read_sql(
                    "SELECT name FROM sys.columns WHERE object_id = OBJECT_ID(?)",
                    conn,
                    params=[table_name],
                )
            except Exception:
                return {}
            if cols_df is None or cols_df.empty:
                return {}
            return {str(c).strip().lower(): str(c).strip() for c in cols_df["name"].tolist() if str(c).strip()}

        def _pick_col(cols_map: dict, candidates: list[str]) -> str | None:
            for cand in candidates:
                key = str(cand or "").strip().lower()
                if key and key in cols_map:
                    return cols_map[key]
            return None

        def _first_existing_table(candidates: list[str]) -> str | None:
            for table_name in candidates:
                try:
                    pd.read_sql(f"SELECT TOP 0 * FROM {table_name}", conn)
                    return table_name
                except Exception:
                    continue
            return None

        def _q_ident(name: str) -> str:
            return f"[{str(name).replace(']', ']]')}]"

        def _safe_int_expr(raw_expr: str) -> str:
            expr = str(raw_expr or "").strip()
            if not expr:
                return "CAST(NULL AS INT)"
            txt = f"LTRIM(RTRIM(CONVERT(NVARCHAR(100), {expr})))"
            return (
                "CASE "
                f"WHEN {txt} = '' THEN NULL "
                f"WHEN {txt} LIKE '%[^0-9]%' THEN NULL "
                f"ELSE CAST({expr} AS INT) END"
            )

        lookback_hours = int(lookback_hours or 24)
        lookback_hours = max(1, min(24 * 90, lookback_hours))
        limit = int(limit or 1200)
        limit = max(100, min(8000, limit))
        per_table_limit = max(50, min(2500, int(limit)))

        now_ts = datetime.now(tz=LOCAL_TZ)
        from_ts = now_ts - timedelta(hours=lookback_hours)
        from_ts_sql = from_ts.strftime("%Y-%m-%d %H:%M:%S")

        user_table = _first_existing_table([
            "dbo.User_Mst",
            "dbo.user_mst",
            "dbo.UserMst",
        ])
        user_cols = _cols_map(user_table) if user_table else {}
        user_id_col = _pick_col(user_cols, ["UserId", "Userid", "User_ID", "Id", "AccountId"]) if user_cols else None
        user_name_col = _pick_col(user_cols, ["UserName", "User_Name", "Username", "Name", "EmpName"]) if user_cols else None

        table_configs = [
            {
                "table_key": "corp_receipt_mst",
                "table_label": "Corp_Receipt_Mst",
                "table_name": "dbo.Corp_Receipt_Mst",
                "pk_candidates": ["Receipt_ID", "receiptId", "ReceiptId", "Id"],
                "add_time_candidates": ["Receipt_Date", "ReceiptDate", "InsertedOn", "CreatedOn"],
                "upd_time_candidates": ["Updated_On", "UpdatedOn", "ModifiedOn"],
                "actor_candidates": ["Updated_By", "UpdatedBy", "InsertedBy"],
                "detail_candidates": ["CReceipt_No", "Amount", "NetAmt", "WriteOffAmt", "rebateDiscountAmt", "CPayment_Mode", "Cancelstatus", "UTRNo"],
            },
            {
                "table_key": "corp_receipt_dtl",
                "table_label": "Corp_Receipt_Dtl",
                "table_name": "dbo.Corp_Receipt_Dtl",
                "pk_candidates": ["recDtlId", "RecDtlId", "Id"],
                "add_time_candidates": ["insertedOn", "ReceiptDate", "CreatedOn"],
                "upd_time_candidates": ["UpdatedOn", "ModifiedOn"],
                "actor_candidates": ["insertedBy", "UpdatedBy", "InsertedBy"],
                "detail_candidates": ["receiptId", "billId", "receiptAmt", "billAmt", "dueAmt", "visitId", "PatientId"],
            },
            {
                "table_key": "corp_bill_mst",
                "table_label": "Corp_Bill_Mst",
                "table_name": "dbo.Corp_Bill_Mst",
                "pk_candidates": ["CBill_ID", "CBillId", "BillId", "Id"],
                "add_time_candidates": ["Submit_Date", "CBill_Date", "CreatedOn"],
                "upd_time_candidates": ["Updated_On", "accUpdatedDate", "ModifiedOn"],
                "actor_candidates": ["Updated_By", "UpdatedBy", "InsertedBy"],
                "detail_candidates": ["CBill_NO", "Bill_No", "CAmount", "Due_Amt", "Status", "Audited", "Visit_ID", "PatientID"],
            },
            {
                "table_key": "corpopening",
                "table_label": "CorpOpening",
                "table_name": "dbo.CorpOpening",
                "pk_candidates": ["OPId", "OpId", "Id"],
                "add_time_candidates": ["DueDate", "CreatedOn", "InsertedOn"],
                "upd_time_candidates": ["UpdatedOn", "ModifiedOn"],
                "actor_candidates": ["UpdatedBy", "InsertedBy"],
                "detail_candidates": ["RefNo", "PatientName", "PatientId", "DueAmount", "ReceiptAmt", "WriteOffAmt", "DueDate", "ReceiptId", "PatientTypeId", "PatientSubTypeId"],
            },
        ]

        event_frames = []
        for cfg in table_configs:
            table_name = cfg["table_name"]
            cols_map = _cols_map(table_name)
            if not cols_map:
                continue

            pk_col = _pick_col(cols_map, cfg["pk_candidates"])
            if not pk_col:
                continue
            add_time_col = _pick_col(cols_map, cfg["add_time_candidates"])
            upd_time_col = _pick_col(cols_map, cfg["upd_time_candidates"])
            actor_col = _pick_col(cols_map, cfg["actor_candidates"])

            detail_cols = []
            for cand in cfg["detail_candidates"]:
                found = _pick_col(cols_map, [cand])
                if found and found not in detail_cols:
                    detail_cols.append(found)
                if len(detail_cols) >= 6:
                    break

            detail_aliases = []
            detail_select = []
            for idx, col in enumerate(detail_cols):
                alias = f"Detail{idx + 1}"
                detail_aliases.append((alias, col))
                detail_select.append(f"CONVERT(NVARCHAR(300), {_q_ident(col)}) AS {alias}")
            while len(detail_aliases) < 6:
                alias = f"Detail{len(detail_aliases) + 1}"
                detail_aliases.append((alias, ""))
                detail_select.append(f"CAST(NULL AS NVARCHAR(300)) AS {alias}")

            actor_raw_expr = f"CONVERT(NVARCHAR(120), {_q_ident(actor_col)})" if actor_col else "CAST(NULL AS NVARCHAR(120))"
            actor_id_expr = _safe_int_expr(_q_ident(actor_col)) if actor_col else "CAST(NULL AS INT)"
            actor_join_sql = ""
            actor_name_expr = actor_raw_expr
            if actor_col and user_table and user_id_col and user_name_col:
                actor_join_sql = (
                    f"LEFT JOIN {user_table} u_evt WITH (NOLOCK) "
                    f"ON u_evt.[{user_id_col}] = {actor_id_expr}"
                )
                actor_name_expr = (
                    f"LTRIM(RTRIM(ISNULL(CONVERT(NVARCHAR(200), u_evt.[{user_name_col}]), {actor_raw_expr})))"
                )

            def _read_event_block(event_type: str, time_col: str | None):
                if not time_col:
                    return None
                raw_time_expr = f"CONVERT(NVARCHAR(50), {_q_ident(time_col)})"
                time_expr = (
                    "CASE "
                    f"WHEN ISDATE({raw_time_expr}) = 1 THEN CAST({_q_ident(time_col)} AS DATETIME) "
                    "ELSE NULL END"
                )
                where_sql = f"{time_expr} IS NOT NULL AND {time_expr} >= ?"
                if event_type == "update" and add_time_col and add_time_col.lower() != time_col.lower():
                    raw_add_expr = f"CONVERT(NVARCHAR(50), {_q_ident(add_time_col)})"
                    add_expr = (
                        "CASE "
                        f"WHEN ISDATE({raw_add_expr}) = 1 THEN CAST({_q_ident(add_time_col)} AS DATETIME) "
                        "ELSE NULL END"
                    )
                    where_sql += f" AND ({add_expr} IS NULL OR {time_expr} > {add_expr})"
                sql = f"""
                    SELECT TOP {int(per_table_limit)}
                        CAST('{cfg["table_key"]}' AS NVARCHAR(80)) AS TableKey,
                        CAST('{cfg["table_label"]}' AS NVARCHAR(120)) AS TableLabel,
                        CAST('{event_type}' AS NVARCHAR(20)) AS EventType,
                        {time_expr} AS EventTime,
                        CONVERT(NVARCHAR(120), {_q_ident(pk_col)}) AS RecordId,
                        {actor_id_expr} AS ChangedById,
                        {actor_raw_expr} AS ChangedByRaw,
                        {actor_name_expr} AS ChangedBy,
                        {", ".join(detail_select)}
                    FROM {table_name} WITH (NOLOCK)
                    {actor_join_sql}
                    WHERE {where_sql}
                    ORDER BY {time_expr} DESC, {_q_ident(pk_col)} DESC
                """
                try:
                    return pd.read_sql(sql, conn, params=[from_ts_sql])
                except Exception:
                    return None

            add_df = _read_event_block("addition", add_time_col)
            if add_df is not None and not add_df.empty:
                add_df["DetailPairs"] = add_df.apply(
                    lambda row: [
                        {"name": col_name, "value": row.get(alias)}
                        for alias, col_name in detail_aliases
                        if col_name and row.get(alias) not in (None, "", "NULL")
                    ],
                    axis=1,
                )
                event_frames.append(add_df)

            upd_df = _read_event_block("update", upd_time_col)
            if upd_df is not None and not upd_df.empty:
                upd_df["DetailPairs"] = upd_df.apply(
                    lambda row: [
                        {"name": col_name, "value": row.get(alias)}
                        for alias, col_name in detail_aliases
                        if col_name and row.get(alias) not in (None, "", "NULL")
                    ],
                    axis=1,
                )
                event_frames.append(upd_df)

        if event_frames:
            events_df = pd.concat(event_frames, ignore_index=True)
        else:
            events_df = pd.DataFrame(
                columns=[
                    "TableKey",
                    "TableLabel",
                    "EventType",
                    "EventTime",
                    "RecordId",
                    "ChangedBy",
                    "Detail1",
                    "Detail2",
                    "Detail3",
                    "Detail4",
                    "Detail5",
                    "Detail6",
                    "DetailPairs",
                ]
            )

        if not events_df.empty:
            events_df["EventTime"] = pd.to_datetime(events_df["EventTime"], errors="coerce")
            events_df = events_df[events_df["EventTime"].notna()].copy()
            events_df = events_df.sort_values(["EventTime", "RecordId"], ascending=[False, False], na_position="last")
            events_df = events_df.head(limit).copy()
            events_df["Unit"] = (unit or "").upper()

        return {
            "events": events_df,
            "meta": {
                "from_ts": from_ts.isoformat(),
                "as_of": now_ts.isoformat(),
                "lookback_hours": int(lookback_hours),
                "limit": int(limit),
            },
        }
    except Exception as e:
        print(f"Error fetching corporate updates raw ({unit}): {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def fetch_corporate_updates_context(
    unit: str,
    bill_ids: list[int] | None = None,
    receipt_ids: list[int] | None = None,
    visit_ids: list[int] | None = None,
    patient_ids: list[int] | None = None,
):
    """
    Fetch lightweight context for updates panel enrichment:
    - bill/opening -> patient + visit context
    - receipt -> linked bill/visit/patient ids
    - visit -> visit type/dates + patient/type/subtype labels
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Could not connect to {unit} for corporate updates context")
        return None
    try:
        def _norm_ids(values):
            out = []
            seen = set()
            for raw in values or []:
                try:
                    val = int(raw)
                except Exception:
                    continue
                if val <= 0 or val in seen:
                    continue
                seen.add(val)
                out.append(val)
            return out

        def _chunks(values, size=700):
            seq = list(values or [])
            for i in range(0, len(seq), size):
                yield seq[i : i + size]

        bill_ids = _norm_ids(bill_ids)
        receipt_ids = _norm_ids(receipt_ids)
        visit_ids = _norm_ids(visit_ids)
        patient_ids = _norm_ids(patient_ids)

        bill_map = {}
        receipt_map = {}
        visit_map = {}

        for chunk in _chunks(bill_ids):
            placeholders = ",".join("?" for _ in chunk)
            bill_sql = f"""
                SELECT
                    CAST(b.CBill_ID AS INT) AS BillId,
                    CAST(NULLIF(b.Visit_ID, 0) AS INT) AS VisitId,
                    CAST(NULLIF(b.PatientID, 0) AS INT) AS PatientId,
                    ISNULL(v.TypeOfVisit, '') AS TypeOfVisit,
                    v.VisitDate,
                    v.DischargeDate,
                    CASE
                        WHEN v.PatientID IS NOT NULL THEN ISNULL(dbo.fn_patientfullname(v.PatientID), '')
                        WHEN b.PatientID IS NOT NULL THEN ISNULL(dbo.fn_patientfullname(b.PatientID), '')
                        ELSE ''
                    END AS PatientName,
                    CASE
                        WHEN v.PatientType_ID IS NOT NULL THEN ISNULL(dbo.fn_pat_type(v.PatientType_ID), '')
                        WHEN b.PatientTypeId IS NOT NULL THEN ISNULL(dbo.fn_pat_type(b.PatientTypeId), '')
                        ELSE ''
                    END AS PatientType,
                    CASE
                        WHEN v.PatientSubType_ID IS NOT NULL THEN ISNULL(dbo.fn_patsub_type(v.PatientSubType_ID), '')
                        WHEN b.PatientTypeIdSrNo IS NOT NULL THEN ISNULL(dbo.fn_patsub_type(b.PatientTypeIdSrNo), '')
                        ELSE ''
                    END AS PatientSubType
                FROM dbo.Corp_Bill_Mst b WITH (NOLOCK)
                LEFT JOIN dbo.Visit v WITH (NOLOCK)
                    ON v.Visit_ID = b.Visit_ID
                WHERE b.CBill_ID IN ({placeholders})
            """
            bill_df = pd.read_sql(bill_sql, conn, params=chunk)
            if bill_df is None or bill_df.empty:
                bill_df = pd.DataFrame()
            if not bill_df.empty:
                bill_df["VisitDate"] = pd.to_datetime(bill_df.get("VisitDate"), errors="coerce")
                bill_df["DischargeDate"] = pd.to_datetime(bill_df.get("DischargeDate"), errors="coerce")
                for _, row in bill_df.iterrows():
                    try:
                        bill_id = int(row.get("BillId"))
                    except Exception:
                        continue
                    bill_map[bill_id] = {
                        "bill_id": bill_id,
                        "visit_id": int(row["VisitId"]) if pd.notna(row.get("VisitId")) else None,
                        "patient_id": int(row["PatientId"]) if pd.notna(row.get("PatientId")) else None,
                        "patient_name": str(row.get("PatientName") or "").strip(),
                        "patient_type": str(row.get("PatientType") or "").strip(),
                        "patient_subtype": str(row.get("PatientSubType") or "").strip(),
                        "type_of_visit": str(row.get("TypeOfVisit") or "").strip(),
                        "visit_date": row["VisitDate"].strftime("%Y-%m-%d") if pd.notna(row.get("VisitDate")) else "",
                        "discharge_date": row["DischargeDate"].strftime("%Y-%m-%d") if pd.notna(row.get("DischargeDate")) else "",
                        "source": "BILL",
                    }

            opening_sql = f"""
                SELECT
                    CAST(o.OPId AS INT) AS BillId,
                    CAST(NULLIF(o.PatientId, 0) AS INT) AS PatientId,
                    ISNULL(CONVERT(NVARCHAR(255), o.PatientName), '') AS PatientName,
                    CASE
                        WHEN o.PatientTypeId IS NULL THEN ''
                        ELSE ISNULL(dbo.fn_pat_type(o.PatientTypeId), '')
                    END AS PatientType,
                    CASE
                        WHEN o.PatientSubTypeId IS NULL THEN ''
                        ELSE ISNULL(dbo.fn_patsub_type(o.PatientSubTypeId), '')
                    END AS PatientSubType
                FROM dbo.CorpOpening o WITH (NOLOCK)
                WHERE o.OPId IN ({placeholders})
            """
            opening_df = pd.read_sql(opening_sql, conn, params=chunk)
            if opening_df is None or opening_df.empty:
                continue
            for _, row in opening_df.iterrows():
                try:
                    bill_id = int(row.get("BillId"))
                except Exception:
                    continue
                if bill_id in bill_map:
                    continue
                bill_map[bill_id] = {
                    "bill_id": bill_id,
                    "visit_id": None,
                    "patient_id": int(row["PatientId"]) if pd.notna(row.get("PatientId")) else None,
                    "patient_name": str(row.get("PatientName") or "").strip(),
                    "patient_type": str(row.get("PatientType") or "").strip(),
                    "patient_subtype": str(row.get("PatientSubType") or "").strip(),
                    "type_of_visit": "",
                    "visit_date": "",
                    "discharge_date": "",
                    "source": "OPENING",
                }

        receipt_visit_ids = set()
        for chunk in _chunks(receipt_ids):
            placeholders = ",".join("?" for _ in chunk)
            receipt_sql = f"""
                SELECT
                    CAST(d.receiptId AS INT) AS ReceiptId,
                    MAX(CASE WHEN ISNULL(d.billId, 0) > 0 THEN CAST(d.billId AS INT) ELSE NULL END) AS BillId,
                    MAX(CASE WHEN ISNULL(d.visitId, 0) > 0 THEN CAST(d.visitId AS INT) ELSE NULL END) AS DtlVisitId,
                    MAX(CASE WHEN ISNULL(d.PatientId, 0) > 0 THEN CAST(d.PatientId AS INT) ELSE NULL END) AS DtlPatientId,
                    MAX(CASE WHEN ISNULL(m.VisitID, 0) > 0 THEN CAST(m.VisitID AS INT) ELSE NULL END) AS MstVisitId,
                    MAX(CASE WHEN ISNULL(m.PatientID, 0) > 0 THEN CAST(m.PatientID AS INT) ELSE NULL END) AS MstPatientId
                FROM dbo.Corp_Receipt_Dtl d WITH (NOLOCK)
                LEFT JOIN dbo.Corp_Receipt_Mst m WITH (NOLOCK)
                    ON d.receiptId = m.Receipt_ID
                WHERE d.receiptId IN ({placeholders})
                GROUP BY d.receiptId
            """
            rec_df = pd.read_sql(receipt_sql, conn, params=chunk)
            if rec_df is None or rec_df.empty:
                continue
            for _, row in rec_df.iterrows():
                try:
                    receipt_id = int(row.get("ReceiptId"))
                except Exception:
                    continue
                bill_id = int(row["BillId"]) if pd.notna(row.get("BillId")) else None
                visit_id = None
                for c in ("DtlVisitId", "MstVisitId"):
                    if pd.notna(row.get(c)):
                        visit_id = int(row.get(c))
                        break
                patient_id = None
                for c in ("DtlPatientId", "MstPatientId"):
                    if pd.notna(row.get(c)):
                        patient_id = int(row.get(c))
                        break
                receipt_map[receipt_id] = {
                    "receipt_id": receipt_id,
                    "bill_id": bill_id,
                    "visit_id": visit_id,
                    "patient_id": patient_id,
                }
                if visit_id:
                    receipt_visit_ids.add(visit_id)

        visit_need = set(visit_ids) | set(receipt_visit_ids)
        for entry in bill_map.values():
            vid = entry.get("visit_id")
            if vid:
                visit_need.add(int(vid))

        for chunk in _chunks(_norm_ids(list(visit_need))):
            placeholders = ",".join("?" for _ in chunk)
            visit_sql = f"""
                SELECT
                    CAST(v.Visit_ID AS INT) AS VisitId,
                    CAST(NULLIF(v.PatientID, 0) AS INT) AS PatientId,
                    ISNULL(v.TypeOfVisit, '') AS TypeOfVisit,
                    v.VisitDate,
                    v.DischargeDate,
                    CASE
                        WHEN v.PatientID IS NULL THEN ''
                        ELSE ISNULL(dbo.fn_patientfullname(v.PatientID), '')
                    END AS PatientName,
                    CASE
                        WHEN v.PatientType_ID IS NULL THEN ''
                        ELSE ISNULL(dbo.fn_pat_type(v.PatientType_ID), '')
                    END AS PatientType,
                    CASE
                        WHEN v.PatientSubType_ID IS NULL THEN ''
                        ELSE ISNULL(dbo.fn_patsub_type(v.PatientSubType_ID), '')
                    END AS PatientSubType
                FROM dbo.Visit v WITH (NOLOCK)
                WHERE v.Visit_ID IN ({placeholders})
            """
            visit_df = pd.read_sql(visit_sql, conn, params=chunk)
            if visit_df is None or visit_df.empty:
                continue
            visit_df["VisitDate"] = pd.to_datetime(visit_df.get("VisitDate"), errors="coerce")
            visit_df["DischargeDate"] = pd.to_datetime(visit_df.get("DischargeDate"), errors="coerce")
            for _, row in visit_df.iterrows():
                try:
                    visit_id = int(row.get("VisitId"))
                except Exception:
                    continue
                visit_map[visit_id] = {
                    "visit_id": visit_id,
                    "patient_id": int(row["PatientId"]) if pd.notna(row.get("PatientId")) else None,
                    "patient_name": str(row.get("PatientName") or "").strip(),
                    "patient_type": str(row.get("PatientType") or "").strip(),
                    "patient_subtype": str(row.get("PatientSubType") or "").strip(),
                    "type_of_visit": str(row.get("TypeOfVisit") or "").strip(),
                    "visit_date": row["VisitDate"].strftime("%Y-%m-%d") if pd.notna(row.get("VisitDate")) else "",
                    "discharge_date": row["DischargeDate"].strftime("%Y-%m-%d") if pd.notna(row.get("DischargeDate")) else "",
                }

        return {
            "bill": bill_map,
            "receipt": receipt_map,
            "visit": visit_map,
            "meta": {
                "bill_count": int(len(bill_map)),
                "receipt_count": int(len(receipt_map)),
                "visit_count": int(len(visit_map)),
                "patient_hint_count": int(len(patient_ids)),
            },
        }
    except Exception as e:
        print(f"Error fetching corporate updates context ({unit}): {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


# ===================== Refund Tracker (Billing MIS) =====================
def fetch_refund_tracker_raw(unit: str, from_date: str, to_date: str):
    """
    Fetch refund tracker rows from refund master + linked receipt details.
    Date filter is driven by refundmst.refundInitializeDate.
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Could not connect to {unit} for refund tracker")
        return None
    try:
        sql = """
            SET NOCOUNT ON;
            SELECT
                rm.id AS RefundId,
                rm.visitId AS VisitId,
                rm.patientid AS MasterPatientId,
                rm.refundDeductionAmt AS RefundDeductionAmt,
                rm.refundremark AS RefundRemark,
                rm.refundStatus AS RefundStatus,
                rm.refundInitializeDate AS RefundInitializeDate,
                rm.refundInitializeBy AS RefundInitializeBy,
                rm.accountDeductionAmt AS AccountDeductionAmt,
                rm.accountInitializeDate AS AccountInitializeDate,
                rm.accountInitializeBy AS AccountInitializeBy,
                rm.accountRemark AS AccountRemark,
                rm.patientAccountNo AS PatientAccountNo,
                rm.patientBankName AS PatientBankName,
                rm.patientbankIFSC AS PatientBankIFSC,
                rm.patientaccHolderName AS PatientAccHolderName,
                rm.patientRelationName AS PatientRelationName,
                rm.adminStatus AS AdminStatus,
                rm.adminlockDate AS AdminLockDate,
                rm.corpStatus AS CorpStatus,
                rm.corplockDate AS CorpLockDate,
                rm.accountStatus AS AccountStatus,
                rm.accountlockDate AS AccountLockDate,
                rm.corpDeductionAmt AS CorpDeductionAmt,
                rm.corpRefundAmt AS CorpRefundAmt,
                rm.totalReceiveAmt AS TotalReceiveAmt,
                rm.refundAmt AS RefundAmt,
                rm.corpRemarks AS CorpRemarks,
                rm.utrno AS UTRNo,
                v.VisitNo,
                v.VisitDate,
                v.DischargeDate,
                COALESCE(v.PatientID, rm.patientid) AS PatientID,
                CASE
                    WHEN COALESCE(v.PatientID, rm.patientid) IS NULL THEN ''
                    ELSE ISNULL(dbo.fn_regno(COALESCE(v.PatientID, rm.patientid)), '')
                END AS RegNo,
                CASE
                    WHEN COALESCE(v.PatientID, rm.patientid) IS NULL THEN ''
                    ELSE ISNULL(dbo.fn_patientfullname(COALESCE(v.PatientID, rm.patientid)), '')
                END AS PatientName,
                CASE
                    WHEN v.PatientType_ID IS NULL THEN ''
                    ELSE ISNULL(dbo.fn_pat_type(v.PatientType_ID), '')
                END AS PatientType,
                CASE
                    WHEN v.PatientSubType_ID IS NULL THEN ''
                    ELSE ISNULL(dbo.fn_patsub_type(v.PatientSubType_ID), '')
                END AS CorpType,
                rrd.recId AS RecId,
                rrd.refundId AS DetailRefundId,
                rrd.refRecAmount AS RefRecAmount,
                rrd.recDate AS RecDate,
                rrd.oldrecid AS OldReceiptId,
                COALESCE(rc.Receipt_Date, rrd.oldrecDate) AS OldRecDate,
                ISNULL(um.UserName, '') AS LinkedUserName,
                rrd.recDate AS InsertedDate,
                rc.Receipt_ID AS ReceiptId,
                rc.Receipt_No AS ReceiptNo,
                rc.Amount AS ReceiptAmount,
                CASE
                    WHEN pm.PModeName IS NULL OR LTRIM(RTRIM(CONVERT(NVARCHAR(100), pm.PModeName))) = ''
                        THEN ISNULL(CONVERT(NVARCHAR(100), rc.PaymentMode), '')
                    ELSE CONVERT(NVARCHAR(100), pm.PModeName)
                END AS PaymentMode,
                rc.ReceiptType AS ReceiptType,
                rc.CancelStatus AS CancelStatus
            FROM dbo.refundmst rm WITH (NOLOCK)
            LEFT JOIN dbo.visit v WITH (NOLOCK)
                ON v.Visit_ID = rm.visitId
            LEFT JOIN dbo.refundreceivedtl rrd WITH (NOLOCK)
                ON rrd.refundId = rm.id
            LEFT JOIN dbo.receipt_mst rc WITH (NOLOCK)
                ON rc.Receipt_ID = rrd.oldrecid
            LEFT JOIN dbo.paymentmode_mst pm WITH (NOLOCK)
                ON pm.PModeId = rc.PaymentMode
               AND ISNULL(pm.Deactive, 0) = 0
            LEFT JOIN dbo.user_mst um WITH (NOLOCK)
                ON um.UserID = rrd.userid
            WHERE rm.refundInitializeDate >= ?
              AND rm.refundInitializeDate < DATEADD(DAY, 1, CAST(? AS DATETIME))
            ORDER BY
                rm.refundInitializeDate DESC,
                rm.id DESC,
                rrd.recId DESC
        """
        df = pd.read_sql(sql, conn, params=[from_date, to_date])
        if df is None or df.empty:
            return df
        df.columns = [c.strip() for c in df.columns]
        df["Unit"] = (unit or "").upper()
        return df
    except Exception as e:
        print(f"Error fetching refund tracker ({unit}): {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


# ===================== Volume Tracker (NEW) =====================

# Final column order expected by the UI/Excel
VOLUME_TRACKER_COL_ORDER = [
    "VisitNo","VisitDate","DischargeDate","PatientName","RegNo",
    "TypeOfVisit","Consultant","ReferringConsultant","ExternalRefConsultant",
    "Address","City","PatientType","CorpType","PatientContact","Age","Dept"
]
VOLUME_HCV_DOCTOR_COL_ORDER = [
    "HCVId",
    "VisitId",
    "PatientId",
    "VisitNo",
    "VisitDate",
    "RegNo",
    "PatientName",
    "Dept",
    "Consultant",
    "CorpType",
    "PackageId",
    "Unit",
]

def _as_ist_string(ts):
    if pd.isna(ts):
        return None
    try:
        # If naive, localize to IST; if tz-aware, convert to IST
        t = pd.Timestamp(ts)
        if t.tzinfo is None:
            t = t.tz_localize(LOCAL_TZ)
        else:
            t = t.tz_convert(LOCAL_TZ)
        return t.strftime("%Y-%m-%d %H:%M")
    except Exception:
        try:
            return pd.to_datetime(ts, errors="coerce").strftime("%Y-%m-%d %H:%M")
        except Exception:
            return None

# ... (everything above unchanged)

def fetch_volume_raw(units: list[str], from_date: str, to_date: str) -> pd.DataFrame:
    """
    Calls your stored procedure on EACH allowed unit DB and unions results:
        EXEC dbo.usp_RptVisitMarketManagerMIS @fromdate=?, @todate=?
    """
    if not units:
        return pd.DataFrame()

    frames = []
    for unit in units:
        conn = None
        try:
            conn = get_sql_connection(unit)  # unit is required in your project
            if not conn:
                print(f"âš ï¸ Volume: could not connect to {unit}")
                continue

            sql = "EXEC dbo.usp_RptVisitMarketManagerMIS ?, ?"
            df = pd.read_sql(sql, conn, params=[from_date, to_date])

            if df is not None and not df.empty:
                df.columns = [c.strip() for c in df.columns]
                df["Unit"] = unit.upper()
                frames.append(df)
        except Exception as e:
            print(f"âŒ Error fetching volume ({unit}): {e}")
        finally:
            try:
                if conn:
                    conn.close()
            except Exception:
                pass

    # Exclude empty/all-NA frames to avoid the pandas FutureWarning
    valid_frames = []
    for f in frames:
        if f is None:
            continue
        if f.dropna(axis=0, how='all').empty:
            continue
        if f.dropna(axis=1, how='all').shape[1] == 0:
            continue
        valid_frames.append(f)

    if not valid_frames:
        return pd.DataFrame()

    return pd.concat(valid_frames, ignore_index=True, copy=False)


def shape_volume_details(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Shapes the raw proc result to the exact columns requested.
    Uses your daycare/IPD flag when VisitTypeID=1, else shows TypeOfVisit.
    """
    if df_raw is None or df_raw.empty:
        return pd.DataFrame(columns=VOLUME_TRACKER_COL_ORDER)

    def get(col):
        return df_raw[col] if col in df_raw.columns else pd.Series([None]*len(df_raw))

    gender_col = _find_column(
        df_raw,
        ["Gender", "Sex", "PatientGender", "GenderName", "SexName", "Sex_Name", "Gender_Name", "Patient_Sex"]
    )
    gender_series = get(gender_col) if gender_col else pd.Series([None] * len(df_raw))
    age_col = _find_column(
        df_raw,
        ["AgeYears", "Age_Years", "AgeYrs", "AgeInYears", "AgeInYrs", "Age (Years)", "PatientAge", "Age", "age"]
    )
    age_series = get(age_col) if age_col else pd.Series([None] * len(df_raw))
    visit_type_id_col = _find_column(
        df_raw,
        ["VisitTypeID", "VisitTypeId", "VisitType_ID", "Visit_Type_ID"]
    )
    visit_type_id_series = get(visit_type_id_col) if visit_type_id_col else pd.Series([None] * len(df_raw))

    # Build TypeOfVisit display per your rule
    def _visit_display(row):
        try:
            if int(row.get("VisitTypeID") or 0) == 1:
                # Use the derived column from your proc
                flag = row.get("IPD_Daycare_Flag")
                return "Daycare" if str(flag).strip().lower() == "daycare" else "IPD"
            else:
                return row.get("TypeOfVisit")
        except Exception:
            return row.get("TypeOfVisit")

    # Compose details
    details = pd.DataFrame({
        "VisitNo":             get("Visitno"),
        "VisitDate":           pd.to_datetime(get("VisitDate"), errors="coerce"),
        "DischargeDate":       pd.to_datetime(get("DischargeDate"), errors="coerce"),
        "PatientName":         get("PatientName"),
        "RegNo":               get("Registration_No"),
        "TypeOfVisit":         df_raw.apply(_visit_display, axis=1),
        "Consultant":          get("DoctorIncharge"),
        "ReferringConsultant": get("ReferringDoctor"),
        "ExternalRefConsultant": get("OutsideRefDoctor"),
        "Address":             get("address"),
        "City":                get("City_Name"),
        "PatientType":         get("PatientType"),
        "CorpType":            get("PatSubType"),  # from patsubtype
        "PatientContact":      get("Mobile"),
        "Age":                 age_series,
        "Dept":                get("Dept_Name"),
    })

    # Order columns
    details = details[VOLUME_TRACKER_COL_ORDER].copy()
    details["Gender"] = gender_series
    details["VisitTypeID"] = visit_type_id_series

    # Format datetimes as IST display strings
    details["VisitDate"]     = details["VisitDate"].apply(_as_ist_string)
    details["DischargeDate"] = details["DischargeDate"].apply(_as_ist_string)

    # Clean string fields
    for col in ["PatientName","Consultant","ReferringConsultant",
                "ExternalRefConsultant","Address","City","PatientType",
                "CorpType","Dept","PatientContact","RegNo","TypeOfVisit","Gender"]:
        if col in details.columns:
            details[col] = details[col].astype(str).str.strip().replace({"nan":"", "None":""})

    return details

def get_volume_details(units: list[str], from_date: str, to_date: str) -> pd.DataFrame:
    raw = fetch_volume_raw(units, from_date, to_date)
    return shape_volume_details(raw)


def fetch_volume_hcv_doctor_visits(unit: str, from_date: str, to_date: str) -> pd.DataFrame:
    """
    Fetch Health Checkup doctor visit rows from Visit_HCV joined to Visit.
    Date filtering is strictly based on master Visit.VisitDate.
    """
    empty = pd.DataFrame(columns=VOLUME_HCV_DOCTOR_COL_ORDER)
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Volume HCV doctor KPI: could not connect to {unit}")
        return empty

    try:
        hcv_table = _resolve_table_name(conn, ["Visit_HCV", "visit_hcv"])
        visit_table = _resolve_table_name(conn, ["Visit", "visit"])
        if not hcv_table or not visit_table:
            return empty

        hcv_id_col = _resolve_column(conn, hcv_table, ["HCVId", "HCVID", "HcvId", "Id"])
        hcv_visit_id_col = _resolve_column(conn, hcv_table, ["VisitId", "VisitID", "Visit_ID"])
        hcv_patient_id_col = _resolve_column(conn, hcv_table, ["PatientId", "PatientID", "Patient_ID"])
        hcv_visit_no_col = _resolve_column(conn, hcv_table, ["VisitNo", "Visit_No"])
        hcv_dept_col = _resolve_column(conn, hcv_table, ["DepartmentId", "DepartmentID", "Department_ID"])
        hcv_doctor_col = _resolve_column(conn, hcv_table, ["DoctorId", "DoctorID", "Doctor_ID"])
        hcv_package_col = _resolve_column(conn, hcv_table, ["PackageId", "PackageID", "Package_ID"])

        visit_id_col = _resolve_column(conn, visit_table, ["Visit_ID", "VisitId", "VisitID"])
        visit_date_col = _resolve_column(conn, visit_table, ["VisitDate", "Visit_Date"])
        visit_no_col = _resolve_column(conn, visit_table, ["VisitNo", "Visit_No"])
        visit_patient_col = _resolve_column(conn, visit_table, ["PatientID", "PatientId", "Patient_ID"])
        visit_subtype_col = _resolve_column(
            conn,
            visit_table,
            ["PatientSubType_ID", "PatientSubTypeId", "PatientSubTypeID"],
        )

        if not hcv_visit_id_col or not visit_id_col or not visit_date_col:
            return empty

        hcv_patient_expr = f"h.[{hcv_patient_id_col}]" if hcv_patient_id_col else "NULL"
        visit_patient_expr = f"v.[{visit_patient_col}]" if visit_patient_col else "NULL"
        patient_expr = f"COALESCE({hcv_patient_expr}, {visit_patient_expr})"
        hcv_visit_no_expr = f"h.[{hcv_visit_no_col}]" if hcv_visit_no_col else "NULL"
        visit_no_expr = f"v.[{visit_no_col}]" if visit_no_col else "NULL"
        merged_visit_no_expr = f"COALESCE({hcv_visit_no_expr}, {visit_no_expr})"

        hcv_id_expr = f"h.[{hcv_id_col}]" if hcv_id_col else "NULL"
        dept_expr = (
            f"CASE WHEN h.[{hcv_dept_col}] IS NULL THEN N'' ELSE dbo.Fn_subDept(h.[{hcv_dept_col}]) END"
            if hcv_dept_col
            else "N''"
        )
        doctor_expr = (
            f"CASE WHEN h.[{hcv_doctor_col}] IS NULL THEN N'' ELSE dbo.fn_doctorfirstname(h.[{hcv_doctor_col}]) END"
            if hcv_doctor_col
            else "N''"
        )
        corp_expr = (
            f"CASE WHEN v.[{visit_subtype_col}] IS NULL THEN N'' ELSE dbo.fn_patsub_type(v.[{visit_subtype_col}]) END"
            if visit_subtype_col
            else "N''"
        )
        package_expr = f"h.[{hcv_package_col}]" if hcv_package_col else "NULL"

        sql = f"""
            SELECT
                {hcv_id_expr} AS HCVId,
                h.[{hcv_visit_id_col}] AS VisitId,
                {patient_expr} AS PatientId,
                {merged_visit_no_expr} AS VisitNo,
                v.[{visit_date_col}] AS VisitDate,
                CASE WHEN {patient_expr} IS NULL THEN N'' ELSE dbo.fn_regno({patient_expr}) END AS RegNo,
                CASE WHEN {patient_expr} IS NULL THEN N'' ELSE dbo.fn_patientfullname({patient_expr}) END AS PatientName,
                {dept_expr} AS Dept,
                {doctor_expr} AS Consultant,
                {corp_expr} AS CorpType,
                {package_expr} AS PackageId
            FROM dbo.[{hcv_table}] h WITH (NOLOCK)
            INNER JOIN dbo.[{visit_table}] v WITH (NOLOCK)
                ON v.[{visit_id_col}] = h.[{hcv_visit_id_col}]
            WHERE CONVERT(VARCHAR(10), v.[{visit_date_col}], 120)
                    BETWEEN CONVERT(VARCHAR(10), ?, 120)
                        AND CONVERT(VARCHAR(10), ?, 120)
            ORDER BY v.[{visit_date_col}] DESC, h.[{hcv_visit_id_col}] DESC
        """
        df = pd.read_sql(sql, conn, params=[from_date, to_date])
        if df is None or df.empty:
            return empty

        df.columns = [str(c).strip() for c in df.columns]
        df["VisitDate"] = pd.to_datetime(df.get("VisitDate"), errors="coerce").apply(_as_ist_string)
        df["Unit"] = (unit or "").strip().upper()

        for col in ["RegNo", "PatientName", "Dept", "Consultant", "CorpType", "VisitNo", "VisitDate"]:
            if col in df.columns:
                df[col] = (
                    df[col]
                    .astype(str)
                    .str.strip()
                    .replace({"nan": "", "None": "", "NaT": ""})
                )

        for col in ["HCVId", "VisitId", "PatientId", "PackageId"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        for col in VOLUME_HCV_DOCTOR_COL_ORDER:
            if col not in df.columns:
                df[col] = None
        return df[VOLUME_HCV_DOCTOR_COL_ORDER].copy()
    except Exception as e:
        print(f"Volume HCV doctor KPI fetch failed ({unit}): {e}")
        return empty
    finally:
        try:
            conn.close()
        except Exception:
            pass

def _empty_volume_discharge_kpi_payload(
    from_date: str | None = None,
    to_date: str | None = None,
    cured_ids: list[int] | None = None,
    lama_ids: list[int] | None = None,
    referred_ids: list[int] | None = None,
    death_ids: list[int] | None = None,
) -> dict:
    return {
        "from_date": from_date or "",
        "to_date": to_date or "",
        "cured_type_ids": list(cured_ids or [1, 2]),
        "lama_type_ids": list(lama_ids or [4, 5, 7, 8]),
        "referred_type_ids": list(referred_ids or [3]),
        "death_type_ids": list(death_ids or [6]),
        "overall": {
            "total_discharges": 0,
            "cured_cases": 0,
            "cured_percent": 0.0,
            "lama_cases": 0,
            "lama_percent": 0.0,
            "referred_cases": 0,
            "referred_percent": 0.0,
            "death_cases": 0,
            "mortality_ratio": 0.0,
            "type_breakdown": [],
            "ward_breakdown": [],
        },
        "units": [],
    }


def fetch_volume_discharge_rows(unit: str, from_date: str, to_date: str) -> pd.DataFrame:
    """
    Fetch latest (deduped) IPD discharge rows in date range.
    Uses VisitTypeID=1 and keeps only latest discharge per Visit_ID
    to avoid duplicate/cancel-recreate discharge records.
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Volume discharge KPI: could not connect to {unit}")
        return pd.DataFrame()

    try:
        visit_table = _resolve_table_name(conn, ["Visit", "visit"])
        discharge_table = _resolve_table_name(conn, ["Discharge", "discharge"])
        dischargetype_table = _resolve_table_name(conn, ["DischargeType_Mst", "dischargetype_mst"])

        if not visit_table or not discharge_table:
            print(f"Volume discharge KPI: missing Visit/Discharge table for {unit}")
            return pd.DataFrame()

        visit_id_col = _resolve_column(conn, visit_table, ["Visit_ID", "VisitId", "VisitID"])
        visit_type_col = _resolve_column(conn, visit_table, ["VisitTypeID", "VisitTypeId", "VisitType_ID"])
        ward_col = _resolve_column(conn, visit_table, ["WardID", "WardId", "Ward_ID"])
        patient_id_col = _resolve_column(conn, visit_table, ["PatientID", "PatientId", "Patient_ID"])
        doctor_col = _resolve_column(conn, visit_table, ["DocInCharge", "DoctorInCharge", "Doc_In_Charge"])

        dis_visit_id_col = _resolve_column(conn, discharge_table, ["Visit_ID", "VisitId", "VisitID"])
        dis_id_col = _resolve_column(conn, discharge_table, ["Discharge_ID", "DischargeId", "DischargeID"])
        dis_type_id_col = _resolve_column(conn, discharge_table, ["DischargeType_ID", "DischargeTypeId", "DischargeTypeID"])
        dis_type_text_col = _resolve_column(conn, discharge_table, ["DischargeType"])
        dis_cancel_col = _resolve_column(conn, discharge_table, ["CancelStatus", "Cancelled", "Canceled"])
        dis_date_col = _resolve_column(conn, discharge_table, ["Discharge_Date", "DischargeDate"])
        dis_admin_date_col = _resolve_column(conn, discharge_table, ["adminDischargeDate", "AdminDischargeDate"])
        dis_cd_date_col = _resolve_column(conn, discharge_table, ["CDdate", "CDDate"])
        dis_updated_col = _resolve_column(conn, discharge_table, ["UpdatedOn", "Updated_ON"])
        dis_inserted_col = _resolve_column(conn, discharge_table, ["InsertedON", "InsertedOn", "Inserted_ON"])

        dt_id_col = None
        dt_name_col = None
        if dischargetype_table:
            dt_id_col = _resolve_column(conn, dischargetype_table, ["DischargeType_ID", "DischargeTypeId", "DischargeTypeID"])
            dt_name_col = _resolve_column(conn, dischargetype_table, ["DischargeType"])

        required = [visit_id_col, visit_type_col, dis_visit_id_col, dis_id_col]
        if any(not c for c in required):
            print(f"Volume discharge KPI: required columns missing for {unit}")
            return pd.DataFrame()

        discharge_date_cols = [
            c for c in [dis_admin_date_col, dis_date_col, dis_cd_date_col, dis_updated_col, dis_inserted_col]
            if c
        ]
        if not discharge_date_cols:
            print(f"Volume discharge KPI: no discharge date column found for {unit}")
            return pd.DataFrame()

        # SQL Server old-version safe datetime parsing (no TRY_CONVERT dependency).
        date_type_map = {}
        try:
            placeholders = ",".join("?" for _ in discharge_date_cols)
            type_sql = f"""
                SELECT c.name AS ColName, t.name AS TypeName
                FROM sys.columns c
                INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
                WHERE c.object_id = OBJECT_ID(?)
                  AND c.name IN ({placeholders})
            """
            type_df = pd.read_sql(type_sql, conn, params=[discharge_table, *discharge_date_cols])
            if type_df is not None and not type_df.empty:
                for _, trow in type_df.iterrows():
                    cname = str(trow.get("ColName") or "").strip()
                    tname = str(trow.get("TypeName") or "").strip().lower()
                    if cname:
                        date_type_map[cname.lower()] = tname
        except Exception:
            date_type_map = {}

        native_date_types = {
            "date", "datetime", "datetime2", "smalldatetime", "datetimeoffset", "time"
        }
        safe_date_parts = []
        for col in discharge_date_cols:
            part = f"d.[{col}]"
            col_type = date_type_map.get(str(col).lower(), "")
            if col_type in native_date_types:
                safe_date_parts.append(f"CAST({part} AS DATETIME)")
            else:
                safe_date_parts.append(
                    f"(CASE WHEN {part} IS NULL THEN NULL "
                    f"WHEN ISDATE({part}) = 1 THEN CAST({part} AS DATETIME) "
                    f"ELSE NULL END)"
                )
        discharge_date_expr = f"COALESCE({', '.join(safe_date_parts)})"
        if dis_cancel_col:
            cancel_expr = (
                f"(CASE WHEN ISNUMERIC(CAST(d.[{dis_cancel_col}] AS NVARCHAR(30))) = 1 "
                f"THEN CAST(d.[{dis_cancel_col}] AS INT) ELSE 0 END)"
            )
        else:
            cancel_expr = "0"
        dis_type_text_expr = (
            f"CAST(d.[{dis_type_text_col}] AS NVARCHAR(200))" if dis_type_text_col else "NULL"
        )
        dt_name_expr = (
            f"CAST(dt.[{dt_name_col}] AS NVARCHAR(200))" if dt_name_col else "NULL"
        )

        dis_type_id_select = (
            f"CAST(d.[{dis_type_id_col}] AS NVARCHAR(100))" if dis_type_id_col else "NULL"
        )
        dis_type_id_field = "ld.DischargeType_ID"
        dis_type_id_key_expr = f"LTRIM(RTRIM(CAST({dis_type_id_select} AS NVARCHAR(100))))"
        order_col = dis_id_col or dis_visit_id_col

        join_discharge_type = ""
        if dischargetype_table and dt_id_col and dis_type_id_col:
            join_discharge_type = (
                f"LEFT JOIN [dbo].[{dischargetype_table}] dt WITH (NOLOCK) "
                f"ON LTRIM(RTRIM(CAST(dt.[{dt_id_col}] AS NVARCHAR(100)))) = ld.DischargeType_ID_Key"
            )

        visit_type_num_expr = (
            f"(CASE WHEN ISNUMERIC(CAST(v.[{visit_type_col}] AS NVARCHAR(30))) = 1 "
            f"THEN CAST(v.[{visit_type_col}] AS INT) ELSE NULL END)"
        )

        ward_expr = "N'Unknown'"
        if ward_col:
            ward_expr = f"CASE WHEN v.[{ward_col}] IS NULL THEN N'Unknown' ELSE dbo.fn_ward_name(v.[{ward_col}]) END"

        patient_expr = "N''"
        regno_expr = "N''"
        if patient_id_col:
            patient_expr = f"CASE WHEN v.[{patient_id_col}] IS NULL THEN N'' ELSE dbo.fn_patientfullname(v.[{patient_id_col}]) END"
            regno_expr = f"CASE WHEN v.[{patient_id_col}] IS NULL THEN N'' ELSE dbo.fn_regno(v.[{patient_id_col}]) END"

        doctor_expr = "N''"
        if doctor_col:
            doctor_expr = f"CASE WHEN v.[{doctor_col}] IS NULL THEN N'' ELSE dbo.fn_doctorfirstname(v.[{doctor_col}]) END"

        sql = f"""
            ;WITH latest_discharge AS (
                SELECT
                    d.[{dis_visit_id_col}] AS Visit_ID,
                    {dis_type_id_select} AS DischargeType_ID,
                    {dis_type_id_key_expr} AS DischargeType_ID_Key,
                    {dis_type_text_expr} AS DischargeTypeRaw,
                    {discharge_date_expr} AS DischargeDate,
                    {cancel_expr} AS CancelStatus,
                    ROW_NUMBER() OVER (
                        PARTITION BY d.[{dis_visit_id_col}]
                        ORDER BY {discharge_date_expr} DESC, d.[{order_col}] DESC
                    ) AS rn
                FROM [dbo].[{discharge_table}] d WITH (NOLOCK)
                WHERE d.[{dis_visit_id_col}] IS NOT NULL
            )
            SELECT
                ? AS Unit,
                ld.Visit_ID,
                ld.DischargeDate,
                {dis_type_id_field} AS DischargeType_ID,
                COALESCE(NULLIF({dt_name_expr}, N''), NULLIF(ld.DischargeTypeRaw, N''), N'Unknown') AS DischargeType,
                {ward_expr} AS Ward,
                {patient_expr} AS Patient,
                {regno_expr} AS Regno,
                {doctor_expr} AS Doctor
            FROM latest_discharge ld
            INNER JOIN [dbo].[{visit_table}] v WITH (NOLOCK)
                ON v.[{visit_id_col}] = ld.Visit_ID
            {join_discharge_type}
            WHERE
                ld.rn = 1
                AND ISNULL(ld.CancelStatus, 0) = 0
                AND {visit_type_num_expr} = 1
                AND ld.DischargeDate >= ?
                AND ld.DischargeDate < DATEADD(DAY, 1, CAST(? AS DATETIME))
            ORDER BY ld.DischargeDate DESC
        """
        df = pd.read_sql(sql, conn, params=[(unit or "").upper(), from_date, to_date])
        if df is None or df.empty:
            return pd.DataFrame()
        df.columns = [str(c).strip() for c in df.columns]
        if "Unit" not in df.columns:
            df["Unit"] = (unit or "").upper()
        return df
    except Exception as e:
        print(f"Volume discharge KPI fetch failed ({unit}): {e}")
        return pd.DataFrame()
    finally:
        try:
            conn.close()
        except Exception:
            pass


def get_volume_discharge_kpi(
    units: list[str],
    from_date: str,
    to_date: str,
    cured_type_ids: list[int] | None = None,
    lama_type_ids: list[int] | None = None,
    referred_type_ids: list[int] | None = None,
    death_type_ids: list[int] | None = None,
    max_records_per_ward: int = 12,
    include_full_rows: bool = False,
) -> dict:
    """
    Build discharge KPI payload for Volume dashboard:
      - total discharges
      - LAMA% = lama_cases / total_discharges * 100
      - Mortality ratio = death_cases / total_discharges * 100
      - breakdown by discharge type and ward
      - sample patient rows (per ward, latest first)
    """
    def _to_int_set(values, default_values):
        base = values if values is not None else default_values
        out = set()
        for v in base:
            try:
                out.add(int(v))
            except Exception:
                continue
        return out or set(default_values)

    cured_ids = sorted(_to_int_set(cured_type_ids, [1, 2]))
    lama_ids = sorted(_to_int_set(lama_type_ids, [4, 5, 7, 8]))
    referred_ids = sorted(_to_int_set(referred_type_ids, [3]))
    death_ids = sorted(_to_int_set(death_type_ids, [6]))
    payload = _empty_volume_discharge_kpi_payload(from_date, to_date, cured_ids, lama_ids, referred_ids, death_ids)

    if not units:
        return payload

    frames = []
    for unit in units:
        df_u = fetch_volume_discharge_rows(unit, from_date, to_date)
        if df_u is not None and not df_u.empty:
            frames.append(df_u)

    if not frames:
        return payload

    df = pd.concat(frames, ignore_index=True, copy=False)
    if df is None or df.empty:
        return payload

    for col in ["Unit", "Ward", "Patient", "Regno", "Doctor", "DischargeType"]:
        if col not in df.columns:
            df[col] = ""
        df[col] = df[col].fillna("").astype(str).str.strip()

    if "DischargeType_ID" not in df.columns:
        df["DischargeType_ID"] = pd.Series([None] * len(df))
    df["DischargeType_ID"] = pd.to_numeric(df["DischargeType_ID"], errors="coerce")
    df["_DischargeDateDT"] = pd.to_datetime(df.get("DischargeDate"), errors="coerce")
    df["Unit"] = df["Unit"].replace("", "UNKNOWN").str.upper()
    df["Ward"] = df["Ward"].replace("", "Unknown")
    df["DischargeType"] = df["DischargeType"].replace("", "Unknown")

    cured_set = set(cured_ids)
    lama_set = set(lama_ids)
    referred_set = set(referred_ids)
    death_set = set(death_ids)
    type_upper = df["DischargeType"].str.upper()
    cured_keywords = r"(?:\bCURED\b|\bRELIEV(?:ED|E)?\b)"
    lama_keywords = r"(?:LAMA|DOR|LEFT AGAINST|PATIENT REFUSED)"
    referred_keywords = r"(?:REFER|REFERRED|REFFERED|REFERR)"
    df["_is_cured"] = (
        df["DischargeType_ID"].isin(cured_set)
        | type_upper.str.contains(cured_keywords, regex=True, na=False)
    )
    df["_is_lama"] = (
        df["DischargeType_ID"].isin(lama_set)
        | type_upper.str.contains(lama_keywords, regex=True, na=False)
    )
    df["_is_referred"] = (
        df["DischargeType_ID"].isin(referred_set)
        | type_upper.str.contains(referred_keywords, regex=True, na=False)
    )
    df["_is_death"] = (
        df["DischargeType_ID"].isin(death_set)
        | type_upper.str.contains("DEATH", regex=False, na=False)
    )

    def _to_int_or_none(value):
        try:
            if pd.isna(value):
                return None
            return int(value)
        except Exception:
            return None

    def _fmt_date(value):
        try:
            if pd.isna(value):
                return ""
            return pd.Timestamp(value).strftime("%Y-%m-%d %H:%M")
        except Exception:
            return ""

    def _rate(part: int, total: int) -> float:
        if not total:
            return 0.0
        return round((float(part) / float(total)) * 100.0, 2)

    def _metrics(frame: pd.DataFrame) -> dict:
        total = int(len(frame))
        cured_cases = int(frame["_is_cured"].sum()) if total else 0
        lama_cases = int(frame["_is_lama"].sum()) if total else 0
        referred_cases = int(frame["_is_referred"].sum()) if total else 0
        death_cases = int(frame["_is_death"].sum()) if total else 0
        return {
            "total_discharges": total,
            "cured_cases": cured_cases,
            "cured_percent": _rate(cured_cases, total),
            "lama_cases": lama_cases,
            "lama_percent": _rate(lama_cases, total),
            "referred_cases": referred_cases,
            "referred_percent": _rate(referred_cases, total),
            "death_cases": death_cases,
            "mortality_ratio": _rate(death_cases, total),
        }

    def _is_cured_type(type_id, type_name: str) -> bool:
        if type_id is not None and type_id in cured_set:
            return True
        return bool(re.search(cured_keywords, str(type_name or "").upper()))

    def _is_lama_type(type_id, type_name: str) -> bool:
        if type_id is not None and type_id in lama_set:
            return True
        return bool(re.search(lama_keywords, str(type_name or "").upper()))

    def _is_death_type(type_id, type_name: str) -> bool:
        if type_id is not None and type_id in death_set:
            return True
        return "DEATH" in str(type_name or "").upper()

    def _is_referred_type(type_id, type_name: str) -> bool:
        if type_id is not None and type_id in referred_set:
            return True
        return bool(re.search(referred_keywords, str(type_name or "").upper()))

    def _type_breakdown(frame: pd.DataFrame) -> list[dict]:
        if frame is None or frame.empty:
            return []
        grp = (
            frame.groupby(["DischargeType_ID", "DischargeType"], dropna=False)
            .size()
            .reset_index(name="Count")
            .sort_values(["Count", "DischargeType"], ascending=[False, True])
        )
        rows = []
        for _, row in grp.iterrows():
            t_id = _to_int_or_none(row.get("DischargeType_ID"))
            t_name = str(row.get("DischargeType") or "").strip() or "Unknown"
            rows.append({
                "discharge_type_id": t_id,
                "discharge_type": t_name,
                "count": int(row.get("Count") or 0),
                "is_cured": _is_cured_type(t_id, t_name),
                "is_lama": _is_lama_type(t_id, t_name),
                "is_referred": _is_referred_type(t_id, t_name),
                "is_mortality": _is_death_type(t_id, t_name),
            })
        return rows

    def _ward_records(frame: pd.DataFrame) -> list[dict]:
        if frame is None or frame.empty:
            return []
        rows = (
            frame.sort_values("_DischargeDateDT", ascending=False)
            .head(max(1, int(max_records_per_ward)))
            .to_dict(orient="records")
        )
        return [
            {
                "patient": str(r.get("Patient") or ""),
                "regno": str(r.get("Regno") or ""),
                "doctor": str(r.get("Doctor") or ""),
                "discharge_type": str(r.get("DischargeType") or "Unknown"),
                "discharge_date": _fmt_date(r.get("_DischargeDateDT")),
            }
            for r in rows
        ]

    def _all_records(frame: pd.DataFrame) -> list[dict]:
        if frame is None or frame.empty:
            return []
        rows = (
            frame.sort_values("_DischargeDateDT", ascending=False)
            .to_dict(orient="records")
        )
        out = []
        for row in rows:
            type_id = _to_int_or_none(row.get("DischargeType_ID"))
            type_name = str(row.get("DischargeType") or "").strip() or "Unknown"
            out.append({
                "unit": str(row.get("Unit") or "UNKNOWN").strip().upper() or "UNKNOWN",
                "ward": str(row.get("Ward") or "Unknown").strip() or "Unknown",
                "patient": str(row.get("Patient") or "").strip(),
                "regno": str(row.get("Regno") or "").strip(),
                "doctor": str(row.get("Doctor") or "").strip(),
                "discharge_type": type_name,
                "discharge_type_id": type_id,
                "discharge_date": _fmt_date(row.get("_DischargeDateDT")),
                "is_cured": bool(row.get("_is_cured", False)),
                "is_lama": bool(row.get("_is_lama", False)),
                "is_referred": bool(row.get("_is_referred", False)),
                "is_mortality": bool(row.get("_is_death", False)),
            })
        return out

    unit_blocks = []
    for unit_name, unit_df in df.groupby("Unit", dropna=False):
        unit_df = unit_df.copy()
        unit_metrics = _metrics(unit_df)
        unit_block = {
            "unit": str(unit_name or "UNKNOWN"),
            **unit_metrics,
            "type_breakdown": _type_breakdown(unit_df),
            "ward_breakdown": [],
        }

        ward_blocks = []
        for ward_name, ward_df in unit_df.groupby("Ward", dropna=False):
            ward_df = ward_df.copy()
            w_metrics = _metrics(ward_df)
            records = _ward_records(ward_df)
            ward_blocks.append({
                "ward": str(ward_name or "Unknown"),
                **w_metrics,
                "type_breakdown": _type_breakdown(ward_df),
                "records": records,
                "shown_records": len(records),
                "available_records": int(len(ward_df)),
            })

        ward_blocks.sort(
            key=lambda r: (-int(r.get("total_discharges") or 0), str(r.get("ward") or ""))
        )
        unit_block["ward_breakdown"] = ward_blocks
        unit_blocks.append(unit_block)

    unit_blocks.sort(key=lambda r: str(r.get("unit") or ""))

    overall_metrics = _metrics(df)
    overall_ward_breakdown = []
    for (unit_name, ward_name), ward_df in df.groupby(["Unit", "Ward"], dropna=False):
        ward_df = ward_df.copy()
        w_metrics = _metrics(ward_df)
        overall_ward_breakdown.append({
            "unit": str(unit_name or "UNKNOWN"),
            "ward": str(ward_name or "Unknown"),
            **w_metrics,
            "type_breakdown": _type_breakdown(ward_df),
        })

    overall_ward_breakdown.sort(
        key=lambda r: (
            -int(r.get("total_discharges") or 0),
            str(r.get("unit") or ""),
            str(r.get("ward") or ""),
        )
    )

    payload["overall"] = {
        **overall_metrics,
        "type_breakdown": _type_breakdown(df),
        "ward_breakdown": overall_ward_breakdown,
    }
    payload["units"] = unit_blocks
    if include_full_rows:
        payload["all_records"] = _all_records(df)
    return payload


def make_volume_payload(details_df: pd.DataFrame) -> dict:
    """
    Returns chart/table JSON for the UI:
      - visitwise
      - deptwise
      - doctorwise
      - citywise
      - patienttype
      - patienttype_patsubtype
      - details (table rows)
    """
    def _pairs(series: pd.Series):
        if series is None or series.empty:
            return []
        series = series.dropna()
        return [{"name": str(k), "value": int(v)} for k, v in series.items()]

    visitwise = _pairs(details_df["TypeOfVisit"].value_counts()) if "TypeOfVisit" in details_df else []
    deptwise  = _pairs(details_df["Dept"].value_counts()) if "Dept" in details_df else []
    doctorwise= _pairs(details_df["Consultant"].value_counts()) if "Consultant" in details_df else []
    citywise  = _pairs(details_df["City"].value_counts()) if "City" in details_df else []
    patienttype = _pairs(details_df["PatientType"].value_counts()) if "PatientType" in details_df else []

    # PT -> PatSubType
    pt_tree = []
    if "PatientType" in details_df and "CorpType" in details_df:
        g = (details_df.groupby(["PatientType","CorpType"])
              .size().reset_index(name="count"))
        for pt, grp in g.groupby("PatientType"):
            children = [{"name": str(r["CorpType"]), "value": int(r["count"])} for _, r in grp.iterrows()]
            pt_tree.append({"name": str(pt), "children": children, "value": int(grp["count"].sum())})

    details_json = json.loads(details_df.to_json(orient="records"))

    return {
        "meta": {"total": int(len(details_df))},
        "visitwise": visitwise,
        "deptwise": deptwise,
        "doctorwise": doctorwise,
        "citywise": citywise,
        "patienttype": patienttype,
        "patienttype_patsubtype": pt_tree,
        "details": details_json
    }

# ========= Volume Excel writer =========
# ========= SINGLE-SHEET SUMMARY WRITER (unit-aware, normalized) =========
import io, re
import pandas as pd

def _norm_text(s: str) -> str:
    t = str(s or "").strip()
    if not t: return ""
    t = re.sub(r"\s+", " ", t)         # collapse spaces
    t = re.sub(r"\s*-\s*", "-", t)     # unify hyphens
    return t.upper()

def _city_display_from_key(k: str) -> str:
    return (k or "").title()

def _norm_patienttype(s: str) -> str:
    k = _norm_text(s)
    k = k.replace("(CASH-CATEGORY)", "(CASH CATEGORY)").replace("(CASHLESS-CATEGORY)", "(CASHLESS CATEGORY)")
    if k == "GENERAL":
        k = "GENERAL (CASH CATEGORY)"
    return k

def _visit_friendly(v: str) -> str:
    m = {
        "HCV": "Health Checkups",
        "DPV": "Diagnostic Visit",
        "IPD": "In Patient Visits",
        "OPD": "Out Patient Visits"
    }
    return m.get(v, v)

def _find_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    if df is None or df.empty:
        return None
    cols = list(df.columns)
    lower_map = {c.lower(): c for c in cols}
    norm_map = {re.sub(r"[^a-z0-9]+", "", c.lower()): c for c in cols}

    for cand in candidates:
        if cand in cols:
            return cand
        cand_lower = cand.lower()
        if cand_lower in lower_map:
            return lower_map[cand_lower]
        cand_norm = re.sub(r"[^a-z0-9]+", "", cand_lower)
        if cand_norm in norm_map:
            return norm_map[cand_norm]

    for cand in candidates:
        cand_norm = re.sub(r"[^a-z0-9]+", "", cand.lower())
        if not cand_norm:
            continue
        for col_norm, orig in norm_map.items():
            if cand_norm in col_norm:
                return orig
    return None

def _parse_age(value):
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, (int, float)) and not pd.isna(value):
        return float(value)
    s = str(value).strip()
    if not s or s.lower() in {"nan", "none", "null"}:
        return None
    m = re.search(r"\d+(\.\d+)?", s)
    if not m:
        return None
    try:
        return float(m.group(0))
    except Exception:
        return None

def _norm_gender(value):
    s = str(value).strip().upper()
    if not s or s in {"NAN", "NONE", "NULL"}:
        return None
    if re.fullmatch(r"\d+(\.\d+)?", s):
        try:
            code = int(float(s))
        except Exception:
            code = None
        if code == 1:
            return "Male"
        if code == 2:
            return "Female"
        if code == 3:
            return "Other"
    if "FEMALE" in s or re.search(r"\bF\b", s) or s.startswith("F"):
        return "Female"
    if "MALE" in s or re.search(r"\bM\b", s) or s.startswith("M"):
        return "Male"
    if "OTHER" in s or "TRANS" in s or re.search(r"\bO\b", s) or s.startswith("O"):
        return "Other"
    return None

def _is_ipd(value) -> bool:
    s = _norm_text(value)
    if not s:
        return False
    return ("IPD" in s) or ("IN PATIENT" in s) or ("INPATIENT" in s)

def _parse_date_range(from_date: str | None, to_date: str | None):
    if not from_date or not to_date:
        return None, None
    start = pd.to_datetime(from_date, errors="coerce")
    end = pd.to_datetime(to_date, errors="coerce")
    if pd.isna(start) or pd.isna(end):
        return None, None
    start = start.normalize()
    end = end.normalize() + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
    return start, end

def _build_mrd_summary(df: pd.DataFrame, vt_col: str, from_date: str | None, to_date: str | None) -> pd.DataFrame:
    gender_col = _find_column(
        df,
        ["Gender", "Sex", "PatientGender", "GenderName", "SexName", "Sex_Name", "Gender_Name", "Patient_Sex"]
    )
    age_col = _find_column(
        df,
        ["AgeYears", "Age_Years", "AgeYrs", "AgeInYears", "AgeInYrs", "Age (Years)", "PatientAge", "Age"]
    )

    if age_col is None:
        return pd.DataFrame()

    genders = df[gender_col].map(_norm_gender) if gender_col else pd.Series([None] * len(df))
    ages = df[age_col].map(_parse_age)
    if ages.notna().sum() == 0:
        return pd.DataFrame()

    visit_type_id_col = _find_column(
        df,
        ["VisitTypeID", "VisitTypeId", "VisitType_ID", "Visit_Type_ID"]
    )
    if visit_type_id_col:
        vt_id = pd.to_numeric(df[visit_type_id_col], errors="coerce")
        ipd_mask = (vt_id == 1)
    else:
        ipd_mask = df[vt_col].map(_is_ipd).fillna(False)

    visit_dt = pd.to_datetime(df.get("VisitDate"), errors="coerce")
    discharge_dt = pd.to_datetime(df.get("DischargeDate"), errors="coerce")

    start, end = _parse_date_range(from_date, to_date)
    if start is not None and end is not None:
        admission_mask = ipd_mask & (visit_dt >= start) & (visit_dt <= end)
        discharge_mask = ipd_mask & (discharge_dt >= start) & (discharge_dt <= end)
    else:
        admission_mask = ipd_mask & visit_dt.notna()
        discharge_mask = ipd_mask & discharge_dt.notna()

    male_mask = (genders == "Male")
    female_mask = (genders == "Female")
    child_mask = ages.notna() & (ages < 18)
    adult_mask = ages.notna() & (ages >= 18) & (ages < 65)
    ger_mask = ages.notna() & (ages >= 65)

    def _count(mask) -> int:
        return int(mask.sum())

    rows = [
        {"Sl. No.": 1, "Category": "IPD Admission Male Children < 18 Yrs", "Numbers": _count(admission_mask & male_mask & child_mask)},
        {"Sl. No.": 2, "Category": "IPD Admission Male Adults < 65 Yrs", "Numbers": _count(admission_mask & male_mask & adult_mask)},
        {"Sl. No.": 3, "Category": "IPD Admission Female Children < 18 Yrs", "Numbers": _count(admission_mask & female_mask & child_mask)},
        {"Sl. No.": 4, "Category": "IPD Admission Female Adults < 65 Yrs", "Numbers": _count(admission_mask & female_mask & adult_mask)},
        {"Sl. No.": 5, "Category": "IPD Admission Geriatric >= 65 Yrs", "Numbers": _count(admission_mask & ger_mask)},
        {"Sl. No.": 6, "Category": "IPD Discharge Male Children < 18 Yrs", "Numbers": _count(discharge_mask & male_mask & child_mask)},
        {"Sl. No.": 7, "Category": "IPD Discharge Male Adults < 65 Yrs", "Numbers": _count(discharge_mask & male_mask & adult_mask)},
        {"Sl. No.": 8, "Category": "IPD Discharge Female Children < 18 Yrs", "Numbers": _count(discharge_mask & female_mask & child_mask)},
        {"Sl. No.": 9, "Category": "IPD Discharge Female Adults < 65 Yrs", "Numbers": _count(discharge_mask & female_mask & adult_mask)},
        {"Sl. No.": 10, "Category": "IPD Discharge Geriatric >= 65 Yrs", "Numbers": _count(discharge_mask & ger_mask)},
    ]
    return pd.DataFrame(rows)

def build_volume_excel(details_df: pd.DataFrame, from_date: str | None = None, to_date: str | None = None) -> bytes:
    if details_df is None or details_df.empty:
        bio = io.BytesIO()
        with pd.ExcelWriter(bio, engine="xlsxwriter") as w:
            pd.DataFrame({"Note": ["No data in selected range"]}).to_excel(w, index=False, sheet_name="Summary")
        return bio.getvalue()

    df = details_df.copy()

    # ---- Ensure Unit is present and uppercase ----
    if "Unit" not in df.columns:
        df["Unit"] = ""
    df["Unit"] = df["Unit"].astype(str).str.strip().str.upper()

    # ---- Identify common columns (don't rename originals) ----
    dep_candidates = ["Dept", "Dept_Name", "Department", "DeptName", "DepartmentName", "Dept Name"]
    dep_col = next((c for c in dep_candidates if c in df.columns), None) or "Dept"

    doc_candidates = ["Consultant", "Doctor", "DoctorIncharge", "DoctorInCharge", "DoctorName"]
    doc_col = next((c for c in doc_candidates if c in df.columns), None) or "Consultant"

    vt_col = "TypeOfVisit" if "TypeOfVisit" in df.columns else next((c for c in df.columns if "Visit" in c), "TypeOfVisit")
    if vt_col not in df.columns: df[vt_col] = ""

    pt_col = "PatientType" if "PatientType" in df.columns else "PatientType"
    if pt_col not in df.columns: df[pt_col] = ""

    corp_col = "CorpType" if "CorpType" in df.columns else ("PatSubType" if "PatSubType" in df.columns else "CorpType")
    if corp_col not in df.columns: df[corp_col] = ""

    if "City" not in df.columns: df["City"] = ""

    # ---- Normalized keys for grouping ----
    df["_VisitDisp"] = df[vt_col].astype(str).str.strip().map(_visit_friendly)

    df["_DeptKey"] = df[dep_col].map(_norm_text)
    df["_DeptDisp"] = df[dep_col].fillna("").astype(str).str.strip()

    df["_DocKey"] = df[doc_col].map(_norm_text)
    df["_DocDisp"] = df[doc_col].fillna("").astype(str).str.strip()

    df["_CityKey"] = df["City"].map(_norm_text)
    df["_CityDisp"] = df["_CityKey"].map(_city_display_from_key)

    df["_PTDisp"] = df[pt_col].map(_norm_patienttype)
    df["_CorpDisp"] = df[corp_col].fillna("").astype(str).str.strip()

    # ---- Summaries (ALWAYS include Unit) ----
    visit_sum = (df.groupby(["Unit", "_VisitDisp"], dropna=False)
                   .size().reset_index(name="Count")
                   .rename(columns={"_VisitDisp": "Visit"}))

    dept_sum  = (df.groupby(["Unit", "_DeptDisp"], dropna=False)
                   .size().reset_index(name="Count")
                   .rename(columns={"_DeptDisp": "Department"}))

    doctor_sum= (df.groupby(["Unit", "_DocDisp"], dropna=False)
                   .size().reset_index(name="Count")
                   .rename(columns={"_DocDisp": "Doctor"}))

    city_sum  = (df.groupby(["Unit", "_CityKey"], dropna=False)
                   .size().reset_index(name="Count"))
    city_sum["City"] = city_sum["_CityKey"].map(_city_display_from_key)
    city_sum = city_sum[["Unit", "City", "Count"]]

    pt_ct_sum = (df.groupby(["Unit", "_PTDisp", "_CorpDisp"], dropna=False)
                   .size().reset_index(name="Count")
                   .rename(columns={"_PTDisp": "PatientType", "_CorpDisp": "CorpType"}))

    mrd_sum = _build_mrd_summary(df, vt_col, from_date, to_date)

    # ---- NEW: Unit-wise Department-wise Visit Type breakdown ----
    unit_dept_visit_sum = (df.groupby(["Unit", "_DeptDisp", "_VisitDisp"], dropna=False)
                             .size().reset_index(name="Count")
                             .rename(columns={"_DeptDisp": "Department", "_VisitDisp": "Visit Type"}))
    # Sort by Unit, Department, then Count descending
    unit_dept_visit_sum = unit_dept_visit_sum.sort_values(
        ["Unit", "Department", "Count"], 
        ascending=[True, True, False]
    )

    # ---- Details (Unit first) ----
    detail_cols = [
        "Unit","VisitNo","VisitDate","DischargeDate","PatientName","RegNo",
        vt_col, doc_col,"ReferringConsultant","ExternalRefConsultant",
        "Address","City",pt_col,corp_col,"PatientContact","Age",dep_col
    ]
    for c in detail_cols:
        if c not in df.columns: df[c] = ""
    details_out = df[detail_cols].copy()
    # Use UI-friendly visit names in details too
    details_out[vt_col] = details_out[vt_col].map(_visit_friendly)

    # ---- Write ONE sheet: Summary (stacked sections) + Details sheet ----
    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="xlsxwriter") as writer:
        wb = writer.book
        ws = wb.add_worksheet("Summary"); writer.sheets["Summary"] = ws

        hfmt = wb.add_format({"bold": True, "bg_color": "#1e3a8a", "font_color": "white",
                              "align": "center", "valign": "vcenter", "border": 1})
        tfmt = wb.add_format({"bold": True, "font_size": 12})
        numfmt = wb.add_format({"align":"right"})

        def put_section(title, frame, start_row):
            ws.write(start_row, 0, title, tfmt)
            if frame is None or frame.empty:
                ws.write(start_row+1, 0, "No data")
                return start_row + 3
            # headers
            for c, name in enumerate(frame.columns):
                ws.write(start_row+1, c, name, hfmt)
            # rows
            for r, vals in enumerate(frame.itertuples(index=False, name=None), start=start_row+2):
                for c, v in enumerate(vals):
                    if isinstance(v, (int, float)) and (c == len(frame.columns)-1 or str(frame.columns[c]).lower()=="count"):
                        ws.write_number(r, c, float(v), numfmt)
                    else:
                        ws.write(r, c, v)
            # auto-width
            for c in range(len(frame.columns)):
                ws.set_column(c, c, min(40, max(12, int(frame.iloc[:, c].astype(str).str.len().quantile(0.85))+2)))
            return r + 2

        row = 0
        row = put_section("Visit-wise", visit_sum.sort_values(["Unit","Count"], ascending=[True,False]), row)
        row = put_section("Department-wise", dept_sum.sort_values(["Unit","Count"], ascending=[True,False]), row)
        # NEW SECTION - Unit-wise Department-wise Visit Type
        row = put_section("Unit -> Department -> Visit Type", unit_dept_visit_sum, row)
        row = put_section("Doctor-wise", doctor_sum.sort_values(["Unit","Count"], ascending=[True,False]), row)
        row = put_section("City-wise", city_sum.sort_values(["Unit","Count"], ascending=[True,False]), row)
        row = put_section("MRD Data", mrd_sum, row)
        row = put_section("PatientType -> CorpType", pt_ct_sum.sort_values(["Unit","PatientType","Count"], ascending=[True,True,False]), row)

        # Details on a second sheet (kept for your records; feel free to comment out entirely)
        details_out.to_excel(writer, index=False, sheet_name="Details")
        ws2 = writer.sheets["Details"]
        for c, name in enumerate(details_out.columns):
            ws2.write(0, c, name, hfmt)
            ws2.set_column(c, c, min(40, max(12, int(details_out.iloc[:, c].astype(str).str.len().quantile(0.85))+2)))
        ws2.freeze_panes(1, 0)

        ws.activate()

    return bio.getvalue()

# ============================================================
# Preventive Health Checkup (HCV) Journey â€” dual result sets
# ============================================================
def fetch_hcv_journey(unit: str):
    """
    Executes dbo.usp_HCVJourneyReport and returns a tuple:
      (pair_df, history_df)
    Result set 1: one row per HCV -> next IPD pair (with days gap).
    Result set 2: full visit history for patients with an HCV visit.
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"âš ï¸ HCV journey: could not connect to {unit}")
        return None, None

    try:
        cur = conn.cursor()
        cur.execute("EXEC dbo.usp_HCVJourneyReport")

        def _read_current_result(cursor: pyodbc.Cursor):
            if cursor.description is None:
                return pd.DataFrame()
            cols = [col[0] for col in cursor.description]
            rows = cursor.fetchall()
            if not rows:
                return pd.DataFrame(columns=cols)
            df = pd.DataFrame.from_records(rows, columns=cols)
            df.columns = [c.strip() for c in df.columns]
            return df

        pair_df = _read_current_result(cur)

        history_df = pd.DataFrame()
        try:
            if cur.nextset():
                history_df = _read_current_result(cur)
        except Exception:
            history_df = pd.DataFrame()

        return pair_df, history_df
    except Exception as e:
        print(f"âŒ HCV journey fetch failed for {unit}: {e}")
        return None, None
    finally:
        try:
            conn.close()
        except Exception:
            pass


# ===================== Admission & Discharge Summary (Dept Wise) =====================
def fetch_admission_discharge_summary(unit: str, from_date: str, to_date: str):
    """
    Calls dbo.usp_RptMISIPDOPDVisit to get admission/discharge summary rows.
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"A›­AÿA_A, Admission summary: could not connect to {unit}")
        return None
    try:
        sql = "SET NOCOUNT ON; EXEC dbo.usp_RptMISIPDOPDVisit ?, ?"
        cur = conn.cursor()
        cur.execute(sql, (from_date, to_date))

        def _read_current_result(cursor: pyodbc.Cursor):
            if cursor.description is None:
                return None
            cols = [col[0] for col in cursor.description]
            rows = cursor.fetchall()
            if not rows:
                df_empty = pd.DataFrame(columns=cols)
                df_empty.columns = [c.strip() for c in df_empty.columns]
                return df_empty
            df_res = pd.DataFrame.from_records(rows, columns=cols)
            df_res.columns = [c.strip() for c in df_res.columns]
            return df_res

        df = None
        while True:
            df = _read_current_result(cur)
            if df is not None:
                break
            if not cur.nextset():
                df = pd.DataFrame()
                break
        try:
            cur.close()
        except Exception:
            pass
        if df is None or df.empty:
            return df
        df.columns = [c.strip() for c in df.columns]
        df["Unit"] = (unit or "").upper()
        return df
    except Exception as e:
        print(f"A›' Admission summary fetch failed for {unit}: {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


# ===================== Old vs New Patient Visits (Marketing MIS) =====================
def fetch_old_new_patient_visits(
    unit: str,
    visit_type_id: int,
    from_date: str,
    to_date: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Calls dbo.usp_ComprehensiveOldNewPatientVisits and returns two result sets:
      (summary_df, details_df)
    Result set 1: department summary with Total/New/Old visits.
    Result set 2: row-level visit details with PatientType (New/Old).
    """
    summary_cols = ["Dept", "TotalVisits", "NewPatientVisits", "OldPatientVisits"]
    detail_cols = ["Dept", "SubDept", "Doctor", "VisitDate", "Visit_ID", "PatientName", "RegNo", "PatientType"]
    empty_summary = pd.DataFrame(columns=summary_cols)
    empty_details = pd.DataFrame(columns=detail_cols)

    conn = get_sql_connection(unit)
    if not conn:
        print(f"Old/New visits: could not connect to {unit}")
        return empty_summary.copy(), empty_details.copy()

    def _read_current_result(cursor: pyodbc.Cursor):
        if cursor.description is None:
            return None
        cols = [col[0] for col in cursor.description]
        rows = cursor.fetchall()
        if not rows:
            df_empty = pd.DataFrame(columns=cols)
            df_empty.columns = [str(c).strip() for c in df_empty.columns]
            return df_empty
        df_res = pd.DataFrame.from_records(rows, columns=cols)
        df_res.columns = [str(c).strip() for c in df_res.columns]
        return df_res

    try:
        sql = "SET NOCOUNT ON; EXEC dbo.usp_ComprehensiveOldNewPatientVisits @VisitTypeID=?, @StartDate=?, @EndDate=?"
        cur = conn.cursor()
        cur.execute(sql, (int(visit_type_id), from_date, to_date))

        frames = []
        while True:
            df = _read_current_result(cur)
            if df is not None:
                frames.append(df)
                if len(frames) >= 2:
                    break
            if not cur.nextset():
                break

        try:
            cur.close()
        except Exception:
            pass

        summary_df = frames[0] if len(frames) >= 1 else empty_summary.copy()
        details_df = frames[1] if len(frames) >= 2 else empty_details.copy()

        # Summary normalization
        if summary_df is None or summary_df.empty:
            summary_df = empty_summary.copy()
        else:
            cmap = {str(c).strip().lower(): c for c in summary_df.columns}

            def _pick(cols):
                for c in cols:
                    key = str(c).strip().lower()
                    if key in cmap:
                        return cmap[key]
                return None

            dept_col = _pick(["Dept", "Department", "DepartmentName", "DeptName"])
            total_col = _pick(["TotalVisits", "TotalVisit", "Total"])
            new_col = _pick(["NewPatientVisits", "NewVisits", "NewPatientVisit"])
            old_col = _pick(["OldPatientVisits", "OldVisits", "OldPatientVisit"])

            out_sum = pd.DataFrame({
                "Dept": summary_df[dept_col] if dept_col else "",
                "TotalVisits": pd.to_numeric(summary_df[total_col], errors="coerce") if total_col else 0,
                "NewPatientVisits": pd.to_numeric(summary_df[new_col], errors="coerce") if new_col else 0,
                "OldPatientVisits": pd.to_numeric(summary_df[old_col], errors="coerce") if old_col else None,
            })
            out_sum["Dept"] = out_sum["Dept"].fillna("").astype(str).str.strip()
            out_sum["TotalVisits"] = out_sum["TotalVisits"].fillna(0).astype(int)
            out_sum["NewPatientVisits"] = out_sum["NewPatientVisits"].fillna(0).astype(int)
            out_sum["OldPatientVisits"] = pd.to_numeric(out_sum["OldPatientVisits"], errors="coerce")
            out_sum["OldPatientVisits"] = out_sum["OldPatientVisits"].fillna(
                out_sum["TotalVisits"] - out_sum["NewPatientVisits"]
            )
            out_sum["OldPatientVisits"] = out_sum["OldPatientVisits"].astype(int)
            summary_df = out_sum[summary_cols]

        # Details normalization
        if details_df is None or details_df.empty:
            details_df = empty_details.copy()
        else:
            cmap = {str(c).strip().lower(): c for c in details_df.columns}

            def _pick(cols):
                for c in cols:
                    key = str(c).strip().lower()
                    if key in cmap:
                        return cmap[key]
                return None

            dept_col = _pick(["Dept", "Department", "DepartmentName", "DeptName"])
            subdept_col = _pick(["SubDept", "SubDepartment", "SubDepartmentName"])
            doctor_col = _pick(["Doctor", "DoctorName"])
            visit_date_col = _pick(["VisitDate", "Visit_Date"])
            visit_id_col = _pick(["Visit_ID", "VisitId", "VisitID"])
            patient_col = _pick(["PatientName", "Patient", "Patient_Name"])
            reg_col = _pick(["RegNo", "Reg_No", "Registration_No", "RegistrationNo"])
            ptype_col = _pick(["PatientType", "PatType"])

            out_det = pd.DataFrame({
                "Dept": details_df[dept_col] if dept_col else "",
                "SubDept": details_df[subdept_col] if subdept_col else "",
                "Doctor": details_df[doctor_col] if doctor_col else "",
                "VisitDate": details_df[visit_date_col] if visit_date_col else None,
                "Visit_ID": pd.to_numeric(details_df[visit_id_col], errors="coerce") if visit_id_col else None,
                "PatientName": details_df[patient_col] if patient_col else "",
                "RegNo": details_df[reg_col] if reg_col else "",
                "PatientType": details_df[ptype_col] if ptype_col else "",
            })
            out_det["VisitDate"] = pd.to_datetime(out_det["VisitDate"], errors="coerce")
            out_det["VisitDate"] = out_det["VisitDate"].dt.strftime("%Y-%m-%d").fillna("")
            out_det["Visit_ID"] = pd.to_numeric(out_det["Visit_ID"], errors="coerce").fillna(0).astype(int)
            for col in ["Dept", "SubDept", "Doctor", "PatientName", "RegNo", "PatientType"]:
                out_det[col] = out_det[col].astype(str).replace({"nan": "", "None": ""}).str.strip()
            details_df = out_det[detail_cols]

        return summary_df, details_df
    except Exception as e:
        print(f"Old/New visits fetch failed for {unit}: {e}")
        return empty_summary.copy(), empty_details.copy()
    finally:
        try:
            conn.close()
        except Exception:
            pass


def fetch_old_new_followup_flags(
    unit: str,
    base_visit_ids: list[int],
    target_visit_types: list[int],
) -> pd.DataFrame:
    """
    For a set of base visit ids, marks whether the same patient later had
    target follow-up visits such as DPV/IPD.
    """
    out_cols = ["BaseVisit_ID", "HasIPD", "HasDPV"]

    clean_base_ids = []
    for value in base_visit_ids or []:
        try:
            visit_id = int(value)
            if visit_id > 0:
                clean_base_ids.append(visit_id)
        except Exception:
            continue
    clean_base_ids = sorted(set(clean_base_ids))

    clean_targets = []
    for value in target_visit_types or []:
        try:
            vt = int(value)
            if vt in {1, 2, 3, 6}:
                clean_targets.append(vt)
        except Exception:
            continue
    clean_targets = sorted(set(clean_targets))

    if not clean_base_ids or not clean_targets:
        return pd.DataFrame(columns=out_cols)

    conn = get_sql_connection(unit)
    if not conn:
        print(f"Old/New follow-up flags: could not connect to {unit}")
        return pd.DataFrame(columns=out_cols)

    def _empty():
        return pd.DataFrame(columns=out_cols)

    def _norm_reg(value) -> str:
        return str(value or "").strip().upper()

    try:
        visit_table = _resolve_table_name(conn, ["Visit", "visit", "Visit_Mst", "VisitMst"])
        if not visit_table:
            return _empty()

        visit_id_col = _resolve_column(conn, visit_table, ["Visit_ID", "VisitId", "VisitID", "Id", "ID"])
        patient_id_col = _resolve_column(conn, visit_table, ["PatientID", "PatientId", "Patient_ID", "patientId"])
        visit_date_col = _resolve_column(conn, visit_table, ["VisitDate", "Visit_Date", "visitDate"])
        visit_type_id_col = _resolve_column(conn, visit_table, ["VisitTypeID", "VisitTypeId", "VisitType_ID", "visitTypeId"])
        type_of_visit_col = _resolve_column(conn, visit_table, ["TypeOfVisit", "VisitType", "Type Of Visit", "Type_Of_Visit"])

        if not visit_id_col or not visit_date_col:
            return _empty()

        visit_text_expr = (
            f"UPPER(LTRIM(RTRIM(CONVERT(NVARCHAR(120), ISNULL(v.[{type_of_visit_col}], N'')))))"
            if type_of_visit_col else
            "N''"
        )
        type_id_expr = (
            f"ISNULL(CONVERT(INT, v.[{visit_type_id_col}]), 0)"
            if visit_type_id_col else
            "0"
        )
        regno_expr = (
            f"CASE WHEN NULLIF(v.[{patient_id_col}], 0) IS NULL THEN N'' "
            f"ELSE LTRIM(RTRIM(ISNULL(dbo.fn_regno(v.[{patient_id_col}]), N''))) END"
            if patient_id_col else
            "N''"
        )
        norm_visit_expr = (
            "CASE "
            f"WHEN {type_id_expr} = 1 OR {visit_text_expr} LIKE N'%IPD%' OR {visit_text_expr} LIKE N'%IN PATIENT%' OR {visit_text_expr} LIKE N'%INPATIENT%' THEN 1 "
            f"WHEN {type_id_expr} = 3 OR {visit_text_expr} LIKE N'%DPV%' THEN 3 "
            f"WHEN {type_id_expr} = 6 OR {visit_text_expr} LIKE N'%HCV%' OR {visit_text_expr} LIKE N'%HEALTH%' THEN 6 "
            f"WHEN {type_id_expr} = 2 OR {visit_text_expr} LIKE N'%OPD%' OR {visit_text_expr} LIKE N'%OUT PATIENT%' OR {visit_text_expr} LIKE N'%OUTPATIENT%' THEN 2 "
            "ELSE 0 END"
        )

        base_frames = []
        for i in range(0, len(clean_base_ids), 900):
            chunk = clean_base_ids[i:i + 900]
            placeholders = ",".join("?" for _ in chunk)
            base_sql = f"""
                SELECT
                    CAST(v.[{visit_id_col}] AS INT) AS BaseVisit_ID,
                    {"CAST(NULLIF(v.[" + patient_id_col + "], 0) AS INT) AS PatientID," if patient_id_col else "CAST(NULL AS INT) AS PatientID,"}
                    {regno_expr} AS RegNo,
                    CAST(v.[{visit_date_col}] AS DATETIME) AS VisitDate
                FROM dbo.[{visit_table}] v WITH (NOLOCK)
                WHERE v.[{visit_id_col}] IN ({placeholders})
            """
            df_chunk = pd.read_sql(base_sql, conn, params=chunk)
            if df_chunk is not None and not df_chunk.empty:
                base_frames.append(df_chunk)

        if not base_frames:
            return _empty()

        base_df = pd.concat(base_frames, ignore_index=True)
        base_df.columns = [str(c).strip() for c in base_df.columns]
        if "BaseVisit_ID" not in base_df.columns:
            return _empty()

        base_df["BaseVisit_ID"] = pd.to_numeric(base_df["BaseVisit_ID"], errors="coerce").fillna(0).astype(int)
        if "PatientID" not in base_df.columns:
            base_df["PatientID"] = pd.Series(dtype="Int64")
        base_df["PatientID"] = pd.to_numeric(base_df["PatientID"], errors="coerce").astype("Int64")
        if "RegNo" not in base_df.columns:
            base_df["RegNo"] = ""
        base_df["RegNo"] = base_df["RegNo"].map(_norm_reg)
        base_df["VisitDate"] = pd.to_datetime(base_df["VisitDate"], errors="coerce")
        base_df = base_df.drop_duplicates(subset=["BaseVisit_ID"], keep="last").reset_index(drop=True)
        if base_df.empty:
            return _empty()

        min_visit_dt = base_df["VisitDate"].dropna().min()
        min_visit_dt = pd.Timestamp("1900-01-01") if pd.isna(min_visit_dt) else pd.Timestamp(min_visit_dt)
        min_visit_param = min_visit_dt.to_pydatetime()

        patient_ids = []
        if "PatientID" in base_df.columns:
            patient_ids = sorted({
                int(v) for v in base_df["PatientID"].dropna().tolist()
                if pd.notna(v) and int(v) > 0
            })
        reg_nos = sorted({
            _norm_reg(v) for v in base_df.get("RegNo", pd.Series(dtype=str)).tolist()
            if _norm_reg(v)
        })

        target_frames = []
        target_placeholders = ",".join("?" for _ in clean_targets)
        target_prefix_sql = f"""
            SELECT
                CAST(v.[{visit_id_col}] AS INT) AS Visit_ID,
                {"CAST(NULLIF(v.[" + patient_id_col + "], 0) AS INT) AS PatientID," if patient_id_col else "CAST(NULL AS INT) AS PatientID,"}
                {regno_expr} AS RegNo,
                CAST(v.[{visit_date_col}] AS DATETIME) AS VisitDate,
                {norm_visit_expr} AS NormVisitTypeID
            FROM dbo.[{visit_table}] v WITH (NOLOCK)
            WHERE v.[{visit_date_col}] >= ?
              AND {norm_visit_expr} IN ({target_placeholders})
        """
        target_prefix_params = [min_visit_param] + clean_targets

        if patient_id_col and patient_ids:
            for i in range(0, len(patient_ids), 900):
                chunk = patient_ids[i:i + 900]
                pid_placeholders = ",".join("?" for _ in chunk)
                sql = f"{target_prefix_sql} AND v.[{patient_id_col}] IN ({pid_placeholders})"
                df_chunk = pd.read_sql(sql, conn, params=target_prefix_params + chunk)
                if df_chunk is not None and not df_chunk.empty:
                    target_frames.append(df_chunk)

        if reg_nos:
            for i in range(0, len(reg_nos), 500):
                chunk = reg_nos[i:i + 500]
                reg_placeholders = ",".join("?" for _ in chunk)
                sql = f"{target_prefix_sql} AND {regno_expr} IN ({reg_placeholders})"
                df_chunk = pd.read_sql(sql, conn, params=target_prefix_params + chunk)
                if df_chunk is not None and not df_chunk.empty:
                    target_frames.append(df_chunk)

        if target_frames:
            target_df = pd.concat(target_frames, ignore_index=True)
            target_df.columns = [str(c).strip() for c in target_df.columns]
            target_df["Visit_ID"] = pd.to_numeric(target_df["Visit_ID"], errors="coerce").fillna(0).astype(int)
            if "PatientID" not in target_df.columns:
                target_df["PatientID"] = pd.Series(dtype="Int64")
            target_df["PatientID"] = pd.to_numeric(target_df["PatientID"], errors="coerce").astype("Int64")
            target_df["RegNo"] = target_df.get("RegNo", "").map(_norm_reg)
            target_df["VisitDate"] = pd.to_datetime(target_df["VisitDate"], errors="coerce")
            target_df["NormVisitTypeID"] = pd.to_numeric(target_df["NormVisitTypeID"], errors="coerce").fillna(0).astype(int)
            target_df = target_df.drop_duplicates(subset=["Visit_ID"], keep="last").reset_index(drop=True)
        else:
            target_df = pd.DataFrame(columns=["Visit_ID", "PatientID", "RegNo", "VisitDate", "NormVisitTypeID"])

        target_by_pid = {}
        if not target_df.empty and "PatientID" in target_df.columns:
            pid_df = target_df[target_df["PatientID"].notna()].copy()
            if not pid_df.empty:
                for pid, grp in pid_df.groupby("PatientID", dropna=False):
                    try:
                        target_by_pid[int(pid)] = grp.sort_values(["VisitDate", "Visit_ID"], kind="mergesort")
                    except Exception:
                        continue

        target_by_reg = {}
        if not target_df.empty:
            reg_df = target_df[target_df["RegNo"].astype(str).str.strip() != ""].copy()
            if not reg_df.empty:
                for reg_no, grp in reg_df.groupby("RegNo", dropna=False):
                    reg_key = _norm_reg(reg_no)
                    if reg_key:
                        target_by_reg[reg_key] = grp.sort_values(["VisitDate", "Visit_ID"], kind="mergesort")

        out_rows = []
        for _, base_row in base_df.iterrows():
            base_visit_id = int(base_row.get("BaseVisit_ID") or 0)
            base_dt = pd.to_datetime(base_row.get("VisitDate"), errors="coerce")
            base_pid = 0
            if pd.notna(base_row.get("PatientID")):
                try:
                    base_pid = int(base_row.get("PatientID") or 0)
                except Exception:
                    base_pid = 0
            base_reg = _norm_reg(base_row.get("RegNo"))

            matches = []
            if base_pid > 0 and base_pid in target_by_pid:
                matches.append(target_by_pid[base_pid])
            if base_reg and base_reg in target_by_reg:
                matches.append(target_by_reg[base_reg])

            if matches:
                match_df = pd.concat(matches, ignore_index=True).drop_duplicates(subset=["Visit_ID"], keep="last")
                match_df = match_df[match_df["Visit_ID"] != base_visit_id].copy()
                if pd.notna(base_dt):
                    later_mask = (match_df["VisitDate"] > base_dt) | (
                        (match_df["VisitDate"] == base_dt) & (match_df["Visit_ID"] > base_visit_id)
                    )
                    match_df = match_df[later_mask].copy()
            else:
                match_df = pd.DataFrame(columns=["NormVisitTypeID"])

            out_rows.append({
                "BaseVisit_ID": base_visit_id,
                "HasIPD": int((match_df["NormVisitTypeID"] == 1).any()) if 1 in clean_targets else 0,
                "HasDPV": int((match_df["NormVisitTypeID"] == 3).any()) if 3 in clean_targets else 0,
            })

        out_df = pd.DataFrame(out_rows, columns=out_cols)
        if out_df.empty:
            return _empty()
        for col in ["BaseVisit_ID", "HasIPD", "HasDPV"]:
            out_df[col] = pd.to_numeric(out_df[col], errors="coerce").fillna(0).astype(int)
        return out_df
    except Exception as e:
        print(f"Old/New follow-up flags fetch failed for {unit}: {e}")
        return _empty()
    finally:
        try:
            conn.close()
        except Exception:
            pass


def fetch_patient_source_map_for_visits(unit: str, visit_ids: list[int]) -> pd.DataFrame:
    """
    Visit-level patient source mapping aligned with revenue source logic:
      Visit.PatientSourceId -> Occupation_Mst.Occupation_Name.
    Returns Visit_ID, SourceID, SourceName.
    """
    out_cols = ["Visit_ID", "SourceID", "SourceName"]
    if not visit_ids:
        return pd.DataFrame(columns=out_cols)

    clean_ids = []
    for v in visit_ids:
        try:
            iv = int(v)
            if iv > 0:
                clean_ids.append(iv)
        except Exception:
            continue
    clean_ids = sorted(set(clean_ids))
    if not clean_ids:
        return pd.DataFrame(columns=out_cols)

    conn = get_sql_connection(unit)
    if not conn:
        print(f"Patient source map (visit) could not connect to {unit}")
        return pd.DataFrame(columns=out_cols)

    try:
        visit_table = _resolve_table_name(conn, ["Visit", "visit", "Visit_Mst", "VisitMst"])
        if not visit_table:
            return pd.DataFrame(columns=out_cols)

        visit_id_col = _resolve_column(conn, visit_table, ["Visit_ID", "VisitId", "VisitID", "visitId", "ID", "Id"])
        src_col = _resolve_column(conn, visit_table, ["PatientSourceId", "PatientSourceID", "PatientSource_ID", "patientSourceId"])
        if not visit_id_col:
            return pd.DataFrame(columns=out_cols)

        occ_table = _resolve_table_name(conn, ["Occupation_Mst", "occupation_mst", "OccupationMst"])
        occ_id_col = _resolve_column(conn, occ_table, ["Occupation_ID", "OccupationId", "occupation_id"]) if occ_table else None
        occ_name_col = _resolve_column(conn, occ_table, ["Occupation_Name", "OccupationName", "occupation_name"]) if occ_table else None

        src_text_expr = "NULL"
        src_int_expr = "NULL"
        src_id_expr = "CAST(0 AS INT)"
        src_name_expr = "N'Unknown'"
        join_sql = ""

        if src_col:
            src_text_expr = f"LTRIM(RTRIM(CONVERT(NVARCHAR(100), v.[{src_col}])))"
            src_int_expr = (
                f"(CASE WHEN {src_text_expr} <> '' "
                f"AND {src_text_expr} NOT LIKE '%[^0-9]%' "
                f"THEN CONVERT(INT, {src_text_expr}) ELSE NULL END)"
            )
            src_id_expr = f"ISNULL({src_int_expr}, 0)"
            if occ_table and occ_id_col and occ_name_col:
                join_sql = (
                    f"LEFT JOIN dbo.[{occ_table}] om WITH (NOLOCK) "
                    f"ON om.[{occ_id_col}] = {src_int_expr}"
                )
                src_name_expr = f"LTRIM(RTRIM(COALESCE(NULLIF(om.[{occ_name_col}], N''), N'Unknown')))"

        parts = []
        for i in range(0, len(clean_ids), 900):
            chunk = clean_ids[i:i + 900]
            placeholders = ",".join("?" for _ in chunk)
            sql = f"""
                SELECT
                    v.[{visit_id_col}] AS Visit_ID,
                    {src_id_expr} AS SourceID,
                    {src_name_expr} AS SourceName
                FROM dbo.[{visit_table}] v WITH (NOLOCK)
                {join_sql}
                WHERE v.[{visit_id_col}] IN ({placeholders})
            """
            part = pd.read_sql(sql, conn, params=chunk)
            if part is not None and not part.empty:
                parts.append(part)

        if not parts:
            return pd.DataFrame(columns=out_cols)

        out = pd.concat(parts, ignore_index=True, copy=False)
        out.columns = [str(c).strip() for c in out.columns]
        out["Visit_ID"] = pd.to_numeric(out.get("Visit_ID"), errors="coerce").fillna(0).astype(int)
        out["SourceID"] = pd.to_numeric(out.get("SourceID"), errors="coerce").fillna(0).astype(int)
        out["SourceName"] = (
            out.get("SourceName", "")
            .astype(str)
            .str.strip()
            .replace({"": pd.NA, "None": pd.NA, "nan": pd.NA})
            .fillna("Unknown")
        )
        out = out.drop_duplicates(subset=["Visit_ID"], keep="last")
        return out[out_cols]
    except Exception as e:
        print(f"Error fetching visit-level patient source map ({unit}): {e}")
        return pd.DataFrame(columns=out_cols)
    finally:
        try:
            conn.close()
        except Exception:
            pass


# ===================== Pharmacy: Store List + Current Stock =====================
def fetch_pharmacy_stores(unit: str):
    """
    Fetch store list for pharmacy stock selection.
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"A›­AÿA_A, Pharmacy stores: could not connect to {unit}")
        return None
    try:
        sql = """
            SELECT ID, Name
            FROM dbo.Store
            WHERE ISNULL(Mainstore, 0) = 1 OR Mainstore = '1'
        """
        df = pd.read_sql(sql, conn)
        if df is None or df.empty:
            return df
        df.columns = [c.strip() for c in df.columns]
        df["Unit"] = (unit or "").upper()
        return df
    except Exception as e:
        print(f"A›' Pharmacy stores fetch failed for {unit}: {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def fetch_current_pharmacy_stock(unit: str, store_id: int):
    """
    Fetch current pharmacy stock for a store.
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"A›­AÿA_A, Pharmacy stock: could not connect to {unit}")
        return None
    try:
        sql = "SET NOCOUNT ON; EXEC dbo.usp_Rptcurrentphstocknew ?"
        df = pd.read_sql(sql, conn, params=[store_id])
        if df is None or df.empty:
            return df
        df.columns = [c.strip() for c in df.columns]
        df["Unit"] = (unit or "").upper()
        return df
    except Exception as e:
        print(f"A›' Pharmacy stock fetch failed for {unit}: {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def fetch_pharmacy_storewise_purchase(unit: str, from_date: str, to_date: str, store_id: int):
    """
    Fetch store-wise purchase rows (GRN lines) for a given date range and store.
    Calls dbo.usp_storewisepurchaserpt @FromDate, @ToDate, @location.
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Pharmacy store-wise purchase: could not connect to {unit}")
        return None
    try:
        sql = "SET NOCOUNT ON; EXEC dbo.usp_storewisepurchaserpt @FromDate=?, @ToDate=?, @location=?"
        df = pd.read_sql(sql, conn, params=[from_date, to_date, int(store_id or 0)])
        if df is None or df.empty:
            return df
        df.columns = [c.strip() for c in df.columns]
        df["Unit"] = (unit or "").upper()
        return df
    except Exception as e:
        print(f"Pharmacy store-wise purchase fetch failed for {unit}: {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def fetch_pharmacy_purchase_return_itemwise(unit: str, from_date: str, to_date: str, store_id: int | None = None):
    """
    Fetch item-wise purchase return rows for a date range.
    Calls dbo.usp_RPTGetPurRetItemwise @fromdate, @todate.
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Pharmacy purchase return itemwise: could not connect to {unit}")
        return None
    try:
        sql = "SET NOCOUNT ON; EXEC dbo.usp_RPTGetPurRetItemwise @fromdate=?, @todate=?"
        df = pd.read_sql(sql, conn, params=[from_date, to_date])
        if df is None or df.empty:
            return df
        df.columns = [c.strip() for c in df.columns]
        if store_id is not None and "Storeid" in df.columns:
            store_vals = pd.to_numeric(df["Storeid"], errors="coerce")
            df = df[store_vals == int(store_id)]
        df["Unit"] = (unit or "").upper()
        return df
    except Exception as e:
        print(f"Pharmacy purchase return itemwise fetch failed for {unit}: {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


# ===================== Purchase Module (PO) =====================
def fetch_purchase_pack_sizes(unit: str):
    """
    Calls dbo.Usp_GetIvItemPackSize to fetch pack size master.
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Could not connect to {unit} for pack sizes")
        return None
    try:
        df = pd.read_sql("EXEC dbo.Usp_GetIvItemPackSize", conn)
        if df is None or df.empty:
            return df
        df.columns = [c.strip() for c in df.columns]
        return df
    except Exception as e:
        print(f"Error fetching pack sizes ({unit}): {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _resolve_table_name(conn, candidates: list[str]) -> str | None:
    if not candidates:
        return None
    try:
        placeholders = ",".join("?" for _ in candidates)
        df = pd.read_sql(f"SELECT name FROM sys.tables WHERE name IN ({placeholders})", conn, params=candidates)
    except Exception:
        return None
    if df is None or df.empty:
        return None
    names = [str(n).strip() for n in df["name"].tolist()]
    for cand in candidates:
        for name in names:
            if name.lower() == cand.lower():
                return name
    return names[0] if names else None


def _resolve_column(conn, table_name: str, candidates: list[str]) -> str | None:
    if not table_name or not candidates:
        return None
    try:
        cols_df = pd.read_sql(
            "SELECT name FROM sys.columns WHERE object_id = OBJECT_ID(?)",
            conn,
            params=[table_name],
        )
    except Exception:
        return None
    if cols_df is None or cols_df.empty:
        return None
    cols = [str(c).strip() for c in cols_df["name"].tolist()]
    for cand in candidates:
        for col in cols:
            if col.lower() == cand.lower():
                return col
    return None


def _get_table_columns(conn, table_name: str) -> set[str]:
    """Return lowercased column names for a table (best-effort)."""
    if not table_name:
        return set()
    try:
        cols_df = pd.read_sql(
            "SELECT name FROM sys.columns WHERE object_id = OBJECT_ID(?)",
            conn,
            params=[table_name],
        )
    except Exception:
        return set()
    if cols_df is None or cols_df.empty:
        return set()
    return {str(c).strip().lower() for c in cols_df["name"].tolist() if str(c).strip()}


def _fetch_text_column_limits(conn, table_name: str, columns: list[str]) -> dict[str, int]:
    if not table_name or not columns:
        return {}
    try:
        placeholders = ",".join("?" for _ in columns)
        sql = f"""
            SELECT COLUMN_NAME, CHARACTER_MAXIMUM_LENGTH
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = 'dbo'
              AND TABLE_NAME = ?
              AND COLUMN_NAME IN ({placeholders})
        """
        df = pd.read_sql(sql, conn, params=[table_name, *columns])
    except Exception:
        return {}
    if df is None or df.empty:
        return {}
    limits = {}
    for _, row in df.iterrows():
        col = str(row.get("COLUMN_NAME") or "").strip()
        max_len = row.get("CHARACTER_MAXIMUM_LENGTH")
        if not col:
            continue
        try:
            max_len = int(max_len)
        except Exception:
            max_len = None
        if max_len is not None and max_len < 0:
            max_len = None
        limits[col.lower()] = max_len
    return limits


def _apply_text_limits(params: dict, limits: dict[str, int], key_to_col: dict[str, str]) -> dict:
    if not params or not limits or not key_to_col:
        return params
    updated = dict(params)
    for key, col in key_to_col.items():
        if not col:
            continue
        max_len = limits.get(col.lower())
        if not max_len:
            continue
        val = updated.get(key)
        if isinstance(val, str) and len(val) > max_len:
            updated[key] = val[:max_len]
    return updated


_PO_MST_TEXT_PARAM_MAP = {
    "pPono": "PONo",
    "pDeliveryterms": "DeliveryTerms",
    "pPaymentsterms": "PaymentsTerms",
    "pOtherterms": "OtherTerms",
    "pNotes": "Notes",
    "pSpecialNotes": "SpecialNotes",
    "pPreparedby": "Preparedby",
    "pCustom1": "Custom1",
    "pCustom2": "Custom2",
    "pSignauthorityperson": "SignAuthorityPerson",
    "pSignauthoritypdesig": "SignAuthorityPDesig",
    "pRefno": "RefNo",
    "pSubject": "Subject",
    "SeniorApprovalAuthorityName": "SeniorApprovalAuthorityName",
    "SeniorApprovalAuthorityDesignation": "SeniorApprovalAuthorityDesignation",
    "pInsertedByUserID": "InsertedByUserID",
    "pInsertedMacName": "InsertedMacName",
    "pInsertedMacID": "InsertedMacID",
    "pInsertedIPAddress": "InsertedIPAddress",
    "Against": "Against",
    "Status": "Status",
}


def _trim_po_mst_params(conn, params: dict) -> dict:
    if not params:
        return params
    table_name = _resolve_table_name(conn, ["IVPoMst", "IvPoMst", "IVPO_MST", "IV_Po_Mst"])
    if not table_name:
        return params
    columns = list({col for col in _PO_MST_TEXT_PARAM_MAP.values() if col})
    limits = _fetch_text_column_limits(conn, table_name, columns)
    if not limits:
        return params
    return _apply_text_limits(params, limits, _PO_MST_TEXT_PARAM_MAP)


def _ensure_iv_item_technical_specs_column_conn(conn) -> bool:
    """
    Ensure IVItem has a long-form technical specs column for purchase descriptions.
    """
    if not conn:
        return False
    table_name = _resolve_table_name(conn, ["IVItem", "IvItem", "IVITEM"])
    if not table_name:
        return False
    if _resolve_column(conn, table_name, ["TechnicalSpecs", "TechnicalSpec", "TechSpecs", "TechSpec"]):
        return True
    try:
        cur = conn.cursor()
        cur.execute(f"ALTER TABLE dbo.{table_name} ADD TechnicalSpecs NVARCHAR(MAX) NULL")
        conn.commit()
        return True
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        return bool(_resolve_column(conn, table_name, ["TechnicalSpecs", "TechnicalSpec", "TechSpecs", "TechSpec"]))


def _update_iv_item_technical_specs_conn(conn, item_id: int, technical_specs: str | None) -> bool:
    """
    Persist technical specs in IVItem when column exists. Safe no-op if unavailable.
    """
    if not conn:
        return False
    try:
        item_id = int(item_id or 0)
    except Exception:
        return False
    if item_id <= 0:
        return False
    table_name = _resolve_table_name(conn, ["IVItem", "IvItem", "IVITEM"])
    if not table_name:
        return False
    spec_col = _resolve_column(conn, table_name, ["TechnicalSpecs", "TechnicalSpec", "TechSpecs", "TechSpec"])
    if not spec_col and not _ensure_iv_item_technical_specs_column_conn(conn):
        return False
    spec_col = _resolve_column(conn, table_name, ["TechnicalSpecs", "TechnicalSpec", "TechSpecs", "TechSpec"]) or "TechnicalSpecs"
    id_col = _resolve_column(conn, table_name, ["ID", "Id", "ItemID", "ItemId"]) or "ID"
    spec_val = str(technical_specs or "").strip() or None
    try:
        cur = conn.cursor()
        cur.execute(
            f"UPDATE dbo.{table_name} SET [{spec_col}] = ? WHERE [{id_col}] = ?",
            (spec_val, item_id),
        )
        conn.commit()
        return True
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        return False


def _ensure_iv_po_mst_purchasing_dept_column_conn(conn) -> bool:
    """
    Ensure IVPoMst has PurchasingDeptId column for approval routing.
    """
    if not conn:
        return False
    table_name = _resolve_table_name(conn, ["IVPoMst", "IvPoMst", "IVPO_MST", "IV_Po_Mst"])
    if not table_name:
        return False
    if _resolve_column(conn, table_name, ["PurchasingDeptId", "PurchasingDeptID", "PurchaseDeptId", "PurchaseDeptID"]):
        return True
    try:
        cur = conn.cursor()
        cur.execute(f"ALTER TABLE dbo.{table_name} ADD PurchasingDeptId INT NULL")
        conn.commit()
        return True
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        return bool(_resolve_column(conn, table_name, ["PurchasingDeptId", "PurchasingDeptID", "PurchaseDeptId", "PurchaseDeptID"]))


def _update_iv_po_mst_purchasing_dept_conn(conn, po_id: int, purchasing_dept_id: int | None) -> bool:
    """
    Persist purchasing department id in IVPoMst. Safe no-op if column unavailable.
    """
    if not conn:
        return False
    try:
        po_id = int(po_id or 0)
    except Exception:
        return False
    if po_id <= 0:
        return False
    table_name = _resolve_table_name(conn, ["IVPoMst", "IvPoMst", "IVPO_MST", "IV_Po_Mst"])
    if not table_name:
        return False
    dept_col = _resolve_column(conn, table_name, ["PurchasingDeptId", "PurchasingDeptID", "PurchaseDeptId", "PurchaseDeptID"])
    if not dept_col and not _ensure_iv_po_mst_purchasing_dept_column_conn(conn):
        return False
    dept_col = _resolve_column(conn, table_name, ["PurchasingDeptId", "PurchasingDeptID", "PurchaseDeptId", "PurchaseDeptID"]) or "PurchasingDeptId"
    id_col = _resolve_column(conn, table_name, ["ID", "Id"]) or "ID"
    dept_val = None
    try:
        dept_num = int(purchasing_dept_id or 0)
        if dept_num > 0:
            dept_val = dept_num
    except Exception:
        dept_val = None
    try:
        cur = conn.cursor()
        cur.execute(
            f"UPDATE dbo.{table_name} SET [{dept_col}] = ? WHERE [{id_col}] = ?",
            (dept_val, po_id),
        )
        conn.commit()
        return True
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        return False


def _ensure_iv_po_mst_special_notes_column_conn(conn) -> bool:
    """
    Ensure IVPoMst has SpecialNotes column for long-form PO narration.
    """
    if not conn:
        return False
    table_name = _resolve_table_name(conn, ["IVPoMst", "IvPoMst", "IVPO_MST", "IV_Po_Mst"])
    if not table_name:
        return False
    if _resolve_column(conn, table_name, ["SpecialNotes", "SpecialNote"]):
        return True
    try:
        cur = conn.cursor()
        cur.execute(f"ALTER TABLE dbo.{table_name} ADD SpecialNotes NVARCHAR(1000) NULL")
        conn.commit()
        return True
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        return bool(_resolve_column(conn, table_name, ["SpecialNotes", "SpecialNote"]))


def _update_iv_po_mst_special_notes_conn(conn, po_id: int, special_notes: str | None) -> bool:
    """
    Persist long-form special notes in IVPoMst. Safe no-op if column unavailable.
    """
    if not conn:
        return False
    try:
        po_id = int(po_id or 0)
    except Exception:
        return False
    if po_id <= 0:
        return False
    table_name = _resolve_table_name(conn, ["IVPoMst", "IvPoMst", "IVPO_MST", "IV_Po_Mst"])
    if not table_name:
        return False
    note_col = _resolve_column(conn, table_name, ["SpecialNotes", "SpecialNote"])
    if not note_col and not _ensure_iv_po_mst_special_notes_column_conn(conn):
        return False
    note_col = _resolve_column(conn, table_name, ["SpecialNotes", "SpecialNote"]) or "SpecialNotes"
    id_col = _resolve_column(conn, table_name, ["ID", "Id"]) or "ID"
    note_val = str(special_notes or "").strip() or None
    if note_val and len(note_val) > 1000:
        note_val = note_val[:1000]
    try:
        cur = conn.cursor()
        cur.execute(
            f"UPDATE dbo.{table_name} SET [{note_col}] = ? WHERE [{id_col}] = ?",
            (note_val, po_id),
        )
        conn.commit()
        return True
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        return False


def _ensure_iv_po_mst_senior_approval_authority_column_conn(conn) -> bool:
    """
    Ensure IVPoMst has SeniorApprovalAuthorityName column for selective senior signoff on PDF.
    """
    if not conn:
        return False
    table_name = _resolve_table_name(conn, ["IVPoMst", "IvPoMst", "IVPO_MST", "IV_Po_Mst"])
    if not table_name:
        return False
    if _resolve_column(conn, table_name, ["SeniorApprovalAuthorityName", "SeniorApprovalAuthority"]):
        return True
    try:
        cur = conn.cursor()
        cur.execute(f"ALTER TABLE dbo.{table_name} ADD SeniorApprovalAuthorityName NVARCHAR(160) NULL")
        conn.commit()
        return True
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        return bool(_resolve_column(conn, table_name, ["SeniorApprovalAuthorityName", "SeniorApprovalAuthority"]))


def _update_iv_po_mst_senior_approval_authority_conn(conn, po_id: int, authority_name: str | None) -> bool:
    """
    Persist optional senior approval authority name in IVPoMst.
    """
    if not conn:
        return False
    try:
        po_id = int(po_id or 0)
    except Exception:
        return False
    if po_id <= 0:
        return False
    table_name = _resolve_table_name(conn, ["IVPoMst", "IvPoMst", "IVPO_MST", "IV_Po_Mst"])
    if not table_name:
        return False
    auth_col = _resolve_column(conn, table_name, ["SeniorApprovalAuthorityName", "SeniorApprovalAuthority"])
    if not auth_col and not _ensure_iv_po_mst_senior_approval_authority_column_conn(conn):
        return False
    auth_col = _resolve_column(conn, table_name, ["SeniorApprovalAuthorityName", "SeniorApprovalAuthority"]) or "SeniorApprovalAuthorityName"
    id_col = _resolve_column(conn, table_name, ["ID", "Id"]) or "ID"
    auth_val = str(authority_name or "").strip() or None
    if auth_val and len(auth_val) > 160:
        auth_val = auth_val[:160]
    try:
        cur = conn.cursor()
        cur.execute(
            f"UPDATE dbo.{table_name} SET [{auth_col}] = ? WHERE [{id_col}] = ?",
            (auth_val, po_id),
        )
        conn.commit()
        return True
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        return False


def _ensure_iv_po_mst_senior_approval_designation_column_conn(conn) -> bool:
    """
    Ensure IVPoMst has SeniorApprovalAuthorityDesignation column for selective senior signoff on PDF.
    """
    if not conn:
        return False
    table_name = _resolve_table_name(conn, ["IVPoMst", "IvPoMst", "IVPO_MST", "IV_Po_Mst"])
    if not table_name:
        return False
    if _resolve_column(conn, table_name, ["SeniorApprovalAuthorityDesignation", "SeniorApprovalDesignation"]):
        return True
    try:
        cur = conn.cursor()
        cur.execute(f"ALTER TABLE dbo.{table_name} ADD SeniorApprovalAuthorityDesignation NVARCHAR(120) NULL")
        conn.commit()
        return True
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        return bool(_resolve_column(conn, table_name, ["SeniorApprovalAuthorityDesignation", "SeniorApprovalDesignation"]))


def _update_iv_po_mst_senior_approval_designation_conn(conn, po_id: int, designation: str | None) -> bool:
    """
    Persist optional senior approval designation in IVPoMst.
    """
    if not conn:
        return False
    try:
        po_id = int(po_id or 0)
    except Exception:
        return False
    if po_id <= 0:
        return False
    table_name = _resolve_table_name(conn, ["IVPoMst", "IvPoMst", "IVPO_MST", "IV_Po_Mst"])
    if not table_name:
        return False
    desig_col = _resolve_column(conn, table_name, ["SeniorApprovalAuthorityDesignation", "SeniorApprovalDesignation"])
    if not desig_col and not _ensure_iv_po_mst_senior_approval_designation_column_conn(conn):
        return False
    desig_col = _resolve_column(conn, table_name, ["SeniorApprovalAuthorityDesignation", "SeniorApprovalDesignation"]) or "SeniorApprovalAuthorityDesignation"
    id_col = _resolve_column(conn, table_name, ["ID", "Id"]) or "ID"
    desig_val = str(designation or "").strip() or None
    if desig_val and len(desig_val) > 120:
        desig_val = desig_val[:120]
    try:
        cur = conn.cursor()
        cur.execute(
            f"UPDATE dbo.{table_name} SET [{desig_col}] = ? WHERE [{id_col}] = ?",
            (desig_val, po_id),
        )
        conn.commit()
        return True
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        return False


def ensure_iv_item_technical_specs_column(unit: str) -> bool:
    """
    Public helper to create IVItem.TechnicalSpecs if missing.
    """
    conn = get_sql_connection(unit)
    if not conn:
        return False
    try:
        return _ensure_iv_item_technical_specs_column_conn(conn)
    finally:
        try:
            conn.close()
        except Exception:
            pass


def ensure_po_purchasing_dept_column(unit: str) -> bool:
    """
    Public helper to create IVPoMst.PurchasingDeptId if missing.
    """
    conn = get_sql_connection(unit)
    if not conn:
        return False
    try:
        return _ensure_iv_po_mst_purchasing_dept_column_conn(conn)
    finally:
        try:
            conn.close()
        except Exception:
            pass


def ensure_po_special_notes_column(unit: str) -> bool:
    """
    Public helper to create IVPoMst.SpecialNotes if missing.
    """
    conn = get_sql_connection(unit)
    if not conn:
        return False
    try:
        return _ensure_iv_po_mst_special_notes_column_conn(conn)
    finally:
        try:
            conn.close()
        except Exception:
            pass


def ensure_po_senior_approval_authority_column(unit: str) -> bool:
    """
    Public helper to create IVPoMst.SeniorApprovalAuthorityName if missing.
    """
    conn = get_sql_connection(unit)
    if not conn:
        return False
    try:
        return _ensure_iv_po_mst_senior_approval_authority_column_conn(conn)
    finally:
        try:
            conn.close()
        except Exception:
            pass


def ensure_po_senior_approval_designation_column(unit: str) -> bool:
    """
    Public helper to create IVPoMst.SeniorApprovalAuthorityDesignation if missing.
    """
    conn = get_sql_connection(unit)
    if not conn:
        return False
    try:
        return _ensure_iv_po_mst_senior_approval_designation_column_conn(conn)
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _fetch_master_table(conn, table_candidates, id_candidates, name_candidates, code_candidates=None):
    table_name = _resolve_table_name(conn, table_candidates)
    if not table_name:
        return pd.DataFrame()
    id_col = _resolve_column(conn, table_name, id_candidates)
    name_col = _resolve_column(conn, table_name, name_candidates)
    code_col = _resolve_column(conn, table_name, code_candidates or [])
    if not id_col or not name_col:
        return pd.DataFrame()
    select_cols = [f"{id_col} AS ID", f"{name_col} AS Name"]
    if code_col:
        select_cols.append(f"{code_col} AS Code")
    sql = f"SELECT {', '.join(select_cols)} FROM dbo.{table_name}"
    return pd.read_sql(sql, conn)


def _normalize_unit_master_name(value: str) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip())


def _sanitize_unit_code(value: str) -> str:
    raw = str(value or "").strip().upper()
    if not raw:
        return ""
    return re.sub(r"[^A-Z0-9]", "", raw)


def _generate_unit_code_seed(unit_name: str) -> str:
    tokens = re.findall(r"[A-Z0-9]+", str(unit_name or "").upper())
    if not tokens:
        return "UNIT"
    if len(tokens) == 1:
        token = tokens[0]
        if len(token) >= 4:
            return token[:4]
        return token
    acronym = "".join(tok[0] for tok in tokens if tok)
    if len(acronym) >= 2:
        return acronym[:6]
    return (tokens[0] or "UNIT")[:4]


def fetch_purchase_unit_master(unit: str, include_inactive: bool = False):
    """
    Fetch IV unit master rows used in Purchase and Item Master.
    Returns columns: ID, Code, Name, Deactive (best effort).
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Could not connect to {unit} for unit master")
        return None
    try:
        table_name = _resolve_table_name(conn, ["IVUnit", "IvUnit", "IVUnit_Mst", "Unit_Mst"])
        if not table_name:
            return pd.DataFrame(columns=["ID", "Code", "Name", "Deactive"])

        id_col = _resolve_column(conn, table_name, ["ID", "Id", "UnitID", "UnitId"])
        name_col = _resolve_column(conn, table_name, ["Name", "UnitName", "Unit"])
        code_col = _resolve_column(conn, table_name, ["Code", "UnitCode"])
        deactive_col = _resolve_column(conn, table_name, ["Deactive", "DeActive", "IsDeactive", "IsInactive", "Inactive"])
        if not id_col or not name_col:
            return pd.DataFrame(columns=["ID", "Code", "Name", "Deactive"])

        select_cols = [
            f"[{id_col}] AS ID",
            f"[{name_col}] AS Name",
            (f"[{code_col}] AS Code" if code_col else "CAST('' AS NVARCHAR(50)) AS Code"),
            (f"[{deactive_col}] AS Deactive" if deactive_col else "CAST(0 AS INT) AS Deactive"),
        ]
        sql = f"SELECT {', '.join(select_cols)} FROM dbo.[{table_name}]"
        if deactive_col and not include_inactive:
            sql += (
                f" WHERE CASE "
                f"WHEN [{deactive_col}] IS NULL THEN 0 "
                f"WHEN UPPER(LTRIM(RTRIM(CAST([{deactive_col}] AS NVARCHAR(10))))) IN ('1','TRUE','YES','Y') THEN 1 "
                f"ELSE 0 END = 0"
            )
        sql += f" ORDER BY [{name_col}]"

        df = pd.read_sql(sql, conn)
        if df is None or df.empty:
            return df
        df.columns = [str(c).strip() for c in df.columns]
        if "Name" in df.columns:
            df["Name"] = (
                df["Name"]
                .fillna("")
                .astype(str)
                .str.strip()
                .replace({"nan": "", "None": ""})
            )
        if "Code" in df.columns:
            df["Code"] = (
                df["Code"]
                .fillna("")
                .astype(str)
                .str.strip()
                .replace({"nan": "", "None": ""})
            )
        if "Deactive" not in df.columns:
            df["Deactive"] = 0
        return df
    except Exception as e:
        print(f"Error fetching unit master ({unit}): {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def upsert_purchase_unit_master(
    unit: str,
    unit_name: str,
    unit_code: str | None = None,
    unit_id: int | None = None,
    reactivate: bool = True,
):
    """
    Add or update IVUnit row by id/name/code.
    Returns: status/mode/unit_id/name/code or error.
    """
    conn = get_sql_connection(unit)
    if not conn:
        return {"error": f"Could not connect to {unit}"}

    unit_name_clean = _normalize_unit_master_name(unit_name)
    if not unit_name_clean:
        return {"error": "Unit name is required.", "code": "required"}
    unit_code_clean = _sanitize_unit_code(unit_code or "")

    try:
        try:
            unit_id_val = int(unit_id or 0)
        except Exception:
            unit_id_val = 0

        table_name = _resolve_table_name(conn, ["IVUnit", "IvUnit", "IVUnit_Mst", "Unit_Mst"])
        if not table_name:
            return {"error": "IVUnit table not found.", "code": "table_not_found"}

        id_col = _resolve_column(conn, table_name, ["ID", "Id", "UnitID", "UnitId"])
        name_col = _resolve_column(conn, table_name, ["Name", "UnitName", "Unit"])
        code_col = _resolve_column(conn, table_name, ["Code", "UnitCode"])
        deactive_col = _resolve_column(conn, table_name, ["Deactive", "DeActive", "IsDeactive", "IsInactive", "Inactive"])
        if not id_col or not name_col:
            return {"error": "IVUnit columns are missing.", "code": "column_not_found"}

        cursor = conn.cursor()
        id_match_expr = f"LTRIM(RTRIM(CAST([{id_col}] AS NVARCHAR(50))))"

        def _row_to_dict(row_obj, has_code: bool, has_deactive: bool):
            if not row_obj:
                return None
            idx = 0
            out = {
                "id": row_obj[idx] if len(row_obj) > idx else None,
                "name": row_obj[idx + 1] if len(row_obj) > (idx + 1) else None,
            }
            idx = 2
            out["code"] = row_obj[idx] if has_code and len(row_obj) > idx else ""
            idx = idx + (1 if has_code else 0)
            out["deactive"] = row_obj[idx] if has_deactive and len(row_obj) > idx else 0
            return out

        def _is_deactive(raw_val) -> bool:
            text = str(raw_val or "").strip().lower()
            if text in {"true", "1", "y", "yes"}:
                return True
            try:
                return int(raw_val or 0) != 0
            except Exception:
                return False

        def _find_duplicate(exclude_id: int = 0):
            where_parts = [f"UPPER(LTRIM(RTRIM([{name_col}]))) = UPPER(LTRIM(RTRIM(?)))"]
            params = [unit_name_clean]
            if code_col and unit_code_clean:
                where_parts.append(f"UPPER(LTRIM(RTRIM([{code_col}]))) = UPPER(LTRIM(RTRIM(?)))")
                params.append(unit_code_clean)
            where_sql = " OR ".join(where_parts)
            sql = f"""
                SELECT TOP 1
                    [{id_col}] AS ID,
                    [{name_col}] AS Name
                    {f", [{code_col}] AS Code" if code_col else ""}
                    {f", [{deactive_col}] AS Deactive" if deactive_col else ""}
                FROM dbo.[{table_name}]
                WHERE ({where_sql})
            """
            if exclude_id > 0:
                sql += f" AND {id_match_expr} <> ?"
                params.append(str(exclude_id))
            sql += f"""
                ORDER BY
                    CASE WHEN UPPER(LTRIM(RTRIM([{name_col}]))) = UPPER(LTRIM(RTRIM(?))) THEN 0 ELSE 1 END,
                    [{id_col}]
            """
            params.append(unit_name_clean)
            cursor.execute(sql, params)
            return _row_to_dict(cursor.fetchone(), bool(code_col), bool(deactive_col))

        def _code_exists(code_value: str, exclude_id: int = 0) -> bool:
            if not code_col:
                return False
            code_text = _sanitize_unit_code(code_value)
            if not code_text:
                return False
            sql = f"""
                SELECT TOP 1 1
                FROM dbo.[{table_name}]
                WHERE UPPER(LTRIM(RTRIM([{code_col}]))) = UPPER(LTRIM(RTRIM(?)))
            """
            params = [code_text]
            if exclude_id > 0:
                sql += f" AND {id_match_expr} <> ?"
                params.append(str(exclude_id))
            cursor.execute(sql, params)
            return cursor.fetchone() is not None

        def _next_available_code(seed_name: str, preferred_code: str, exclude_id: int = 0) -> str:
            if not code_col:
                return ""
            max_len = None
            limits = _fetch_text_column_limits(conn, table_name, [code_col])
            if limits:
                max_len = limits.get(str(code_col).strip().lower())
            base = _sanitize_unit_code(preferred_code) or _sanitize_unit_code(_generate_unit_code_seed(seed_name)) or "UNIT"
            if max_len:
                base = base[:max_len] or "U"
            if not _code_exists(base, exclude_id=exclude_id):
                return base
            for idx in range(1, 1000):
                suffix = str(idx)
                if max_len:
                    root = base[:max(1, max_len - len(suffix))]
                else:
                    root = base
                candidate = f"{root}{suffix}"
                if not _code_exists(candidate, exclude_id=exclude_id):
                    return candidate
            return base

        def _build_insert_columns(final_code: str, include_fallback_meta: bool = False):
            cols = []
            vals = []
            if code_col:
                cols.append(code_col)
                vals.append(final_code)
            cols.append(name_col)
            vals.append(unit_name_clean)
            if deactive_col:
                cols.append(deactive_col)
                vals.append(0)
            if include_fallback_meta:
                table_cols = _get_table_columns(conn, table_name)
                now_stamp = datetime.now(tz=LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S")
                fallback_defaults = {
                    "updatedby": 0,
                    "updatedon": now_stamp,
                    "updatedmacname": "RID",
                    "updatedmacid": "",
                    "updatedipaddress": "",
                    "insertedbyuserid": 0,
                    "insertedon": now_stamp,
                    "insertedmacname": "RID",
                    "insertedmacid": "",
                    "insertedipaddress": "",
                }
                current_cols = {str(c).strip().lower() for c in cols}
                for col_lc, col_default in fallback_defaults.items():
                    if col_lc in table_cols and col_lc not in current_cols:
                        real_col = _resolve_column(conn, table_name, [col_lc]) or _resolve_column(conn, table_name, [col_lc.upper()]) or _resolve_column(conn, table_name, [col_lc.title()])
                        if not real_col:
                            for candidate in table_cols:
                                if candidate.lower() == col_lc:
                                    real_col = candidate
                                    break
                        if not real_col:
                            continue
                        cols.append(real_col)
                        vals.append(col_default)
            return cols, vals

        if unit_id_val > 0:
            cursor.execute(
                f"""
                SELECT TOP 1
                    [{id_col}] AS ID,
                    [{name_col}] AS Name
                    {f", [{code_col}] AS Code" if code_col else ""}
                    {f", [{deactive_col}] AS Deactive" if deactive_col else ""}
                FROM dbo.[{table_name}]
                WHERE {id_match_expr} = ?
                """,
                (str(unit_id_val),),
            )
            existing = _row_to_dict(cursor.fetchone(), bool(code_col), bool(deactive_col))
            if not existing:
                return {"error": "Unit not found.", "code": "not_found"}

            dup = _find_duplicate(exclude_id=unit_id_val)
            if dup:
                return {
                    "error": "Unit already exists.",
                    "code": "duplicate",
                    "existing_id": int(dup.get("id") or 0) or None,
                }

            final_code = ""
            if code_col:
                existing_code = _sanitize_unit_code(existing.get("code"))
                final_code = _sanitize_unit_code(unit_code_clean) or existing_code
                final_code = _next_available_code(unit_name_clean, final_code, exclude_id=unit_id_val)

            set_parts = [f"[{name_col}] = ?"]
            set_vals = [unit_name_clean]
            if code_col:
                set_parts.append(f"[{code_col}] = ?")
                set_vals.append(final_code)
            if deactive_col and reactivate:
                set_parts.append(f"[{deactive_col}] = ?")
                set_vals.append(0)
            set_sql = ", ".join(set_parts)
            set_vals.append(unit_id_val)
            cursor.execute(
                f"""
                UPDATE dbo.[{table_name}]
                SET {set_sql}
                WHERE {id_match_expr} = ?
                """,
                [*set_vals[:-1], str(set_vals[-1])],
            )
            conn.commit()
            return {
                "status": "success",
                "mode": "update",
                "unit_id": unit_id_val,
                "name": unit_name_clean,
                "code": final_code if code_col else "",
            }

        dup = _find_duplicate(exclude_id=0)
        if dup:
            dup_id = int(dup.get("id") or 0) if dup.get("id") is not None else 0
            dup_code = _sanitize_unit_code(dup.get("code"))
            if deactive_col and reactivate and _is_deactive(dup.get("deactive")) and dup_id > 0:
                set_parts = []
                set_vals = []
                if code_col:
                    final_code = _sanitize_unit_code(unit_code_clean) or dup_code or _next_available_code(unit_name_clean, "", exclude_id=dup_id)
                    if final_code:
                        set_parts.append(f"[{code_col}] = ?")
                        set_vals.append(final_code)
                set_parts.append(f"[{name_col}] = ?")
                set_vals.append(unit_name_clean)
                set_parts.append(f"[{deactive_col}] = ?")
                set_vals.append(0)
                set_vals.append(dup_id)
                cursor.execute(
                    f"""
                    UPDATE dbo.[{table_name}]
                    SET {', '.join(set_parts)}
                    WHERE {id_match_expr} = ?
                    """,
                    [*set_vals[:-1], str(set_vals[-1])],
                )
                conn.commit()
                return {
                    "status": "success",
                    "mode": "reactivate",
                    "unit_id": dup_id,
                    "name": unit_name_clean,
                    "code": final_code if code_col else "",
                }
            return {
                "error": "Unit already exists.",
                "code": "duplicate",
                "existing_id": dup_id or None,
            }

        final_code = ""
        if code_col:
            final_code = _next_available_code(unit_name_clean, unit_code_clean)
        insert_cols, insert_vals = _build_insert_columns(final_code, include_fallback_meta=False)
        quoted_cols = ", ".join(f"[{c}]" for c in insert_cols)
        placeholders = ", ".join("?" for _ in insert_vals)
        insert_sql = f"INSERT INTO dbo.[{table_name}] ({quoted_cols}) VALUES ({placeholders})"
        try:
            cursor.execute(insert_sql, insert_vals)
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            insert_cols, insert_vals = _build_insert_columns(final_code, include_fallback_meta=True)
            quoted_cols = ", ".join(f"[{c}]" for c in insert_cols)
            placeholders = ", ".join("?" for _ in insert_vals)
            insert_sql = f"INSERT INTO dbo.[{table_name}] ({quoted_cols}) VALUES ({placeholders})"
            cursor.execute(insert_sql, insert_vals)

        cursor.execute("SELECT CAST(SCOPE_IDENTITY() AS INT)")
        row = cursor.fetchone()
        conn.commit()
        new_id = int(row[0]) if row and row[0] is not None else 0
        return {
            "status": "success",
            "mode": "add",
            "unit_id": new_id,
            "name": unit_name_clean,
            "code": final_code if code_col else "",
        }
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        return {"error": str(e)}
    finally:
        try:
            conn.close()
        except Exception:
            pass


def set_purchase_unit_master_active(unit: str, unit_id: int, is_active: bool):
    """
    Toggle unit active state through IVUnit.Deactive (best effort).
    """
    conn = get_sql_connection(unit)
    if not conn:
        return {"error": f"Could not connect to {unit}"}
    try:
        try:
            unit_id_val = int(unit_id or 0)
        except Exception:
            unit_id_val = 0
        if unit_id_val <= 0:
            return {"error": "Invalid unit id.", "code": "invalid_id"}

        table_name = _resolve_table_name(conn, ["IVUnit", "IvUnit", "IVUnit_Mst", "Unit_Mst"])
        if not table_name:
            return {"error": "IVUnit table not found.", "code": "table_not_found"}
        id_col = _resolve_column(conn, table_name, ["ID", "Id", "UnitID", "UnitId"])
        deactive_col = _resolve_column(conn, table_name, ["Deactive", "DeActive", "IsDeactive", "IsInactive", "Inactive"])
        if not id_col:
            return {"error": "Unit id column not found.", "code": "column_not_found"}
        if not deactive_col:
            return {"error": "Deactive column not found in IVUnit.", "code": "unsupported"}

        cursor = conn.cursor()
        id_match_expr = f"LTRIM(RTRIM(CAST([{id_col}] AS NVARCHAR(50))))"
        cursor.execute(
            f"SELECT TOP 1 [{id_col}] FROM dbo.[{table_name}] WHERE {id_match_expr} = ?",
            (str(unit_id_val),),
        )
        if cursor.fetchone() is None:
            return {"error": "Unit not found.", "code": "not_found"}

        cursor.execute(
            f"""
            UPDATE dbo.[{table_name}]
            SET [{deactive_col}] = ?
            WHERE {id_match_expr} = ?
            """,
            (0 if is_active else 1, str(unit_id_val)),
        )
        conn.commit()
        return {"status": "success", "unit_id": unit_id_val, "is_active": 1 if is_active else 0}
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        return {"error": str(e)}
    finally:
        try:
            conn.close()
        except Exception:
            pass


def fetch_item_units(unit: str, include_inactive: bool = False):
    """
    Fetch IV unit master for item master entry.
    """
    df = fetch_purchase_unit_master(unit, include_inactive=include_inactive)
    if df is None or df.empty:
        return df
    df = df.copy()
    if "Deactive" in df.columns:
        df = df.drop(columns=["Deactive"])
    return df


def fetch_item_locations(unit: str):
    """
    Fetch IV item locations for item master entry.
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Could not connect to {unit} for item locations")
        return None
    try:
        df = _fetch_master_table(
            conn,
            ["IVItemLocation", "IvItemLocation", "IVItemLocation_Mst", "ItemLocation_Mst"],
            ["ID", "Id", "LocationID", "LocationId"],
            ["Name", "LocationName", "Location"],
            ["Code", "LocationCode"],
        )
        if df is None or df.empty:
            return df
        df.columns = [c.strip() for c in df.columns]
        return df
    except Exception as e:
        print(f"Error fetching item locations ({unit}): {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def fetch_item_groups(unit: str):
    """
    Fetch IV item groups for item master entry.
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Could not connect to {unit} for item groups")
        return None
    try:
        df = _fetch_master_table(
            conn,
            ["IVItemGroup", "IvItemGroup", "IVItemGroup_Mst", "ItemGroup_Mst"],
            ["ID", "Id", "ItemGroupID", "ItemGroupId"],
            ["Name", "GroupName", "ItemGroupName"],
            ["Code", "GroupCode", "ItemGroupCode"],
        )
        if df is None or df.empty:
            return df
        df.columns = [c.strip() for c in df.columns]
        return df
    except Exception as e:
        print(f"Error fetching item groups ({unit}): {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def fetch_item_groups_by_type(unit: str, type_id: int):
    """
    Calls dbo.Usp_GetIvGroupTypewise to fetch item groups for a type.
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Could not connect to {unit} for item groups")
        return None
    try:
        df = pd.read_sql("EXEC dbo.Usp_GetIvGroupTypewise ?", conn, params=[int(type_id)])
        if df is None or df.empty:
            return df
        df.columns = [c.strip() for c in df.columns]
        return df
    except Exception as e:
        print(f"Error fetching item groups ({unit}): {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def fetch_item_subgroups(unit: str, group_id: int | None = None):
    """
    Fetch IV item subgroups for item master entry.
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Could not connect to {unit} for item subgroups")
        return None
    try:
        table_name = _resolve_table_name(
            conn,
            ["IVItemSubGroup", "IvItemSubGroup", "IVItemSubGroup_Mst", "ItemSubGroup_Mst"],
        )
        if not table_name:
            return pd.DataFrame()
        id_col = _resolve_column(conn, table_name, ["ID", "Id", "SubGroupID", "SubGroupId"])
        name_col = _resolve_column(conn, table_name, ["Name", "SubGroupName", "ItemSubGroupName"])
        code_col = _resolve_column(conn, table_name, ["Code", "SubGroupCode", "ItemSubGroupCode"])
        group_col = _resolve_column(conn, table_name, ["ItemGroupID", "ItemGroupId", "GroupID", "GroupId"])
        if not id_col or not name_col:
            return pd.DataFrame()
        select_cols = [f"{id_col} AS ID", f"{name_col} AS Name"]
        if code_col:
            select_cols.append(f"{code_col} AS Code")
        if group_col:
            select_cols.append(f"{group_col} AS GroupID")
        sql = f"SELECT {', '.join(select_cols)} FROM dbo.{table_name}"
        params = None
        if group_id and group_col:
            sql += f" WHERE {group_col} = ?"
            params = [int(group_id)]
        df = pd.read_sql(sql, conn, params=params)
        if df is None or df.empty:
            return df
        df.columns = [c.strip() for c in df.columns]
        return df
    except Exception as e:
        print(f"Error fetching item subgroups ({unit}): {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def fetch_item_subgroups_by_group(unit: str, group_id: int):
    """
    Calls dbo.Usp_GetIvsubGroup to fetch item subgroups for a group.
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Could not connect to {unit} for item subgroups")
        return None
    try:
        df = pd.read_sql("EXEC dbo.Usp_GetIvsubGroup ?", conn, params=[int(group_id)])
        if df is None or df.empty:
            return df
        df.columns = [c.strip() for c in df.columns]
        return df
    except Exception as e:
        print(f"Error fetching item subgroups ({unit}): {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def fetch_store_stock_summary(unit: str, store_ids=None):
    """
    Fetch current stock summary by item from dbo.StoreStock for specific stores.
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Could not connect to {unit} for store stock summary")
        return None
    try:
        store_ids = store_ids or [2, 3, 15]
        store_ids = [int(x) for x in store_ids]
        store_list = ",".join(str(x) for x in store_ids)
        sql = f"""
            DECLARE @itemCol sysname;
            DECLARE @qtyCol sysname;
            DECLARE @storeCol sysname;

            SELECT TOP 1 @itemCol = name
            FROM sys.columns
            WHERE object_id = OBJECT_ID('dbo.StoreStock')
              AND name IN ('ItemId','ItemID','Item_Id','Item_id');

            SELECT TOP 1 @qtyCol = name
            FROM sys.columns
            WHERE object_id = OBJECT_ID('dbo.StoreStock')
              AND name IN ('CurrentStock','ClosingStock','Qty','Stock','BalanceQty','Balance','OnHand','OnHandQty');

            SELECT TOP 1 @storeCol = name
            FROM sys.columns
            WHERE object_id = OBJECT_ID('dbo.StoreStock')
              AND name IN ('StoreId','StoreID','Store_Id','Store_id');

            IF @itemCol IS NULL OR @qtyCol IS NULL OR @storeCol IS NULL
            BEGIN
                SELECT CAST(NULL AS INT) AS ItemId, CAST(0 AS FLOAT) AS CurrentStock WHERE 1=0;
            END
            ELSE
            BEGIN
                DECLARE @sql NVARCHAR(MAX) =
                    N'SELECT ' + QUOTENAME(@itemCol) + ' AS ItemId, ' +
                    N'SUM(ISNULL(' + QUOTENAME(@qtyCol) + ',0)) AS CurrentStock ' +
                    N'FROM dbo.StoreStock WHERE ' + QUOTENAME(@storeCol) + ' IN ({store_list}) ' +
                    N'GROUP BY ' + QUOTENAME(@itemCol);
                EXEC sp_executesql @sql;
            END
        """
        df = pd.read_sql(sql, conn)
        if df is None or df.empty:
            return df
        df.columns = [c.strip() for c in df.columns]
        return df
    except Exception as e:
        print(f"Error fetching store stock summary ({unit}): {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def fetch_last_30_day_item_consumption(unit: str):
    """
    Fetch total item consumption for the last 30 days using dbo.usp_GetLatestItemConsumptionLast30Days.
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Could not connect to {unit} for last 30 day consumption")
        return None
    try:
        df = pd.read_sql("EXEC dbo.usp_GetLatestItemConsumptionLast30Days", conn)
        if df is None or df.empty:
            return df
        df.columns = [c.strip() for c in df.columns]
        return df
    except Exception as e:
        print(f"Error fetching last 30 day consumption ({unit}): {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def fetch_purchase_po_header(unit: str, po_id: int | None = None, po_no: str | None = None):
    """
    Fetch PO header from IVPoMst with supplier details.
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Could not connect to {unit} for PO header")
        return None
    try:
        po_id = int(po_id) if po_id is not None else -1
        po_no = (po_no or "").strip()
        supplier_table = _resolve_table_name(conn, ["IvSupplier", "IVSupplier", "IVSUPPLIER"]) or "IvSupplier"
        supplier_id_col = _resolve_column(conn, supplier_table, ["ID", "Id", "SupplierID", "SupplierId"]) or "ID"
        supplier_name_col = _resolve_column(conn, supplier_table, ["Name", "SupplierName"])
        supplier_code_col = _resolve_column(conn, supplier_table, ["Code", "SupplierCode"])
        gst_col = _resolve_column(conn, supplier_table, ["GSTIN", "GSTNo", "GSTINNo", "GST_No", "GSTIN_No", "GSTNumber", "GSTN"])
        excise_col = _resolve_column(conn, supplier_table, ["ExciseCode", "Excise_Code", "Excise Code"])
        address1_col = _resolve_column(conn, supplier_table, ["Address", "Address1", "Address_1", "AddressLine1", "Addr1", "AddressLine"])
        address2_col = _resolve_column(conn, supplier_table, ["Address2", "Address_2", "AddressLine2", "Addr2"])
        city_col = _resolve_column(conn, supplier_table, ["CityName", "City_Name", "City", "CityDesc", "CityDescription"])
        state_col = _resolve_column(conn, supplier_table, ["StateName", "State_Name", "State", "StateDesc", "StateDescription"])
        pin_col = _resolve_column(conn, supplier_table, ["Pin", "Pincode", "PinCode", "ZIP", "Zip", "PostalCode"])
        country_col = _resolve_column(conn, supplier_table, ["CountryName", "Country_Name", "Country"])
        email_col = _resolve_column(conn, supplier_table, ["Email", "EMail", "EMailId", "EmailID", "E_Mail"])
        if gst_col and excise_col:
            gst_select = (
                f"COALESCE(NULLIF(LTRIM(RTRIM(sup.[{gst_col}])), ''), "
                f"NULLIF(LTRIM(RTRIM(sup.[{excise_col}])), '')) AS SupplierGSTIN"
            )
        elif gst_col:
            gst_select = f"sup.[{gst_col}] AS SupplierGSTIN"
        elif excise_col:
            gst_select = f"sup.[{excise_col}] AS SupplierGSTIN"
        else:
            gst_select = "CAST(NULL AS NVARCHAR(50)) AS SupplierGSTIN"

        supplier_name_select = f"sup.[{supplier_name_col}] AS SupplierName" if supplier_name_col else "CAST(NULL AS NVARCHAR(255)) AS SupplierName"
        supplier_code_select = f"sup.[{supplier_code_col}] AS SupplierCode" if supplier_code_col else "CAST(NULL AS NVARCHAR(120)) AS SupplierCode"
        address1_select = f"sup.[{address1_col}] AS SupplierAddress1" if address1_col else "CAST(NULL AS NVARCHAR(255)) AS SupplierAddress1"
        address2_select = f"sup.[{address2_col}] AS SupplierAddress2" if address2_col else "CAST(NULL AS NVARCHAR(255)) AS SupplierAddress2"
        city_select = f"sup.[{city_col}] AS SupplierCity" if city_col else "CAST(NULL AS NVARCHAR(100)) AS SupplierCity"
        state_select = f"sup.[{state_col}] AS SupplierState" if state_col else "CAST(NULL AS NVARCHAR(100)) AS SupplierState"
        pin_select = f"sup.[{pin_col}] AS SupplierPin" if pin_col else "CAST(NULL AS NVARCHAR(30)) AS SupplierPin"
        country_select = f"sup.[{country_col}] AS SupplierCountry" if country_col else "CAST(NULL AS NVARCHAR(100)) AS SupplierCountry"
        email_select = f"sup.[{email_col}] AS SupplierEmail" if email_col else "CAST(NULL AS NVARCHAR(255)) AS SupplierEmail"

        mst_cols = []
        try:
            mst_cols_df = pd.read_sql(
                "SELECT name FROM sys.columns WHERE object_id = OBJECT_ID('dbo.IVPoMst')",
                conn,
            )
            if mst_cols_df is not None and not mst_cols_df.empty:
                mst_cols = [str(c).strip() for c in mst_cols_df["name"].tolist()]
        except Exception:
            mst_cols = []

        def pick_col(candidates):
            for cand in candidates:
                for col in mst_cols:
                    if col.lower() == cand.lower():
                        return col
            return None

        created_by_col = pick_col(["InsertedByUserID", "InsertedBy", "CreatedBy"])
        created_on_col = pick_col(["InsertedON", "InsertedOn", "CreatedOn", "CreatedAt"])
        updated_on_col = pick_col(["UpdatedOn", "UpdatedAt"])
        custom1_col = pick_col(["Custom1"])
        custom2_col = pick_col(["Custom2"])
        purchasing_dept_col = pick_col(["PurchasingDeptId", "PurchasingDeptID", "PurchaseDeptId", "PurchaseDeptID"])
        special_notes_col = pick_col(["SpecialNotes", "SpecialNote"])
        senior_approval_authority_col = pick_col(["SeniorApprovalAuthorityName", "SeniorApprovalAuthority"])
        senior_approval_designation_col = pick_col(["SeniorApprovalAuthorityDesignation", "SeniorApprovalDesignation"])
        approver_name_col = pick_col(["ApproverName", "ApprovedByName", "ApprovingName"])
        approver_degree_col = pick_col(["ApproverDegree"])
        approver_designation_col = pick_col(["ApproverDesignation"])
        approver_phone_col = pick_col(["ApproverPhone"])
        approver_signature_col = pick_col(["ApproverSignatureFile", "ApproverSignature"])

        created_by_select = f"mst.[{created_by_col}] AS CreatedBy" if created_by_col else "CAST(NULL AS NVARCHAR(100)) AS CreatedBy"
        created_on_select = f"mst.[{created_on_col}] AS CreatedOn" if created_on_col else "CAST(NULL AS DATETIME) AS CreatedOn"
        updated_on_select = f"mst.[{updated_on_col}] AS UpdatedOn" if updated_on_col else "CAST(NULL AS DATETIME) AS UpdatedOn"
        custom1_select = f"mst.[{custom1_col}] AS Custom1" if custom1_col else "CAST(NULL AS NVARCHAR(100)) AS Custom1"
        custom2_select = f"mst.[{custom2_col}] AS Custom2" if custom2_col else "CAST(NULL AS NVARCHAR(100)) AS Custom2"
        purchasing_dept_select = (
            f"mst.[{purchasing_dept_col}] AS PurchasingDeptId"
            if purchasing_dept_col
            else "CAST(NULL AS INT) AS PurchasingDeptId"
        )
        special_notes_select = (
            f"mst.[{special_notes_col}] AS SpecialNotes"
            if special_notes_col
            else "CAST(NULL AS NVARCHAR(1000)) AS SpecialNotes"
        )
        senior_approval_authority_select = (
            f"mst.[{senior_approval_authority_col}] AS SeniorApprovalAuthorityName"
            if senior_approval_authority_col
            else "CAST(NULL AS NVARCHAR(160)) AS SeniorApprovalAuthorityName"
        )
        senior_approval_designation_select = (
            f"mst.[{senior_approval_designation_col}] AS SeniorApprovalAuthorityDesignation"
            if senior_approval_designation_col
            else "CAST(NULL AS NVARCHAR(120)) AS SeniorApprovalAuthorityDesignation"
        )
        approver_name_select = (
            f"mst.[{approver_name_col}] AS ApproverName"
            if approver_name_col
            else "CAST(NULL AS NVARCHAR(160)) AS ApproverName"
        )
        approver_degree_select = (
            f"mst.[{approver_degree_col}] AS ApproverDegree"
            if approver_degree_col
            else "CAST(NULL AS NVARCHAR(160)) AS ApproverDegree"
        )
        approver_designation_select = (
            f"mst.[{approver_designation_col}] AS ApproverDesignation"
            if approver_designation_col
            else "CAST(NULL AS NVARCHAR(160)) AS ApproverDesignation"
        )
        approver_phone_select = (
            f"mst.[{approver_phone_col}] AS ApproverPhone"
            if approver_phone_col
            else "CAST(NULL AS NVARCHAR(40)) AS ApproverPhone"
        )
        approver_signature_select = (
            f"mst.[{approver_signature_col}] AS ApproverSignatureFile"
            if approver_signature_col
            else "CAST(NULL AS NVARCHAR(260)) AS ApproverSignatureFile"
        )
        sql = """
            SET NOCOUNT ON;
            SELECT TOP 1
                mst.ID,
                mst.PONo,
                mst.PODate,
                mst.SupplierID,
                mst.RefNo,
                mst.Subject,
                mst.CreditDays,
                mst.Notes,
                {special_notes_select},
                {senior_approval_authority_select},
                {senior_approval_designation_select},
                mst.Preparedby,
                mst.DeliveryTerms,
                mst.PaymentsTerms,
                mst.OtherTerms,
                mst.Against,
                mst.AgainstId,
                mst.PurchaseIndentId,
                mst.TotalFORe,
                mst.TotalExciseAmt,
                mst.Tax,
                mst.Discount,
                mst.Amount,
                mst.Status,
                {created_by_select},
                {created_on_select},
                {updated_on_select},
                {custom1_select},
                {custom2_select},
                {purchasing_dept_select},
                {approver_name_select},
                {approver_degree_select},
                {approver_designation_select},
                {approver_phone_select},
                {approver_signature_select},
                {supplier_name_select},
                {supplier_code_select},
                {email_select},
                {gst_select},
                {address1_select},
                {address2_select},
                {city_select},
                {state_select},
                {pin_select},
                {country_select}
            FROM dbo.IVPoMst AS mst
            LEFT JOIN dbo.[{supplier_table}] AS sup
                ON mst.SupplierID = sup.[{supplier_id_col}]
            WHERE mst.ID = ? OR mst.PONo = ?
            ORDER BY mst.ID DESC
        """
        df = pd.read_sql(
            sql.format(
                gst_select=gst_select,
                supplier_name_select=supplier_name_select,
                supplier_code_select=supplier_code_select,
                created_by_select=created_by_select,
                created_on_select=created_on_select,
                updated_on_select=updated_on_select,
                custom1_select=custom1_select,
                custom2_select=custom2_select,
                purchasing_dept_select=purchasing_dept_select,
                approver_name_select=approver_name_select,
                approver_degree_select=approver_degree_select,
                approver_designation_select=approver_designation_select,
                approver_phone_select=approver_phone_select,
                approver_signature_select=approver_signature_select,
                special_notes_select=special_notes_select,
                senior_approval_authority_select=senior_approval_authority_select,
                senior_approval_designation_select=senior_approval_designation_select,
                email_select=email_select,
                address1_select=address1_select,
                address2_select=address2_select,
                city_select=city_select,
                state_select=state_select,
                pin_select=pin_select,
                country_select=country_select,
                supplier_table=supplier_table,
                supplier_id_col=supplier_id_col,
            ),
            conn,
            params=[po_id, po_no],
        )
        if df is None or df.empty:
            return df
        df.columns = [c.strip() for c in df.columns]
        return df
    except Exception as e:
        print(f"Error fetching PO header ({unit}): {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def fetch_purchase_po_items(unit: str, po_id: int):
    """
    Fetch PO item details from IVPoDtl with item metadata.
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Could not connect to {unit} for PO items")
        return None
    try:
        unit_key = (unit or "").strip().upper()
        prev_rate_units = {"ACI", "AHL", "CANCER UNIT STORE", "AHL STORE"}
        include_prev_rate = unit_key in prev_rate_units
        if not include_prev_rate and unit_key:
            if "CANCER" in unit_key or "AHL" in unit_key:
                include_prev_rate = True

        if include_prev_rate:
            try:
                fn_df = pd.read_sql("SELECT OBJECT_ID('dbo.fn_prevrate') AS fn_id", conn)
                fn_id = None
                if fn_df is not None and not fn_df.empty:
                    fn_id = fn_df.iloc[0].get("fn_id")
                include_prev_rate = bool(fn_id)
            except Exception:
                include_prev_rate = False

        rate_select = "dtl.Rate,"
        if include_prev_rate:
            rate_select = "dtl.Rate,\n                dbo.fn_prevrate(dtl.ItemID) AS PrevRate,"
        item_spec_col = _resolve_column(conn, "IVItem", ["TechnicalSpecs", "TechnicalSpec", "TechSpecs", "TechSpec"])
        if not item_spec_col:
            # Best effort for alternate table-name casing.
            item_table_name = _resolve_table_name(conn, ["IVItem", "IvItem", "IVITEM"]) or "IVItem"
            item_spec_col = _resolve_column(conn, item_table_name, ["TechnicalSpecs", "TechnicalSpec", "TechSpecs", "TechSpec"])
        technical_specs_select = (
            f"itm.[{item_spec_col}] AS TechnicalSpecs"
            if item_spec_col
            else "CAST(NULL AS NVARCHAR(MAX)) AS TechnicalSpecs"
        )
        # Manufacturer link (best-effort): pick latest linked manufacturer for each item.
        manufacturer_select = "CAST(NULL AS NVARCHAR(200)) AS ManufacturerName"
        manufacturer_join = ""
        manu_link_table = _resolve_table_name(conn, ["IVItemManuLink", "IvItemManuLink", "IVITEMMANULINK"])
        manu_table = _resolve_table_name(conn, ["IVMANUFACTURER", "IvManufacturer", "IVManufacturer"])
        if manu_link_table and manu_table:
            manu_link_item_col = _resolve_column(conn, manu_link_table, ["ItemID", "ItemId"])
            manu_link_manu_col = _resolve_column(conn, manu_link_table, ["ID", "ManufacturerID", "ManuID"])
            manu_link_pk_col = _resolve_column(conn, manu_link_table, ["LinkId", "ID", "Id"])
            manu_id_col = _resolve_column(conn, manu_table, ["ID", "Id"])
            manu_name_col = _resolve_column(conn, manu_table, ["Name", "ManufacturerName", "Manufacturer"])
            if manu_link_item_col and manu_link_manu_col and manu_id_col and manu_name_col:
                order_expr = (
                    f"L.[{manu_link_pk_col}] DESC"
                    if manu_link_pk_col
                    else f"M.[{manu_name_col}] ASC"
                )
                manufacturer_select = "manu.ManufacturerName AS ManufacturerName"
                manufacturer_join = f"""
                OUTER APPLY (
                    SELECT TOP 1
                        M.[{manu_name_col}] AS ManufacturerName
                    FROM dbo.[{manu_link_table}] AS L
                    LEFT JOIN dbo.[{manu_table}] AS M
                        ON L.[{manu_link_manu_col}] = M.[{manu_id_col}]
                    WHERE L.[{manu_link_item_col}] = dtl.ItemID
                    ORDER BY {order_expr}
                ) AS manu
                """
        sql = f"""
            SET NOCOUNT ON;
            SELECT
                dtl.ID AS DetailID,
                dtl.POID,
                dtl.ItemID,
                dtl.Qty,
                dtl.PackSizeId,
                pks.Name AS PackSizeName,
                {rate_select}
                dtl.FreeQty,
                dtl.Discount,
                dtl.Tax,
                dtl.TaxAmount,
                dtl.MRP,
                dtl.Fore,
                dtl.Excisetax,
                dtl.ExciseTaxamt,
                dtl.NetAmount,
                dtl.Custom1,
                dtl.Custom2,
                itm.Name AS ItemName,
                itm.Code AS ItemCode,
                {manufacturer_select},
                {technical_specs_select},
                un.Name AS UnitName,
                loc.Name AS StoreName
            FROM dbo.IVPoDtl AS dtl
            LEFT JOIN dbo.IVItem AS itm
                ON dtl.ItemID = itm.ID
            LEFT JOIN dbo.IVUnit AS un
                ON itm.UnitID = un.ID
            LEFT JOIN dbo.IVItemLocation AS loc
                ON itm.LocationID = loc.ID
            LEFT JOIN dbo.ivpacksize_mst AS pks
                ON dtl.PackSizeId = pks.Id
            {manufacturer_join}
            WHERE dtl.POID = ?
            ORDER BY dtl.ID
        """
        df = pd.read_sql(sql, conn, params=[int(po_id)])
        if df is None or df.empty:
            return df
        df.columns = [c.strip() for c in df.columns]
        return df
    except Exception as e:
        print(f"Error fetching PO items ({unit}): {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def fetch_payment_mode_change_updates(unit: str, from_date: str, to_date: str):
    """
    Fetch modified receipt payment mode updates (Tag='U') and resolve:
    - paymentModeId -> PaymentMode_Mst
    - updatedBy / approvedBy -> User_Mst
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Could not connect to {unit} for payment mode change updates")
        return None

    def _pick_col(cols_map: dict, candidates: list[str]):
        for cand in candidates:
            key = cand.strip().lower()
            if key in cols_map:
                return cols_map[key]
        return None

    def _first_existing_table(candidates: list[str]):
        for table_name in candidates:
            try:
                pd.read_sql(f"SELECT TOP 0 * FROM {table_name}", conn)
                return table_name
            except Exception:
                continue
        return None

    def _table_cols_map(table_name: str) -> dict:
        if not table_name:
            return {}
        try:
            preview_df = pd.read_sql(f"SELECT TOP 0 * FROM {table_name}", conn)
            return {str(c).strip().lower(): str(c).strip() for c in preview_df.columns}
        except Exception:
            return {}

    try:
        pmcu_table = _first_existing_table([
            "dbo.PaymentModeChageUpdate",
            "dbo.PaymentModeChangeUpdate",
        ])
        if not pmcu_table:
            print(f"Payment mode change update table not found in {unit}")
            return pd.DataFrame()

        pm_table = _first_existing_table([
            "dbo.PaymentMode_Mst",
            "dbo.Paymentmode_mst",
            "dbo.PaymentModeMst",
        ])
        visit_table = _first_existing_table([
            "dbo.Visit",
            "dbo.visit",
        ])
        user_table = _first_existing_table([
            "dbo.User_Mst",
            "dbo.user_mst",
        ])

        pmcu_cols = _table_cols_map(pmcu_table)
        pm_cols = _table_cols_map(pm_table) if pm_table else {}
        visit_cols = _table_cols_map(visit_table) if visit_table else {}
        user_cols = _table_cols_map(user_table) if user_table else {}

        id_col = _pick_col(pmcu_cols, ["Id"])
        visit_id_col = _pick_col(pmcu_cols, ["visitId", "Visit_ID", "VisitId"])
        payment_mode_id_col = _pick_col(pmcu_cols, ["paymentModeId", "PaymentModeID", "PModeID"])
        updated_by_col = _pick_col(pmcu_cols, ["updatedBy", "UpdatedBy", "Updated_By"])
        updated_on_col = _pick_col(pmcu_cols, ["updatedOn", "UpdatedOn", "Updated_On"])
        tag_col = _pick_col(pmcu_cols, ["tag", "Tag"])
        approval_col = _pick_col(pmcu_cols, ["approval", "Approval"])
        approved_by_col = _pick_col(pmcu_cols, ["approvedBy", "ApprovedBy", "Approved_By"])
        approved_date_col = _pick_col(pmcu_cols, ["approvedDate", "ApprovedDate", "Approved_Date"])
        receipt_id_col = _pick_col(pmcu_cols, ["receiptId", "ReceiptID", "ReceiptId"])

        required = [id_col, payment_mode_id_col, updated_by_col, updated_on_col, tag_col]
        if any(not col for col in required):
            print(f"Required PaymentModeChageUpdate columns missing in {unit}")
            return None

        pm_id_col = _pick_col(pm_cols, ["PModeID", "PaymentModeID", "Id"]) if pm_cols else None
        pm_name_col = _pick_col(pm_cols, ["PModeName", "PaymentMode", "ModeName", "PaymentModeName"]) if pm_cols else None
        visit_pk_col = _pick_col(visit_cols, ["Visit_ID", "VisitID", "VisitId", "visitId", "Id"]) if visit_cols else None
        visit_patient_id_col = _pick_col(visit_cols, ["PatientID", "PatientId", "patientId"]) if visit_cols else None
        user_id_col = _pick_col(user_cols, ["UserId", "Userid", "User_ID", "Id"]) if user_cols else None
        user_name_col = _pick_col(user_cols, ["UserName", "User_Name", "Username", "Name", "EmpName"]) if user_cols else None

        joins = []
        patient_id_expr = "CAST(NULL AS INT)"
        patient_name_expr = "CAST(NULL AS NVARCHAR(300))"
        reg_no_expr = "CAST(NULL AS NVARCHAR(100))"
        original_payment_mode_id_expr = "CAST(NULL AS INT)"
        original_payment_mode_expr = "CAST(NULL AS NVARCHAR(200))"
        payment_mode_expr = f"CONVERT(NVARCHAR(100), pmcu.[{payment_mode_id_col}])"
        if visit_table and visit_pk_col and visit_id_col:
            joins.append(f"LEFT JOIN {visit_table} v WITH (NOLOCK) ON v.[{visit_pk_col}] = pmcu.[{visit_id_col}]")
            if visit_patient_id_col:
                patient_id_expr = f"v.[{visit_patient_id_col}]"
                patient_name_expr = f"LTRIM(RTRIM(ISNULL(dbo.fn_patientfullname(v.[{visit_patient_id_col}]), '')))"
                reg_no_expr = f"LTRIM(RTRIM(ISNULL(dbo.fn_regno(v.[{visit_patient_id_col}]), '')))"

        if receipt_id_col:
            joins.append(
                f"""
                OUTER APPLY (
                    SELECT TOP 1
                        src.[{payment_mode_id_col}] AS OriginalPaymentModeId
                    FROM {pmcu_table} src WITH (NOLOCK)
                    WHERE src.[{receipt_id_col}] = pmcu.[{receipt_id_col}]
                    ORDER BY
                        CASE
                            WHEN UPPER(LTRIM(RTRIM(ISNULL(CONVERT(NVARCHAR(10), src.[{tag_col}]), '')))) = 'N' THEN 0
                            ELSE 1
                        END,
                        src.[{id_col}] ASC
                ) orig
                """
            )
            original_payment_mode_id_expr = "orig.OriginalPaymentModeId"
            original_payment_mode_expr = "CONVERT(NVARCHAR(100), orig.OriginalPaymentModeId)"

        if pm_table and pm_id_col and pm_name_col:
            joins.append(f"LEFT JOIN {pm_table} pm WITH (NOLOCK) ON pm.[{pm_id_col}] = pmcu.[{payment_mode_id_col}]")
            payment_mode_expr = (
                f"LTRIM(RTRIM(ISNULL(CONVERT(NVARCHAR(200), pm.[{pm_name_col}]), "
                f"CONVERT(NVARCHAR(100), pmcu.[{payment_mode_id_col}]))))"
            )
            if receipt_id_col:
                joins.append(f"LEFT JOIN {pm_table} pm_orig WITH (NOLOCK) ON pm_orig.[{pm_id_col}] = orig.OriginalPaymentModeId")
                original_payment_mode_expr = (
                    f"LTRIM(RTRIM(ISNULL(CONVERT(NVARCHAR(200), pm_orig.[{pm_name_col}]), "
                    f"CONVERT(NVARCHAR(100), orig.OriginalPaymentModeId))))"
                )

        updated_by_name_expr = f"CONVERT(NVARCHAR(100), pmcu.[{updated_by_col}])"
        approved_by_name_expr = "CAST(NULL AS NVARCHAR(200))"
        if user_table and user_id_col and user_name_col:
            joins.append(f"LEFT JOIN {user_table} u_upd WITH (NOLOCK) ON u_upd.[{user_id_col}] = pmcu.[{updated_by_col}]")
            updated_by_name_expr = (
                f"LTRIM(RTRIM(ISNULL(CONVERT(NVARCHAR(200), u_upd.[{user_name_col}]), "
                f"CONVERT(NVARCHAR(100), pmcu.[{updated_by_col}]))))"
            )
            if approved_by_col:
                joins.append(
                    f"LEFT JOIN {user_table} u_apr WITH (NOLOCK) ON u_apr.[{user_id_col}] = pmcu.[{approved_by_col}]"
                )
                approved_by_name_expr = (
                    f"LTRIM(RTRIM(ISNULL(CONVERT(NVARCHAR(200), u_apr.[{user_name_col}]), "
                    f"CONVERT(NVARCHAR(100), pmcu.[{approved_by_col}]))))"
                )

        visit_id_expr = f"pmcu.[{visit_id_col}]" if visit_id_col else "CAST(NULL AS INT)"
        receipt_id_expr = f"pmcu.[{receipt_id_col}]" if receipt_id_col else "CAST(NULL AS INT)"
        approval_expr = f"pmcu.[{approval_col}]" if approval_col else "CAST(NULL AS NVARCHAR(100))"
        approved_by_expr = f"pmcu.[{approved_by_col}]" if approved_by_col else "CAST(NULL AS INT)"
        approved_date_expr = f"pmcu.[{approved_date_col}]" if approved_date_col else "CAST(NULL AS DATETIME)"

        sql = f"""
            SELECT
                pmcu.[{id_col}] AS Id,
                {visit_id_expr} AS VisitId,
                {patient_id_expr} AS PatientId,
                {patient_name_expr} AS Patient,
                {reg_no_expr} AS RegNo,
                {receipt_id_expr} AS ReceiptId,
                {original_payment_mode_id_expr} AS OriginalPaymentModeId,
                {original_payment_mode_expr} AS OriginalPaymentMode,
                pmcu.[{payment_mode_id_col}] AS PaymentModeId,
                {payment_mode_expr} AS PaymentMode,
                pmcu.[{updated_by_col}] AS UpdatedBy,
                {updated_by_name_expr} AS UpdatedByName,
                pmcu.[{updated_on_col}] AS UpdatedOn,
                pmcu.[{tag_col}] AS Tag,
                {approval_expr} AS Approval,
                {approved_by_expr} AS ApprovedBy,
                {approved_by_name_expr} AS ApprovedByName,
                {approved_date_expr} AS ApprovedDate
            FROM {pmcu_table} pmcu WITH (NOLOCK)
            {' '.join(joins)}
            WHERE UPPER(LTRIM(RTRIM(ISNULL(CONVERT(NVARCHAR(10), pmcu.[{tag_col}]), '')))) = 'U'
              AND CONVERT(VARCHAR(10), pmcu.[{updated_on_col}], 120)
                    BETWEEN CONVERT(VARCHAR(10), ?, 120)
                        AND CONVERT(VARCHAR(10), ?, 120)
            ORDER BY pmcu.[{updated_on_col}] DESC, pmcu.[{id_col}] DESC
        """
        df = pd.read_sql(sql, conn, params=[from_date, to_date])
        if df is None or df.empty:
            return df
        df.columns = [c.strip() for c in df.columns]
        df["Unit"] = (unit or "").upper()
        return df
    except Exception as e:
        print(f"Error fetching payment mode change updates ({unit}): {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def fetch_purchase_po_valuation_rows(unit: str, from_date: str):
    """
    Fetch PO detail rows with store mapping for valuation reporting.
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Could not connect to {unit} for PO valuation")
        return None
    try:
        dtl_cols = []
        mst_cols = []
        try:
            dtl_cols_df = pd.read_sql(
                "SELECT name FROM sys.columns WHERE object_id = OBJECT_ID('dbo.IVPoDtl')",
                conn,
            )
            if dtl_cols_df is not None and not dtl_cols_df.empty:
                dtl_cols = [str(c).strip() for c in dtl_cols_df["name"].tolist()]
        except Exception:
            dtl_cols = []
        try:
            mst_cols_df = pd.read_sql(
                "SELECT name FROM sys.columns WHERE object_id = OBJECT_ID('dbo.IVPoMst')",
                conn,
            )
            if mst_cols_df is not None and not mst_cols_df.empty:
                mst_cols = [str(c).strip() for c in mst_cols_df["name"].tolist()]
        except Exception:
            mst_cols = []

        def pick_col(candidates, cols):
            for cand in candidates:
                for col in cols:
                    if col.lower() == cand.lower():
                        return col
            return None

        dept_col = pick_col(
            ["Department", "Dept", "DepartmentName", "DeptName", "DeptDesc", "DeptDescription"],
            dtl_cols,
        ) or pick_col(
            ["Department", "Dept", "DepartmentName", "DeptName", "DeptDesc", "DeptDescription"],
            mst_cols,
        )
        category_col = pick_col(
            ["Category", "CategoryName", "CatName", "CatDesc", "CategoryDesc"],
            dtl_cols,
        ) or pick_col(
            ["Category", "CategoryName", "CatName", "CatDesc", "CategoryDesc"],
            mst_cols,
        )
        subcategory_col = pick_col(
            ["SubCategory", "SubCategoryName", "SubCatName", "SubCategoryDesc", "SubCatDesc", "Sub_Category"],
            dtl_cols,
        ) or pick_col(
            ["SubCategory", "SubCategoryName", "SubCatName", "SubCategoryDesc", "SubCatDesc", "Sub_Category"],
            mst_cols,
        )
        purchasing_dept_col = pick_col(
            ["PurchasingDeptId", "PurchasingDeptID", "PurchaseDeptId", "PurchaseDeptID"],
            mst_cols,
        )

        dept_select = f"dtl.[{dept_col}] AS DeptName" if dept_col in dtl_cols else (
            f"mst.[{dept_col}] AS DeptName" if dept_col in mst_cols else "CAST(NULL AS NVARCHAR(120)) AS DeptName"
        )
        category_select = f"dtl.[{category_col}] AS CategoryName" if category_col in dtl_cols else (
            f"mst.[{category_col}] AS CategoryName" if category_col in mst_cols else "CAST(NULL AS NVARCHAR(120)) AS CategoryName"
        )
        subcategory_select = f"dtl.[{subcategory_col}] AS SubCategoryName" if subcategory_col in dtl_cols else (
            f"mst.[{subcategory_col}] AS SubCategoryName" if subcategory_col in mst_cols else "CAST(NULL AS NVARCHAR(120)) AS SubCategoryName"
        )
        purchasing_dept_select = (
            f"mst.[{purchasing_dept_col}] AS PurchasingDeptId"
            if purchasing_dept_col in mst_cols
            else "CAST(NULL AS INT) AS PurchasingDeptId"
        )

        sql = f"""
            SET NOCOUNT ON;
            SELECT
                mst.ID AS POID,
                mst.PONo,
                mst.PODate,
                mst.Status,
                mst.SupplierID,
                sup.Name AS SupplierName,
                {purchasing_dept_select},
                dtl.ID AS DetailID,
                dtl.ItemID,
                itm.Name AS ItemName,
                dtl.Qty,
                dtl.Rate,
                dtl.NetAmount,
                dtl.Fore,
                loc.ID AS StoreId,
                loc.Name AS StoreName,
                {dept_select},
                {category_select},
                {subcategory_select}
            FROM dbo.IVPoMst AS mst
            INNER JOIN dbo.IVPoDtl AS dtl
                ON dtl.POID = mst.ID
            LEFT JOIN dbo.IvSupplier AS sup
                ON mst.SupplierID = sup.ID
            LEFT JOIN dbo.IVItem AS itm
                ON dtl.ItemID = itm.ID
            LEFT JOIN dbo.IVItemLocation AS loc
                ON itm.LocationID = loc.ID
            WHERE CONVERT(date, mst.PODate) >= ?
        """
        df = pd.read_sql(sql, conn, params=[from_date])
        if df is None or df.empty:
            return df
        df.columns = [c.strip() for c in df.columns]
        df["Unit"] = (unit or "").upper()

        for col in ["Qty", "Rate", "NetAmount", "Fore"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
            else:
                df[col] = 0

        df["LineValue"] = df["NetAmount"].fillna(0)
        zero_mask = df["LineValue"] == 0
        if zero_mask.any():
            df.loc[zero_mask, "LineValue"] = df.loc[zero_mask, "Fore"].fillna(0)
        zero_mask = df["LineValue"] == 0
        if zero_mask.any():
            df.loc[zero_mask, "LineValue"] = df.loc[zero_mask, "Qty"] * df.loc[zero_mask, "Rate"]
        return df
    except Exception as e:
        print(f"Error fetching PO valuation rows ({unit}): {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def update_iv_po_mst(unit: str, params: dict):
    """
    Updates dbo.IVPoMst for an existing PO (draft updates).
    """
    conn = get_sql_connection(unit)
    if not conn:
        return {"error": f"Could not connect to {unit} for PO update"}
    try:
        params = _trim_po_mst_params(conn, params)
        sql = """
            UPDATE dbo.IVPoMst
            SET SupplierID = ?,
                TenderID = ?,
                PONo = ?,
                PODate = ?,
                DeliveryTerms = ?,
                PaymentsTerms = ?,
                OtherTerms = ?,
                TaxID = ?,
                Tax = ?,
                Discount = ?,
                Amount = ?,
                CreditDays = ?,
                POComplete = ?,
                Notes = ?,
                Preparedby = ?,
                Custom1 = ?,
                Custom2 = ?,
                SignAuthorityPerson = ?,
                SignAuthorityPDesig = ?,
                RefNo = ?,
                Subject = ?,
                AuthorizationID = ?,
                PurchaseIndentId = ?,
                Against = ?,
                QuotationId = ?,
                TotalFORe = ?,
                TotalExciseAmt = ?,
                AgainstId = ?,
                Status = ?
            WHERE ID = ?
        """
        cur = conn.cursor()
        cur.execute(
            sql,
            params.get("pSupplierid"),
            params.get("pTenderid"),
            params.get("pPono"),
            params.get("pPodate"),
            params.get("pDeliveryterms"),
            params.get("pPaymentsterms"),
            params.get("pOtherterms"),
            params.get("pTaxid"),
            params.get("pTax"),
            params.get("pDiscount"),
            params.get("pAmount"),
            params.get("pCreditdays"),
            params.get("pPocomplete"),
            params.get("pNotes"),
            params.get("pPreparedby"),
            params.get("pCustom1"),
            params.get("pCustom2"),
            params.get("pSignauthorityperson"),
            params.get("pSignauthoritypdesig"),
            params.get("pRefno"),
            params.get("pSubject"),
            params.get("pAuthorizationid"),
            params.get("pPurchaseIndentId"),
            params.get("Against"),
            params.get("QuotationId"),
            params.get("TotalFORe"),
            params.get("TotalExciseAmt"),
            params.get("AgainstId"),
            params.get("Status"),
            params.get("pId"),
        )
        conn.commit()
        _update_iv_po_mst_purchasing_dept_conn(conn, params.get("pId"), params.get("PurchasingDeptId"))
        _update_iv_po_mst_special_notes_conn(conn, params.get("pId"), params.get("pSpecialNotes"))
        _update_iv_po_mst_senior_approval_authority_conn(conn, params.get("pId"), params.get("SeniorApprovalAuthorityName"))
        _update_iv_po_mst_senior_approval_designation_conn(conn, params.get("pId"), params.get("SeniorApprovalAuthorityDesignation"))
        return {"status": "success"}
    except Exception as e:
        print(f"Error updating PO master ({unit}): {e}")
        return {"error": str(e)}
    finally:
        try:
            conn.close()
        except Exception:
            pass


def clear_iv_po_dtl(unit: str, po_id: int):
    """
    Deletes existing PO details for a PO (used before re-insert on update).
    """
    conn = get_sql_connection(unit)
    if not conn:
        return {"error": f"Could not connect to {unit} for PO detail delete"}
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM dbo.IVPoDtl WHERE POID = ?", int(po_id))
        conn.commit()
        return {"status": "success"}
    except Exception as e:
        print(f"Error clearing PO details ({unit}): {e}")
        return {"error": str(e)}
    finally:
        try:
            conn.close()
        except Exception:
            pass


def update_po_status(unit: str, po_id: int, status: str):
    """
    Update PO status in IVPoMst.
    """
    conn = get_sql_connection(unit)
    if not conn:
        return {"error": f"Could not connect to {unit} for PO status update"}
    try:
        cur = conn.cursor()
        cur.execute("UPDATE dbo.IVPoMst SET Status = ? WHERE ID = ?", (status, int(po_id)))
        conn.commit()
        return {"status": "success"}
    except Exception as e:
        print(f"Error updating PO status ({unit}): {e}")
        return {"error": str(e)}
    finally:
        try:
            conn.close()
        except Exception:
            pass


def fetch_purchase_po_list(
    unit: str,
    status: str | None = None,
    limit: int = 200,
    query: str | None = None,
    item_query: str | None = None,
):
    """
    Fetch recent POs for lookup dropdown.
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Could not connect to {unit} for PO list")
        return None
    try:
        limit = max(1, min(int(limit or 200), 500))
        status_map = {
            "draft": "D",
            "pending": "P",
            "pending approval": "P",
            "approved": "A",
            "d": "D",
            "p": "P",
            "a": "A",
            "open": "D,P",
            "draft_pending": "D,P",
        }
        status_key = str(status or "").strip().lower()
        status_code = status_map.get(status_key, "")
        params = []
        where_parts = []

        supplier_table = _resolve_table_name(conn, ["IvSupplier", "IVSupplier", "IVSUPPLIER"]) or "IvSupplier"
        supplier_id_col = _resolve_column(conn, supplier_table, ["ID", "Id", "SupplierID", "SupplierId"]) or "ID"
        supplier_name_col = _resolve_column(conn, supplier_table, ["Name", "SupplierName"]) or "Name"
        supplier_email_col = _resolve_column(conn, supplier_table, ["Email", "EMail", "EMailId", "EmailID", "E_Mail"])
        supplier_name_select = (
            f"sup.[{supplier_name_col}] AS SupplierName"
            if supplier_name_col
            else "CAST(NULL AS NVARCHAR(255)) AS SupplierName"
        )
        supplier_email_select = (
            f"sup.[{supplier_email_col}] AS SupplierEmail"
            if supplier_email_col
            else "CAST(NULL AS NVARCHAR(255)) AS SupplierEmail"
        )
        query_supplier_name_col = f"sup.[{supplier_name_col}]" if supplier_name_col else "CAST('' AS NVARCHAR(255))"

        if status_code:
            if "," in status_code:
                where_parts.append("mst.Status IN (?, ?)")
                parts = [s.strip() for s in status_code.split(",") if s.strip()]
                params.extend(parts[:2])
            else:
                where_parts.append("mst.Status = ?")
                params.append(status_code)

        q = (query or "").strip()
        if q:
            where_parts.append(f"(mst.PONo LIKE ? OR CONVERT(VARCHAR(20), mst.ID) LIKE ? OR {query_supplier_name_col} LIKE ?)")
            q_like = f"%{q}%"
            params.extend([q_like, q_like, q_like])

        item_q = (item_query or "").strip()
        if item_q:
            po_dtl_table = _resolve_table_name(conn, ["IVPoDtl", "IvPoDtl", "IVPODTL"]) or "IVPoDtl"
            po_dtl_po_col = _resolve_column(conn, po_dtl_table, ["POID", "PoID", "PoId", "Poid"]) or "POID"
            po_dtl_item_col = _resolve_column(conn, po_dtl_table, ["ItemID", "ItemId", "itemid"])
            po_dtl_item_name_col = _resolve_column(conn, po_dtl_table, ["ItemName", "Name", "ProductName"])

            item_table = _resolve_table_name(conn, ["IVItem", "IvItem", "IVITEM"]) or "IVItem"
            item_id_col = _resolve_column(conn, item_table, ["ID", "Id", "ItemID", "ItemId"])
            item_name_col = _resolve_column(conn, item_table, ["Name", "ItemName", "DescriptiveName", "Descriptive_Name"])

            item_join_sql = ""
            item_name_expr = None
            if item_name_col and po_dtl_item_col and item_id_col:
                item_join_sql = (
                    f"LEFT JOIN dbo.[{item_table}] AS itm "
                    f"ON dtl.[{po_dtl_item_col}] = itm.[{item_id_col}]"
                )
                item_name_expr = f"itm.[{item_name_col}]"
            if not item_name_expr and po_dtl_item_name_col:
                item_name_expr = f"dtl.[{po_dtl_item_name_col}]"

            if item_name_expr and po_dtl_po_col:
                where_parts.append(
                    f"""
                    EXISTS (
                        SELECT 1
                        FROM dbo.[{po_dtl_table}] AS dtl
                        {item_join_sql}
                        WHERE dtl.[{po_dtl_po_col}] = mst.ID
                          AND {item_name_expr} LIKE ?
                    )
                    """
                )
                params.append(f"%{item_q}%")
            else:
                where_parts.append("1 = 0")

        where_clause = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
        sql = f"""
            SET NOCOUNT ON;
            SELECT TOP {limit}
                mst.ID,
                mst.PONo,
                mst.PODate,
                mst.Status,
                mst.SupplierID AS SupplierID,
                mst.Amount,
                {supplier_name_select},
                {supplier_email_select}
            FROM dbo.IVPoMst AS mst
            LEFT JOIN dbo.[{supplier_table}] AS sup
                ON mst.SupplierID = sup.[{supplier_id_col}]
            {where_clause}
            ORDER BY mst.ID DESC
        """
        df = pd.read_sql(sql, conn, params=params)
        if df is None or df.empty:
            return df
        df.columns = [c.strip() for c in df.columns]
        return df
    except Exception as e:
        print(f"Error fetching PO list ({unit}): {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def fetch_purchase_po_number(unit: str):
    """
    Calls dbo.usp_GetIvPONo to get the next PO number string.
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Could not connect to {unit} for PO number")
        return None
    try:
        cur = conn.cursor()
        cur.execute("EXEC dbo.usp_GetIvPONo")
        row = cur.fetchone()
        return str(row[0]) if row and row[0] is not None else None
    except Exception as e:
        print(f"Error fetching PO number ({unit}): {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def fetch_purchase_against(unit: str):
    """
    Calls dbo.usp_GetPOAgainst for PO against master list.
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Could not connect to {unit} for PO against list")
        return None
    try:
        df = pd.read_sql("EXEC dbo.usp_GetPOAgainst", conn)
        if df is None or df.empty:
            return df
        df.columns = [c.strip() for c in df.columns]
        return df
    except Exception as e:
        print(f"Error fetching PO against list ({unit}): {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def fetch_authorized_po_indents(unit: str):
    """
    Calls dbo.Usp_GetAuthPOIndentforRaiseOrder for authorized indent list.
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Could not connect to {unit} for authorized indents")
        return None
    try:
        df = pd.read_sql("EXEC dbo.Usp_GetAuthPOIndentforRaiseOrder", conn)
        if df is None or df.empty:
            return df
        df.columns = [c.strip() for c in df.columns]
        return df
    except Exception as e:
        print(f"Error fetching authorized indents ({unit}): {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def fetch_indent_suppliers_for_po(unit: str, indent_id: int):
    """
    Calls dbo.usp_getPmIndentSuppForPO to fetch suppliers for a given indent.
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Could not connect to {unit} for indent suppliers")
        return None
    try:
        sql = "EXEC dbo.usp_getPmIndentSuppForPO ?"
        df = pd.read_sql(sql, conn, params=[indent_id])
        if df is None or df.empty:
            return df
        df.columns = [c.strip() for c in df.columns]
        return df
    except Exception as e:
        print(f"Error fetching indent suppliers ({unit}): {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def fetch_indent_details_for_po(unit: str, indent_id: int):
    """
    Calls dbo.Usp_GetPUrchaseIndenttlForPO to fetch indent item details.
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Could not connect to {unit} for indent details")
        return None
    try:
        sql = "EXEC dbo.Usp_GetPUrchaseIndenttlForPO ?"
        df = pd.read_sql(sql, conn, params=[indent_id])
        if df is None or df.empty:
            return df
        df.columns = [c.strip() for c in df.columns]
        return df
    except Exception as e:
        print(f"Error fetching indent details ({unit}): {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def fetch_iv_suppliers(unit: str):
    """
    Calls dbo.Usp_GetIvsupplier to fetch supplier master list.
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Could not connect to {unit} for suppliers")
        return None
    try:
        df = pd.read_sql("EXEC dbo.Usp_GetIvsupplier", conn)
        if df is None or df.empty:
            return df
        df.columns = [c.strip() for c in df.columns]
        return df
    except Exception as e:
        print(f"Error fetching suppliers ({unit}): {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def update_iv_supplier_email(unit: str, supplier_id: int, email: str | None):
    """
    Update only supplier email in IVSupplier/IvSupplier.
    """
    conn = get_sql_connection(unit)
    if not conn:
        return {"error": f"Could not connect to {unit}"}
    try:
        supplier_id = int(supplier_id or 0)
    except Exception:
        return {"error": "Invalid supplier ID"}
    if supplier_id <= 0:
        return {"error": "Invalid supplier ID"}
    try:
        table_name = _resolve_table_name(conn, ["IvSupplier", "IVSupplier", "IVSUPPLIER"])
        if not table_name:
            return {"error": "Supplier table not found"}
        id_col = _resolve_column(conn, table_name, ["ID", "Id", "SupplierID", "SupplierId"]) or "ID"
        email_col = _resolve_column(conn, table_name, ["Email", "EMail", "EMailId", "EmailID", "E_Mail"])
        if not email_col:
            return {"error": "Supplier email column not found"}

        email_val = str(email or "").strip()
        if not email_val:
            email_val = None

        cursor = conn.cursor()
        cursor.execute(
            f"SELECT COUNT(1) FROM dbo.{table_name} WHERE [{id_col}] = ?",
            (supplier_id,),
        )
        row = cursor.fetchone()
        exists = int(row[0]) if row and row[0] is not None else 0
        if exists <= 0:
            return {"error": "Supplier not found"}

        cursor.execute(
            f"UPDATE dbo.{table_name} SET [{email_col}] = ? WHERE [{id_col}] = ?",
            (email_val, supplier_id),
        )
        conn.commit()
        return {"status": "success", "supplier_id": supplier_id, "email": email_val}
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        return {"error": str(e)}
    finally:
        try:
            conn.close()
        except Exception:
            pass


def fetch_iv_supplier_master_list(unit: str):
    """
    Fetch supplier id/code/name list from dbo.vw_IvSupplier for master search.
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Could not connect to {unit} for supplier master list")
        return None
    try:
        df = pd.read_sql(
            "SELECT ID, Code, Name FROM dbo.vw_IvSupplier ORDER BY Name",
            conn,
        )
        if df is None or df.empty:
            return df
        df.columns = [c.strip() for c in df.columns]
        return df
    except Exception as e:
        print(f"Error fetching supplier master list ({unit}): {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def fetch_iv_supplier_master_detail(unit: str, supplier_id: int):
    """
    Fetch supplier master details from dbo.vw_IvSupplier by id.
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Could not connect to {unit} for supplier master detail")
        return None
    try:
        df = pd.read_sql(
            "SELECT TOP 1 * FROM dbo.vw_IvSupplier WHERE ID = ?",
            conn,
            params=[int(supplier_id)],
        )
        if df is None or df.empty:
            return df
        df.columns = [c.strip() for c in df.columns]
        return df
    except Exception as e:
        print(f"Error fetching supplier master detail ({unit}): {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def fetch_iv_manufacturer_master_list(unit: str):
    """
    Calls dbo.Usp_GetIvManufacturerId to fetch manufacturer id/code/name list.
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Could not connect to {unit} for manufacturer master list")
        return None
    try:
        df = pd.read_sql("EXEC dbo.Usp_GetIvManufacturerId", conn)
        if df is None or df.empty:
            return df
        df.columns = [c.strip() for c in df.columns]
        return df
    except Exception as e:
        print(f"Error fetching manufacturer master list ({unit}): {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def fetch_iv_manufacturer_master_detail(unit: str, manufacturer_id: int):
    """
    Calls dbo.Usp_GetIvManufacturer to fetch manufacturer master details.
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Could not connect to {unit} for manufacturer master detail")
        return None
    try:
        df = pd.read_sql(
            "EXEC dbo.Usp_GetIvManufacturer ?",
            conn,
            params=[int(manufacturer_id)],
        )
        if df is None or df.empty:
            return df
        df.columns = [c.strip() for c in df.columns]
        return df
    except Exception as e:
        print(f"Error fetching manufacturer master detail ({unit}): {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def fetch_indent_number(unit: str):
    """
    Calls dbo.usp_GetPMPINIndentNo to get the next indent number.
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Could not connect to {unit} for indent number")
        return None
    try:
        cur = conn.cursor()
        cur.execute("EXEC dbo.usp_GetPMPINIndentNo")
        row = cur.fetchone()
        return str(row[0]) if row and row[0] is not None else None
    except Exception as e:
        print(f"Error fetching indent number ({unit}): {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def fetch_indent_id_by_number(unit: str, indent_number: str):
    """
    Fetches indent id by indent number from PMIndent_Mst.
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Could not connect to {unit} for indent lookup")
        return None
    try:
        cur = conn.cursor()
        cur.execute("SELECT IndentId FROM dbo.PMIndent_Mst WHERE IndentNumber = ?", indent_number)
        row = cur.fetchone()
        return int(row[0]) if row and row[0] is not None else None
    except Exception as e:
        print(f"Error fetching indent id ({unit}): {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def fetch_departments_for_indent(unit: str):
    """
    Calls dbo.usp_getDepartmentforIndent to fetch active departments.
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Could not connect to {unit} for indent departments")
        return None
    try:
        df = pd.read_sql("EXEC dbo.usp_getDepartmentforIndent", conn)
        if df is None or df.empty:
            return df
        df.columns = [c.strip() for c in df.columns]
        return df
    except Exception as e:
        print(f"Error fetching indent departments ({unit}): {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def fetch_substores_for_department(unit: str, department_id: int):
    """
    Calls dbo.usp_GetUserSubStores to fetch stores for a department.
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Could not connect to {unit} for indent stores")
        return None
    try:
        df = pd.read_sql("EXEC dbo.usp_GetUserSubStores ?", conn, params=[int(department_id)])
        if df is None or df.empty:
            return df
        df.columns = [c.strip() for c in df.columns]
        return df
    except Exception as e:
        print(f"Error fetching indent stores ({unit}): {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def fetch_indent_items_catalog(unit: str, item_type_id: int):
    """
    Calls dbo.usp_getIvItemsforLastPoRateforIndent for indent items.
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Could not connect to {unit} for indent items")
        return None
    try:
        df = pd.read_sql("EXEC dbo.usp_getIvItemsforLastPoRateforIndent ?", conn, params=[int(item_type_id)])
        if df is None or df.empty:
            return df
        df.columns = [c.strip() for c in df.columns]
        return df
    except Exception as e:
        print(f"Error fetching indent items ({unit}): {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def fetch_item_categories(unit: str):
    """
    Fetch item categories from IVItemType (fallback when no proc is available).
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Could not connect to {unit} for item categories")
        return None
    try:
        df = pd.read_sql("SELECT ID, Code, Name FROM dbo.IVItemType", conn)
        if df is None or df.empty:
            return df
        df.columns = [c.strip() for c in df.columns]
        return df
    except Exception as e:
        print(f"Error fetching item categories ({unit}): {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _get_app_lock(cursor, resource: str, timeout_ms: int):
    try:
        cursor.execute(
            "DECLARE @result int; "
            "EXEC @result = sp_getapplock "
            "@Resource=?, @LockMode='Exclusive', @LockOwner='Session', @LockTimeout=?; "
            "SELECT @result;",
            (resource, int(timeout_ms)),
        )
        row = cursor.fetchone()
    except Exception as e:
        return {"error": str(e)}
    if not row or row[0] is None:
        return {"error": "Failed to acquire application lock"}
    try:
        result = int(row[0])
    except Exception:
        result = row[0]
    if isinstance(result, int) and result < 0:
        return {"error": f"Failed to acquire application lock (code {result})"}
    return {"success": True}


def _release_app_lock(cursor, resource: str):
    try:
        cursor.execute(
            "EXEC sp_releaseapplock @Resource=?, @LockOwner='Session'",
            (resource,),
        )
    except Exception:
        pass


def add_pm_indent_mst(unit: str, params: dict):
    """
    Executes dbo.usp_addPMIndent_Mst to create an indent master record.
    """
    conn = get_sql_connection(unit)
    if not conn:
        return {"error": f"Could not connect to {unit}"}
    try:
        cursor = conn.cursor()
        sql = """
        EXEC dbo.usp_addPMIndent_Mst
            @pIndentid=?,
            @pIndentnumber=?,
            @pDepartmentid=?,
            @pBudgetid=?,
            @pRemarks=?,
            @pPropindication=?,
            @pIndentnature=?,
            @pDeliverystartdate=?,
            @pDeliveryenddate=?,
            @pItemcategoryid=?,
            @pUpdatedon=?,
            @pUpdatedby=?,
            @pStatus=?,
            @pAuthorisedremarks=?,
            @pAuthorisedby=?,
            @pAuthorisedon=?,
            @pAuthorised=?,
            @pProcurementId=?,
            @pStoreId=?,
            @pInsertedby=?
        """
        values = [
            params.get("pIndentid"),
            params.get("pIndentnumber"),
            params.get("pDepartmentid"),
            params.get("pBudgetid"),
            params.get("pRemarks"),
            params.get("pPropindication"),
            params.get("pIndentnature"),
            params.get("pDeliverystartdate"),
            params.get("pDeliveryenddate"),
            params.get("pItemcategoryid"),
            params.get("pUpdatedon"),
            params.get("pUpdatedby"),
            params.get("pStatus"),
            params.get("pAuthorisedremarks"),
            params.get("pAuthorisedby"),
            params.get("pAuthorisedon"),
            params.get("pAuthorised"),
            params.get("pProcurementId"),
            params.get("pStoreId"),
            params.get("pInsertedby"),
        ]
        cursor.execute(sql, values)
        row = None
        try:
            if cursor.description is not None:
                row = cursor.fetchone()
        except pyodbc.ProgrammingError:
            row = None
        try:
            conn.commit()
        except Exception:
            pass
        indent_id = None
        if row and row[0] is not None:
            try:
                indent_id = int(row[0])
            except Exception:
                indent_id = row[0]
        return {"indent_id": indent_id}
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        return {"error": str(e)}
    finally:
        try:
            conn.close()
        except Exception:
            pass


def add_pm_indent_details(unit: str, params: dict):
    """
    Executes dbo.usp_addPMIndent_Details for a single indent item.
    """
    conn = get_sql_connection(unit)
    if not conn:
        return {"error": f"Could not connect to {unit}"}
    try:
        cursor = conn.cursor()
        sql = """
        EXEC dbo.usp_addPMIndent_Details
            @pIndentdetailid=?,
            @pIndentid=?,
            @pItemid=?,
            @pItemrate=?,
            @pItemqty=?,
            @pEstimatedcost=?,
            @pSalestax=?,
            @pExcisetax=?,
            @pEscalated=?,
            @pLandingrate=?,
            @pDeliveryStartDate=?,
            @pDeliveryendDate=?,
            @pAuthoriseQty=?,
            @ppacksizeId=?,
            @pfreeqty=?,
            @pDiscount=?,
            @pTax=?,
            @pTaxAmount=?,
            @pVATOn=?,
            @pVAT=?,
            @pMRP=?,
            @pConsumeQty=?,
            @pIssueQty=?
        """
        values = [
            params.get("pIndentdetailid"),
            params.get("pIndentid"),
            params.get("pItemid"),
            params.get("pItemrate"),
            params.get("pItemqty"),
            params.get("pEstimatedcost"),
            params.get("pSalestax"),
            params.get("pExcisetax"),
            params.get("pEscalated"),
            params.get("pLandingrate"),
            params.get("pDeliveryStartDate"),
            params.get("pDeliveryendDate"),
            params.get("pAuthoriseQty"),
            params.get("ppacksizeId"),
            params.get("pfreeqty"),
            params.get("pDiscount"),
            params.get("pTax"),
            params.get("pTaxAmount"),
            params.get("pVATOn"),
            params.get("pVAT"),
            params.get("pMRP"),
            params.get("pConsumeQty"),
            params.get("pIssueQty"),
        ]
        cursor.execute(sql, values)
        try:
            conn.commit()
        except Exception:
            pass
        return {"success": True}
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        return {"error": str(e)}
    finally:
        try:
            conn.close()
        except Exception:
            pass


def clear_pm_indent_details(unit: str, indent_id: int):
    """
    Deletes existing indent details for a draft update.
    """
    conn = get_sql_connection(unit)
    if not conn:
        return {"error": f"Could not connect to {unit}"}
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM dbo.PMIndent_Details WHERE IndentId = ?", int(indent_id))
        conn.commit()
        return {"success": True}
    except Exception as e:
        return {"error": str(e)}
    finally:
        try:
            conn.close()
        except Exception:
            pass


def fetch_pm_indents_by_status(unit: str, status: str | list[str] | tuple[str, ...]):
    """
    Fetch indents by status from PMIndent_Mst.
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Could not connect to {unit} for indents")
        return None
    try:
        if isinstance(status, (list, tuple, set)):
            status_list = [str(s) for s in status if str(s).strip()]
        else:
            status_list = [str(status)]
        status_list = [s.strip() for s in status_list if s is not None]
        if not status_list:
            return pd.DataFrame()
        if len(status_list) == 1:
            sql = """
                SELECT IndentId, IndentNumber, DeliveryStartDate, Status
                FROM dbo.PMIndent_Mst
                WHERE Status = ?
                ORDER BY IndentId DESC
            """
            params = [status_list[0]]
        else:
            placeholders = ",".join("?" for _ in status_list)
            sql = f"""
                SELECT IndentId, IndentNumber, DeliveryStartDate, Status
                FROM dbo.PMIndent_Mst
                WHERE Status IN ({placeholders})
                ORDER BY IndentId DESC
            """
            params = status_list
        df = pd.read_sql(sql, conn, params=params)
        if df is None or df.empty:
            return df
        df.columns = [c.strip() for c in df.columns]
        return df
    except Exception as e:
        print(f"Error fetching indents ({unit}): {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def fetch_pending_pm_indents(unit: str):
    """
    Calls dbo.Usp_GetPMIndentMst to fetch pending indents for authorization.
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Could not connect to {unit} for pending indents")
        return None
    try:
        df = pd.read_sql("EXEC dbo.Usp_GetPMIndentMst", conn)
        if df is None or df.empty:
            return df
        df.columns = [c.strip() for c in df.columns]
        return df
    except Exception as e:
        print(f"Error fetching pending indents ({unit}): {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def fetch_pm_indent_mst_other(unit: str, indent_id: int):
    """
    Calls dbo.usp_getPmIndentMstOther to fetch indent master details.
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Could not connect to {unit} for indent master")
        return None
    try:
        df = pd.read_sql("EXEC dbo.usp_getPmIndentMstOther ?", conn, params=[int(indent_id)])
        if df is None or df.empty:
            return df
        df.columns = [c.strip() for c in df.columns]
        return df
    except Exception as e:
        print(f"Error fetching indent master ({unit}): {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def fetch_pm_indent_items_detail(unit: str, indent_id: int):
    """
    Calls dbo.usp_getPmIndentItemsDetail to fetch indent item details.
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Could not connect to {unit} for indent items detail")
        return None
    try:
        df = pd.read_sql("EXEC dbo.usp_getPmIndentItemsDetail ?", conn, params=[int(indent_id)])
        if df is None or df.empty:
            return df
        df.columns = [c.strip() for c in df.columns]
        return df
    except Exception as e:
        print(f"Error fetching indent items detail ({unit}): {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def update_pm_indent_mst(unit: str, params: dict):
    """
    Executes dbo.usp_updatePMIndent_Mst to update/authorize an indent.
    """
    conn = get_sql_connection(unit)
    if not conn:
        return {"error": f"Could not connect to {unit}"}
    try:
        cursor = conn.cursor()
        sql = """
        EXEC dbo.usp_updatePMIndent_Mst
            @pIndentid=?,
            @pIndentnumber=?,
            @pDepartmentid=?,
            @pBudgetid=?,
            @pRemarks=?,
            @pPropindication=?,
            @pIndentnature=?,
            @pDeliverystartdate=?,
            @pDeliveryenddate=?,
            @pItemcategoryid=?,
            @pUpdatedon=?,
            @pUpdatedby=?,
            @pStatus=?,
            @pAuthorisedremarks=?,
            @pAuthorisedby=?,
            @pAuthorisedon=?,
            @pAuthorised=?,
            @pProcurementId=?,
            @pStoreId=?
        """
        values = [
            params.get("pIndentid"),
            params.get("pIndentnumber"),
            params.get("pDepartmentid"),
            params.get("pBudgetid"),
            params.get("pRemarks"),
            params.get("pPropindication"),
            params.get("pIndentnature"),
            params.get("pDeliverystartdate"),
            params.get("pDeliveryenddate"),
            params.get("pItemcategoryid"),
            params.get("pUpdatedon"),
            params.get("pUpdatedby"),
            params.get("pStatus"),
            params.get("pAuthorisedremarks"),
            params.get("pAuthorisedby"),
            params.get("pAuthorisedon"),
            params.get("pAuthorised"),
            params.get("pProcurementId"),
            params.get("pStoreId"),
        ]
        cursor.execute(sql, values)
        try:
            conn.commit()
        except Exception:
            pass
        return {"success": True}
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        return {"error": str(e)}
    finally:
        try:
            conn.close()
        except Exception:
            pass


def update_pm_indent_details_authorised_qty(unit: str, indent_id: int, authorised_qty: float | None):
    """
    Updates PMIndent_Details.AuthorisedQty for a given indent.
    If authorised_qty is None, set AuthorisedQty = ItemQty.
    """
    conn = get_sql_connection(unit)
    if not conn:
        return {"error": f"Could not connect to {unit}"}
    try:
        cur = conn.cursor()
        if authorised_qty is None:
            cur.execute(
                "UPDATE dbo.PMIndent_Details SET AuthorisedQty = ItemQty WHERE IndentId = ?",
                int(indent_id),
            )
        else:
            cur.execute(
                "UPDATE dbo.PMIndent_Details SET AuthorisedQty = ? WHERE IndentId = ?",
                float(authorised_qty),
                int(indent_id),
            )
        try:
            conn.commit()
        except Exception:
            pass
        return {"success": True}
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        return {"error": str(e)}
    finally:
        try:
            conn.close()
        except Exception:
            pass

def fetch_iv_supplier_number(unit: str, after_add: bool = False):
    """
    Calls dbo.usp_GetIvSupplierNumber to get the next supplier code.
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Could not connect to {unit} for supplier number")
        return None
    try:
        cur = conn.cursor()
        cur.execute("EXEC dbo.usp_GetIvSupplierNumber ?", (1 if after_add else 0,))
        row = cur.fetchone()
        return str(row[0]) if row and row[0] is not None else None
    except Exception as e:
        print(f"Error fetching supplier number ({unit}): {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def fetch_default_city_state(unit: str):
    """
    Calls dbo.usp_GetDefaultCityState to fetch default city/state/country values.
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Could not connect to {unit} for default city/state")
        return None
    try:
        df = pd.read_sql("EXEC dbo.usp_GetDefaultCityState", conn)
        if df is None or df.empty:
            return df
        df.columns = [c.strip() for c in df.columns]
        return df
    except Exception as e:
        print(f"Error fetching default city/state ({unit}): {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def fetch_iv_manufacturer_number(unit: str):
    """
    Calls dbo.usp_GetIvManufacturerNumber to get the next manufacturer code.
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Could not connect to {unit} for manufacturer number")
        return None
    try:
        cur = conn.cursor()
        cur.execute("EXEC dbo.usp_GetIvManufacturerNumber")
        row = cur.fetchone()
        return str(row[0]) if row and row[0] is not None else None
    except Exception as e:
        print(f"Error fetching manufacturer number ({unit}): {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def fetch_city_list(unit: str, state_id: int = 0):
    """
    Calls dbo.usp_GetCity to fetch city list for the given state.
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Could not connect to {unit} for city list")
        return None
    try:
        df = pd.read_sql(
            "EXEC dbo.usp_GetCity ?",
            conn,
            params=[int(state_id or 0)],
        )
        if df is None or df.empty:
            return df
        df.columns = [c.strip() for c in df.columns]
        return df
    except Exception as e:
        print(f"Error fetching city list ({unit}): {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def fetch_state_list(unit: str):
    """
    Fetches state list from State_Mst (filters deactive if column exists).
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Could not connect to {unit} for state list")
        return None
    try:
        sql = """
        SELECT State_ID, State_Name, State_Code
        FROM State_Mst
        WHERE (CASE WHEN COL_LENGTH('State_Mst', 'deactive') IS NULL THEN 0 ELSE deactive END) = 0
        ORDER BY State_Name
        """
        df = pd.read_sql(sql, conn)
        if df is None or df.empty:
            return df
        df.columns = [c.strip() for c in df.columns]
        return df
    except Exception as e:
        print(f"Error fetching state list ({unit}): {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def add_iv_supplier(unit: str, params: dict):
    """
    Executes dbo.usp_addIVSupplier to add a supplier master record.
    """
    conn = get_sql_connection(unit)
    if not conn:
        return {"error": f"Could not connect to {unit}"}
    try:
        cursor = conn.cursor()
        sql = """
        EXEC dbo.usp_addIVSupplier
            @pId=?,
            @pCode=?,
            @pName=?,
            @pAddress=?,
            @pCity=?,
            @pState=?,
            @pPin=?,
            @pContactperson=?,
            @pContactdesignation=?,
            @pGroupid=?,
            @pCreditperiod=?,
            @pDateofassociation=?,
            @pFax=?,
            @pPhone1=?,
            @pPhone2=?,
            @pCellphone=?,
            @pEmail=?,
            @pWeb=?,
            @pCst=?,
            @pMst=?,
            @pTds=?,
            @pExcisecode=?,
            @pExportcode=?,
            @pLedgerid=?,
            @pEligableforadv=?,
            @pBankname=?,
            @pBankbranch=?,
            @pBankacno=?,
            @pMcirno=?,
            @pNote=?,
            @pProposed=?,
            @pUpdatedby=?,
            @pUpdatedon=?,
            @pSupptype=?,
            @pSociety=?,
            @plandmark=?,
            @pIncomeTaxNo=?,
            @pVillage=?,
            @pPaytermsid=?
        """
        values = [
            params.get("pId"),
            params.get("pCode"),
            params.get("pName"),
            params.get("pAddress"),
            params.get("pCity"),
            params.get("pState"),
            params.get("pPin"),
            params.get("pContactperson"),
            params.get("pContactdesignation"),
            params.get("pGroupid"),
            params.get("pCreditperiod"),
            params.get("pDateofassociation"),
            params.get("pFax"),
            params.get("pPhone1"),
            params.get("pPhone2"),
            params.get("pCellphone"),
            params.get("pEmail"),
            params.get("pWeb"),
            params.get("pCst"),
            params.get("pMst"),
            params.get("pTds"),
            params.get("pExcisecode"),
            params.get("pExportcode"),
            params.get("pLedgerid"),
            params.get("pEligableforadv"),
            params.get("pBankname"),
            params.get("pBankbranch"),
            params.get("pBankacno"),
            params.get("pMcirno"),
            params.get("pNote"),
            params.get("pProposed"),
            params.get("pUpdatedby"),
            params.get("pUpdatedon"),
            params.get("pSupptype"),
            params.get("pSociety"),
            params.get("pLandmark"),
            params.get("pIncomeTaxNo"),
            params.get("pVillage"),
            params.get("pPaytermsid"),
        ]
        cursor.execute(sql, values)
        row = None
        try:
            if cursor.description is not None:
                row = cursor.fetchone()
        except pyodbc.ProgrammingError:
            row = None
        try:
            conn.commit()
        except Exception:
            pass
        supplier_id = None
        if row and row[0] is not None:
            try:
                supplier_id = int(row[0])
            except Exception:
                supplier_id = row[0]
        return {"supplier_id": supplier_id}
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        return {"error": str(e)}
    finally:
        try:
            conn.close()
        except Exception:
            pass


def update_iv_supplier(unit: str, params: dict):
    """
    Executes dbo.usp_updateIVSupplier to update a supplier master record.
    """
    conn = get_sql_connection(unit)
    if not conn:
        return {"error": f"Could not connect to {unit}"}
    try:
        cursor = conn.cursor()
        sql = """
        EXEC dbo.usp_updateIVSupplier
            @pId=?,
            @pCode=?,
            @pName=?,
            @pAddress=?,
            @pCity=?,
            @pState=?,
            @pPin=?,
            @pContactperson=?,
            @pContactdesignation=?,
            @pGroupid=?,
            @pCreditperiod=?,
            @pDateofassociation=?,
            @pFax=?,
            @pPhone1=?,
            @pPhone2=?,
            @pCellphone=?,
            @pEmail=?,
            @pWeb=?,
            @pCst=?,
            @pMst=?,
            @pTds=?,
            @pExcisecode=?,
            @pExportcode=?,
            @pLedgerid=?,
            @pEligableforadv=?,
            @pBankname=?,
            @pBankbranch=?,
            @pBankacno=?,
            @pMcirno=?,
            @pNote=?,
            @pProposed=?,
            @pUpdatedby=?,
            @pUpdatedon=?,
            @pSupptype=?,
            @pSociety=?,
            @plandmark=?,
            @pIncomeTaxNo=?,
            @pVillage=?,
            @pPaytermsid=?
        """
        values = [
            params.get("pId"),
            params.get("pCode"),
            params.get("pName"),
            params.get("pAddress"),
            params.get("pCity"),
            params.get("pState"),
            params.get("pPin"),
            params.get("pContactperson"),
            params.get("pContactdesignation"),
            params.get("pGroupid"),
            params.get("pCreditperiod"),
            params.get("pDateofassociation"),
            params.get("pFax"),
            params.get("pPhone1"),
            params.get("pPhone2"),
            params.get("pCellphone"),
            params.get("pEmail"),
            params.get("pWeb"),
            params.get("pCst"),
            params.get("pMst"),
            params.get("pTds"),
            params.get("pExcisecode"),
            params.get("pExportcode"),
            params.get("pLedgerid"),
            params.get("pEligableforadv"),
            params.get("pBankname"),
            params.get("pBankbranch"),
            params.get("pBankacno"),
            params.get("pMcirno"),
            params.get("pNote"),
            params.get("pProposed"),
            params.get("pUpdatedby"),
            params.get("pUpdatedon"),
            params.get("pSupptype"),
            params.get("pSociety"),
            params.get("pLandmark"),
            params.get("pIncomeTaxNo"),
            params.get("pVillage"),
            params.get("pPaytermsid"),
        ]
        cursor.execute(sql, values)
        row = None
        try:
            if cursor.description is not None:
                row = cursor.fetchone()
        except pyodbc.ProgrammingError:
            row = None
        try:
            conn.commit()
        except Exception:
            pass
        supplier_id = params.get("pId")
        if row and row[0] is not None:
            try:
                supplier_id = int(row[0])
            except Exception:
                supplier_id = row[0]
        return {"supplier_id": supplier_id}
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        return {"error": str(e)}
    finally:
        try:
            conn.close()
        except Exception:
            pass


def add_iv_manufacturer(unit: str, params: dict):
    """
    Executes dbo.usp_addIVManufacturer to add a manufacturer master record.
    """
    conn = get_sql_connection(unit)
    if not conn:
        return {"error": f"Could not connect to {unit}"}
    try:
        cursor = conn.cursor()
        sql = """
        EXEC dbo.usp_addIVManufacturer
            @pId=?,
            @pCode=?,
            @pName=?,
            @pAddress=?,
            @pCity=?,
            @pState=?,
            @pPin=?,
            @pContactperson=?,
            @pContactdesignation=?,
            @pPhone1=?,
            @pPhone2=?,
            @pCellphone=?,
            @pWeb=?,
            @pEmail=?,
            @pBankname=?,
            @pBankacno=?,
            @pBankbranch=?,
            @pNote=?,
            @pActive=?,
            @pEnteredon=?,
            @pEnteredby=?,
            @pUpdatedon=?,
            @pUpdatedby=?,
            @pSociety=?,
            @plandmark=?,
            @pVillage=?
        """
        values = [
            params.get("pId"),
            params.get("pCode"),
            params.get("pName"),
            params.get("pAddress"),
            params.get("pCity"),
            params.get("pState"),
            params.get("pPin"),
            params.get("pContactperson"),
            params.get("pContactdesignation"),
            params.get("pPhone1"),
            params.get("pPhone2"),
            params.get("pCellphone"),
            params.get("pWeb"),
            params.get("pEmail"),
            params.get("pBankname"),
            params.get("pBankacno"),
            params.get("pBankbranch"),
            params.get("pNote"),
            params.get("pActive"),
            params.get("pEnteredon"),
            params.get("pEnteredby"),
            params.get("pUpdatedon"),
            params.get("pUpdatedby"),
            params.get("pSociety"),
            params.get("pLandmark"),
            params.get("pVillage"),
        ]
        cursor.execute(sql, values)
        row = None
        try:
            if cursor.description is not None:
                row = cursor.fetchone()
        except pyodbc.ProgrammingError:
            row = None
        try:
            conn.commit()
        except Exception:
            pass
        manufacturer_id = None
        if row and row[0] is not None:
            try:
                manufacturer_id = int(row[0])
            except Exception:
                manufacturer_id = row[0]
        return {"manufacturer_id": manufacturer_id}
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        return {"error": str(e)}
    finally:
        try:
            conn.close()
        except Exception:
            pass


def update_iv_manufacturer(unit: str, params: dict):
    """
    Executes dbo.usp_updateIVManufacturer to update a manufacturer master record.
    """
    conn = get_sql_connection(unit)
    if not conn:
        return {"error": f"Could not connect to {unit}"}
    try:
        cursor = conn.cursor()
        sql = """
        EXEC dbo.usp_updateIVManufacturer
            @pId=?,
            @pCode=?,
            @pName=?,
            @pAddress=?,
            @pCity=?,
            @pState=?,
            @pPin=?,
            @pContactperson=?,
            @pContactdesignation=?,
            @pPhone1=?,
            @pPhone2=?,
            @pCellphone=?,
            @pWeb=?,
            @pEmail=?,
            @pBankname=?,
            @pBankacno=?,
            @pBankbranch=?,
            @pNote=?,
            @pActive=?,
            @pEnteredon=?,
            @pEnteredby=?,
            @pUpdatedon=?,
            @pUpdatedby=?,
            @pSociety=?,
            @plandmark=?,
            @pVillage=?
        """
        values = [
            params.get("pId"),
            params.get("pCode"),
            params.get("pName"),
            params.get("pAddress"),
            params.get("pCity"),
            params.get("pState"),
            params.get("pPin"),
            params.get("pContactperson"),
            params.get("pContactdesignation"),
            params.get("pPhone1"),
            params.get("pPhone2"),
            params.get("pCellphone"),
            params.get("pWeb"),
            params.get("pEmail"),
            params.get("pBankname"),
            params.get("pBankacno"),
            params.get("pBankbranch"),
            params.get("pNote"),
            params.get("pActive"),
            params.get("pEnteredon"),
            params.get("pEnteredby"),
            params.get("pUpdatedon"),
            params.get("pUpdatedby"),
            params.get("pSociety"),
            params.get("pLandmark"),
            params.get("pVillage"),
        ]
        cursor.execute(sql, values)
        row = None
        try:
            if cursor.description is not None:
                row = cursor.fetchone()
        except pyodbc.ProgrammingError:
            row = None
        try:
            conn.commit()
        except Exception:
            pass
        manufacturer_id = params.get("pId")
        if row and row[0] is not None:
            try:
                manufacturer_id = int(row[0])
            except Exception:
                manufacturer_id = row[0]
        return {"manufacturer_id": manufacturer_id}
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        return {"error": str(e)}
    finally:
        try:
            conn.close()
        except Exception:
            pass


def fetch_new_pharmacy_indent_count(unit: str):
    """
    Calls dbo.usp_CountNewPharmacyIndent for pending indent count.
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Could not connect to {unit} for indent count")
        return None
    try:
        cur = conn.cursor()
        cur.execute("EXEC dbo.usp_CountNewPharmacyIndent")
        row = cur.fetchone()
        if row is None:
            return 0
        try:
            return int(row[0])
        except Exception:
            return 0
    except Exception as e:
        msg = str(e)
        if "Could not find stored procedure" in msg:
            return 0
        print(f"Error fetching indent count ({unit}): {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def fetch_items_last_po_rate(unit: str):
    """
    Calls dbo.usp_getIvItemsforLastPoRate for item list and last PO rates.
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Could not connect to {unit} for items list")
        return None
    try:
        df = pd.read_sql("EXEC dbo.usp_getIvItemsforLastPoRate", conn)
        if df is None or df.empty:
            return df
        df.columns = [c.strip() for c in df.columns]
        return df
    except Exception as e:
        print(f"Error fetching items list ({unit}): {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def add_iv_item(unit: str, params: dict):
    """
    Executes dbo.usp_addIVItem to create a new item master.
    Returns the created item id if available.
    """
    conn = get_sql_connection(unit)
    if not conn:
        return {"error": f"Could not connect to {unit}"}
    try:
        cursor = conn.cursor()
        sql = """
        EXEC dbo.usp_addIVItem
            @pId=?,
            @pCode=?,
            @pItemsubtypeid=?,
            @pName=?,
            @pStandardrate=?,
            @pSealingrate=?,
            @pOpbalance=?,
            @pMaxlevel=?,
            @pMinlevel=?,
            @pReorderlevel=?,
            @pCurrentqty=?,
            @pSalestax=?,
            @pItemtypeid=?,
            @pUnitid=?,
            @pLocationid=?,
            @pItemgroupid=?,
            @pSubgroupid=?,
            @pAbcCode=?,
            @pStocktypeid=?,
            @pBatchrequired=?,
            @pExpirydtrequired=?,
            @pactive=?,
            @pSalesprice=?,
            @pDepartmentid=?,
            @pPurchasefrequency=?,
            @pPacksizeid=?,
            @pItemchangeid=?,
            @pUpdatedby=?,
            @pUpdatedon=?,
            @pGenericId=?,
            @pBrandId=?,
            @pVatOn=?,
            @pMedicineTypeId=?,
            @pInsertedByUserID=?,
            @pInsertedON=?,
            @pInsertedMacName=?,
            @pInsertedMacID=?,
            @pInsertedIPAddress=?,
            @pHmsCategoryID=?,
            @DescriptiveName=?,
            @pTaxid=?,
            @pChkLooseSelling=?,
            @pchkQualityCtrl=?,
            @pProductCode=?
        """
        values = [
            params.get("pId"),
            params.get("pCode"),
            params.get("pItemsubtypeid"),
            params.get("pName"),
            params.get("pStandardrate"),
            params.get("pSealingrate"),
            params.get("pOpbalance"),
            params.get("pMaxlevel"),
            params.get("pMinlevel"),
            params.get("pReorderlevel"),
            params.get("pCurrentqty"),
            params.get("pSalestax"),
            params.get("pItemtypeid"),
            params.get("pUnitid"),
            params.get("pLocationid"),
            params.get("pItemgroupid"),
            params.get("pSubgroupid"),
            params.get("pAbcCode"),
            params.get("pStocktypeid"),
            params.get("pBatchrequired"),
            params.get("pExpirydtrequired"),
            params.get("pactive"),
            params.get("pSalesprice"),
            params.get("pDepartmentid"),
            params.get("pPurchasefrequency"),
            params.get("pPacksizeid"),
            params.get("pItemchangeid"),
            params.get("pUpdatedby"),
            params.get("pUpdatedon"),
            params.get("pGenericId"),
            params.get("pBrandId"),
            params.get("pVatOn"),
            params.get("pMedicineTypeId"),
            params.get("pInsertedByUserID"),
            params.get("pInsertedON"),
            params.get("pInsertedMacName"),
            params.get("pInsertedMacID"),
            params.get("pInsertedIPAddress"),
            params.get("pHmsCategoryID"),
            params.get("DescriptiveName"),
            params.get("pTaxid"),
            params.get("pChkLooseSelling"),
            params.get("pchkQualityCtrl"),
            params.get("pProductCode"),
        ]
        cursor.execute(sql, values)
        row = cursor.fetchone()
        try:
            conn.commit()
        except Exception:
            pass
        item_id = None
        if row and row[0] is not None:
            try:
                item_id = int(row[0])
            except Exception:
                item_id = row[0]
        if item_id:
            _update_iv_item_technical_specs_conn(conn, int(item_id), params.get("TechnicalSpecs"))
            return {"item_id": int(item_id)}
        return {"item_id": None}
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        return {"error": str(e)}
    finally:
        try:
            conn.close()
        except Exception:
            pass


def fetch_item_master_list(unit: str):
    """
    Calls dbo.Usp_GetIvItemId to fetch item ids/codes/names for item master search.
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Could not connect to {unit} for item master list")
        return None
    try:
        df = pd.read_sql("EXEC dbo.Usp_GetIvItemId", conn)
        if df is None or df.empty:
            return df
        df.columns = [c.strip() for c in df.columns]
        return df
    except Exception as e:
        print(f"Error fetching item master list ({unit}): {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def fetch_item_master_detail(unit: str, item_id: int):
    """
    Fetch item master details from dbo.IVItem by id.
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Could not connect to {unit} for item master detail")
        return None
    try:
        df = pd.read_sql("SELECT TOP 1 * FROM dbo.IVItem WHERE ID = ?", conn, params=[int(item_id)])
        if df is None or df.empty:
            return df
        df.columns = [c.strip() for c in df.columns]
        return df
    except Exception as e:
        print(f"Error fetching item master detail ({unit}): {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def fetch_item_manufacturer_links(unit: str, item_id: int):
    """
    Fetch manufacturer links for an item from dbo.IVItemManuLink.
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Could not connect to {unit} for item manufacturer links")
        return None
    try:
        sql = """
        SELECT
            L.LinkId,
            L.ItemID,
            L.ID AS ManufacturerID,
            M.Name AS ManufacturerName,
            M.Code AS ManufacturerCode
        FROM dbo.IVItemManuLink L
        LEFT JOIN dbo.IVMANUFACTURER M ON L.ID = M.ID
        WHERE L.ItemID = ?
        """
        df = pd.read_sql(sql, conn, params=[int(item_id)])
        if df is None or df.empty:
            return df
        df.columns = [c.strip() for c in df.columns]
        return df
    except Exception as e:
        print(f"Error fetching item manufacturer links ({unit}): {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def add_item_manufacturer_link(
    unit: str,
    item_id: int,
    manufacturer_id: int,
    user_token: str,
    now_str: str,
    remote_addr: str | None = None,
    mac_name: str | None = None,
    mac_id: str | None = None,
):
    """
    Insert a link into dbo.IVItemManuLink if it does not exist.
    """
    conn = get_sql_connection(unit)
    if not conn:
        return {"error": f"Could not connect to {unit}"}
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT LinkId FROM dbo.IVItemManuLink WHERE ItemID = ? AND ID = ?",
            int(item_id),
            int(manufacturer_id),
        )
        row = cursor.fetchone()
        if row and row[0] is not None:
            return {"link_id": row[0], "created": False}
        cursor.execute(
            """
            INSERT INTO dbo.IVItemManuLink (
                ItemID, ID, EnteredOn, EnteredBy,
                UpdatedBy, UpdatedON, UpdatedMacName,
                UpdatedMacID, UpdatedIPAddress,
                InsertedByUserID, InsertedON,
                InsertedMacName, InsertedMacID, InsertedIPAddress
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(item_id),
                int(manufacturer_id),
                now_str,
                user_token,
                user_token,
                now_str,
                mac_name,
                mac_id,
                remote_addr,
                user_token,
                now_str,
                mac_name,
                mac_id,
                remote_addr,
            ),
        )
        cursor.execute("SELECT SCOPE_IDENTITY()")
        new_row = cursor.fetchone()
        try:
            conn.commit()
        except Exception:
            pass
        link_id = None
        if new_row and new_row[0] is not None:
            try:
                link_id = int(new_row[0])
            except Exception:
                link_id = new_row[0]
        return {"link_id": link_id, "created": True}
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        return {"error": str(e)}
    finally:
        try:
            conn.close()
        except Exception:
            pass


def add_pm_indent_mst_with_autonumber(unit: str, params: dict, lock_timeout_ms: int = 10000):
    """
    Generates a new indent number under an app lock, then creates indent master.
    """
    conn = get_sql_connection(unit)
    if not conn:
        return {"error": f"Could not connect to {unit}"}
    cursor = conn.cursor()
    lock_resource = f"PM_INDENT_NO_{(unit or '').strip().upper()}"
    indent_no = None
    try:
        lock_result = _get_app_lock(cursor, lock_resource, lock_timeout_ms)
        if lock_result.get("error"):
            return lock_result
        cursor.execute("EXEC dbo.usp_GetPMPINIndentNo")
        row = cursor.fetchone()
        indent_no = str(row[0]) if row and row[0] is not None else None
        if not indent_no:
            return {"error": "Failed to generate indent number"}
        params = dict(params or {})
        params["pIndentnumber"] = indent_no
        sql = """
        EXEC dbo.usp_addPMIndent_Mst
            @pIndentid=?,
            @pIndentnumber=?,
            @pDepartmentid=?,
            @pBudgetid=?,
            @pRemarks=?,
            @pPropindication=?,
            @pIndentnature=?,
            @pDeliverystartdate=?,
            @pDeliveryenddate=?,
            @pItemcategoryid=?,
            @pUpdatedon=?,
            @pUpdatedby=?,
            @pStatus=?,
            @pAuthorisedremarks=?,
            @pAuthorisedby=?,
            @pAuthorisedon=?,
            @pAuthorised=?,
            @pProcurementId=?,
            @pStoreId=?,
            @pInsertedby=?
        """
        values = [
            params.get("pIndentid"),
            params.get("pIndentnumber"),
            params.get("pDepartmentid"),
            params.get("pBudgetid"),
            params.get("pRemarks"),
            params.get("pPropindication"),
            params.get("pIndentnature"),
            params.get("pDeliverystartdate"),
            params.get("pDeliveryenddate"),
            params.get("pItemcategoryid"),
            params.get("pUpdatedon"),
            params.get("pUpdatedby"),
            params.get("pStatus"),
            params.get("pAuthorisedremarks"),
            params.get("pAuthorisedby"),
            params.get("pAuthorisedon"),
            params.get("pAuthorised"),
            params.get("pProcurementId"),
            params.get("pStoreId"),
            params.get("pInsertedby"),
        ]
        cursor.execute(sql, values)
        row = None
        try:
            if cursor.description is not None:
                row = cursor.fetchone()
        except pyodbc.ProgrammingError:
            row = None
        try:
            conn.commit()
        except Exception:
            pass
        indent_id = None
        if row and row[0] is not None:
            try:
                indent_id = int(row[0])
            except Exception:
                indent_id = row[0]
        return {"indent_id": indent_id, "indent_no": indent_no}
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        return {"error": str(e)}
    finally:
        _release_app_lock(cursor, lock_resource)
        try:
            conn.close()
        except Exception:
            pass


def fetch_item_master_rate_mrp(unit: str, item_ids: list[int]):
    """
    Fetches standard rate, sales price (MRP), and sales tax from IVItem for the given item ids.
    """
    if not item_ids:
        return pd.DataFrame()
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Could not connect to {unit} for item master rates")
        return None
    try:
        table_name = _resolve_table_name(conn, ["IVItem", "IvItem", "IVItem_Mst", "Item_Mst"])
        if not table_name:
            return pd.DataFrame()
        id_col = _resolve_column(conn, table_name, ["ID", "Id", "ItemID", "ItemId"])
        rate_col = _resolve_column(
            conn,
            table_name,
            ["StandardRate", "Standardrate", "Standard_Rate", "Standard Rate", "PurchaseRate", "ItemRate", "Rate"],
        )
        mrp_col = _resolve_column(
            conn,
            table_name,
            ["MRP", "Mrp", "SalesPrice", "Salesprice", "Sales_Price", "Sales Price", "SalePrice"],
        )
        tax_col = _resolve_column(
            conn,
            table_name,
            ["SalesTax", "Sales_Tax", "Sales Tax", "Tax", "VAT", "Vat"],
        )
        if not id_col or (rate_col is None and mrp_col is None):
            return pd.DataFrame()
        select_cols = [f"{id_col} AS ItemID"]
        if rate_col:
            select_cols.append(f"{rate_col} AS StandardRate")
        if mrp_col:
            select_cols.append(f"{mrp_col} AS SalesPrice")
        if tax_col:
            select_cols.append(f"{tax_col} AS SalesTax")
        unique_ids = []
        seen = set()
        for item_id in item_ids:
            try:
                item_id = int(item_id)
            except Exception:
                continue
            if item_id in seen:
                continue
            seen.add(item_id)
            unique_ids.append(item_id)

        if not unique_ids:
            return pd.DataFrame()

        chunk_size = 900
        frames = []
        for i in range(0, len(unique_ids), chunk_size):
            chunk = unique_ids[i:i + chunk_size]
            placeholders = ",".join("?" for _ in chunk)
            sql = f"SELECT {', '.join(select_cols)} FROM dbo.{table_name} WHERE {id_col} IN ({placeholders})"
            df = pd.read_sql(sql, conn, params=chunk)
            if df is None or df.empty:
                continue
            df.columns = [c.strip() for c in df.columns]
            frames.append(df)

        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True, copy=False)
    except Exception as e:
        print(f"Error fetching item master rates ({unit}): {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def fetch_item_thresholds(unit: str, item_ids: list[int]):
    """
    Fetch min/reorder levels for a list of item ids from dbo.IVItem.
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Could not connect to {unit} for item thresholds")
        return {}
    try:
        ids = []
        for item_id in (item_ids or []):
            try:
                val = int(item_id)
            except Exception:
                continue
            if val > 0:
                ids.append(val)
        if not ids:
            return {}
        unique_ids = list(dict.fromkeys(ids))
        placeholders = ",".join("?" for _ in unique_ids)
        sql = f"SELECT ID, MinLevel, ReOrderLevel FROM dbo.IVItem WHERE ID IN ({placeholders})"
        df = pd.read_sql(sql, conn, params=unique_ids)
        if df is None or df.empty:
            return {}
        df.columns = [c.strip() for c in df.columns]
        cols = {str(c).strip().lower(): c for c in df.columns}
        id_col = cols.get("id")
        min_col = cols.get("minlevel")
        reorder_col = cols.get("reorderlevel") or cols.get("reorder_level")
        if not id_col:
            return {}
        out = {}
        for _, row in df.iterrows():
            item_id = row.get(id_col)
            if item_id is None:
                continue
            out[int(item_id)] = {
                "min_level": row.get(min_col) if min_col else 0,
                "reorder_level": row.get(reorder_col) if reorder_col else 0,
            }
        return out
    except Exception as e:
        print(f"Error fetching item thresholds ({unit}): {e}")
        return {}
    finally:
        try:
            conn.close()
        except Exception:
            pass


def update_iv_item(unit: str, params: dict):
    """
    Executes dbo.usp_updateIVItem to update an item master.
    """
    conn = get_sql_connection(unit)
    if not conn:
        return {"error": f"Could not connect to {unit}"}
    try:
        cursor = conn.cursor()
        sql = """
        EXEC dbo.usp_updateIVItem
            @pId=?,
            @pCode=?,
            @pItemsubtypeid=?,
            @pName=?,
            @pStandardrate=?,
            @pSealingrate=?,
            @pOpbalance=?,
            @pMaxlevel=?,
            @pMinlevel=?,
            @pReorderlevel=?,
            @pCurrentqty=?,
            @pSalestax=?,
            @pItemtypeid=?,
            @pUnitid=?,
            @pLocationid=?,
            @pItemgroupid=?,
            @pSubgroupid=?,
            @pAbcCode=?,
            @pStocktypeid=?,
            @pBatchrequired=?,
            @pExpirydtrequired=?,
            @pActive=?,
            @pSalesprice=?,
            @pDepartmentid=?,
            @pPurchasefrequency=?,
            @pPacksizeid=?,
            @pItemchangeid=?,
            @pUpdatedby=?,
            @pUpdatedon=?,
            @pGenericId=?,
            @pBrandId=?,
            @pVatOn=?,
            @pMedicineTypeId=?,
            @pHmscategoryID=?,
            @pUpdatedMacID=?,
            @pUpdatedIPAddress=?,
            @DescriptiveName=?,
            @pTaxid=?,
            @pChkLooseSelling=?,
            @pchkQualityCtrl=?,
            @pProductCode=?
        """
        values = [
            params.get("pId"),
            params.get("pCode"),
            params.get("pItemsubtypeid"),
            params.get("pName"),
            params.get("pStandardrate"),
            params.get("pSealingrate"),
            params.get("pOpbalance"),
            params.get("pMaxlevel"),
            params.get("pMinlevel"),
            params.get("pReorderlevel"),
            params.get("pCurrentqty"),
            params.get("pSalestax"),
            params.get("pItemtypeid"),
            params.get("pUnitid"),
            params.get("pLocationid"),
            params.get("pItemgroupid"),
            params.get("pSubgroupid"),
            params.get("pAbcCode"),
            params.get("pStocktypeid"),
            params.get("pBatchrequired"),
            params.get("pExpirydtrequired"),
            params.get("pActive"),
            params.get("pSalesprice"),
            params.get("pDepartmentid"),
            params.get("pPurchasefrequency"),
            params.get("pPacksizeid"),
            params.get("pItemchangeid"),
            params.get("pUpdatedby"),
            params.get("pUpdatedon"),
            params.get("pGenericId"),
            params.get("pBrandId"),
            params.get("pVatOn"),
            params.get("pMedicineTypeId"),
            params.get("pHmscategoryID"),
            params.get("pUpdatedMacID"),
            params.get("pUpdatedIPAddress"),
            params.get("DescriptiveName"),
            params.get("pTaxid"),
            params.get("pChkLooseSelling"),
            params.get("pchkQualityCtrl"),
            params.get("pProductCode"),
        ]
        cursor.execute(sql, values)
        try:
            conn.commit()
        except Exception:
            pass
        try:
            item_id = int(params.get("pId") or 0)
        except Exception:
            item_id = 0
        if item_id > 0:
            _update_iv_item_technical_specs_conn(conn, item_id, params.get("TechnicalSpecs"))
        return {"status": "success"}
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        return {"error": str(e)}
    finally:
        try:
            conn.close()
        except Exception:
            pass


def add_iv_po_mst(unit: str, params: dict):
    """
    Executes dbo.usp_addIVPoMst. Params must include all proc parameters by name.
    Returns the created PO id (int) if available.
    """
    conn = get_sql_connection(unit)
    if not conn:
        return {"error": f"Could not connect to {unit}"}
    try:
        params = _trim_po_mst_params(conn, params)
        cursor = conn.cursor()
        sql = """
        EXEC dbo.usp_addIVPoMst
            @pId=?,
            @pSupplierid=?,
            @pTenderid=?,
            @pPono=?,
            @pPodate=?,
            @pDeliveryterms=?,
            @pPaymentsterms=?,
            @pOtherterms=?,
            @pTaxid=?,
            @pTax=?,
            @pDiscount=?,
            @pAmount=?,
            @pCreditdays=?,
            @pPocomplete=?,
            @pNotes=?,
            @pPreparedby=?,
            @pCustom1=?,
            @pCustom2=?,
            @pUpdatedby=?,
            @pUpdatedon=?,
            @pSignauthorityperson=?,
            @pSignauthoritypdesig=?,
            @pRefno=?,
            @pSubject=?,
            @pAuthorizationid=?,
            @pPurchaseIndentId=?,
            @pInsertedByUserID=?,
            @pInsertedON=?,
            @pInsertedMacName=?,
            @pInsertedMacID=?,
            @pInsertedIPAddress=?,
            @Against=?,
            @QuotationId=?,
            @TotalFORe=?,
            @TotalExciseAmt=?,
            @AgainstId=?,
            @Status=?
        """
        values = [
            params.get("pId"),
            params.get("pSupplierid"),
            params.get("pTenderid"),
            params.get("pPono"),
            params.get("pPodate"),
            params.get("pDeliveryterms"),
            params.get("pPaymentsterms"),
            params.get("pOtherterms"),
            params.get("pTaxid"),
            params.get("pTax"),
            params.get("pDiscount"),
            params.get("pAmount"),
            params.get("pCreditdays"),
            params.get("pPocomplete"),
            params.get("pNotes"),
            params.get("pPreparedby"),
            params.get("pCustom1"),
            params.get("pCustom2"),
            params.get("pUpdatedby"),
            params.get("pUpdatedon"),
            params.get("pSignauthorityperson"),
            params.get("pSignauthoritypdesig"),
            params.get("pRefno"),
            params.get("pSubject"),
            params.get("pAuthorizationid"),
            params.get("pPurchaseIndentId"),
            params.get("pInsertedByUserID"),
            params.get("pInsertedON"),
            params.get("pInsertedMacName"),
            params.get("pInsertedMacID"),
            params.get("pInsertedIPAddress"),
            params.get("Against"),
            params.get("QuotationId"),
            params.get("TotalFORe"),
            params.get("TotalExciseAmt"),
            params.get("AgainstId"),
            params.get("Status"),
        ]
        cursor.execute(sql, values)
        row = cursor.fetchone()
        try:
            conn.commit()
        except Exception:
            pass
        po_id = None
        if row and row[0] is not None:
            try:
                po_id = int(row[0])
            except Exception:
                po_id = row[0]
        if po_id:
            _update_iv_po_mst_purchasing_dept_conn(conn, int(po_id), params.get("PurchasingDeptId"))
            _update_iv_po_mst_special_notes_conn(conn, int(po_id), params.get("pSpecialNotes"))
            _update_iv_po_mst_senior_approval_authority_conn(conn, int(po_id), params.get("SeniorApprovalAuthorityName"))
            _update_iv_po_mst_senior_approval_designation_conn(conn, int(po_id), params.get("SeniorApprovalAuthorityDesignation"))
            return {"po_id": int(po_id)}
        return {"po_id": None}
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        return {"error": str(e)}
    finally:
        try:
            conn.close()
        except Exception:
            pass


_PO_DTL_INSERT_SQL = """
        EXEC dbo.usp_addIVPoDtl
            @pId=?,
            @pPoid=?,
            @pItemid=?,
            @pQty=?,
            @pPackSizeId=?,
            @pRate=?,
            @pFreeqty=?,
            @pDiscount=?,
            @pTax=?,
            @pTaxamount=?,
            @pMRP=?,
            @pVATOn=?,
            @pVAT=?,
            @pCustom1=?,
            @pCustom2=?,
            @pInsertedByUserID=?,
            @pInsertedON=?,
            @pInsertedMacName=?,
            @pInsertedMacID=?,
            @pInsertedIPAddress=?,
            @Fore=?,
            @Excisetax=?,
            @ExciseTaxamt=?,
            @NetAmount=?,
            @lendingRate=?,
            @UnitFor=?,
            @UnitDiscount=?
        """


def _po_dtl_insert_values(params: dict) -> list:
    return [
        params.get("pId"),
        params.get("pPoid"),
        params.get("pItemid"),
        params.get("pQty"),
        params.get("pPackSizeId"),
        params.get("pRate"),
        params.get("pFreeqty"),
        params.get("pDiscount"),
        params.get("pTax"),
        params.get("pTaxamount"),
        params.get("pMRP"),
        params.get("pVATOn"),
        params.get("pVAT"),
        params.get("pCustom1"),
        params.get("pCustom2"),
        params.get("pInsertedByUserID"),
        params.get("pInsertedON"),
        params.get("pInsertedMacName"),
        params.get("pInsertedMacID"),
        params.get("pInsertedIPAddress"),
        params.get("Fore"),
        params.get("Excisetax"),
        params.get("ExciseTaxamt"),
        params.get("NetAmount"),
        params.get("lendingRate"),
        params.get("UnitFor"),
        params.get("UnitDiscount"),
    ]


def add_iv_po_dtl(unit: str, params: dict):
    """
    Executes dbo.usp_addIVPoDtl for a single item row.
    """
    conn = get_sql_connection(unit)
    if not conn:
        return {"error": f"Could not connect to {unit}"}
    try:
        cursor = conn.cursor()
        cursor.execute(_PO_DTL_INSERT_SQL, _po_dtl_insert_values(params))
        try:
            conn.commit()
        except Exception:
            pass
        return {"success": True}
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        return {"error": str(e)}
    finally:
        try:
            conn.close()
        except Exception:
            pass


def add_iv_po_dtl_many(unit: str, params_list: list[dict]):
    """
    Executes dbo.usp_addIVPoDtl for multiple rows using a single DB connection.
    Returns per-row errors while continuing remaining rows.
    """
    rows = list(params_list or [])
    if not rows:
        return {"success": True, "success_count": 0, "errors": []}
    conn = get_sql_connection(unit)
    if not conn:
        return {"error": f"Could not connect to {unit}"}
    try:
        cursor = conn.cursor()
        success_count = 0
        errors = []
        for idx, params in enumerate(rows, start=1):
            try:
                cursor.execute(_PO_DTL_INSERT_SQL, _po_dtl_insert_values(params))
                try:
                    conn.commit()
                except Exception:
                    pass
                success_count += 1
            except Exception as e:
                try:
                    conn.rollback()
                except Exception:
                    pass
                row_no = params.get("_row_no") or idx
                item_name = str(params.get("_item_name") or "").strip()
                label = f"Row {row_no}"
                if item_name:
                    label += f" ({item_name})"
                errors.append(f"{label}: {e}")
        return {"success": True, "success_count": success_count, "errors": errors}
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        return {"error": str(e)}
    finally:
        try:
            conn.close()
        except Exception:
            pass


# ===================== Pharmacy: GSS Issue/Return Register =====================
def fetch_pharmacy_gss_issue_register(unit: str, from_date: str, to_date: str):
    """
    Fetch GSS Issue/Return register data for the given date range.
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Aƒ?§A?A-AA¨A_A, Pharmacy GSS register: could not connect to {unit}")
        return None
    try:
        sql = "SET NOCOUNT ON; EXEC dbo.usp_rptpatissueregisterGSS @fromdate=?, @todate=?"
        df = pd.read_sql(sql, conn, params=[from_date, to_date])
        if df is None or df.empty:
            return df
        df.columns = [c.strip() for c in df.columns]
        df["Unit"] = (unit or "").upper()
        return df
    except Exception as e:
        print(f"Aƒ?§A?' Pharmacy GSS register fetch failed for {unit}: {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def fetch_pharmacy_ward_medicine_tat(unit: str, from_date: str, to_date: str):
    """
    Fetch ward medicine TAT lines (indent -> issue -> ward acknowledgement).
    Date filter is applied on indent date.
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Pharmacy ward TAT: could not connect to {unit}")
        return None
    try:
        indent_table = _resolve_table_name(conn, ["IVPatientIndentMst", "IvPatientIndentMst"])
        issue_mst_table = _resolve_table_name(conn, ["IvPatientIssueMst", "IVPatientIssueMst"])
        issue_dtl_table = _resolve_table_name(conn, ["IvPatientIssueDtl", "IVPatientIssueDtl"])
        item_table = _resolve_table_name(conn, ["IVItem", "IvItem"])
        user_table = _resolve_table_name(conn, ["User_Mst", "user_mst", "UserMst"])

        if not indent_table or not issue_mst_table or not issue_dtl_table:
            print(
                "Pharmacy ward TAT: missing required tables "
                f"(indent={indent_table}, issue_mst={issue_mst_table}, issue_dtl={issue_dtl_table})"
            )
            return pd.DataFrame()

        indent_id_col = _resolve_column(conn, indent_table, ["IndentID", "IndentId", "Indent_ID", "ID", "Id"])
        indent_no_col = _resolve_column(conn, indent_table, ["IndentNo", "Indent_No", "IndentNumber"])
        indent_date_col = _resolve_column(conn, indent_table, ["IndentDate", "Indent_Date", "InsertedON", "InsertedOn"])
        indent_patient_col = _resolve_column(conn, indent_table, ["PatientID", "PatientId", "Patient_ID"])
        indent_visit_col = _resolve_column(conn, indent_table, ["VisitID", "VisitId", "Visit_ID"])
        indent_cancel_col = _resolve_column(conn, indent_table, ["CancelStatus", "Canceled", "Cancelled", "IsCancel", "IsCanceled"])

        issue_id_col = _resolve_column(conn, issue_mst_table, ["IssueId", "IssueID", "Issue_Id", "ID", "Id"])
        issue_no_col = _resolve_column(conn, issue_mst_table, ["IssueNo", "Issue_No"])
        issue_date_col = _resolve_column(conn, issue_mst_table, ["IssueDate", "Issue_Date", "InsertedON", "InsertedOn"])
        issue_indent_col = _resolve_column(conn, issue_mst_table, ["IndentId", "IndentID", "Indent_ID"])
        issue_patient_col = _resolve_column(conn, issue_mst_table, ["PatientID", "PatientId", "Patient_ID"])
        issue_visit_col = _resolve_column(conn, issue_mst_table, ["VisitID", "VisitId", "Visit_ID"])
        issue_ack_status_col = _resolve_column(conn, issue_mst_table, ["acknowStatus", "AcknowStatus", "AcknowledgementStatus", "Accepted"])

        issue_dtl_id_col = _resolve_column(conn, issue_dtl_table, ["IssueDtlId", "IssueDtlID", "IssueDtl_Id", "ID", "Id"])
        issue_dtl_issue_col = _resolve_column(conn, issue_dtl_table, ["Issueid", "IssueId", "IssueID", "Issue_ID"])
        issue_dtl_item_col = _resolve_column(conn, issue_dtl_table, ["ItemId", "ItemID", "Item_Id"])
        issue_dtl_qty_col = _resolve_column(conn, issue_dtl_table, ["Issueqty", "IssueQty", "Qty", "Quantity"])
        issue_dtl_ack_by_col = _resolve_column(conn, issue_dtl_table, ["acknowBy", "AcknowBy", "AcknowledgedBy"])
        issue_dtl_ack_dt_col = _resolve_column(conn, issue_dtl_table, ["acknowDatetime", "AcknowDatetime", "AcknowledgedOn", "AcknowledgedDate"])

        if not indent_id_col or not indent_date_col or not issue_id_col or not issue_indent_col or not issue_dtl_issue_col:
            print(
                "Pharmacy ward TAT: missing required columns "
                f"(indent_id={indent_id_col}, indent_date={indent_date_col}, issue_id={issue_id_col}, "
                f"issue_indent={issue_indent_col}, issue_dtl_issue={issue_dtl_issue_col})"
            )
            return pd.DataFrame()

        patient_expr_parts = []
        if issue_patient_col:
            patient_expr_parts.append(f"im.{issue_patient_col}")
        if indent_patient_col:
            patient_expr_parts.append(f"ind.{indent_patient_col}")
        if not patient_expr_parts:
            patient_expr = "NULL"
        elif len(patient_expr_parts) == 1:
            patient_expr = patient_expr_parts[0]
        else:
            patient_expr = f"COALESCE({', '.join(patient_expr_parts)})"

        visit_expr_parts = []
        if issue_visit_col:
            visit_expr_parts.append(f"im.{issue_visit_col}")
        if indent_visit_col:
            visit_expr_parts.append(f"ind.{indent_visit_col}")
        if not visit_expr_parts:
            visit_expr = "NULL"
        elif len(visit_expr_parts) == 1:
            visit_expr = visit_expr_parts[0]
        else:
            visit_expr = f"COALESCE({', '.join(visit_expr_parts)})"

        patient_name_expr = (
            f"CASE WHEN {patient_expr} IS NULL THEN NULL ELSE dbo.fn_patientfullname({patient_expr}) END AS PatientName"
            if patient_expr != "NULL"
            else "NULL AS PatientName"
        )
        reg_no_expr = (
            f"CASE WHEN {patient_expr} IS NULL THEN NULL ELSE dbo.fn_regno({patient_expr}) END AS RegistrationNo"
            if patient_expr != "NULL"
            else "NULL AS RegistrationNo"
        )

        issue_no_expr = (
            f"im.{issue_no_col} AS IssueNo"
            if issue_no_col
            else f"CAST(im.{issue_id_col} AS VARCHAR(50)) AS IssueNo"
        )
        indent_no_expr = (
            f"ind.{indent_no_col} AS IndentNo"
            if indent_no_col
            else f"CAST(ind.{indent_id_col} AS VARCHAR(50)) AS IndentNo"
        )
        issue_date_expr = f"im.{issue_date_col} AS IssueDate" if issue_date_col else "NULL AS IssueDate"
        issue_dtl_id_expr = f"dtl.{issue_dtl_id_col} AS IssueDtlId" if issue_dtl_id_col else "NULL AS IssueDtlId"
        issue_qty_expr = f"dtl.{issue_dtl_qty_col} AS IssueQty" if issue_dtl_qty_col else "NULL AS IssueQty"
        ack_by_expr = f"dtl.{issue_dtl_ack_by_col} AS AcknowBy" if issue_dtl_ack_by_col else "NULL AS AcknowBy"
        ack_dt_expr = f"dtl.{issue_dtl_ack_dt_col} AS AcknowDatetime" if issue_dtl_ack_dt_col else "NULL AS AcknowDatetime"
        ack_status_expr = f"im.{issue_ack_status_col} AS AcknowStatus" if issue_ack_status_col else "NULL AS AcknowStatus"

        tat_indent_issue_expr = (
            f"CASE WHEN ind.{indent_date_col} IS NOT NULL AND im.{issue_date_col} IS NOT NULL "
            f"THEN DATEDIFF(MINUTE, ind.{indent_date_col}, im.{issue_date_col}) END AS TatIndentToIssueMinutes"
            if issue_date_col
            else "NULL AS TatIndentToIssueMinutes"
        )
        tat_issue_ack_expr = (
            f"CASE WHEN im.{issue_date_col} IS NOT NULL AND dtl.{issue_dtl_ack_dt_col} IS NOT NULL "
            f"THEN DATEDIFF(MINUTE, im.{issue_date_col}, dtl.{issue_dtl_ack_dt_col}) END AS TatIssueToAckMinutes"
            if issue_date_col and issue_dtl_ack_dt_col
            else "NULL AS TatIssueToAckMinutes"
        )
        tat_indent_ack_expr = (
            f"CASE WHEN ind.{indent_date_col} IS NOT NULL AND dtl.{issue_dtl_ack_dt_col} IS NOT NULL "
            f"THEN DATEDIFF(MINUTE, ind.{indent_date_col}, dtl.{issue_dtl_ack_dt_col}) END AS TatIndentToAckMinutes"
            if issue_dtl_ack_dt_col
            else "NULL AS TatIndentToAckMinutes"
        )

        item_join_sql = ""
        item_id_expr = f"dtl.{issue_dtl_item_col} AS ItemId" if issue_dtl_item_col else "NULL AS ItemId"
        item_code_expr = "NULL AS ItemCode"
        item_name_expr = "NULL AS ItemName"
        if item_table and issue_dtl_item_col:
            item_id_col = _resolve_column(conn, item_table, ["ID", "Id", "ItemID", "ItemId"])
            item_code_col = _resolve_column(conn, item_table, ["Code", "ItemCode"])
            item_name_col = _resolve_column(conn, item_table, ["Name", "ItemName", "DescriptiveName"])
            if item_id_col:
                item_join_sql = f"LEFT JOIN dbo.{item_table} it WITH (NOLOCK) ON it.{item_id_col} = dtl.{issue_dtl_item_col}"
                if item_code_col:
                    item_code_expr = f"it.{item_code_col} AS ItemCode"
                if item_name_col:
                    item_name_expr = f"it.{item_name_col} AS ItemName"

        user_join_sql = ""
        if user_table and issue_dtl_ack_by_col:
            user_id_col = _resolve_column(conn, user_table, ["UserID", "UserId", "ID", "Id"])
            user_name_col = _resolve_column(conn, user_table, ["UserName", "User_Name", "Name", "LoginName"])
            if user_id_col and user_name_col:
                user_join_sql = (
                    f"LEFT JOIN dbo.{user_table} um WITH (NOLOCK) "
                    f"ON LTRIM(RTRIM(CAST(um.{user_id_col} AS NVARCHAR(50)))) = "
                    f"LTRIM(RTRIM(CAST(dtl.{issue_dtl_ack_by_col} AS NVARCHAR(50))))"
                )
                ack_by_expr = (
                    f"COALESCE(NULLIF(LTRIM(RTRIM(CAST(um.{user_name_col} AS NVARCHAR(200)))), ''), "
                    f"NULLIF(LTRIM(RTRIM(CAST(dtl.{issue_dtl_ack_by_col} AS NVARCHAR(200)))), '')) AS AcknowBy"
                )
            else:
                ack_by_expr = (
                    f"NULLIF(LTRIM(RTRIM(CAST(dtl.{issue_dtl_ack_by_col} AS NVARCHAR(200)))), '') AS AcknowBy"
                )

        visit_join_sql = ""
        ward_expr = "NULL AS WardName"
        visit_table = _resolve_table_name(conn, ["Visit", "visit", "Visit_Mst", "VisitMst"])
        if visit_table and visit_expr != "NULL":
            visit_id_col = _resolve_column(conn, visit_table, ["VisitId", "VisitID", "Visit_Id", "ID", "Id"])
            ward_id_col = _resolve_column(conn, visit_table, ["WardId", "WardID", "Ward_Id", "CurrentWardId", "CurrentWardID"])
            if visit_id_col:
                visit_join_sql = f"LEFT JOIN dbo.{visit_table} v WITH (NOLOCK) ON v.{visit_id_col} = {visit_expr}"
                if ward_id_col:
                    ward_expr = f"CASE WHEN v.{ward_id_col} IS NULL THEN NULL ELSE dbo.Fn_Ward_Name(v.{ward_id_col}) END AS WardName"

        where_sql = [f"ind.{indent_date_col} >= ?", f"ind.{indent_date_col} < DATEADD(DAY, 1, ?)"]
        if indent_cancel_col:
            where_sql.append(f"ISNULL(ind.{indent_cancel_col}, 0) = 0")
        where_clause = " AND ".join(where_sql)

        if issue_date_col:
            order_expr = f"ind.{indent_date_col} DESC, im.{issue_date_col} DESC, im.{issue_id_col} DESC"
        else:
            order_expr = f"ind.{indent_date_col} DESC, im.{issue_id_col} DESC"
        if issue_dtl_id_col:
            order_expr += f", dtl.{issue_dtl_id_col} DESC"

        sql = f"""
            SET NOCOUNT ON;
            SELECT
                ind.{indent_id_col} AS IndentID,
                {indent_no_expr},
                ind.{indent_date_col} AS IndentDate,
                im.{issue_id_col} AS IssueId,
                {issue_no_expr},
                {issue_date_expr},
                {issue_dtl_id_expr},
                {visit_expr} AS VisitId,
                {patient_expr} AS PatientId,
                {patient_name_expr},
                {reg_no_expr},
                {ward_expr},
                {item_id_expr},
                {item_code_expr},
                {item_name_expr},
                {issue_qty_expr},
                {ack_status_expr},
                {ack_by_expr},
                {ack_dt_expr},
                {tat_indent_issue_expr},
                {tat_issue_ack_expr},
                {tat_indent_ack_expr}
            FROM dbo.{issue_mst_table} im WITH (NOLOCK)
            INNER JOIN dbo.{indent_table} ind WITH (NOLOCK)
                ON im.{issue_indent_col} = ind.{indent_id_col}
            INNER JOIN dbo.{issue_dtl_table} dtl WITH (NOLOCK)
                ON dtl.{issue_dtl_issue_col} = im.{issue_id_col}
            {item_join_sql}
            {user_join_sql}
            {visit_join_sql}
            WHERE {where_clause}
            ORDER BY {order_expr}
        """

        df = pd.read_sql(sql, conn, params=[from_date, to_date])
        if df is None or df.empty:
            return df
        df.columns = [c.strip() for c in df.columns]
        df["Unit"] = (unit or "").upper()
        return df
    except Exception as e:
        print(f"Pharmacy ward TAT fetch failed for {unit}: {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def add_iv_po_mst_with_autonumber(unit: str, params: dict, lock_timeout_ms: int = 10000):
    """
    Generates a new PO number under an app lock, then creates PO master.
    """
    conn = get_sql_connection(unit)
    if not conn:
        return {"error": f"Could not connect to {unit}"}
    cursor = conn.cursor()
    lock_resource = f"IVPO_NO_{(unit or '').strip().upper()}"
    po_no = None
    po_table_name = None
    po_id_col = None
    po_no_col = None
    try:
        po_table_name = _resolve_table_name(conn, ["IVPoMst", "IvPoMst", "IVPO_MST", "IV_Po_Mst"])
        if po_table_name:
            po_id_col = _resolve_column(conn, po_table_name, ["ID", "Id"]) or "ID"
            po_no_col = _resolve_column(conn, po_table_name, ["PONo", "PoNo", "PO_No"]) or "PONo"
        po_id_match_expr = (
            f"LTRIM(RTRIM(CAST([{po_id_col}] AS NVARCHAR(50))))"
            if po_id_col
            else ""
        )

        def _fetch_saved_po_no(target_po_id: int) -> str:
            if not po_table_name or not po_id_col or not po_no_col:
                return ""
            try:
                cur = conn.cursor()
                cur.execute(
                    f"""
                    SELECT TOP 1 [{po_no_col}]
                    FROM dbo.[{po_table_name}]
                    WHERE {po_id_match_expr} = ?
                    """,
                    (str(int(target_po_id)),),
                )
                row = cur.fetchone()
                return str(row[0] or "").strip() if row else ""
            except Exception:
                return ""

        def _set_saved_po_no_if_blank(target_po_id: int, po_no_value: str) -> bool:
            if not po_no_value or not po_table_name or not po_id_col or not po_no_col:
                return False
            try:
                cur = conn.cursor()
                cur.execute(
                    f"""
                    UPDATE dbo.[{po_table_name}]
                    SET [{po_no_col}] = ?
                    WHERE {po_id_match_expr} = ?
                      AND NULLIF(LTRIM(RTRIM(CAST([{po_no_col}] AS NVARCHAR(100)))), '') IS NULL
                    """,
                    (po_no_value, str(int(target_po_id))),
                )
                try:
                    conn.commit()
                except Exception:
                    pass
                return True
            except Exception:
                try:
                    conn.rollback()
                except Exception:
                    pass
                return False

        lock_result = _get_app_lock(cursor, lock_resource, lock_timeout_ms)
        if lock_result.get("error"):
            return lock_result
        cursor.execute("EXEC dbo.usp_GetIvPONo")
        row = cursor.fetchone()
        po_no = str(row[0]) if row and row[0] is not None else None
        if not po_no:
            return {"error": "Failed to generate PO number"}
        params = dict(params or {})
        params["pPono"] = po_no
        params = _trim_po_mst_params(conn, params)
        sql = """
        EXEC dbo.usp_addIVPoMst
            @pId=?,
            @pSupplierid=?,
            @pTenderid=?,
            @pPono=?,
            @pPodate=?,
            @pDeliveryterms=?,
            @pPaymentsterms=?,
            @pOtherterms=?,
            @pTaxid=?,
            @pTax=?,
            @pDiscount=?,
            @pAmount=?,
            @pCreditdays=?,
            @pPocomplete=?,
            @pNotes=?,
            @pPreparedby=?,
            @pCustom1=?,
            @pCustom2=?,
            @pUpdatedby=?,
            @pUpdatedon=?,
            @pSignauthorityperson=?,
            @pSignauthoritypdesig=?,
            @pRefno=?,
            @pSubject=?,
            @pAuthorizationid=?,
            @pPurchaseIndentId=?,
            @pInsertedByUserID=?,
            @pInsertedON=?,
            @pInsertedMacName=?,
            @pInsertedMacID=?,
            @pInsertedIPAddress=?,
            @Against=?,
            @QuotationId=?,
            @TotalFORe=?,
            @TotalExciseAmt=?,
            @AgainstId=?,
            @Status=?
        """
        values = [
            params.get("pId"),
            params.get("pSupplierid"),
            params.get("pTenderid"),
            params.get("pPono"),
            params.get("pPodate"),
            params.get("pDeliveryterms"),
            params.get("pPaymentsterms"),
            params.get("pOtherterms"),
            params.get("pTaxid"),
            params.get("pTax"),
            params.get("pDiscount"),
            params.get("pAmount"),
            params.get("pCreditdays"),
            params.get("pPocomplete"),
            params.get("pNotes"),
            params.get("pPreparedby"),
            params.get("pCustom1"),
            params.get("pCustom2"),
            params.get("pUpdatedby"),
            params.get("pUpdatedon"),
            params.get("pSignauthorityperson"),
            params.get("pSignauthoritypdesig"),
            params.get("pRefno"),
            params.get("pSubject"),
            params.get("pAuthorizationid"),
            params.get("pPurchaseIndentId"),
            params.get("pInsertedByUserID"),
            params.get("pInsertedON"),
            params.get("pInsertedMacName"),
            params.get("pInsertedMacID"),
            params.get("pInsertedIPAddress"),
            params.get("Against"),
            params.get("QuotationId"),
            params.get("TotalFORe"),
            params.get("TotalExciseAmt"),
            params.get("AgainstId"),
            params.get("Status"),
        ]
        cursor.execute(sql, values)
        row = cursor.fetchone()
        try:
            conn.commit()
        except Exception:
            pass
        po_id = None
        if row and row[0] is not None:
            try:
                po_id = int(row[0])
            except Exception:
                po_id = row[0]
        if po_id:
            saved_po_no = _fetch_saved_po_no(int(po_id))
            if not saved_po_no and po_no:
                _set_saved_po_no_if_blank(int(po_id), str(po_no))
                saved_po_no = _fetch_saved_po_no(int(po_id))
            final_po_no = str(saved_po_no or po_no or "").strip()
            _update_iv_po_mst_purchasing_dept_conn(conn, int(po_id), params.get("PurchasingDeptId"))
            _update_iv_po_mst_special_notes_conn(conn, int(po_id), params.get("pSpecialNotes"))
            _update_iv_po_mst_senior_approval_authority_conn(conn, int(po_id), params.get("SeniorApprovalAuthorityName"))
            _update_iv_po_mst_senior_approval_designation_conn(conn, int(po_id), params.get("SeniorApprovalAuthorityDesignation"))
            return {"po_id": int(po_id), "po_no": final_po_no}
        return {"po_id": None, "po_no": str(po_no or "").strip()}
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        return {"error": str(e)}
    finally:
        _release_app_lock(cursor, lock_resource)
        try:
            conn.close()
        except Exception:
            pass


def ensure_purchase_po_number(unit: str, po_id: int, preferred_po_no: str | None = None, lock_timeout_ms: int = 10000):
    """
    Ensures IVPoMst has a non-empty PO number for the provided PO ID.
    """
    try:
        target_po_id = int(po_id)
    except Exception:
        return {"error": "Invalid PO ID"}
    if target_po_id <= 0:
        return {"error": "Invalid PO ID"}

    conn = get_sql_connection(unit)
    if not conn:
        return {"error": f"Could not connect to {unit}"}

    cursor = conn.cursor()
    lock_resource = f"IVPO_NO_{(unit or '').strip().upper()}"
    lock_acquired = False
    try:
        po_table_name = _resolve_table_name(conn, ["IVPoMst", "IvPoMst", "IVPO_MST", "IV_Po_Mst"])
        if not po_table_name:
            return {"error": "IVPoMst table not found"}
        po_id_col = _resolve_column(conn, po_table_name, ["ID", "Id"]) or "ID"
        po_no_col = _resolve_column(conn, po_table_name, ["PONo", "PoNo", "PO_No"]) or "PONo"
        po_id_match_expr = f"LTRIM(RTRIM(CAST([{po_id_col}] AS NVARCHAR(50))))"

        def _fetch_saved_po_no() -> str:
            try:
                cur = conn.cursor()
                cur.execute(
                    f"""
                    SELECT TOP 1 [{po_no_col}]
                    FROM dbo.[{po_table_name}]
                    WHERE {po_id_match_expr} = ?
                    """,
                    (str(target_po_id),),
                )
                row = cur.fetchone()
                return str(row[0] or "").strip() if row else ""
            except Exception:
                return ""

        current_po_no = _fetch_saved_po_no()
        if current_po_no:
            return {"po_no": current_po_no, "updated": False}

        po_no_value = str(preferred_po_no or "").strip()
        if not po_no_value:
            lock_result = _get_app_lock(cursor, lock_resource, lock_timeout_ms)
            if lock_result.get("error"):
                return lock_result
            lock_acquired = True
            cursor.execute("EXEC dbo.usp_GetIvPONo")
            row = cursor.fetchone()
            po_no_value = str(row[0] or "").strip() if row and row[0] is not None else ""
            if not po_no_value:
                return {"error": "Failed to generate PO number"}

        cur = conn.cursor()
        cur.execute(
            f"""
            UPDATE dbo.[{po_table_name}]
            SET [{po_no_col}] = ?
            WHERE {po_id_match_expr} = ?
              AND NULLIF(LTRIM(RTRIM(CAST([{po_no_col}] AS NVARCHAR(100)))), '') IS NULL
            """,
            (po_no_value, str(target_po_id)),
        )
        try:
            updated = int(cur.rowcount or 0) > 0
        except Exception:
            updated = False
        try:
            conn.commit()
        except Exception:
            pass

        saved_po_no = _fetch_saved_po_no()
        final_po_no = str(saved_po_no or po_no_value).strip()
        return {"po_no": final_po_no, "updated": updated}
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        return {"error": str(e)}
    finally:
        if lock_acquired:
            _release_app_lock(cursor, lock_resource)
        try:
            conn.close()
        except Exception:
            pass


# ===================== Pharmacy: Itemwise Consumption =====================
def fetch_pharmacy_itemwise_consumption(
    unit: str,
    from_date: str,
    to_date: str,
    doc_type: str | None = None,
    doc_id: int | None = None,
):
    """
    Calls dbo.usp_Itemwiseconsumption @FromDate, @ToDate, @DocType, @DocId.
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Pharmacy itemwise consumption: could not connect to {unit}")
        return None
    try:
        sql = "SET NOCOUNT ON; EXEC dbo.usp_Itemwiseconsumption @FromDate=?, @ToDate=?, @DocType=?, @DocId=?"
        params = [
            from_date,
            to_date,
            (doc_type.strip().upper() if doc_type else None),
            doc_id,
        ]
        df = pd.read_sql(sql, conn, params=params)
        if df is None or df.empty:
            return df
        df.columns = [c.strip() for c in df.columns]
        df["Unit"] = (unit or "").upper()
        return df
    except Exception as e:
        print(f"Pharmacy itemwise consumption fetch failed for {unit}: {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


# ===================== Pharmacy: Expired Items =====================
def fetch_pharmacy_expired_items(unit: str, from_date: str, to_date: str):
    """
    Calls dbo.usp_PHExpiredItems @FromDate, @ToDate for a given unit.
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Pharmacy expired items: could not connect to {unit}")
        return None
    try:
        sql = "SET NOCOUNT ON; EXEC dbo.usp_PHExpiredItems @FromDate=?, @ToDate=?"
        df = pd.read_sql(sql, conn, params=[from_date, to_date])
        if df is None or df.empty:
            return df
        df.columns = [c.strip() for c in df.columns]
        df["Unit"] = (unit or "").upper()
        return df
    except Exception as e:
        print(f"Pharmacy expired items fetch failed for {unit}: {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


# ===================== Pharmacy: Non-Moving Items =====================
def fetch_pharmacy_non_moving_items(unit: str, store_id: int):
    """
    Calls dbo.usp_RptNonMoving @storeid for a given unit.
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Pharmacy non-moving items: could not connect to {unit}")
        return None
    try:
        sql = "SET NOCOUNT ON; EXEC dbo.usp_RptNonMoving @storeid=?"
        df = pd.read_sql(sql, conn, params=[int(store_id or 0)])
        if df is None or df.empty:
            return df
        df.columns = [c.strip() for c in df.columns]
        df["Unit"] = (unit or "").upper()
        return df
    except Exception as e:
        print(f"Pharmacy non-moving items fetch failed for {unit}: {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


# ===================== Discharge-wise bills (Billing/Collections) =====================
def fetch_discharge_bills(unit: str, from_date: str, to_date: str):
    """
    Calls dbo.usp_RptDischargewisebills @FromDate, @ToDate for a given unit.
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"âš ï¸ Could not connect to {unit} for discharge bills")
        return None
    try:
        # Try to run under SNAPSHOT to satisfy DB/proc; fall back to READ COMMITTED if unavailable.
        try:
            cur = conn.cursor()
            cur.execute("SET IMPLICIT_TRANSACTIONS OFF; SET TRANSACTION ISOLATION LEVEL SNAPSHOT;")
            cur.close()
            sql = "SET NOCOUNT ON; EXEC dbo.usp_RptDischargewisebills @FromDate=?, @ToDate=?"
            df = pd.read_sql(sql, conn, params=[from_date, to_date])
        except Exception:
            try:
                cur = conn.cursor()
                cur.execute("SET IMPLICIT_TRANSACTIONS OFF; SET TRANSACTION ISOLATION LEVEL READ COMMITTED;")
                cur.close()
            except Exception:
                pass
            sql = "SET NOCOUNT ON; EXEC dbo.usp_RptDischargewisebills @FromDate=?, @ToDate=?"
            df = pd.read_sql(sql, conn, params=[from_date, to_date])
        if df is None or df.empty:
            return df
        df.columns = [c.strip() for c in df.columns]
        df["Unit"] = (unit or "").upper()
        return df
    except Exception as e:
        print(f"â›” Error fetching discharge bills ({unit}): {e}")
        return None
    finally:
        # Ensure pooled connections are returned with default isolation.
        try:
            cur = conn.cursor()
            cur.execute("SET IMPLICIT_TRANSACTIONS OFF; SET TRANSACTION ISOLATION LEVEL READ COMMITTED;")
            cur.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass


# ===================== Pharmacy: Product Margin =====================
def fetch_pharmacy_product_margin(unit: str, from_date: str, to_date: str, medicine_type_id: int):
    """
    Calls dbo.usp_RptProductMargin @fromdate, @todate, @manuid for margin data.
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Pharmacy margin: could not connect to {unit}")
        return None
    try:
        sql = "SET NOCOUNT ON; EXEC dbo.usp_RptProductMargin @fromdate=?, @todate=?, @manuid=?"
        df = pd.read_sql(sql, conn, params=[from_date, to_date, int(medicine_type_id or 0)])
        if df is None or df.empty:
            return df
        df.columns = [c.strip() for c in df.columns]
        df["Unit"] = (unit or "").upper()
        return df
    except Exception as e:
        print(f"Pharmacy margin fetch failed for {unit}: {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def fetch_pharmacy_issue_margin(unit: str, from_date: str, to_date: str, manufacturer_id: int):
    """
    Calls dbo.usp_RptissueMargin @fromdate, @todate, @manid for issue margin data.
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Issue margin: could not connect to {unit}")
        return None
    try:
        sql = "SET NOCOUNT ON; EXEC dbo.usp_RptissueMargin @fromdate=?, @todate=?, @manid=?"
        df = pd.read_sql(sql, conn, params=[from_date, to_date, int(manufacturer_id or 0)])
        if df is None or df.empty:
            return df
        df.columns = [c.strip() for c in df.columns]
        df["Unit"] = (unit or "").upper()
        return df
    except Exception as e:
        print(f"Issue margin fetch failed for {unit}: {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def fetch_medicine_types(unit: str):
    """
    Fetches medicine type list from MedicineType_Mst.
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Medicine types: could not connect to {unit}")
        return None
    try:
        df = pd.read_sql(
            "SELECT MedicineTypeId, MedicineTypeDesc FROM dbo.MedicineType_Mst ORDER BY MedicineTypeDesc",
            conn,
        )
        if df is None or df.empty:
            return df
        df.columns = [c.strip() for c in df.columns]
        return df
    except Exception as e:
        print(f"Medicine type fetch failed for {unit}: {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass

# ===================== IPD Cardiac Consumables =====================
def fetch_ipd_cardiac_consumables(unit: str, from_date: str, to_date: str):
    """
    Calls dbo.usp_ipdcardiacconsumables @FromDate, @ToDate for a given unit.
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"A›­AÿA_A, Could not connect to {unit} for IPD cardiac consumables")
        return None
    try:
        sql = "SET NOCOUNT ON; EXEC dbo.usp_ipdcardiacconsumables @FromDate=?, @ToDate=?"
        df = pd.read_sql(sql, conn, params=[from_date, to_date])
        if df is None or df.empty:
            return df
        df.columns = [c.strip() for c in df.columns]
        df["Unit"] = (unit or "").upper()
        return df
    except Exception as e:
        print(f"A›ƒ?§ƒ?? Error fetching IPD cardiac consumables ({unit}): {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass

# ===================== Due_Discount_track =====================
def fetch_discount_due_track(unit: str, from_date: str, to_date: str):
    """
    Calls dbo.usp_DiscountDueTrackAll to fetch discount/due tracking records.
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"âš ï¸ Could not connect to {unit} for discount/due track")
        return None
    try:
        sql = "EXEC dbo.usp_DiscountDueTrackAll ?, ?"
        df = pd.read_sql(sql, conn, params=[from_date, to_date])
        if df is None or df.empty:
            return df
        df.columns = [c.strip() for c in df.columns]
        df["Unit"] = (unit or "").upper()
        return df
    except Exception as e:
        print(f"â›” Error fetching discount/due track ({unit}): {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass

# ===================== Discount/Cancellation - Bill Discount Apply =====================
def apply_bill_discount(unit: str, bill_id: int | None, bill_no: str | None, discount_amount, force: bool = False):
    """
    Apply a discount to a specific bill with due check.
    - If discount exceeds DueAmount and force is False, returns {"warning": True, ...} without applying.
    - Otherwise updates DiscountAmount, NetAmount, and DueAmount accordingly.
    """
    conn = get_sql_connection(unit)
    if not conn:
        return {"error": f"Unable to connect to {unit}"}
    try:
        conn.autocommit = False
        cursor = conn.cursor()

        # Lock the target row
        if bill_id:
            cursor.execute("""
                SELECT Bill_ID, BillNo, GrossAmount, DiscountAmount, NetAmount, DueAmount
                FROM Billing_Mst WITH (ROWLOCK, UPDLOCK)
                WHERE Bill_ID = ?
            """, bill_id)
        else:
            cursor.execute("""
                SELECT TOP 1 Bill_ID, BillNo, GrossAmount, DiscountAmount, NetAmount, DueAmount
                FROM Billing_Mst WITH (ROWLOCK, UPDLOCK)
                WHERE BillNo = ?
            """, bill_no)
        row = cursor.fetchone()
        if not row:
            conn.rollback()
            return {"error": "Bill not found"}

        bill_id_db = row.Bill_ID
        bill_no_db = row.BillNo
        gross = Decimal(str(row.GrossAmount or 0))
        disc = Decimal(str(row.DiscountAmount or 0))
        net = Decimal(str(row.NetAmount or 0))
        due = Decimal(str(row.DueAmount or 0))
        disc_req = Decimal(str(discount_amount))

        new_disc = disc + disc_req
        new_net = net - disc_req
        if new_net < 0:
            new_net = Decimal("0")

        warning = False
        new_due = due - disc_req
        if new_due < 0:
            warning = True
            if not force:
                conn.rollback()
                return {
                    "warning": True,
                    "message": f"Discount exceeds due by {abs(new_due)}",
                    "current_due": float(due),
                    "requested_discount": float(disc_req),
                }

        # Apply update
        cursor.execute("""
            UPDATE Billing_Mst
            SET DiscountAmount = ?, NetAmount = ?, DueAmount = ?, UpdatedOn = GETDATE()
            WHERE Bill_ID = ?
        """, (float(new_disc), float(new_net), float(new_due), bill_id_db))

        conn.commit()
        return {
            "success": True,
            "bill_id": bill_id_db,
            "bill_no": bill_no_db,
            "gross": float(gross),
            "discount": float(new_disc),
            "net": float(new_net),
            "due": float(new_due),
            "warning_applied": warning,
        }
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        return {"error": str(e)}
    finally:
        try:
            conn.close()
        except Exception:
            pass
# ===================== Administrative MIS (Balances & Ledgers) =====================
def fetch_ipd_bill_tracking(unit: str, from_date: str, to_date: str):
    """
    Calls [dbo].[Usp_IPDBillTracking] to fetch IPD Billing Ledger/Balances.
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"âš ï¸ Could not connect to {unit} for IPD Bill Tracking")
        return None
    try:
        # Using parameterized query for safety
        sql = "{CALL [dbo].[Usp_IPDBillTracking] (?, ?)}"
        params = (from_date, to_date)
        
        df = pd.read_sql(sql, conn, params=params)
        
        if df is None or df.empty:
            return pd.DataFrame()
            
        # Clean column names
        df.columns = [c.strip() for c in df.columns]
        return df
        
    except Exception as e:
        print(f"â›” Error fetching IPD Bill Tracking ({unit}): {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass

# ===================== Discount/Cancellation - Bill Register =====================
def fetch_bill_register_comprehensive(unit: str, from_date: str, to_date: str):
    """
    Calls dbo.Usp_BillRegisterComprehensive @fromdate, @todate for discount/cancellation desk.
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Could not connect to {unit} for bill register")
        return None
    try:
        sql = "SET NOCOUNT ON; EXEC dbo.Usp_BillRegisterComprehensive @fromdate=?, @todate=?"
        df = pd.read_sql(sql, conn, params=[from_date, to_date])
        if df is None or df.empty:
            return df
        df.columns = [c.strip() for c in df.columns]
        df["Unit"] = (unit or "").upper()
        return df
    except Exception as e:
        print(f"Error fetching bill register ({unit}): {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass

# ===================== Receipts Register (for cancellations) =====================
def fetch_receipt_register_comprehensive(unit: str, from_date: str, to_date: str):
    """
    Calls dbo.Usp_Receiptregistercomprehensive @fromdate, @todate.
    Intended for receipt reversals/cancellations view.
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Could not connect to {unit} for receipt register")
        return None
    try:
        sql = "SET NOCOUNT ON; EXEC dbo.Usp_Receiptregistercomprehensive @fromdate=?, @todate=?"
        df = pd.read_sql(sql, conn, params=[from_date, to_date])
        if df is None or df.empty:
            return df
        df.columns = [c.strip() for c in df.columns]
        df["Unit"] = (unit or "").upper()
        return df
    except Exception as e:
        print(f"Error fetching receipt register ({unit}): {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass

# ===================== Locked Bills (Recent) =====================
def fetch_locked_bills_recent(unit: str, from_date: str, to_date: str, bill_type: str | None = None):
    """
    Pull recent bills using dbo.usp_billfind for locked-bill modifications.
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Could not connect to {unit} for locked bills")
        return None
    try:
        sql = "SET NOCOUNT ON; EXEC dbo.usp_billfind @FromDate=?, @ToDate=?, @BillType=?"
        df = pd.read_sql(sql, conn, params=[from_date, to_date, bill_type])
        if df is None or df.empty:
            return df
        df.columns = [c.strip() for c in df.columns]

        cols_lower = {c.lower(): c for c in df.columns}
        def pick(candidates):
            for c in candidates:
                key = c.lower()
                if key in cols_lower:
                    return cols_lower[key]
            return None

        reg_col = pick(["RegNo", "Registration_No", "RegistrationNo"])
        if reg_col and "RegNo" not in df.columns:
            df["RegNo"] = df[reg_col]

        ph_ret_col = pick(["PHRetAmount", "PHReturnAmount", "PH_Return_Amount"])
        if ph_ret_col and "PHRetAmount" not in df.columns:
            df["PHRetAmount"] = df[ph_ret_col]

        bill_amt_col = pick(["BillAmount", "GrossAmount", "NetAmount"])
        if bill_amt_col and "BillAmount" not in df.columns:
            df["BillAmount"] = df[bill_amt_col]

        df["Unit"] = (unit or "").upper()
        return df
    except Exception as e:
        print(f"Error fetching locked bills ({unit}): {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass

# ===================== Summarized IP Ledger =====================
def fetch_visit_billing_receipt_summary(unit: str, from_date: str, to_date: str):
    """
    Calls dbo.usp_VisitBillingReceiptSummary @FromDate, @ToDate.
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Could not connect to {unit} for visit billing receipt summary")
        return None
    try:
        sql = "SET NOCOUNT ON; EXEC dbo.usp_VisitBillingReceiptSummary @FromDate=?, @ToDate=?"
        df = pd.read_sql(sql, conn, params=[from_date, to_date])
        if df is None or df.empty:
            return df
        df.columns = [c.strip() for c in df.columns]
        df["Unit"] = (unit or "").upper()
        return df
    except Exception as e:
        print(f"Error fetching visit billing receipt summary ({unit}): {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass

# ===================== Health-Checkup Ledger =====================
def fetch_phc_ledger(unit: str, from_date: str, to_date: str):
    """
    Calls dbo.Usp_PHCLedger @fromdate, @todate for a given unit.
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Could not connect to {unit} for PHC ledger")
        return None
    try:
        sql = "SET NOCOUNT ON; EXEC dbo.Usp_PHCLedger @fromdate=?, @todate=?"
        df = pd.read_sql(sql, conn, params=[from_date, to_date])
        if df is None or df.empty:
            return df
        df.columns = [c.strip() for c in df.columns]
        df["Unit"] = (unit or "").upper()
        return df
    except Exception as e:
        print(f"Error fetching PHC ledger ({unit}): {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass

# ===================== IP Package Ledger =====================
def fetch_ip_package_ledger(unit: str, from_date: str, to_date: str):
    """
    Fetch IP package ledger rows from Billing + BillingDetails + Service_Mst + CPAHealthPlanMst + Visit.
    Criteria:
      - Billing_Mst.submitted = 1
      - Service_Mst.Category_Id = 35
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Could not connect to {unit} for IP package ledger")
        return None
    try:
        sql = """
            SELECT
                bm.Bill_ID,
                bm.BillNo,
                bm.BillDate,
                bm.Registration_No,
                bm.Visit_ID,
                v.VisitNo,
                v.VisitDate,
                CASE WHEN v.PatientID IS NULL THEN ISNULL(bm.Registration_No, '') ELSE dbo.fn_regno(v.PatientID) END AS RegNo,
                CASE WHEN v.PatientID IS NULL THEN '' ELSE dbo.fn_patientfullname(v.PatientID) END AS PatientName,
                CASE WHEN v.DocInCharge IS NULL THEN '' ELSE dbo.fn_doctorfirstname(v.DocInCharge) END AS DoctorInCharge,
                CASE WHEN v.WardID IS NULL THEN '' ELSE dbo.fn_ward_name(v.WardID) END AS Ward,
                bd.BillDetailID,
                bd.ServiceID,
                sm.Service_Code AS PackageServiceCode,
                sm.Service_Name AS PackageServiceName,
                ISNULL(bd.Rate, 0) AS Rate,
                ISNULL(bd.Quantity, 0) AS Quantity,
                ISNULL(bd.Amount, 0) AS Amount,
                ISNULL(cp.HealthPlanID, 0) AS PackageID,
                ISNULL(cp.HealthPlanCode, sm.Service_Code) AS PackageCode,
                ISNULL(cp.HealthPlanName, sm.Service_Name) AS PackageName
            FROM dbo.Billing_Mst bm WITH (NOLOCK)
            INNER JOIN dbo.BillingDetails bd WITH (NOLOCK)
                ON bm.Bill_ID = bd.Bill_ID
            INNER JOIN dbo.Service_Mst sm WITH (NOLOCK)
                ON sm.Service_ID = bd.ServiceID
            LEFT JOIN dbo.Visit v WITH (NOLOCK)
                ON v.Visit_ID = bm.Visit_ID
            OUTER APPLY (
                SELECT TOP 1
                    hp.HealthPlanID,
                    hp.HealthPlanCode,
                    hp.HealthPlanName
                FROM dbo.CPAHealthPlanMst hp WITH (NOLOCK)
                WHERE
                    (hp.ServiceID = sm.Service_ID OR hp.HealthPlanCode = sm.Service_Code)
                    AND (ISNULL(hp.HealthPlanCode, '') LIKE 'IPP-%' OR hp.ServiceID = sm.Service_ID)
                ORDER BY
                    CASE WHEN hp.ServiceID = sm.Service_ID THEN 0 ELSE 1 END,
                    hp.HealthPlanID DESC
            ) cp
            WHERE
                bm.BillDate >= ?
                AND bm.BillDate < DATEADD(DAY, 1, CAST(? AS DATETIME))
                AND ISNULL(bm.submitted, 0) = 1
                AND ISNULL(bm.CancelStatus, 0) = 0
                AND ISNULL(sm.Category_Id, 0) = 35
            ORDER BY
                bm.BillDate DESC,
                bm.Bill_ID DESC,
                bd.BillDetailID DESC
        """
        df = pd.read_sql(sql, conn, params=[from_date, to_date])
        if df is None or df.empty:
            return df
        df.columns = [c.strip() for c in df.columns]
        for col in ["Rate", "Quantity", "Amount"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
        df["Unit"] = (unit or "").upper()
        return df
    except Exception as e:
        print(f"Error fetching IP package ledger ({unit}): {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass

# ===================== Service Ledger =====================
def fetch_service_consumptions(unit: str, from_date: str, to_date: str, service_name: str = "", service_id: int | None = None):
    """
    Calls dbo.usp_GetServiceConsumptions for a given unit.
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"âš ï¸ Could not connect to {unit} for Service Ledger")
        return None
    try:
        # Using parameterized query
        if service_id:
            try:
                sql = "EXEC dbo.usp_GetServiceConsumptions @StartDate=?, @EndDate=?, @ServiceId=?"
                params = (from_date, to_date, service_id)
                df = pd.read_sql(sql, conn, params=params)
            except Exception as e:
                try:
                    print(f"ServiceId search failed, trying @Service_ID ({unit}): {e}")
                    sql = "EXEC dbo.usp_GetServiceConsumptions @StartDate=?, @EndDate=?, @Service_ID=?"
                    params = (from_date, to_date, service_id)
                    df = pd.read_sql(sql, conn, params=params)
                except Exception as e2:
                    print(f"ServiceId search failed, falling back to ServiceName ({unit}): {e2}")
                    sql = "EXEC dbo.usp_GetServiceConsumptions @StartDate=?, @EndDate=?, @ServiceName=?"
                    params = (from_date, to_date, service_name)
                    df = pd.read_sql(sql, conn, params=params)
        else:
            sql = "EXEC dbo.usp_GetServiceConsumptions @StartDate=?, @EndDate=?, @ServiceName=?"
            params = (from_date, to_date, service_name)
            df = pd.read_sql(sql, conn, params=params)
        
        if df is None or df.empty:
            return pd.DataFrame()
            
        # Clean column names
        df.columns = [c.strip() for c in df.columns]

        def _norm_key(val: str) -> str:
            return re.sub(r"[\s_]+", "", str(val or "")).lower()

        def _rename_if_missing(canonical: str, candidates: list[str]):
            if canonical in df.columns:
                return
            for cand in candidates:
                if cand in df.columns:
                    df.rename(columns={cand: canonical}, inplace=True)
                    return
            cols_map = {_norm_key(c): c for c in df.columns}
            for cand in candidates:
                key = _norm_key(cand)
                if key in cols_map:
                    df.rename(columns={cols_map[key]: canonical}, inplace=True)
                    return

        _rename_if_missing("Total_Amt", ["TotalAmount", "Total Amount", "Total_Amt", "Total Amt", "TotalAmt", "Total_Amount"])
        _rename_if_missing("Rate", ["RateAmount", "Rate Amount", "UnitRate", "Unit Rate", "ServiceRate", "Service Rate", "Rate"])
        _rename_if_missing("Quantity", ["Qty", "QTY", "Quantity"])

        def _to_number(series: pd.Series) -> pd.Series:
            cleaned = series.astype(str)
            cleaned = cleaned.str.replace(",", "", regex=False)
            cleaned = cleaned.str.replace(r"[^0-9.\-]", "", regex=True)
            return pd.to_numeric(cleaned, errors="coerce").fillna(0.0)

        # Normalize numeric columns for UI + exports
        for col in ["Rate", "Total_Amt", "Quantity"]:
            if col in df.columns:
                df[col] = _to_number(df[col])
        df["Unit"] = (unit or "").upper()
        return df
        
    except Exception as e:
        print(f"â›” Error fetching Service Ledger ({unit}): {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


# ===================== Laboratory Summary (Diagnostics MIS) =====================
def fetch_laboratory_summary(unit: str, from_date: str, to_date: str):
    """
    Calls dbo.usp_LaboratorySummary to fetch lab revenue/volume rows.
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Could not connect to {unit} for laboratory summary")
        return None
    try:
        sql = "EXEC dbo.usp_LaboratorySummary @FromDate=?, @ToDate=?"
        df = pd.read_sql(sql, conn, params=[from_date, to_date])
        if df is None or df.empty:
            return df
        df.columns = [c.strip() for c in df.columns]
        df["Unit"] = (unit or "").upper()
        return df
    except Exception as e:
        print(f"Error fetching laboratory summary ({unit}): {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass

# ===================== Receipts (Receipt_mst) =====================
def fetch_receipts_recent(unit: str, from_date: str, to_date: str):
    """
    Fetch receipts directly from Receipt_mst for modification workflows.
    Includes Patient and RegNo via scalar functions.
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Could not connect to {unit} for receipts")
        return None
    try:
        sql = """
            SELECT
                rm.Receipt_ID,
                rm.Receipt_No,
                rm.PatientID,
                rm.TmpPatientId,
                rm.Visit_ID,
                rm.Amount,
                rm.Receipt_Date,
                rm.Receipt_Time,
                rm.Note,
                rm.InvoiceNo,
                rm.PaymentAgainst,
                rm.PaymentMode,
                rm.Bank_ID,
                rm.Cheque_No,
                rm.Cheque_Date,
                rm.CCID,
                rm.CCNo,
                rm.CCExpDate,
                rm.UpdatedOn,
                rm.UpdatedBy,
                rm.ReceiptType,
                rm.CorporateClientID,
                rm.CancelStatus,
                rm.CanceledBy,
                rm.CancelDate,
                rm.CancelReasonid,
                rm.PostedToFA,
                rm.UpdatedMacName,
                rm.UpdatedMacID,
                rm.UpdatedIPAddress,
                rm.InsertedByUserID,
                rm.InsertedON,
                rm.InsertedMacName,
                rm.InsertedMacID,
                rm.InsertedIPAddress,
                rm.OPDAdmCounterID,
                rm.CenterID,
                rm.TransID,
                rm.StatementID,
                rm.Payeeid,
                rm.JE,
                CASE
                    WHEN ISNULL(rm.PatientID, 0) > 0 THEN dbo.fn_patientfullname(rm.PatientID)
                    ELSE LTRIM(RTRIM(REPLACE(REPLACE(CONCAT(tp.FirstName, ' ', tp.MiddleName, ' ', tp.LastName), '  ', ' '), '  ', ' ')))
                END AS Patient,
                CASE
                    WHEN ISNULL(rm.PatientID, 0) > 0 THEN dbo.fn_regno(rm.PatientID)
                    ELSE NULL
                END AS RegNo
            FROM Receipt_mst rm WITH (NOLOCK)
            LEFT JOIN TMPPatient tp WITH (NOLOCK)
                ON rm.TmpPatientId = tp.TmpPatientID
            WHERE rm.Receipt_Date >= ?
              AND rm.Receipt_Date < DATEADD(day, 1, ?)
            ORDER BY rm.Receipt_Date DESC
        """
        df = pd.read_sql(sql, conn, params=[from_date, to_date])
        if df is None or df.empty:
            return df
        df.columns = [c.strip() for c in df.columns]
        df["Unit"] = (unit or "").upper()
        return df
    except Exception as e:
        print(f"Error fetching receipts ({unit}): {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass

# ===================== Visit Reference Masters =====================
def _filter_active_master(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    cols_lower = {c.lower(): c for c in df.columns}
    deactive_col = cols_lower.get("deactive") or cols_lower.get("inactive")
    if deactive_col:
        try:
            raw = df[deactive_col]
            if raw.dtype == bool:
                mask = ~raw.fillna(False)
            else:
                cleaned = raw.astype(str).str.strip().str.lower()
                cleaned = cleaned.replace({"false": "0", "true": "1", "no": "0", "yes": "1"})
                numeric = pd.to_numeric(cleaned, errors="coerce").fillna(0)
                mask = numeric == 0
            df = df[mask]
        except Exception:
            pass
        return df
    active_col = cols_lower.get("isactive") or cols_lower.get("active")
    if active_col:
        try:
            df = df[df[active_col].fillna(1).astype(int) == 1]
        except Exception:
            pass
    return df


def fetch_departments(unit: str):
    """
    Fetch active departments from Department_mst and normalize columns.
    Returns DepartmentID, DepartmentName.
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Could not connect to {unit} for departments")
        return None
    try:
        df = pd.read_sql("SELECT * FROM Department_mst WITH (NOLOCK)", conn)
        if df is None or df.empty:
            return df
        df.columns = [c.strip() for c in df.columns]
        df = _filter_active_master(df)
        cols_lower = {c.lower(): c for c in df.columns}
        def pick(candidates):
            for c in candidates:
                key = c.lower()
                if key in cols_lower:
                    return cols_lower[key]
            return None
        id_col = pick(["DepartmentID", "DepartmentId", "DeptID", "DeptId", "Department_Id", "Dept_Id", "Id"])
        name_col = pick(["DepartmentName", "Department_Name", "DeptName", "Department", "Dept", "Name"])
        if id_col and name_col:
            out = df[[id_col, name_col]].copy()
            out.columns = ["DepartmentID", "DepartmentName"]
            return out
        return df
    except Exception as e:
        print(f"Error fetching departments ({unit}): {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def fetch_wards(unit: str):
    """
    Fetch active wards from Ward_mst.
    Returns WardID, WardName.
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Could not connect to {unit} for wards")
        return None
    try:
        df = pd.read_sql("SELECT * FROM Ward_mst WITH (NOLOCK)", conn)
        if df is None or df.empty:
            return df
        df.columns = [c.strip() for c in df.columns]
        df = _filter_active_master(df)
        cols_lower = {c.lower(): c for c in df.columns}
        def pick(candidates):
            for c in candidates:
                key = c.lower()
                if key in cols_lower:
                    return cols_lower[key]
            return None
        id_col = pick(["Ward_ID", "WardID", "WardId", "Ward_Id", "Id"])
        name_col = pick(["Ward_Name", "WardName", "Ward", "Name"])
        if id_col and name_col:
            out = df[[id_col, name_col]].copy()
            out.columns = ["WardID", "WardName"]
            return out
        return df
    except Exception as e:
        print(f"Error fetching wards ({unit}): {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def fetch_rooms(unit: str):
    """
    Fetch active rooms from Room_mst.
    Returns RoomID, RoomName, WardID.
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Could not connect to {unit} for rooms")
        return None
    try:
        df = pd.read_sql("SELECT * FROM Room_mst WITH (NOLOCK)", conn)
        if df is None or df.empty:
            return df
        df.columns = [c.strip() for c in df.columns]
        df = _filter_active_master(df)
        cols_lower = {c.lower(): c for c in df.columns}
        def pick(candidates):
            for c in candidates:
                key = c.lower()
                if key in cols_lower:
                    return cols_lower[key]
            return None
        id_col = pick(["Room_ID", "RoomID", "RoomId", "Room_Id", "Id"])
        name_col = pick(["Room_Name", "RoomName", "Room", "Name"])
        ward_col = pick(["Ward_ID", "WardID", "WardId", "Ward_Id"])
        cols = [c for c in [id_col, name_col, ward_col] if c]
        if len(cols) >= 2:
            out = df[cols].copy()
            rename_map = {}
            if id_col: rename_map[id_col] = "RoomID"
            if name_col: rename_map[name_col] = "RoomName"
            if ward_col: rename_map[ward_col] = "WardID"
            out = out.rename(columns=rename_map)
            if "WardID" not in out.columns:
                out["WardID"] = None
            return out[["RoomID", "RoomName", "WardID"]]
        return df
    except Exception as e:
        print(f"Error fetching rooms ({unit}): {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def fetch_beds(unit: str):
    """
    Fetch active beds from Bed_mst.
    Returns BedID, BedName, RoomID, BedStatusId.
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Could not connect to {unit} for beds")
        return None
    try:
        df = pd.read_sql("SELECT * FROM Bed_mst WITH (NOLOCK)", conn)
        if df is None or df.empty:
            return df
        df.columns = [c.strip() for c in df.columns]
        df = _filter_active_master(df)
        cols_lower = {c.lower(): c for c in df.columns}
        def pick(candidates):
            for c in candidates:
                key = c.lower()
                if key in cols_lower:
                    return cols_lower[key]
            return None
        id_col = pick(["Bed_ID", "BedID", "BedId", "Bed_Id", "Id"])
        name_col = pick(["Bed_Name", "BedName", "Bed", "Name"])
        room_col = pick(["Room_ID", "RoomID", "RoomId", "Room_Id"])
        status_col = pick(["Bed_Status_ID", "BedStatusId", "BedStatus_ID", "BedStatus", "Bed_Status"])
        cols = [c for c in [id_col, name_col, room_col, status_col] if c]
        if len(cols) >= 2:
            out = df[cols].copy()
            rename_map = {}
            if id_col: rename_map[id_col] = "BedID"
            if name_col: rename_map[name_col] = "BedName"
            if room_col: rename_map[room_col] = "RoomID"
            if status_col: rename_map[status_col] = "BedStatusId"
            out = out.rename(columns=rename_map)
            if "RoomID" not in out.columns:
                out["RoomID"] = None
            if "BedStatusId" not in out.columns:
                out["BedStatusId"] = None
            return out[["BedID", "BedName", "RoomID", "BedStatusId"]]
        return df
    except Exception as e:
        print(f"Error fetching beds ({unit}): {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def fetch_doctors(unit: str):
    """
    Fetch active doctors from employee_mst.
    Returns DoctorId, DoctorName.
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Could not connect to {unit} for doctors")
        return None
    try:
        df = pd.read_sql(
            """
            SELECT empid AS DoctorId,
                   dbo.fn_doctorfirstname(empid) AS DoctorName
            FROM employee_mst WITH (NOLOCK)
            WHERE emptype = 'Doc'
              AND ISNULL(deactive, 0) = 0
              AND ISNULL(docunit_id, 0) IN (1, 2, 3)
            ORDER BY DoctorName
            """,
            conn,
        )
        if df is None or df.empty:
            return df
        df.columns = [c.strip() for c in df.columns]
        return df
    except Exception as e:
        print(f"Error fetching doctors ({unit}): {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def fetch_users(unit: str):
    """
    Fetch users from User_mst for visit NOC/Open fields.
    Returns UserID, UserName (and Deactive when available).
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Could not connect to {unit} for users")
        return None
    try:
        df = pd.read_sql("SELECT * FROM User_mst WITH (NOLOCK)", conn)
        if df is None or df.empty:
            return df
        df.columns = [c.strip() for c in df.columns]
        cols_lower = {c.lower(): c for c in df.columns}
        def pick(candidates):
            for c in candidates:
                key = c.lower()
                if key in cols_lower:
                    return cols_lower[key]
            return None
        id_col = pick(["UserID", "UserId", "User_ID", "Id"])
        name_col = pick(["UserName", "Username", "User_Name", "Name"])
        deactive_col = pick(["Deactive", "Inactive", "IsActive", "Active"])
        if id_col and name_col:
            out = df[[id_col, name_col]].copy()
            out.columns = ["UserID", "UserName"]
            if deactive_col:
                out["Deactive"] = df[deactive_col]
            return out
        return df
    except Exception as e:
        print(f"Error fetching users ({unit}): {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def fetch_patient_types(unit: str):
    """
    Fetch active patient types from patienttype_mst.
    Returns PatientTypeID, PatientTypeName.
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Could not connect to {unit} for patient types")
        return None
    try:
        df = pd.read_sql("SELECT * FROM patienttype_mst WITH (NOLOCK)", conn)
        if df is None or df.empty:
            return df
        df.columns = [c.strip() for c in df.columns]
        df = _filter_active_master(df)
        cols_lower = {c.lower(): c for c in df.columns}
        def pick(candidates):
            for c in candidates:
                key = c.lower()
                if key in cols_lower:
                    return cols_lower[key]
            return None
        id_col = pick(["PatientTypeID", "PatientTypeId", "PatientType_ID", "PatientTypeId", "TypeID", "TypeId", "Id"])
        name_col = pick(["PatientTypeName", "PatientType", "TypeName", "Name"])
        if id_col and name_col:
            out = df[[id_col, name_col]].copy()
            out.columns = ["PatientTypeID", "PatientTypeName"]
            return out
        return df
    except Exception as e:
        print(f"Error fetching patient types ({unit}): {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def fetch_patient_subtypes(unit: str):
    """
    Fetch active patient subtypes from patientsubtype_mst.
    Returns PatientSubTypeID, PatientSubTypeName, PatientTypeID.
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Could not connect to {unit} for patient subtypes")
        return None
    try:
        df = pd.read_sql("SELECT * FROM patientsubtype_mst WITH (NOLOCK)", conn)
        if df is None or df.empty:
            return df
        df.columns = [c.strip() for c in df.columns]
        df = _filter_active_master(df)
        cols_lower = {c.lower(): c for c in df.columns}
        def pick(candidates):
            for c in candidates:
                key = c.lower()
                if key in cols_lower:
                    return cols_lower[key]
            return None
        id_col = pick(["PatientSubTypeID", "PatientSubTypeId", "PatientSubType_ID", "SubTypeID", "SubTypeId", "Id"])
        name_col = pick(["PatientSubTypeDesc", "PatientSubType_Desc", "PatientSubTypeName", "PatientSubType", "SubTypeName", "Name"])
        type_col = pick(["PatientTypeID", "PatientTypeId", "PatientType_ID", "TypeID", "TypeId", "PatientType"])
        cols = [c for c in [id_col, name_col, type_col] if c]
        if len(cols) >= 2:
            out = df[cols].copy()
            rename_map = {}
            if id_col: rename_map[id_col] = "PatientSubTypeID"
            if name_col: rename_map[name_col] = "PatientSubTypeName"
            if type_col: rename_map[type_col] = "PatientTypeID"
            out = out.rename(columns=rename_map)
            if "PatientTypeID" not in out.columns:
                out["PatientTypeID"] = None
            return out[["PatientSubTypeID", "PatientSubTypeName", "PatientTypeID"]]
        return df
    except Exception as e:
        print(f"Error fetching patient subtypes ({unit}): {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def fetch_tariff_schemes(unit: str):
    """
    Fetch active tariff schemes from cpatariffscheme_mst.
    Returns TariffSchemeID, TariffSchemeName.
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Could not connect to {unit} for tariff schemes")
        return None
    try:
        df = pd.read_sql("SELECT * FROM cpatariffscheme_mst WITH (NOLOCK)", conn)
        if df is None or df.empty:
            return df
        df.columns = [c.strip() for c in df.columns]
        df = _filter_active_master(df)
        cols_lower = {c.lower(): c for c in df.columns}
        def pick(candidates):
            for c in candidates:
                key = c.lower()
                if key in cols_lower:
                    return cols_lower[key]
            return None
        id_col = pick(["TariffSchemeID", "TariffSchemeId", "TariffScheme_ID", "SchemeID", "SchemeId", "Id"])
        name_col = pick(["TariffSchemeDesc", "TariffScheme_Desc", "TariffSchemeName", "TariffScheme", "SchemeName", "Name"])
        if id_col and name_col:
            out = df[[id_col, name_col]].copy()
            out.columns = ["TariffSchemeID", "TariffSchemeName"]
            return out
        return df
    except Exception as e:
        print(f"Error fetching tariff schemes ({unit}): {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def fetch_health_checkups(unit: str):
    """
    Fetch active health checkup plans from CPAHealthPlanMST (PackageorHCPPlan='HCP').
    Returns HealthPlanID, HealthPlanName.
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Could not connect to {unit} for health checkups")
        return None
    try:
        df = pd.read_sql(
            """
            SELECT *
            FROM CPAHealthPlanMST WITH (NOLOCK)
            """,
            conn,
        )
        if df is None or df.empty:
            return df
        df.columns = [c.strip() for c in df.columns]
        df = _filter_active_master(df)
        package_col = _find_column(
            df,
            [
                "PackageorHCPPlan",
                "PackageOrHCPPlan",
                "Package_or_HCPPlan",
                "PackageOrHCP",
                "PackageorHCP",
                "PackageorHCPlan",
                "PackageType",
                "Package_Type",
                "PlanType",
                "Plan_Type",
            ],
        )
        if package_col and package_col in df.columns:
            try:
                df = df[df[package_col].astype(str).str.strip().str.upper() == "HCP"]
            except Exception:
                pass
        cols_lower = {c.lower(): c for c in df.columns}
        def pick(candidates):
            for c in candidates:
                key = c.lower()
                if key in cols_lower:
                    return cols_lower[key]
            return None
        id_col = pick(["HealthPlanID", "HealthPlanId", "HealthPlan_ID", "PlanID", "PlanId", "Id"])
        name_col = pick(["HealthPlanName", "HealthPlan_Desc", "HealthPlanDesc", "PlanName", "Name"])
        if id_col and name_col:
            out = df[[id_col, name_col]].copy()
            out.columns = ["HealthPlanID", "HealthPlanName"]
            return out
        return df
    except Exception as e:
        print(f"Error fetching health checkups ({unit}): {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass

# ===================== Visits (Visit) =====================
def fetch_visits_recent(unit: str, from_date: str, to_date: str, limit: int = 1000):
    """
    Fetch recent visits from Visit table for modification workflows.
    Includes Patient and RegNo via scalar functions.
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Could not connect to {unit} for visits")
        return None
    limit_val = 1000
    try:
        if limit is not None:
            limit_val = max(1, min(int(limit), 5000))
    except Exception:
        limit_val = 1000
    try:
        visit_cols = _get_table_columns(conn, "dbo.Visit")
        def col_expr(col: str) -> str:
            if not visit_cols:
                return f"v.{col}"
            return f"v.{col}" if col.lower() in visit_cols else f"NULL AS {col}"
        def doc_expr(col: str, alias: str) -> str:
            if not visit_cols:
                return f"dbo.fn_doctorfirstname(v.{col}) AS {alias}"
            return f"dbo.fn_doctorfirstname(v.{col}) AS {alias}" if col.lower() in visit_cols else f"NULL AS {alias}"
        def patient_expr(fn_name: str, alias: str) -> str:
            if not visit_cols:
                return f"CASE WHEN ISNULL(v.PatientID, 0) > 0 THEN dbo.{fn_name}(v.PatientID) ELSE NULL END AS {alias}"
            return f"CASE WHEN ISNULL(v.PatientID, 0) > 0 THEN dbo.{fn_name}(v.PatientID) ELSE NULL END AS {alias}" if "patientid" in visit_cols else f"NULL AS {alias}"

        select_list = [
            col_expr("Visit_ID"),
            col_expr("AdmissionNo"),
            col_expr("PatientID"),
            col_expr("VisitNo"),
            col_expr("VisitTypeID"),
            col_expr("VisitDate"),
            col_expr("TypeOfVisit"),
            col_expr("WardID"),
            col_expr("RoomID"),
            col_expr("BedID"),
            col_expr("DepartmentID"),
            col_expr("DocInCharge"),
            col_expr("RefDocID"),
            doc_expr("DocInCharge", "DocInChargeName"),
            doc_expr("RefDocID", "RefDocName"),
            doc_expr("subdocid", "SubDocName"),
            col_expr("DischargeType"),
            col_expr("DischargeDate"),
            col_expr("PatientType_ID"),
            col_expr("TariffScheme_ID"),
            col_expr("PatientSubType_ID"),
            col_expr("HealthCheckUpID"),
            col_expr("OutsideRefDoctor"),
            col_expr("subdocid"),
            col_expr("payType"),
            col_expr("IPPhNoc"),
            col_expr("IPPhNocDate"),
            col_expr("OPPhNoc"),
            col_expr("OPPhNocDate"),
            col_expr("OPPhNocBy"),
            col_expr("IPPhNocBy"),
            col_expr("billSubmitStatus"),
            col_expr("tag"),
            col_expr("openBy"),
            col_expr("openDatetime"),
            patient_expr("fn_patientfullname", "Patient"),
            patient_expr("fn_regno", "RegNo"),
        ]
        select_clause = ",\n                ".join(select_list)
        sql = f"""
            SELECT TOP {limit_val}
                {select_clause}
            FROM Visit v WITH (NOLOCK)
            WHERE v.VisitDate >= ?
              AND v.VisitDate < DATEADD(day, 1, ?)
            ORDER BY v.VisitDate DESC
        """
        df = pd.read_sql(sql, conn, params=[from_date, to_date])
        if df is None or df.empty:
            return df
        df.columns = [c.strip() for c in df.columns]
        df["Unit"] = (unit or "").upper()
        return df
    except Exception as e:
        print(f"Error fetching visits ({unit}): {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def fetch_visits_search(unit: str, query: str, from_date: str, to_date: str, limit: int = 2000,
                        visit_type: str | None = None):
    """
    Search visits within a date window for matching Reg No, Patient, Visit No, or Admission No.
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Could not connect to {unit} for visits search")
        return None
    query = (query or "").strip()
    if not query:
        return pd.DataFrame()
    limit_val = 2000
    try:
        if limit is not None:
            limit_val = max(1, min(int(limit), 5000))
    except Exception:
        limit_val = 2000
    try:
        search_like = f"%{query}%"
        visit_type = (visit_type or "").strip().upper()
        type_filter_sql = ""
        type_params = []
        visit_cols = _get_table_columns(conn, "dbo.Visit")
        if visit_type and visit_type != "ALL" and (not visit_cols or "typeofvisit" in visit_cols):
            if visit_type == "HCV":
                type_filter_sql = " AND (v.TypeOfVisit LIKE ? OR v.TypeOfVisit LIKE ?)"
                type_params.extend(["%HCV%", "%HEALTH%"])
            else:
                type_filter_sql = " AND v.TypeOfVisit LIKE ?"
                type_params.append(f"%{visit_type}%")
        def col_expr(col: str) -> str:
            if not visit_cols:
                return f"v.{col}"
            return f"v.{col}" if col.lower() in visit_cols else f"NULL AS {col}"
        def doc_expr(col: str, alias: str) -> str:
            if not visit_cols:
                return f"dbo.fn_doctorfirstname(v.{col}) AS {alias}"
            return f"dbo.fn_doctorfirstname(v.{col}) AS {alias}" if col.lower() in visit_cols else f"NULL AS {alias}"
        def patient_expr(fn_name: str, alias: str) -> str:
            if not visit_cols:
                return f"CASE WHEN ISNULL(v.PatientID, 0) > 0 THEN dbo.{fn_name}(v.PatientID) ELSE NULL END AS {alias}"
            return f"CASE WHEN ISNULL(v.PatientID, 0) > 0 THEN dbo.{fn_name}(v.PatientID) ELSE NULL END AS {alias}" if "patientid" in visit_cols else f"NULL AS {alias}"

        select_list = [
            col_expr("Visit_ID"),
            col_expr("AdmissionNo"),
            col_expr("PatientID"),
            col_expr("VisitNo"),
            col_expr("VisitTypeID"),
            col_expr("VisitDate"),
            col_expr("TypeOfVisit"),
            col_expr("WardID"),
            col_expr("RoomID"),
            col_expr("BedID"),
            col_expr("DepartmentID"),
            col_expr("DocInCharge"),
            col_expr("RefDocID"),
            doc_expr("DocInCharge", "DocInChargeName"),
            doc_expr("RefDocID", "RefDocName"),
            doc_expr("subdocid", "SubDocName"),
            col_expr("DischargeType"),
            col_expr("DischargeDate"),
            col_expr("PatientType_ID"),
            col_expr("TariffScheme_ID"),
            col_expr("PatientSubType_ID"),
            col_expr("HealthCheckUpID"),
            col_expr("OutsideRefDoctor"),
            col_expr("subdocid"),
            col_expr("payType"),
            col_expr("IPPhNoc"),
            col_expr("IPPhNocDate"),
            col_expr("OPPhNoc"),
            col_expr("OPPhNocDate"),
            col_expr("OPPhNocBy"),
            col_expr("IPPhNocBy"),
            col_expr("billSubmitStatus"),
            col_expr("tag"),
            col_expr("openBy"),
            col_expr("openDatetime"),
            patient_expr("fn_patientfullname", "Patient"),
            patient_expr("fn_regno", "RegNo"),
        ]

        params = [from_date, to_date, search_like, search_like, search_like, search_like]
        extra_numeric = ""
        if query.isdigit():
            extra_numeric = " OR v.Visit_ID = ? OR v.PatientID = ?"
            params.extend([int(query), int(query)])
        select_clause = ",\n                ".join(select_list)
        sql = f"""
            SELECT TOP {limit_val}
                {select_clause}
            FROM Visit v WITH (NOLOCK)
            WHERE v.VisitDate >= ?
              AND v.VisitDate < DATEADD(day, 1, ?)
              {type_filter_sql}
              AND (
                v.VisitNo LIKE ?
                OR v.AdmissionNo LIKE ?
                OR dbo.fn_regno(v.PatientID) LIKE ?
                OR dbo.fn_patientfullname(v.PatientID) LIKE ?
                {extra_numeric}
              )
            ORDER BY v.VisitDate DESC
        """
        df = pd.read_sql(sql, conn, params=params[:2] + type_params + params[2:])
        if df is None or df.empty:
            return df
        df.columns = [c.strip() for c in df.columns]
        df["Unit"] = (unit or "").upper()
        return df
    except Exception as e:
        print(f"Error searching visits ({unit}): {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def fetch_modification_patients_search(unit: str, query: str, limit: int = 200):
    """
    Search active patients for virtual visit creation workflows.
    Matches PatientId, Registration_No, patient name parts, and mobile.
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Could not connect to {unit} for patient search")
        return None
    query = (query or "").strip()
    if not query:
        return pd.DataFrame()
    try:
        limit_val = max(1, min(int(limit), 1000))
    except Exception:
        limit_val = 200
    try:
        search_like = f"%{query}%"
        params = [search_like, search_like, search_like, search_like, search_like, search_like]
        extra_numeric_sql = ""
        if query.isdigit():
            extra_numeric_sql = " OR p.PatientId = ?"
            params.append(int(query))
        sql = f"""
            SELECT TOP {limit_val}
                p.PatientId,
                p.Registration_No,
                p.Registration_Date,
                CASE
                    WHEN ISNULL(p.PatientId, 0) > 0
                        THEN dbo.fn_patientfullname(p.PatientId)
                    ELSE NULL
                END AS Patient,
                p.Gender,
                p.Age,
                p.Mobile,
                p.PatientType_ID,
                p.PatientSubType_ID
            FROM Patient p WITH (NOLOCK)
            WHERE ISNULL(p.Deactive, 0) = 0
              AND (
                    CAST(p.PatientId AS VARCHAR(30)) LIKE ?
                    OR p.Registration_No LIKE ?
                    OR p.First_Name LIKE ?
                    OR p.Middle_Name LIKE ?
                    OR p.Last_Name LIKE ?
                    OR p.Mobile LIKE ?
                    {extra_numeric_sql}
                  )
            ORDER BY p.Registration_Date DESC, p.PatientId DESC
        """
        df = pd.read_sql(sql, conn, params=params)
        if df is None or df.empty:
            return df
        df.columns = [c.strip() for c in df.columns]
        if "Patient" in df.columns:
            df["PatientName"] = df["Patient"].fillna("").astype(str).str.strip()
        elif "PatientName" not in df.columns:
            df["PatientName"] = ""
        df["Unit"] = (unit or "").upper()
        return df
    except Exception as e:
        print(f"Error searching patients ({unit}): {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def fetch_virtual_visit_history(unit: str, limit: int | None = None):
    """
    Fetch recent Visit_Duplicate entries for audit/confirmation in the UI.
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Could not connect to {unit} for virtual visit history")
        return None
    limit_val = None
    if limit is not None:
        try:
            limit_val = max(1, min(int(limit), 200000))
        except Exception:
            limit_val = None
    try:
        vd_table = "dbo.Visit_Duplicate"
        p_table = "dbo.Patient"

        def _col_expr(prefix: str, table_name: str, candidates: list[str], alias: str) -> str:
            if prefix == "p" and (not patient_id_col_vd or not patient_id_col_p):
                return f"NULL AS {alias}"
            col = _resolve_column(conn, table_name, candidates)
            if not col:
                return f"NULL AS {alias}"
            return f"{prefix}.{col} AS {alias}"

        visit_id_col = _resolve_column(conn, vd_table, ["visitId", "VisitId", "VisitID", "Visit_ID"])
        inserted_on_col = _resolve_column(conn, vd_table, ["insertedOn", "InsertedOn", "InsertedON", "Inserted_On"])
        patient_id_col_vd = _resolve_column(conn, vd_table, ["patientId", "PatientId", "PatientID", "Patient_ID"])
        patient_id_col_p = _resolve_column(conn, p_table, ["PatientId", "patientId", "PatientID", "Patient_ID"])

        join_patient = bool(patient_id_col_vd and patient_id_col_p)
        if join_patient:
            patient_expr = f"CASE WHEN ISNULL(p.{patient_id_col_p}, 0) > 0 THEN dbo.fn_patientfullname(p.{patient_id_col_p}) ELSE NULL END AS Patient"
        else:
            patient_expr = "NULL AS Patient"

        join_sql = ""
        if join_patient:
            join_sql = f"LEFT JOIN Patient p WITH (NOLOCK) ON p.{patient_id_col_p} = vd.{patient_id_col_vd}"

        order_cols = []
        if inserted_on_col:
            order_cols.append(f"vd.{inserted_on_col} DESC")
        if visit_id_col:
            order_cols.append(f"vd.{visit_id_col} DESC")
        order_sql = f"ORDER BY {', '.join(order_cols)}" if order_cols else ""

        top_sql = f"TOP {limit_val} " if limit_val else ""

        sql = f"""
            SELECT {top_sql}
                {_col_expr('vd', vd_table, ['visitId', 'VisitId', 'VisitID', 'Visit_ID'], 'visitId')},
                {_col_expr('vd', vd_table, ['patientId', 'PatientId', 'PatientID', 'Patient_ID'], 'patientId')},
                {_col_expr('vd', vd_table, ['visitDate', 'VisitDate', 'Visit_Date'], 'visitDate')},
                {_col_expr('vd', vd_table, ['dischargeDate', 'DischargeDate', 'Discharge_Date'], 'dischargeDate')},
                {_col_expr('vd', vd_table, ['patientSubTypeId', 'PatientSubTypeId', 'PatientSubTypeID', 'PatientSubType_ID'], 'patientSubTypeId')},
                {_col_expr('vd', vd_table, ['docId', 'DocId', 'DocID', 'DoctorId', 'DoctorID'], 'docId')},
                {_col_expr('vd', vd_table, ['dischargeTypeId', 'DischargeTypeId', 'DischargeTypeID', 'DischargeType_ID'], 'dischargeTypeId')},
                {_col_expr('vd', vd_table, ['visitTypeId', 'VisitTypeId', 'VisitTypeID', 'VisitType_ID'], 'visitTypeId')},
                {_col_expr('vd', vd_table, ['visitStatus', 'VisitStatus'], 'visitStatus')},
                {_col_expr('vd', vd_table, ['insertedBy', 'InsertedBy', 'InsertedByUserID'], 'insertedBy')},
                {_col_expr('vd', vd_table, ['insertedOn', 'InsertedOn', 'InsertedON', 'Inserted_On'], 'insertedOn')},
                {_col_expr('p', p_table, ['Registration_No', 'RegistrationNo', 'Registration_No'], 'Registration_No')},
                {patient_expr}
            FROM Visit_Duplicate vd WITH (NOLOCK)
            {join_sql}
            {order_sql}
        """
        df = pd.read_sql(sql, conn)
        if df is None or df.empty:
            return df
        df.columns = [c.strip() for c in df.columns]
        if "Patient" in df.columns and "PatientName" not in df.columns:
            df["PatientName"] = df["Patient"]
        df["Unit"] = (unit or "").upper()
        return df
    except Exception as e:
        print(f"Error fetching virtual visit history ({unit}): {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass

# ===================== Payment Mode Master =====================
def fetch_payment_modes(unit: str):
    """
    Fetch payment mode master data for receipt payment mode updates.
    Attempts to normalize column names to PaymentModeId, PaymentModeName.
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Could not connect to {unit} for payment modes")
        return None
    try:
        df = pd.read_sql(
            """
            SELECT
                PModeId,
                PmodeCode,
                PModeName,
                UpdatedBy,
                UpdatedOn,
                Deactive,
                UpdatedMacName,
                UpdatedMacID,
                UpdatedIPAddress,
                InsertedByUserID,
                InsertedON,
                InsertedMacName,
                InsertedMacID,
                InsertedIPAddress
            FROM dbo.PaymentMode_mst
            """,
            conn
        )
        if df is None or df.empty:
            return df
        df.columns = [c.strip() for c in df.columns]
        cols_lower = {c.lower(): c for c in df.columns}
        def pick(candidates):
            for name in candidates:
                key = name.lower()
                if key in cols_lower:
                    return cols_lower[key]
            return None
        id_col = pick(["PaymentModeId", "PModeId", "PayModeId", "ModeId", "Id"])
        name_col = pick(["PaymentModeName", "PModeName", "PaymentMode", "ModeName", "Name"])
        if id_col and name_col:
            out = df[[id_col, name_col]].copy()
            out.columns = ["PaymentModeId", "PaymentModeName"]
            return out
        return df
    except Exception as e:
        print(f"Error fetching payment modes ({unit}): {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass

# ===================== Bill-linked Receipts =====================
def fetch_bill_receipts(unit: str, bill_id: int | None = None, bill_no: str | None = None,
                        reg_no: str | None = None, bill_date: str | None = None,
                        bill_type: str | None = None):
    """
    Fetch receipts linked to a bill via Visit_ID, limited to ReceiptType based on bill type.
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Could not connect to {unit} for bill receipts")
        return None
    try:
        cursor = conn.cursor()
        if bill_id:
            cursor.execute("""
                SELECT TOP 1 Bill_ID, BillNo, Visit_ID
                FROM Billing_Mst WITH (NOLOCK)
                WHERE Bill_ID = ?
                ORDER BY BillDate DESC
            """, bill_id)
        elif bill_no:
            cursor.execute("""
                SELECT TOP 1 Bill_ID, BillNo, Visit_ID
                FROM Billing_Mst WITH (NOLOCK)
                WHERE BillNo = ?
                ORDER BY BillDate DESC
            """, bill_no)
        elif reg_no and bill_date:
            cursor.execute("""
                SELECT TOP 1 Bill_ID, BillNo, Visit_ID
                FROM Billing_Mst WITH (NOLOCK)
                WHERE Registration_No = ?
                  AND BillDate >= ?
                  AND BillDate < DATEADD(day, 1, ?)
                ORDER BY BillDate DESC
            """, (reg_no, bill_date, bill_date))
        else:
            return pd.DataFrame()

        row = cursor.fetchone()
        if not row or not getattr(row, "Visit_ID", None):
            return pd.DataFrame()
        visit_id = row.Visit_ID
        bill_type_norm = str(bill_type or "").strip().upper()
        receipt_type = "PH" if bill_type_norm == "PH" else "P"

        df = pd.read_sql(
            """
            SELECT Receipt_ID, Receipt_No, Visit_ID, Amount,
                   Receipt_Date, Receipt_Time, PaymentMode,
                   ReceiptType, CancelStatus, CancelReasonid
            FROM Receipt_mst WITH (NOLOCK)
            WHERE Visit_ID = ? AND ReceiptType = ?
            ORDER BY Receipt_Date DESC
            """,
            conn,
            params=[visit_id, receipt_type]
        )
        if df is None or df.empty:
            return df
        df.columns = [c.strip() for c in df.columns]
        df["Unit"] = (unit or "").upper()
        return df
    except Exception as e:
        print(f"Error fetching bill receipts ({unit}): {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


# ===================== Radiology Summary (Imaging MIS) =====================
def fetch_radiology_summary(unit: str, from_date: str, to_date: str):
    """
    Calls dbo.usp_RadiologySummary to fetch radiology revenue/volume rows.
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Could not connect to {unit} for radiology summary")
        return None
    try:
        sql = "EXEC dbo.usp_RadiologySummary @FromDate=?, @ToDate=?"
        df = pd.read_sql(sql, conn, params=[from_date, to_date])
        if df is None or df.empty:
            return df
        df.columns = [c.strip() for c in df.columns]
        df["Unit"] = (unit or "").upper()
        return df
    except Exception as e:
        print(f"Error fetching radiology summary ({unit}): {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


# ===================== Diagnostics Share (Billing MIS) =====================
def fetch_diagnostics_share(unit: str, from_date: str, to_date: str, doc_id: int):
    """
    Calls dbo.usp_DiagnosticsShare to fetch diagnostics share rows.
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Could not connect to {unit} for diagnostics share")
        return None
    try:
        sql = "SET NOCOUNT ON; EXEC dbo.usp_DiagnosticsShare @FromDate=?, @ToDate=?, @DocId=?"
        df = pd.read_sql(sql, conn, params=[from_date, to_date, doc_id])
        if df is None or df.empty:
            return df
        df.columns = [c.strip() for c in df.columns]
        df["Unit"] = (unit or "").upper()
        return df
    except Exception as e:
        print(f"Error fetching diagnostics share ({unit}): {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


# ===================== Doctor Medicine Sale & Issue =====================
def fetch_doct_medicine_sale_issue(unit: str, from_date: str, to_date: str, doc_id: int):
    """
    Calls dbo.usp_DoctMedicine_SaleAndIssue and returns (sale_df, issue_df).
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Could not connect to {unit} for doctor medicine sale/issue")
        return None, None
    try:
        cur = conn.cursor()
        sql = "SET NOCOUNT ON; EXEC dbo.usp_DoctMedicine_SaleAndIssue @FromDate=?, @ToDate=?, @DocId=?"
        cur.execute(sql, (from_date, to_date, doc_id))

        def _read_current_result(cursor: pyodbc.Cursor):
            if cursor.description is None:
                return pd.DataFrame()
            cols = [col[0] for col in cursor.description]
            rows = cursor.fetchall()
            if not rows:
                return pd.DataFrame(columns=cols)
            df = pd.DataFrame.from_records(rows, columns=cols)
            df.columns = [c.strip() for c in df.columns]
            return df

        sale_df = _read_current_result(cur)
        issue_df = pd.DataFrame()
        try:
            if cur.nextset():
                issue_df = _read_current_result(cur)
        except Exception:
            issue_df = pd.DataFrame()

        return sale_df, issue_df
    except Exception as e:
        print(f"Error fetching doctor medicine sale/issue ({unit}): {e}")
        return None, None
    finally:
        try:
            conn.close()
        except Exception:
            pass


# ===================== Doctorwise DPV Procedures =====================
def fetch_doctorwise_dpv_procedures(unit: str, from_date: str, to_date: str, doc_id: int):
    """
    Calls dbo.usp_DoctorwiseDPVProcedures to fetch DPV procedure rows.
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Could not connect to {unit} for doctorwise DPV procedures")
        return None
    try:
        sql = "SET NOCOUNT ON; EXEC dbo.usp_DoctorwiseDPVProcedures @FromDate=?, @ToDate=?, @ID=?"
        df = pd.read_sql(sql, conn, params=[from_date, to_date, doc_id])
        if df is None or df.empty:
            return df
        df.columns = [c.strip() for c in df.columns]
        df["Unit"] = (unit or "").upper()
        return df
    except Exception as e:
        print(f"Error fetching doctorwise DPV procedures ({unit}): {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


# ===================== Blood Bank Service-wise Billing =====================
def fetch_bloodbank_service_billing(unit: str, from_date: str, to_date: str):
    """
    Returns combined Blood Bank billing + order rows:
    - Billing rows via dbo.Usp_ServiceWiseBilling_BloodBank
    - Order rows via OrderMst/OrderDtl (+ User_Mst tracking)

    Notes:
    - Order rows include pending/shifted/cancelled status markers.
    - Order date window is anchored to OrderMst.OrdDateTime.
      Shifted/Billing dates are still surfaced in columns for audit trail.
    - Blood Bank service filter is unit-specific for known units:
      AHL -> (2598,2596,2600,4801,2599,6658,2004,2597,6605)
      ACI -> (2598,2596,2600,4801,2599,6658,2004,2597,5535)
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Could not connect to {unit} for blood bank service-wise billing")
        return None
    try:
        unit_key = (unit or "").strip().upper()
        shared_ids = (2598, 2596, 2600, 4801, 2599, 6658, 2004, 2597)
        unit_service_ids = {
            "AHL": shared_ids + (6605,),
            "ACI": shared_ids + (5535,),
        }
        bloodbank_service_ids = unit_service_ids.get(unit_key, shared_ids + (6605, 5535))
        bloodbank_service_ids_sql = ",".join(str(int(v)) for v in bloodbank_service_ids)

        billing_frames = []
        billing_sql = "SET NOCOUNT ON; EXEC dbo.Usp_ServiceWiseBilling_BloodBank @fromdate=?, @todate=?"
        # For AHL/ACI we rely on explicit ID-mapped SQL below to avoid cross-unit ID mismatch.
        if unit_key not in {"AHL", "ACI"}:
            try:
                billing_sp_df = pd.read_sql(billing_sql, conn, params=[from_date, to_date])
                if billing_sp_df is not None and not billing_sp_df.empty:
                    billing_sp_df.columns = [c.strip() for c in billing_sp_df.columns]
                    billing_frames.append(billing_sp_df)
            except Exception as sp_err:
                print(f"Blood bank SP fallback engaged for {unit}: {sp_err}")

        billing_fallback_sql = f"""
            SET NOCOUNT ON;

            SELECT
                v.TypeOfVisit AS TypeOfVisit,
                dbo.fn_patientfullname(v.PatientID) AS Patient,
                dbo.fn_regno(v.PatientID) AS RegNo,
                s.Service_Name,
                CAST(bd.Rate AS DECIMAL(18,2)) AS Rate,
                CAST(bd.Quantity AS DECIMAL(18,2)) AS Quantity,
                s.Category_Id,
                v.PatientID,
                dbo.Fn_subDept(s.SubDepartmentId) AS Sub_Dept,
                bm.BillDate,
                v.VisitDate,
                v.DischargeDate
            FROM dbo.Billing_Mst bm WITH (NOLOCK)
            INNER JOIN dbo.BillingDetails bd WITH (NOLOCK)
                ON bm.Bill_ID = bd.Bill_ID
            LEFT JOIN dbo.Service_Mst s WITH (NOLOCK)
                ON bd.ServiceID = s.Service_ID
            LEFT JOIN dbo.Visit v WITH (NOLOCK)
                ON v.Visit_ID = bm.Visit_ID
            WHERE bm.BillDate >= ?
              AND bm.BillDate < DATEADD(DAY, 1, ?)
              AND bm.BillType = 'P'
              AND bd.ServiceID IN ({bloodbank_service_ids_sql})
        """
        billing_fb_df = pd.read_sql(billing_fallback_sql, conn, params=[from_date, to_date])
        if billing_fb_df is not None and not billing_fb_df.empty:
            billing_fb_df.columns = [c.strip() for c in billing_fb_df.columns]
            billing_frames.append(billing_fb_df)

        billing_df = pd.concat(billing_frames, ignore_index=True, copy=False) if billing_frames else pd.DataFrame()
        if billing_df is not None and not billing_df.empty:
            billing_df.columns = [c.strip() for c in billing_df.columns]
            billing_df["RecordSource"] = "BILLING"
            billing_df["OrderStatus"] = "Billed"
            billing_df["PendingToBillingFlag"] = 0

        order_sql = """
            SET NOCOUNT ON;

            SELECT
                v.TypeOfVisit AS TypeOfVisit,
                dbo.fn_patientfullname(pctx.EffectivePatientID) AS Patient,
                dbo.fn_regno(pctx.EffectivePatientID) AS RegNo,
                s.Service_Name,
                CAST(COALESCE(NULLIF(od.ServiceAmount, 0), bill_line.BillRate, 0) AS DECIMAL(18,2)) AS Rate,
                CAST(COALESCE(NULLIF(od.OrdQty, 0), bill_line.BillQty, 0) AS DECIMAL(18,2)) AS Quantity,
                s.Category_Id,
                pctx.EffectivePatientID AS PatientID,
                dbo.Fn_subDept(s.SubDepartmentId) AS Sub_Dept,
                bm.BillDate,
                v.VisitDate,
                v.DischargeDate,
                om.OrdId AS OrderId,
                om.OrdNo AS OrderNo,
                om.OrdVoucherNo AS OrderVoucherNo,
                om.OrdDateTime AS OrderDateTime,
                od.OrdDtlID AS OrderDetailId,
                od.ServBilled,
                od.ServBillId,
                CASE
                    WHEN ISNULL(od.Cancelled, 0) = 1 OR ISNULL(om.OrdCanceled, 0) = 1 THEN 1
                    ELSE 0
                END AS IsOrderCancelled,
                COALESCE(od.CancelDateTime, CASE WHEN ISNULL(om.OrdCanceled, 0) = 1 THEN om.UpdatedOn END) AS OrderCancelledAt,
                COALESCE(NULLIF(uc.UserName, ''), CAST(om.InsertedByUserID AS NVARCHAR(80))) AS OrderCreatedBy,
                COALESCE(NULLIF(udc.UserName, ''), NULLIF(umc.UserName, ''), CAST(COALESCE(od.CanceledBy, om.OrdCanceledBy) AS NVARCHAR(80))) AS OrderCancelledBy,
                bm.BillDate AS ShiftedToBillingDate,
                CASE
                    WHEN ISNULL(od.Cancelled, 0) = 1 OR ISNULL(om.OrdCanceled, 0) = 1 THEN 'Cancelled'
                    WHEN ISNULL(od.ServBilled, 0) = 1 OR od.ServBillId IS NOT NULL THEN 'Shifted to Billing'
                    ELSE 'Pending Billing'
                END AS OrderStatus,
                CASE
                    WHEN ISNULL(od.Cancelled, 0) = 1 OR ISNULL(om.OrdCanceled, 0) = 1 THEN 0
                    WHEN ISNULL(od.ServBilled, 0) = 1 OR od.ServBillId IS NOT NULL THEN 0
                    ELSE 1
                END AS PendingToBillingFlag,
                CAST(COALESCE(NULLIF(od.NetAmount, 0),
                        COALESCE(NULLIF(od.ServiceAmount, 0), bill_line.BillRate, 0) * COALESCE(NULLIF(od.OrdQty, 0), bill_line.BillQty, 0), 0) AS DECIMAL(18,2)) AS Amount,
                CASE
                    WHEN ISNULL(od.Cancelled, 0) = 1 OR ISNULL(om.OrdCanceled, 0) = 1
                        THEN CONCAT('Cancelled on ', CONVERT(VARCHAR(16), COALESCE(od.CancelDateTime, om.UpdatedOn), 120))
                    WHEN ISNULL(od.ServBilled, 0) = 1 OR od.ServBillId IS NOT NULL
                        THEN CONCAT('Ordered ', CONVERT(VARCHAR(16), om.OrdDateTime, 120), ' -> Shifted ', CONVERT(VARCHAR(16), bm.BillDate, 120))
                    ELSE CONCAT('Pending since ', CONVERT(VARCHAR(16), om.OrdDateTime, 120))
                END AS ShiftHistory,
                'ORDER' AS RecordSource
            FROM dbo.OrderMst om WITH (NOLOCK)
            INNER JOIN dbo.OrderDtl od WITH (NOLOCK)
                ON om.OrdId = od.OrdID
            LEFT JOIN dbo.Visit v WITH (NOLOCK)
                ON v.Visit_ID = om.OrdVisitID
            LEFT JOIN dbo.Service_Mst s WITH (NOLOCK)
                ON od.ServiceId = s.Service_ID
            LEFT JOIN dbo.Billing_Mst bm WITH (NOLOCK)
                ON bm.Bill_ID = od.ServBillId
            OUTER APPLY (
                SELECT COALESCE(NULLIF(v.PatientID, 0), NULLIF(om.OrdPatientID, 0), NULLIF(od.PatientId, 0)) AS EffectivePatientID
            ) pctx
            OUTER APPLY (
                SELECT TOP 1
                    CAST(bd.Rate AS DECIMAL(18,2)) AS BillRate,
                    CAST(bd.Quantity AS DECIMAL(18,2)) AS BillQty
                FROM dbo.BillingDetails bd WITH (NOLOCK)
                WHERE bd.Bill_ID = od.ServBillId
                  AND bd.ServiceID = od.ServiceId
            ) bill_line
            LEFT JOIN dbo.User_Mst uc WITH (NOLOCK)
                ON uc.UserID = om.InsertedByUserID
            LEFT JOIN dbo.User_Mst umc WITH (NOLOCK)
                ON umc.UserID = om.OrdCanceledBy
            LEFT JOIN dbo.User_Mst udc WITH (NOLOCK)
                ON udc.UserID = od.CanceledBy
            WHERE od.ServiceId IN ({bloodbank_service_ids_sql})
              AND om.OrdDateTime >= ?
              AND om.OrdDateTime < DATEADD(DAY, 1, ?)
        """.format(bloodbank_service_ids_sql=bloodbank_service_ids_sql)
        order_df = pd.read_sql(
            order_sql,
            conn,
            params=[from_date, to_date],
        )
        if order_df is not None and not order_df.empty:
            order_df.columns = [c.strip() for c in order_df.columns]

        frames = []
        if billing_df is not None and not billing_df.empty:
            frames.append(billing_df)
        if order_df is not None and not order_df.empty:
            frames.append(order_df)

        if not frames:
            return pd.DataFrame()

        combined = pd.concat(frames, ignore_index=True, copy=False)
        combined.columns = [c.strip() for c in combined.columns]
        combined["Unit"] = (unit or "").upper()

        # Prefer enriched ORDER rows over plain BILLING rows when both represent same billed item.
        def _norm_text(v):
            return str(v or "").strip().upper()

        def _dedupe_key(row):
            return (
                _norm_text(row.get("RegNo")),
                _norm_text(row.get("Service_Name")),
                _norm_text(row.get("BillDate")),
                round(float(pd.to_numeric(row.get("Quantity"), errors="coerce") or 0.0), 3),
                round(float(pd.to_numeric(row.get("Rate"), errors="coerce") or 0.0), 2),
                _norm_text(row.get("TypeOfVisit")),
            )

        if not combined.empty:
            src_series = combined.get("RecordSource", pd.Series([""] * len(combined), index=combined.index))
            src_series = src_series.astype(str).str.strip().str.upper()
            combined["_src_priority"] = src_series.map(lambda s: 0 if s == "ORDER" else 1)
            combined["_dedupe_key"] = combined.apply(_dedupe_key, axis=1)
            combined = (
                combined.sort_values(["_src_priority"])
                .drop_duplicates(subset=["_dedupe_key"], keep="first")
                .drop(columns=["_src_priority", "_dedupe_key"], errors="ignore")
                .reset_index(drop=True)
            )

            # Second-pass reconciliation:
            # if an ORDER row is already shifted to billing, hide matching BILLING-only row.
            def _date_only(v):
                dt = pd.to_datetime(v, errors="coerce")
                if pd.notna(dt):
                    return dt.strftime("%Y-%m-%d")
                s = str(v or "").strip()
                return s[:10] if len(s) >= 10 else s

            def _to_float(v, default=0.0):
                num = pd.to_numeric(v, errors="coerce")
                if pd.isna(num):
                    return float(default)
                return float(num)

            def _to_int(v, default=0):
                num = pd.to_numeric(v, errors="coerce")
                if pd.isna(num):
                    return int(default)
                return int(num)

            def _num(v, places):
                return round(_to_float(v, 0.0), places)

            def _recon_amount(row):
                amt = _to_float(row.get("Amount"), float("nan"))
                if not pd.isna(amt):
                    return round(amt, 2)
                return round(
                    _num(row.get("Quantity"), 3) * _num(row.get("Rate"), 2),
                    2,
                )

            def _recon_key(row):
                return (
                    _norm_text(row.get("RegNo")),
                    _to_int(row.get("PatientID"), 0),
                    _norm_text(row.get("Service_Name")),
                    _date_only(row.get("BillDate")),
                    _num(row.get("Quantity"), 3),
                    _recon_amount(row),
                )

            def _patient_token(row):
                reg = _norm_text(row.get("RegNo"))
                if reg:
                    return f"REG:{reg}"
                pid = _to_int(row.get("PatientID"), 0)
                if pid > 0:
                    return f"PID:{pid}"
                pname = _norm_text(row.get("Patient"))
                if pname:
                    return f"PAT:{pname}"
                return ""

            def _loose_key(row):
                return (
                    _patient_token(row),
                    _norm_text(row.get("Service_Name")),
                    _date_only(row.get("BillDate")),
                )

            src_series = combined.get("RecordSource", pd.Series([""] * len(combined), index=combined.index))
            src_series = src_series.astype(str).str.strip().str.upper()
            status_series = combined.get("OrderStatus", pd.Series([""] * len(combined), index=combined.index))
            status_series = status_series.astype(str).str.strip().str.lower()
            serv_billed = pd.to_numeric(
                combined.get("ServBilled", pd.Series([0] * len(combined), index=combined.index)),
                errors="coerce",
            ).fillna(0).astype(int)
            serv_bill_id = pd.to_numeric(
                combined.get("ServBillId", pd.Series([0] * len(combined), index=combined.index)),
                errors="coerce",
            ).fillna(0).astype(int)

            shifted_order_mask = (
                src_series.eq("ORDER")
                & (serv_billed.eq(1) | serv_bill_id.gt(0) | status_series.str.contains("shift"))
            )
            if shifted_order_mask.any():
                billing_mask = src_series.eq("BILLING")
                shifted_idx = combined.index[shifted_order_mask].tolist()
                billing_idx = combined.index[billing_mask].tolist()

                def _group_by_key(indices, key_fn):
                    grouped = {}
                    for ridx in indices:
                        key = key_fn(combined.loc[ridx])
                        grouped.setdefault(key, []).append(ridx)
                    return grouped

                consumed_shifted = set()
                drop_idx = set()

                # Pass 1: strict pair matching (includes qty/amount).
                strict_shifted = _group_by_key(shifted_idx, _recon_key)
                strict_billing = _group_by_key(billing_idx, _recon_key)
                for key, s_rows in strict_shifted.items():
                    b_rows = strict_billing.get(key, [])
                    pair_count = min(len(s_rows), len(b_rows))
                    if pair_count <= 0:
                        continue
                    consumed_shifted.update(s_rows[:pair_count])
                    drop_idx.update(b_rows[:pair_count])

                # Pass 2: fallback loose matching for remaining pairs (patient+service+bill date).
                rem_shifted = [r for r in shifted_idx if r not in consumed_shifted]
                rem_billing = [r for r in billing_idx if r not in drop_idx]
                loose_shifted = _group_by_key(rem_shifted, _loose_key)
                loose_billing = _group_by_key(rem_billing, _loose_key)
                for key, s_rows in loose_shifted.items():
                    patient_key, service_key, bill_date_key = key
                    if not patient_key or not service_key or not bill_date_key:
                        continue
                    b_rows = loose_billing.get(key, [])
                    pair_count = min(len(s_rows), len(b_rows))
                    if pair_count <= 0:
                        continue
                    drop_idx.update(b_rows[:pair_count])

                if drop_idx:
                    combined = combined.drop(index=list(drop_idx)).reset_index(drop=True)
        return combined
    except Exception as e:
        print(f"Error fetching blood bank service-wise billing ({unit}): {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


# ===================== Billwise Doctor Share =====================
def fetch_billwise_doctorshare(unit: str, from_date: str, to_date: str, doc_id: int, vtype: int):
    """
    Calls dbo.usp_BillwiseDoctorshare to fetch billwise doctor share rows.
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Could not connect to {unit} for billwise doctor share")
        return None
    try:
        sql = "SET NOCOUNT ON; EXEC dbo.usp_BillwiseDoctorshare @FromDate=?, @ToDate=?, @DocId=?, @vtype=?"
        df = pd.read_sql(sql, conn, params=[from_date, to_date, doc_id, vtype])
        if df is None or df.empty:
            return df
        df.columns = [c.strip() for c in df.columns]
        df["Unit"] = (unit or "").upper()
        return df
    except Exception as e:
        print(f"Error fetching billwise doctor share ({unit}): {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


# ===================== Doctor Directory =====================
def fetch_doctor_directory(unit: str):
    """
    Returns empid + doctor name list for diagnostics selection.
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Could not connect to {unit} for doctor directory")
        return None
    try:
        sql = """
            SELECT
                empid AS DoctorId,
                dbo.fn_doctorfirstname(empid) AS Doctor
            FROM employee_mst
            WHERE emptype = 'Doc'
            ORDER BY Doctor
        """
        df = pd.read_sql(sql, conn)
        if df is None or df.empty:
            return df
        df.columns = [c.strip() for c in df.columns]
        return df
    except Exception as e:
        print(f"Error fetching doctor directory ({unit}): {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


# ===================== GST Compliance: Drug Sale GST-wise =====================
def fetch_gst_drug_sales(unit: str, start_date: str, end_date: str):
    """
    Calls dbo.GetDrugSaleDetailsGSTwise @StartDate, @EndDate.
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Could not connect to {unit} for GST drug sales")
        return None
    try:
        sql = "SET NOCOUNT ON; EXEC dbo.GetDrugSaleDetailsGSTwise @StartDate=?, @EndDate=?"
        df = pd.read_sql(sql, conn, params=[start_date, end_date])
        if df is None or df.empty:
            return df
        df.columns = [c.strip() for c in df.columns]
        df["Unit"] = (unit or "").upper()
        return df
    except Exception as e:
        print(f"Error fetching GST drug sales ({unit}): {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


# ===================== GST Compliance: Drug Return GST-wise =====================
def fetch_gst_drug_returns(unit: str, start_date: str, end_date: str):
    """
    Calls dbo.GetDrugreturnDetailsGSTwise @StartDate, @EndDate.
    """
    conn = get_sql_connection(unit)
    if not conn:
        print(f"Could not connect to {unit} for GST drug returns")
        return None
    try:
        sql = "SET NOCOUNT ON; EXEC dbo.GetDrugreturnDetailsGSTwise @StartDate=?, @EndDate=?"
        df = pd.read_sql(sql, conn, params=[start_date, end_date])
        if df is None or df.empty:
            return df
        df.columns = [c.strip() for c in df.columns]
        df["Unit"] = (unit or "").upper()
        return df
    except Exception as e:
        print(f"Error fetching GST drug returns ({unit}): {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


# ===================== Receipt Cancellation =====================
def apply_receipt_cancellation(unit: str, receipt_no: str, canceled_by: str) -> dict:
    """
    Cancels a receipt in Receipt_mst for the specified unit.
    Sets CancelStatus=1, CancelReasonid=1, CancelDate=GETDATE(), CanceledBy=<user>.
    Uses Receipt_No as the primary identifier.
    """
    unit_norm = (unit or "").strip().upper()
    if not unit_norm:
        return {"success": False, "error": "Unit is required"}
    if not receipt_no:
        return {"success": False, "error": "receipt_no is required"}

    conn = get_sql_connection(unit_norm)
    if not conn:
        return {"success": False, "error": f"Unable to connect to unit {unit_norm}"}
    try:
        try:
            conn.autocommit = False
        except Exception:
            pass
        cursor = conn.cursor()
        canceled_by_id = None
        if canceled_by not in (None, "", " "):
            try:
                canceled_by_id = int(str(canceled_by).strip())
            except Exception:
                pass
        # Try resolving username to AccountId if we didn't get a numeric id
        if canceled_by_id is None and canceled_by not in (None, "", " "):
            try:
                cursor.execute("SELECT TOP 1 AccountId FROM HID_User_Mst WHERE UserName = ?", (canceled_by,))
                row = cursor.fetchone()
                if row and row[0] is not None:
                    canceled_by_id = int(row[0])
            except Exception:
                pass
        if canceled_by_id is None:
            canceled_by_id = 0  # fallback to 0 if we cannot resolve

        cursor.execute(
            """
            SELECT TOP 1 Receipt_ID, Receipt_No, Visit_ID, Amount, ReceiptType, CancelStatus
            FROM Receipt_mst WITH (ROWLOCK, UPDLOCK)
            WHERE Receipt_No = ?;
            """,
            (receipt_no,),
        )
        row = cursor.fetchone()
        if not row:
            conn.rollback()
            return {"success": False, "error": "Receipt not found"}
        if getattr(row, "CancelStatus", None) in (1, True, "1"):
            conn.rollback()
            return {"success": False, "error": "Receipt already cancelled"}

        receipt_type = str(getattr(row, "ReceiptType", "") or "").strip().upper()
        visit_id = getattr(row, "Visit_ID", None)
        amount = Decimal(str(getattr(row, "Amount", 0) or 0))

        cursor.execute(
            """
            UPDATE Receipt_mst
            SET CancelStatus = 1,
                CancelReasonid = 1,
                CancelDate = GETDATE(),
                CanceledBy = ?
            WHERE Receipt_No = ?;
            """,
            (canceled_by_id, receipt_no),
        )

        if receipt_type in ("P", "PH") and visit_id:
            bill_type = "PH" if receipt_type == "PH" else "P"
            cursor.execute(
                """
                UPDATE Billing_Mst
                SET DueAmount = CASE
                    WHEN NetAmount IS NOT NULL
                         AND NetAmount > 0
                         AND ISNULL(DueAmount, 0) + ? > NetAmount
                        THEN NetAmount
                    ELSE ISNULL(DueAmount, 0) + ?
                END,
                UpdatedOn = GETDATE()
                WHERE Visit_ID = ? AND BillType = ?;
                """,
                (float(amount), float(amount), visit_id, bill_type),
            )

        conn.commit()
        return {"success": True, "rows_affected": cursor.rowcount}
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        return {"success": False, "error": str(e)}
    finally:
        try:
            conn.close()
        except Exception:
            pass


 # ===================== Service Search (Autofill) =====================
def search_services_in_db(unit: str, query: str):
    """
    Searches Service_Mst for active matching service names.
    Returns top 20 matches (id + name) to keep it fast.
    """
    conn = get_sql_connection(unit)
    if not conn:
        return []
    
    try:
        # GROUP BY avoids duplicates if master has messy data
        # TOP 20 limits the dropdown size for performance
        sql = """
        SELECT TOP 20 MIN(Service_ID) AS Service_ID, Service_Name
        FROM dbo.Service_Mst
        WHERE ISNULL(Deactive, 0) = 0
          AND Service_Name LIKE ?
        GROUP BY Service_Name
        ORDER BY Service_Name
        """
        # The query param will be '%echo%'
        search_pattern = f"%{query}%"
        
        cursor = conn.cursor()
        cursor.execute(sql, (search_pattern,))
        
        results = [{"service_id": row[0], "service_name": row[1]} for row in cursor.fetchall()]
        return results
        
    except Exception as e:
        print(f"âš ï¸ Error searching services ({unit}): {e}")
        return []
    finally:
        try:
            conn.close()
        except:
            pass       


# ===================== Service Search (Ledger Scoped) =====================
def search_services_in_ledger(unit: str, from_date: str, to_date: str, query: str):
    """
    Searches service names within usp_GetServiceConsumptions for a date range.
    Returns top 20 distinct matches to keep it fast.
    """
    conn = get_sql_connection(unit)
    if not conn:
        return []
    try:
        svc_param = f"%{query}%"
        sql = "EXEC dbo.usp_GetServiceConsumptions @StartDate=?, @EndDate=?, @ServiceName=?"
        df = pd.read_sql(sql, conn, params=[from_date, to_date, svc_param])
        if df is None or df.empty:
            return []
        df.columns = [c.strip() for c in df.columns]
        cols_map = {str(c).strip().lower(): c for c in df.columns}
        svc_col = None
        for cand in ["service_name", "servicename", "service"]:
            if cand in cols_map:
                svc_col = cols_map[cand]
                break
        if not svc_col:
            return []
        series = df[svc_col].dropna().astype(str)
        q = query.lower().strip()
        if q:
            series = series[series.str.lower().str.contains(q)]
        unique = series.drop_duplicates().head(20).tolist()
        return unique
    except Exception as e:
        print(f"Error searching services in ledger ({unit}): {e}")
        return []
    finally:
        try:
            conn.close()
        except:
            pass


# ===================== IP Package Service Addition =====================
def _rows_as_dicts(cursor):
    cols = [c[0] for c in (cursor.description or [])]
    rows = cursor.fetchall()
    out = []
    for row in rows:
        rec = {}
        for idx, col in enumerate(cols):
            val = row[idx]
            if isinstance(val, datetime):
                val = val.isoformat(sep=" ")
            elif hasattr(val, "isoformat") and val.__class__.__name__ == "date":
                val = val.isoformat()
            if isinstance(val, Decimal):
                val = float(val)
            if isinstance(val, memoryview):
                val = val.tobytes().decode(errors="ignore")
            elif isinstance(val, bytes):
                val = val.decode(errors="ignore")
            rec[col] = val
        out.append(rec)
    return out


def _to_int(raw, default=None):
    if raw is None:
        return default
    try:
        txt = str(raw).strip()
        if txt == "":
            return default
        return int(float(txt))
    except Exception:
        return default


def _to_decimal(raw, default=Decimal("0")):
    if raw is None:
        return default
    try:
        txt = str(raw).strip()
        if txt == "":
            return default
        return Decimal(txt)
    except Exception:
        return default


def _to_bool(raw, default=False) -> bool:
    if raw is None:
        return default
    if isinstance(raw, bool):
        return raw
    txt = str(raw).strip().lower()
    if txt in {"1", "true", "yes", "y", "on"}:
        return True
    if txt in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _to_datetime(raw):
    if isinstance(raw, datetime):
        return raw
    if raw is None:
        return None
    txt = str(raw).strip()
    if not txt:
        return None
    try:
        return datetime.fromisoformat(txt)
    except Exception:
        pass
    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(txt, fmt)
        except Exception:
            pass
    return None


def _to_int_list(raw):
    if raw is None:
        return []
    items = []
    if isinstance(raw, (list, tuple, set)):
        items = list(raw)
    elif isinstance(raw, str):
        txt = raw.strip()
        if not txt:
            return []
        try:
            parsed = json.loads(txt)
            if isinstance(parsed, (list, tuple, set)):
                items = list(parsed)
            else:
                items = re.split(r"[,\|/]+", txt)
        except Exception:
            items = re.split(r"[,\|/]+", txt)
    else:
        items = [raw]

    vals = []
    seen = set()
    for item in items:
        iv = _to_int(item)
        if iv is None or iv <= 0 or iv in seen:
            continue
        vals.append(iv)
        seen.add(iv)
    return vals


def _parse_ip_package_services(raw):
    if raw is None:
        return []
    items = []
    if isinstance(raw, (list, tuple, set)):
        items = list(raw)
    elif isinstance(raw, dict):
        items = [raw]
    elif isinstance(raw, str):
        txt = raw.strip()
        if not txt:
            return []
        try:
            parsed = json.loads(txt)
            if isinstance(parsed, (list, tuple, set)):
                items = list(parsed)
            elif isinstance(parsed, dict):
                items = [parsed]
            else:
                items = [parsed]
        except Exception:
            return []
    else:
        return []

    rows = []
    for item in items:
        if not isinstance(item, dict):
            continue
        service_id = _to_int(item.get("service_id") or item.get("pServiceid") or item.get("ServiceID"))
        if not service_id:
            continue
        qty = _to_int(item.get("quantity") or item.get("pQuantity") or item.get("Quantity"), 1) or 1
        rate = _to_decimal(item.get("rate") or item.get("pRate") or item.get("Rate"), Decimal("0"))
        amount = _to_decimal(item.get("amount"), rate * Decimal(qty))
        rows.append({
            "service_id": service_id,
            "service_code": str(item.get("service_code") or item.get("Service_Code") or "").strip(),
            "service_name": str(item.get("service_name") or item.get("Service_Name") or "").strip(),
            "category_id": _to_int(item.get("category_id") or item.get("Category_Id") or item.get("ServiceCategoryID")),
            "category_code": str(
                item.get("service_category_code")
                or item.get("ServiceCategory_Code")
                or item.get("ServiceCategoryCode")
                or ""
            ).strip(),
            "cghs_code": str(item.get("cghs_code") or item.get("CGHSCode") or "").strip(),
            "quantity": max(1, qty),
            "rate": rate,
            "amount": amount,
        })
    return rows


def fetch_ip_package_init(unit: str):
    conn = get_sql_connection(unit)
    if not conn:
        return {"status": "error", "message": "Unable to connect to database"}
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT TariffScheme_ID, TariffScheme_Code, TariffScheme_Desc, Deactive, IsBaseTariffScheme
            FROM dbo.CPATariffScheme_Mst
            ORDER BY TariffScheme_Desc
        """)
        schemes = _rows_as_dicts(cursor)
        return {
            "status": "success",
            "unit": (unit or "").upper(),
            "tariff_schemes": schemes,
            "search_modes": [
                {"id": 1, "label": "Service Name"},
                {"id": 2, "label": "Service Code"},
                {"id": 3, "label": "CGHS Code"},
            ],
        }
    except Exception as e:
        return {"status": "error", "message": f"Failed to load IP package masters: {e}"}
    finally:
        try:
            conn.close()
        except Exception:
            pass


def search_ip_packages(unit: str, query: str = "", page: int = 1, page_size: int = 20):
    conn = get_sql_connection(unit)
    if not conn:
        return {"status": "error", "message": "Unable to connect to database"}
    try:
        q = (query or "").strip()
        page = max(1, _to_int(page, 1) or 1)
        page_size = max(1, min(100, _to_int(page_size, 20) or 20))

        where_sql = "ISNULL(HealthPlanCode, '') LIKE 'IPP-%'"
        params = []
        if q:
            like = f"%{q}%"
            qid = _to_int(q, -1)
            where_sql += " AND (HealthPlanName LIKE ? OR HealthPlanCode LIKE ? OR HealthPlanID = ?)"
            params.extend([like, like, qid])

        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(1) FROM dbo.CPAHealthPlanMst WHERE {where_sql}", params)
        row = cursor.fetchone()
        total = int(row[0]) if row and row[0] is not None else 0

        start_row = (page - 1) * page_size + 1
        end_row = page * page_size
        cursor.execute(f"""
            WITH p AS (
                SELECT
                    HealthPlanID, HealthPlanCode, HealthPlanName, LaunchDate,
                    PackageCost, ValidityPeriod, NoOfVisitsAllowed, Deactive,
                    Discount, BasicCost, ServiceID,
                    CASE WHEN ISNULL(Deactive, 0) = 1 THEN 'Deactive' ELSE 'Active' END AS Status,
                    ROW_NUMBER() OVER (ORDER BY ISNULL(Deactive, 0), HealthPlanName, HealthPlanID) AS rn
                FROM dbo.CPAHealthPlanMst
                WHERE {where_sql}
            )
            SELECT
                HealthPlanID, HealthPlanCode, HealthPlanName, LaunchDate,
                PackageCost, ValidityPeriod, NoOfVisitsAllowed, Deactive,
                Discount, BasicCost, ServiceID, Status
            FROM p
            WHERE rn BETWEEN ? AND ?
            ORDER BY rn
        """, (*params, start_row, end_row))
        packs = _rows_as_dicts(cursor)
        return {
            "status": "success",
            "packages": packs,
            "page": page,
            "page_size": page_size,
            "total": total,
        }
    except Exception as e:
        return {"status": "error", "message": f"Failed to search IP packages: {e}"}
    finally:
        try:
            conn.close()
        except Exception:
            pass


def get_ip_package_next_code(unit: str, prefix: str = "IPP-"):
    conn = get_sql_connection(unit)
    if not conn:
        return {"status": "error", "message": "Unable to connect to database"}
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT HealthPlanCode, HealthPlanID
            FROM dbo.CPAHealthPlanMst
            WHERE ISNULL(HealthPlanCode, '') LIKE 'IPP-%'
        """)
        rows = cursor.fetchall()
        max_num = 0
        code_re = re.compile(r"^\s*" + re.escape(prefix) + r"(\d+)\s*$", re.IGNORECASE)
        for row in rows:
            code = str(row[0] or "").strip()
            m = code_re.match(code)
            if m:
                try:
                    max_num = max(max_num, int(m.group(1)))
                except Exception:
                    pass
        if max_num <= 0:
            for row in rows:
                hid = _to_int(row[1], 0) or 0
                max_num = max(max_num, hid)

        next_num = max_num + 1
        while True:
            candidate = f"{prefix}{next_num:03d}"
            cursor.execute(
                """
                SELECT TOP 1 1
                FROM dbo.CPAHealthPlanMst
                WHERE HealthPlanCode = ?
                """,
                (candidate,),
            )
            if not cursor.fetchone():
                return {"status": "success", "next_code": candidate}
            next_num += 1
    except Exception as e:
        return {"status": "error", "message": f"Failed to generate IP package code: {e}"}
    finally:
        try:
            conn.close()
        except Exception:
            pass


def get_ip_package_detail(unit: str, package_id: int):
    conn = get_sql_connection(unit)
    if not conn:
        return {"status": "error", "message": "Unable to connect to database"}
    try:
        pid = _to_int(package_id)
        if not pid:
            return {"status": "error", "message": "Package ID is required.", "http_status": 400}

        cursor = conn.cursor()
        cursor.execute("""
            SELECT TOP 1
                HealthPlanID, HealthPlanCode, HealthPlanName, LaunchDate,
                PackageCost, ValidityPeriod, NoOfVisitsAllowed, Discount, BasicCost,
                Deactive, ServiceID
            FROM dbo.CPAHealthPlanMst
            WHERE HealthPlanID = ?
        """, (pid,))
        row = cursor.fetchone()
        if not row:
            return {"status": "error", "message": "IP package not found.", "http_status": 404}

        package = {
            "HealthPlanID": row[0],
            "HealthPlanCode": row[1],
            "HealthPlanName": row[2],
            "LaunchDate": row[3].isoformat(sep=" ") if isinstance(row[3], datetime) else row[3],
            "PackageCost": float(row[4] or 0),
            "ValidityPeriod": int(row[5] or 0),
            "NoOfVisitsAllowed": int(row[6] or 0),
            "Discount": float(row[7] or 0),
            "BasicCost": float(row[8] or 0),
            "Deactive": int(row[9] or 0),
            "ServiceID": _to_int(row[10]),
        }

        cursor.execute("""
            SELECT
                d.ServiceID AS pServiceid,
                d.ServiceCode AS Service_Code,
                COALESCE(NULLIF(CONVERT(NVARCHAR(300), d.service_name), ''), s.Service_Name, '') AS Service_Name,
                d.ServiceCategoryID AS Category_Id,
                d.ServiceCategoryCode AS ServiceCategory_Code,
                ISNULL(c.ServiceCategory_Name, '') AS ServiceCategory_Name,
                ISNULL(s.CGHSCode, '') AS CGHSCode,
                d.Quantity AS pQuantity,
                d.Rate AS pRate,
                d.Amount
            FROM dbo.CPAHealthPlanDtl d
            LEFT JOIN dbo.Service_Mst s ON s.Service_ID = d.ServiceID
            LEFT JOIN dbo.Service_Category_Mst c ON c.ServiceCategory_ID = d.ServiceCategoryID
            WHERE d.HealthPlanID = ?
            ORDER BY d.HealthPlanDtlID
        """, (pid,))
        services = _rows_as_dicts(cursor)

        tariff_scheme_ids = []
        service_id = package.get("ServiceID")
        if service_id:
            cursor.execute("""
                SELECT DISTINCT TariffScheme_ID
                FROM dbo.ServiceTariffSheet
                WHERE ServiceId = ?
                  AND ISNULL(BillingClassID, 0) = 2
            """, (service_id,))
            tariff_scheme_ids = sorted(
                [_to_int(r[0]) for r in cursor.fetchall() if _to_int(r[0])]
            )
        package["tariff_scheme_ids"] = tariff_scheme_ids

        return {
            "status": "success",
            "package": package,
            "services": services,
        }
    except Exception as e:
        return {"status": "error", "message": f"Failed to load IP package detail: {e}"}
    finally:
        try:
            conn.close()
        except Exception:
            pass


def search_ip_package_services(unit: str, scheme_id: int, query: str, tag: int = 1):
    conn = get_sql_connection(unit)
    if not conn:
        return {"status": "error", "message": "Unable to connect to database"}
    try:
        sid = _to_int(scheme_id)
        if not sid:
            return {"status": "error", "message": "Tariff scheme is required."}

        q = (query or "").strip()
        if len(q) < 2:
            return {"status": "success", "services": []}

        tag_val = _to_int(tag, 1) or 1
        like = f"%{q}%"
        if tag_val == 2:
            filter_sql = "s.Service_Code LIKE ?"
        elif tag_val == 3:
            filter_sql = "ISNULL(s.CGHSCode, '') LIKE ?"
        else:
            filter_sql = "s.Service_Name LIKE ?"

        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT TOP 150
                s.Service_ID,
                s.Service_Code,
                s.Service_Name,
                s.Category_Id,
                ISNULL(c.ServiceCategory_Code, '') AS ServiceCategory_Code,
                ISNULL(c.ServiceCategory_Name, '') AS ServiceCategory_Name,
                ISNULL(s.CGHSCode, '') AS CGHSCode,
                CAST(ISNULL(ts.Rate, ISNULL(s.FinalRate, ISNULL(s.SaleRate, 0))) AS DECIMAL(18,2)) AS Rate
            FROM dbo.Service_Mst s
            OUTER APPLY (
                SELECT TOP 1 Rate
                FROM dbo.ServiceTariffSheet
                WHERE ServiceId = s.Service_ID
                  AND TariffScheme_ID = ?
                  AND ISNULL(BillingClassID, 0) = 2
                ORDER BY Tariff_ID DESC
            ) ts
            LEFT JOIN dbo.Service_Category_Mst c
                ON c.ServiceCategory_ID = s.Category_Id
            WHERE
                ISNULL(s.Deactive, 0) = 0
                AND ISNULL(s.Service_Code, '') NOT LIKE 'IPP-%'
                AND {filter_sql}
            ORDER BY s.Service_Name
        """, (sid, like))
        services = _rows_as_dicts(cursor)
        return {"status": "success", "services": services}
    except Exception as e:
        return {"status": "error", "message": f"Failed to search services: {e}"}
    finally:
        try:
            conn.close()
        except Exception:
            pass


def save_ip_package(unit: str, payload: dict, username: str = ""):
    conn = get_sql_connection(unit)
    if not conn:
        return {"status": "error", "message": "Unable to connect to database"}

    data = payload or {}
    mode = str(data.get("mode") or "").strip().lower()
    package_id = _to_int(data.get("package_id"))
    is_update = bool(package_id) and mode == "update"

    package_code = str(data.get("package_code") or "").strip()
    package_name = str(data.get("package_name") or "").strip()
    launch_date = _to_datetime(data.get("launch_date"))
    package_cost = _to_decimal(data.get("package_cost"), Decimal("0"))
    validity_period = _to_int(data.get("validity_period"), 0) or 0
    visits_allowed = _to_int(data.get("visits_allowed"), 0) or 0
    discount_amount = _to_decimal(data.get("discount_amount"), Decimal("0"))
    basic_cost = _to_decimal(data.get("basic_cost"), Decimal("0"))
    updated_by = _to_int(data.get("updated_by"), 0) or 0

    deactive = 1 if _to_bool(data.get("deactive"), False) else None
    if deactive is None:
        # Backward compatibility with payload using "activation" as deactive flag.
        deactive = 1 if _to_int(data.get("activation"), 0) == 1 else 0
    activation = 0 if deactive == 1 else 1

    services = _parse_ip_package_services(data.get("services"))

    raw_scheme_ids = data.get("tariff_scheme_ids")
    if raw_scheme_ids is None:
        raw_scheme_ids = data.get("tariff_scheme_id")
    scheme_ids = _to_int_list(raw_scheme_ids)
    if not scheme_ids:
        fallback_scheme = _to_int(data.get("scheme_id"))
        if fallback_scheme:
            scheme_ids = [fallback_scheme]

    if not package_code or not package_name or not launch_date:
        return {"status": "error", "message": "Package code, name, and launch date are required."}
    if not services:
        return {"status": "error", "message": "Add at least one service before saving."}
    if mode == "update" and not package_id:
        return {"status": "error", "message": "Select an IP package to update."}
    if not scheme_ids:
        return {"status": "error", "message": "Select at least one tariff scheme."}

    try:
        conn.autocommit = False
    except Exception:
        pass

    stage = "init"
    try:
        cursor = conn.cursor()

        # Validate duplicate code
        stage = "validate_duplicate_code"
        if is_update:
            cursor.execute("""
                SELECT TOP 1 HealthPlanID
                FROM dbo.CPAHealthPlanMst
                WHERE HealthPlanCode = ?
                  AND HealthPlanID <> ?
            """, (package_code, package_id))
            if cursor.fetchone():
                conn.rollback()
                return {
                    "status": "error",
                    "message": "Package code already exists. Please change the code.",
                    "http_status": 409,
                }
        else:
            cursor.execute("""
                SELECT TOP 1 HealthPlanID
                FROM dbo.CPAHealthPlanMst
                WHERE HealthPlanCode = ?
            """, (package_code,))
            if cursor.fetchone():
                conn.rollback()
                return {
                    "status": "error",
                    "message": "Package code already exists. Please refresh for a new code.",
                    "http_status": 409,
                }

        stage = "upsert_package_master"
        if is_update:
            cursor.execute("""
                UPDATE dbo.CPAHealthPlanMst
                SET
                    HealthPlanCode = ?,
                    HealthPlanName = ?,
                    LaunchDate = ?,
                    PackageCost = ?,
                    ValidityPeriod = ?,
                    NoOfVisitsAllowed = ?,
                    Deactive = ?,
                    UpdatedBy = ?,
                    updatedOn = GETDATE(),
                    Activation = ?,
                    Discount = ?,
                    BasicCost = ?,
                    TypeOfVisit = 1
                WHERE HealthPlanID = ?
            """, (
                package_code, package_name, launch_date, float(package_cost),
                int(validity_period), int(visits_allowed), int(deactive), int(updated_by),
                int(activation), float(discount_amount), float(basic_cost), int(package_id),
            ))
        else:
            cursor.execute("""
                SELECT ISNULL(MAX(HealthPlanID), 0) + 1
                FROM dbo.CPAHealthPlanMst WITH (UPDLOCK, HOLDLOCK)
            """)
            row = cursor.fetchone()
            package_id = int(row[0]) if row and row[0] else None
            if not package_id:
                raise RuntimeError("Unable to generate HealthPlanID")

            cursor.execute("""
                INSERT INTO dbo.CPAHealthPlanMst (
                    HealthPlanID, HealthPlanCode, HealthPlanName, LaunchDate,
                    PackageCost, ValidityPeriod, NoOfVisitsAllowed,
                    MinimumAge, MaximumAge, Deactive, Comments,
                    UpdatedBy, updatedOn, Activation, Discount, BasicCost,
                    InsertedByUserID, InsertedON, ServiceID, TypeOfVisit,
                    AdvanceReq, MinAdvance, SpecializationID
                )
                VALUES (
                    ?, ?, ?, ?,
                    ?, ?, ?,
                    0, 99, ?, NULL,
                    ?, GETDATE(), ?, ?, ?,
                    ?, GETDATE(), 0, 1,
                    0, 0, 0
                )
            """, (
                int(package_id), package_code, package_name, launch_date,
                float(package_cost), int(validity_period), int(visits_allowed),
                int(deactive), int(updated_by), int(activation),
                float(discount_amount), float(basic_cost),
                int(updated_by),
            ))

        # Fill missing service metadata from Service_Mst
        stage = "hydrate_service_metadata"
        missing_ids = [
            s["service_id"] for s in services
            if s["service_id"] and (not s["service_code"] or not s["category_id"] or not s["service_name"])
        ]
        if missing_ids:
            placeholders = ",".join("?" for _ in missing_ids)
            cursor.execute(
                f"""
                SELECT Service_ID, Service_Code, Category_Id, Service_Name
                FROM dbo.Service_Mst
                WHERE Service_ID IN ({placeholders})
                """,
                tuple(missing_ids),
            )
            svc_map = {
                int(r[0]): {
                    "code": str(r[1] or "").strip(),
                    "category_id": _to_int(r[2]),
                    "name": str(r[3] or "").strip(),
                }
                for r in cursor.fetchall()
            }
            for svc in services:
                info = svc_map.get(int(svc["service_id"]))
                if not info:
                    continue
                if not svc.get("service_code"):
                    svc["service_code"] = info["code"]
                if not svc.get("category_id"):
                    svc["category_id"] = info["category_id"]
                if not svc.get("service_name"):
                    svc["service_name"] = info["name"]

        missing_category = [s["service_id"] for s in services if not s.get("category_id")]
        if missing_category:
            raise RuntimeError(f"Category is missing for services: {missing_category}")

        missing_cat_codes = sorted({int(s["category_id"]) for s in services if s.get("category_id") and not s.get("category_code")})
        cat_code_map = {}
        if missing_cat_codes:
            placeholders = ",".join("?" for _ in missing_cat_codes)
            cursor.execute(
                f"""
                SELECT ServiceCategory_ID, ServiceCategory_Code
                FROM dbo.Service_Category_Mst
                WHERE ServiceCategory_ID IN ({placeholders})
                """,
                tuple(missing_cat_codes),
            )
            for row in cursor.fetchall():
                cat_code_map[int(row[0])] = str(row[1] or "").strip()

        # Rebuild package details
        stage = "rebuild_package_details"
        cursor.execute("DELETE FROM dbo.CPAHealthPlanDtl WHERE HealthPlanID = ?", (int(package_id),))
        stage = "rebuild_package_details_pk_mode"
        cursor.execute("""
            SELECT COLUMNPROPERTY(OBJECT_ID('dbo.CPAHealthPlanDtl'), 'HealthPlanDtlID', 'IsIdentity')
        """)
        identity_row = cursor.fetchone()
        dtl_id_is_identity = bool(identity_row and identity_row[0] == 1)
        next_dtl_id = None
        if not dtl_id_is_identity:
            cursor.execute("""
                SELECT ISNULL(MAX(HealthPlanDtlID), 0) + 1
                FROM dbo.CPAHealthPlanDtl WITH (UPDLOCK, HOLDLOCK)
            """)
            next_row = cursor.fetchone()
            next_dtl_id = int(next_row[0]) if next_row and next_row[0] else 1

        for svc in services:
            svc_cat_id = int(svc["category_id"])
            svc_cat_code = str(svc.get("category_code") or cat_code_map.get(svc_cat_id, "")).strip()
            if dtl_id_is_identity:
                cursor.execute("""
                    INSERT INTO dbo.CPAHealthPlanDtl (
                        HealthPlanID, ServiceID, ServiceCode, ServiceCategoryID, ServiceCategoryCode,
                        Quantity, Rate, Amount, UpdatedBy, UpdatedON, InsertedByUserID, InsertedON
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, GETDATE(), ?, GETDATE())
                """, (
                    int(package_id),
                    int(svc["service_id"]),
                    str(svc.get("service_code") or "").strip(),
                    svc_cat_id,
                    svc_cat_code,
                    int(svc.get("quantity") or 1),
                    float(_to_decimal(svc.get("rate"), Decimal("0"))),
                    float(_to_decimal(svc.get("amount"), Decimal("0"))),
                    int(updated_by),
                    int(updated_by),
                ))
            else:
                cursor.execute("""
                    INSERT INTO dbo.CPAHealthPlanDtl (
                        HealthPlanDtlID, HealthPlanID, ServiceID, ServiceCode, ServiceCategoryID, ServiceCategoryCode,
                        Quantity, Rate, Amount, UpdatedBy, UpdatedON, InsertedByUserID, InsertedON
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, GETDATE(), ?, GETDATE())
                """, (
                    int(next_dtl_id),
                    int(package_id),
                    int(svc["service_id"]),
                    str(svc.get("service_code") or "").strip(),
                    svc_cat_id,
                    svc_cat_code,
                    int(svc.get("quantity") or 1),
                    float(_to_decimal(svc.get("rate"), Decimal("0"))),
                    float(_to_decimal(svc.get("amount"), Decimal("0"))),
                    int(updated_by),
                    int(updated_by),
                ))
                next_dtl_id += 1

        # Upsert package row in Service_Mst
        stage = "upsert_package_service_master"
        package_service_id = None
        cursor.execute("SELECT ServiceID FROM dbo.CPAHealthPlanMst WHERE HealthPlanID = ?", (int(package_id),))
        row = cursor.fetchone()
        if row and row[0]:
            package_service_id = _to_int(row[0])

        if not package_service_id:
            cursor.execute("SELECT TOP 1 Service_ID FROM dbo.Service_Mst WHERE Service_Code = ? ORDER BY Service_ID DESC", (package_code,))
            row = cursor.fetchone()
            if row and row[0]:
                package_service_id = _to_int(row[0])

        dept_id = None
        subdept_id = None
        cat_id = None
        cursor.execute("""
            SELECT TOP 1 DepartmentId, SubDepartmentId, Category_Id
            FROM dbo.Service_Mst
            WHERE Service_Code LIKE 'IPP-%'
              AND DepartmentId IS NOT NULL
              AND Category_Id IS NOT NULL
            ORDER BY Service_ID DESC
        """)
        row = cursor.fetchone()
        if row:
            dept_id = _to_int(row[0])
            subdept_id = _to_int(row[1], 0)
            cat_id = _to_int(row[2])
        if not dept_id or not cat_id:
            first_service_id = _to_int(services[0].get("service_id"))
            if first_service_id:
                cursor.execute("""
                    SELECT TOP 1 DepartmentId, SubDepartmentId, Category_Id
                    FROM dbo.Service_Mst
                    WHERE Service_ID = ?
                """, (first_service_id,))
                row = cursor.fetchone()
                if row:
                    dept_id = dept_id or _to_int(row[0], 0)
                    subdept_id = subdept_id if subdept_id is not None else _to_int(row[1], 0)
                    cat_id = cat_id or _to_int(row[2], 0)

        dept_id = dept_id or 0
        subdept_id = subdept_id or 0
        cat_id = cat_id or 0
        sale_rate = float(max(Decimal("0"), package_cost))
        final_rate = float(max(Decimal("0"), package_cost))
        discount_val = float(max(Decimal("0"), discount_amount))
        remarks = f"IP Package #{package_id}"

        if package_service_id:
            cursor.execute("""
                UPDATE dbo.Service_Mst
                SET
                    Service_Code = ?,
                    Service_Name = ?,
                    Deactive = ?,
                    DepartmentId = ?,
                    SubDepartmentId = ?,
                    Category_Id = ?,
                    Updated_By = ?,
                    Updated_On = GETDATE(),
                    Autorender = 1,
                    PatInvestigationVisible = 1,
                    UpdatedBy = ?,
                    UpdatedON = GETDATE(),
                    BasicRate = ?,
                    ProfitPer = 0,
                    SaleRate = ?,
                    Discount = ?,
                    FinalRate = ?,
                    PackageID = ?,
                    DefaultItem = 0,
                    EmergencyCharges = 0,
                    Surcharge = 0,
                    DoctorCut = 0,
                    DocRequired = 1,
                    ClinicalReportingFlag = 0,
                    SurchargeAmt = 0,
                    IsBedSideProcedure = 0,
                    Remarks = ?,
                    CGHSCode = '',
                    IsOutSourceService = 0,
                    IsExclusiveService = 0,
                    IsRenewal = 0,
                    ServiceAliasName = ?,
                    BlockedByTPA = 0,
                    IsRoutineService = 0
                WHERE Service_ID = ?
            """, (
                package_code, package_name, int(deactive), int(dept_id), int(subdept_id), int(cat_id),
                int(updated_by), int(updated_by),
                sale_rate, sale_rate, discount_val, final_rate, int(package_id),
                remarks, package_name[:50], int(package_service_id),
            ))
        else:
            cursor.execute("""
                SELECT ISNULL(MAX(Service_ID), 0) + 1
                FROM dbo.Service_Mst WITH (UPDLOCK, HOLDLOCK)
            """)
            row = cursor.fetchone()
            package_service_id = int(row[0]) if row and row[0] else None
            if not package_service_id:
                raise RuntimeError("Unable to generate Service_ID for IP package")

            cursor.execute("""
                INSERT INTO dbo.Service_Mst (
                    Service_ID, Service_Code, Service_Name, Deactive, DepartmentId, SubDepartmentId, Category_Id,
                    Updated_By, Updated_On, Autorender, ServiceType, PatInvestigationVisible,
                    UpdatedBy, UpdatedON, BasicRate, ProfitPer, SaleRate, Discount, FinalRate, PackageID,
                    DefaultItem, EmergencyCharges, Surcharge, DoctorCut, DocRequired,
                    ClinicalReportingFlag, SurchargeAmt, IsBedSideProcedure, Remarks, CGHSCode,
                    IsOutSourceService, IsExclusiveService, IsRenewal, ServiceAliasName, BlockedByTPA, IsRoutineService,
                    InsertedByUserID, InsertedON
                )
                VALUES (
                    ?, ?, ?, ?, ?, ?, ?,
                    ?, GETDATE(), 1, NULL, 1,
                    ?, GETDATE(), ?, 0, ?, ?, ?, ?,
                    0, 0, 0, 0, 1,
                    0, 0, 0, ?, '',
                    0, 0, 0, ?, 0, 0,
                    ?, GETDATE()
                )
            """, (
                int(package_service_id), package_code, package_name, int(deactive),
                int(dept_id), int(subdept_id), int(cat_id),
                int(updated_by), int(updated_by),
                sale_rate, sale_rate, discount_val, final_rate, int(package_id),
                remarks, package_name[:50], int(updated_by),
            ))

        # Sync package master back to service link
        stage = "sync_package_master_service_link"
        cursor.execute("""
            UPDATE dbo.CPAHealthPlanMst
            SET ServiceID = ?, TypeOfVisit = 1,
                Activation = ?, Deactive = ?, UpdatedBy = ?, updatedOn = GETDATE()
            WHERE HealthPlanID = ?
        """, (
            int(package_service_id), int(activation), int(deactive), int(updated_by), int(package_id),
        ))

        # Upsert tariff rows for BillingClassID=2 only
        stage = "upsert_service_tariff_sheet"
        cursor.execute("""
            SELECT Tariff_ID, TariffScheme_ID
            FROM dbo.ServiceTariffSheet WITH (UPDLOCK, HOLDLOCK)
            WHERE ServiceId = ?
              AND ISNULL(BillingClassID, 0) = 2
        """, (int(package_service_id),))
        existing = {}
        for row in cursor.fetchall():
            tid = _to_int(row[0])
            sid = _to_int(row[1])
            if tid and sid:
                existing[sid] = tid

        tariff_ids = []
        next_tariff_id = None
        for scheme_id in scheme_ids:
            existing_id = existing.get(scheme_id)
            if existing_id:
                cursor.execute("""
                    UPDATE dbo.ServiceTariffSheet
                    SET Rate = ?, DiscountAmt = 0, UpdatedBy = ?, UpdatedOn = GETDATE()
                    WHERE Tariff_ID = ?
                """, (
                    float(package_cost), int(updated_by), int(existing_id),
                ))
                tariff_ids.append(int(existing_id))
            else:
                if next_tariff_id is None:
                    cursor.execute("""
                        SELECT ISNULL(MAX(Tariff_ID), 0) + 1
                        FROM dbo.ServiceTariffSheet WITH (UPDLOCK, HOLDLOCK)
                    """)
                    row = cursor.fetchone()
                    next_tariff_id = int(row[0]) if row and row[0] else None
                    if not next_tariff_id:
                        raise RuntimeError("Unable to generate Tariff_ID")
                cursor.execute("""
                    INSERT INTO dbo.ServiceTariffSheet (
                        Tariff_ID, ServiceId, TariffScheme_ID, RoomID, BillingClassID,
                        Rate, DiscountAmt, UpdatedBy, UpdatedOn,
                        InsertedByUserID, InsertedON
                    )
                    VALUES (?, ?, ?, 0, 2, ?, 0, ?, GETDATE(), ?, GETDATE())
                """, (
                    int(next_tariff_id), int(package_service_id), int(scheme_id),
                    float(package_cost), int(updated_by), int(updated_by),
                ))
                tariff_ids.append(int(next_tariff_id))
                next_tariff_id += 1

        if scheme_ids:
            placeholders = ",".join("?" for _ in scheme_ids)
            cursor.execute(
                f"""
                DELETE FROM dbo.ServiceTariffSheet
                WHERE ServiceId = ?
                  AND ISNULL(BillingClassID, 0) = 2
                  AND TariffScheme_ID NOT IN ({placeholders})
                """,
                (int(package_service_id), *tuple(scheme_ids)),
            )

        stage = "commit"
        conn.commit()
        action = "updated" if is_update else ("copied" if mode == "copy" else "added")
        return {
            "status": "success",
            "package_id": int(package_id),
            "service_id": int(package_service_id),
            "tariff_ids": tariff_ids,
            "action": action,
            "message": f"IP package {action} successfully.",
        }
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        return {"status": "error", "message": f"Failed to save IP package at [{stage}]: {e}"}
    finally:
        try:
            conn.close()
        except Exception:
            pass
