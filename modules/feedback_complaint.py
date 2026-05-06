from __future__ import annotations

import html
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from secrets import token_hex
from threading import Lock

import pandas as pd
from flask import Blueprint, jsonify, render_template, request, session

import config
from modules.db_connection import get_sql_connection


HID_UNIT = "ACI"
PUBLIC_UNITS = ("AHL", "ACI", "BALLIA")
STATUS_VALUES = {
    "OPEN",
    "IN_PROGRESS",
    "DEPT_REPLIED",
    "ESCALATED_L1",
    "ESCALATED_L2",
    "PENDING_FINAL_CLOSURE",
    "CLOSED",
    "REOPENED",
}
FINAL_CLOSURE_ROLES = {"IT", "Management"}
ASSIGNMENT_LEVELS = {
    "DEPARTMENT_USER",
    "CENTER_HEAD",
    "VP_OPERATIONS",
    "DIRECTOR",
    "IT_ADMIN",
}
ASSIGNMENT_LEVEL_ORDER = [
    "DEPARTMENT_USER",
    "CENTER_HEAD",
    "VP_OPERATIONS",
    "DIRECTOR",
    "IT_ADMIN",
]
DEPARTMENTS = [
    "Billing",
    "Patient Counselling",
    "Food & Dietary Services",
    "Nursing",
    "Housekeeping",
    "Doctor / Clinical Care",
    "Patient Experience & Operations",
    "Security",
    "Quality",
    "Pharmacy",
    "HMIS / IT Support",
    "Reception",
    "PHC",
    "Radiology",
    "Pathology",
    "Information Desk / Call Center",
]

_SCHEMA_LOCK = Lock()
_SCHEMA_READY = False
_RATE_LOCK = Lock()
_PUBLIC_RATE = {}


def _clean_text(value, max_len=None):
    text = str(value or "").strip()
    text = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", "", text)
    if max_len and len(text) > max_len:
        return text[:max_len]
    return text


def _digits(value, max_len=20):
    return re.sub(r"\D+", "", str(value or ""))[:max_len]


def _safe_int(value, default=0):
    try:
        if value in (None, ""):
            return default
        return int(value)
    except Exception:
        return default


def _is_valid_email(value):
    email_value = str(value or "").strip()
    if not email_value:
        return True
    return bool(re.match(r"^[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}$", email_value, flags=re.IGNORECASE))


def _role_base(role_name: str | None) -> str:
    return str(role_name or "").split(":", 1)[0].strip()


def _json_value(value):
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    return value


def _rows_from_cursor(cursor):
    rows = cursor.fetchall()
    cols = [str(c[0]).strip() for c in (cursor.description or [])]
    return [{cols[i]: _json_value(row[i]) for i in range(len(cols))} for row in rows]


def _df_rows(df):
    if df is None or df.empty:
        return []
    out = df.copy()
    for col in out.columns:
        if pd.api.types.is_datetime64_any_dtype(out[col]):
            out[col] = pd.to_datetime(out[col], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")
    out = out.astype(object).where(pd.notna(out), None)
    return out.to_dict(orient="records")


def _hid_conn():
    return get_sql_connection(HID_UNIT)


def ensure_feedback_schema(force=False):
    global _SCHEMA_READY
    if _SCHEMA_READY and not force:
        return True
    with _SCHEMA_LOCK:
        if _SCHEMA_READY and not force:
            return True
        conn = _hid_conn()
        if not conn:
            return False
        try:
            cur = conn.cursor()
            cur.execute(
                """
                IF OBJECT_ID('dbo.FeedbackComplaint_Department_Mst', 'U') IS NULL
                BEGIN
                    CREATE TABLE dbo.FeedbackComplaint_Department_Mst (
                        DepartmentID INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
                        DepartmentName NVARCHAR(100) NOT NULL,
                        IsActive BIT NOT NULL CONSTRAINT DF_FeedbackComplaint_Department_IsActive DEFAULT (1)
                    );
                    CREATE UNIQUE INDEX UX_FeedbackComplaint_Department_Name
                    ON dbo.FeedbackComplaint_Department_Mst (DepartmentName);
                END;

                IF OBJECT_ID('dbo.FeedbackComplaint_Mst', 'U') IS NULL
                BEGIN
                    CREATE TABLE dbo.FeedbackComplaint_Mst (
                        ComplaintID BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
                        ComplaintNo VARCHAR(30) NOT NULL UNIQUE,
                        SourceUnit VARCHAR(20) NOT NULL,
                        PatientID BIGINT NULL,
                        RegistrationNo VARCHAR(50) NULL,
                        PatientName NVARCHAR(200) NULL,
                        MobileNo VARCHAR(20) NULL,
                        VisitID BIGINT NULL,
                        VisitNo VARCHAR(50) NULL,
                        VisitType VARCHAR(20) NULL,
                        VisitDate DATETIME NULL,
                        DoctorName NVARCHAR(200) NULL,
                        ClinicalDepartment NVARCHAR(200) NULL,
                        ComplaintDepartmentID INT NOT NULL,
                        ComplaintType VARCHAR(20) NOT NULL,
                        Details NVARCHAR(MAX) NOT NULL,
                        Status VARCHAR(30) NOT NULL CONSTRAINT DF_FeedbackComplaint_Status DEFAULT ('OPEN'),
                        CurrentLevel VARCHAR(30) NOT NULL CONSTRAINT DF_FeedbackComplaint_Level DEFAULT ('DEPARTMENT'),
                        SubmittedAt DATETIME NOT NULL CONSTRAINT DF_FeedbackComplaint_SubmittedAt DEFAULT (GETDATE()),
                        DepartmentDueAt DATETIME NOT NULL,
                        L1DueAt DATETIME NULL,
                        ClosedAt DATETIME NULL,
                        FinalClosedBy INT NULL,
                        CreatedIP VARCHAR(50) NULL,
                        IsActive BIT NOT NULL CONSTRAINT DF_FeedbackComplaint_IsActive DEFAULT (1)
                    );
                END;

                IF OBJECT_ID('dbo.FeedbackComplaint_Assignment_Mst', 'U') IS NULL
                BEGIN
                    CREATE TABLE dbo.FeedbackComplaint_Assignment_Mst (
                        AssignmentID INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
                        DepartmentID INT NOT NULL,
                        Unit VARCHAR(20) NOT NULL,
                        UserID INT NOT NULL,
                        AssignmentUserName NVARCHAR(150) NULL,
                        RoleLevel VARCHAR(30) NOT NULL,
                        NotificationEmail NVARCHAR(200) NULL,
                        IsActive BIT NOT NULL CONSTRAINT DF_FeedbackComplaint_Assignment_IsActive DEFAULT (1),
                        CreatedAt DATETIME NOT NULL CONSTRAINT DF_FeedbackComplaint_Assignment_CreatedAt DEFAULT (GETDATE())
                    );
                END;

                IF OBJECT_ID('dbo.FeedbackComplaint_Remarks', 'U') IS NULL
                BEGIN
                    CREATE TABLE dbo.FeedbackComplaint_Remarks (
                        RemarkID BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
                        ComplaintID BIGINT NOT NULL,
                        UserID INT NULL,
                        UserName NVARCHAR(150) NULL,
                        RemarkText NVARCHAR(MAX) NULL,
                        OldStatus VARCHAR(30) NULL,
                        NewStatus VARCHAR(30) NULL,
                        ActionType VARCHAR(50) NOT NULL,
                        CreatedAt DATETIME NOT NULL CONSTRAINT DF_FeedbackComplaint_Remarks_CreatedAt DEFAULT (GETDATE())
                    );
                END;

                IF OBJECT_ID('dbo.FeedbackComplaint_AuditLog', 'U') IS NULL
                BEGIN
                    CREATE TABLE dbo.FeedbackComplaint_AuditLog (
                        AuditID BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
                        ComplaintID BIGINT NULL,
                        Action NVARCHAR(200) NOT NULL,
                        PerformedByUserID INT NULL,
                        PerformedByName NVARCHAR(150) NULL,
                        Details NVARCHAR(MAX) NULL,
                        CreatedAt DATETIME NOT NULL CONSTRAINT DF_FeedbackComplaint_Audit_CreatedAt DEFAULT (GETDATE()),
                        IPAddress VARCHAR(50) NULL
                    );
                END;

                IF COL_LENGTH('dbo.FeedbackComplaint_Assignment_Mst', 'CreatedBy') IS NULL
                    ALTER TABLE dbo.FeedbackComplaint_Assignment_Mst ADD CreatedBy NVARCHAR(150) NULL;

                IF COL_LENGTH('dbo.FeedbackComplaint_Assignment_Mst', 'NotificationEmail') IS NULL
                    ALTER TABLE dbo.FeedbackComplaint_Assignment_Mst ADD NotificationEmail NVARCHAR(200) NULL;

                IF COL_LENGTH('dbo.FeedbackComplaint_Assignment_Mst', 'AssignmentUserName') IS NULL
                    ALTER TABLE dbo.FeedbackComplaint_Assignment_Mst ADD AssignmentUserName NVARCHAR(150) NULL;

                IF NOT EXISTS (
                    SELECT 1 FROM sys.indexes
                    WHERE object_id = OBJECT_ID('dbo.FeedbackComplaint_Mst')
                      AND name = 'IX_FeedbackComplaint_Dashboard'
                )
                BEGIN
                    CREATE INDEX IX_FeedbackComplaint_Dashboard
                    ON dbo.FeedbackComplaint_Mst (IsActive, SubmittedAt DESC, SourceUnit, ComplaintDepartmentID, Status, CurrentLevel);
                END;

                IF NOT EXISTS (
                    SELECT 1 FROM sys.indexes
                    WHERE object_id = OBJECT_ID('dbo.FeedbackComplaint_Remarks')
                      AND name = 'IX_FeedbackComplaint_Remarks_Complaint'
                )
                BEGIN
                    CREATE INDEX IX_FeedbackComplaint_Remarks_Complaint
                    ON dbo.FeedbackComplaint_Remarks (ComplaintID, CreatedAt DESC);
                END;

                IF NOT EXISTS (
                    SELECT 1 FROM sys.indexes
                    WHERE object_id = OBJECT_ID('dbo.FeedbackComplaint_Assignment_Mst')
                      AND name = 'IX_FeedbackComplaint_Assignment_Scope'
                )
                BEGIN
                    CREATE INDEX IX_FeedbackComplaint_Assignment_Scope
                    ON dbo.FeedbackComplaint_Assignment_Mst (IsActive, Unit, DepartmentID, UserID, RoleLevel);
                END;
                """
            )
            for dept in DEPARTMENTS:
                cur.execute(
                    """
                    IF NOT EXISTS (
                        SELECT 1 FROM dbo.FeedbackComplaint_Department_Mst
                        WHERE UPPER(LTRIM(RTRIM(DepartmentName))) = UPPER(LTRIM(RTRIM(?)))
                    )
                    BEGIN
                        INSERT INTO dbo.FeedbackComplaint_Department_Mst (DepartmentName, IsActive)
                        VALUES (?, 1);
                    END;
                    """,
                    (dept, dept),
                )
            try:
                conn.commit()
            except Exception:
                pass
            _SCHEMA_READY = True
            return True
        except Exception as exc:
            print(f"Feedback schema bootstrap failed: {exc}")
            return False
        finally:
            try:
                conn.close()
            except Exception:
                pass


def _rate_allowed(ip, bucket="public", limit=45, window=300):
    key = (bucket, ip or "")
    now = time.time()
    with _RATE_LOCK:
        hits = [ts for ts in _PUBLIC_RATE.get(key, []) if now - ts <= window]
        if len(hits) >= limit:
            _PUBLIC_RATE[key] = hits
            return False
        hits.append(now)
        _PUBLIC_RATE[key] = hits
        return True


def _public_units(unit=None):
    unit_key = _clean_text(unit, 20).upper()
    configured = {str(u).strip().upper() for u in getattr(config, "DB_CONFIGS", {})}
    if unit_key:
        return [unit_key] if unit_key in PUBLIC_UNITS and unit_key in configured else []
    return [u for u in PUBLIC_UNITS if u in configured]


def _visit_type_expr():
    return """
        CASE
            WHEN (
                    ISNULL(v.VisitTypeID, 0) = 1
                    OR UPPER(LTRIM(RTRIM(ISNULL(v.TypeOfVisit, N'')))) IN (N'IPD', N'IN PATIENT VISITS', N'INPATIENT VISITS')
                 )
                 AND (
                    UPPER(LTRIM(RTRIM(ISNULL(v.TypeOfVisit, N'')))) LIKE N'%DAY%'
                    OR (
                        v.DischargeDate IS NOT NULL
                        AND DATEDIFF(MINUTE, CAST(v.VisitDate AS DATETIME), CAST(v.DischargeDate AS DATETIME)) BETWEEN 0 AND 480
                    )
                 ) THEN N'Daycare'
            WHEN ISNULL(v.VisitTypeID, 0) = 1 OR UPPER(LTRIM(RTRIM(ISNULL(v.TypeOfVisit, N'')))) IN (N'IPD', N'IN PATIENT VISITS', N'INPATIENT VISITS') THEN N'IPD'
            WHEN ISNULL(v.VisitTypeID, 0) = 3 OR UPPER(LTRIM(RTRIM(ISNULL(v.TypeOfVisit, N'')))) = N'DPV' THEN N'DPV'
            WHEN ISNULL(v.VisitTypeID, 0) = 6 OR UPPER(LTRIM(RTRIM(ISNULL(v.TypeOfVisit, N'')))) = N'HCV' THEN N'HCV'
            WHEN ISNULL(v.VisitTypeID, 0) = 2 OR UPPER(LTRIM(RTRIM(ISNULL(v.TypeOfVisit, N'')))) = N'OPD' THEN N'OPD'
            WHEN UPPER(LTRIM(RTRIM(ISNULL(v.TypeOfVisit, N'')))) LIKE N'%DAY%' THEN N'Daycare'
            ELSE NULLIF(LTRIM(RTRIM(ISNULL(v.TypeOfVisit, N''))), N'')
        END
    """


def _current_ipd_condition():
    return """
        (
            ISNULL(v.VisitTypeID, 0) = 1
            OR UPPER(LTRIM(RTRIM(ISNULL(v.TypeOfVisit, N'')))) IN (N'IPD', N'IN PATIENT VISITS', N'INPATIENT VISITS')
        )
        AND v.DischargeDate IS NULL
        AND ISNULL(v.DischargeType, 0) <> 2
    """


def _valid_qr_visit_condition():
    current_ipd = _current_ipd_condition()
    return f"""
        (
            ({current_ipd})
            OR (
                CAST(v.VisitDate AS DATETIME) >= DATEADD(MONTH, -3, CAST(GETDATE() AS DATE))
                AND CAST(v.VisitDate AS DATETIME) < DATEADD(DAY, 1, CAST(GETDATE() AS DATE))
                AND (
                    ISNULL(v.VisitTypeID, 0) IN (2, 3, 6)
                    OR UPPER(LTRIM(RTRIM(ISNULL(v.TypeOfVisit, N'')))) IN (N'OPD', N'DPV', N'HCV')
                    OR (
                        (
                            ISNULL(v.VisitTypeID, 0) = 1
                            OR UPPER(LTRIM(RTRIM(ISNULL(v.TypeOfVisit, N'')))) IN (N'IPD', N'IN PATIENT VISITS', N'INPATIENT VISITS')
                        )
                        AND (
                            UPPER(LTRIM(RTRIM(ISNULL(v.TypeOfVisit, N'')))) LIKE N'%DAY%'
                            OR (
                                v.DischargeDate IS NOT NULL
                                AND DATEDIFF(MINUTE, CAST(v.VisitDate AS DATETIME), CAST(v.DischargeDate AS DATETIME)) BETWEEN 0 AND 480
                            )
                        )
                    )
                )
            )
        )
    """


def _clinical_department_expr():
    return """
        COALESCE(
            NULLIF(LTRIM(RTRIM(ISNULL(dbo.fn_dept(v.DepartmentID), N''))), N''),
            NULLIF(LTRIM(RTRIM(ISNULL(dbo.Fn_subDept(v.UnitID), N''))), N''),
            N''
        )
    """


def _visit_type_groups(type_values):
    outpatient_order = ["OPD", "DPV", "HCV"]
    inpatient_order = ["IPD", "Daycare"]
    label_map = {
        "OPD": "OPD",
        "DPV": "DPV",
        "HCV": "HCV",
        "IPD": "IPD",
        "DAYCARE": "Daycare",
        "DAY CARE": "Daycare",
        "DC": "Daycare",
    }
    seen = set()
    normal = []
    for value in type_values or []:
        raw = str(value or "").strip()
        if not raw:
            continue
        key = raw.upper().replace("-", " ")
        if "DAY" in key:
            label = "Daycare"
        else:
            label = label_map.get(key, raw)
        normalized_key = label.upper()
        if normalized_key in seen:
            continue
        seen.add(normalized_key)
        normal.append(label)
    outpatient = [label for label in outpatient_order if label in normal]
    inpatient = [label for label in inpatient_order if label in normal]
    other = [label for label in normal if label not in outpatient and label not in inpatient]
    visit_types = outpatient + inpatient + other
    parts = []
    if outpatient:
        parts.append("Outpatient: " + ", ".join(outpatient))
    if inpatient:
        parts.append("Inpatient: " + ", ".join(inpatient))
    if other:
        parts.append("Other: " + ", ".join(other))
    return {
        "visit_types": visit_types,
        "visit_type_groups": {
            "outpatient": outpatient,
            "inpatient": inpatient,
            "other": other,
        },
        "visit_type_label": " | ".join(parts),
    }


def _patient_visit_type_summary_unit(conn, patient_ids):
    ids = []
    for value in patient_ids or []:
        parsed = _safe_int(value, 0)
        if parsed > 0 and parsed not in ids:
            ids.append(parsed)
    if not ids:
        return {}
    placeholders = ",".join("?" for _ in ids)
    try:
        current_ipd = _current_ipd_condition()
        valid_qr_visit = _valid_qr_visit_condition()
        sql = f"""
            SELECT
                CAST(t.PatientID AS BIGINT) AS patient_id,
                t.visit_type,
                MAX(t.is_current_ipd) AS is_current_ipd
            FROM (
                SELECT
                    v.PatientID,
                    {_visit_type_expr()} AS visit_type,
                    CAST(CASE
                        WHEN {current_ipd}
                            THEN 1 ELSE 0 END AS INT) AS is_current_ipd
                FROM dbo.Visit v WITH (NOLOCK)
                WHERE v.PatientID IN ({placeholders})
                  AND ({valid_qr_visit})
                  AND (
                        ISNULL(v.VisitTypeID, 0) IN (1, 2, 3, 6)
                        OR UPPER(LTRIM(RTRIM(ISNULL(v.TypeOfVisit, N'')))) IN (N'IPD', N'OPD', N'DPV', N'HCV', N'IN PATIENT VISITS', N'INPATIENT VISITS')
                        OR UPPER(LTRIM(RTRIM(ISNULL(v.TypeOfVisit, N'')))) LIKE N'%DAY%'
                      )
            ) t
            WHERE NULLIF(LTRIM(RTRIM(ISNULL(t.visit_type, N''))), N'') IS NOT NULL
            GROUP BY t.PatientID, t.visit_type;
        """
        df = pd.read_sql(sql, conn, params=ids)
        grouped = {}
        admitted = {}
        for row in _df_rows(df):
            patient_id = _safe_int(row.get("patient_id"), 0)
            if patient_id <= 0:
                continue
            grouped.setdefault(patient_id, []).append(row.get("visit_type"))
            if _safe_int(row.get("is_current_ipd"), 0) == 1:
                admitted[patient_id] = True
        summary = {}
        for patient_id, values in grouped.items():
            item = _visit_type_groups(values)
            item["is_current_ipd"] = bool(admitted.get(patient_id))
            item["admission_status"] = "Admitted" if item["is_current_ipd"] else ""
            summary[patient_id] = item
        return summary
    except Exception as exc:
        print(f"Feedback visit type summary failed: {exc}")
        return {}


def _patient_search_unit(unit, query, limit=25):
    query_text = _clean_text(query, 50)
    conn = get_sql_connection(unit)
    if not conn:
        return []
    try:
        effective_limit = max(1, min(int(limit or 15), 30))
        query_like = f"%{query_text}%"
        query_prefix = f"{query_text}%"
        query_digits = _digits(query_text, 30)
        patient_id_value = _safe_int(query_digits, 0) if query_digits and len(query_digits) <= 18 else 0
        sql = f"""
            SELECT TOP {effective_limit}
                CAST(p.PatientId AS BIGINT) AS patient_id,
                LTRIM(RTRIM(ISNULL(p.Registration_No, N''))) AS registration_no,
                LTRIM(RTRIM(ISNULL(dbo.fn_patientfullname(p.PatientId), N''))) AS patient_name,
                LTRIM(RTRIM(ISNULL(p.Mobile, N''))) AS mobile_no,
                CAST(p.Registration_Date AS DATETIME) AS registration_date,
                LTRIM(RTRIM(ISNULL(p.Gender, N''))) AS gender,
                LTRIM(RTRIM(COALESCE(CONVERT(NVARCHAR(30), p.Age), N''))) AS age
            FROM dbo.Patient p WITH (NOLOCK)
            WHERE ISNULL(p.Deactive, 0) = 0
              AND (
                    LTRIM(RTRIM(ISNULL(p.Registration_No, N''))) = ?
                    OR LTRIM(RTRIM(ISNULL(p.Mobile, N''))) = ?
                    OR LTRIM(RTRIM(ISNULL(p.Registration_No, N''))) LIKE ?
                    OR LTRIM(RTRIM(ISNULL(p.Registration_No, N''))) LIKE ?
                    OR LTRIM(RTRIM(ISNULL(p.Mobile, N''))) LIKE ?
                    OR (? > 0 AND p.PatientId = ?)
                    OR (? > 0 AND CONVERT(VARCHAR(30), p.PatientId) LIKE ?)
                  )
            ORDER BY
                CASE
                    WHEN LTRIM(RTRIM(ISNULL(p.Registration_No, N''))) = ? THEN 0
                    WHEN (? > 0 AND p.PatientId = ?) THEN 1
                    WHEN LTRIM(RTRIM(ISNULL(p.Mobile, N''))) = ? THEN 2
                    WHEN LTRIM(RTRIM(ISNULL(p.Registration_No, N''))) LIKE ? THEN 3
                    WHEN LTRIM(RTRIM(ISNULL(p.Registration_No, N''))) LIKE ? THEN 4
                    ELSE 5
                END,
                p.Registration_Date DESC,
                p.PatientId DESC;
        """
        df = pd.read_sql(
            sql,
            conn,
            params=[
                query_text,
                query_text,
                query_prefix,
                query_like,
                query_prefix,
                patient_id_value,
                patient_id_value,
                patient_id_value,
                f"{patient_id_value}%" if patient_id_value else "",
                query_text,
                patient_id_value,
                patient_id_value,
                query_text,
                query_prefix,
                query_like,
            ],
        )
        rows = _df_rows(df)
        visit_type_summary = _patient_visit_type_summary_unit(conn, [row.get("patient_id") for row in rows])
        for row in rows:
            row["source_unit"] = unit
            row["masked_mobile"] = _mask_mobile(row.get("mobile_no"))
            default_summary = _visit_type_groups([])
            default_summary["is_current_ipd"] = False
            default_summary["admission_status"] = ""
            row.update(visit_type_summary.get(_safe_int(row.get("patient_id"), 0), default_summary))
            row["has_valid_visit"] = bool(row.get("is_current_ipd") or row.get("visit_types"))
        return rows
    except Exception as exc:
        print(f"Feedback patient search failed for {unit}: {exc}")
        return []
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _mask_mobile(value):
    digits = _digits(value)
    if len(digits) < 4:
        return ""
    return f"******{digits[-4:]}"


def search_patients(query, unit=None):
    q = _clean_text(query, 50)
    if len(q) < 3:
        return []
    rows = []
    unit_keys = _public_units(unit)
    if len(unit_keys) <= 1:
        for unit_key in unit_keys:
            rows.extend(_patient_search_unit(unit_key, q))
    else:
        with ThreadPoolExecutor(max_workers=min(len(unit_keys), 3)) as pool:
            future_map = {pool.submit(_patient_search_unit, unit_key, q): unit_key for unit_key in unit_keys}
            for future in as_completed(future_map):
                try:
                    rows.extend(future.result() or [])
                except Exception as exc:
                    print(f"Feedback parallel patient search failed for {future_map[future]}: {exc}")
    seen = set()
    deduped = []
    for row in rows:
        key = (row.get("source_unit"), str(row.get("patient_id") or ""), row.get("registration_no"))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    return deduped[:60]


def _visits_for_patient_unit(unit, patient_id=None, mobile=None, reg_no=None, visit_id=None):
    conn = get_sql_connection(unit)
    if not conn:
        return []
    try:
        direct_visit_lookup = bool(visit_id)
        if visit_id:
            where = ["v.Visit_ID = ?"]
            params = [int(visit_id)]
            if patient_id:
                where.append("v.PatientID = ?")
                params.append(int(patient_id))
            if reg_no:
                where.append("LTRIM(RTRIM(ISNULL(p.Registration_No, N''))) = ?")
                params.append(_clean_text(reg_no, 50))
            where_sql = " AND ".join(f"({part})" for part in where)
        else:
            where = []
            params = []
            if patient_id:
                where.append("v.PatientID = ?")
                params.append(int(patient_id))
            if reg_no:
                where.append("LTRIM(RTRIM(ISNULL(p.Registration_No, N''))) = ?")
                params.append(_clean_text(reg_no, 50))
            if mobile:
                where.append("LTRIM(RTRIM(ISNULL(p.Mobile, N''))) = ?")
                params.append(_clean_text(mobile, 20))
            if not where:
                return []
            where_sql = " OR ".join(f"({part})" for part in where)
        if not where_sql:
            return []
        current_ipd = _current_ipd_condition()
        valid_qr_visit = _valid_qr_visit_condition()
        validity_sql = "" if direct_visit_lookup else f"AND ({valid_qr_visit})"
        top_count = 20 if direct_visit_lookup else 1
        clinical_department_expr = _clinical_department_expr()
        sql = f"""
            SELECT TOP {top_count}
                CAST(v.Visit_ID AS BIGINT) AS visit_id,
                CAST(v.PatientID AS BIGINT) AS patient_id,
                LTRIM(RTRIM(ISNULL(p.Registration_No, N''))) AS registration_no,
                LTRIM(RTRIM(ISNULL(dbo.fn_patientfullname(v.PatientID), N''))) AS patient_name,
                LTRIM(RTRIM(ISNULL(p.Mobile, N''))) AS mobile_no,
                LTRIM(RTRIM(ISNULL(v.VisitNo, N''))) AS visit_no,
                {_visit_type_expr()} AS visit_type,
                CAST(v.VisitDate AS DATETIME) AS visit_date,
                LTRIM(RTRIM(ISNULL(dbo.fn_doctorfirstname(v.DocInCharge), N''))) AS doctor_name,
                LTRIM(RTRIM(ISNULL({clinical_department_expr}, N''))) AS clinical_department,
                LTRIM(RTRIM(ISNULL(dbo.Fn_subDept(v.UnitID), N''))) AS unit_name,
                CAST(CASE
                    WHEN {current_ipd}
                        THEN 1 ELSE 0 END AS BIT) AS is_current_ipd
            FROM dbo.Visit v WITH (NOLOCK)
            INNER JOIN dbo.Patient p WITH (NOLOCK)
                ON p.PatientId = v.PatientID
            WHERE ({where_sql})
              AND ISNULL(p.Deactive, 0) = 0
              {validity_sql}
              AND (
                    ISNULL(v.VisitTypeID, 0) IN (1, 2, 3, 6)
                    OR UPPER(LTRIM(RTRIM(ISNULL(v.TypeOfVisit, N'')))) IN (N'IPD', N'OPD', N'DPV', N'HCV', N'IN PATIENT VISITS', N'INPATIENT VISITS')
                    OR UPPER(LTRIM(RTRIM(ISNULL(v.TypeOfVisit, N'')))) LIKE N'%DAY%'
                  )
            ORDER BY
                CASE
                    WHEN {current_ipd}
                        THEN 0 ELSE 1 END,
                ISNULL(v.VisitDate, '19000101') DESC,
                v.Visit_ID DESC;
        """
        df = pd.read_sql(sql, conn, params=params)
        rows = _df_rows(df)
        for row in rows:
            row["source_unit"] = unit
        return rows
    except Exception as exc:
        print(f"Feedback visits fetch failed for {unit}: {exc}")
        return []
    finally:
        try:
            conn.close()
        except Exception:
            pass


def patient_visits(patient_id=None, mobile=None, reg_no=None, unit=None):
    rows = []
    for unit_key in _public_units(unit):
        rows.extend(_visits_for_patient_unit(unit_key, patient_id=patient_id, mobile=mobile, reg_no=reg_no))
    seen = set()
    out = []
    for row in rows:
        key = (row.get("source_unit"), row.get("visit_id"))
        if key in seen:
            continue
        seen.add(key)
        out.append(row)
    def _sort_key(row):
        raw_dt = str(row.get("visit_date") or "")
        ts = 0.0
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                ts = datetime.strptime(raw_dt[:19 if "H" in fmt else 10], fmt).timestamp()
                break
            except Exception:
                pass
        return (0 if row.get("is_current_ipd") else 1, -ts, -_safe_int(row.get("visit_id"), 0))

    return sorted(out, key=_sort_key)[:1]


def _fetch_visit_by_id(unit, patient_id, visit_id):
    rows = _visits_for_patient_unit(unit, patient_id=patient_id, visit_id=visit_id)
    return rows[0] if rows else None


def _department_rows():
    if not ensure_feedback_schema():
        return []
    conn = _hid_conn()
    if not conn:
        return []
    try:
        df = pd.read_sql(
            """
            SELECT DepartmentID AS department_id, DepartmentName AS department_name
            FROM dbo.FeedbackComplaint_Department_Mst WITH (NOLOCK)
            WHERE IsActive = 1
            ORDER BY DepartmentID;
            """,
            conn,
        )
        return _df_rows(df)
    except Exception as exc:
        print(f"Feedback department fetch failed: {exc}")
        return []
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _insert_audit(cur, complaint_id, action, user_id=None, user_name=None, details=None, ip_address=None):
    cur.execute(
        """
        INSERT INTO dbo.FeedbackComplaint_AuditLog
            (ComplaintID, Action, PerformedByUserID, PerformedByName, Details, IPAddress)
        VALUES (?, ?, ?, ?, ?, ?);
        """,
        (complaint_id, action, user_id, user_name, details, ip_address),
    )


def _insert_remark(cur, complaint_id, user_id, user_name, text, old_status, new_status, action_type):
    cur.execute(
        """
        INSERT INTO dbo.FeedbackComplaint_Remarks
            (ComplaintID, UserID, UserName, RemarkText, OldStatus, NewStatus, ActionType)
        VALUES (?, ?, ?, ?, ?, ?, ?);
        """,
        (complaint_id, user_id, user_name, text, old_status, new_status, action_type),
    )


def _complaint_notification_row(cur, complaint_id):
    cur.execute(
        """
        SELECT TOP 1
            fc.ComplaintID,
            fc.ComplaintNo,
            fc.PatientName,
            fc.SourceUnit,
            fc.Status,
            fc.SubmittedAt,
            fc.VisitType,
            fc.VisitNo,
            dm.DepartmentName
        FROM dbo.FeedbackComplaint_Mst fc WITH (NOLOCK)
        INNER JOIN dbo.FeedbackComplaint_Department_Mst dm WITH (NOLOCK)
            ON dm.DepartmentID = fc.ComplaintDepartmentID
        WHERE fc.ComplaintID = ?;
        """,
        (int(complaint_id),),
    )
    rows = _rows_from_cursor(cur)
    return rows[0] if rows else {}


def submit_complaint(payload, ip_address=None, send_graph_mail_with_attachment=None, dashboard_url=None):
    if not ensure_feedback_schema():
        return {"status": "error", "message": "Feedback service is temporarily unavailable."}, 503
    unit = _clean_text(payload.get("source_unit"), 20).upper()
    patient_id = _safe_int(payload.get("patient_id"), 0)
    visit_id = _safe_int(payload.get("visit_id"), 0)
    dept_id = _safe_int(payload.get("complaint_department_id"), 0)
    complaint_type = _clean_text(payload.get("complaint_type") or "COMPLAINT", 20).upper()
    details = _clean_text(payload.get("details"), 8000)

    if unit not in _public_units(unit) or patient_id <= 0 or visit_id <= 0:
        return {"status": "error", "message": "Please select a valid patient visit."}, 400
    if complaint_type not in {"FEEDBACK", "COMPLAINT"}:
        return {"status": "error", "message": "Please select feedback or complaint."}, 400
    if dept_id <= 0:
        return {"status": "error", "message": "Please select a department."}, 400
    if len(details) < 3:
        return {"status": "error", "message": "Please enter a little more detail."}, 400

    visit = _fetch_visit_by_id(unit, patient_id, visit_id)
    if not visit:
        return {"status": "error", "message": "We could not verify this visit. Please search again."}, 400

    conn = _hid_conn()
    if not conn:
        return {"status": "error", "message": "Feedback service is temporarily unavailable."}, 503
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT TOP 1 DepartmentID
            FROM dbo.FeedbackComplaint_Department_Mst WITH (NOLOCK)
            WHERE DepartmentID = ? AND IsActive = 1;
            """,
            (dept_id,),
        )
        if not cur.fetchone():
            return {"status": "error", "message": "Please select a valid department."}, 400
        temp_no = f"FCTMP-{token_hex(8).upper()}"
        cur.execute(
            """
            INSERT INTO dbo.FeedbackComplaint_Mst (
                ComplaintNo, SourceUnit, PatientID, RegistrationNo, PatientName, MobileNo,
                VisitID, VisitNo, VisitType, VisitDate, DoctorName, ClinicalDepartment,
                ComplaintDepartmentID, ComplaintType, Details, Status, CurrentLevel,
                DepartmentDueAt, CreatedIP
            )
            OUTPUT INSERTED.ComplaintID
            VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                'OPEN', 'DEPARTMENT', DATEADD(HOUR, 12, GETDATE()), ?
            );
            """,
            (
                temp_no,
                unit,
                patient_id,
                _clean_text(visit.get("registration_no"), 50),
                _clean_text(visit.get("patient_name"), 200),
                _clean_text(visit.get("mobile_no"), 20),
                visit_id,
                _clean_text(visit.get("visit_no"), 50),
                _clean_text(visit.get("visit_type"), 20),
                visit.get("visit_date"),
                _clean_text(visit.get("doctor_name"), 200),
                _clean_text(visit.get("clinical_department"), 200),
                dept_id,
                complaint_type,
                details,
                _clean_text(ip_address, 50),
            ),
        )
        complaint_id = int(cur.fetchone()[0])
        complaint_no = f"FC{datetime.now():%y%m%d}{complaint_id:06d}"[-30:]
        cur.execute(
            "UPDATE dbo.FeedbackComplaint_Mst SET ComplaintNo = ? WHERE ComplaintID = ?;",
            (complaint_no, complaint_id),
        )
        _insert_remark(cur, complaint_id, None, "Patient", "Submitted from patient QR flow", None, "OPEN", "SUBMIT")
        _insert_audit(cur, complaint_id, "Patient submitted feedback/complaint", None, "Patient", complaint_type, ip_address)
        conn.commit()
        mail_status = {"status": "skipped", "message": "Mail sender or recipients unavailable."}
        try:
            row = _complaint_notification_row(cur, complaint_id)
            recipients = _recipient_emails(cur, complaint_id, {"DEPARTMENT_USER", "IT_ADMIN"})
            mail_status = _send_feedback_email(
                send_graph_mail_with_attachment,
                "New Patient Feedback / Complaint",
                row,
                recipients,
                dashboard_url,
            )
            _insert_audit(
                cur,
                complaint_id,
                "New complaint notification email",
                None,
                "System",
                f"Recipients: {len(recipients)} | Status: {mail_status.get('status')} | {mail_status.get('message') or ''}",
                ip_address,
            )
            conn.commit()
        except Exception as mail_exc:
            try:
                _insert_audit(
                    cur,
                    complaint_id,
                    "New complaint notification email failed",
                    None,
                    "System",
                    str(mail_exc),
                    ip_address,
                )
                conn.commit()
            except Exception:
                pass
        return {
            "status": "success",
            "complaint_id": complaint_id,
            "complaint_no": complaint_no,
            "mail_status": mail_status,
        }, 200
    except Exception as exc:
        print(f"Feedback submit failed: {exc}")
        return {"status": "error", "message": "We could not submit this right now. Please try again."}, 500
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _scope_sql_for_session(allowed_units_for_session):
    role = _role_base(session.get("role"))
    account_id = _safe_int(session.get("accountid") or session.get("account_id"), 0)
    user_name = _clean_text(session.get("username"), 150)
    allowed_units = [str(u or "").strip().upper() for u in (allowed_units_for_session() or []) if str(u or "").strip()]
    clauses = ["fc.IsActive = 1"]
    params = []
    if allowed_units:
        clauses.append("fc.SourceUnit IN (" + ",".join("?" for _ in allowed_units) + ")")
        params.extend(allowed_units)
    if role not in {"IT", "Management"}:
        clauses.append(
            """
            EXISTS (
                SELECT 1
                FROM dbo.FeedbackComplaint_Assignment_Mst ax WITH (NOLOCK)
                WHERE ax.IsActive = 1
                  AND (
                        (NULLIF(LTRIM(RTRIM(ISNULL(ax.AssignmentUserName, N''))), N'') IS NOT NULL AND ax.AssignmentUserName = ?)
                        OR
                        (NULLIF(LTRIM(RTRIM(ISNULL(ax.AssignmentUserName, N''))), N'') IS NULL AND ax.UserID = ?)
                      )
                  AND (ax.Unit = fc.SourceUnit OR ax.Unit = 'ALL' OR ax.Unit = '')
                  AND (
                        ax.RoleLevel IN ('CENTER_HEAD', 'VP_OPERATIONS', 'DIRECTOR', 'IT_ADMIN')
                        OR ax.DepartmentID = 0
                        OR ax.DepartmentID = fc.ComplaintDepartmentID
                      )
            )
            """
        )
        params.extend([user_name, account_id])
    return " AND ".join(clauses), params


def dashboard_payload(args, allowed_units_for_session):
    if not ensure_feedback_schema():
        return {"status": "error", "message": "Feedback database is unavailable."}, 503
    conn = _hid_conn()
    if not conn:
        return {"status": "error", "message": "Feedback database is unavailable."}, 503
    try:
        effective_status_sql = """
            CASE
                WHEN fc.Status = 'CLOSED' OR fc.CurrentLevel = 'CLOSED' THEN 'CLOSED'
                WHEN fc.Status IN ('PENDING_FINAL_CLOSURE', 'DEPT_REPLIED') THEN 'PENDING_FINAL_CLOSURE'
                WHEN fc.CurrentLevel = 'DIRECTOR' THEN 'ESCALATED_L2'
                WHEN fc.CurrentLevel = 'CENTER_HEAD_VP' THEN 'ESCALATED_L1'
                WHEN fc.CurrentLevel = 'DEPARTMENT' AND fc.Status IN ('ESCALATED_L1', 'ESCALATED_L2') THEN 'OPEN'
                ELSE fc.Status
            END
        """
        due_sql = """
            CASE
                WHEN fc.Status = 'CLOSED' OR fc.CurrentLevel = 'CLOSED' THEN NULL
                WHEN fc.CurrentLevel = 'DEPARTMENT' THEN fc.DepartmentDueAt
                WHEN fc.CurrentLevel = 'CENTER_HEAD_VP' THEN fc.L1DueAt
                ELSE NULL
            END
        """
        where_sql, params = _scope_sql_for_session(allowed_units_for_session)
        filters = []
        filter_params = []
        from_date = _clean_text(args.get("from_date"), 10)
        to_date = _clean_text(args.get("to_date"), 10)
        unit = _clean_text(args.get("unit"), 20).upper()
        status = _clean_text(args.get("status"), 30).upper()
        level = _clean_text(args.get("level"), 30).upper()
        dept_id = _safe_int(args.get("department_id"), 0)
        q = _clean_text(args.get("q"), 80)
        if from_date:
            filters.append("fc.SubmittedAt >= CONVERT(DATETIME, ?)")
            filter_params.append(from_date)
        if to_date:
            filters.append("fc.SubmittedAt < DATEADD(DAY, 1, CONVERT(DATETIME, ?))")
            filter_params.append(to_date)
        if unit:
            filters.append("fc.SourceUnit = ?")
            filter_params.append(unit)
        if status:
            filters.append(f"({effective_status_sql}) = ?")
            filter_params.append(status)
        if level:
            filters.append("fc.CurrentLevel = ?")
            filter_params.append(level)
        if dept_id:
            filters.append("fc.ComplaintDepartmentID = ?")
            filter_params.append(dept_id)
        if q:
            like = f"%{q}%"
            filters.append(
                """
                (
                    fc.ComplaintNo LIKE ? OR fc.RegistrationNo LIKE ? OR fc.MobileNo LIKE ?
                    OR fc.PatientName LIKE ? OR fc.VisitNo LIKE ?
                )
                """
            )
            filter_params.extend([like, like, like, like, like])
        if filters:
            where_sql = where_sql + " AND " + " AND ".join(filters)
        all_params = params + filter_params
        assigned_user_apply = _assignment_user_apply_sql(conn.cursor(), assignment_alias="ax", apply_alias="ux")
        summary_sql = f"""
            SELECT
                OpenCount = SUM(CASE WHEN ({effective_status_sql}) = 'OPEN' THEN 1 ELSE 0 END),
                InProgressCount = SUM(CASE WHEN ({effective_status_sql}) = 'IN_PROGRESS' THEN 1 ELSE 0 END),
                EscalatedCount = SUM(CASE WHEN ({effective_status_sql}) IN ('ESCALATED_L1','ESCALATED_L2') THEN 1 ELSE 0 END),
                PendingFinalClosureCount = SUM(CASE WHEN ({effective_status_sql}) = 'PENDING_FINAL_CLOSURE' THEN 1 ELSE 0 END),
                ClosedTodayCount = SUM(CASE WHEN ({effective_status_sql}) = 'CLOSED' AND CONVERT(date, fc.ClosedAt) = CONVERT(date, GETDATE()) THEN 1 ELSE 0 END),
                SlaBreachedCount = SUM(CASE
                    WHEN ({effective_status_sql}) <> 'CLOSED'
                     AND (
                        (fc.CurrentLevel = 'DEPARTMENT' AND fc.DepartmentDueAt < GETDATE())
                        OR (fc.CurrentLevel = 'CENTER_HEAD_VP' AND fc.L1DueAt < GETDATE())
                     )
                    THEN 1 ELSE 0 END)
            FROM dbo.FeedbackComplaint_Mst fc WITH (NOLOCK)
            WHERE {where_sql};
        """
        rows_sql = f"""
            SELECT TOP 250
                fc.ComplaintID AS complaint_id,
                fc.ComplaintNo AS complaint_no,
                fc.SubmittedAt AS submitted_at,
                fc.SourceUnit AS source_unit,
                fc.PatientName AS patient_name,
                fc.RegistrationNo AS registration_no,
                fc.MobileNo AS mobile_no,
                fc.VisitType AS visit_type,
                fc.VisitDate AS visit_date,
                dm.DepartmentName AS complaint_department,
                fc.ComplaintType AS complaint_type,
                fc.Status AS raw_status,
                ({effective_status_sql}) AS status,
                fc.CurrentLevel AS current_level,
                ({due_sql}) AS due_at,
                DATEDIFF(MINUTE, GETDATE(), ({due_sql})) AS due_minutes,
                AssignedTo = CASE WHEN fc.CurrentLevel = 'CLOSED' THEN N'Closed' ELSE COALESCE(assign.AssignedTo, N'Unassigned') END
            FROM dbo.FeedbackComplaint_Mst fc WITH (NOLOCK)
            INNER JOIN dbo.FeedbackComplaint_Department_Mst dm WITH (NOLOCK)
                ON dm.DepartmentID = fc.ComplaintDepartmentID
            OUTER APPLY (
                SELECT STRING_AGG(
                    COALESCE(NULLIF(ax.AssignmentUserName, N''), NULLIF(ux.UserName, N''), CONVERT(NVARCHAR(40), ax.UserID)),
                    N', '
                ) AS AssignedTo
                FROM dbo.FeedbackComplaint_Assignment_Mst ax WITH (NOLOCK)
                {assigned_user_apply}
                WHERE ax.IsActive = 1
                  AND (ax.DepartmentID = 0 OR ax.DepartmentID = fc.ComplaintDepartmentID)
                  AND (ax.Unit = fc.SourceUnit OR ax.Unit = 'ALL' OR ax.Unit = '')
                  AND (
                        (fc.CurrentLevel = 'DEPARTMENT' AND ax.RoleLevel = 'DEPARTMENT_USER')
                        OR (fc.CurrentLevel = 'CENTER_HEAD_VP' AND ax.RoleLevel IN ('CENTER_HEAD', 'VP_OPERATIONS'))
                        OR (fc.CurrentLevel = 'DIRECTOR' AND ax.RoleLevel = 'DIRECTOR')
                      )
            ) assign
            WHERE {where_sql}
            ORDER BY fc.SubmittedAt DESC, fc.ComplaintID DESC;
        """
        summary = pd.read_sql(summary_sql, conn, params=all_params)
        rows = pd.read_sql(rows_sql, conn, params=all_params)
        return {
            "status": "success",
            "summary": (_df_rows(summary)[0] if not summary.empty else {}),
            "rows": _df_rows(rows),
            "departments": _department_rows(),
        }, 200
    except Exception as exc:
        print(f"Feedback dashboard payload failed: {exc}")
        return {"status": "error", "message": "Could not load feedback dashboard."}, 500
    finally:
        try:
            conn.close()
        except Exception:
            pass


def complaint_detail(complaint_id, allowed_units_for_session):
    if not ensure_feedback_schema():
        return {"status": "error", "message": "Feedback database is unavailable."}, 503
    conn = _hid_conn()
    if not conn:
        return {"status": "error", "message": "Feedback database is unavailable."}, 503
    try:
        effective_status_sql = """
            CASE
                WHEN fc.Status = 'CLOSED' OR fc.CurrentLevel = 'CLOSED' THEN 'CLOSED'
                WHEN fc.Status IN ('PENDING_FINAL_CLOSURE', 'DEPT_REPLIED') THEN 'PENDING_FINAL_CLOSURE'
                WHEN fc.CurrentLevel = 'DIRECTOR' THEN 'ESCALATED_L2'
                WHEN fc.CurrentLevel = 'CENTER_HEAD_VP' THEN 'ESCALATED_L1'
                WHEN fc.CurrentLevel = 'DEPARTMENT' AND fc.Status IN ('ESCALATED_L1', 'ESCALATED_L2') THEN 'OPEN'
                ELSE fc.Status
            END
        """
        due_sql = """
            CASE
                WHEN fc.Status = 'CLOSED' OR fc.CurrentLevel = 'CLOSED' THEN NULL
                WHEN fc.CurrentLevel = 'DEPARTMENT' THEN fc.DepartmentDueAt
                WHEN fc.CurrentLevel = 'CENTER_HEAD_VP' THEN fc.L1DueAt
                ELSE NULL
            END
        """
        where_sql, params = _scope_sql_for_session(allowed_units_for_session)
        params.append(int(complaint_id))
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT TOP 1
                fc.*, dm.DepartmentName AS ComplaintDepartmentName,
                ({effective_status_sql}) AS EffectiveStatus,
                ({due_sql}) AS EffectiveDueAt,
                DATEDIFF(MINUTE, GETDATE(), ({due_sql})) AS DueMinutes
            FROM dbo.FeedbackComplaint_Mst fc WITH (NOLOCK)
            INNER JOIN dbo.FeedbackComplaint_Department_Mst dm WITH (NOLOCK)
                ON dm.DepartmentID = fc.ComplaintDepartmentID
            WHERE {where_sql} AND fc.ComplaintID = ?;
            """,
            params,
        )
        rows = _rows_from_cursor(cur)
        if not rows:
            return {"status": "error", "message": "Complaint not found."}, 404
        complaint = rows[0]
        cur.execute(
            """
            SELECT RemarkID AS remark_id, UserID AS user_id, UserName AS user_name,
                   RemarkText AS remark_text, OldStatus AS old_status, NewStatus AS new_status,
                   ActionType AS action_type, CreatedAt AS created_at
            FROM dbo.FeedbackComplaint_Remarks WITH (NOLOCK)
            WHERE ComplaintID = ?
            ORDER BY CreatedAt DESC, RemarkID DESC;
            """,
            (int(complaint_id),),
        )
        remarks = _rows_from_cursor(cur)
        cur.execute(
            """
            SELECT AuditID AS audit_id, Action AS action, PerformedByUserID AS user_id,
                   PerformedByName AS user_name, Details AS details, CreatedAt AS created_at, IPAddress AS ip_address
            FROM dbo.FeedbackComplaint_AuditLog WITH (NOLOCK)
            WHERE ComplaintID = ?
            ORDER BY CreatedAt DESC, AuditID DESC;
            """,
            (int(complaint_id),),
        )
        audit = _rows_from_cursor(cur)
        return {"status": "success", "complaint": complaint, "remarks": remarks, "audit": audit}, 200
    except Exception as exc:
        print(f"Feedback complaint detail failed: {exc}")
        return {"status": "error", "message": "Could not load complaint details."}, 500
    finally:
        try:
            conn.close()
        except Exception:
            pass


def complaint_action(
    complaint_id,
    payload,
    allowed_units_for_session,
    ip_address=None,
    send_graph_mail_with_attachment=None,
    dashboard_url=None,
):
    if not ensure_feedback_schema():
        return {"status": "error", "message": "Feedback database is unavailable."}, 503
    conn = _hid_conn()
    if not conn:
        return {"status": "error", "message": "Feedback database is unavailable."}, 503
    user_id = _safe_int(session.get("accountid") or session.get("account_id"), 0)
    user_name = _clean_text(session.get("username"), 150)
    role = _role_base(session.get("role"))
    try:
        remark = _clean_text(payload.get("remark"), 8000)
        new_status = _clean_text(payload.get("status"), 30).upper()
        action_type = _clean_text(payload.get("action_type") or "REMARK", 50).upper()
        if new_status and new_status not in STATUS_VALUES:
            return {"status": "error", "message": "Invalid status."}, 400
        if not remark and not new_status:
            return {"status": "error", "message": "Add a remark or choose a status."}, 400
        cur = conn.cursor()
        where_sql, scope_params = _scope_sql_for_session(allowed_units_for_session)
        cur.execute(
            f"""
            SELECT TOP 1 fc.Status, fc.CurrentLevel
            FROM dbo.FeedbackComplaint_Mst fc WITH (UPDLOCK, ROWLOCK)
            WHERE {where_sql} AND fc.ComplaintID = ?;
            """,
            [*scope_params, int(complaint_id)],
        )
        row = cur.fetchone()
        if not row:
            return {"status": "error", "message": "Complaint not found."}, 404
        old_status, old_level = row[0], row[1]
        final_status = new_status or old_status
        final_level = old_level
        closed_at_sql = ""
        final_closed_by_sql = ""
        l1_due_sql = ""
        params = []
        if final_status in {"DEPT_REPLIED", "PENDING_FINAL_CLOSURE"}:
            final_status = "PENDING_FINAL_CLOSURE"
            final_level = "CENTER_HEAD_VP"
        elif final_status == "CLOSED":
            if role not in FINAL_CLOSURE_ROLES and not _user_has_final_assignment(cur, user_id, user_name):
                return {"status": "error", "message": "Final closure is not permitted for your role."}, 403
            final_level = "CLOSED"
            closed_at_sql = ", ClosedAt = GETDATE()"
            final_closed_by_sql = ", FinalClosedBy = ?"
            params.append(user_id)
        elif final_status == "REOPENED":
            final_level = "DEPARTMENT"
        elif final_status == "ESCALATED_L1":
            final_level = "CENTER_HEAD_VP"
            l1_due_sql = ", L1DueAt = DATEADD(HOUR, 12, GETDATE())"
            action_type = "ESCALATION"
        elif final_status == "ESCALATED_L2":
            final_level = "DIRECTOR"
            action_type = "ESCALATION"
        update_needed = final_status != old_status or final_level != old_level
        if update_needed:
            cur.execute(
                f"""
                UPDATE dbo.FeedbackComplaint_Mst
                SET Status = ?, CurrentLevel = ?{l1_due_sql}{closed_at_sql}{final_closed_by_sql}
                WHERE ComplaintID = ?;
                """,
                [final_status, final_level, *params, int(complaint_id)],
            )
        _insert_remark(
            cur,
            int(complaint_id),
            user_id,
            user_name,
            remark,
            old_status,
            final_status,
            action_type if update_needed else "REMARK",
        )
        _insert_audit(
            cur,
            int(complaint_id),
            "Complaint updated" if update_needed else "Remark added",
            user_id,
            user_name,
            f"{old_status} -> {final_status}" if update_needed else "Remark",
            ip_address,
        )
        if update_needed and final_status in {"ESCALATED_L1", "ESCALATED_L2"}:
            try:
                mail_row = _complaint_notification_row(cur, int(complaint_id))
                if final_status == "ESCALATED_L1":
                    recipients = _recipient_emails(cur, int(complaint_id), {"CENTER_HEAD", "VP_OPERATIONS", "IT_ADMIN"})
                    event_label = "Escalated to Center Head / VP Operations"
                else:
                    recipients = _recipient_emails(cur, int(complaint_id), {"DIRECTOR", "IT_ADMIN"})
                    event_label = "Escalated to Director"
                mail_status = _send_feedback_email(
                    send_graph_mail_with_attachment,
                    event_label,
                    mail_row,
                    recipients,
                    dashboard_url,
                )
                _insert_audit(
                    cur,
                    int(complaint_id),
                    "Manual escalation notification email",
                    user_id,
                    user_name,
                    f"{event_label} | Recipients: {len(recipients)} | Status: {mail_status.get('status')} | {mail_status.get('message') or ''}",
                    ip_address,
                )
            except Exception as mail_exc:
                _insert_audit(
                    cur,
                    int(complaint_id),
                    "Manual escalation notification email failed",
                    user_id,
                    user_name,
                    str(mail_exc),
                    ip_address,
                )
        conn.commit()
        return {"status": "success", "message": "Complaint updated."}, 200
    except Exception as exc:
        print(f"Feedback action failed: {exc}")
        return {"status": "error", "message": "Could not update complaint."}, 500
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _user_has_final_assignment(cur, user_id, user_name=None):
    user_name = _clean_text(user_name, 150)
    if not user_id and not user_name:
        return False
    cur.execute(
        """
        SELECT TOP 1 1
        FROM dbo.FeedbackComplaint_Assignment_Mst WITH (NOLOCK)
        WHERE IsActive = 1
          AND (
                (NULLIF(LTRIM(RTRIM(ISNULL(AssignmentUserName, N''))), N'') IS NOT NULL AND AssignmentUserName = ?)
                OR
                (NULLIF(LTRIM(RTRIM(ISNULL(AssignmentUserName, N''))), N'') IS NULL AND UserID = ?)
              )
          AND RoleLevel IN ('CENTER_HEAD', 'VP_OPERATIONS', 'DIRECTOR', 'IT_ADMIN');
        """,
        (user_name, int(user_id or 0)),
    )
    return bool(cur.fetchone())


def _hid_user_email_expr(cur, alias="u"):
    try:
        cur.execute("SELECT COL_LENGTH('dbo.HID_User_Mst', 'Email');")
        has_email = bool(cur.fetchone()[0])
    except Exception:
        has_email = False
    return f"{alias}.Email" if has_email else "CAST(NULL AS NVARCHAR(200))"


def _assignment_user_apply_sql(cur, assignment_alias="a", apply_alias="ux"):
    user_email_expr = _hid_user_email_expr(cur, "u")
    return f"""
        OUTER APPLY (
            SELECT TOP 1
                u.UserName,
                u.RoleName,
                u.UnitScope,
                {user_email_expr} AS Email
            FROM dbo.HID_User_Mst u WITH (NOLOCK)
            WHERE ISNULL(u.IsActive, 1) = 1
              AND (
                    (NULLIF(LTRIM(RTRIM(ISNULL({assignment_alias}.AssignmentUserName, N''))), N'') IS NOT NULL
                     AND u.UserName = {assignment_alias}.AssignmentUserName)
                    OR
                    (NULLIF(LTRIM(RTRIM(ISNULL({assignment_alias}.AssignmentUserName, N''))), N'') IS NULL
                     AND u.AccountId = {assignment_alias}.UserID)
                  )
            ORDER BY
                CASE WHEN u.UserName = {assignment_alias}.AssignmentUserName THEN 0 ELSE 1 END,
                u.UserName
        ) {apply_alias}
    """


def assignment_payload():
    if not ensure_feedback_schema():
        return {"status": "error", "message": "Feedback database is unavailable."}, 503
    conn = _hid_conn()
    if not conn:
        return {"status": "error", "message": "Feedback database is unavailable."}, 503
    try:
        cur = conn.cursor()
        user_apply_sql = _assignment_user_apply_sql(cur)
        cur.execute(
            f"""
            SELECT
                a.AssignmentID AS assignment_id,
                a.DepartmentID AS department_id,
                COALESCE(d.DepartmentName, N'All Departments') AS department_name,
                a.Unit AS unit,
                a.UserID AS user_id,
                COALESCE(NULLIF(LTRIM(RTRIM(ISNULL(a.AssignmentUserName, N''))), N''), ux.UserName, CONVERT(NVARCHAR(40), a.UserID)) AS user_name,
                NULLIF(LTRIM(RTRIM(ISNULL(a.AssignmentUserName, N''))), N'') AS assignment_user_name,
                a.RoleLevel AS role_level,
                NULLIF(LTRIM(RTRIM(ISNULL(a.NotificationEmail, N''))), N'') AS assignment_email,
                COALESCE(NULLIF(LTRIM(RTRIM(ISNULL(a.NotificationEmail, N''))), N''), NULLIF(LTRIM(RTRIM(ISNULL(ux.Email, N''))), N'')) AS notification_email,
                a.IsActive AS is_active,
                a.CreatedAt AS created_at
            FROM dbo.FeedbackComplaint_Assignment_Mst a WITH (NOLOCK)
            LEFT JOIN dbo.FeedbackComplaint_Department_Mst d WITH (NOLOCK)
                ON d.DepartmentID = a.DepartmentID
            {user_apply_sql}
            WHERE a.IsActive = 1
            ORDER BY d.DepartmentName, a.Unit, a.RoleLevel, user_name;
            """
        )
        assignments = _rows_from_cursor(cur)
        user_email_expr = _hid_user_email_expr(cur, "u")
        cur.execute(
            f"""
            SELECT
                u.AccountId AS user_id,
                u.UserName AS user_name,
                u.RoleName AS role_name,
                u.UnitScope AS unit_scope,
                NULLIF(LTRIM(RTRIM(ISNULL({user_email_expr}, N''))), N'') AS email
            FROM dbo.HID_User_Mst u WITH (NOLOCK)
            WHERE ISNULL(IsActive, 1) = 1
            ORDER BY UserName;
            """
        )
        users = _rows_from_cursor(cur)
        return {
            "status": "success",
            "assignments": assignments,
            "departments": [{"department_id": 0, "department_name": "All Departments"}] + _department_rows(),
            "users": users,
            "role_levels": ASSIGNMENT_LEVEL_ORDER,
            "units": list(PUBLIC_UNITS) + ["ALL"],
        }, 200
    except Exception as exc:
        print(f"Feedback assignment payload failed: {exc}")
        return {"status": "error", "message": "Could not load assignments."}, 500
    finally:
        try:
            conn.close()
        except Exception:
            pass


def save_assignment(payload):
    if not ensure_feedback_schema():
        return {"status": "error", "message": "Feedback database is unavailable."}, 503
    conn = _hid_conn()
    if not conn:
        return {"status": "error", "message": "Feedback database is unavailable."}, 503
    try:
        assignment_id = _safe_int(payload.get("assignment_id"), 0)
        dept_id = _safe_int(payload.get("department_id"), 0)
        unit = _clean_text(payload.get("unit") or "ALL", 20).upper()
        user_id = _safe_int(payload.get("user_id"), 0)
        assignment_user_name = _clean_text(payload.get("user_name") or payload.get("assignment_user_name"), 150)
        role_level = _clean_text(payload.get("role_level"), 30).upper()
        notification_email = _clean_text(payload.get("notification_email"), 200).lower()
        is_active = 1 if bool(payload.get("is_active", True)) else 0
        if user_id <= 0 or role_level not in ASSIGNMENT_LEVELS:
            return {"status": "error", "message": "Please complete department, user, and role level."}, 400
        if dept_id < 0:
            return {"status": "error", "message": "Invalid department scope."}, 400
        if unit not in set(PUBLIC_UNITS) | {"ALL", ""}:
            return {"status": "error", "message": "Invalid unit."}, 400
        if not _is_valid_email(notification_email):
            return {"status": "error", "message": "Please enter a valid notification email."}, 400
        cur = conn.cursor()
        if assignment_user_name:
            cur.execute(
                """
                SELECT TOP 1 AccountId, UserName
                FROM dbo.HID_User_Mst WITH (NOLOCK)
                WHERE ISNULL(IsActive, 1) = 1
                  AND UserName = ?;
                """,
                (assignment_user_name,),
            )
            user_row = cur.fetchone()
            if not user_row:
                return {"status": "error", "message": "Selected HID user was not found."}, 400
            user_id = _safe_int(user_row[0], user_id)
            assignment_user_name = _clean_text(user_row[1], 150)
        else:
            cur.execute(
                """
                SELECT TOP 1 UserName
                FROM dbo.HID_User_Mst WITH (NOLOCK)
                WHERE ISNULL(IsActive, 1) = 1
                  AND AccountId = ?
                ORDER BY UserName;
                """,
                (user_id,),
            )
            user_row = cur.fetchone()
            assignment_user_name = _clean_text(user_row[0], 150) if user_row else ""
        if assignment_id <= 0:
            cur.execute(
                """
                SELECT TOP 1 AssignmentID
                FROM dbo.FeedbackComplaint_Assignment_Mst WITH (UPDLOCK, HOLDLOCK)
                WHERE DepartmentID = ?
                  AND UPPER(LTRIM(RTRIM(Unit))) = UPPER(LTRIM(RTRIM(?)))
                  AND UPPER(LTRIM(RTRIM(RoleLevel))) = UPPER(LTRIM(RTRIM(?)))
                  AND UPPER(LTRIM(RTRIM(ISNULL(AssignmentUserName, N'')))) = UPPER(LTRIM(RTRIM(?)));
                """,
                (dept_id, unit, role_level, assignment_user_name),
            )
            existing = cur.fetchone()
            if existing:
                assignment_id = _safe_int(existing[0], 0)
        if assignment_id > 0:
            cur.execute(
                """
                UPDATE dbo.FeedbackComplaint_Assignment_Mst
                SET DepartmentID = ?, Unit = ?, UserID = ?, AssignmentUserName = ?, RoleLevel = ?, NotificationEmail = ?, IsActive = ?
                WHERE AssignmentID = ?;
                """,
                (dept_id, unit, user_id, assignment_user_name or None, role_level, notification_email or None, is_active, assignment_id),
            )
        else:
            cur.execute(
                """
                INSERT INTO dbo.FeedbackComplaint_Assignment_Mst
                    (DepartmentID, Unit, UserID, AssignmentUserName, RoleLevel, NotificationEmail, IsActive, CreatedBy)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?);
                """,
                (dept_id, unit, user_id, assignment_user_name or None, role_level, notification_email or None, is_active, _clean_text(session.get("username"), 150)),
            )
        conn.commit()
        return {"status": "success", "message": "Assignment saved."}, 200
    except Exception as exc:
        print(f"Feedback assignment save failed: {exc}")
        return {"status": "error", "message": "Could not save assignment."}, 500
    finally:
        try:
            conn.close()
        except Exception:
            pass


def remove_assignment(assignment_id):
    if not ensure_feedback_schema():
        return {"status": "error", "message": "Feedback database is unavailable."}, 503
    assignment_id = _safe_int(assignment_id, 0)
    if assignment_id <= 0:
        return {"status": "error", "message": "Invalid assignment."}, 400
    conn = _hid_conn()
    if not conn:
        return {"status": "error", "message": "Feedback database is unavailable."}, 503
    try:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE dbo.FeedbackComplaint_Assignment_Mst
            SET IsActive = 0
            WHERE AssignmentID = ?;
            """,
            (assignment_id,),
        )
        conn.commit()
        return {"status": "success", "message": "Assignment removed."}, 200
    except Exception as exc:
        print(f"Feedback assignment remove failed: {exc}")
        return {"status": "error", "message": "Could not remove assignment."}, 500
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _recipient_emails(cur, complaint_id, role_levels=None):
    role_levels = {str(x or "").strip().upper() for x in (role_levels or []) if str(x or "").strip()}
    params = [int(complaint_id)]
    role_sql = ""
    if role_levels:
        role_sql = " AND a.RoleLevel IN (" + ",".join("?" for _ in role_levels) + ")"
        params.extend(sorted(role_levels))
    try:
        user_apply_sql = _assignment_user_apply_sql(cur)
        recipient_expr = (
            "COALESCE("
            "NULLIF(LTRIM(RTRIM(ISNULL(a.NotificationEmail, N''))), N''), "
            "NULLIF(LTRIM(RTRIM(ISNULL(ux.Email, N''))), N'')"
            ")"
        )
        cur.execute(
            f"""
            SELECT DISTINCT {recipient_expr} AS Email
            FROM dbo.FeedbackComplaint_Mst fc WITH (NOLOCK)
            INNER JOIN dbo.FeedbackComplaint_Assignment_Mst a WITH (NOLOCK)
                ON a.IsActive = 1
               AND (a.Unit = fc.SourceUnit OR a.Unit = 'ALL' OR a.Unit = '')
               AND (a.DepartmentID = 0 OR a.DepartmentID = fc.ComplaintDepartmentID OR a.RoleLevel IN ('CENTER_HEAD', 'VP_OPERATIONS', 'DIRECTOR', 'IT_ADMIN'))
            {user_apply_sql}
            WHERE fc.ComplaintID = ?
              AND {recipient_expr} IS NOT NULL
              {role_sql};
            """,
            params,
        )
        return [str(row[0] or "").strip() for row in cur.fetchall() if str(row[0] or "").strip()]
    except Exception:
        return []


def _email_body(event_label, row, dashboard_url):
    complaint_no = html.escape(str(row.get("ComplaintNo") or row.get("complaint_no") or ""))
    patient = html.escape(str(row.get("PatientName") or row.get("patient_name") or ""))
    unit = html.escape(str(row.get("SourceUnit") or row.get("source_unit") or ""))
    department = html.escape(str(row.get("DepartmentName") or row.get("complaint_department") or ""))
    status = html.escape(str(row.get("Status") or row.get("status") or ""))
    submitted = html.escape(str(row.get("SubmittedAt") or row.get("submitted_at") or ""))
    link = html.escape(str(dashboard_url or ""))
    return f"""
    <div style="font-family:Arial,sans-serif;color:#10213d;line-height:1.5">
      <p>Dear Team,</p>
      <p><b>{html.escape(event_label)}</b></p>
      <p>
        <b>Complaint No:</b> {complaint_no}<br>
        <b>Patient:</b> {patient}<br>
        <b>Unit:</b> {unit}<br>
        <b>Department:</b> {department}<br>
        <b>Submitted:</b> {submitted}<br>
        <b>Status:</b> {status}
      </p>
      <p>Please review this item in HID Feedback / Complaint Management.</p>
      {f'<p><a href="{link}">Open HID Dashboard</a></p>' if link else ''}
      <p style="font-size:12px;color:#64748b">This is an automated notification from Hospital Intelligence Dashboard.</p>
    </div>
    """


def _send_feedback_email(send_graph_mail_with_attachment, event_label, row, recipients, dashboard_url=None):
    recipients = [str(x or "").strip() for x in dict.fromkeys(recipients or []) if str(x or "").strip()]
    if not recipients or not send_graph_mail_with_attachment:
        return {"status": "skipped", "message": "Mail sender or recipients unavailable."}
    subject = f"HID Feedback / Complaint - {event_label} - {row.get('ComplaintNo') or ''}".strip()
    try:
        return send_graph_mail_with_attachment(
            subject=subject,
            body_html=_email_body(event_label, row, dashboard_url),
            to_recipients=recipients,
        )
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


def run_feedback_escalation_job(*, send_graph_mail_with_attachment=None, dashboard_url=None, actor="scheduler"):
    if not ensure_feedback_schema():
        return {"status": "skipped", "message": "Feedback schema unavailable."}
    conn = _hid_conn()
    if not conn:
        return {"status": "skipped", "message": "Feedback database unavailable."}
    try:
        cur = conn.cursor()
        results = {"l1": 0, "l2": 0}
        cur.execute(
            """
            SELECT fc.ComplaintID, fc.ComplaintNo, fc.PatientName, fc.SourceUnit, fc.Status, fc.SubmittedAt, dm.DepartmentName
            FROM dbo.FeedbackComplaint_Mst fc WITH (UPDLOCK, READPAST)
            INNER JOIN dbo.FeedbackComplaint_Department_Mst dm WITH (NOLOCK)
                ON dm.DepartmentID = fc.ComplaintDepartmentID
            WHERE fc.IsActive = 1
              AND fc.CurrentLevel = 'DEPARTMENT'
              AND fc.Status IN ('OPEN', 'IN_PROGRESS', 'REOPENED')
              AND fc.DepartmentDueAt <= GETDATE()
              AND NOT EXISTS (
                  SELECT 1 FROM dbo.FeedbackComplaint_Remarks r WITH (NOLOCK)
                  WHERE r.ComplaintID = fc.ComplaintID
                    AND r.UserID IS NOT NULL
                    AND r.CreatedAt >= fc.SubmittedAt
                    AND ISNULL(r.ActionType, '') <> 'SUBMIT'
              );
            """
        )
        l1_rows = _rows_from_cursor(cur)
        for row in l1_rows:
            cid = int(row["ComplaintID"])
            cur.execute(
                """
                UPDATE dbo.FeedbackComplaint_Mst
                SET Status = 'ESCALATED_L1',
                    CurrentLevel = 'CENTER_HEAD_VP',
                    L1DueAt = DATEADD(HOUR, 12, GETDATE())
                WHERE ComplaintID = ?;
                """,
                (cid,),
            )
            _insert_remark(cur, cid, None, "Scheduler", "Auto escalated after department TAT breach.", row.get("Status"), "ESCALATED_L1", "ESCALATION")
            _insert_audit(cur, cid, "Auto escalated to Center Head / VP Operations", None, actor, "Department TAT breached", None)
            conn.commit()
            recipients = _recipient_emails(cur, cid, {"CENTER_HEAD", "VP_OPERATIONS", "IT_ADMIN"})
            mail_status = _send_feedback_email(send_graph_mail_with_attachment, "Escalated to Center Head / VP Operations", row, recipients, dashboard_url)
            _insert_audit(
                cur,
                cid,
                "Escalation notification email",
                None,
                actor,
                f"L1 recipients: {len(recipients)} | Status: {mail_status.get('status')} | {mail_status.get('message') or ''}",
                None,
            )
            conn.commit()
            results["l1"] += 1
        cur.execute(
            """
            SELECT fc.ComplaintID, fc.ComplaintNo, fc.PatientName, fc.SourceUnit, fc.Status, fc.SubmittedAt, dm.DepartmentName
            FROM dbo.FeedbackComplaint_Mst fc WITH (UPDLOCK, READPAST)
            INNER JOIN dbo.FeedbackComplaint_Department_Mst dm WITH (NOLOCK)
                ON dm.DepartmentID = fc.ComplaintDepartmentID
            WHERE fc.IsActive = 1
              AND fc.CurrentLevel = 'CENTER_HEAD_VP'
              AND fc.Status = 'ESCALATED_L1'
              AND fc.L1DueAt <= GETDATE()
              AND NOT EXISTS (
                  SELECT 1 FROM dbo.FeedbackComplaint_Remarks r WITH (NOLOCK)
                  WHERE r.ComplaintID = fc.ComplaintID
                    AND r.UserID IS NOT NULL
                    AND r.CreatedAt >= DATEADD(HOUR, -12, fc.L1DueAt)
              );
            """
        )
        l2_rows = _rows_from_cursor(cur)
        for row in l2_rows:
            cid = int(row["ComplaintID"])
            cur.execute(
                """
                UPDATE dbo.FeedbackComplaint_Mst
                SET Status = 'ESCALATED_L2',
                    CurrentLevel = 'DIRECTOR'
                WHERE ComplaintID = ?;
                """,
                (cid,),
            )
            _insert_remark(cur, cid, None, "Scheduler", "Auto escalated to Director after L1 TAT breach.", row.get("Status"), "ESCALATED_L2", "ESCALATION")
            _insert_audit(cur, cid, "Auto escalated to Director", None, actor, "L1 TAT breached", None)
            conn.commit()
            recipients = _recipient_emails(cur, cid, {"DIRECTOR", "IT_ADMIN"})
            mail_status = _send_feedback_email(send_graph_mail_with_attachment, "Escalated to Director", row, recipients, dashboard_url)
            _insert_audit(
                cur,
                cid,
                "Escalation notification email",
                None,
                actor,
                f"L2 recipients: {len(recipients)} | Status: {mail_status.get('status')} | {mail_status.get('message') or ''}",
                None,
            )
            conn.commit()
            results["l2"] += 1
        return {"status": "success", "results": results}
    except Exception as exc:
        print(f"Feedback escalation job failed: {exc}")
        return {"status": "error", "message": str(exc)}
    finally:
        try:
            conn.close()
        except Exception:
            pass


def create_feedback_complaint_blueprint(
    *,
    login_required,
    allowed_units_for_session,
    sanitize_json_payload,
    local_tz,
    send_graph_mail_with_attachment=None,
):
    bp = Blueprint("feedback_complaint", __name__)

    def _json(payload, status=200):
        try:
            return jsonify(sanitize_json_payload(payload)), status
        except Exception:
            return jsonify(payload), status

    @bp.route("/feedback/patient")
    def public_feedback_page():
        return render_template("feedback_complaint/public.html", departments=DEPARTMENTS, units=list(PUBLIC_UNITS))

    @bp.route("/feedback-complaint")
    @login_required(allowed_roles={"IT", "Management", "Departmental Head", "Executive"})
    def dashboard_page():
        return render_template(
            "feedback_complaint/dashboard.html",
            departments=_department_rows(),
            units=[u for u in PUBLIC_UNITS if u in {str(x).strip().upper() for x in (allowed_units_for_session() or [])}] or list(PUBLIC_UNITS),
            can_manage_assignments=_role_base(session.get("role")) == "IT",
            scoped_notice=_role_base(session.get("role")) not in {"IT", "Management"},
            statuses=sorted(STATUS_VALUES),
        )

    @bp.route("/api/feedback/departments")
    def api_departments():
        return _json({"status": "success", "departments": _department_rows()})

    @bp.route("/api/feedback/search_patient")
    def api_search_patient():
        ip = request.headers.get("X-Forwarded-For", request.remote_addr or "").split(",", 1)[0].strip()
        if not _rate_allowed(ip, "patient_search"):
            return _json({"status": "error", "message": "Please wait a moment before searching again."}, 429)
        query = request.args.get("query") or request.args.get("q") or ""
        unit = request.args.get("unit")
        rows = search_patients(query, unit=unit)
        return _json({"status": "success", "patients": rows, "count": len(rows)})

    @bp.route("/api/feedback/patient_visits")
    def api_patient_visits():
        ip = request.headers.get("X-Forwarded-For", request.remote_addr or "").split(",", 1)[0].strip()
        if not _rate_allowed(ip, "patient_visits"):
            return _json({"status": "error", "message": "Please wait a moment before trying again."}, 429)
        rows = patient_visits(
            patient_id=_safe_int(request.args.get("patient_id"), 0),
            mobile=request.args.get("mobile"),
            reg_no=request.args.get("reg_no"),
            unit=request.args.get("unit"),
        )
        return _json({"status": "success", "visits": rows, "count": len(rows)})

    @bp.route("/api/feedback/submit", methods=["POST"])
    def api_submit():
        ip = request.headers.get("X-Forwarded-For", request.remote_addr or "").split(",", 1)[0].strip()
        if not _rate_allowed(ip, "submit", limit=12, window=600):
            return _json({"status": "error", "message": "Please wait a moment before submitting again."}, 429)
        payload = request.get_json(silent=True) or {}
        result, status = submit_complaint(
            payload,
            ip_address=ip,
            send_graph_mail_with_attachment=send_graph_mail_with_attachment,
            dashboard_url=request.host_url.rstrip("/") + "/feedback-complaint",
        )
        return _json(result, status)

    @bp.route("/api/feedback/dashboard")
    @login_required(allowed_roles={"IT", "Management", "Departmental Head", "Executive"})
    def api_dashboard():
        payload, status = dashboard_payload(request.args, allowed_units_for_session)
        return _json(payload, status)

    @bp.route("/api/feedback/complaints/<int:complaint_id>")
    @login_required(allowed_roles={"IT", "Management", "Departmental Head", "Executive"})
    def api_complaint_detail(complaint_id):
        payload, status = complaint_detail(complaint_id, allowed_units_for_session)
        return _json(payload, status)

    @bp.route("/api/feedback/complaints/<int:complaint_id>/action", methods=["POST"])
    @login_required(allowed_roles={"IT", "Management", "Departmental Head", "Executive"})
    def api_complaint_action(complaint_id):
        ip = request.headers.get("X-Forwarded-For", request.remote_addr or "").split(",", 1)[0].strip()
        payload, status = complaint_action(
            complaint_id,
            request.get_json(silent=True) or {},
            allowed_units_for_session,
            ip_address=ip,
            send_graph_mail_with_attachment=send_graph_mail_with_attachment,
            dashboard_url=request.host_url.rstrip("/") + "/feedback-complaint",
        )
        return _json(payload, status)

    @bp.route("/api/feedback/assignments")
    @login_required(allowed_roles={"IT"}, required_section="feedback_complaint")
    def api_assignments():
        payload, status = assignment_payload()
        return _json(payload, status)

    @bp.route("/api/feedback/assignments", methods=["POST"])
    @login_required(allowed_roles={"IT"}, required_section="feedback_complaint")
    def api_save_assignment():
        payload, status = save_assignment(request.get_json(silent=True) or {})
        return _json(payload, status)

    @bp.route("/api/feedback/assignments/<int:assignment_id>", methods=["DELETE"])
    @login_required(allowed_roles={"IT"}, required_section="feedback_complaint")
    def api_remove_assignment(assignment_id):
        payload, status = remove_assignment(assignment_id)
        return _json(payload, status)

    @bp.route("/api/feedback/run_escalation", methods=["POST"])
    @login_required(allowed_roles={"IT"}, required_section="feedback_complaint")
    def api_run_escalation():
        result = run_feedback_escalation_job(
            send_graph_mail_with_attachment=send_graph_mail_with_attachment,
            dashboard_url=request.host_url.rstrip("/") + "/feedback-complaint",
            actor=session.get("username") or "manual",
        )
        return _json(result, 200 if result.get("status") in {"success", "skipped"} else 500)

    return bp
