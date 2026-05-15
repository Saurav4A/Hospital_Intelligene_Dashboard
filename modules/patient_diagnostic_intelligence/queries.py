from __future__ import annotations

from datetime import date
from threading import Lock
from time import monotonic
from typing import Any

from modules.db_connection import get_sql_connection

from .utils import MAX_PAGE_SIZE, parse_positive_int, validate_int_list

_METADATA_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
_METADATA_LOCK = Lock()
_METADATA_TTL_SECONDS = 900


def _q(name: str) -> str:
    return "[" + str(name).replace("]", "]]") + "]"


def _table_ref(name: str) -> str:
    if "." in name:
        schema, table = name.split(".", 1)
        return f"{_q(schema)}.{_q(table)}"
    return f"dbo.{_q(name)}"


def _fetch_all_dicts(cursor) -> list[dict[str, Any]]:
    columns = [col[0] for col in (cursor.description or [])]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def _schema(conn) -> dict[str, dict[str, str]]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = 'dbo'
        """
    )
    schema: dict[str, dict[str, str]] = {}
    for table_schema, table_name, column_name in cur.fetchall():
        full = f"{table_schema}.{table_name}"
        schema.setdefault(full.lower(), {})[str(column_name).lower()] = str(column_name)
    return schema


def _resolve_table(schema: dict[str, dict[str, str]], candidates: list[str]) -> str | None:
    for item in candidates:
        key = item.lower() if "." in item else f"dbo.{item}".lower()
        if key in schema:
            return key
    for item in candidates:
        target = item.split(".")[-1].lower()
        for key in schema:
            if key.split(".")[-1].lower() == target:
                return key
    return None


def _col(cols: dict[str, str] | None, candidates: list[str]) -> str | None:
    if not cols:
        return None
    for cand in candidates:
        found = cols.get(cand.lower())
        if found:
            return found
    return None


def _text_expr(alias: str, col: str | None, default: str = "") -> str:
    if not col:
        return f"CAST(N'{default}' AS NVARCHAR(300))"
    return f"LTRIM(RTRIM(ISNULL(CONVERT(NVARCHAR(300), {alias}.{_q(col)}), N'')))"


def _date_expr(alias: str, col: str | None) -> str:
    if not col:
        return "CAST(NULL AS DATETIME)"
    raw = f"CONVERT(NVARCHAR(50), {alias}.{_q(col)})"
    return f"CASE WHEN ISDATE({raw}) = 1 THEN CONVERT(DATETIME, {alias}.{_q(col)}) ELSE NULL END"


def _int_expr(alias: str, col: str | None) -> str:
    if not col:
        return "CAST(NULL AS INT)"
    raw = f"CONVERT(NVARCHAR(50), {alias}.{_q(col)})"
    return f"CASE WHEN ISNUMERIC({raw}) = 1 THEN CONVERT(INT, {alias}.{_q(col)}) ELSE NULL END"


def _decimal_expr(sql_expr: str) -> str:
    cleaned = f"NULLIF(REPLACE(REPLACE(CONVERT(NVARCHAR(100), {sql_expr}), ',', ''), ' ', ''), '')"
    return f"CASE WHEN ISNUMERIC({cleaned}) = 1 THEN CONVERT(decimal(18,4), {cleaned}) ELSE NULL END"


def _has_result_expr(alias: str = "r") -> str:
    return f"NULLIF(LTRIM(RTRIM(CONVERT(NVARCHAR(MAX), {alias}.Result))), N'') IS NOT NULL"


def _result_event_datetime_expr(meta: dict[str, Any] | None = None) -> str:
    result_cols = (meta or {}).get("schema", {}).get("dbo.labtestresultup", {})
    result_parts = []
    for col in ("ResultAuthDtTime", "UpdatedON", "InsertedON"):
        found = _col(result_cols, [col])
        if found:
            result_parts.append(f"r.{_q(found)}")
    if not result_parts:
        result_parts.append("r.ResultAuthDtTime")
    parts = [result_parts[0], "s.SmpGenDateTime", "om.OrdDateTime", *result_parts[1:]]
    return "COALESCE(" + ", ".join(parts) + ")"


def _result_only_datetime_expr(meta: dict[str, Any] | None = None) -> str:
    result_cols = (meta or {}).get("schema", {}).get("dbo.labtestresultup", {})
    parts = []
    for col in ("ResultAuthDtTime", "UpdatedON", "InsertedON"):
        found = _col(result_cols, [col])
        if found:
            parts.append(f"r.{_q(found)}")
    return "COALESCE(" + ", ".join(parts or ["r.ResultAuthDtTime"]) + ")"


def _active_expr(alias: str, col: str | None) -> str:
    if not col:
        return "1 = 1"
    txt = f"LOWER(LTRIM(RTRIM(CONVERT(NVARCHAR(40), ISNULL({alias}.{_q(col)}, 0)))))"
    return f"{txt} NOT IN ('1','true','yes','y','cancelled','canceled','cancel','inactive')"


def _metadata(conn) -> dict[str, Any]:
    try:
        db_name = conn.getinfo(16)
    except Exception:
        db_name = str(conn)
    cache_key = str(db_name or "").lower()
    now = monotonic()
    with _METADATA_LOCK:
        cached = _METADATA_CACHE.get(cache_key)
        if cached and now - cached[0] < _METADATA_TTL_SECONDS:
            return cached[1]
    schema = _schema(conn)
    patient_table = _resolve_table(schema, ["Patient", "Patient_Mst", "PatientMst", "M_Patient"])
    visit_table = _resolve_table(schema, ["Visit", "Visit_Mst", "PatientVisit"])
    doctor_table = _resolve_table(schema, ["Doctor_Mst", "DoctorMst", "Doctor", "Employee_Mst"])
    patient_type_table = _resolve_table(schema, ["PatientType_mst", "PatientType_Mst", "PatientType"])
    patient_subtype_table = _resolve_table(schema, ["PatientSubType_Mst", "PatientSubType_mst", "PatientSubType"])
    patient_cols = schema.get(patient_table or "", {})
    visit_cols = schema.get(visit_table or "", {})
    doctor_cols = schema.get(doctor_table or "", {})
    patient_type_cols = schema.get(patient_type_table or "", {})
    patient_subtype_cols = schema.get(patient_subtype_table or "", {})
    payload = {
        "schema": schema,
        "patient_table": patient_table,
        "visit_table": visit_table,
        "doctor_table": doctor_table,
        "patient_type_table": patient_type_table,
        "patient_subtype_table": patient_subtype_table,
        "patient_cols": patient_cols,
        "visit_cols": visit_cols,
        "doctor_cols": doctor_cols,
        "patient_type_cols": patient_type_cols,
        "patient_subtype_cols": patient_subtype_cols,
    }
    with _METADATA_LOCK:
        _METADATA_CACHE[cache_key] = (now, payload)
    return payload


def _resolve_patient_search_ids(conn, meta: dict[str, Any], search: str, limit: int = 25) -> list[int]:
    search = str(search or "").strip()
    if not search or not meta.get("patient_table"):
        return []
    patient_cols = meta.get("patient_cols") or {}
    pt_id_col = _col(patient_cols, ["Patient_ID", "PatientID", "PatientId", "patientId"])
    if not pt_id_col:
        return []
    reg_col = _col(patient_cols, ["Registration_No", "RegistrationNo", "RegNo", "UHID", "MRNo"])
    mobile_col = _col(patient_cols, ["Mobile", "MobileNo", "Mobile1", "Phone", "ContactNo", "CellNo"])
    clauses: list[str] = []
    params: list[Any] = []
    like = f"%{search.lower()}%"
    if search.isdigit():
        clauses.extend([
            f"CONVERT(NVARCHAR(50), pt.{_q(pt_id_col)}) = ?",
            f"LOWER({_text_expr('pt', reg_col)}) = ?",
            f"LOWER({_text_expr('pt', reg_col)}) LIKE ?",
        ])
        params.extend([search, search.lower(), f"%-{search.lower()}"])
        if len(search) >= 7:
            clauses.append(f"LOWER({_text_expr('pt', mobile_col)}) LIKE ?")
            params.append(like)
    else:
        clauses.extend([
            f"LOWER({_text_expr('pt', reg_col)}) LIKE ?",
            f"LOWER({_patient_name_expr(patient_cols)}) LIKE ?",
            f"LOWER({_text_expr('pt', mobile_col)}) LIKE ?",
        ])
        params.extend([like, like, like])
    sql = f"""
        SELECT TOP ({parse_positive_int(limit, 25, maximum=100)})
            pt.{_q(pt_id_col)} AS patient_id
        FROM {_table_ref(meta['patient_table'])} pt WITH (NOLOCK)
        WHERE {" OR ".join(f"({clause})" for clause in clauses)}
        ORDER BY pt.{_q(pt_id_col)}
    """
    cur = conn.cursor()
    cur.execute(sql, params)
    out: list[int] = []
    seen = set()
    for row in cur.fetchall():
        try:
            value = int(row[0])
        except Exception:
            continue
        if value not in seen:
            out.append(value)
            seen.add(value)
    return out


def _fetch_patient_details(conn, meta: dict[str, Any], patient_ids: list[int]) -> list[dict[str, Any]]:
    patient_ids = validate_int_list(patient_ids)
    if not patient_ids or not meta.get("patient_table"):
        return []
    patient_cols = meta.get("patient_cols") or {}
    pt_id_col = _col(patient_cols, ["Patient_ID", "PatientID", "PatientId", "patientId"])
    if not pt_id_col:
        return []
    reg_col = _col(patient_cols, ["Registration_No", "RegistrationNo", "RegNo", "UHID", "MRNo"])
    mobile_col = _col(patient_cols, ["Mobile", "MobileNo", "Mobile1", "Phone", "ContactNo", "CellNo"])
    age_col = _col(patient_cols, ["Age", "AgeYears", "Age_Years", "AgeInYears", "PatientAge"])
    sql = f"""
        SELECT
            pt.{_q(pt_id_col)} AS patient_id,
            {_text_expr('pt', reg_col)} AS registration_no,
            {_patient_name_expr(patient_cols)} AS patient_name,
            {_text_expr('pt', age_col)} AS patient_age,
            {_text_expr('pt', mobile_col)} AS mobile
        FROM {_table_ref(meta['patient_table'])} pt WITH (NOLOCK)
        WHERE pt.{_q(pt_id_col)} IN ({','.join('?' for _ in patient_ids)})
        ORDER BY pt.{_q(pt_id_col)}
    """
    cur = conn.cursor()
    cur.execute(sql, patient_ids)
    return _fetch_all_dicts(cur)


def _patient_name_expr(patient_cols: dict[str, str]) -> str:
    patient_name = _col(patient_cols, ["PatientName", "Patient_Name", "Name", "Patient"])
    first = _col(patient_cols, ["FirstName", "First_Name", "FName"])
    middle = _col(patient_cols, ["MiddleName", "Middle_Name", "MName"])
    last = _col(patient_cols, ["LastName", "Last_Name", "LName", "SurName", "Surname"])
    if patient_name:
        return _text_expr("pt", patient_name)
    parts = [_text_expr("pt", first), _text_expr("pt", middle), _text_expr("pt", last)]
    return f"LTRIM(RTRIM(CONCAT({parts[0]}, N' ', {parts[1]}, N' ', {parts[2]})))"


def _doctor_name_expr(meta: dict[str, Any]) -> str:
    sample_doctor = "NULLIF(LTRIM(RTRIM(ISNULL(CONVERT(NVARCHAR(250), s.Doctorname), N''))), N'')"
    doctor_cols = meta["doctor_cols"]
    name_col = _col(doctor_cols, ["DoctorName", "Doctor_Name", "Name", "EmpName", "EmployeeName"])
    if meta["doctor_table"] and name_col:
        return f"COALESCE({sample_doctor}, {_text_expr('doc', name_col)})"
    return f"ISNULL({sample_doctor}, N'')"


def _base_sql(
    meta: dict[str, Any],
    where_sql: str,
    order_sql: str,
    page_sql: str = "",
    *,
    from_clause: str = "FROM dbo.LABTestResultup r WITH (NOLOCK)",
    cte_prefix: str = "",
    direct_patient_visit_join: bool = False,
) -> str:
    patient_join = ""
    visit_join = ""
    type_join = ""
    doctor_join = ""
    patient_cols = meta["patient_cols"]
    visit_cols = meta["visit_cols"]
    pt_id_col = _col(patient_cols, ["Patient_ID", "PatientID", "PatientId", "patientId"])
    reg_col = _col(patient_cols, ["Registration_No", "RegistrationNo", "RegNo", "UHID", "MRNo"])
    mobile_col = _col(patient_cols, ["Mobile", "MobileNo", "Mobile1", "Phone", "ContactNo", "CellNo"])
    age_col = _col(patient_cols, ["Age", "AgeYears", "Age_Years", "AgeInYears", "PatientAge"])
    visit_id_col = _col(visit_cols, ["Visit_ID", "VisitID", "VisitId", "visitId"])
    visit_no_col = _col(visit_cols, ["VisitNo", "Visit_No", "VisitNumber"])
    visit_date_col = _col(visit_cols, ["VisitDate", "Visit_Date", "AdmissionDate", "AdmitDate"])
    visit_type_col = _col(visit_cols, ["TypeOfVisit", "VisitType", "Visit_Type"])
    patient_type_col = _col(visit_cols, ["PatientType", "Patient_Type", "TypeOfPatient"])
    patient_subtype_col = _col(visit_cols, ["PatientSubType", "PatientSubTypeName", "PatientSubType_Desc"])
    patient_type_id_col = _col(visit_cols, ["PatientType_ID", "PatientTypeId", "PatientTypeID"])
    patient_subtype_id_col = _col(visit_cols, ["PatientSubType_ID", "PatientSubTypeId", "PatientSubTypeID"])
    ptype_id_col = _col(meta["patient_type_cols"], ["PatientType_ID", "PatientTypeId", "PatientTypeID", "TypeID", "TypeId", "Id"])
    ptype_name_col = _col(meta["patient_type_cols"], ["PatientType", "PatientTypeName", "TypeName", "Name"])
    psubtype_id_col = _col(meta["patient_subtype_cols"], ["PatientSubType_ID", "PatientSubTypeId", "PatientSubTypeID", "SubTypeID", "SubTypeId", "Id"])
    psubtype_name_col = _col(meta["patient_subtype_cols"], ["PatientSubType_Desc", "PatientSubTypeDesc", "PatientSubTypeName", "PatientSubType", "SubTypeName", "Name"])
    doctor_visit_col = _col(visit_cols, ["DocInCharge", "DoctorInCharge", "DoctorID", "DoctorId", "OrdByDocID"])
    doctor_id_col = _col(meta["doctor_cols"], ["Doctor_ID", "DoctorID", "DoctorId", "DocID", "ID", "EmpID"]) if meta["doctor_table"] else None
    result_cols = meta["schema"].get("dbo.labtestresultup", {})
    machine_col = _col(result_cols, ["MachineName", "Machine_Name", "Machine"])

    if meta["patient_table"] and pt_id_col:
        patient_key = "r.PatientID" if direct_patient_visit_join else "COALESCE(r.PatientID, s.PatientID, om.OrdPatientID)"
        patient_join = (
            f"LEFT JOIN {_table_ref(meta['patient_table'])} pt WITH (NOLOCK) "
            f"ON {patient_key} = pt.{_q(pt_id_col)}"
        )
    else:
        patient_cols = {}
        reg_col = None
        mobile_col = None
        age_col = None
    if meta["visit_table"] and visit_id_col:
        visit_key = "r.PatientVisitID" if direct_patient_visit_join else "COALESCE(r.PatientVisitID, s.VisitID, om.OrdVisitID)"
        visit_join = (
            f"LEFT JOIN {_table_ref(meta['visit_table'])} v WITH (NOLOCK) "
            f"ON {visit_key} = v.{_q(visit_id_col)}"
        )
    else:
        visit_cols = {}
        visit_no_col = None
        visit_date_col = None
        visit_type_col = None
        patient_type_col = None
        patient_subtype_col = None
        patient_type_id_col = None
        patient_subtype_id_col = None
        doctor_visit_col = None
    if meta["doctor_table"] and doctor_id_col:
        doctor_key = f"COALESCE(om.OrdByDocID, od.DocID, s.Doctid, {_int_expr('v', doctor_visit_col)})"
        doctor_join = f"LEFT JOIN {_table_ref(meta['doctor_table'])} doc WITH (NOLOCK) ON {doctor_key} = doc.{_q(doctor_id_col)}"
    if meta["patient_type_table"] and patient_type_id_col and ptype_id_col:
        type_join += (
            f"\n        LEFT JOIN {_table_ref(meta['patient_type_table'])} ptyp WITH (NOLOCK) "
            f"ON ptyp.{_q(ptype_id_col)} = v.{_q(patient_type_id_col)}"
        )
    if meta["patient_subtype_table"] and patient_subtype_id_col and psubtype_id_col:
        type_join += (
            f"\n        LEFT JOIN {_table_ref(meta['patient_subtype_table'])} pstyp WITH (NOLOCK) "
            f"ON pstyp.{_q(psubtype_id_col)} = v.{_q(patient_subtype_id_col)}"
        )

    visit_date = _date_expr("v", visit_date_col)
    patient_type_expr = f"COALESCE(NULLIF({_text_expr('ptyp', ptype_name_col)}, N''), NULLIF({_text_expr('v', patient_type_col)}, N''), NULLIF({_text_expr('v', patient_type_id_col)}, N''), N'')"
    patient_subtype_expr = f"COALESCE(NULLIF({_text_expr('pstyp', psubtype_name_col)}, N''), NULLIF({_text_expr('v', patient_subtype_col)}, N''), NULLIF({_text_expr('v', patient_subtype_id_col)}, N''), N'')"
    prefix = f"{cte_prefix}\n        " if cte_prefix else ""
    return f"""
    ;WITH {prefix}base AS (
        SELECT
            COALESCE(r.PatientID, s.PatientID, om.OrdPatientID) AS patient_id,
            {_text_expr('pt', reg_col)} AS registration_no,
            {_patient_name_expr(patient_cols)} AS patient_name,
            {_text_expr('pt', age_col)} AS patient_age,
            {_text_expr('pt', mobile_col)} AS mobile,
            COALESCE(r.PatientVisitID, s.VisitID, om.OrdVisitID) AS visit_id,
            {_text_expr('v', visit_no_col)} AS visit_no,
            COALESCE({visit_date}, {_result_event_datetime_expr(meta)}) AS visit_date,
            COALESCE(NULLIF({_text_expr('v', visit_type_col)}, N''), NULLIF(CONVERT(NVARCHAR(50), om.VisitType), N''), N'') AS visit_type,
            {patient_type_expr} AS patient_type,
            {patient_subtype_expr} AS patient_subtype,
            om.OrdId AS order_id,
            CONVERT(NVARCHAR(80), om.OrdNo) AS order_no,
            om.OrdDateTime AS order_datetime,
            s.SampleID AS sample_id,
            CONVERT(NVARCHAR(80), s.SampleNo) AS sample_no,
            s.SmpGenDateTime AS sample_datetime,
            s.SampleAccptDtTime AS sample_accept_datetime,
            r.TestID AS test_id,
            COALESCE(NULLIF(t.test_name, N''), NULLIF(sm.Service_Name, N''), CONVERT(NVARCHAR(50), r.TestID)) AS test_name,
            sm.Service_ID AS service_id,
            sm.Service_Name AS service_name,
            sm.Category_Id AS service_category_id,
            r.ParamID AS parameter_id,
            COALESCE(NULLIF(p.parameter_name, N''), CONVERT(NVARCHAR(50), r.ParamID)) AS parameter_name,
            r.Result AS result,
            p.unit_name AS unit,
            p.normal_range AS normal_range,
            p.low_value AS low_value,
            p.high_value AS high_value,
            r.ParamLow AS param_low,
            r.ParamHigh AS param_high,
            r.AbnormalFlag AS abnormal_flag,
            r.ResultAuthFlag AS result_auth_flag,
            r.ResultAuthBy AS result_auth_by,
            r.ResultAuthDtTime AS result_auth_datetime,
            {_text_expr('r', machine_col)} AS machine_name,
            {_doctor_name_expr(meta)} AS doctor_name,
            ROW_NUMBER() OVER ({order_sql}) AS rn,
            CAST(0 AS INT) AS total_rows
        {from_clause}
        LEFT JOIN dbo.lab_test_update t WITH (NOLOCK) ON r.TestID = t.test_id
        LEFT JOIN dbo.lab_parameter_update p WITH (NOLOCK) ON r.ParamID = p.id AND r.TestID = p.test_id
        LEFT JOIN dbo.LABSampleup s WITH (NOLOCK) ON r.SampleID = s.SampleID
        LEFT JOIN dbo.OrderMst om WITH (NOLOCK) ON r.OrderID = om.OrdId
        LEFT JOIN dbo.OrderDtl od WITH (NOLOCK) ON r.OrderDtlID = od.OrdDtlID
        LEFT JOIN dbo.service_mst sm WITH (NOLOCK) ON COALESCE(od.ServiceId, t.service_id) = sm.Service_ID
        {patient_join}
        {visit_join}
        {type_join}
        {doctor_join}
        WHERE {_active_expr('om', 'OrdCanceled')} AND {_active_expr('od', 'Cancelled')} AND {_has_result_expr('r')} {where_sql}
    )
    SELECT * FROM base
    {page_sql}
    ORDER BY rn;
    """


def _result_status_sql(status: str) -> str:
    if status == "all":
        return ""
    numeric = _decimal_expr("r.Result")
    low = _decimal_expr("COALESCE(r.ParamLow, p.low_value)")
    high = _decimal_expr("COALESCE(r.ParamHigh, p.high_value)")
    flag = "LOWER(LTRIM(RTRIM(CONVERT(NVARCHAR(50), ISNULL(r.AbnormalFlag, N'')))))"
    abnormal_flags = f"{flag} IN ('1','true','yes','y','a','abnormal','h','high','l','low','critical','c','panic')"
    if status == "abnormal":
        return f" AND ({abnormal_flags} OR ({numeric} IS NOT NULL AND (({low} IS NOT NULL AND {numeric} < {low}) OR ({high} IS NOT NULL AND {numeric} > {high}))))"
    if status == "high":
        return f" AND ({flag} IN ('h','high') OR ({numeric} IS NOT NULL AND {high} IS NOT NULL AND {numeric} > {high}))"
    if status == "low":
        return f" AND ({flag} IN ('l','low') OR ({numeric} IS NOT NULL AND {low} IS NOT NULL AND {numeric} < {low}))"
    if status == "normal":
        return f" AND NOT ({abnormal_flags}) AND {numeric} IS NOT NULL AND ({low} IS NULL OR {numeric} >= {low}) AND ({high} IS NULL OR {numeric} <= {high})"
    if status == "critical":
        return f" AND {flag} IN ('critical','c','panic')"
    if status == "unclassified":
        return f" AND NOT ({abnormal_flags}) AND ({numeric} IS NULL OR ({low} IS NULL AND {high} IS NULL))"
    return ""


def _build_filters(filters: dict[str, Any], params: list[Any], meta: dict[str, Any] | None = None) -> str:
    where = []
    test_ids = validate_int_list(filters.get("test_ids"))
    if test_ids:
        where.append(f"r.TestID IN ({','.join('?' for _ in test_ids)})")
        params.extend(test_ids)
    parameter_ids = validate_int_list(filters.get("parameter_ids"))
    if parameter_ids:
        where.append(f"r.ParamID IN ({','.join('?' for _ in parameter_ids)})")
        params.extend(parameter_ids)
    from_date = filters.get("from_date")
    to_date = filters.get("to_date")
    if from_date:
        where.append(f"{_result_event_datetime_expr(meta)} >= ?")
        params.append(str(from_date))
    if to_date:
        where.append(f"{_result_event_datetime_expr(meta)} < DATEADD(day, 1, ?)")
        params.append(str(to_date))
    auth = filters.get("auth_status")
    if auth == "authorized":
        where.append("LOWER(LTRIM(RTRIM(CONVERT(NVARCHAR(50), ISNULL(r.ResultAuthFlag, N''))))) IN ('1','true','yes','y','authorized','authorised','a')")
    elif auth == "pending":
        where.append("LOWER(LTRIM(RTRIM(CONVERT(NVARCHAR(50), ISNULL(r.ResultAuthFlag, N''))))) NOT IN ('1','true','yes','y','authorized','authorised','a')")
    result_status = filters.get("result_status") or "all"
    if result_status != "all":
        where.append(_result_status_sql(result_status).replace(" AND ", "", 1))
    visit_types = [str(v).strip().upper() for v in (filters.get("visit_types") or []) if str(v).strip()]
    if visit_types:
        visit_type_col = _col((meta or {}).get("visit_cols"), ["TypeOfVisit", "VisitType", "Visit_Type"])
        visit_type_expr = f"COALESCE({_text_expr('v', visit_type_col)}, CONVERT(NVARCHAR(50), om.VisitType), N'')"
        expanded_visit_types = _expand_visit_type_values(visit_types)
        where.append(f"UPPER({visit_type_expr}) IN ({','.join('?' for _ in expanded_visit_types)})")
        params.extend(expanded_visit_types)
    patient_id = filters.get("patient_id")
    if patient_id:
        where.append("COALESCE(r.PatientID, s.PatientID, om.OrdPatientID) = ?")
        params.append(int(patient_id))
    resolved_patient_ids = validate_int_list(filters.get("_resolved_patient_ids"))
    if resolved_patient_ids:
        where.append(f"COALESCE(r.PatientID, s.PatientID, om.OrdPatientID) IN ({','.join('?' for _ in resolved_patient_ids)})")
        params.extend(resolved_patient_ids)
    search = str(filters.get("patient_search") or "").strip()
    if search and not patient_id and not resolved_patient_ids:
        patient_cols = (meta or {}).get("patient_cols") or {}
        visit_cols = (meta or {}).get("visit_cols") or {}
        reg_col = _col(patient_cols, ["Registration_No", "RegistrationNo", "RegNo", "UHID", "MRNo"])
        mobile_col = _col(patient_cols, ["Mobile", "MobileNo", "Mobile1", "Phone", "ContactNo", "CellNo"])
        visit_no_col = _col(visit_cols, ["VisitNo", "Visit_No", "VisitNumber"])
        like = f"%{search.lower()}%"
        compact = f"%{search.lower().replace('-', '').replace('/', '').replace(' ', '')}%"
        if search.isdigit():
            numeric_clauses = [
                "CONVERT(NVARCHAR(50), COALESCE(r.PatientID, s.PatientID, om.OrdPatientID)) = ?",
                f"LOWER({_text_expr('pt', reg_col)}) = ?",
                f"LOWER({_text_expr('pt', reg_col)}) LIKE ?",
                f"LOWER({_text_expr('v', visit_no_col)}) LIKE ?",
            ]
            numeric_params = [search, search.lower(), f"%-{search.lower()}", f"%/{search.lower()}"]
            if len(search) >= 7:
                numeric_clauses.append(f"LOWER({_text_expr('pt', mobile_col)}) LIKE ?")
                numeric_params.append(like)
            where.append("(" + " OR ".join(numeric_clauses) + ")")
            params.extend(numeric_params)
        else:
            clauses = [
                f"LOWER({_text_expr('pt', reg_col)}) LIKE ?",
                f"LOWER({_patient_name_expr(patient_cols)}) LIKE ?",
                f"LOWER({_text_expr('pt', mobile_col)}) LIKE ?",
                f"LOWER({_text_expr('v', visit_no_col)}) LIKE ?",
                f"REPLACE(REPLACE(REPLACE(LOWER({_text_expr('pt', reg_col)}), N'-', N''), N'/', N''), N' ', N'') LIKE ?",
            ]
            where.append("(" + " OR ".join(clauses) + ")")
            params.extend([like, like, like, like, compact])
    return (" AND " + " AND ".join(f"({w})" for w in where)) if where else ""


def _expand_visit_type_values(visit_types: list[str]) -> list[str]:
    aliases = {
        "OPD": ["OPD", "OUT PATIENT VISITS", "OUT PATIENT VISIT", "OUTPATIENT", "OUT PATIENT"],
        "IPD": ["IPD", "IN PATIENT VISITS", "IN PATIENT VISIT", "INPATIENT", "IN PATIENT"],
        "DPV": ["DPV", "DIAGNOSTIC VISIT", "DIAGNOSTIC VISITS"],
        "HCV": ["HCV", "HEALTH CHECKUPS", "HEALTH CHECKUP", "HEALTH CHECK-UP"],
    }
    out: list[str] = []
    seen = set()
    for item in visit_types:
        for value in aliases.get(item, [item]):
            norm = str(value or "").strip().upper()
            if norm and norm not in seen:
                out.append(norm)
                seen.add(norm)
    return out


def fetch_test_master(unit: str, search: str = "", limit: int = 300) -> list[dict[str, Any]]:
    conn = get_sql_connection(unit)
    if not conn:
        raise RuntimeError("Database connection failed")
    try:
        limit = parse_positive_int(limit, 300, maximum=1000)
        schema = _schema(conn)
        test_cols = schema.get("dbo.lab_test_update", {})
        service_cols = schema.get("dbo.service_mst", {})
        result_tag_col = _col(test_cols, ["resultTag", "ResultTag", "Result_Tag"])
        profile_col = _col(test_cols, ["profile_id", "ProfileID", "ProfileId"])
        print_order_col = _col(test_cols, ["print_order", "PrintOrder", "printorder"])
        service_category_col = _col(service_cols, ["Category_Id", "CategoryID", "CategoryId"])
        deactive_col = _col(service_cols, ["Deactive", "DeActive", "IsDeactive", "IsActive"])
        result_tag_expr = f"t.{_q(result_tag_col)}" if result_tag_col else "CAST(NULL AS NVARCHAR(100))"
        profile_expr = f"t.{_q(profile_col)}" if profile_col else "CAST(NULL AS INT)"
        print_order_expr = f"t.{_q(print_order_col)}" if print_order_col else "CAST(NULL AS INT)"
        category_expr = f"sm.{_q(service_category_col)}" if service_category_col else "CAST(NULL AS INT)"
        params: list[Any] = []
        if deactive_col and deactive_col.lower() == "isactive":
            where = f"WHERE ISNULL(sm.{_q(deactive_col)}, 1) = 1"
        elif deactive_col:
            where = f"WHERE ISNULL(sm.{_q(deactive_col)}, 0) = 0"
        else:
            where = "WHERE 1 = 1"
        if search:
            where += " AND (LOWER(t.test_name) LIKE ? OR LOWER(sm.Service_Name) LIKE ? OR CONVERT(NVARCHAR(30), t.test_id) LIKE ?)"
            like = f"%{search.lower()}%"
            params.extend([like, like, like])
        sql = f"""
        SELECT TOP ({limit})
            t.test_id, t.test_name, t.service_id, sm.Service_Name AS service_name,
            {category_expr} AS category_id, {profile_expr} AS profile_id,
            {print_order_expr} AS print_order, {result_tag_expr} AS result_tag
        FROM dbo.lab_test_update t WITH (NOLOCK)
        LEFT JOIN dbo.service_mst sm WITH (NOLOCK) ON t.service_id = sm.Service_ID
        {where}
        ORDER BY ISNULL(sm.Service_Name, t.test_name), t.test_name
        """
        cur = conn.cursor()
        cur.execute(sql, params)
        return _fetch_all_dicts(cur)
    finally:
        conn.close()


def fetch_parameter_master(unit: str, test_ids: list[int]) -> list[dict[str, Any]]:
    conn = get_sql_connection(unit)
    if not conn:
        raise RuntimeError("Database connection failed")
    try:
        params: list[Any] = []
        where = ""
        test_ids = validate_int_list(test_ids)
        if test_ids:
            where = f"WHERE p.test_id IN ({','.join('?' for _ in test_ids)})"
            params.extend(test_ids)
        sql = f"""
        SELECT TOP (2000)
            p.id AS parameter_id, p.test_id, p.test_name, p.parameter_name,
            p.unit_name, p.normal_range, p.low_value, p.high_value, p.printorder
        FROM dbo.lab_parameter_update p WITH (NOLOCK)
        {where}
        ORDER BY p.test_name, ISNULL(p.printorder, 9999), p.parameter_name
        """
        cur = conn.cursor()
        cur.execute(sql, params)
        return _fetch_all_dicts(cur)
    finally:
        conn.close()


def fetch_patient_test_suggestions(unit: str, filters: dict[str, Any], limit: int = 80) -> dict[str, Any]:
    conn = get_sql_connection(unit)
    if not conn:
        raise RuntimeError("Database connection failed")
    try:
        meta = _metadata(conn)
        search = str(filters.get("patient_search") or "").strip()
        patient_ids = validate_int_list(filters.get("patient_id"))
        if not patient_ids and search:
            patient_ids = _resolve_patient_search_ids(conn, meta, search, limit=10)
        if not patient_ids:
            return {"patient_ids": [], "patients": [], "rows": []}
        patient_details = _fetch_patient_details(conn, meta, patient_ids)

        params: list[Any] = list(patient_ids)
        where = [
            _has_result_expr("r"),
            _active_expr("om", "OrdCanceled"),
            _active_expr("od", "Cancelled"),
            f"COALESCE(r.PatientID, s.PatientID, om.OrdPatientID) IN ({','.join('?' for _ in patient_ids)})",
        ]
        from_date = filters.get("from_date")
        to_date = filters.get("to_date")
        if from_date:
            where.append(f"{_result_event_datetime_expr(meta)} >= ?")
            params.append(str(from_date))
        if to_date:
            where.append(f"{_result_event_datetime_expr(meta)} < DATEADD(day, 1, ?)")
            params.append(str(to_date))
        visit_types = [str(v).strip().upper() for v in (filters.get("visit_types") or []) if str(v).strip()]
        limit = parse_positive_int(limit, 80, maximum=200)
        if not visit_types:
            params = list(patient_ids)
            where = [
                _has_result_expr("r"),
                f"r.PatientID IN ({','.join('?' for _ in patient_ids)})",
            ]
            from_date = filters.get("from_date")
            to_date = filters.get("to_date")
            if from_date:
                where.append(f"{_result_only_datetime_expr(meta)} >= ?")
                params.append(str(from_date))
            if to_date:
                where.append(f"{_result_only_datetime_expr(meta)} < DATEADD(day, 1, ?)")
                params.append(str(to_date))
            sql = f"""
            SELECT TOP ({limit})
                r.TestID AS test_id,
                COALESCE(NULLIF(t.test_name, N''), NULLIF(sm.Service_Name, N''), CONVERT(NVARCHAR(50), r.TestID)) AS test_name,
                sm.Service_Name AS service_name,
                COUNT(1) AS result_count,
                COUNT(DISTINCT CONCAT(
                    ISNULL(CONVERT(NVARCHAR(50), r.PatientVisitID), N''), N'|',
                    ISNULL(CONVERT(NVARCHAR(50), r.OrderID), N''), N'|',
                    ISNULL(CONVERT(NVARCHAR(50), r.SampleID), N''), N'|',
                    ISNULL(CONVERT(NVARCHAR(50), {_result_only_datetime_expr(meta)}, 120), N'')
                )) AS iteration_count,
                MAX({_result_only_datetime_expr(meta)}) AS last_test_date
            FROM dbo.LABTestResultup r WITH (NOLOCK)
            LEFT JOIN dbo.lab_test_update t WITH (NOLOCK) ON r.TestID = t.test_id
            LEFT JOIN dbo.service_mst sm WITH (NOLOCK) ON t.service_id = sm.Service_ID
            WHERE {" AND ".join(f"({clause})" for clause in where)}
            GROUP BY r.TestID, COALESCE(NULLIF(t.test_name, N''), NULLIF(sm.Service_Name, N''), CONVERT(NVARCHAR(50), r.TestID)), sm.Service_Name
            ORDER BY MAX({_result_only_datetime_expr(meta)}) DESC, COUNT(DISTINCT r.PatientVisitID) DESC, test_name
            """
            cur = conn.cursor()
            cur.execute(sql, params)
            return {"patient_ids": patient_ids, "patients": patient_details, "rows": _fetch_all_dicts(cur)}

        visit_join = ""
        if visit_types:
            visit_type_col = _col(meta.get("visit_cols"), ["TypeOfVisit", "VisitType", "Visit_Type"])
            visit_type_expr = f"COALESCE({_text_expr('v', visit_type_col)}, CONVERT(NVARCHAR(50), om.VisitType), N'')"
            expanded_visit_types = _expand_visit_type_values(visit_types)
            where.append(f"UPPER({visit_type_expr}) IN ({','.join('?' for _ in expanded_visit_types)})")
            params.extend(expanded_visit_types)
        if meta.get("visit_table"):
            visit_id_col = _col(meta.get("visit_cols"), ["Visit_ID", "VisitID", "VisitId"]) or "Visit_ID"
            visit_join = (
                f"LEFT JOIN {_table_ref(meta['visit_table'])} v WITH (NOLOCK) "
                f"ON COALESCE(r.PatientVisitID, s.VisitID, om.OrdVisitID) = v.{_q(visit_id_col)}"
            )

        sql = f"""
        SELECT TOP ({limit})
            r.TestID AS test_id,
            COALESCE(NULLIF(t.test_name, N''), NULLIF(sm.Service_Name, N''), CONVERT(NVARCHAR(50), r.TestID)) AS test_name,
            sm.Service_Name AS service_name,
            COUNT(1) AS result_count,
            COUNT(DISTINCT CONCAT(
                ISNULL(CONVERT(NVARCHAR(50), COALESCE(r.PatientVisitID, s.VisitID, om.OrdVisitID)), N''), N'|',
                ISNULL(CONVERT(NVARCHAR(50), r.OrderID), N''), N'|',
                ISNULL(CONVERT(NVARCHAR(50), r.SampleID), N''), N'|',
                ISNULL(CONVERT(NVARCHAR(50), {_result_event_datetime_expr(meta)}, 120), N'')
            )) AS iteration_count,
            MAX({_result_event_datetime_expr(meta)}) AS last_test_date
        FROM dbo.LABTestResultup r WITH (NOLOCK)
        LEFT JOIN dbo.lab_test_update t WITH (NOLOCK) ON r.TestID = t.test_id
        LEFT JOIN dbo.LABSampleup s WITH (NOLOCK) ON r.SampleID = s.SampleID
        LEFT JOIN dbo.OrderMst om WITH (NOLOCK) ON r.OrderID = om.OrdId
        LEFT JOIN dbo.OrderDtl od WITH (NOLOCK) ON r.OrderDtlID = od.OrdDtlID
        LEFT JOIN dbo.service_mst sm WITH (NOLOCK) ON COALESCE(od.ServiceId, t.service_id) = sm.Service_ID
        {visit_join}
        WHERE {" AND ".join(f"({clause})" for clause in where)}
        GROUP BY r.TestID, COALESCE(NULLIF(t.test_name, N''), NULLIF(sm.Service_Name, N''), CONVERT(NVARCHAR(50), r.TestID)), sm.Service_Name
        ORDER BY MAX({_result_event_datetime_expr(meta)}) DESC, iteration_count DESC, test_name
        """
        cur = conn.cursor()
        cur.execute(sql, params)
        return {"patient_ids": patient_ids, "patients": patient_details, "rows": _fetch_all_dicts(cur)}
    finally:
        conn.close()


def search_patients(unit: str, query: str, limit: int = 50) -> list[dict[str, Any]]:
    conn = get_sql_connection(unit)
    if not conn:
        raise RuntimeError("Database connection failed")
    try:
        meta = _metadata(conn)
        patient_table = meta["patient_table"]
        visit_table = meta["visit_table"]
        if not patient_table or not visit_table:
            return []
        pc = meta["patient_cols"]
        vc = meta["visit_cols"]
        pt_id = _col(pc, ["Patient_ID", "PatientID", "PatientId"])
        reg = _col(pc, ["Registration_No", "RegistrationNo", "RegNo", "UHID"])
        mobile = _col(pc, ["Mobile", "MobileNo", "Mobile1", "Phone", "ContactNo"])
        v_id = _col(vc, ["Visit_ID", "VisitID", "VisitId"])
        v_no = _col(vc, ["VisitNo", "Visit_No", "VisitNumber"])
        v_date = _col(vc, ["VisitDate", "Visit_Date", "AdmissionDate"])
        v_patient = _col(vc, ["PatientID", "PatientId", "Patient_ID"])
        if not all([pt_id, v_id, v_patient]):
            return []
        like = f"%{query.strip().lower()}%"
        compact = f"%{query.strip().lower().replace('-', '').replace('/', '').replace(' ', '')}%"
        sql = f"""
        SELECT TOP ({parse_positive_int(limit, 50, maximum=200)})
            pt.{_q(pt_id)} AS patient_id,
            {_text_expr('pt', reg)} AS registration_no,
            {_patient_name_expr(pc)} AS patient_name,
            {_text_expr('pt', mobile)} AS mobile,
            v.{_q(v_id)} AS latest_visit_id,
            {_text_expr('v', v_no)} AS latest_visit_no,
            {_date_expr('v', v_date)} AS latest_visit_date
        FROM {_table_ref(patient_table)} pt WITH (NOLOCK)
        LEFT JOIN {_table_ref(visit_table)} v WITH (NOLOCK) ON pt.{_q(pt_id)} = v.{_q(v_patient)}
        WHERE LOWER({_text_expr('pt', reg)}) LIKE ?
           OR LOWER({_patient_name_expr(pc)}) LIKE ?
           OR LOWER({_text_expr('pt', mobile)}) LIKE ?
           OR LOWER({_text_expr('v', v_no)}) LIKE ?
           OR REPLACE(REPLACE(REPLACE(LOWER({_text_expr('pt', reg)}), N'-', N''), N'/', N''), N' ', N'') LIKE ?
        ORDER BY {_date_expr('v', v_date)} DESC, v.{_q(v_id)} DESC
        """
        cur = conn.cursor()
        cur.execute(sql, [like, like, like, like, compact])
        return _fetch_all_dicts(cur)
    finally:
        conn.close()


def _build_seed_query_parts(
    filters: dict[str, Any],
    meta: dict[str, Any] | None = None,
    *,
    include_date_filter: bool = True,
    direct_result_patient: bool = False,
) -> tuple[str, str, list[Any]]:
    if direct_result_patient:
        seed_from = "FROM dbo.LABTestResultup r WITH (NOLOCK)"
        clauses = [_has_result_expr("r")]
        params: list[Any] = []
        test_ids = validate_int_list(filters.get("test_ids"))
        if test_ids:
            clauses.append(f"r.TestID IN ({','.join('?' for _ in test_ids)})")
            params.extend(test_ids)
        parameter_ids = validate_int_list(filters.get("parameter_ids"))
        if parameter_ids:
            clauses.append(f"r.ParamID IN ({','.join('?' for _ in parameter_ids)})")
            params.extend(parameter_ids)
        patient_id = filters.get("patient_id")
        resolved_patient_ids = validate_int_list(filters.get("_resolved_patient_ids"))
        if patient_id:
            clauses.append("r.PatientID = ?")
            params.append(int(patient_id))
        elif resolved_patient_ids:
            clauses.append(f"r.PatientID IN ({','.join('?' for _ in resolved_patient_ids)})")
            params.extend(resolved_patient_ids)
        if include_date_filter:
            from_date = filters.get("from_date")
            to_date = filters.get("to_date")
            if from_date:
                clauses.append(f"{_result_only_datetime_expr(meta)} >= ?")
                params.append(str(from_date))
            if to_date:
                clauses.append(f"{_result_only_datetime_expr(meta)} < DATEADD(day, 1, ?)")
                params.append(str(to_date))
        return seed_from, ("WHERE " + " AND ".join(clauses)) if clauses else "", params

    seed_from = """
        FROM dbo.LABTestResultup r WITH (NOLOCK)
        LEFT JOIN dbo.LABSampleup s WITH (NOLOCK) ON r.SampleID = s.SampleID
        LEFT JOIN dbo.OrderMst om WITH (NOLOCK) ON r.OrderID = om.OrdId
        LEFT JOIN dbo.OrderDtl od WITH (NOLOCK) ON r.OrderDtlID = od.OrdDtlID
    """
    patient_join = ""
    visit_join = ""
    if meta:
        patient_cols = meta.get("patient_cols") or {}
        visit_cols = meta.get("visit_cols") or {}
        pt_id_col = _col(patient_cols, ["Patient_ID", "PatientID", "PatientId", "patientId"])
        visit_id_col = _col(visit_cols, ["Visit_ID", "VisitID", "VisitId", "visitId"])
        if meta.get("patient_table") and pt_id_col:
            patient_join = (
                f" LEFT JOIN {_table_ref(meta['patient_table'])} pt WITH (NOLOCK) "
                f"ON COALESCE(r.PatientID, s.PatientID, om.OrdPatientID) = pt.{_q(pt_id_col)}"
            )
        if meta.get("visit_table") and visit_id_col:
            visit_join = (
                f" LEFT JOIN {_table_ref(meta['visit_table'])} v WITH (NOLOCK) "
                f"ON COALESCE(r.PatientVisitID, s.VisitID, om.OrdVisitID) = v.{_q(visit_id_col)}"
            )
    seed_from = seed_from + patient_join + visit_join
    clauses = [_has_result_expr("r"), _active_expr("om", "OrdCanceled"), _active_expr("od", "Cancelled")]
    params: list[Any] = []
    test_ids = validate_int_list(filters.get("test_ids"))
    if test_ids:
        clauses.append(f"r.TestID IN ({','.join('?' for _ in test_ids)})")
        params.extend(test_ids)
    parameter_ids = validate_int_list(filters.get("parameter_ids"))
    if parameter_ids:
        clauses.append(f"r.ParamID IN ({','.join('?' for _ in parameter_ids)})")
        params.extend(parameter_ids)
    patient_id = filters.get("patient_id")
    if patient_id:
        clauses.append("r.PatientID = ?")
        params.append(int(patient_id))
    resolved_patient_ids = validate_int_list(filters.get("_resolved_patient_ids"))
    if resolved_patient_ids:
        clauses.append(f"COALESCE(r.PatientID, s.PatientID, om.OrdPatientID) IN ({','.join('?' for _ in resolved_patient_ids)})")
        params.extend(resolved_patient_ids)
    search = str(filters.get("patient_search") or "").strip()
    if search and not patient_id and not resolved_patient_ids:
        patient_cols = (meta or {}).get("patient_cols") or {}
        visit_cols = (meta or {}).get("visit_cols") or {}
        reg_col = _col(patient_cols, ["Registration_No", "RegistrationNo", "RegNo", "UHID", "MRNo"])
        mobile_col = _col(patient_cols, ["Mobile", "MobileNo", "Mobile1", "Phone", "ContactNo", "CellNo"])
        visit_no_col = _col(visit_cols, ["VisitNo", "Visit_No", "VisitNumber"])
        like = f"%{search.lower()}%"
        compact = f"%{search.lower().replace('-', '').replace('/', '').replace(' ', '')}%"
        if search.isdigit():
            search_clauses = [
                "CONVERT(NVARCHAR(50), COALESCE(r.PatientID, s.PatientID, om.OrdPatientID)) = ?",
                f"LOWER({_text_expr('pt', reg_col)}) = ?",
                f"LOWER({_text_expr('pt', reg_col)}) LIKE ?",
                f"LOWER({_text_expr('v', visit_no_col)}) LIKE ?",
            ]
            search_params = [search, search.lower(), f"%-{search.lower()}", f"%/{search.lower()}"]
            if len(search) >= 7:
                search_clauses.append(f"LOWER({_text_expr('pt', mobile_col)}) LIKE ?")
                search_params.append(like)
            clauses.append("(" + " OR ".join(search_clauses) + ")")
            params.extend(search_params)
        else:
            search_clauses = [
                f"LOWER({_text_expr('pt', reg_col)}) LIKE ?",
                f"LOWER({_patient_name_expr(patient_cols)}) LIKE ?",
                f"LOWER({_text_expr('pt', mobile_col)}) LIKE ?",
                f"LOWER({_text_expr('v', visit_no_col)}) LIKE ?",
                f"REPLACE(REPLACE(REPLACE(LOWER({_text_expr('pt', reg_col)}), N'-', N''), N'/', N''), N' ', N'') LIKE ?",
            ]
            clauses.append("(" + " OR ".join(search_clauses) + ")")
            params.extend([like, like, like, like, compact])
    if include_date_filter:
        from_date = filters.get("from_date")
        to_date = filters.get("to_date")
        if from_date:
            clauses.append(f"{_result_event_datetime_expr(meta)} >= ?")
            params.append(str(from_date))
        if to_date:
            clauses.append(f"{_result_event_datetime_expr(meta)} < DATEADD(day, 1, ?)")
            params.append(str(to_date))
    return seed_from, ("WHERE " + " AND ".join(clauses)) if clauses else "", params


def fetch_diagnostic_results(
    unit: str,
    filters: dict[str, Any],
    *,
    page: int = 1,
    page_size: int = 50,
    for_export: bool = False,
    export_limit: int | None = None,
    max_page_size: int = MAX_PAGE_SIZE,
) -> dict[str, Any]:
    conn = get_sql_connection(unit)
    if not conn:
        raise RuntimeError("Database connection failed")
    try:
        meta = _metadata(conn)
        filters = dict(filters)
        search = str(filters.get("patient_search") or "").strip()
        if search and not filters.get("patient_id"):
            resolved_ids = _resolve_patient_search_ids(conn, meta, search)
            if resolved_ids:
                filters["_resolved_patient_ids"] = resolved_ids
        params: list[Any] = []
        where_sql = _build_filters(filters, params, meta)
        order_sql = f"ORDER BY {_result_event_datetime_expr(meta)} DESC, r.ResultID DESC"
        if for_export:
            page_sql = ""
            if export_limit:
                page_sql = "WHERE rn <= ?"
                params.append(parse_positive_int(export_limit, export_limit, maximum=export_limit))
        else:
            page = parse_positive_int(page, 1)
            page_size = parse_positive_int(page_size, 50, maximum=max(MAX_PAGE_SIZE, int(max_page_size or MAX_PAGE_SIZE)))
            start = ((page - 1) * page_size) + 1
            end = page * page_size
            probe_end = end + 1
            page_sql = "WHERE rn BETWEEN ? AND ?"
            patient_scoped = bool(filters.get("patient_id") or filters.get("_resolved_patient_ids"))
            simple_result_seed = patient_scoped or not bool(filters.get("visit_types") or filters.get("patient_search"))
            seed_from, seed_where, seed_params = _build_seed_query_parts(
                filters,
                meta,
                include_date_filter=not patient_scoped,
                direct_result_patient=simple_result_seed,
            )
            candidate_limit = max(probe_end * (5 if patient_scoped else 6), 500 if patient_scoped else 300)
            seed_order_sql = "r.ResultID DESC" if patient_scoped else f"{_result_only_datetime_expr(meta)} DESC, r.ResultID DESC"
            cte_prefix = f"""
            seed AS (
                SELECT TOP ({candidate_limit}) r.ResultID
                {seed_from}
                {seed_where}
                ORDER BY {seed_order_sql}
            ),
            """
            from_clause = "FROM seed sr INNER JOIN dbo.LABTestResultup r WITH (NOLOCK) ON sr.ResultID = r.ResultID"
            params = seed_params + params
            params.extend([start, probe_end])
            sql = _base_sql(
                meta,
                where_sql,
                order_sql,
                page_sql,
                from_clause=from_clause,
                cte_prefix=cte_prefix,
                direct_patient_visit_join=not bool(filters.get("visit_types") or filters.get("patient_search")),
            )
        if for_export:
            sql = _base_sql(meta, where_sql, order_sql, page_sql)
        cur = conn.cursor()
        cur.execute(sql, params)
        rows = _fetch_all_dicts(cur)
        if for_export:
            total = len(rows)
            has_more = False
        else:
            has_more = len(rows) > page_size
            if has_more:
                rows = rows[:page_size]
            total = ((page - 1) * page_size) + len(rows) + (1 if has_more else 0)
        return {"rows": rows, "total": total, "has_more": has_more}
    finally:
        conn.close()


def fetch_period_summary(unit: str, periods: list[dict[str, str]], filters: dict[str, Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for period in periods:
        local_filters = dict(filters)
        local_filters["from_date"] = period["from_date"]
        local_filters["to_date"] = period["to_date"]
        payload = fetch_diagnostic_results(unit, local_filters, page=1, page_size=1, for_export=True)
        rows = payload["rows"]
        patient_ids = {r.get("patient_id") for r in rows if r.get("patient_id") is not None}
        test_ids = {r.get("test_id") for r in rows if r.get("test_id") is not None}
        abnormal = sum(1 for r in rows if str(r.get("result_status") or "").lower() in {"abnormal", "high", "low", "critical"})
        out.append({
            "period": period["name"],
            "from_date": period["from_date"],
            "to_date": period["to_date"],
            "test_name": "Selected Tests",
            "total_patients": len(patient_ids),
            "total_tests": len(test_ids),
            "normal_results": sum(1 for r in rows if str(r.get("result_status") or "").lower() == "normal"),
            "abnormal_results": abnormal,
            "high_results": sum(1 for r in rows if str(r.get("result_status") or "").lower() == "high"),
            "low_results": sum(1 for r in rows if str(r.get("result_status") or "").lower() == "low"),
            "unclassified_results": sum(1 for r in rows if str(r.get("result_status") or "").lower() == "unclassified"),
            "abnormal_percentage": round((abnormal / len(rows) * 100), 2) if rows else 0,
            "repeat_patients": 0,
            "new_patients": len(patient_ids),
            "followup_due_patients": 0,
        })
    return out
