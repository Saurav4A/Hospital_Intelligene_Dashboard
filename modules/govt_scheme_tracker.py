from __future__ import annotations

import io
import math
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from decimal import Decimal

import numpy as np
import pandas as pd
from flask import Blueprint, jsonify, render_template, request, send_file

from modules import data_fetch
from modules.db_connection import get_sql_connection


GOVT_SCHEME_TRACKER_SECTION = "govt_scheme_tracker"
GOVT_SCHEME_ALLOWED_UNITS = ("AHL", "ACI", "BALLIA")
GOVT_SCHEME_DEFINITIONS = (
    {
        "patient_type": "GSS",
        "patient_type_like": "GSS%",
        "patient_subtype": "MGBUY",
        "label": "GSS / MGBUY",
    },
)


def create_govt_scheme_tracker_blueprint(
    *,
    login_required,
    analytics_allowed_units_for_session,
    route_section_map: dict | None = None,
    local_tz=None,
):
    """Create the Govt. Scheme Tracker blueprint and register section gates."""
    bp = Blueprint("govt_scheme_tracker", __name__)

    if isinstance(route_section_map, dict):
        route_section_map.update(
            {
                "/govt-scheme-tracker": GOVT_SCHEME_TRACKER_SECTION,
                "/api/govt-scheme-tracker/summary": GOVT_SCHEME_TRACKER_SECTION,
                "/api/govt-scheme-tracker/grids": GOVT_SCHEME_TRACKER_SECTION,
                "/api/govt-scheme-tracker/grid": GOVT_SCHEME_TRACKER_SECTION,
                "/api/govt-scheme-tracker/export": GOVT_SCHEME_TRACKER_SECTION,
            }
        )

    def _allowed_govt_units() -> list[str]:
        allowed = analytics_allowed_units_for_session() or []
        allowed_set = {str(unit or "").strip().upper() for unit in allowed if str(unit or "").strip()}
        return [unit for unit in GOVT_SCHEME_ALLOWED_UNITS if unit in allowed_set]

    def _units_for_request(unit_value: str | None) -> tuple[list[str], str | None]:
        allowed_units = _allowed_govt_units()
        unit = str(unit_value or "ALL").strip().upper()
        if unit in {"", "ALL"}:
            return allowed_units, None
        if unit not in GOVT_SCHEME_ALLOWED_UNITS:
            return [], f"Unit {unit} is not available for Govt. Scheme Tracker."
        if unit not in allowed_units:
            return [], f"Unit {unit} is outside your access scope."
        return [unit], None

    def _default_date_range() -> tuple[str, str]:
        today = datetime.now(tz=local_tz).date() if local_tz else date.today()
        start = today.replace(day=1)
        return start.isoformat(), today.isoformat()

    @bp.route("/govt-scheme-tracker")
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def page():
        allowed_units = _allowed_govt_units()
        if not allowed_units:
            return "Govt. Scheme Tracker is available only for AHL, ACI, and Ballia within your unit scope.", 403
        default_from, default_to = _default_date_range()
        return render_template(
            "govt_scheme_tracker.html",
            allowed_units=allowed_units,
            default_from=default_from,
            default_to=default_to,
        )

    @bp.route("/api/govt-scheme-tracker/summary")
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_summary():
        filters, error_response = _parse_request_filters(_units_for_request)
        if error_response:
            return error_response
        try:
            payload = build_govt_scheme_summary_payload(filters["units"], filters["from_date"], filters["to_date"])
            return jsonify(_json_safe(payload))
        except Exception as exc:
            print(f"/api/govt-scheme-tracker/summary error: {exc}")
            return jsonify({"status": "error", "message": "Failed to build Govt. Scheme summary."}), 500

    @bp.route("/api/govt-scheme-tracker/grid")
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_grid():
        filters, error_response = _parse_request_filters(_units_for_request)
        if error_response:
            return error_response
        grid_key = (request.args.get("grid") or "").strip().lower()
        page, page_size = _parse_page_args()
        search = (request.args.get("search") or "").strip()
        try:
            payload = fetch_govt_scheme_grid_page(
                filters["units"],
                filters["from_date"],
                filters["to_date"],
                grid_key,
                page=page,
                page_size=page_size,
                search=search,
            )
            return jsonify(_json_safe(payload))
        except ValueError as exc:
            return jsonify({"status": "error", "message": str(exc)}), 400
        except Exception as exc:
            print(f"/api/govt-scheme-tracker/grid error: {exc}")
            return jsonify({"status": "error", "message": "Failed to load Govt. Scheme grid page."}), 500

    @bp.route("/api/govt-scheme-tracker/grids")
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_grids():
        filters, error_response = _parse_request_filters(_units_for_request)
        if error_response:
            return error_response
        try:
            payload = build_govt_scheme_payload(filters["units"], filters["from_date"], filters["to_date"], include_grids=True)
            return jsonify(_json_safe(payload))
        except Exception as exc:
            print(f"/api/govt-scheme-tracker/grids error: {exc}")
            return jsonify({"status": "error", "message": "Failed to load Govt. Scheme grids."}), 500

    @bp.route("/api/govt-scheme-tracker/export")
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_export():
        filters, error_response = _parse_request_filters(_units_for_request, export_mode=True)
        if error_response:
            return error_response
        try:
            payload = build_govt_scheme_export_payload(filters["units"], filters["from_date"], filters["to_date"])
            workbook = build_govt_scheme_excel(payload, filters)
            unit_label = filters["unit"] if filters["unit"] != "ALL" else "ALL"
            filename = f"Govt_Scheme_Tracker_MGBUY_{unit_label}_{filters['from_date']}_to_{filters['to_date']}.xlsx"
            return send_file(
                io.BytesIO(workbook),
                as_attachment=True,
                download_name=filename,
                mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        except Exception as exc:
            print(f"/api/govt-scheme-tracker/export error: {exc}")
            return jsonify({"status": "error", "message": "Govt. Scheme export failed."}), 500

    return bp


def _parse_request_filters(units_for_request, export_mode: bool = False):
    from_date = (request.args.get("from") or request.args.get("from_date") or "").strip()
    to_date = (request.args.get("to") or request.args.get("to_date") or "").strip()
    unit = (request.args.get("unit") or "ALL").strip().upper() or "ALL"
    if not from_date or not to_date:
        return None, _json_or_text({"status": "error", "message": "Missing date range."}, 400, export_mode)
    if not _valid_iso_date(from_date) or not _valid_iso_date(to_date):
        return None, _json_or_text({"status": "error", "message": "Invalid date format. Use YYYY-MM-DD."}, 400, export_mode)
    if from_date > to_date:
        return None, _json_or_text({"status": "error", "message": "From date cannot be after To date."}, 400, export_mode)

    units, error = units_for_request(unit)
    if error:
        return None, _json_or_text({"status": "error", "message": error}, 403, export_mode)
    if not units:
        return None, _json_or_text({"status": "error", "message": "No Govt. Scheme unit access assigned."}, 403, export_mode)
    return {"unit": unit, "units": units, "from_date": from_date, "to_date": to_date}, None


def _parse_page_args() -> tuple[int, int]:
    try:
        page = int(request.args.get("page") or 1)
    except Exception:
        page = 1
    try:
        page_size = int(request.args.get("page_size") or 25)
    except Exception:
        page_size = 25
    return max(1, page), max(5, min(page_size, 100))


def _json_or_text(payload: dict, status: int, export_mode: bool):
    if export_mode:
        return payload.get("message") or "Request failed", status
    return jsonify(payload), status


def _valid_iso_date(value: str) -> bool:
    if not re.match(r"^\d{4}-\d{2}-\d{2}$", value or ""):
        return False
    try:
        datetime.strptime(value, "%Y-%m-%d")
        return True
    except ValueError:
        return False


def _q_ident(name: str) -> str:
    return f"[{str(name).replace(']', ']]')}]"


def _table_ref(table_name: str | None) -> str:
    return f"dbo.{_q_ident(table_name)}" if table_name else ""


def _sql_text(alias: str, col_name: str | None, size: int = 255) -> str:
    if not alias or not col_name:
        return f"CAST(N'' AS NVARCHAR({size}))"
    return f"LTRIM(RTRIM(ISNULL(CONVERT(NVARCHAR({size}), {alias}.{_q_ident(col_name)}), N'')))"


def _sql_num(alias: str, col_name: str | None, sql_type: str = "INT") -> str:
    if not alias or not col_name:
        return f"CAST(NULL AS {sql_type})"
    text_expr = _sql_text(alias, col_name, 100)
    return (
        "CASE "
        f"WHEN NULLIF({text_expr}, N'') IS NULL THEN NULL "
        f"WHEN ISNUMERIC({text_expr}) = 1 THEN CONVERT({sql_type}, {text_expr}) "
        "ELSE NULL END"
    )


def _sql_money(alias: str, col_name: str | None) -> str:
    if not alias or not col_name:
        return "CAST(0 AS DECIMAL(18,2))"
    return f"CAST(ISNULL({alias}.{_q_ident(col_name)}, 0) AS DECIMAL(18,2))"


def _first_col(conn, table_name: str | None, candidates: list[str]) -> str | None:
    return data_fetch._resolve_column(conn, table_name, candidates) if table_name else None


def _schema(conn, unit: str | None = None) -> dict:
    """Resolve unit schema details once so SQL stays portable across HID databases."""
    visit = data_fetch._resolve_table_name(conn, ["Visit", "visit", "Visit_Mst", "VisitMst"])
    billing = data_fetch._resolve_table_name(conn, ["Billing_Mst", "BillingMst", "Bill_Mst", "BillMst"])
    receipt_mst = data_fetch._resolve_table_name(conn, ["Receipt_mst", "Receipt_Mst", "ReceiptMst"])
    receipt_dtls = data_fetch._resolve_table_name(conn, ["Receipt_Dtls", "Receipt_dtls", "ReceiptDetails", "Receipt_Details"])
    employee = data_fetch._resolve_table_name(conn, ["Employee_Mst", "employee_mst", "EmployeeMst"])
    department = data_fetch._resolve_table_name(conn, ["Department_Mst", "department_mst", "DepartmentMst"])
    subdepartment = data_fetch._resolve_table_name(conn, ["SubDepartment_Mst", "SubDepartmentMst", "SubDepartment_mst", "subdepartment_mst"])

    if not visit:
        raise RuntimeError("Visit table was not found.")

    cols = {
        "visit_id": _first_col(conn, visit, ["Visit_ID", "VisitId", "VisitID"]),
        "visit_no": _first_col(conn, visit, ["VisitNo", "Visit_No"]),
        "visit_date": _first_col(conn, visit, ["VisitDate", "Visit_Date"]),
        "discharge_date": _first_col(conn, visit, ["DischargeDate", "Discharge_Date", "DischargeDateTime"]),
        "patient_id": _first_col(conn, visit, ["PatientID", "PatientId", "Patient_ID"]),
        "visit_type": _first_col(conn, visit, ["TypeOfVisit", "VisitType", "Type_Of_Visit"]),
        "visit_type_id": _first_col(conn, visit, ["VisitTypeID", "VisitTypeId", "VisitType_ID"]),
        "patient_type_id": _first_col(conn, visit, ["PatientType_ID", "PatientTypeId", "PatientTypeID", "PatientType"]),
        "patient_subtype_id": _first_col(conn, visit, ["PatientSubType_ID", "PatientSubTypeId", "PatientSubTypeID", "PatientSubType"]),
        "doc_in_charge": _first_col(conn, visit, ["DocInCharge", "DocIncharge", "DoctorInCharge", "DoctorIncharge", "DocInChargeID", "DocInchargeID"]),
        "bill_id": _first_col(conn, billing, ["Bill_ID", "BillId", "BillID", "ID"]),
        "bill_no": _first_col(conn, billing, ["BillNo", "Bill_No", "BillNumber"]),
        "bill_type": _first_col(conn, billing, ["BillType", "Bill_Type", "TypeOfBill", "Type_Of_Bill", "BillTypeCode", "Bill_Type_Code"]),
        "bill_date": _first_col(conn, billing, ["BillDate", "Bill_Date", "CreatedDate"]),
        "bill_visit_id": _first_col(conn, billing, ["Visit_ID", "VisitId", "VisitID"]),
        "bill_patient_id": _first_col(conn, billing, ["PatientID", "PatientId", "Patient_ID"]),
        "gross": _first_col(conn, billing, ["GrossAmount", "Gross_Amount", "TotalAmount", "Amount"]),
        "discount": _first_col(conn, billing, ["DiscountAmount", "Discount_Amount", "DiscAmount"]),
        "net": _first_col(conn, billing, ["NetAmount", "Net_Amount", "BillAmount"]),
        "ph_amount": _first_col(conn, billing, ["PHAmount", "Pharmacy", "PharmacyAmount", "PH_Amount"]),
        "ph_return_amount": _first_col(conn, billing, ["PHReturnAmount", "PharmacyReturnAmount", "PH_ReturnAmount", "PHReturn_Amount"]),
        "paid": _first_col(conn, billing, ["PaidAmount", "ReceiptAmount", "ReceivedAmount", "ReceivedAmt"]),
        "due": _first_col(conn, billing, ["DueAmount", "BalanceAmount", "OutstandingAmount"]),
        "cancel": _first_col(conn, billing, ["CancelStatus", "Cancelled", "Canceled", "IsCancelled", "IsCanceled"]),
        "receipt_id": _first_col(conn, receipt_mst, ["Receipt_ID", "ReceiptId", "ReceiptID"]),
        "receipt_cancel": _first_col(conn, receipt_mst, ["CancelStatus", "Cancelled", "Canceled", "IsCancelled", "IsCanceled"]),
        "receipt_date": _first_col(conn, receipt_mst, ["Receipt_Date", "ReceiptDate", "CreatedDate"]),
        "receipt_detail_receipt_id": _first_col(conn, receipt_dtls, ["Receipt_ID", "ReceiptId", "ReceiptID"]),
        "receipt_invoice": _first_col(conn, receipt_dtls, ["InvoiceNo", "BillNo", "Bill_ID", "BillId"]),
        "receipt_amount": _first_col(conn, receipt_dtls, ["Amount", "ReceiptAmount", "PaidAmount"]),
        "emp_id": _first_col(conn, employee, ["EmpID", "EmpId", "EmployeeID", "EmployeeId", "ID", "Id"]),
        "emp_first_name": _first_col(conn, employee, ["FirstName", "First_Name", "FName"]),
        "emp_middle_name": _first_col(conn, employee, ["MiddleName", "Middle_Name", "MName"]),
        "emp_last_name": _first_col(conn, employee, ["LastName", "Last_Name", "LName"]),
        "emp_short_name": _first_col(conn, employee, ["Short_Name", "ShortName", "Short"]),
        "emp_department_id": _first_col(conn, employee, ["Department_ID", "DepartmentID", "DepartmentId", "department_id", "departmentid"]),
        "emp_subdepartment_id": _first_col(conn, employee, ["SubDepartment_ID", "SubDepartmentId", "SubDepartment_Id", "SubDeptID", "SubDeptId", "subdepartment_id"]),
        "dept_id": _first_col(conn, department, ["Department_ID", "DepartmentID", "DepartmentId", "DeptID", "DeptId", "ID", "Id"]),
        "dept_name": _first_col(conn, department, ["Department_Name", "DepartmentName", "Department", "DeptName", "Dept", "Name"]),
        "subdept_id": _first_col(conn, subdepartment, ["SubDepartment_ID", "SubDepartmentID", "SubDepartmentId", "SubDeptID", "SubDeptId", "ID", "Id"]),
        "subdept_name": _first_col(conn, subdepartment, ["SubDepartment_Name", "SubDepartmentName", "SubDepartment", "SubDeptName", "SubDept", "Name"]),
    }
    if not cols["visit_id"] or not cols["visit_date"] or not cols["patient_id"]:
        raise RuntimeError("Visit table is missing required columns.")
    return {
        "unit": str(unit or "").strip().upper(),
        "visit": visit,
        "billing": billing,
        "receipt_mst": receipt_mst,
        "receipt_dtls": receipt_dtls,
        "employee": employee,
        "department": department,
        "subdepartment": subdepartment,
        "cols": cols,
    }


def _visit_type_expr(cols: dict) -> str:
    visit_type_text = _sql_text("v", cols.get("visit_type"), 120)
    visit_type_id = _sql_num("v", cols.get("visit_type_id"), "INT")
    discharge_date = f"CAST(v.{_q_ident(cols['discharge_date'])} AS DATETIME)" if cols.get("discharge_date") else "CAST(NULL AS DATETIME)"
    visit_date = f"CAST(v.{_q_ident(cols['visit_date'])} AS DATETIME)"
    return f"""
        CASE
            WHEN ({visit_type_id} = 1 OR UPPER({visit_type_text}) LIKE N'%IPD%' OR UPPER({visit_type_text}) LIKE N'%IN%PATIENT%')
                 AND {discharge_date} IS NOT NULL
                 AND DATEDIFF(MINUTE, {visit_date}, {discharge_date}) BETWEEN 0 AND 480
                THEN N'Daycare'
            WHEN UPPER({visit_type_text}) LIKE N'%DAY%' THEN N'Daycare'
            WHEN {visit_type_id} = 1 OR UPPER({visit_type_text}) LIKE N'%IPD%' OR UPPER({visit_type_text}) LIKE N'%IN%PATIENT%' THEN N'In-Patient'
            WHEN {visit_type_id} = 3 OR UPPER({visit_type_text}) LIKE N'%DPV%' OR UPPER({visit_type_text}) LIKE N'%DIAGNOSTIC%' THEN N'Out-Patient'
            WHEN {visit_type_id} = 2 OR UPPER({visit_type_text}) LIKE N'%OPD%' OR UPPER({visit_type_text}) LIKE N'%OUT%PATIENT%' THEN N'Out-Patient'
            ELSE N'Out-Patient'
        END
    """


def _scheme_filter(cols: dict, alias: str = "v") -> tuple[str, list[str]]:
    pt_expr = _sql_pat_type_expr(cols, alias)
    subtype_expr = _sql_pat_subtype_expr(cols, alias)
    clauses = []
    params = []
    for scheme in GOVT_SCHEME_DEFINITIONS:
        clauses.append(f"({pt_expr} LIKE ? AND {subtype_expr} = ?)")
        params.extend([
            str(scheme.get("patient_type_like") or scheme["patient_type"]).upper(),
            scheme["patient_subtype"].upper(),
        ])
    return " OR ".join(clauses), params


def _sql_pat_type_expr(cols: dict, alias: str = "v") -> str:
    patient_type_id = _sql_num(alias, cols.get("patient_type_id"), "INT")
    return f"UPPER(LTRIM(RTRIM(ISNULL(dbo.fn_pat_type({patient_type_id}), N''))))"


def _sql_pat_subtype_expr(cols: dict, alias: str = "v") -> str:
    patient_subtype_id = _sql_num(alias, cols.get("patient_subtype_id"), "INT")
    return f"UPPER(LTRIM(RTRIM(ISNULL(dbo.fn_patsub_type({patient_subtype_id}), N''))))"


def _patient_identity_join(patient_id_sql: str, patient_alias: str = "p", title_alias: str = "tm") -> str:
    return f"""
        LEFT JOIN dbo.Patient {patient_alias} WITH (NOLOCK)
            ON {patient_alias}.PatientId = {patient_id_sql}
        LEFT JOIN dbo.Title_Mst {title_alias} WITH (NOLOCK)
            ON {title_alias}.Title_ID = {patient_alias}.Title
    """


def _patient_reg_expr(patient_alias: str = "p") -> str:
    return f"ISNULL(CONVERT(NVARCHAR(80), {patient_alias}.Registration_No), N'')"


def _patient_name_expr(patient_alias: str = "p", title_alias: str = "tm") -> str:
    title_part = f"COALESCE(NULLIF(LTRIM(RTRIM(CONVERT(NVARCHAR(80), {title_alias}.Title_Code))), N''), NULLIF(LTRIM(RTRIM(CONVERT(NVARCHAR(80), {title_alias}.Title_Name))), N''), N'')"
    first = f"ISNULL(CONVERT(NVARCHAR(120), {patient_alias}.First_Name), N'')"
    middle = f"ISNULL(CONVERT(NVARCHAR(120), {patient_alias}.Middle_Name), N'')"
    last = f"ISNULL(CONVERT(NVARCHAR(120), {patient_alias}.Last_Name), N'')"
    combined = f"{title_part} + N' ' + {first} + N' ' + {middle} + N' ' + {last}"
    return f"LTRIM(RTRIM(REPLACE(REPLACE(REPLACE({combined}, N'  ', N' '), N'  ', N' '), N'  ', N' ')))"


def _doctor_context_join(schema: dict, visit_alias: str = "v", employee_alias: str = "e", dept_alias: str = "dm", subdept_alias: str = "sdm", doc_id_sql: str | None = None) -> str:
    cols = schema["cols"]
    if not (schema.get("employee") and cols.get("emp_id") and (doc_id_sql or cols.get("doc_in_charge"))):
        return ""
    doc_expr = doc_id_sql or f"{visit_alias}.{_q_ident(cols['doc_in_charge'])}"
    dept_join = ""
    if schema.get("department") and cols.get("emp_department_id") and cols.get("dept_id"):
        dept_join = f"""
        LEFT JOIN {_table_ref(schema['department'])} {dept_alias} WITH (NOLOCK)
            ON {dept_alias}.{_q_ident(cols['dept_id'])} = {employee_alias}.{_q_ident(cols['emp_department_id'])}
        """
    subdept_join = ""
    if schema.get("subdepartment") and cols.get("emp_subdepartment_id") and cols.get("subdept_id"):
        subdept_join = f"""
        LEFT JOIN {_table_ref(schema['subdepartment'])} {subdept_alias} WITH (NOLOCK)
            ON {subdept_alias}.{_q_ident(cols['subdept_id'])} = {employee_alias}.{_q_ident(cols['emp_subdepartment_id'])}
        """
    return f"""
        LEFT JOIN {_table_ref(schema['employee'])} {employee_alias} WITH (NOLOCK)
            ON {employee_alias}.{_q_ident(cols['emp_id'])} = {doc_expr}
        {dept_join}
        {subdept_join}
    """


def _doctor_name_expr(schema: dict, employee_alias: str = "e") -> str:
    cols = schema["cols"]
    if not (schema.get("employee") and cols.get("emp_id") and cols.get("emp_first_name")):
        return "CAST(N'' AS NVARCHAR(240))"
    first = _sql_text(employee_alias, cols.get("emp_first_name"), 120)
    return f"ISNULL(NULLIF({first}, N''), N'')"


def _doctor_department_expr(schema: dict, employee_alias: str = "e", dept_alias: str = "dm", subdept_alias: str = "sdm") -> str:
    cols = schema["cols"]
    if schema.get("unit") == "ACI":
        if schema.get("subdepartment") and cols.get("subdept_name"):
            return f"ISNULL(NULLIF({_sql_text(subdept_alias, cols.get('subdept_name'), 180)}, N''), N'')"
        if cols.get("emp_subdepartment_id"):
            return f"ISNULL(dbo.Fn_subDept({employee_alias}.{_q_ident(cols['emp_subdepartment_id'])}), N'')"
    if schema.get("department") and cols.get("dept_name"):
        return f"ISNULL(NULLIF({_sql_text(dept_alias, cols.get('dept_name'), 180)}, N''), N'')"
    if cols.get("emp_department_id"):
        return f"ISNULL(dbo.fn_dept({employee_alias}.{_q_ident(cols['emp_department_id'])}), N'')"
    return "CAST(N'' AS NVARCHAR(180))"


def _bill_join(cols: dict) -> str:
    if not cols.get("bill_visit_id"):
        return "1 = 0"
    return f"bm.{_q_ident(cols['bill_visit_id'])} = v.{_q_ident(cols['visit_id'])}"


def _bill_type_expr(cols: dict, alias: str = "bm") -> str:
    if not cols.get("bill_type"):
        return "N'P'"
    return f"UPPER(LTRIM(RTRIM(ISNULL(CONVERT(NVARCHAR(20), {alias}.{_q_ident(cols['bill_type'])}), N''))))"


def _period_bill_amount_exprs(cols: dict, alias: str = "bm") -> dict[str, str]:
    bill_type = _bill_type_expr(cols, alias)
    is_hospital = f"{bill_type} = N'P'"
    is_pharmacy = f"{bill_type} IN (N'PH', N'PI')"
    gross = _sql_money(alias, cols.get("gross"))
    discount = _sql_money(alias, cols.get("discount"))
    net = _sql_money(alias, cols.get("net"))
    ph_amount = _sql_money(alias, cols.get("ph_amount"))
    ph_return = _sql_money(alias, cols.get("ph_return_amount"))
    paid = _sql_money(alias, cols.get("paid"))
    due = _sql_money(alias, cols.get("due"))
    return {
        "bill_type": bill_type,
        "gross": f"CASE WHEN {is_hospital} THEN {gross} ELSE CAST(0 AS DECIMAL(18,2)) END",
        "discount": f"CASE WHEN {is_hospital} THEN {discount} ELSE CAST(0 AS DECIMAL(18,2)) END",
        "net": f"CASE WHEN {is_hospital} THEN {net} ELSE CAST(0 AS DECIMAL(18,2)) END",
        "ph_amount": f"CASE WHEN {is_pharmacy} THEN {net} ELSE {ph_amount} END",
        "ph_return_amount": ph_return,
        "paid": f"CASE WHEN {is_hospital} THEN {paid} ELSE CAST(0 AS DECIMAL(18,2)) END",
        "due": f"CASE WHEN {is_hospital} THEN {due} ELSE CAST(0 AS DECIMAL(18,2)) END",
    }


def _bill_cancel_pred(cols: dict) -> str:
    cancel_col = cols.get("cancel")
    if not cancel_col:
        return "1 = 1"
    cancel_text = _sql_text("bm", cancel_col, 40)
    return (
        "ISNULL("
        f"CASE WHEN ISNUMERIC({cancel_text}) = 1 THEN CONVERT(INT, {cancel_text}) ELSE 0 END"
        ", 0) = 0"
    )


def _bill_to_date_pred(cols: dict) -> str:
    if not cols.get("bill_date"):
        return "1 = 1"
    return f"bm.{_q_ident(cols['bill_date'])} < DATEADD(DAY, 1, CAST(? AS DATETIME))"


def _bill_to_date_params(cols: dict, to_date: str) -> list[str]:
    """Return the parameter list needed by _bill_to_date_pred."""
    return [to_date] if cols.get("bill_date") else []


def _receipt_apply(schema: dict) -> str:
    cols = schema["cols"]
    if not (schema.get("receipt_mst") and schema.get("receipt_dtls") and cols.get("receipt_id") and cols.get("receipt_detail_receipt_id") and cols.get("receipt_invoice") and cols.get("receipt_amount") and cols.get("bill_id")):
        return "OUTER APPLY (SELECT CAST(NULL AS DECIMAL(18,2)) AS PaidAmount) rec"
    cancel = (
        "AND ISNULL("
        f"CASE WHEN ISNUMERIC({_sql_text('rm', cols['receipt_cancel'], 40)}) = 1 "
        f"THEN CONVERT(INT, {_sql_text('rm', cols['receipt_cancel'], 40)}) ELSE 0 END"
        ", 0) = 0"
        if cols.get("receipt_cancel")
        else ""
    )
    return f"""
        OUTER APPLY (
            SELECT CAST(ISNULL(SUM(ISNULL(rd.{_q_ident(cols['receipt_amount'])}, 0)), 0) AS DECIMAL(18,2)) AS PaidAmount
            FROM {_table_ref(schema['receipt_dtls'])} rd WITH (NOLOCK)
            INNER JOIN {_table_ref(schema['receipt_mst'])} rm WITH (NOLOCK)
                ON rm.{_q_ident(cols['receipt_id'])} = rd.{_q_ident(cols['receipt_detail_receipt_id'])}
            WHERE ISNUMERIC(rd.{_q_ident(cols['receipt_invoice'])}) = 1
              AND CONVERT(INT, rd.{_q_ident(cols['receipt_invoice'])}) = bm.{_q_ident(cols['bill_id'])}
              {cancel}
        ) rec
    """


def _receipt_cancel_pred(cols: dict) -> str:
    if not cols.get("receipt_cancel"):
        return "1 = 1"
    cancel_text = _sql_text("rm", cols["receipt_cancel"], 40)
    return (
        "ISNULL("
        f"CASE WHEN ISNUMERIC({cancel_text}) = 1 THEN CONVERT(INT, {cancel_text}) ELSE 0 END"
        ", 0) = 0"
    )


def _has_receipt_amount(schema: dict) -> bool:
    cols = schema["cols"]
    return bool(
        schema.get("receipt_mst")
        and schema.get("receipt_dtls")
        and cols.get("receipt_id")
        and cols.get("receipt_detail_receipt_id")
        and cols.get("receipt_invoice")
        and cols.get("receipt_amount")
        and cols.get("bill_id")
    )


def build_govt_scheme_payload(units: list[str], from_date: str, to_date: str, include_grids: bool = True) -> dict:
    """Fetch all Govt. Scheme tracker datasets and calculate KPI totals in Python."""
    frames = {
        "tracking": [],
        "visit_type": [],
        "new_patient_revenue": [],
        "period_revenue": [],
        "exceptions": [],
    }
    errors = []

    for unit in units:
        unit_key = str(unit or "").strip().upper()
        conn = None
        try:
            conn = get_sql_connection(unit_key)
            if not conn:
                raise RuntimeError("Database connection failed.")
            schema = _schema(conn, unit_key)
            frames["tracking"].append(_fetch_tracking_summary(conn, schema, unit_key, to_date))
            frames["visit_type"].append(_fetch_new_visit_type_details(conn, schema, unit_key, from_date, to_date))
            frames["new_patient_revenue"].append(_fetch_new_patient_revenue(conn, schema, unit_key, from_date, to_date))
            frames["period_revenue"].append(_fetch_period_revenue(conn, schema, unit_key, from_date, to_date))
            frames["exceptions"].append(_fetch_exceptions(conn, schema, unit_key, from_date, to_date))
        except Exception as exc:
            print(f"Govt. Scheme Tracker fetch failed for {unit_key}: {exc}")
            errors.append({"unit": unit_key, "message": str(exc)})
        finally:
            try:
                if conn:
                    conn.close()
            except Exception:
                pass

    dataframes = {key: _concat_frames(value) for key, value in frames.items()}
    kpis = _build_kpis(dataframes)
    return {
        "status": "success" if not errors else ("partial" if any(not df.empty for df in dataframes.values()) else "error"),
        "scheme": GOVT_SCHEME_DEFINITIONS[0]["label"],
        "filters": {"unit": "ALL" if len(units) > 1 else units[0], "units": units, "from_date": from_date, "to_date": to_date},
        "kpis": kpis,
        "errors": errors,
        "grids": {key: _df_records(df) for key, df in dataframes.items()} if include_grids else {},
    }


def build_govt_scheme_summary_payload(units: list[str], from_date: str, to_date: str) -> dict:
    """Build KPI payload with SQL aggregates only, without loading grid detail rows."""
    frames = {"tracking": [], "visit_type": [], "new_revenue": [], "period_revenue": [], "exceptions": []}
    errors = []
    for unit in units:
        unit_key = str(unit or "").strip().upper()
        conn = None
        try:
            conn = get_sql_connection(unit_key)
            if not conn:
                raise RuntimeError("Database connection failed.")
            schema = _schema(conn, unit_key)
            frames["tracking"].append(_fetch_tracking_summary(conn, schema, unit_key, to_date))
            frames["visit_type"].append(_fetch_new_visit_type_summary(conn, schema, unit_key, from_date, to_date))
            frames["new_revenue"].append(_fetch_new_patient_revenue_summary(conn, schema, unit_key, from_date, to_date))
            frames["period_revenue"].append(_fetch_period_revenue_summary(conn, schema, unit_key, from_date, to_date))
            frames["exceptions"].append(_fetch_exception_status_summary(conn, schema, unit_key, from_date, to_date))
        except Exception as exc:
            print(f"Govt. Scheme Tracker summary failed for {unit_key}: {exc}")
            errors.append({"unit": unit_key, "message": str(exc)})
        finally:
            try:
                if conn:
                    conn.close()
            except Exception:
                pass

    tracking = _concat_frames(frames["tracking"])
    visit_type = _concat_frames(frames["visit_type"])
    new_revenue = _concat_frames(frames["new_revenue"])
    period_revenue = _concat_frames(frames["period_revenue"])
    exceptions = _concat_frames(frames["exceptions"])
    has_data = any(not df.empty for df in [tracking, visit_type, new_revenue, period_revenue, exceptions])
    kpis = {
        "total_tracking": {
            "total_visits": _sum_col(tracking, "Total Visits"),
            "unique_patients": _sum_col(tracking, "Unique Patients"),
            "by_unit": _records_from_group(tracking, "Unit", ["Total Visits", "Unique Patients"]),
        },
        "unique_by_visit_type": {
            "total_unique_patients": _sum_col(visit_type, "Unique Patients"),
            "by_unit_visit_type": _df_records(visit_type),
        },
        "revenue_per_new_patient": {
            "total_revenue": _new_patient_total_revenue(new_revenue),
            "billing_count": _sum_col(new_revenue, "Billing Count"),
            "by_unit_visit_type": _df_records(new_revenue),
        },
        "period_revenue": {
            "total_revenue": _period_total_revenue(period_revenue),
            "bill_count": _sum_col(period_revenue, "Bill Count"),
            "by_unit_visit_type": _df_records(period_revenue),
        },
        "exceptions": {
            "count": _sum_col(exceptions, "Count"),
            "by_status": _df_records(exceptions),
        },
    }
    return {
        "status": "success" if not errors else ("partial" if has_data else "error"),
        "scheme": GOVT_SCHEME_DEFINITIONS[0]["label"],
        "filters": {"unit": "ALL" if len(units) > 1 else units[0], "units": units, "from_date": from_date, "to_date": to_date},
        "kpis": kpis,
        "errors": errors,
    }


GRID_COLUMNS = {
    "tracking": ["Unit", "Patient Type", "Patient SubType", "Total Visits", "Unique Patients", "First Visit Date"],
    "visit_type": ["Unit", "Visit Type", "Reg No", "Patient Name", "Visit No", "First Visit Date", "DocInCharge", "Department", "Patient Type", "Patient SubType"],
    "new_patient_revenue": ["Unit", "Reg No", "Patient Name", "Visit Type", "First Visit Date", "DocInCharge", "Department", "Total Revenue", "PHAmount", "PHReturnAmount", "Billing Count"],
    "period_revenue": ["Unit", "Bill Date", "Reg No", "Patient Name", "Visit No", "Visit Type", "DocInCharge", "Department", "Bill No", "Gross Amount", "Discount", "Net Amount", "PHAmount", "PHReturnAmount", "Paid Amount", "Due Amount"],
    "exceptions": ["Unit", "Reg No", "Patient Name", "Visit Type", "Visit Date", "DocInCharge", "Department", "Patient Type", "Patient SubType", "Billing Status", "Revenue", "Remarks / Exception Reason"],
}
INTERNAL_GRID_COLUMNS = {
    "new_patient_revenue": GRID_COLUMNS["new_patient_revenue"] + ["Hospital Bill Count"],
    "period_revenue": GRID_COLUMNS["period_revenue"] + ["BillType"],
}


def fetch_govt_scheme_grid_page(
    units: list[str],
    from_date: str,
    to_date: str,
    grid_key: str,
    *,
    page: int,
    page_size: int,
    search: str = "",
) -> dict:
    """Fetch one searchable grid page; detail grids are paged in SQL per unit."""
    if grid_key not in GRID_COLUMNS:
        raise ValueError("Invalid grid requested.")
    errors = []
    frames = []
    total_rows = 0
    is_all_units = len(units) > 1
    fetch_page = 1 if is_all_units else page
    fetch_size = page * page_size if is_all_units else page_size

    def fetch_unit(unit: str) -> tuple[pd.DataFrame, int, dict | None]:
        unit_key = str(unit or "").strip().upper()
        conn = None
        try:
            conn = get_sql_connection(unit_key)
            if not conn:
                raise RuntimeError("Database connection failed.")
            schema = _schema(conn, unit_key)
            if grid_key == "tracking":
                df = _filter_df_search(_fetch_tracking_summary(conn, schema, unit_key, to_date), search)
                return _slice_df_page(df, fetch_page, fetch_size), len(df), None
            elif grid_key == "visit_type":
                df, count = _fetch_new_visit_type_detail_page(conn, schema, unit_key, from_date, to_date, fetch_page, fetch_size, search)
                return df, count, None
            elif grid_key == "new_patient_revenue":
                df, count = _fetch_new_patient_revenue_page(conn, schema, unit_key, from_date, to_date, fetch_page, fetch_size, search)
                return df, count, None
            elif grid_key == "period_revenue":
                df, count = _fetch_period_revenue_page(conn, schema, unit_key, from_date, to_date, fetch_page, fetch_size, search)
                return df, count, None
            elif grid_key == "exceptions":
                df, count = _fetch_exceptions_page(conn, schema, unit_key, from_date, to_date, fetch_page, fetch_size, search)
                return df, count, None
            return pd.DataFrame(columns=GRID_COLUMNS[grid_key]), 0, None
        except Exception as exc:
            print(f"Govt. Scheme Tracker grid {grid_key} failed for {unit_key}: {exc}")
            return pd.DataFrame(columns=GRID_COLUMNS[grid_key]), 0, {"unit": unit_key, "message": str(exc)}
        finally:
            try:
                if conn:
                    conn.close()
            except Exception:
                pass

    if len(units) > 1:
        with ThreadPoolExecutor(max_workers=min(len(units), len(GOVT_SCHEME_ALLOWED_UNITS))) as executor:
            future_map = {executor.submit(fetch_unit, unit): unit for unit in units}
            for future in as_completed(future_map):
                df, count, error = future.result()
                total_rows += int(count or 0)
                if df is not None and not df.empty:
                    frames.append(df)
                if error:
                    errors.append(error)
    else:
        df, count, error = fetch_unit(units[0])
        total_rows += int(count or 0)
        if df is not None and not df.empty:
            frames.append(df)
        if error:
            errors.append(error)

    combined = _concat_frames(frames)
    combined = _sort_grid_df(combined, grid_key)
    if is_all_units:
        combined = _slice_df_page(combined, page, page_size)
    combined = combined.reindex(columns=GRID_COLUMNS[grid_key]) if not combined.empty else pd.DataFrame(columns=GRID_COLUMNS[grid_key])
    total_pages = max(1, math.ceil(total_rows / page_size)) if total_rows else 1
    return {
        "status": "success" if not errors else ("partial" if total_rows or not combined.empty else "error"),
        "grid": grid_key,
        "columns": GRID_COLUMNS[grid_key],
        "rows": _df_records(combined),
        "page": page,
        "page_size": page_size,
        "total_rows": int(total_rows),
        "total_pages": int(total_pages),
        "errors": errors,
    }


def build_govt_scheme_export_payload(units: list[str], from_date: str, to_date: str) -> dict:
    """Build export payload with direct sheet queries capped to the selected date range."""
    errors = []
    frames = {key: [] for key in GRID_COLUMNS}

    def fetch_unit(unit: str) -> tuple[dict[str, pd.DataFrame], list[dict]]:
        unit_key = str(unit or "").strip().upper()
        conn = None
        unit_frames = {key: pd.DataFrame(columns=GRID_COLUMNS[key]) for key in GRID_COLUMNS}
        unit_errors: list[dict] = []
        try:
            conn = get_sql_connection(unit_key)
            if not conn:
                raise RuntimeError("Database connection failed.")
            schema = _schema(conn, unit_key)
            unit_frames["tracking"] = _fetch_tracking_summary(conn, schema, unit_key, to_date)
            unit_frames["visit_type"] = _fetch_new_visit_type_details(conn, schema, unit_key, from_date, to_date)
            unit_frames["new_patient_revenue"] = _fetch_new_patient_revenue(conn, schema, unit_key, from_date, to_date)
            unit_frames["period_revenue"] = _fetch_period_revenue(conn, schema, unit_key, from_date, to_date)
            unit_frames["exceptions"] = _fetch_exceptions(conn, schema, unit_key, from_date, to_date)
        except Exception as exc:
            print(f"Govt. Scheme Tracker export failed for {unit_key}: {exc}")
            unit_errors.append({"unit": unit_key, "message": str(exc)})
        finally:
            try:
                if conn:
                    conn.close()
            except Exception:
                pass
        return unit_frames, unit_errors

    if len(units) > 1:
        with ThreadPoolExecutor(max_workers=min(len(units), len(GOVT_SCHEME_ALLOWED_UNITS))) as executor:
            future_map = {executor.submit(fetch_unit, unit): unit for unit in units}
            for future in as_completed(future_map):
                unit_frames, unit_errors = future.result()
                errors.extend(unit_errors)
                for key, df in unit_frames.items():
                    if df is not None and not df.empty:
                        frames[key].append(df)
    else:
        unit_frames, unit_errors = fetch_unit(units[0])
        errors.extend(unit_errors)
        for key, df in unit_frames.items():
            if df is not None and not df.empty:
                frames[key].append(df)

    grids: dict[str, list[dict]] = {}
    dataframes: dict[str, pd.DataFrame] = {}
    internal_grids: dict[str, list[dict]] = {}
    for grid_key, frame_list in frames.items():
        combined = _sort_grid_df(_concat_frames(frame_list), grid_key)
        if grid_key in INTERNAL_GRID_COLUMNS:
            internal = combined.reindex(columns=INTERNAL_GRID_COLUMNS[grid_key]) if not combined.empty else pd.DataFrame(columns=INTERNAL_GRID_COLUMNS[grid_key])
            internal_grids[grid_key] = _df_records(internal)
        visible = combined.reindex(columns=GRID_COLUMNS[grid_key]) if not combined.empty else pd.DataFrame(columns=GRID_COLUMNS[grid_key])
        dataframes[grid_key] = visible
        grids[grid_key] = _df_records(visible)

    status = "success" if not errors else "partial"
    return {
        "status": status,
        "scheme": GOVT_SCHEME_DEFINITIONS[0]["label"],
        "filters": {"unit": "ALL" if len(units) > 1 else units[0], "units": units, "from_date": from_date, "to_date": to_date},
        "kpis": _build_kpis(dataframes),
        "errors": errors,
        "grids": grids,
        "internal_grids": internal_grids,
    }


def _fetch_tracking_summary(conn, schema: dict, unit: str, to_date: str | None = None) -> pd.DataFrame:
    cols = schema["cols"]
    scheme_sql, params = _scheme_filter(cols)
    date_sql = ""
    if to_date:
        date_sql = f" AND v.{_q_ident(cols['visit_date'])} < DATEADD(DAY, 1, CAST(? AS DATETIME))"
        params = params + [to_date]
    sql = f"""
        SELECT
            CAST(? AS NVARCHAR(20)) AS Unit,
            {_sql_pat_type_expr(cols)} AS [Patient Type],
            {_sql_pat_subtype_expr(cols)} AS [Patient SubType],
            COUNT_BIG(1) AS [Total Visits],
            COUNT(DISTINCT v.{_q_ident(cols['patient_id'])}) AS [Unique Patients],
            MIN(CAST(v.{_q_ident(cols['visit_date'])} AS DATETIME)) AS [First Visit Date]
        FROM {_table_ref(schema['visit'])} v WITH (NOLOCK)
        WHERE {scheme_sql}
          {date_sql}
        GROUP BY {_sql_pat_type_expr(cols)}, {_sql_pat_subtype_expr(cols)}
        ORDER BY Unit, [Patient Type], [Patient SubType];
    """
    return _read_sql(conn, sql, [unit] + params)


def _fetch_visit_type_summary(conn, schema: dict, unit: str) -> pd.DataFrame:
    cols = schema["cols"]
    scheme_sql, params = _scheme_filter(cols)
    visit_type = _visit_type_expr(cols)
    sql = f"""
        SELECT
            CAST(? AS NVARCHAR(20)) AS Unit,
            {visit_type} AS [Visit Type],
            COUNT(DISTINCT v.{_q_ident(cols['patient_id'])}) AS [Unique Patients],
            COUNT_BIG(1) AS [Total Visits],
            MIN(CAST(v.{_q_ident(cols['visit_date'])} AS DATETIME)) AS [First Visit Date]
        FROM {_table_ref(schema['visit'])} v WITH (NOLOCK)
        WHERE {scheme_sql}
        GROUP BY {visit_type}
        ORDER BY Unit, [Visit Type];
    """
    return _read_sql(conn, sql, [unit] + params)


def _new_visit_type_base_cte(schema: dict) -> tuple[str, list]:
    cols = schema["cols"]
    scheme_sql, params = _scheme_filter(cols)
    visit_type = _visit_type_expr(cols)
    cte = f"""
        scheme_visits AS (
            SELECT
                v.{_q_ident(cols['visit_id'])} AS VisitID,
                v.{_q_ident(cols['patient_id'])} AS PatientID,
                {_sql_num('v', cols.get('doc_in_charge'), 'INT')} AS DocInChargeID,
                {_sql_text('v', cols.get('visit_no'), 80)} AS VisitNo,
                CAST(v.{_q_ident(cols['visit_date'])} AS DATETIME) AS VisitDate,
                {visit_type} AS VisitType,
                {_sql_pat_type_expr(cols)} AS PatientType,
                {_sql_pat_subtype_expr(cols)} AS PatientSubType
            FROM {_table_ref(schema['visit'])} v WITH (NOLOCK)
            WHERE {scheme_sql}
        ),
        first_visit_type AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY PatientID, VisitType
                    ORDER BY VisitDate, VisitID
                ) AS rn
            FROM scheme_visits
        )
    """
    return cte, params


def _fetch_new_visit_type_summary(conn, schema: dict, unit: str, from_date: str, to_date: str) -> pd.DataFrame:
    cte, params = _new_visit_type_base_cte(schema)
    sql = f"""
        ;WITH {cte}
        SELECT
            CAST(? AS NVARCHAR(20)) AS Unit,
            VisitType AS [Visit Type],
            COUNT(DISTINCT PatientID) AS [Unique Patients],
            COUNT(1) AS [Total Visits],
            MIN(VisitDate) AS [First Visit Date]
        FROM first_visit_type
        WHERE rn = 1
          AND VisitDate >= CAST(? AS DATETIME)
          AND VisitDate < DATEADD(DAY, 1, CAST(? AS DATETIME))
        GROUP BY VisitType
        ORDER BY Unit, [Visit Type];
    """
    return _read_sql(conn, sql, params + [unit, from_date, to_date])


def _fetch_new_visit_type_details(conn, schema: dict, unit: str, from_date: str, to_date: str) -> pd.DataFrame:
    cte, params = _new_visit_type_base_cte(schema)
    sql = f"""
        ;WITH {cte}
        SELECT
            CAST(? AS NVARCHAR(20)) AS Unit,
            fvt.VisitType AS [Visit Type],
            {_patient_reg_expr('p')} AS [Reg No],
            {_patient_name_expr('p', 'tm')} AS [Patient Name],
            fvt.VisitNo AS [Visit No],
            fvt.VisitDate AS [First Visit Date],
            {_doctor_name_expr(schema, 'e')} AS DocInCharge,
            {_doctor_department_expr(schema, 'e', 'dm')} AS Department,
            fvt.PatientType AS [Patient Type],
            fvt.PatientSubType AS [Patient SubType]
        FROM first_visit_type fvt
        {_patient_identity_join('fvt.PatientID', 'p', 'tm')}
        {_doctor_context_join(schema, employee_alias='e', dept_alias='dm', doc_id_sql='fvt.DocInChargeID')}
        WHERE fvt.rn = 1
          AND fvt.VisitDate >= CAST(? AS DATETIME)
          AND fvt.VisitDate < DATEADD(DAY, 1, CAST(? AS DATETIME))
        ORDER BY VisitDate DESC, [Patient Name];
    """
    return _read_sql(conn, sql, params + [unit, from_date, to_date])


def _fetch_new_patient_revenue(conn, schema: dict, unit: str, from_date: str, to_date: str) -> pd.DataFrame:
    """Fetch first-ever visit-type patients, then attach billing totals for those exact visits."""
    base = _fetch_new_patient_revenue_base(conn, schema, unit, from_date, to_date)
    return _attach_first_visit_billing_totals(conn, schema, base, to_date)


def _fetch_new_patient_revenue_base(conn, schema: dict, unit: str, from_date: str, to_date: str) -> pd.DataFrame:
    cols = schema["cols"]
    if not schema.get("visit"):
        return pd.DataFrame(columns=GRID_COLUMNS["new_patient_revenue"])
    scheme_sql, params = _scheme_filter(cols)
    visit_type = _visit_type_expr(cols)
    sql = f"""
        ;WITH scheme_visits AS (
            SELECT
                v.{_q_ident(cols['visit_id'])} AS VisitID,
                v.{_q_ident(cols['patient_id'])} AS PatientID,
                {_sql_num('v', cols.get('doc_in_charge'), 'INT')} AS DocInChargeID,
                CAST(v.{_q_ident(cols['visit_date'])} AS DATETIME) AS VisitDate,
                {visit_type} AS VisitType,
                ROW_NUMBER() OVER (
                    PARTITION BY v.{_q_ident(cols['patient_id'])}, {visit_type}
                    ORDER BY CAST(v.{_q_ident(cols['visit_date'])} AS DATETIME), v.{_q_ident(cols['visit_id'])}
                ) AS rn
            FROM {_table_ref(schema['visit'])} v WITH (NOLOCK)
            WHERE {scheme_sql}
        ),
        first_patients AS (
            SELECT VisitID, PatientID, VisitType, VisitDate, DocInChargeID
            FROM scheme_visits
            WHERE rn = 1
              AND VisitDate >= CAST(? AS DATETIME)
              AND VisitDate < DATEADD(DAY, 1, CAST(? AS DATETIME))
        )
        SELECT
            CAST(? AS NVARCHAR(20)) AS Unit,
            fp.VisitID,
            {_patient_reg_expr('p')} AS [Reg No],
            {_patient_name_expr('p', 'tm')} AS [Patient Name],
            fp.VisitType AS [Visit Type],
            fp.VisitDate AS [First Visit Date],
            {_doctor_name_expr(schema, 'e')} AS DocInCharge,
            {_doctor_department_expr(schema, 'e', 'dm')} AS Department,
            CAST(0 AS DECIMAL(18,2)) AS [Total Revenue],
            CAST(0 AS DECIMAL(18,2)) AS PHAmount,
            CAST(0 AS DECIMAL(18,2)) AS PHReturnAmount,
            CAST(0 AS INT) AS [Billing Count],
            CAST(0 AS INT) AS [Hospital Bill Count]
        FROM first_patients fp
        {_patient_identity_join('fp.PatientID', 'p', 'tm')}
        {_doctor_context_join(schema, employee_alias='e', dept_alias='dm', doc_id_sql='fp.DocInChargeID')}
        ORDER BY fp.VisitDate DESC, [Patient Name];
    """
    return _read_sql(conn, sql, params + [from_date, to_date] + [unit])


def _attach_first_visit_billing_totals(conn, schema: dict, df: pd.DataFrame, to_date: str) -> pd.DataFrame:
    cols = schema["cols"]
    if df is None or df.empty:
        return pd.DataFrame(columns=GRID_COLUMNS["new_patient_revenue"])
    out = df.copy()
    if not (schema.get("billing") and cols.get("bill_id") and cols.get("bill_visit_id")):
        out = out.drop(columns=["VisitID"], errors="ignore")
        return out.reindex(columns=INTERNAL_GRID_COLUMNS["new_patient_revenue"])
    visit_ids = pd.to_numeric(out.get("VisitID"), errors="coerce").dropna().astype(int).drop_duplicates().tolist()
    totals = {}
    for idx in range(0, len(visit_ids), 900):
        chunk = visit_ids[idx:idx + 900]
        if not chunk:
            continue
        values_sql = ", ".join(["(?)"] * len(chunk))
        sql = f"""
            SELECT
                src.VisitID,
                COUNT(DISTINCT bm.{_q_ident(cols['bill_id'])}) AS [Billing Count],
                COUNT(DISTINCT CASE WHEN {_bill_type_expr(cols)} = N'P' THEN bm.{_q_ident(cols['bill_id'])} END) AS [Hospital Bill Count],
                CAST(ISNULL(SUM({_sql_money('bm', cols.get('net'))}), 0) AS DECIMAL(18,2)) AS [Total Revenue],
                CAST(ISNULL(SUM({_sql_money('bm', cols.get('ph_amount'))}), 0) AS DECIMAL(18,2)) AS PHAmount,
                CAST(ISNULL(SUM({_sql_money('bm', cols.get('ph_return_amount'))}), 0) AS DECIMAL(18,2)) AS PHReturnAmount
            FROM (VALUES {values_sql}) src(VisitID)
            LEFT JOIN {_table_ref(schema['billing'])} bm WITH (NOLOCK)
                ON bm.{_q_ident(cols['bill_visit_id'])} = src.VisitID
               AND {_bill_cancel_pred(cols)}
               AND {_bill_to_date_pred(cols)}
            GROUP BY src.VisitID;
        """
        total_df = _read_sql(conn, sql, chunk + _bill_to_date_params(cols, to_date))
        for row in _df_records(total_df):
            try:
                totals[int(row.get("VisitID"))] = row
            except Exception:
                continue
    for col in ["Total Revenue", "PHAmount", "PHReturnAmount", "Billing Count", "Hospital Bill Count"]:
        out[col] = out["VisitID"].map(lambda visit_id: (totals.get(int(visit_id), {}) if pd.notna(visit_id) else {}).get(col, 0))
    out = out.drop(columns=["VisitID"], errors="ignore")
    return out.reindex(columns=INTERNAL_GRID_COLUMNS["new_patient_revenue"])


def _fetch_period_revenue(conn, schema: dict, unit: str, from_date: str, to_date: str) -> pd.DataFrame:
    cols = schema["cols"]
    if not schema.get("billing") or not cols.get("bill_id") or not cols.get("bill_date"):
        return pd.DataFrame(columns=_period_revenue_columns())
    scheme_sql, params = _scheme_filter(cols)
    visit_type = _visit_type_expr(cols)
    amount_exprs = _period_bill_amount_exprs(cols)
    bill_paid_select = f"{amount_exprs['paid']} AS [Paid Amount]," if cols.get("paid") else ""
    receipt_cte = ""
    receipt_join = ""
    paid_select = "pb.[Paid Amount]"
    if not cols.get("paid"):
        if _has_receipt_amount(schema):
            receipt_cte = f""",
        receipt_totals AS (
            SELECT
                CONVERT(INT, rd.{_q_ident(cols['receipt_invoice'])}) AS BillID,
                CAST(ISNULL(SUM(ISNULL(rd.{_q_ident(cols['receipt_amount'])}, 0)), 0) AS DECIMAL(18,2)) AS PaidAmount
            FROM {_table_ref(schema['receipt_dtls'])} rd WITH (NOLOCK)
            INNER JOIN {_table_ref(schema['receipt_mst'])} rm WITH (NOLOCK)
                ON rm.{_q_ident(cols['receipt_id'])} = rd.{_q_ident(cols['receipt_detail_receipt_id'])}
            INNER JOIN period_bills pb
                ON ISNUMERIC(rd.{_q_ident(cols['receipt_invoice'])}) = 1
               AND CONVERT(INT, rd.{_q_ident(cols['receipt_invoice'])}) = pb.BillID
            WHERE {_receipt_cancel_pred(cols)}
            GROUP BY CONVERT(INT, rd.{_q_ident(cols['receipt_invoice'])})
        )"""
            receipt_join = "LEFT JOIN receipt_totals rec ON rec.BillID = pb.BillID"
            paid_select = "CASE WHEN pb.BillType = N'P' THEN ISNULL(rec.PaidAmount, 0) ELSE CAST(0 AS DECIMAL(18,2)) END"
        else:
            paid_select = "CAST(0 AS DECIMAL(18,2))"
    sql = f"""
        ;WITH period_bills AS (
            SELECT
                bm.{_q_ident(cols['bill_id'])} AS BillID,
                {amount_exprs['bill_type']} AS BillType,
                CAST(? AS NVARCHAR(20)) AS Unit,
                CAST(bm.{_q_ident(cols['bill_date'])} AS DATETIME) AS [Bill Date],
                v.{_q_ident(cols['patient_id'])} AS PatientID,
                {_sql_text('v', cols.get('visit_no'), 80)} AS [Visit No],
                {visit_type} AS [Visit Type],
                {_doctor_name_expr(schema, 'e')} AS DocInCharge,
                {_doctor_department_expr(schema, 'e', 'dm')} AS Department,
                {_sql_text('bm', cols.get('bill_no'), 80)} AS [Bill No],
                {amount_exprs['gross']} AS [Gross Amount],
                {amount_exprs['discount']} AS Discount,
                {amount_exprs['net']} AS [Net Amount],
                {amount_exprs['ph_amount']} AS PHAmount,
                {amount_exprs['ph_return_amount']} AS PHReturnAmount,
                {bill_paid_select}
                {amount_exprs['due']} AS [Due Amount]
            FROM {_table_ref(schema['billing'])} bm WITH (NOLOCK)
            INNER JOIN {_table_ref(schema['visit'])} v WITH (NOLOCK)
                ON {_bill_join(cols)}
            {_doctor_context_join(schema, 'v', 'e', 'dm')}
            WHERE {scheme_sql}
              AND {_bill_cancel_pred(cols)}
              AND bm.{_q_ident(cols['bill_date'])} >= CAST(? AS DATETIME)
              AND bm.{_q_ident(cols['bill_date'])} < DATEADD(DAY, 1, CAST(? AS DATETIME))
        )
        {receipt_cte}
        SELECT
            pb.Unit,
            pb.[Bill Date],
            pb.PatientID,
            pb.[Visit No],
            pb.[Visit Type],
            pb.DocInCharge,
            pb.Department,
            pb.[Bill No],
            pb.BillType,
            pb.[Gross Amount],
            pb.Discount,
            pb.[Net Amount],
            pb.PHAmount,
            pb.PHReturnAmount,
            {paid_select} AS [Paid Amount],
            pb.[Due Amount]
        FROM period_bills pb
        {receipt_join}
        ORDER BY pb.[Bill Date] DESC, pb.[Bill No];
    """
    df = _read_sql(conn, sql, [unit] + params + [from_date, to_date])
    return _attach_patient_identity(conn, df)


def _fetch_exceptions(conn, schema: dict, unit: str, from_date: str, to_date: str) -> pd.DataFrame:
    cols = schema["cols"]
    pt_expr = _sql_pat_type_expr(cols)
    subtype_expr = _sql_pat_subtype_expr(cols)
    exact_sql, exact_params = _scheme_filter(cols)
    visit_type = _visit_type_expr(cols)

    if schema.get("billing") and cols.get("bill_id"):
        bill_apply = f"""
            OUTER APPLY (
                SELECT
                    COUNT(DISTINCT bm.{_q_ident(cols['bill_id'])}) AS BillingCount,
                    CAST(ISNULL(SUM({_sql_money('bm', cols.get('net'))}), 0) AS DECIMAL(18,2)) AS Revenue
                FROM {_table_ref(schema['billing'])} bm WITH (NOLOCK)
                WHERE {_bill_join(cols)}
                  AND {_bill_cancel_pred(cols)}
                  AND {_bill_to_date_pred(cols)}
            ) bill
        """
    else:
        bill_apply = "OUTER APPLY (SELECT CAST(0 AS INT) AS BillingCount, CAST(0 AS DECIMAL(18,2)) AS Revenue) bill"

    sql = f"""
        SELECT
            CAST(? AS NVARCHAR(20)) AS Unit,
            {_patient_reg_expr('p')} AS [Reg No],
            {_patient_name_expr('p', 'tm')} AS [Patient Name],
            {visit_type} AS [Visit Type],
            CAST(v.{_q_ident(cols['visit_date'])} AS DATETIME) AS [Visit Date],
            {_doctor_name_expr(schema, 'e')} AS DocInCharge,
            {_doctor_department_expr(schema, 'e', 'dm')} AS Department,
            {pt_expr} AS [Patient Type],
            {subtype_expr} AS [Patient SubType],
            CASE
                WHEN ISNULL(bill.BillingCount, 0) = 0 THEN N'Missing billing'
                WHEN ISNULL(bill.Revenue, 0) = 0 THEN N'Zero revenue'
                ELSE N'OK'
            END AS [Billing Status],
            ISNULL(bill.Revenue, 0) AS Revenue,
            CASE
                WHEN ISNULL(bill.BillingCount, 0) = 0 THEN N'No active billing found for this MGBUY visit.'
                WHEN ISNULL(bill.Revenue, 0) = 0 THEN N'Active billing found, but net revenue is zero.'
                ELSE N''
            END AS [Remarks / Exception Reason]
        FROM {_table_ref(schema['visit'])} v WITH (NOLOCK)
        {_patient_identity_join(f"v.{_q_ident(cols['patient_id'])}", 'p', 'tm')}
        {_doctor_context_join(schema, 'v', 'e', 'dm')}
        {bill_apply}
        WHERE v.{_q_ident(cols['visit_date'])} >= CAST(? AS DATETIME)
          AND v.{_q_ident(cols['visit_date'])} < DATEADD(DAY, 1, CAST(? AS DATETIME))
          AND ({exact_sql})
          AND (
                ISNULL(bill.BillingCount, 0) = 0
                OR ISNULL(bill.Revenue, 0) = 0
          )
        ORDER BY [Visit Date] DESC, [Patient Name];
    """
    params = [unit] + _bill_to_date_params(cols, to_date) + [from_date, to_date] + exact_params
    return _read_sql(conn, sql, params)


def _fetch_new_patient_revenue_summary(conn, schema: dict, unit: str, from_date: str, to_date: str) -> pd.DataFrame:
    columns = ["Unit", "Visit Type", "Total Revenue", "PHAmount", "PHReturnAmount", "Billing Count"]
    details = _fetch_new_patient_revenue(conn, schema, unit, from_date, to_date)
    if details is None or details.empty:
        return pd.DataFrame(columns=columns)
    records = _group_sum_records(details, ["Unit", "Visit Type"], ["Total Revenue", "PHAmount", "PHReturnAmount", "Billing Count"])
    return pd.DataFrame(records).reindex(columns=columns)


def _fetch_period_revenue_summary(conn, schema: dict, unit: str, from_date: str, to_date: str) -> pd.DataFrame:
    cols = schema["cols"]
    if not schema.get("billing") or not cols.get("bill_id") or not cols.get("bill_date"):
        return pd.DataFrame(columns=["Unit", "Visit Type", "Gross Amount", "Discount", "Net Amount", "PHAmount", "PHReturnAmount", "Paid Amount", "Due Amount", "Bill Count"])
    scheme_sql, params = _scheme_filter(cols)
    visit_type = _visit_type_expr(cols)
    amount_exprs = _period_bill_amount_exprs(cols)
    bill_paid_select = f"{amount_exprs['paid']} AS PaidAmount," if cols.get("paid") else ""
    receipt_cte = ""
    receipt_join = ""
    paid_sum_expr = "pb.PaidAmount"
    if not cols.get("paid"):
        if _has_receipt_amount(schema):
            receipt_cte = f""",
        receipt_totals AS (
            SELECT
                CONVERT(INT, rd.{_q_ident(cols['receipt_invoice'])}) AS BillID,
                CAST(ISNULL(SUM(ISNULL(rd.{_q_ident(cols['receipt_amount'])}, 0)), 0) AS DECIMAL(18,2)) AS PaidAmount
            FROM {_table_ref(schema['receipt_dtls'])} rd WITH (NOLOCK)
            INNER JOIN {_table_ref(schema['receipt_mst'])} rm WITH (NOLOCK)
                ON rm.{_q_ident(cols['receipt_id'])} = rd.{_q_ident(cols['receipt_detail_receipt_id'])}
            INNER JOIN period_bills pb
                ON ISNUMERIC(rd.{_q_ident(cols['receipt_invoice'])}) = 1
               AND CONVERT(INT, rd.{_q_ident(cols['receipt_invoice'])}) = pb.BillID
            WHERE {_receipt_cancel_pred(cols)}
            GROUP BY CONVERT(INT, rd.{_q_ident(cols['receipt_invoice'])})
        )"""
            receipt_join = "LEFT JOIN receipt_totals rec ON rec.BillID = pb.BillID"
            paid_sum_expr = "CASE WHEN pb.BillType = N'P' THEN ISNULL(rec.PaidAmount, 0) ELSE CAST(0 AS DECIMAL(18,2)) END"
        else:
            paid_sum_expr = "CAST(0 AS DECIMAL(18,2))"
    sql = f"""
        ;WITH period_bills AS (
            SELECT
                bm.{_q_ident(cols['bill_id'])} AS BillID,
                {amount_exprs['bill_type']} AS BillType,
                CAST(? AS NVARCHAR(20)) AS Unit,
                {visit_type} AS VisitType,
                {amount_exprs['gross']} AS GrossAmount,
                {amount_exprs['discount']} AS DiscountAmount,
                {amount_exprs['net']} AS NetAmount,
                {amount_exprs['ph_amount']} AS PHAmount,
                {amount_exprs['ph_return_amount']} AS PHReturnAmount,
                {bill_paid_select}
                {amount_exprs['due']} AS DueAmount
            FROM {_table_ref(schema['billing'])} bm WITH (NOLOCK)
            INNER JOIN {_table_ref(schema['visit'])} v WITH (NOLOCK)
                ON {_bill_join(cols)}
            WHERE {scheme_sql}
              AND {_bill_cancel_pred(cols)}
              AND bm.{_q_ident(cols['bill_date'])} >= CAST(? AS DATETIME)
              AND bm.{_q_ident(cols['bill_date'])} < DATEADD(DAY, 1, CAST(? AS DATETIME))
        )
        {receipt_cte}
        SELECT
            pb.Unit,
            pb.VisitType AS [Visit Type],
            CAST(ISNULL(SUM(pb.GrossAmount), 0) AS DECIMAL(18,2)) AS [Gross Amount],
            CAST(ISNULL(SUM(pb.DiscountAmount), 0) AS DECIMAL(18,2)) AS Discount,
            CAST(ISNULL(SUM(pb.NetAmount), 0) AS DECIMAL(18,2)) AS [Net Amount],
            CAST(ISNULL(SUM(pb.PHAmount), 0) AS DECIMAL(18,2)) AS PHAmount,
            CAST(ISNULL(SUM(pb.PHReturnAmount), 0) AS DECIMAL(18,2)) AS PHReturnAmount,
            CAST(ISNULL(SUM({paid_sum_expr}), 0) AS DECIMAL(18,2)) AS [Paid Amount],
            CAST(ISNULL(SUM(pb.DueAmount), 0) AS DECIMAL(18,2)) AS [Due Amount],
            COUNT(DISTINCT pb.BillID) AS [Bill Count]
        FROM period_bills pb
        {receipt_join}
        GROUP BY pb.Unit, pb.VisitType
        ORDER BY Unit, [Visit Type];
    """
    return _read_sql(conn, sql, [unit] + params + [from_date, to_date])


def _fetch_exception_status_summary(conn, schema: dict, unit: str, from_date: str, to_date: str) -> pd.DataFrame:
    # Count-only version avoids loading exception detail rows.
    cols = schema["cols"]
    pt_expr = _sql_pat_type_expr(cols)
    subtype_expr = _sql_pat_subtype_expr(cols)
    exact_sql, exact_params = _scheme_filter(cols)
    if schema.get("billing") and cols.get("bill_id"):
        bill_apply = f"""
            OUTER APPLY (
                SELECT
                    COUNT(DISTINCT bm.{_q_ident(cols['bill_id'])}) AS BillingCount,
                    CAST(ISNULL(SUM({_sql_money('bm', cols.get('net'))}), 0) AS DECIMAL(18,2)) AS Revenue
                FROM {_table_ref(schema['billing'])} bm WITH (NOLOCK)
                WHERE {_bill_join(cols)}
                  AND {_bill_cancel_pred(cols)}
                  AND {_bill_to_date_pred(cols)}
            ) bill
        """
    else:
        bill_apply = "OUTER APPLY (SELECT CAST(0 AS INT) AS BillingCount, CAST(0 AS DECIMAL(18,2)) AS Revenue) bill"
    sql = f"""
        SELECT
            CAST(? AS NVARCHAR(20)) AS Unit,
            StatusName AS [Billing Status],
            COUNT(1) AS [Count]
        FROM (
            SELECT
                CASE
                    WHEN ISNULL(bill.BillingCount, 0) = 0 THEN N'Missing billing'
                    WHEN ISNULL(bill.Revenue, 0) = 0 THEN N'Zero revenue'
                    ELSE N'OK'
                END AS StatusName
            FROM {_table_ref(schema['visit'])} v WITH (NOLOCK)
            {bill_apply}
            WHERE v.{_q_ident(cols['visit_date'])} >= CAST(? AS DATETIME)
              AND v.{_q_ident(cols['visit_date'])} < DATEADD(DAY, 1, CAST(? AS DATETIME))
              AND ({exact_sql})
              AND (
                    ISNULL(bill.BillingCount, 0) = 0
                    OR ISNULL(bill.Revenue, 0) = 0
              )
        ) x
        GROUP BY StatusName
        ORDER BY StatusName;
    """
    params = [unit] + _bill_to_date_params(cols, to_date) + [from_date, to_date] + exact_params
    return _read_sql(conn, sql, params)


def _fetch_new_patient_revenue_page(conn, schema: dict, unit: str, from_date: str, to_date: str, page: int, page_size: int, search: str) -> tuple[pd.DataFrame, int]:
    details = _filter_df_search(_fetch_new_patient_revenue(conn, schema, unit, from_date, to_date), search)
    total = int(len(details)) if details is not None else 0
    return _slice_df_page(details, page, page_size).reindex(columns=GRID_COLUMNS["new_patient_revenue"]), total


def _fetch_new_visit_type_detail_page(conn, schema: dict, unit: str, from_date: str, to_date: str, page: int, page_size: int, search: str) -> tuple[pd.DataFrame, int]:
    cte, params = _new_visit_type_base_cte(schema)
    base_sql = f"""
        {cte},
        base_data AS (
            SELECT
                CAST(? AS NVARCHAR(20)) AS Unit,
                fvt.VisitType AS [Visit Type],
                {_patient_reg_expr('p')} AS [Reg No],
                {_patient_name_expr('p', 'tm')} AS [Patient Name],
                fvt.VisitNo AS [Visit No],
                fvt.VisitDate AS [First Visit Date],
                {_doctor_name_expr(schema, 'e')} AS DocInCharge,
                {_doctor_department_expr(schema, 'e', 'dm')} AS Department,
                fvt.PatientType AS [Patient Type],
                fvt.PatientSubType AS [Patient SubType]
            FROM first_visit_type fvt
            {_patient_identity_join('fvt.PatientID', 'p', 'tm')}
            {_doctor_context_join(schema, employee_alias='e', dept_alias='dm', doc_id_sql='fvt.DocInChargeID')}
            WHERE fvt.rn = 1
              AND fvt.VisitDate >= CAST(? AS DATETIME)
              AND fvt.VisitDate < DATEADD(DAY, 1, CAST(? AS DATETIME))
        )
    """
    return _paged_cte(
        conn,
        base_sql,
        params + [unit, from_date, to_date],
        GRID_COLUMNS["visit_type"],
        "[First Visit Date] DESC, [Patient Name]",
        page,
        page_size,
        search,
    )


def _fetch_period_revenue_page(conn, schema: dict, unit: str, from_date: str, to_date: str, page: int, page_size: int, search: str) -> tuple[pd.DataFrame, int]:
    cols = schema["cols"]
    if not schema.get("billing") or not cols.get("bill_id") or not cols.get("bill_date"):
        return pd.DataFrame(columns=GRID_COLUMNS["period_revenue"]), 0
    scheme_sql, params = _scheme_filter(cols)
    visit_type = _visit_type_expr(cols)
    amount_exprs = _period_bill_amount_exprs(cols)
    include_patient_names = bool(str(search or "").strip())
    reg_expr = _patient_reg_expr("p") if include_patient_names else "CAST(N'' AS NVARCHAR(80))"
    name_expr = _patient_name_expr("p", "tm") if include_patient_names else "CAST(N'' AS NVARCHAR(240))"
    patient_join_sql = _patient_identity_join(f"v.{_q_ident(cols['patient_id'])}", "p", "tm") if include_patient_names else ""
    bill_paid_select = f"{amount_exprs['paid']} AS [Paid Amount]," if cols.get("paid") else ""
    receipt_cte = ""
    receipt_join = ""
    paid_select = "pb.[Paid Amount]"
    if not cols.get("paid"):
        if _has_receipt_amount(schema):
            receipt_cte = f""",
        receipt_totals AS (
            SELECT
                CONVERT(INT, rd.{_q_ident(cols['receipt_invoice'])}) AS BillID,
                CAST(ISNULL(SUM(ISNULL(rd.{_q_ident(cols['receipt_amount'])}, 0)), 0) AS DECIMAL(18,2)) AS PaidAmount
            FROM {_table_ref(schema['receipt_dtls'])} rd WITH (NOLOCK)
            INNER JOIN {_table_ref(schema['receipt_mst'])} rm WITH (NOLOCK)
                ON rm.{_q_ident(cols['receipt_id'])} = rd.{_q_ident(cols['receipt_detail_receipt_id'])}
            INNER JOIN period_bills pb
                ON ISNUMERIC(rd.{_q_ident(cols['receipt_invoice'])}) = 1
               AND CONVERT(INT, rd.{_q_ident(cols['receipt_invoice'])}) = pb.BillID
            WHERE {_receipt_cancel_pred(cols)}
            GROUP BY CONVERT(INT, rd.{_q_ident(cols['receipt_invoice'])})
        )"""
            receipt_join = "LEFT JOIN receipt_totals rec ON rec.BillID = pb.BillID"
            paid_select = "CASE WHEN pb.BillType = N'P' THEN ISNULL(rec.PaidAmount, 0) ELSE CAST(0 AS DECIMAL(18,2)) END"
        else:
            paid_select = "CAST(0 AS DECIMAL(18,2))"
    base_sql = f"""
        period_bills AS (
            SELECT
                bm.{_q_ident(cols['bill_id'])} AS BillID,
                {amount_exprs['bill_type']} AS BillType,
                CAST(? AS NVARCHAR(20)) AS Unit,
                CAST(bm.{_q_ident(cols['bill_date'])} AS DATETIME) AS [Bill Date],
                v.{_q_ident(cols['patient_id'])} AS PatientID,
                {reg_expr} AS [Reg No],
                {name_expr} AS [Patient Name],
                {_sql_text('v', cols.get('visit_no'), 80)} AS [Visit No],
                {visit_type} AS [Visit Type],
                {_doctor_name_expr(schema, 'e')} AS DocInCharge,
                {_doctor_department_expr(schema, 'e', 'dm')} AS Department,
                {_sql_text('bm', cols.get('bill_no'), 80)} AS [Bill No],
                {amount_exprs['gross']} AS [Gross Amount],
                {amount_exprs['discount']} AS Discount,
                {amount_exprs['net']} AS [Net Amount],
                {amount_exprs['ph_amount']} AS PHAmount,
                {amount_exprs['ph_return_amount']} AS PHReturnAmount,
                {bill_paid_select}
                {amount_exprs['due']} AS [Due Amount]
            FROM {_table_ref(schema['billing'])} bm WITH (NOLOCK)
            INNER JOIN {_table_ref(schema['visit'])} v WITH (NOLOCK)
                ON {_bill_join(cols)}
            {patient_join_sql}
            {_doctor_context_join(schema, 'v', 'e', 'dm')}
            WHERE {scheme_sql}
              AND {_bill_cancel_pred(cols)}
              AND bm.{_q_ident(cols['bill_date'])} >= CAST(? AS DATETIME)
              AND bm.{_q_ident(cols['bill_date'])} < DATEADD(DAY, 1, CAST(? AS DATETIME))
        )
        {receipt_cte},
        base_data AS (
            SELECT
                pb.Unit,
                pb.[Bill Date],
                pb.PatientID,
                pb.[Reg No],
                pb.[Patient Name],
                pb.[Visit No],
                pb.[Visit Type],
                pb.DocInCharge,
                pb.Department,
                pb.[Bill No],
                pb.BillType,
                pb.[Gross Amount],
                pb.Discount,
                pb.[Net Amount],
                pb.PHAmount,
                pb.PHReturnAmount,
                {paid_select} AS [Paid Amount],
                pb.[Due Amount]
            FROM period_bills pb
            {receipt_join}
        )
    """
    df, total = _paged_cte(
        conn,
        base_sql,
        [unit] + params + [from_date, to_date],
        GRID_COLUMNS["period_revenue"],
        "[Bill Date] DESC, [Bill No]",
        page,
        page_size,
        search,
        extra_columns=["PatientID"],
    )
    if not include_patient_names:
        df = _attach_patient_identity(conn, df)
    return df, total


def _fetch_exceptions_page(conn, schema: dict, unit: str, from_date: str, to_date: str, page: int, page_size: int, search: str) -> tuple[pd.DataFrame, int]:
    cols = schema["cols"]
    pt_expr = _sql_pat_type_expr(cols)
    subtype_expr = _sql_pat_subtype_expr(cols)
    exact_sql, exact_params = _scheme_filter(cols)
    visit_type = _visit_type_expr(cols)
    if schema.get("billing") and cols.get("bill_id"):
        bill_apply = f"""
            OUTER APPLY (
                SELECT
                    COUNT(DISTINCT bm.{_q_ident(cols['bill_id'])}) AS BillingCount,
                    CAST(ISNULL(SUM({_sql_money('bm', cols.get('net'))}), 0) AS DECIMAL(18,2)) AS Revenue
                FROM {_table_ref(schema['billing'])} bm WITH (NOLOCK)
                WHERE {_bill_join(cols)}
                  AND {_bill_cancel_pred(cols)}
                  AND {_bill_to_date_pred(cols)}
            ) bill
        """
    else:
        bill_apply = "OUTER APPLY (SELECT CAST(0 AS INT) AS BillingCount, CAST(0 AS DECIMAL(18,2)) AS Revenue) bill"
    base_sql = f"""
        base_data AS (
            SELECT
                CAST(? AS NVARCHAR(20)) AS Unit,
                {_patient_reg_expr('p')} AS [Reg No],
                {_patient_name_expr('p', 'tm')} AS [Patient Name],
                {visit_type} AS [Visit Type],
                CAST(v.{_q_ident(cols['visit_date'])} AS DATETIME) AS [Visit Date],
                {_doctor_name_expr(schema, 'e')} AS DocInCharge,
                {_doctor_department_expr(schema, 'e', 'dm')} AS Department,
                {pt_expr} AS [Patient Type],
                {subtype_expr} AS [Patient SubType],
                CASE
                    WHEN ISNULL(bill.BillingCount, 0) = 0 THEN N'Missing billing'
                    WHEN ISNULL(bill.Revenue, 0) = 0 THEN N'Zero revenue'
                    ELSE N'OK'
                END AS [Billing Status],
                ISNULL(bill.Revenue, 0) AS Revenue,
                CASE
                    WHEN ISNULL(bill.BillingCount, 0) = 0 THEN N'No active billing found for this MGBUY visit.'
                    WHEN ISNULL(bill.Revenue, 0) = 0 THEN N'Active billing found, but net revenue is zero.'
                    ELSE N''
                END AS [Remarks / Exception Reason]
            FROM {_table_ref(schema['visit'])} v WITH (NOLOCK)
            {_patient_identity_join(f"v.{_q_ident(cols['patient_id'])}", 'p', 'tm')}
            {_doctor_context_join(schema, 'v', 'e', 'dm')}
            {bill_apply}
            WHERE v.{_q_ident(cols['visit_date'])} >= CAST(? AS DATETIME)
              AND v.{_q_ident(cols['visit_date'])} < DATEADD(DAY, 1, CAST(? AS DATETIME))
              AND ({exact_sql})
              AND (
                    ISNULL(bill.BillingCount, 0) = 0
                    OR ISNULL(bill.Revenue, 0) = 0
              )
        )
    """
    params = [unit] + _bill_to_date_params(cols, to_date) + [from_date, to_date] + exact_params
    return _paged_cte(
        conn,
        base_sql,
        params,
        GRID_COLUMNS["exceptions"],
        "[Visit Date] DESC, [Patient Name]",
        page,
        page_size,
        search,
    )


def _paged_cte(
    conn,
    base_cte_sql: str,
    base_params: list,
    columns: list[str],
    order_by: str,
    page: int,
    page_size: int,
    search: str,
    extra_columns: list[str] | None = None,
) -> tuple[pd.DataFrame, int]:
    start_row = ((page - 1) * page_size) + 1
    end_row = page * page_size
    search_sql = ""
    params = list(base_params)
    output_columns = list(columns) + [col for col in (extra_columns or []) if col not in columns]
    if search:
        expr = " + N' ' + ".join([f"ISNULL(CONVERT(NVARCHAR(4000), {_q_ident(col)}), N'')" for col in columns])
        search_sql = f"WHERE LOWER({expr}) LIKE ?"
        params.append(f"%{search.lower()}%")
    sql = f"""
        ;WITH {base_cte_sql},
        filtered AS (
            SELECT
                *,
                COUNT(1) OVER() AS __TotalRows,
                ROW_NUMBER() OVER(ORDER BY {order_by}) AS __RowNum
            FROM base_data
            {search_sql}
        )
        SELECT {", ".join(_q_ident(col) for col in output_columns)}, __TotalRows
        FROM filtered
        WHERE __RowNum BETWEEN ? AND ?
        ORDER BY __RowNum;
    """
    params.extend([start_row, end_row])
    df = _read_sql(conn, sql, params)
    total = 0
    if "__TotalRows" in df.columns:
        total_value = pd.to_numeric(df["__TotalRows"], errors="coerce").fillna(0).max()
        total = int(total_value) if pd.notna(total_value) else 0
        df = df.drop(columns=["__TotalRows"])
    return df, total


def _attach_patient_identity(conn, df: pd.DataFrame) -> pd.DataFrame:
    """Batch-fill Reg No and honorific patient name from dbo.Patient, then remove PatientID."""
    if df is None or df.empty or "PatientID" not in df.columns:
        return df if df is not None else pd.DataFrame()
    out = df.copy()
    patient_ids = (
        pd.to_numeric(out["PatientID"], errors="coerce")
        .dropna()
        .astype(int)
        .drop_duplicates()
        .tolist()
    )
    identity = {}
    chunk_size = 900
    for idx in range(0, len(patient_ids), chunk_size):
        chunk = patient_ids[idx:idx + chunk_size]
        if not chunk:
            continue
        values_sql = ", ".join(["(?)"] * len(chunk))
        sql = f"""
            SELECT
                src.PatientID,
                {_patient_reg_expr('p')} AS [Reg No],
                {_patient_name_expr('p', 'tm')} AS [Patient Name]
            FROM (VALUES {values_sql}) src(PatientID)
            {_patient_identity_join('src.PatientID', 'p', 'tm')};
        """
        id_df = _read_sql(conn, sql, chunk)
        for row in _df_records(id_df):
            try:
                identity[int(row.get("PatientID"))] = {
                    "Reg No": row.get("Reg No") or "",
                    "Patient Name": row.get("Patient Name") or "",
                }
            except Exception:
                continue
    out["Reg No"] = out["PatientID"].map(lambda pid: identity.get(int(pid), {}).get("Reg No", "") if pd.notna(pid) else "")
    out["Patient Name"] = out["PatientID"].map(lambda pid: identity.get(int(pid), {}).get("Patient Name", "") if pd.notna(pid) else "")
    return out.drop(columns=["PatientID"])


def _filter_df_search(df: pd.DataFrame, search: str) -> pd.DataFrame:
    if df is None or df.empty or not str(search or "").strip():
        return df if df is not None else pd.DataFrame()
    q = str(search).strip().lower()
    mask = df.astype(str).apply(lambda col: col.str.lower().str.contains(re.escape(q), na=False)).any(axis=1)
    return df.loc[mask].copy()


def _slice_df_page(df: pd.DataFrame, page: int, page_size: int) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame() if df is None else df
    start = (page - 1) * page_size
    return df.iloc[start:start + page_size].copy()


def _sort_grid_df(df: pd.DataFrame, grid_key: str) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame() if df is None else df
    sort_map = {
        "tracking": ["Unit", "Patient Type", "Patient SubType"],
        "visit_type": ["Unit", "Visit Type"],
        "new_patient_revenue": ["First Visit Date", "Patient Name"],
        "period_revenue": ["Bill Date", "Bill No"],
        "exceptions": ["Visit Date", "Patient Name"],
    }
    cols = [col for col in sort_map.get(grid_key, []) if col in df.columns]
    if not cols:
        return df
    ascending = [False if "Date" in col else True for col in cols]
    return df.sort_values(cols, ascending=ascending, kind="stable").reset_index(drop=True)


def _period_revenue_columns() -> list[str]:
    return [
        "Unit",
        "Bill Date",
        "Reg No",
        "Patient Name",
        "Visit No",
        "Visit Type",
        "DocInCharge",
        "Department",
        "Bill No",
        "Gross Amount",
        "Discount",
        "Net Amount",
        "PHAmount",
        "PHReturnAmount",
        "Paid Amount",
        "Due Amount",
    ]


def _read_sql(conn, sql: str, params: list) -> pd.DataFrame:
    df = pd.read_sql(sql, conn, params=params)
    if df is None:
        return pd.DataFrame()
    df.columns = [str(col).strip() for col in df.columns]
    return _normalize_frame(df)


def _concat_frames(frames: list[pd.DataFrame]) -> pd.DataFrame:
    valid = [frame for frame in frames if frame is not None and not frame.empty]
    if not valid:
        return pd.DataFrame()
    return _normalize_frame(pd.concat(valid, ignore_index=True, copy=False))


def _normalize_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame() if df is None else df
    out = df.copy()
    date_cols = [col for col in out.columns if "date" in str(col).lower()]
    for col in date_cols:
        out[col] = pd.to_datetime(out[col], errors="coerce")
    for col in out.columns:
        if pd.api.types.is_object_dtype(out[col]):
            out[col] = out[col].fillna("").astype(str).str.strip().replace({"nan": "", "None": "", "NaT": ""})
    return out


def _build_kpis(frames: dict[str, pd.DataFrame]) -> dict:
    tracking = frames.get("tracking", pd.DataFrame())
    visit_type = _visit_type_summary_for_kpi(frames.get("visit_type", pd.DataFrame()))
    new_rev = frames.get("new_patient_revenue", pd.DataFrame())
    period = frames.get("period_revenue", pd.DataFrame())
    exceptions = frames.get("exceptions", pd.DataFrame())

    total_visits = _sum_col(tracking, "Total Visits")
    unique_patients = _sum_col(tracking, "Unique Patients")
    unique_revenue = _new_patient_total_revenue(new_rev)
    period_revenue = _period_total_revenue(period)

    return {
        "total_tracking": {
            "total_visits": total_visits,
            "unique_patients": unique_patients,
            "by_unit": _records_from_group(tracking, "Unit", ["Total Visits", "Unique Patients"]),
        },
        "unique_by_visit_type": {
            "total_unique_patients": _sum_col(visit_type, "Unique Patients"),
            "by_unit_visit_type": _df_records(visit_type),
        },
        "revenue_per_new_patient": {
            "total_revenue": unique_revenue,
            "billing_count": _sum_col(new_rev, "Billing Count"),
            "by_unit_visit_type": _group_sum_records(new_rev, ["Unit", "Visit Type"], ["Total Revenue", "PHAmount", "PHReturnAmount", "Billing Count"]),
        },
        "period_revenue": {
            "total_revenue": period_revenue,
            "bill_count": _count_nonblank(period, "Bill No"),
            "by_unit_visit_type": _group_sum_records(period, ["Unit", "Visit Type"], ["Gross Amount", "Discount", "Net Amount", "PHAmount", "PHReturnAmount", "Paid Amount", "Due Amount"]),
        },
        "exceptions": {
            "count": int(len(exceptions)) if exceptions is not None else 0,
            "by_status": _records_from_group(exceptions, "Billing Status", []),
        },
    }


def _sum_col(df: pd.DataFrame, col: str) -> float:
    if df is None or df.empty or col not in df.columns:
        return 0
    total = pd.to_numeric(df[col], errors="coerce").fillna(0).sum()
    if float(total).is_integer():
        return int(total)
    return round(float(total), 2)


def _period_total_revenue(df: pd.DataFrame) -> float:
    total = _sum_col(df, "Net Amount") + _sum_col(df, "PHAmount") + _sum_col(df, "PHReturnAmount")
    if float(total).is_integer():
        return int(total)
    return round(float(total), 2)


def _new_patient_total_revenue(df: pd.DataFrame) -> float:
    total = _sum_col(df, "Total Revenue") + _sum_col(df, "PHAmount") + _sum_col(df, "PHReturnAmount")
    if float(total).is_integer():
        return int(total)
    return round(float(total), 2)


def _visit_type_summary_for_kpi(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["Unit", "Visit Type", "Unique Patients", "Total Visits", "First Visit Date"])
    if "Unique Patients" in df.columns:
        return df
    if not {"Unit", "Visit Type", "Reg No", "First Visit Date"}.issubset(set(df.columns)):
        return pd.DataFrame(columns=["Unit", "Visit Type", "Unique Patients", "Total Visits", "First Visit Date"])
    work = df.copy()
    work["First Visit Date"] = pd.to_datetime(work["First Visit Date"], errors="coerce")
    grouped = (
        work.groupby(["Unit", "Visit Type"], dropna=False)
        .agg(
            **{
                "Unique Patients": ("Reg No", lambda s: int(s.astype(str).str.strip().replace("", np.nan).dropna().nunique())),
                "Total Visits": ("Reg No", "size"),
                "First Visit Date": ("First Visit Date", "min"),
            }
        )
        .reset_index()
    )
    return _normalize_frame(grouped)


def _count_nonblank(df: pd.DataFrame, col: str) -> int:
    if df is None or df.empty or col not in df.columns:
        return 0
    return int(df[col].astype(str).str.strip().replace({"": np.nan}).dropna().nunique())


def _records_from_group(df: pd.DataFrame, group_col: str, value_cols: list[str]) -> list[dict]:
    if df is None or df.empty or group_col not in df.columns:
        return []
    if value_cols:
        work = df.copy()
        for col in value_cols:
            work[col] = pd.to_numeric(work.get(col), errors="coerce").fillna(0)
        grouped = work.groupby(group_col, dropna=False)[value_cols].sum().reset_index()
    else:
        grouped = df.groupby(group_col, dropna=False).size().reset_index(name="Count")
    return _df_records(grouped)


def _group_sum_records(df: pd.DataFrame, groups: list[str], value_cols: list[str]) -> list[dict]:
    if df is None or df.empty or any(col not in df.columns for col in groups):
        return []
    work = df.copy()
    present_values = [col for col in value_cols if col in work.columns]
    if not present_values:
        return []
    for col in present_values:
        work[col] = pd.to_numeric(work[col], errors="coerce").fillna(0)
    grouped = work.groupby(groups, dropna=False)[present_values].sum().reset_index()
    return _df_records(grouped)


def _df_records(df: pd.DataFrame) -> list[dict]:
    if df is None or df.empty:
        return []
    out = df.copy()
    for col in out.columns:
        if pd.api.types.is_datetime64_any_dtype(out[col]):
            out[col] = out[col].dt.strftime("%Y-%m-%d %H:%M:%S").fillna("")
    out = out.replace({np.nan: None, np.inf: None, -np.inf: None})
    return out.to_dict(orient="records")


def _json_safe(value):
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_safe(v) for v in value]
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        if math.isnan(float(value)) or math.isinf(float(value)):
            return None
        return float(value)
    if pd.isna(value) if not isinstance(value, (str, bytes, list, tuple, dict)) else False:
        return None
    return value


def build_govt_scheme_excel(payload: dict, filters: dict) -> bytes:
    """Build the multi-sheet Govt. Scheme Tracker workbook in the backend."""
    output = io.BytesIO()
    grids = payload.get("grids") or {}
    kpis = payload.get("kpis") or {}

    with pd.ExcelWriter(output, engine="xlsxwriter", datetime_format="yyyy-mm-dd hh:mm:ss", date_format="yyyy-mm-dd") as writer:
        workbook = writer.book
        title_fmt = workbook.add_format({"bold": True, "font_size": 15, "font_color": "#1e3a8a", "align": "center"})
        meta_fmt = workbook.add_format({"font_size": 10, "align": "center", "font_color": "#475569"})
        header_fmt = workbook.add_format({"bold": True, "bg_color": "#1e3a8a", "font_color": "white", "border": 1, "align": "center", "valign": "vcenter"})
        money_fmt = workbook.add_format({"num_format": u'₹ #,##,##0.00', "border": 1, "align": "right"})
        count_fmt = workbook.add_format({"num_format": "#,##,##0", "border": 1, "align": "right"})
        text_fmt = workbook.add_format({"border": 1, "align": "left", "valign": "top"})
        date_fmt = workbook.add_format({"num_format": "yyyy-mm-dd hh:mm:ss", "border": 1, "align": "left"})

        summary_rows = _summary_rows(kpis)
        summary_df = pd.DataFrame(summary_rows, columns=["KPI", "Value"])
        _write_sheet(
            writer,
            summary_df,
            "Summary",
            "Govt. Scheme Tracker - GSS / MGBUY",
            filters,
            header_fmt,
            title_fmt,
            meta_fmt,
            text_fmt,
            count_fmt,
            money_fmt,
            date_fmt,
        )

        _write_sheet(
            writer,
            _department_new_old_summary(payload.get("internal_grids") or grids),
            "Department New Old",
            "Department-wise New vs Old Revenue",
            filters,
            header_fmt,
            title_fmt,
            meta_fmt,
            text_fmt,
            count_fmt,
            money_fmt,
            date_fmt,
        )

        sheet_map = [
            ("tracking", "MGBUY Tracking", "Total MGBUY Patient Tracking"),
            ("visit_type", "New Visit Type Patients", "Unique MGBUY Patients by Visit Type"),
            ("new_patient_revenue", "New Patient Revenue", "Revenue per New / Unique MGBUY Patient"),
            ("period_revenue", "Period Revenue", "All MGBUY Revenue in Date Range"),
            ("exceptions", "Scheme Exceptions", "Scheme Summary / Exception View"),
        ]
        for key, sheet_name, title in sheet_map:
            _write_sheet(
                writer,
                pd.DataFrame(grids.get(key) or []),
                sheet_name,
                title,
                filters,
                header_fmt,
                title_fmt,
                meta_fmt,
                text_fmt,
                count_fmt,
                money_fmt,
                date_fmt,
            )

    return output.getvalue()


def _summary_rows(kpis: dict) -> list[tuple[str, object]]:
    return [
        ("Total Visits", (kpis.get("total_tracking") or {}).get("total_visits", 0)),
        ("Unique Patients", (kpis.get("total_tracking") or {}).get("unique_patients", 0)),
        ("Unique Patients by Visit Type", (kpis.get("unique_by_visit_type") or {}).get("total_unique_patients", 0)),
        ("Revenue per New / Unique Patient", (kpis.get("revenue_per_new_patient") or {}).get("total_revenue", 0)),
        ("All MGBUY Revenue in Date Range", (kpis.get("period_revenue") or {}).get("total_revenue", 0)),
        ("Bills in Date Range", (kpis.get("period_revenue") or {}).get("bill_count", 0)),
        ("Exceptions", (kpis.get("exceptions") or {}).get("count", 0)),
    ]


def _department_new_old_summary(grids: dict) -> pd.DataFrame:
    """Create the department comparison sheet from already exported detail datasets."""
    columns = ["Departments", "New", "New Average", "Old", "Old Average"]
    new_df = _normalize_frame(pd.DataFrame((grids or {}).get("new_patient_revenue") or []))
    period_df = _normalize_frame(pd.DataFrame((grids or {}).get("period_revenue") or []))
    if new_df.empty and period_df.empty:
        return pd.DataFrame(columns=columns)

    dept_rows: dict[str, dict] = {}

    def bucket(department: object) -> dict:
        dept = str(department or "").strip() or "Unknown"
        if dept not in dept_rows:
            dept_rows[dept] = {"Departments": dept, "New": 0.0, "New Bills": 0, "Old": 0.0, "Old Bills": 0}
        return dept_rows[dept]

    new_keys = set()
    if not new_df.empty:
        for col in ["Total Revenue", "PHAmount", "PHReturnAmount"]:
            new_df[col] = pd.to_numeric(new_df.get(col, 0), errors="coerce").fillna(0)
        new_df["Hospital Bill Count"] = pd.to_numeric(new_df.get("Hospital Bill Count", new_df.get("Billing Count", 0)), errors="coerce").fillna(0)
        for _, row in new_df.iterrows():
            reg_no = str(row.get("Reg No") or "").strip().upper()
            visit_type = str(row.get("Visit Type") or "").strip().upper()
            if reg_no or visit_type:
                new_keys.add((reg_no, visit_type))
            amount = float(row.get("Total Revenue") or 0) + float(row.get("PHAmount") or 0) + float(row.get("PHReturnAmount") or 0)
            item = bucket(row.get("Department"))
            item["New"] += amount
            item["New Bills"] += int(row.get("Hospital Bill Count") or 0)

    if not period_df.empty:
        for col in ["Net Amount", "PHAmount", "PHReturnAmount"]:
            period_df[col] = pd.to_numeric(period_df.get(col, 0), errors="coerce").fillna(0)
        if "BillType" in period_df.columns:
            period_df["BillType"] = period_df["BillType"].astype(str).str.strip().str.upper()
        else:
            period_df["BillType"] = np.where(pd.to_numeric(period_df.get("Net Amount", 0), errors="coerce").fillna(0) != 0, "P", "")
        for _, row in period_df.iterrows():
            reg_no = str(row.get("Reg No") or "").strip().upper()
            visit_type = str(row.get("Visit Type") or "").strip().upper()
            if (reg_no, visit_type) in new_keys:
                continue
            amount = float(row.get("Net Amount") or 0) + float(row.get("PHAmount") or 0) + float(row.get("PHReturnAmount") or 0)
            item = bucket(row.get("Department"))
            item["Old"] += amount
            if str(row.get("BillType") or "").strip().upper() == "P":
                item["Old Bills"] += 1

    rows = []
    for item in dept_rows.values():
        rows.append({
            "Departments": item["Departments"],
            "New": round(float(item["New"]), 2),
            "New Average": round(float(item["New"]) / int(item["New Bills"]), 2) if int(item["New Bills"]) else 0,
            "Old": round(float(item["Old"]), 2),
            "Old Average": round(float(item["Old"]) / int(item["Old Bills"]), 2) if int(item["Old Bills"]) else 0,
        })
    return pd.DataFrame(rows, columns=columns).sort_values(["New", "Old", "Departments"], ascending=[False, False, True], kind="stable")


def _write_sheet(
    writer,
    df: pd.DataFrame,
    sheet_name: str,
    title: str,
    filters: dict,
    header_fmt,
    title_fmt,
    meta_fmt,
    text_fmt,
    count_fmt,
    money_fmt,
    date_fmt,
):
    workbook = writer.book
    export_df = df.copy() if df is not None and not df.empty else pd.DataFrame({"Message": ["No data available for selected filters."]})
    money_cols = {"Gross Amount", "Discount", "Net Amount", "PHAmount", "PHReturnAmount", "Paid Amount", "Due Amount", "Total Revenue", "Revenue", "New", "New Average", "Old", "Old Average"}
    percent_cols = set()
    count_cols = {"Total Visits", "Unique Patients", "Billing Count", "Bills in Date Range", "Exceptions"}
    date_cols = {col for col in export_df.columns if "date" in str(col).lower()}
    value_money_kpis = {"Revenue per New / Unique Patient", "All MGBUY Revenue in Date Range"}
    if not export_df.empty:
        for col_name in money_cols.intersection(set(map(str, export_df.columns))):
            export_df[col_name] = pd.to_numeric(export_df[col_name], errors="coerce").fillna(0)
        for col_name in count_cols.intersection(set(map(str, export_df.columns))):
            export_df[col_name] = pd.to_numeric(export_df[col_name], errors="coerce").fillna(0)
        for col_name in percent_cols.intersection(set(map(str, export_df.columns))):
            export_df[col_name] = pd.to_numeric(export_df[col_name], errors="coerce").fillna(0)
        for col_name in date_cols:
            export_df[col_name] = pd.to_datetime(export_df[col_name], errors="coerce")
    start_row = 5
    export_df.to_excel(writer, index=False, sheet_name=sheet_name, startrow=start_row)
    ws = writer.sheets[sheet_name]
    last_col = max(0, len(export_df.columns) - 1)
    _merge_or_write(ws, 0, 0, 0, last_col, title, title_fmt)
    unit_label = filters.get("unit") or "ALL"
    _merge_or_write(ws, 1, 0, 1, last_col, f"Scheme: GSS / MGBUY | Unit: {unit_label}", meta_fmt)
    _merge_or_write(ws, 2, 0, 2, last_col, f"Date Range: {filters.get('from_date')} to {filters.get('to_date')}", meta_fmt)
    _merge_or_write(ws, 3, 0, 3, last_col, f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", meta_fmt)

    for col_idx, col_name in enumerate(export_df.columns):
        ws.write(start_row, col_idx, col_name, header_fmt)
        series = export_df[col_name].astype(str)
        width = min(42, max(12, int(series.str.len().quantile(0.90) if len(series) else len(str(col_name))) + 2, len(str(col_name)) + 2))
        col_name_text = str(col_name)
        fmt = text_fmt
        if col_name_text in money_cols:
            fmt = money_fmt
        elif col_name_text in percent_cols:
            fmt = workbook.add_format({"num_format": "0.00%", "border": 1, "align": "right"})
        elif col_name_text in count_cols:
            fmt = count_fmt
        elif "date" in col_name_text.lower():
            fmt = date_fmt
        elif sheet_name == "Summary" and col_name_text == "Value":
            fmt = text_fmt
        ws.set_column(col_idx, col_idx, width)

        for row_offset, value in enumerate(export_df[col_name], start=start_row + 1):
            if pd.isna(value):
                ws.write_blank(row_offset, col_idx, None, fmt)
            elif col_name_text in money_cols:
                ws.write_number(row_offset, col_idx, float(value), money_fmt)
            elif col_name_text in percent_cols:
                ws.write_number(row_offset, col_idx, float(value), fmt)
            elif col_name_text in count_cols:
                ws.write_number(row_offset, col_idx, float(value), count_fmt)
            elif "date" in col_name_text.lower() and isinstance(value, (datetime, pd.Timestamp)):
                ws.write_datetime(row_offset, col_idx, value.to_pydatetime() if isinstance(value, pd.Timestamp) else value, date_fmt)
            elif sheet_name != "Summary" or col_name_text != "Value":
                ws.write(row_offset, col_idx, value, text_fmt)

        if len(export_df) > 0 and sheet_name == "Summary" and col_name_text == "Value":
            for row_offset, kpi_name in enumerate(export_df.get("KPI", []), start=start_row + 1):
                value = export_df.iloc[row_offset - start_row - 1, col_idx]
                if str(kpi_name) in value_money_kpis:
                    ws.write_number(row_offset, col_idx, float(value or 0), money_fmt)
                elif str(kpi_name) in {"Total Visits", "Unique Patients", "Unique Patients by Visit Type", "Bills in Date Range", "Exceptions"}:
                    ws.write_number(row_offset, col_idx, float(value or 0), count_fmt)
    ws.freeze_panes(start_row + 1, 0)


def _merge_or_write(ws, first_row: int, first_col: int, last_row: int, last_col: int, value, cell_format):
    if first_row == last_row and first_col == last_col:
        ws.write(first_row, first_col, value, cell_format)
    else:
        ws.merge_range(first_row, first_col, last_row, last_col, value, cell_format)
