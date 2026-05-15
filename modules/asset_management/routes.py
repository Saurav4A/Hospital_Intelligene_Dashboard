import io
import hashlib
import hmac
import os
import re
import time
from datetime import datetime, timedelta
from secrets import token_hex
from threading import Lock, Thread
from urllib.parse import urlparse

import requests
from flask import Blueprint, current_app, jsonify, render_template, request, send_file, session, url_for
from werkzeug.utils import secure_filename

import config
from modules import data_fetch
from . import reports, services


DEFAULT_COVERAGE_ROLE_LABELS = {
    "AHL": ["Center Head AHL", "Biomedical AHL", "IT", "VP Operations", "Asset-Purchase"],
    "ACI": ["Center Head ACI", "Biomedical ACI", "IT", "VP Operations", "Asset-Purchase"],
}
ASSET_BIOMEDICAL_ROLES = {
    "BIOMEDICAL AHL",
    "BIOMEDICAL ACI",
    "BIOMEDICAL BALLIA",
}
ASSET_IT_ROLES = {"IT"}
ASSET_BREAKDOWN_IMMEDIATE_ROLES = ASSET_BIOMEDICAL_ROLES | ASSET_IT_ROLES
ASSET_BREAKDOWN_ESCALATION_ROLES = {
    "CENTER HEAD AHL",
    "CENTER HEAD ACI",
    "CENTER HEAD BALLIA",
    "VP OPERATIONS",
    "ASSET-PURCHASE",
    "PURCHASE",
}
ASSET_DIRECTOR_ROLES = {"DIRECTOR"}
ASSET_BREAKDOWN_NOTIFY_ROLES = ASSET_BREAKDOWN_IMMEDIATE_ROLES | ASSET_BREAKDOWN_ESCALATION_ROLES
ASSET_COVERAGE_NOTIFY_ROLES = {
    "CENTER HEAD AHL",
    "CENTER HEAD ACI",
    "CENTER HEAD BALLIA",
    "BIOMEDICAL AHL",
    "BIOMEDICAL ACI",
    "BIOMEDICAL BALLIA",
    "IT",
    "VP OPERATIONS",
    "ASSET-PURCHASE",
    "PURCHASE",
}
ASSET_COVERAGE_COMMON_ROLES = ASSET_COVERAGE_NOTIFY_ROLES - ASSET_BIOMEDICAL_ROLES - ASSET_IT_ROLES
ASSET_PURCHASE_ROLES = {"ASSET-PURCHASE", "PURCHASE"}
ASSET_FINAL_UPDATE_ROLES = {"ASSET-PURCHASE", "PURCHASE", "ACCOUNTS"}
PUBLIC_BREAKDOWN_STATUS_OPTIONS = [
    "Not Working",
    "Partially Working",
    "Performance Issue",
    "Physical Damage",
    "Electrical Issue",
    "Calibration / Accuracy Issue",
    "Other",
]
PUBLIC_BREAKDOWN_STATUS_SET = set(PUBLIC_BREAKDOWN_STATUS_OPTIONS)
PUBLIC_BREAKDOWN_PRIORITY_OPTIONS = {"Normal", "Urgent", "Critical"}
_PUBLIC_BREAKDOWN_RATE_LOCK = Lock()
_PUBLIC_BREAKDOWN_RATE = {}


def _public_rate_allowed(ip, bucket="asset_public", limit=45, window=300):
    key = (bucket, str(ip or ""))
    now = time.time()
    with _PUBLIC_BREAKDOWN_RATE_LOCK:
        hits = [ts for ts in _PUBLIC_BREAKDOWN_RATE.get(key, []) if now - ts <= window]
        if len(hits) >= limit:
            _PUBLIC_BREAKDOWN_RATE[key] = hits
            return False
        hits.append(now)
        _PUBLIC_BREAKDOWN_RATE[key] = hits
        return True


def _valid_email(value):
    text = str(value or "").strip()
    return bool(text and "@" in text and "." in text.rsplit("@", 1)[-1])


def _coverage_recipients_for_unit(unit_code, role_names=None):
    unit = str(unit_code or "").strip().upper()
    role_filter = {str(role or "").strip().upper() for role in (role_names or []) if str(role or "").strip()}
    configured = getattr(config, "ASSET_COVERAGE_EMAIL_GROUPS", {}) or {}
    raw = configured.get(unit) or configured.get(unit.lower()) or []
    recipients = []
    if not role_filter:
        if isinstance(raw, str):
            raw = [part.strip() for part in raw.replace(";", ",").split(",")]
        for item in raw or []:
            if isinstance(item, dict):
                email = str(item.get("email") or item.get("Email") or "").strip()
            else:
                email = str(item or "").strip()
            if _valid_email(email) and email.lower() not in {x.lower() for x in recipients}:
                recipients.append(email)

    for row in data_fetch.fetch_asset_coverage_recipients(unit, role_names=role_filter or None):
        email = str(row.get("email") or row.get("Email") or "").strip()
        if _valid_email(email) and email.lower() not in {x.lower() for x in recipients}:
            recipients.append(email)
    return recipients


def _breakdown_email_body(unit_code, title, rows):
    return f"""
    <div style="font-family:Arial,sans-serif;color:#10213d;line-height:1.5">
      <p>Dear Team,</p>
      <p>Please find attached the Asset Breakdown tracking PDF for <b>{unit_code}</b>.</p>
      <p><b>{title}</b><br><b>Open/Affected tickets:</b> {len(rows or [])}</p>
      <p>Kindly coordinate the technician visit, repair verdict, and closure update in HID Asset Management.</p>
      <p style="color:#64748b;font-size:12px">This is an automated notification from Hospital Intelligence Dashboard.</p>
    </div>
    """


def _breakdown_dt(row):
    raw = (row or {}).get("breakdown_datetime") or (row or {}).get("created_at")
    if isinstance(raw, datetime):
        return raw
    text = str(raw or "").strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%d-%m-%Y %H:%M:%S", "%d-%m-%Y %H:%M"):
        try:
            return datetime.strptime(text[:19], fmt)
        except Exception:
            continue
    try:
        return datetime.fromisoformat(text.replace("Z", ""))
    except Exception:
        return None


def _breakdown_escalation_rows(rows, *, as_of=None, days_open=2):
    as_of_dt = as_of or datetime.now(tz=data_fetch.LOCAL_TZ).replace(tzinfo=None)
    cutoff_date = (as_of_dt.date() - timedelta(days=max(0, int(days_open or 0))))
    due = []
    for row in rows or []:
        dt_val = _breakdown_dt(row)
        if dt_val and dt_val.date() <= cutoff_date:
            due.append(row)
    return due


def _breakdown_row_key(row):
    row = row or {}
    return row.get("ticket_id") or row.get("ticket_no") or id(row)


def _asset_final_update_body(unit_code, title, ticket):
    asset_code = ticket.get("asset_code") or "Asset"
    asset_name = ticket.get("asset_name") or ""
    return f"""
    <div style="font-family:Arial,sans-serif;color:#10213d;line-height:1.5">
      <p>Dear Team,</p>
      <p><b>{title}</b></p>
      <p>Asset <b>{asset_code}</b> {asset_name} has been marked for final action in HID Asset Management.</p>
      <p><b>Unit:</b> {unit_code}<br><b>Ticket:</b> {ticket.get("ticket_no") or "-"}<br><b>Status:</b> {ticket.get("status_label") or ticket.get("status")}</p>
      <p>Please review required purchase/accounts actions.</p>
    </div>
    """


def _coverage_alert_email_body(unit_code, alert_type, rows):
    roles = ", ".join(DEFAULT_COVERAGE_ROLE_LABELS.get(str(unit_code or "").upper(), ["Asset stakeholders"]))
    return f"""
    <div style="font-family:Arial,sans-serif;color:#10213d;line-height:1.5">
      <p>Dear Team,</p>
      <p>Please find attached the Asset Management coverage alert list for <b>{unit_code}</b>.</p>
      <p><b>Alert:</b> {alert_type}<br><b>Affected assets:</b> {len(rows or [])}<br><b>Stakeholders:</b> {roles}</p>
      <p>Kindly review and update the coverage lifecycle details or renewal action status in HID Asset Management.</p>
      <p style="color:#64748b;font-size:12px">This is an automated reminder from Hospital Intelligence Dashboard.</p>
    </div>
    """


def _asset_is_it_item(row):
    normalized = " ".join(
        str((row or {}).get(key) or "")
        for key in ("machine_type_name", "equipment_name", "asset_code", "model_name", "asset_name")
    ).upper()
    word_text = f" {normalized.replace('-', ' ').replace('_', ' ')} "
    return (
        "INFORMATION TECHNOLOGY" in normalized
        or "/IT/" in normalized
        or normalized.endswith("/IT")
        or " IT " in word_text
    )


def _split_asset_rows_for_it(rows):
    non_it_rows = []
    it_rows = []
    for row in rows or []:
        if _asset_is_it_item(row):
            it_rows.append(row)
        else:
            non_it_rows.append(row)
    return non_it_rows, it_rows


def _clean_public_text(value, max_len=200):
    text = str(value or "").strip()
    text = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", "", text)
    if max_len and len(text) > int(max_len):
        return text[: int(max_len)]
    return text


def _looks_like_emp_id(value):
    text = _clean_public_text(value, 80)
    if not text or " " in text:
        return False
    digits = re.sub(r"\D+", "", text)
    if text.isdigit():
        return 1 <= len(text) <= 12
    return bool(digits and len(text) <= 20 and re.match(r"^[A-Za-z0-9_\-/]+$", text))


def _unit_from_employee_deputation(value):
    text = _clean_public_text(value, 300).lower()
    if "asarfi cancer institute" in text:
        return "ACI"
    if "asarfi hospital ballia" in text:
        return "BALLIA"
    if "asarfi hospital dhanbad" in text:
        return "AHL"
    return ""


def _mask_public_phone(value):
    digits = re.sub(r"\D+", "", str(value or ""))
    if len(digits) <= 4:
        return digits
    return f"{'X' * max(0, len(digits) - 4)}{digits[-4:]}"


def _webhook_endpoint_label(url):
    try:
        parsed = urlparse(str(url or ""))
        return parsed.netloc or ""
    except Exception:
        return ""


def _audit_employee_webhook_lookup(audit_callback, *, status, emp_id, summary, unit=None, details=None):
    if not audit_callback:
        return
    try:
        audit_callback(
            "public_employee_webhook_lookup",
            status=status,
            entity_type="employee",
            entity_id=emp_id,
            unit=unit,
            summary=summary,
            details=details or {},
        )
    except Exception:
        return


def fetch_employee_from_webhook(emp_id: str, audit_callback=None):
    emp = _clean_public_text(emp_id, 80)
    if not emp:
        return None
    url = str(getattr(config, "EMP_LOOKUP_WEBHOOK_URL", "") or "").strip()
    secret = str(getattr(config, "EMP_LOOKUP_WEBHOOK_SECRET_KEY", "") or "").strip()
    timeout = int(getattr(config, "EMP_LOOKUP_WEBHOOK_TIMEOUT_SECS", 8) or 8)
    endpoint = _webhook_endpoint_label(url)
    if not url:
        _audit_employee_webhook_lookup(
            audit_callback,
            status="skipped",
            emp_id=emp,
            summary="Employee webhook lookup skipped: URL not configured.",
            details={"reason": "missing_url"},
        )
        return None
    if not secret:
        try:
            current_app.logger.warning("EMP_LOOKUP_WEBHOOK_SECRET_KEY missing; skipping employee webhook lookup.")
        except Exception:
            print("EMP_LOOKUP_WEBHOOK_SECRET_KEY missing; skipping employee webhook lookup.")
        _audit_employee_webhook_lookup(
            audit_callback,
            status="skipped",
            emp_id=emp,
            summary="Employee webhook lookup skipped: secret not configured.",
            details={"endpoint": endpoint, "reason": "missing_secret"},
        )
        return None
    started_at = time.perf_counter()
    try:
        timestamp = int(time.time())
        signature = hmac.new(
            secret.encode("utf-8"),
            f"{emp}:{timestamp}".encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        response = requests.post(
            url,
            headers={"Content-Type": "application/json"},
            json={"code": emp, "timestamp": timestamp, "signature": signature},
            timeout=max(1, min(timeout, 30)),
        )
        elapsed_ms = int((time.perf_counter() - started_at) * 1000)
        response.raise_for_status()
        payload = response.json() if response.content else {}
        if isinstance(payload, dict) and payload.get("success") is False:
            _audit_employee_webhook_lookup(
                audit_callback,
                status="warning",
                emp_id=emp,
                summary="Employee webhook lookup returned unsuccessful response.",
                details={"endpoint": endpoint, "http_status": response.status_code, "elapsed_ms": elapsed_ms},
            )
            return None
        employee = payload.get("employee") if isinstance(payload, dict) else None
        if not employee and isinstance(payload, dict) and isinstance(payload.get("data"), dict):
            employee = payload.get("data")
        if not employee and isinstance(payload, dict):
            employee = payload
        if not isinstance(employee, dict):
            _audit_employee_webhook_lookup(
                audit_callback,
                status="warning",
                emp_id=emp,
                summary="Employee webhook lookup returned no usable employee object.",
                details={"endpoint": endpoint, "http_status": response.status_code, "elapsed_ms": elapsed_ms},
            )
            return None
        emp_id_value = _clean_public_text(
            employee.get("emp_id")
            or employee.get("employee_id")
            or employee.get("EmpID")
            or employee.get("EmployeeCode")
            or employee.get("code")
            or employee.get("employeeCode"),
            50,
        )
        name_value = _clean_public_text(
            employee.get("emp_name")
            or employee.get("employee_name")
            or employee.get("EmployeeName")
            or employee.get("name")
            or employee.get("employeeName")
            or employee.get("fullName"),
            200,
        )
        if not emp_id_value or not name_value:
            _audit_employee_webhook_lookup(
                audit_callback,
                status="warning",
                emp_id=emp,
                summary="Employee webhook lookup returned incomplete employee data.",
                details={
                    "endpoint": endpoint,
                    "http_status": response.status_code,
                    "elapsed_ms": elapsed_ms,
                    "has_employee_id": bool(emp_id_value),
                    "has_employee_name": bool(name_value),
                },
            )
            return None
        deputation = _clean_public_text(employee.get("deputation") or employee.get("Deputation"), 300)
        mobile = _clean_public_text(employee.get("mobile") or employee.get("emp_mobile") or employee.get("MobileNo") or employee.get("mobileNo") or employee.get("PhoneNumber") or employee.get("phone"), 30)
        explicit_unit = _clean_public_text(employee.get("unit") or employee.get("Unit") or employee.get("unitName") or employee.get("unit_code"), 20).upper()
        mapped_unit = explicit_unit or _unit_from_employee_deputation(deputation)
        _audit_employee_webhook_lookup(
            audit_callback,
            status="success",
            emp_id=emp_id_value,
            unit=mapped_unit,
            summary="Employee webhook lookup completed successfully.",
            details={
                "requested_emp_id": emp,
                "endpoint": endpoint,
                "http_status": response.status_code,
                "elapsed_ms": elapsed_ms,
                "employee_id": emp_id_value,
                "employee_name": name_value,
                "department": _clean_public_text(employee.get("department") or employee.get("Department") or employee.get("departmentName") or employee.get("dept"), 200),
                "designation": _clean_public_text(employee.get("designation") or employee.get("Designation") or employee.get("designationName"), 200),
                "deputation": deputation,
                "mapped_unit": mapped_unit,
                "mobile_masked": _mask_public_phone(mobile),
                "source": "PUBLIC_QR",
            },
        )
        return {
            "employee_id": emp_id_value,
            "employee_name": name_value,
            "mobile": mobile,
            "mobile_masked": _mask_public_phone(mobile),
            "department": _clean_public_text(employee.get("department") or employee.get("Department") or employee.get("departmentName") or employee.get("dept"), 200),
            "designation": _clean_public_text(employee.get("designation") or employee.get("Designation") or employee.get("designationName"), 200),
            "deputation": deputation,
            "unit": mapped_unit,
            "source": "webhook",
        }
    except Exception as exc:
        elapsed_ms = int((time.perf_counter() - started_at) * 1000)
        try:
            current_app.logger.warning("Employee webhook lookup failed: %s", exc)
        except Exception:
            print(f"Employee webhook lookup failed: {exc}")
        _audit_employee_webhook_lookup(
            audit_callback,
            status="error",
            emp_id=emp,
            summary="Employee webhook lookup failed.",
            details={
                "endpoint": endpoint,
                "elapsed_ms": elapsed_ms,
                "error_type": type(exc).__name__,
                "message": str(exc)[:250],
            },
        )
        return None


def _merge_employee_rows(primary, fallback_rows):
    rows = []
    seen = set()
    for item in ([primary] if primary else []) + list(fallback_rows or []):
        if not isinstance(item, dict):
            continue
        emp_id = _clean_public_text(item.get("employee_id"), 50)
        unit = _clean_public_text(item.get("unit"), 20).upper()
        key = (unit, emp_id)
        if not emp_id or key in seen:
            continue
        seen.add(key)
        rows.append(
            {
                "employee_id": emp_id,
                "employee_name": _clean_public_text(item.get("employee_name"), 200),
                "mobile": _clean_public_text(item.get("mobile"), 30),
                "mobile_masked": _clean_public_text(item.get("mobile_masked"), 30) or _mask_public_phone(item.get("mobile")),
                "department": _clean_public_text(item.get("department"), 200),
                "designation": _clean_public_text(item.get("designation"), 200),
                "deputation": _clean_public_text(item.get("deputation"), 300),
                "unit": unit,
                "source": _clean_public_text(item.get("source"), 20) or "local",
            }
        )
    return rows


def run_asset_coverage_reminder_job(*, send_graph_mail_with_attachment=None, units=None, force=False, actor="scheduler"):
    units_to_run = [str(u or "").strip().upper() for u in (units or ["AHL", "ACI"]) if str(u or "").strip()]
    results = []
    today = datetime.now(tz=data_fetch.LOCAL_TZ).date()
    for unit in units_to_run:
        run_id = data_fetch.begin_asset_coverage_reminder_run(
            run_date=today,
            run_type="manual" if force else "daily_10am",
            unit_code=unit,
        )
        if not run_id and not force:
            results.append({"unit": unit, "status": "skipped", "message": "Already run for this unit/date."})
            continue

        rows = data_fetch.fetch_expiry_alert_assets_by_unit(unit, threshold_days=90, include_expired=True, include_pending=True)
        if not rows:
            if run_id:
                data_fetch.finish_asset_coverage_reminder_run(run_id, status="done", message="No affected assets.")
            results.append({"unit": unit, "status": "skipped", "message": "No affected assets."})
            continue

        alert_type = "Pending / Expired / 90-60-30-15 Day Coverage Alert"
        non_it_rows, it_rows = _split_asset_rows_for_it(rows)
        coverage_mail_jobs = []
        if non_it_rows:
            coverage_mail_jobs.append(
                {
                    "group": "medical_nonmedical",
                    "rows": non_it_rows,
                    "roles": ASSET_COVERAGE_COMMON_ROLES | ASSET_BIOMEDICAL_ROLES,
                    "title": f"{alert_type} - Medical + Non-Medical",
                    "subject": f"HID Asset Coverage Alert - {unit} - {len(non_it_rows)} medical/non-medical asset(s)",
                }
            )
        if it_rows:
            coverage_mail_jobs.append(
                {
                    "group": "it",
                    "rows": it_rows,
                    "roles": ASSET_COVERAGE_COMMON_ROLES | ASSET_IT_ROLES,
                    "title": f"{alert_type} - IT",
                    "subject": f"HID Asset Coverage Alert - {unit} - {len(it_rows)} IT asset(s)",
                }
            )

        mail_results = []
        for job in coverage_mail_jobs:
            recipients = _coverage_recipients_for_unit(unit, job["roles"])
            if not recipients:
                mail_results.append({"group": job["group"], "status": "skipped", "message": "No configured recipients.", "recipient_count": 0})
                continue
            pdf_bytes, filename = reports.build_coverage_alert_pdf(unit_code=unit, alert_type=job["title"], rows=job["rows"])
            filename = filename.replace(
                "Asset_Coverage_Alert",
                "Asset_Coverage_IT" if job["group"] == "it" else "Asset_Coverage_Medical_NonMedical",
            )
            body = _coverage_alert_email_body(unit, job["title"], job["rows"])
            if not send_graph_mail_with_attachment:
                mail_results.append({"group": job["group"], "status": "skipped", "message": "Mail sender unavailable.", "recipient_count": len(recipients)})
                continue
            result = send_graph_mail_with_attachment(
                subject=job["subject"],
                body_html=body,
                to_recipients=recipients,
                filename=filename,
                content_bytes=pdf_bytes,
            )
            result["group"] = job["group"]
            result["recipient_count"] = len(recipients)
            mail_results.append(result)

        success_like = {"success", "skipped"}
        status = "done" if any(str(row.get("status") or "").lower() in success_like for row in mail_results) else "error"
        msg = "; ".join(
            f"{row.get('group')}: {row.get('message') or row.get('status')}"
            for row in mail_results
        ) or "No coverage reminder recipients configured."
        if run_id:
            data_fetch.finish_asset_coverage_reminder_run(run_id, status=status, message=msg)
        results.append(
            {
                "unit": unit,
                "status": status,
                "message": msg,
                "affected_count": len(rows),
                "medical_nonmedical_count": len(non_it_rows),
                "it_count": len(it_rows),
                "recipient_count": sum(services.to_int(row.get("recipient_count"), 0) or 0 for row in mail_results),
                "mail_results": mail_results,
                "actor": actor,
            }
        )
    return {"status": "success", "results": results}


def run_asset_breakdown_reminder_job(*, send_graph_mail_with_attachment=None, units=None, force=False, actor="scheduler"):
    units_to_run = [str(u or "").strip().upper() for u in (units or ["AHL", "ACI"]) if str(u or "").strip()]
    results = []
    today = datetime.now(tz=data_fetch.LOCAL_TZ).date()
    for unit in units_to_run:
        run_id = data_fetch.begin_asset_breakdown_reminder_run(
            run_date=today,
            run_type="manual" if force else "daily_1015",
            unit_code=unit,
        )
        if not run_id and not force:
            results.append({"unit": unit, "status": "skipped", "message": "Already run for this unit/date."})
            continue

        rows = data_fetch.fetch_open_asset_breakdowns_by_unit(unit)
        if not rows:
            if run_id:
                data_fetch.finish_asset_breakdown_reminder_run(run_id, status="done", message="No open breakdown tickets.")
            results.append({"unit": unit, "status": "skipped", "message": "No open breakdown tickets."})
            continue

        non_it_rows, it_rows = _split_asset_rows_for_it(rows)
        biomedical_recipients = _coverage_recipients_for_unit(unit, ASSET_BIOMEDICAL_ROLES) if non_it_rows else []
        it_recipients = _coverage_recipients_for_unit(unit, ASSET_IT_ROLES) if it_rows else []
        escalation_rows = _breakdown_escalation_rows(rows, days_open=2)
        director_escalation_rows = _breakdown_escalation_rows(rows, days_open=3)
        director_escalation_keys = {_breakdown_row_key(row) for row in director_escalation_rows}
        t_plus_2_only_rows = [
            row for row in escalation_rows
            if _breakdown_row_key(row) not in director_escalation_keys
        ]
        escalation_mail_jobs = []
        if t_plus_2_only_rows:
            escalation_mail_jobs.append({
                "group": "t_plus_2",
                "rows": t_plus_2_only_rows,
                "roles": set(ASSET_BREAKDOWN_ESCALATION_ROLES),
                "title": "T+2 Open Breakdown Escalation",
                "subject_prefix": "HID Asset Breakdown T+2 Escalation",
            })
        if director_escalation_rows:
            escalation_mail_jobs.append({
                "group": "t_plus_3",
                "rows": director_escalation_rows,
                "roles": set(ASSET_BREAKDOWN_ESCALATION_ROLES) | ASSET_DIRECTOR_ROLES,
                "title": "T+3 Open Breakdown Escalation",
                "subject_prefix": "HID Asset Breakdown T+3 Escalation",
            })
        for job in escalation_mail_jobs:
            escalation_non_it_rows, escalation_it_rows = _split_asset_rows_for_it(job["rows"])
            if escalation_non_it_rows:
                job["roles"] |= ASSET_BIOMEDICAL_ROLES
            if escalation_it_rows:
                job["roles"] |= ASSET_IT_ROLES
            job["recipients"] = _coverage_recipients_for_unit(unit, job["roles"])
        if not biomedical_recipients and not it_recipients and not any(job.get("recipients") for job in escalation_mail_jobs):
            msg = "No asset breakdown recipients configured."
            if run_id:
                data_fetch.finish_asset_breakdown_reminder_run(run_id, status="skipped", message=msg)
            results.append({"unit": unit, "status": "skipped", "message": msg, "affected_count": len(rows)})
            continue

        mail_results = []
        if biomedical_recipients:
            pdf_bytes, filename = reports.build_breakdown_ticket_pdf(
                unit_code=unit,
                title="Daily Open Breakdown Reminder - Medical + Non-Medical",
                rows=non_it_rows,
            )
            subject = f"HID Asset Breakdown Reminder - {unit} - {len(non_it_rows)} medical/non-medical open ticket(s)"
            body = _breakdown_email_body(unit, "Daily Open Breakdown Reminder - Medical + Non-Medical", non_it_rows)
            if not send_graph_mail_with_attachment:
                mail_results.append({"group": "biomedical", "status": "skipped", "message": "Mail sender unavailable.", "recipient_count": len(biomedical_recipients)})
            else:
                result = send_graph_mail_with_attachment(
                    subject=subject,
                    body_html=body,
                    to_recipients=biomedical_recipients,
                    filename=filename,
                    content_bytes=pdf_bytes,
                )
                result["group"] = "biomedical"
                result["recipient_count"] = len(biomedical_recipients)
                mail_results.append(result)

        if it_recipients:
            pdf_bytes, filename = reports.build_breakdown_ticket_pdf(
                unit_code=unit,
                title="Daily Open Breakdown Reminder - IT",
                rows=it_rows,
            )
            subject = f"HID Asset Breakdown Reminder - {unit} - {len(it_rows)} IT open ticket(s)"
            body = _breakdown_email_body(unit, "Daily Open Breakdown Reminder - IT", it_rows)
            if not send_graph_mail_with_attachment:
                mail_results.append({"group": "it", "status": "skipped", "message": "Mail sender unavailable.", "recipient_count": len(it_recipients)})
            else:
                result = send_graph_mail_with_attachment(
                    subject=subject,
                    body_html=body,
                    to_recipients=it_recipients,
                    filename=filename,
                    content_bytes=pdf_bytes,
                )
                result["group"] = "it"
                result["recipient_count"] = len(it_recipients)
                mail_results.append(result)

        for job in escalation_mail_jobs:
            escalation_recipients = job.get("recipients") or []
            if not escalation_recipients:
                mail_results.append({"group": job["group"], "status": "skipped", "message": "No configured recipients.", "recipient_count": 0})
                continue
            pdf_bytes, filename = reports.build_breakdown_ticket_pdf(
                unit_code=unit,
                title=job["title"],
                rows=job["rows"],
            )
            subject = f"{job['subject_prefix']} - {unit} - {len(job['rows'])} open ticket(s)"
            body = _breakdown_email_body(unit, job["title"], job["rows"])
            if not send_graph_mail_with_attachment:
                mail_results.append({"group": job["group"], "status": "skipped", "message": "Mail sender unavailable.", "recipient_count": len(escalation_recipients)})
            else:
                result = send_graph_mail_with_attachment(
                    subject=subject,
                    body_html=body,
                    to_recipients=escalation_recipients,
                    filename=filename,
                    content_bytes=pdf_bytes,
                )
                result["group"] = job["group"]
                result["recipient_count"] = len(escalation_recipients)
                mail_results.append(result)

        success_like = {"success", "skipped"}
        status = "done" if any(str(row.get("status") or "").lower() in success_like for row in mail_results) else "error"
        msg = "; ".join(
            f"{row.get('group')}: {row.get('message') or row.get('status')}"
            for row in mail_results
        ) or "No mail attempts."
        if run_id:
            data_fetch.finish_asset_breakdown_reminder_run(run_id, status=status, message=msg)
        results.append(
            {
                "unit": unit,
                "status": status,
                "message": msg,
                "affected_count": len(rows),
                "medical_nonmedical_count": len(non_it_rows),
                "it_count": len(it_rows),
                "t_plus_2_count": len(escalation_rows),
                "t_plus_3_count": len(director_escalation_rows),
                "recipient_count": sum(services.to_int(row.get("recipient_count"), 0) or 0 for row in mail_results),
                "mail_results": mail_results,
                "actor": actor,
            }
        )
    return {"status": "success", "results": results}


def create_asset_management_blueprint(
    *,
    login_required,
    allowed_units_for_session,
    excel_job_get,
    excel_job_update,
    export_cache_get_bytes,
    export_cache_put_bytes,
    export_executor,
    audit_log_event,
    local_tz,
    get_login_db_connection,
    push_user_notification,
    send_graph_mail_with_attachment=None,
):
    bp = Blueprint("asset_management", __name__)

    _allowed_units_for_session = allowed_units_for_session
    _excel_job_get = excel_job_get
    _excel_job_update = excel_job_update
    _export_cache_get_bytes = export_cache_get_bytes
    _export_cache_put_bytes = export_cache_put_bytes
    _export_executor = export_executor
    _audit_log_event = audit_log_event
    _local_tz = local_tz
    _get_login_db_connection = get_login_db_connection
    _push_user_notification = push_user_notification
    _send_graph_mail_with_attachment = send_graph_mail_with_attachment

    def _asset_access_codes():
        role = services.clean_text(session.get("role"))
        role_base = role.split(":", 1)[0].strip() if role else ""
        if role_base == "IT":
            return services.asset_scope_from_units("*")

        session_unit_scope = session.get("unit_scope")
        if services.has_explicit_unit_scope(session_unit_scope):
            return services.asset_scope_from_units(session_unit_scope)

        if role.startswith("Departmental Head") and ":" in role:
            return services.asset_scope_from_units(role.split(":", 1)[1])

        return []

    def _forbidden():
        return jsonify({"status": "error", "message": "Asset Management access is not available for your scope."}), 403

    def _current_username():
        return session.get("username") or session.get("user") or "Unknown"

    def _current_account_id():
        return session.get("accountid") or session.get("account_id") or 0

    def _payload():
        if request.is_json:
            return request.get_json(silent=True) or {}
        return request.form.to_dict(flat=True)

    def _load_lookups(location_codes):
        return {
            "machine_types": data_fetch.fetch_asset_machine_types(),
            "equipment": data_fetch.fetch_asset_equipment(),
            "manufacturers": data_fetch.fetch_asset_manufacturers(),
            "suppliers": data_fetch.fetch_asset_suppliers(),
            "locations": data_fetch.fetch_asset_locations(location_codes),
            "users": data_fetch.fetch_asset_assignment_users(location_codes),
            "departments": data_fetch.fetch_asset_departments(),
            "status_options": services.ASSET_STATUS_OPTIONS,
            "assignment_type_options": services.ASSIGNMENT_TYPE_OPTIONS,
            "warranty_bucket_options": services.WARRANTY_BUCKET_OPTIONS,
            "coverage_type_options": services.COVERAGE_TYPE_OPTIONS,
            "coverage_alert_options": services.COVERAGE_ALERT_OPTIONS,
            "sort_options": services.SORT_OPTIONS,
        }

    def _audit(action, *, status="success", entity_type=None, entity_id=None, unit=None, summary=None, details=None):
        if not _audit_log_event:
            return
        try:
            _audit_log_event(
                "asset_management",
                action,
                status=status,
                entity_type=entity_type,
                entity_id=str(entity_id) if entity_id is not None else None,
                unit=unit,
                summary=summary,
                details=details,
            )
        except Exception:
            return

    def _send_breakdown_ticket_email(ticket, title="New Asset Breakdown Ticket"):
        unit = services.clean_text(ticket.get("unit_code")).upper()
        role_filter = ASSET_IT_ROLES if _asset_is_it_item(ticket) else ASSET_BIOMEDICAL_ROLES
        recipients = _coverage_recipients_for_unit(unit, role_filter)
        if not recipients:
            data_fetch.update_asset_breakdown_initial_email(ticket.get("ticket_id"), "no_recipients")
            return {"status": "skipped", "message": "No asset breakdown recipients configured."}
        pdf_bytes, filename = reports.build_breakdown_ticket_pdf(unit_code=unit, title=title, rows=[ticket])
        subject = f"HID Asset Breakdown - {unit} - {ticket.get('ticket_no')} - {ticket.get('asset_code')}"
        body = _breakdown_email_body(unit, title, [ticket])
        if not _send_graph_mail_with_attachment:
            data_fetch.update_asset_breakdown_initial_email(ticket.get("ticket_id"), "mail_unavailable")
            return {"status": "skipped", "message": "Mail sender unavailable."}
        result = _send_graph_mail_with_attachment(
            subject=subject,
            body_html=body,
            to_recipients=recipients,
            filename=filename,
            content_bytes=pdf_bytes,
        )
        data_fetch.update_asset_breakdown_initial_email(ticket.get("ticket_id"), result.get("status") or "sent")
        return result

    def _queue_breakdown_ticket_email(ticket, title="New Asset Breakdown Ticket"):
        ticket_id = ticket.get("ticket_id") if isinstance(ticket, dict) else None
        data_fetch.update_asset_breakdown_initial_email(ticket_id, "queued")
        try:
            app_obj = current_app._get_current_object()
        except Exception:
            app_obj = None

        def _worker():
            try:
                if app_obj:
                    with app_obj.app_context():
                        _send_breakdown_ticket_email(ticket, title)
                else:
                    _send_breakdown_ticket_email(ticket, title)
            except Exception as exc:
                data_fetch.update_asset_breakdown_initial_email(ticket_id, "error")
                _audit(
                    "breakdown_initial_email",
                    status="error",
                    entity_type="asset_breakdown",
                    entity_id=ticket_id,
                    unit=(ticket or {}).get("unit_code"),
                    summary="Breakdown initial email failed.",
                    details={"error": str(exc)[:250]},
                )

        try:
            if _export_executor:
                _export_executor.submit(_worker)
            else:
                Thread(target=_worker, daemon=True).start()
            return {"status": "queued", "message": "Breakdown notification email queued."}
        except Exception as exc:
            data_fetch.update_asset_breakdown_initial_email(ticket_id, "queue_error")
            return {"status": "error", "message": f"Unable to queue breakdown notification email: {exc}"}

    def _send_asset_final_update(ticket, title="Asset Final Action Update"):
        unit = services.clean_text(ticket.get("unit_code")).upper()
        recipients = _coverage_recipients_for_unit(unit, ASSET_FINAL_UPDATE_ROLES)
        if not recipients:
            return {"status": "skipped", "message": "No Asset-Purchase/Accounts recipients configured."}
        pdf_bytes, filename = reports.build_breakdown_ticket_pdf(unit_code=unit, title=title, rows=[ticket])
        subject = f"HID Asset Final Action - {unit} - {ticket.get('asset_code')}"
        body = _asset_final_update_body(unit, title, ticket)
        if not _send_graph_mail_with_attachment:
            return {"status": "skipped", "message": "Mail sender unavailable."}
        return _send_graph_mail_with_attachment(
            subject=subject,
            body_html=body,
            to_recipients=recipients,
            filename=filename,
            content_bytes=pdf_bytes,
        )

    def _send_asset_action_otp_email(otp_payload, action_label):
        unit = services.clean_text(otp_payload.get("unit_code")).upper()
        recipients = _coverage_recipients_for_unit(unit, ASSET_PURCHASE_ROLES)
        if not recipients:
            return {"status": "skipped", "message": "No Asset-Purchase recipients configured."}
        asset = otp_payload.get("asset") or {}
        code = otp_payload.get("otp_code")
        body = f"""
        <div style="font-family:Arial,sans-serif;color:#10213d;line-height:1.5">
          <p>Dear Asset-Purchase Team,</p>
          <p>An OTP approval is required for <b>{action_label}</b>.</p>
          <p><b>Asset:</b> {asset.get('asset_code') or ''} - {asset.get('equipment_name') or ''}<br>
          <b>Unit:</b> {unit}<br><b>OTP:</b> <span style="font-size:20px;font-weight:800">{code}</span><br>
          <b>Valid till:</b> {otp_payload.get('expires_at') or ''}</p>
          <p>Please share this OTP only after validating the requested action.</p>
        </div>
        """
        if not _send_graph_mail_with_attachment:
            return {"status": "skipped", "message": "Mail sender unavailable.", "otp_code": code}
        return _send_graph_mail_with_attachment(
            subject=f"HID Asset OTP - {unit} - {action_label}",
            body_html=body,
            to_recipients=recipients,
        )

    def _create_cross_unit_issue_notification(asset_before, movement_result, movement_type):
        if services.clean_text(movement_type).upper() not in {"ISSUE", "TRANSFER"}:
            return None
        before = asset_before or {}
        updated_asset = ((movement_result or {}).get("detail") or {}).get("asset") or {}
        source_unit = services.clean_text(before.get("location_code")).upper()
        target_unit = services.clean_text(updated_asset.get("location_code")).upper()
        if not source_unit or not target_unit or source_unit == target_unit:
            return None
        asset_code = updated_asset.get("asset_code") or before.get("asset_code") or "Asset"
        asset_name = updated_asset.get("equipment_name") or before.get("equipment_name") or ""
        action_label = "issued" if services.clean_text(movement_type).upper() == "ISSUE" else "transferred"
        title = f"{asset_code} {action_label.title()} To {target_unit}"
        message = (
            f"{asset_code} {asset_name} has been {action_label} from {source_unit} to {target_unit}. "
            "Please review and accept the asset receipt."
        ).strip()
        return data_fetch.create_asset_issue_notification(
            updated_asset.get("asset_id") or before.get("asset_id"),
            (movement_result or {}).get("movement_log_id"),
            target_unit,
            title,
            message,
            url_for("asset_management.asset_detail_page", asset_id=updated_asset.get("asset_id") or before.get("asset_id")),
            actor_username=_current_username(),
        )

    def _detail_or_404(asset_id, location_codes):
        detail = data_fetch.fetch_asset_detail(asset_id, location_codes=location_codes)
        if detail:
            return detail, None
        return None, (jsonify({"status": "error", "message": "Asset not found for your scope."}), 404)

    @bp.route("/asset/breakdown/public")
    @bp.route("/feedback/asset-breakdown")
    def public_breakdown_complaint_page():
        return render_template(
            "asset_management/public_breakdown_complaint.html",
            status_options=PUBLIC_BREAKDOWN_STATUS_OPTIONS,
            priority_options=["Normal", "Urgent", "Critical"],
        )

    @bp.route("/api/asset-breakdown/public/employee-search")
    def api_public_employee_search():
        ip = request.headers.get("X-Forwarded-For", request.remote_addr or "").split(",", 1)[0].strip()
        if not _public_rate_allowed(ip, "employee_search", limit=45, window=300):
            return jsonify({"success": False, "status": "error", "message": "Please wait a moment before searching again."}), 429
        q = _clean_public_text(request.args.get("q") or request.args.get("query"), 80)
        if len(q) < 2 and not re.sub(r"\D+", "", q):
            return jsonify({"success": False, "status": "error", "message": "Please enter Emp ID, Name, or Mobile No."}), 400
        webhook_employee = fetch_employee_from_webhook(q, audit_callback=_audit) if _looks_like_emp_id(q) else None
        local_rows = data_fetch.search_asset_public_employees(q, limit=12)
        rows = _merge_employee_rows(webhook_employee, local_rows)
        return jsonify({"success": True, "status": "success", "employees": rows, "count": len(rows)})

    @bp.route("/api/asset-breakdown/public/assets")
    def api_public_assets():
        ip = request.headers.get("X-Forwarded-For", request.remote_addr or "").split(",", 1)[0].strip()
        if not _public_rate_allowed(ip, "asset_search", limit=75, window=300):
            return jsonify({"success": False, "status": "error", "message": "Please wait a moment before searching again."}), 429

        q = _clean_public_text(request.args.get("q") or request.args.get("query"), 120)
        unit = _clean_public_text(request.args.get("unit"), 20).upper()

    # Public QR page should not expose/load asset list without a search term.
        if len(q) < 2:
            return jsonify({
                "success": True,
                "status": "success",
                "assets": [],
                "count": 0,
                "message": "Please enter at least 2 characters to search assets."
            })

        location_codes = [unit] if unit in {"AHL", "ACI", "BALLIA"} else None
        rows = data_fetch.search_asset_public_assets(q, limit=25, location_codes=location_codes)
        return jsonify({"success": True, "status": "success", "assets": rows, "count": len(rows)})

    @bp.route("/api/asset-breakdown/public/submit", methods=["POST"])
    def api_public_breakdown_submit():
        ip = request.headers.get("X-Forwarded-For", request.remote_addr or "").split(",", 1)[0].strip()
        if not _public_rate_allowed(ip, "submit", limit=10, window=600):
            return jsonify({"success": False, "status": "error", "message": "Please wait a moment before submitting again."}), 429
        payload = request.get_json(silent=True) or {}
        reported_status = _clean_public_text(payload.get("reported_status"), 80)
        priority = _clean_public_text(payload.get("priority") or "Normal", 20)
        if reported_status not in PUBLIC_BREAKDOWN_STATUS_SET:
            return jsonify({"success": False, "status": "error", "message": "Please select a valid current status."}), 400
        if priority not in PUBLIC_BREAKDOWN_PRIORITY_OPTIONS:
            priority = "Normal"
        result, status_code = data_fetch.create_public_asset_breakdown_complaint(
            {
                "employee_id": payload.get("employee_id"),
                "employee_name": payload.get("employee_name"),
                "employee_mobile": payload.get("employee_mobile"),
                "employee_department": payload.get("employee_department"),
                "employee_designation": payload.get("employee_designation"),
                "employee_unit": payload.get("employee_unit"),
                "asset_id": payload.get("asset_id"),
                "asset_code": payload.get("asset_code"),
                "issue_description": payload.get("issue_description"),
                "reported_status": reported_status,
                "priority": priority,
                "contact_mobile": payload.get("contact_mobile"),
            }
        )
        ticket = result.get("ticket") if isinstance(result, dict) else None
        if status_code == 200 and ticket and not result.get("existing_breakdown"):
            result["mail"] = _queue_breakdown_ticket_email(ticket, "New Public QR Asset Breakdown Complaint")
            _audit(
                "public_breakdown_complaint",
                entity_type="asset_breakdown",
                entity_id=ticket.get("ticket_id"),
                unit=ticket.get("unit_code"),
                summary=f"Public QR breakdown complaint {ticket.get('ticket_no')}",
                details={"source": "PUBLIC_QR", "employee_id": payload.get("employee_id")},
            )
        elif status_code == 200 and ticket and result.get("existing_breakdown"):
            _audit(
                "public_breakdown_duplicate_acknowledged",
                entity_type="asset_breakdown",
                entity_id=ticket.get("ticket_id"),
                unit=ticket.get("unit_code"),
                summary=f"Public QR duplicate complaint acknowledged for {ticket.get('ticket_no')}",
                details={"source": "PUBLIC_QR", "employee_id": payload.get("employee_id"), "asset_id": payload.get("asset_id")},
            )
        if isinstance(result, dict):
            result.setdefault("status", "success" if result.get("success") else "error")
        return jsonify(result), status_code

    def _reference_master_context(kind, search_text=""):
        master_kind = services.normalize_reference_master_kind(kind)
        meta = services.reference_master_meta(master_kind)
        source = services.reference_master_source(master_kind)
        if source == "manufacturer":
            rows = data_fetch.fetch_asset_manufacturer_master_rows(search_text)
            next_code = data_fetch.fetch_asset_next_manufacturer_code()
        elif source == "supplier":
            rows = data_fetch.fetch_asset_supplier_master_rows(search_text)
            next_code = data_fetch.fetch_asset_next_supplier_code()
        else:
            rows = data_fetch.fetch_asset_equipment_master_rows(search_text)
            next_code = data_fetch.fetch_asset_next_equipment_code()
        default_geo = data_fetch.fetch_asset_reference_default_city_state() if source in {"manufacturer", "supplier"} else {}
        states = data_fetch.fetch_asset_reference_states() if source in {"manufacturer", "supplier"} else []
        cities = data_fetch.fetch_asset_reference_cities(default_geo.get("state_id") or 0) if source in {"manufacturer", "supplier"} else []
        return {
            "kind": master_kind,
            "meta": meta,
            "rows": rows,
            "states": states,
            "cities": cities,
            "default_geo": default_geo,
            "next_code": next_code,
            "source_unit": data_fetch.ASSET_CENTER_UNIT,
            "machine_types": data_fetch.fetch_asset_machine_types() if source == "equipment" else [],
        }

    def _reference_master_rows(kind, search_text=""):
        return _reference_master_context(kind, search_text).get("rows") or []

    def _reference_master_detail(kind, record_id):
        source = services.reference_master_source(kind)
        if source == "manufacturer":
            return data_fetch.fetch_asset_manufacturer_master_detail(record_id)
        if source == "equipment":
            return data_fetch.fetch_asset_equipment_master_detail(record_id)
        return data_fetch.fetch_asset_supplier_master_detail(record_id)

    def _reference_master_save(kind, payload):
        source = services.reference_master_source(kind)
        if source == "manufacturer":
            return data_fetch.upsert_asset_manufacturer_master(
                payload,
                actor_user_id=_current_account_id(),
                actor_username=_current_username(),
                actor_ip=request.remote_addr or "",
            )
        if source == "equipment":
            return data_fetch.upsert_asset_equipment_master(
                payload,
                actor_user_id=_current_account_id(),
                actor_username=_current_username(),
                actor_ip=request.remote_addr or "",
            )
        return data_fetch.upsert_asset_supplier_master(
            payload,
            actor_user_id=_current_account_id(),
            actor_username=_current_username(),
            actor_ip=request.remote_addr or "",
        )

    def _queue_export_job(job_id, export_format, location_codes, filters, exported_by):
        _excel_job_update(job_id, state="running", format=export_format)
        try:
            export_filters = dict(filters or {})
            export_filters["page"] = 1
            export_filters["page_size"] = 10000
            register_payload = data_fetch.fetch_asset_register_rows(location_codes, export_filters)
            register_rows = register_payload.get("rows") or []
            if not register_rows:
                _excel_job_update(job_id, state="error", error="No assets available for the selected filters.", format=export_format)
                return
            summary = data_fetch.fetch_asset_summary(location_codes, filters)
            movement_rows = data_fetch.fetch_asset_movement_rows(
                location_codes=location_codes,
                asset_ids=[row.get("asset_id") for row in register_rows if row.get("asset_id")],
                limit=15000,
            )
            data, filename, mimetype = reports.build_asset_export(
                export_format=export_format,
                summary=summary,
                register_rows=register_rows,
                movement_rows=movement_rows,
                filters=filters,
                exported_by=exported_by,
                scope_codes=location_codes,
            )
            _export_cache_put_bytes("asset_management_export_job", data, job_id)
            _excel_job_update(job_id, state="done", filename=filename, format=export_format, mimetype=mimetype)
            if _push_user_notification and exported_by:
                try:
                    _push_user_notification(
                        exported_by,
                        "Asset export is ready",
                        f"{filename} has been prepared for download.",
                        link=f"/api/asset-management/export_job_result?job_id={job_id}",
                        ref=f"asset_export:{job_id}",
                    )
                except Exception:
                    pass
        except Exception as exc:
            _excel_job_update(job_id, state="error", error=str(exc), format=export_format)

    @bp.route("/asset-management")
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"}, required_section="asset_management")
    def home():
        location_codes = _asset_access_codes()
        if not location_codes:
            return _forbidden()
        summary = data_fetch.fetch_asset_summary(location_codes)
        recent_activity = data_fetch.fetch_asset_recent_activity(location_codes, limit=10)
        recent_assets = data_fetch.fetch_asset_register_rows(
            location_codes,
            {"page": 1, "page_size": 8, "sort": "updated", "direction": "desc"},
        )
        return render_template(
            "asset_management/home.html",
            page_title="Asset Management",
            page_subtitle="Track asset availability, movement, warranty, and unit-wise activity across your permitted hospital units.",
            active_tab="home",
            scope_locations=location_codes,
            summary=summary,
            recent_activity=recent_activity,
            recent_assets=recent_assets.get("rows") or [],
        )

    @bp.route("/asset-management/register")
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"}, required_section="asset_management")
    def register_page():
        location_codes = _asset_access_codes()
        if not location_codes:
            return _forbidden()
        filters = services.normalize_filters(request.args)
        lookups = _load_lookups(location_codes)
        register_data = data_fetch.fetch_asset_register_rows(location_codes, filters)
        return render_template(
            "asset_management/register.html",
            page_title="Asset Register",
            page_subtitle="Search the register, update asset master details, and start issue or transfer actions from one place.",
            active_tab="register",
            scope_locations=location_codes,
            filters=filters,
            rows=register_data.get("rows") or [],
            pager=register_data,
            lookups=lookups,
        )

    @bp.route("/asset-management/breakdowns")
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"}, required_section="asset_management")
    def breakdowns_page():
        location_codes = _asset_access_codes()
        if not location_codes:
            return _forbidden()
        status = services.clean_text(request.args.get("status") or "OPEN").upper()
        lookups = _load_lookups(location_codes)
        asset_rows = data_fetch.fetch_asset_register_rows(
            location_codes,
            {"page": 1, "page_size": 5000, "sort": "asset_code"},
        ).get("rows") or []
        breakdown_asset_options = [
            {
                "asset_id": row.get("asset_id"),
                "asset_code": row.get("asset_code") or "",
                "equipment_name": row.get("equipment_name") or "",
                "location_code": row.get("location_code") or "",
                "machine_type_name": row.get("machine_type_name") or "",
            }
            for row in asset_rows
        ]
        tickets = data_fetch.fetch_asset_breakdown_tickets(
            location_codes=location_codes,
            status=status if status else None,
            limit=1000,
        )
        return render_template(
            "asset_management/breakdowns.html",
            page_title="Breakdown Tracking",
            page_subtitle="Open, monitor, and close asset breakdown tickets by machine type and unit.",
            active_tab="breakdowns",
            scope_locations=location_codes,
            status=status,
            lookups=lookups,
            asset_rows=asset_rows,
            breakdown_asset_options=breakdown_asset_options,
            tickets=tickets,
            open_count=len(data_fetch.fetch_asset_breakdown_tickets(location_codes=location_codes, status="OPEN", limit=5000)),
            closed_count=len(data_fetch.fetch_asset_breakdown_tickets(location_codes=location_codes, status="CLOSED", limit=5000)),
        )

    @bp.route("/asset-management/assets/<int:asset_id>")
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"}, required_section="asset_management")
    def asset_detail_page(asset_id: int):
        location_codes = _asset_access_codes()
        if not location_codes:
            return _forbidden()
        detail, error_response = _detail_or_404(asset_id, location_codes)
        if error_response:
            return error_response
        lookups = _load_lookups(location_codes)
        return render_template(
            "asset_management/detail.html",
            page_title=detail["asset"].get("asset_code") or "Asset Detail",
            page_subtitle="Review procurement, warranty, maintenance, attachments, and movement history for this asset.",
            active_tab="register",
            scope_locations=location_codes,
            detail=detail,
            asset=detail["asset"],
            maintenance=detail.get("maintenance") or {},
            attachments=detail.get("attachments") or [],
            history=detail.get("history") or [],
            lookups=lookups,
        )

    @bp.route("/asset-management/reports")
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"}, required_section="asset_management")
    def reports_page():
        location_codes = _asset_access_codes()
        if not location_codes:
            return _forbidden()
        filters = services.normalize_filters(request.args)
        summary = data_fetch.fetch_asset_summary(location_codes, filters)
        preview = data_fetch.fetch_asset_register_rows(location_codes, {**filters, "page": 1, "page_size": 20})
        lookups = _load_lookups(location_codes)
        return render_template(
            "asset_management/reports.html",
            page_title="Asset Reports",
            page_subtitle="",
            active_tab="reports",
            scope_locations=location_codes,
            filters=filters,
            summary=summary,
            preview_rows=preview.get("rows") or [],
            lookups=lookups,
        )

    @bp.route("/asset-management/masters")
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"}, required_section="asset_management")
    def masters_home():
        location_codes = _asset_access_codes()
        if not location_codes:
            return _forbidden()
        master_cards = []
        for item in services.REFERENCE_MASTER_OPTIONS:
            meta = services.reference_master_meta(item["value"])
            rows = _reference_master_rows(item["value"])
            master_cards.append(
                {
                    "kind": item["value"],
                    "label": item["label"],
                    "title": meta["title"],
                    "subtitle": meta["subtitle"],
                    "count": len(rows),
                    "href": url_for("asset_management.reference_master_page", kind=item["value"]),
                }
            )
        return render_template(
            "asset_management/masters_home.html",
            page_title="Asset Masters",
            page_subtitle="Manage company, manufacturer, supplier, and equipment records used during asset entry.",
            active_tab="masters",
            scope_locations=location_codes,
            master_cards=master_cards,
        )

    @bp.route("/asset-management/masters/<kind>")
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"}, required_section="asset_management")
    def reference_master_page(kind: str):
        location_codes = _asset_access_codes()
        if not location_codes:
            return _forbidden()
        search_text = services.clean_text(request.args.get("search"))
        try:
            ctx = _reference_master_context(kind, search_text)
        except ValueError:
            return "Master page not found.", 404
        return render_template(
            "asset_management/reference_master.html",
            page_title=ctx["meta"]["title"],
            page_subtitle=ctx["meta"]["subtitle"],
            active_tab="masters",
            scope_locations=location_codes,
            master_kind=ctx["kind"],
            master_meta=ctx["meta"],
            master_rows=ctx["rows"],
            master_states=ctx["states"],
            master_cities=ctx["cities"],
            master_default_geo=ctx["default_geo"],
            master_next_code=ctx["next_code"],
            master_source_unit=ctx["source_unit"],
            master_search=search_text,
            master_machine_types=ctx["machine_types"],
        )

    @bp.route("/api/asset-management/summary")
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"}, required_section="asset_management")
    def api_summary():
        location_codes = _asset_access_codes()
        if not location_codes:
            return _forbidden()
        filters = services.normalize_filters(request.args)
        return jsonify({"status": "success", "summary": data_fetch.fetch_asset_summary(location_codes, filters)})

    @bp.route("/api/asset-management/register")
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"}, required_section="asset_management")
    def api_register():
        location_codes = _asset_access_codes()
        if not location_codes:
            return _forbidden()

        filters = services.normalize_filters(request.args)
        return jsonify({"status": "success", **data_fetch.fetch_asset_register_rows(location_codes, filters)})

    @bp.route("/api/asset-management/po_lookup")
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"}, required_section="asset_management")
    def api_po_lookup():
        location_codes = _asset_access_codes()
        if not location_codes:
            return _forbidden()

        po_no = services.clean_text(request.args.get("po_no"))
        unit = services.clean_text(request.args.get("unit")).upper()

        allowed_units = list(data_fetch.ASSET_PO_LOOKUP_UNITS)
        units = [unit] if unit in allowed_units else allowed_units

        result = data_fetch.fetch_asset_po_details_from_store_units(po_no, units=units)
        status_code = 200 if result.get("status") == "success" else 404
        return jsonify(result), status_code

    @bp.route("/api/asset-management/masters/states")
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"}, required_section="asset_management")
    def api_reference_states():
        location_codes = _asset_access_codes()
        if not location_codes:
            return _forbidden()
        return jsonify({"status": "success", "states": data_fetch.fetch_asset_reference_states()})

    @bp.route("/api/asset-management/masters/cities")
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"}, required_section="asset_management")
    def api_reference_cities():
        location_codes = _asset_access_codes()
        if not location_codes:
            return _forbidden()
        state_id = services.to_int(request.args.get("state_id"), 0) or 0
        return jsonify({"status": "success", "cities": data_fetch.fetch_asset_reference_cities(state_id)})

    @bp.route("/api/asset-management/masters/<kind>/list")
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"}, required_section="asset_management")
    def api_reference_master_list(kind: str):
        location_codes = _asset_access_codes()
        if not location_codes:
            return _forbidden()
        search_text = services.clean_text(request.args.get("search"))
        try:
            ctx = _reference_master_context(kind, search_text)
        except ValueError as exc:
            return jsonify({"status": "error", "message": str(exc)}), 400
        return jsonify(
            {
                "status": "success",
                "kind": ctx["kind"],
                "rows": ctx["rows"],
                "next_code": ctx["next_code"],
                "default_geo": ctx["default_geo"],
            }
        )

    @bp.route("/api/asset-management/masters/<kind>/details")
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"}, required_section="asset_management")
    def api_reference_master_detail(kind: str):
        location_codes = _asset_access_codes()
        if not location_codes:
            return _forbidden()
        record_id = services.to_int(request.args.get("id"), 0) or 0
        if record_id <= 0:
            return jsonify({"status": "error", "message": "Record id is required."}), 400
        try:
            master_kind = services.normalize_reference_master_kind(kind)
        except ValueError as exc:
            return jsonify({"status": "error", "message": str(exc)}), 400
        detail = _reference_master_detail(master_kind, record_id)
        if not detail:
            return jsonify({"status": "error", "message": "Master record not found."}), 404
        return jsonify({"status": "success", "kind": master_kind, "detail": detail})

    @bp.route("/api/asset-management/masters/<kind>", methods=["POST"])
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"}, required_section="asset_management")
    def api_reference_master_save(kind: str):
        location_codes = _asset_access_codes()
        if not location_codes:
            return _forbidden()
        try:
            master_kind = services.normalize_reference_master_kind(kind)
        except ValueError as exc:
            return jsonify({"status": "error", "message": str(exc)}), 400
        payload = services.build_reference_master_payload(master_kind, _payload())
        result = _reference_master_save(master_kind, payload)
        if result.get("status") != "success":
            return jsonify(result), 400 if result.get("code") in {"required", "duplicate"} else 500
        detail = result.get("detail") or _reference_master_detail(master_kind, result.get("id"))
        _audit(
            f"save_{master_kind}_master",
            entity_type=f"asset_{master_kind}_master",
            entity_id=result.get("id"),
            summary=f"{services.reference_master_meta(master_kind)['title']} saved",
            details={"name": payload.get("name"), "code": result.get("code")},
        )
        return jsonify(
            {
                "status": "success",
                "kind": master_kind,
                "mode": result.get("mode"),
                "message": services.reference_master_meta(master_kind)["success_label"],
                "detail": detail,
                "rows": _reference_master_rows(master_kind),
                "next_code": (
                    data_fetch.fetch_asset_next_manufacturer_code()
                    if services.reference_master_source(master_kind) == "manufacturer"
                    else data_fetch.fetch_asset_next_supplier_code()
                    if services.reference_master_source(master_kind) == "supplier"
                    else data_fetch.fetch_asset_next_equipment_code()
                ),
            }
        )

    @bp.route("/api/asset-management/assets/<int:asset_id>", methods=["GET"])
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"}, required_section="asset_management")
    def api_asset_detail(asset_id: int):
        location_codes = _asset_access_codes()
        if not location_codes:
            return _forbidden()
        detail, error_response = _detail_or_404(asset_id, location_codes)
        if error_response:
            return error_response
        return jsonify({"status": "success", "detail": detail})

    @bp.route("/api/asset-management/assets", methods=["POST"])
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"}, required_section="asset_management")
    def api_create_asset():
        location_codes = _asset_access_codes()
        if not location_codes:
            return _forbidden()
        payload = services.build_asset_payload(_payload())
        coverage_payload = services.build_coverage_payload(_payload())
        missing = services.validate_asset_payload(payload, require_asset_value=True)
        coverage_missing = services.validate_coverage_payload(coverage_payload)
        missing.extend([item for item in coverage_missing if item not in missing])
        if missing:
            return jsonify({"status": "error", "message": f"Missing required fields: {', '.join(missing)}"}), 400
        if payload.get("locationId") not in [item.get("id") for item in data_fetch.fetch_asset_locations(location_codes)]:
            return jsonify({"status": "error", "message": "Selected location is outside your scope."}), 403

        result = data_fetch.create_asset_record(
            payload,
            actor_user_id=_current_account_id(),
            actor_username=_current_username(),
            coverage_payload=coverage_payload,
        )
        if result.get("status") != "success":
            _audit("create_asset", status="error", entity_type="asset", summary=result.get("message"), details=payload)
            return jsonify(result), 500

        detail = result.get("detail") or {}
        asset = detail.get("asset") or {}
        _audit(
            "create_asset",
            entity_type="asset",
            entity_id=asset.get("asset_id"),
            unit=asset.get("location_code"),
            summary=f"Created asset {asset.get('asset_code')}",
            details={"asset_code": asset.get("asset_code"), "location": asset.get("location_name"), "asset_value": asset.get("asset_value")},
        )
        return jsonify(
            {
                **result,
                "redirect_url": url_for("asset_management.asset_detail_page", asset_id=result.get("asset_id")),
            }
        )

    @bp.route("/api/asset-management/assets/<int:asset_id>", methods=["POST"])
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"}, required_section="asset_management")
    def api_update_asset(asset_id: int):
        location_codes = _asset_access_codes()
        if not location_codes:
            return _forbidden()
        detail, error_response = _detail_or_404(asset_id, location_codes)
        if error_response:
            return error_response
        payload = services.build_asset_payload(_payload())
        coverage_payload = services.build_coverage_payload(_payload())
        missing = services.validate_asset_payload(payload, require_asset_value=False)
        if services.clean_text(coverage_payload.get("coverage_type")):
            coverage_missing = services.validate_coverage_payload(coverage_payload)
            missing.extend([item for item in coverage_missing if item not in missing])
        if missing:
            return jsonify({"status": "error", "message": f"Missing required fields: {', '.join(missing)}"}), 400
        if payload.get("locationId") not in [item.get("id") for item in data_fetch.fetch_asset_locations(location_codes)]:
            return jsonify({"status": "error", "message": "Selected location is outside your scope."}), 403

        result = data_fetch.update_asset_record(asset_id, payload, actor_user_id=_current_account_id())
        if result.get("status") != "success":
            _audit("update_asset", status="error", entity_type="asset", entity_id=asset_id, summary=result.get("message"), details=payload)
            return jsonify(result), 500

        if services.clean_text(coverage_payload.get("coverage_type")):
            coverage_result = data_fetch.save_asset_coverage(
                asset_id,
                coverage_payload,
                actor_user_id=_current_account_id(),
                actor_username=_current_username(),
            )
            if coverage_result.get("status") != "success":
                return jsonify(coverage_result), 400 if coverage_result.get("code") == "required" else 500
            result["coverage"] = coverage_result.get("coverage")
            result["detail"] = coverage_result.get("detail") or result.get("detail")

        updated_asset = (result.get("detail") or detail).get("asset") or detail["asset"]
        _audit(
            "update_asset",
            entity_type="asset",
            entity_id=asset_id,
            unit=updated_asset.get("location_code"),
            summary=f"Updated asset {updated_asset.get('asset_code')}",
            details=payload,
        )
        return jsonify(result)

    @bp.route("/api/asset-management/assets/<int:asset_id>/maintenance", methods=["POST"])
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"}, required_section="asset_management")
    def api_upsert_maintenance(asset_id: int):
        location_codes = _asset_access_codes()
        if not location_codes:
            return _forbidden()
        detail, error_response = _detail_or_404(asset_id, location_codes)
        if error_response:
            return error_response
        payload = services.build_maintenance_payload(_payload())
        result = data_fetch.upsert_asset_maintenance(asset_id, payload, actor_user_id=_current_account_id())
        if result.get("status") != "success":
            _audit("save_maintenance", status="error", entity_type="asset", entity_id=asset_id, summary=result.get("message"), details=payload)
            return jsonify(result), 500
        _audit(
            "save_maintenance",
            entity_type="asset",
            entity_id=asset_id,
            unit=detail["asset"].get("location_code"),
            summary=f"Saved maintenance for {detail['asset'].get('asset_code')}",
            details=payload,
        )
        return jsonify(result)

    @bp.route("/api/asset-management/assets/<int:asset_id>/coverage", methods=["GET"])
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"}, required_section="asset_management")
    def api_asset_coverage(asset_id: int):
        location_codes = _asset_access_codes()
        if not location_codes:
            return _forbidden()
        detail, error_response = _detail_or_404(asset_id, location_codes)
        if error_response:
            return error_response
        return jsonify(
            {
                "status": "success",
                "coverage": detail.get("coverage") or {},
                "history": detail.get("coverage_history") or [],
            }
        )

    @bp.route("/api/asset-management/assets/<int:asset_id>/coverage", methods=["POST"])
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"}, required_section="asset_management")
    def api_save_asset_coverage(asset_id: int):
        location_codes = _asset_access_codes()
        if not location_codes:
            return _forbidden()
        detail, error_response = _detail_or_404(asset_id, location_codes)
        if error_response:
            return error_response
        payload = services.build_coverage_payload(_payload())
        missing = services.validate_coverage_payload(payload)
        if missing:
            return jsonify({"status": "error", "message": f"Missing required fields: {', '.join(missing)}"}), 400
        result = data_fetch.save_asset_coverage(
            asset_id,
            payload,
            actor_user_id=_current_account_id(),
            actor_username=_current_username(),
        )
        if result.get("status") != "success":
            _audit("save_coverage", status="error", entity_type="asset", entity_id=asset_id, summary=result.get("message"), details=payload)
            return jsonify(result), 400 if result.get("code") == "required" else 500
        _audit(
            "save_coverage",
            entity_type="asset",
            entity_id=asset_id,
            unit=detail["asset"].get("location_code"),
            summary=f"Saved coverage for {detail['asset'].get('asset_code')}",
            details=payload,
        )
        return jsonify(result)

    @bp.route("/api/asset-management/coverage-alerts")
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"}, required_section="asset_management")
    def api_coverage_alert_assets():
        location_codes = _asset_access_codes()
        if not location_codes:
            return _forbidden()
        priority = services.clean_text(request.args.get("priority") or request.args.get("coverage_alert")).lower()
        rows = data_fetch.fetch_asset_coverage_alert_assets(location_codes=location_codes, priority=priority, limit=5000)
        return jsonify({"status": "success", "rows": rows, "count": len(rows)})

    @bp.route("/api/asset-management/coverage-reminders/test", methods=["POST"])
    @login_required(allowed_roles={"IT"}, required_section="asset_management")
    def api_test_coverage_reminders():
        role_base = services.clean_text(session.get("role")).split(":", 1)[0].strip()
        if role_base != "IT":
            return jsonify({"status": "error", "message": "Only IT can trigger test reminders."}), 403
        payload = request.get_json(silent=True) or {}
        units = payload.get("units") or _asset_access_codes()
        result = run_asset_coverage_reminder_job(
            send_graph_mail_with_attachment=_send_graph_mail_with_attachment,
            units=units,
            force=True,
            actor=_current_username(),
        )
        return jsonify(result)

    @bp.route("/api/asset-management/breakdowns")
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"}, required_section="asset_management")
    def api_breakdowns():
        location_codes = _asset_access_codes()
        if not location_codes:
            return _forbidden()
        status = services.clean_text(request.args.get("status") or "")
        rows = data_fetch.fetch_asset_breakdown_tickets(location_codes=location_codes, status=status or None, limit=1000)
        return jsonify({"status": "success", "rows": rows, "count": len(rows)})

    @bp.route("/api/asset-management/breakdowns", methods=["POST"])
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"}, required_section="asset_management")
    def api_create_breakdown():
        location_codes = _asset_access_codes()
        if not location_codes:
            return _forbidden()
        payload = _payload()
        asset_id = services.to_int(payload.get("asset_id"))
        detail, error_response = _detail_or_404(asset_id, location_codes)
        if error_response:
            return error_response
        result = data_fetch.create_asset_breakdown_ticket(
            payload,
            actor_user_id=_current_account_id(),
            actor_username=_current_username(),
        )
        if result.get("status") != "success":
            return jsonify(result), 400
        ticket = result.get("ticket") or {}
        mail_result = _send_breakdown_ticket_email(ticket, "New Asset Breakdown Ticket")
        result["mail"] = mail_result
        _audit(
            "open_breakdown",
            entity_type="asset_breakdown",
            entity_id=ticket.get("ticket_id"),
            unit=ticket.get("unit_code") or detail["asset"].get("location_code"),
            summary=f"Opened breakdown ticket {ticket.get('ticket_no')}",
            details=payload,
        )
        return jsonify(result)

    @bp.route("/api/asset-management/breakdowns/<int:ticket_id>/close", methods=["POST"])
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"}, required_section="asset_management")
    def api_close_breakdown(ticket_id: int):
        location_codes = _asset_access_codes()
        if not location_codes:
            return _forbidden()
        ticket = data_fetch.fetch_asset_breakdown_ticket(ticket_id, location_codes=location_codes)
        if not ticket:
            return jsonify({"status": "error", "message": "Breakdown ticket not found for your scope."}), 404
        result = data_fetch.close_asset_breakdown_ticket(
            ticket_id,
            _payload(),
            actor_user_id=_current_account_id(),
            actor_username=_current_username(),
        )
        if result.get("status") == "otp_required":
            return jsonify(result), 428
        if result.get("status") != "success":
            return jsonify(result), 400
        closed_ticket = result.get("ticket") or {}
        if closed_ticket.get("non_repairable"):
            result["final_update_mail"] = _send_asset_final_update(closed_ticket, "Asset Marked Non-Repairable")
        audit_action = "close_breakdown" if closed_ticket.get("status") != "OPEN" else "update_breakdown"
        audit_summary = (
            f"Closed breakdown ticket {closed_ticket.get('ticket_no')}"
            if closed_ticket.get("status") != "OPEN"
            else f"Updated breakdown ticket {closed_ticket.get('ticket_no')}"
        )
        _audit(
            audit_action,
            entity_type="asset_breakdown",
            entity_id=ticket_id,
            unit=closed_ticket.get("unit_code"),
            summary=audit_summary,
            details=_payload(),
        )
        return jsonify(result)

    @bp.route("/api/asset-management/assets/<int:asset_id>/action-otp", methods=["POST"])
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"}, required_section="asset_management")
    def api_request_asset_action_otp(asset_id: int):
        location_codes = _asset_access_codes()
        if not location_codes:
            return _forbidden()
        detail, error_response = _detail_or_404(asset_id, location_codes)
        if error_response:
            return error_response
        payload = _payload()
        action_type = services.clean_text(payload.get("action_type") or "NON_REPAIRABLE").upper()
        ticket_id = services.to_int(payload.get("ticket_id"))
        result = data_fetch.create_asset_action_otp(
            asset_id,
            action_type,
            ticket_id=ticket_id,
            actor_user_id=_current_account_id(),
            actor_username=_current_username(),
        )
        if result.get("status") != "success":
            return jsonify(result), 400
        action_label = "Non-Repairable" if action_type == "NON_REPAIRABLE" else "Condemned / Disposal / Decommission"
        mail_result = _send_asset_action_otp_email(result, action_label)
        response = {"status": "success", "message": mail_result.get("message") or "OTP sent to Asset-Purchase.", "mail": mail_result}
        if mail_result.get("otp_code"):
            response["otp_code"] = mail_result.get("otp_code")
        _audit("request_action_otp", entity_type="asset", entity_id=asset_id, unit=detail["asset"].get("location_code"), summary=f"Requested OTP for {action_label}")
        return jsonify(response)

    @bp.route("/api/asset-management/breakdown-reminders/test", methods=["POST"])
    @login_required(allowed_roles={"IT"}, required_section="asset_management")
    def api_test_breakdown_reminders():
        payload = request.get_json(silent=True) or {}
        units = payload.get("units") or _asset_access_codes()
        result = run_asset_breakdown_reminder_job(
            send_graph_mail_with_attachment=_send_graph_mail_with_attachment,
            units=units,
            force=True,
            actor=_current_username(),
        )
        return jsonify(result)

    @bp.route("/api/asset-management/notifications")
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"}, required_section="asset_management")
    def api_asset_notifications():
        location_codes = _asset_access_codes()
        if not location_codes:
            return _forbidden()
        unread_only = str(request.args.get("unread") or "").strip().lower() in {"1", "true", "yes"}
        rows = data_fetch.fetch_asset_notifications(
            location_codes=location_codes,
            username=_current_username(),
            unread_only=unread_only,
            limit=50,
        )
        unread_count = len(
            data_fetch.fetch_asset_notifications(
                location_codes=location_codes,
                username=_current_username(),
                unread_only=True,
                limit=100,
            )
        )
        return jsonify({"status": "success", "notifications": rows, "unread_count": unread_count})

    @bp.route("/api/asset-management/notifications/<int:notification_id>")
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"}, required_section="asset_management")
    def api_asset_notification_detail(notification_id: int):
        location_codes = _asset_access_codes()
        if not location_codes:
            return _forbidden()
        item = data_fetch.fetch_asset_notification(notification_id, location_codes=location_codes, username=_current_username())
        if not item:
            return jsonify({"status": "error", "message": "Notification not found for your scope."}), 404
        return jsonify({"status": "success", "notification": item})

    @bp.route("/api/asset-management/notifications/<int:notification_id>/read", methods=["POST"])
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"}, required_section="asset_management")
    def api_asset_notification_read(notification_id: int):
        location_codes = _asset_access_codes()
        if not location_codes:
            return _forbidden()
        item = data_fetch.fetch_asset_notification(notification_id, location_codes=location_codes, username=_current_username())
        if not item:
            return jsonify({"status": "error", "message": "Notification not found for your scope."}), 404
        result = data_fetch.mark_asset_notification_read(notification_id, _current_username())
        return jsonify(result), 200 if result.get("status") == "success" else 400

    @bp.route("/api/asset-management/notifications/<int:notification_id>/accept", methods=["POST"])
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"}, required_section="asset_management")
    def api_asset_notification_accept(notification_id: int):
        location_codes = _asset_access_codes()
        if not location_codes:
            return _forbidden()
        item = data_fetch.fetch_asset_notification(notification_id, location_codes=location_codes, username=_current_username())
        if not item:
            return jsonify({"status": "error", "message": "Notification not found for your scope."}), 404
        result = data_fetch.accept_asset_notification(notification_id, _current_username(), account_id=_current_account_id())
        if result.get("status") == "success":
            _audit(
                "accept_cross_unit_issue",
                entity_type="asset_notification",
                entity_id=notification_id,
                unit=item.get("target_unit_code"),
                summary=f"Accepted cross-unit asset issue for {item.get('asset_code')}",
                details=item,
            )
        return jsonify(result), 200 if result.get("status") == "success" else 400

    def _movement_response(asset_id, movement_type):
        location_codes = _asset_access_codes()
        if not location_codes:
            return _forbidden()
        detail, error_response = _detail_or_404(asset_id, location_codes)
        if error_response:
            return error_response
        lookups = _load_lookups(location_codes)
        try:
            payload = services.build_movement_payload(movement_type, _payload(), detail["asset"], services.build_lookup_maps(lookups))
        except ValueError as exc:
            return jsonify({"status": "error", "message": str(exc)}), 400

        if movement_type == "STATUS" and services.to_int(payload.get("status_after")) == 5:
            otp_code = services.clean_text(_payload().get("otp_code"))
            verified = data_fetch.verify_asset_action_otp(
                asset_id,
                "COND_DECOMM",
                otp_code,
                actor_username=_current_username(),
                consume=True,
            )
            if verified.get("status") != "success":
                return jsonify({"status": "otp_required", "message": verified.get("message") or "Valid Asset-Purchase OTP is required."}), 428

        result = data_fetch.record_asset_movement(
            asset_id,
            payload,
            actor_username=_current_username(),
            actor_account_id=_current_account_id(),
        )
        if result.get("status") != "success":
            _audit(
                f"{movement_type.lower()}_asset",
                status="error",
                entity_type="asset",
                entity_id=asset_id,
                summary=result.get("message"),
                details=payload,
            )
            return jsonify(result), 500

        updated_asset = (result.get("detail") or detail).get("asset") or detail["asset"]
        _audit(
            f"{movement_type.lower()}_asset",
            entity_type="asset",
            entity_id=asset_id,
            unit=updated_asset.get("location_code"),
            summary=f"{movement_type.title()} completed for {updated_asset.get('asset_code')}",
            details=payload,
        )
        cross_unit_notice = _create_cross_unit_issue_notification(detail.get("asset") or {}, result, movement_type)
        if cross_unit_notice:
            result["notification"] = cross_unit_notice
        if movement_type == "STATUS" and services.to_int(payload.get("status_after")) == 5:
            final_ticket = {
                "ticket_no": "Status Update",
                "asset_code": updated_asset.get("asset_code"),
                "asset_name": updated_asset.get("equipment_name"),
                "machine_type_name": updated_asset.get("machine_type_name"),
                "unit_code": updated_asset.get("location_code"),
                "status_label": "Condemned / Disposed / Decommissioned",
                "breakdown_reason": payload.get("remarks"),
            }
            result["final_update_mail"] = _send_asset_final_update(final_ticket, "Asset Marked Condemned / Disposed / Decommissioned")
        return jsonify(result)

    @bp.route("/api/asset-management/assets/<int:asset_id>/issue", methods=["POST"])
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"}, required_section="asset_management")
    def api_issue_asset(asset_id: int):
        return _movement_response(asset_id, "ISSUE")

    @bp.route("/api/asset-management/assets/<int:asset_id>/return", methods=["POST"])
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"}, required_section="asset_management")
    def api_return_asset(asset_id: int):
        return _movement_response(asset_id, "RETURN")

    @bp.route("/api/asset-management/assets/<int:asset_id>/transfer", methods=["POST"])
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"}, required_section="asset_management")
    def api_transfer_asset(asset_id: int):
        return _movement_response(asset_id, "TRANSFER")

    @bp.route("/api/asset-management/assets/<int:asset_id>/status", methods=["POST"])
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"}, required_section="asset_management")
    def api_status_asset(asset_id: int):
        return _movement_response(asset_id, "STATUS")

    @bp.route("/api/asset-management/assets/<int:asset_id>/upload", methods=["POST"])
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"}, required_section="asset_management")
    def api_upload_attachment(asset_id: int):
        location_codes = _asset_access_codes()
        if not location_codes:
            return _forbidden()
        detail, error_response = _detail_or_404(asset_id, location_codes)
        if error_response:
            return error_response
        upload = request.files.get("file")
        if not upload or not secure_filename(upload.filename or ""):
            return jsonify({"status": "error", "message": "Please choose a file to upload."}), 400

        safe_name = secure_filename(upload.filename)
        asset_dir = os.path.join(current_app.root_path, "data", "uploads", "asset_management", str(asset_id))
        os.makedirs(asset_dir, exist_ok=True)
        stored_name = f"{int(time.time())}_{safe_name}"
        abs_path = os.path.join(asset_dir, stored_name)
        upload.save(abs_path)
        relative_path = os.path.relpath(abs_path, current_app.root_path).replace("\\", "/")
        result = data_fetch.add_asset_attachment(asset_id, safe_name, relative_path)
        if result.get("status") != "success":
            try:
                if os.path.exists(abs_path):
                    os.remove(abs_path)
            except Exception:
                pass
            _audit("upload_attachment", status="error", entity_type="asset", entity_id=asset_id, summary=result.get("message"))
            return jsonify(result), 500
        _audit(
            "upload_attachment",
            entity_type="asset",
            entity_id=asset_id,
            unit=detail["asset"].get("location_code"),
            summary=f"Uploaded attachment for {detail['asset'].get('asset_code')}",
            details={"file_name": safe_name},
        )
        return jsonify(result)

    @bp.route("/api/asset-management/assets/<int:asset_id>/files/<int:file_id>")
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"}, required_section="asset_management")
    def download_attachment(asset_id: int, file_id: int):
        location_codes = _asset_access_codes()
        if not location_codes:
            return _forbidden()
        detail, error_response = _detail_or_404(asset_id, location_codes)
        if error_response:
            return error_response
        attachment = data_fetch.fetch_asset_attachment_detail(asset_id, file_id)
        if not attachment:
            return "Attachment not found.", 404

        raw_path = services.clean_text(attachment.get("file_url"))
        candidates = []
        if raw_path:
            if os.path.isabs(raw_path):
                candidates.append(raw_path)
            candidates.append(os.path.join(current_app.root_path, raw_path.lstrip("/\\")))
            candidates.append(os.path.abspath(raw_path))

        file_path = next((path for path in candidates if os.path.exists(path)), None)
        if not file_path:
            return "Attachment file is not available on disk.", 404

        _audit(
            "download_attachment",
            entity_type="asset",
            entity_id=asset_id,
            unit=detail["asset"].get("location_code"),
            summary=f"Downloaded attachment for {detail['asset'].get('asset_code')}",
            details={"file_id": file_id},
        )
        return send_file(file_path, as_attachment=True, download_name=attachment.get("file_name") or os.path.basename(file_path))

    @bp.route("/api/asset-management/export_job", methods=["POST"])
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"}, required_section="asset_management")
    def api_export_job():
        location_codes = _asset_access_codes()
        if not location_codes:
            return _forbidden()
        payload = request.get_json(silent=True) or {}
        filters = services.normalize_filters(payload)
        export_format = services.clean_text(payload.get("format"), "xlsx").lower()
        if export_format not in {"xlsx", "pdf"}:
            return jsonify({"status": "error", "message": "Unsupported export format."}), 400

        job_id = token_hex(16)
        _excel_job_update(job_id, state="queued", filename=None, format=export_format)
        _export_executor.submit(_queue_export_job, job_id, export_format, location_codes, filters, _current_username())
        _audit(
            "queue_export",
            entity_type="asset_export",
            entity_id=job_id,
            summary=f"Queued {export_format.upper()} export",
            details={"filters": filters, "format": export_format},
        )
        return jsonify({"status": "queued", "job_id": job_id, "format": export_format})

    @bp.route("/api/asset-management/export_job_status")
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"}, required_section="asset_management")
    def api_export_job_status():
        job_id = services.clean_text(request.args.get("job_id"))
        if not job_id:
            return jsonify({"status": "error", "message": "Missing job id."}), 400
        entry = _excel_job_get(job_id)
        if not entry:
            return jsonify({"status": "error", "message": "Job not found."}), 404
        return jsonify(
            {
                "status": "success",
                "state": entry.get("state"),
                "error": entry.get("error"),
                "filename": entry.get("filename"),
                "format": entry.get("format"),
            }
        )

    @bp.route("/api/asset-management/export_job_result")
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"}, required_section="asset_management")
    def api_export_job_result():
        job_id = services.clean_text(request.args.get("job_id"))
        if not job_id:
            return "Missing job id.", 400
        entry = _excel_job_get(job_id)
        if not entry:
            return "Job not found.", 404
        if entry.get("state") != "done":
            return "Job not ready.", 409
        data = _export_cache_get_bytes("asset_management_export_job", job_id)
        if not data:
            return "Export has expired.", 404
        filename = entry.get("filename") or "Asset_Management_Report.xlsx"
        mimetype = entry.get("mimetype") or ("application/pdf" if filename.lower().endswith(".pdf") else "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        return send_file(io.BytesIO(data), as_attachment=True, download_name=filename, mimetype=mimetype)

    return bp
