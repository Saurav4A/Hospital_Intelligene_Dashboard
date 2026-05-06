from __future__ import annotations

import copy
import io
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta

import pandas as pd
from flask import jsonify, render_template, request, send_file, session

from modules import canteen_reports, data_fetch, sms_gateway


def register_canteen_routes(
    app,
    *,
    login_required,
    allowed_units_for_session,
    has_section_access,
    clean_df_columns,
    sanitize_json_payload,
    safe_float,
    safe_int,
    audit_log_event,
    local_tz,
    send_canteen_bulk_notification=None,
):
    _allowed_units_for_session = allowed_units_for_session
    _has_section_access = has_section_access
    _clean_df_columns = clean_df_columns
    _sanitize_json_payload = sanitize_json_payload
    _safe_float = safe_float
    _safe_int = safe_int
    _audit_log_event = audit_log_event
    _send_canteen_bulk_notification = send_canteen_bulk_notification
    LOCAL_TZ = local_tz
    CANTEEN_MENU_MASTER_SECTION = "canteen_menu_master"
    CANTEEN_CANCEL_SECTION = "canteen_bill_receipt_cancel"

    def _no_store_headers():
        return {
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
            "Expires": "0",
        }

    def _canteen_allowed_units() -> list[str]:
        units = []
        for unit in (_allowed_units_for_session() or []):
            unit_key = str(unit or "").strip().upper()
            if unit_key in data_fetch.CANTEEN_ALLOWED_UNITS and unit_key not in units:
                units.append(unit_key)
        return units

    def _unit_error_response(message: str = "Canteen is available only for the units assigned to your login."):
        if (request.path or "").startswith("/api/"):
            return jsonify({"status": "error", "message": message}), 403
        return (message, 403)

    def _can_manage_menu_master() -> bool:
        return bool(_has_section_access(CANTEEN_MENU_MASTER_SECTION))

    def _can_process_cancellations() -> bool:
        return bool(_has_section_access(CANTEEN_CANCEL_SECTION))

    def _can_edit_sale_rate() -> bool:
        return str(session.get("role") or "").strip().upper() == "IT"

    def _audit_canteen_bill_sms_future(future, *, unit: str, result: dict, actor: dict):
        try:
            sms_result = future.result()
        except Exception as exc:
            sms_result = {
                "status": "error",
                "message": str(exc),
            }
        else:
            sms_result = sms_result.as_dict() if hasattr(sms_result, "as_dict") else dict(sms_result or {})
        sms_status = str(sms_result.get("status") or "").strip().lower()
        if sms_status == "success":
            action = "bill_sms_sent"
            status = "success"
            summary = "Canteen bill SMS sent"
        elif sms_status == "skipped":
            action = "bill_sms_skipped"
            status = "warning"
            summary = "Canteen bill SMS skipped"
        else:
            action = "bill_sms_failed"
            status = "error"
            summary = "Canteen bill SMS failed"
        _audit_log_event(
            "canteen",
            action,
            status=status,
            entity_type="bill",
            entity_id=str(result.get("bill_id") or ""),
            unit=unit,
            summary=summary,
            details={
                "bill_no": result.get("bill_no"),
                "mobile": sms_result.get("mobile"),
                "sms_result": sms_result,
            },
            username=str(actor.get("username") or ""),
            role=str(actor.get("role") or ""),
            account_id=_safe_int(actor.get("account_id"), 0) or None,
        )

    def _resolve_canteen_unit(preferred=None):
        allowed = _canteen_allowed_units()
        if not allowed:
            return None, _unit_error_response()
        preferred_key = str(preferred or request.args.get("unit") or "").strip().upper()
        if preferred_key:
            if preferred_key not in allowed:
                return None, _unit_error_response("You do not have access to the selected canteen unit.")
            return preferred_key, None
        return allowed[0], None

    def _format_iso_date(value) -> str:
        if value is None:
            return ""
        raw_text = str(value).strip()
        if raw_text.lower() in {"", "nan", "nat", "none"}:
            return ""
        if isinstance(value, datetime):
            return value.date().isoformat()
        if isinstance(value, date):
            return value.isoformat()
        for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d-%m-%Y"):
            try:
                return datetime.strptime(raw_text[:26], fmt).date().isoformat()
            except Exception:
                continue
        return raw_text[:10]

    def _format_display_datetime(value) -> str:
        if value is None:
            return ""
        raw_text = str(value).strip()
        if raw_text.lower() in {"", "nan", "nat", "none"}:
            return ""
        if isinstance(value, datetime):
            return value.strftime("%d-%b-%Y %I:%M %p")
        if isinstance(value, date):
            return value.strftime("%d-%b-%Y")
        for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                dt = datetime.strptime(raw_text[:26], fmt)
                if "H" in fmt:
                    return dt.strftime("%d-%b-%Y %I:%M %p")
                return dt.strftime("%d-%b-%Y")
            except Exception:
                continue
        return raw_text

    def _branding_for(unit: str) -> dict:
        return copy.deepcopy(data_fetch.CANTEEN_UNIT_BRANDING.get(unit) or {})

    def _report_kind_rows() -> list[dict]:
        return [
            {"id": "menu_wise", "label": "Menu-wise Bill Register"},
            {"id": "bill_register", "label": "Bill Register"},
            {"id": "collections", "label": "Collections"},
            {"id": "ledger", "label": "Ledger"},
            {"id": "wallet_summary", "label": "Wallet Summary"},
            {"id": "wallet_ledger", "label": "Wallet Ledger"},
            {"id": "dues", "label": "Dues List"},
        ]

    def _normalize_report_key(value) -> str:
        report_key = str(value or "bill_register").strip().lower()
        allowed = {row.get("id") for row in _report_kind_rows()}
        return report_key if report_key in allowed else "bill_register"

    def _report_label_for(report_key: str) -> str:
        key = _normalize_report_key(report_key)
        for row in _report_kind_rows():
            if row.get("id") == key:
                return str(row.get("label") or key).strip()
        return key.replace("_", " ").title()

    def _report_type_label_for(unit: str, type_id: int) -> str:
        type_id_int = _safe_int(type_id, 0)
        if type_id_int <= 0:
            return "All Types"
        try:
            for row in _normalize_type_rows(data_fetch.fetch_canteen_types(unit)):
                if _safe_int(row.get("type_id"), 0) == type_id_int:
                    return str(row.get("type_name") or f"Type {type_id_int}").strip() or f"Type {type_id_int}"
        except Exception:
            pass
        return f"Type {type_id_int}"

    def _report_request_params(default_limit: int = 800) -> dict:
        return {
            "report_key": _normalize_report_key(request.args.get("report_key") or "bill_register"),
            "from_date": request.args.get("from_date") or request.args.get("from"),
            "to_date": request.args.get("to_date") or request.args.get("to"),
            "type_id": _safe_int(request.args.get("type_id"), 0),
            "query": request.args.get("q") or "",
            "limit": _safe_int(request.args.get("limit"), default_limit) or default_limit,
        }

    def _df_to_rows(df) -> list[dict]:
        if df is None or df.empty:
            return []
        clean_df = _clean_df_columns(df.copy())
        clean_df = clean_df.astype(object).where(pd.notna(clean_df), None)
        return _sanitize_json_payload(clean_df.to_dict(orient="records"))

    def _normalize_type_rows(df) -> list[dict]:
        rows = []
        for row in _df_to_rows(df):
            rows.append(
                {
                    "type_id": _safe_int(row.get("TypeID"), 0),
                    "type_name": str(row.get("TypeName") or "").strip(),
                    "credit_tag": _safe_int(row.get("CreditTag"), 0),
                }
            )
        return rows

    def _normalize_billing_type_rows(type_rows: list[dict]) -> list[dict]:
        ordered = []
        seen = set()
        for row in (type_rows or []):
            snapshot = data_fetch.canteen_ui_type_snapshot(
                type_id=row.get("type_id"),
                type_name=row.get("type_name"),
                credit_tag=row.get("credit_tag"),
            )
            key = str(snapshot.get("key") or "").strip().lower()
            if key not in {"cash", "room_service_patient", "room_service_delivery"} or key in seen:
                continue
            seen.add(key)
            ordered.append(
                {
                    "type_id": _safe_int(snapshot.get("type_id"), 0),
                    "type_name": str(snapshot.get("type_name") or "").strip(),
                    "credit_tag": _safe_int(snapshot.get("credit_tag"), 0),
                    "type_key": key,
                }
            )
        return ordered

    def _billing_category_rows() -> list[dict]:
        return [
            {
                "category_key": data_fetch.CANTEEN_BILLING_CATEGORY_GENERAL,
                "category_name": "General Billing",
                "type_id": 1,
                "pricing_type_id": 1,
                "wallet_allowed": True,
            },
            {
                "category_key": data_fetch.CANTEEN_BILLING_CATEGORY_ROOM_SERVICE,
                "category_name": "Room Service Billing",
                "type_id": data_fetch.CANTEEN_ROOM_SERVICE_PATIENT_TYPE_ID,
                "pricing_type_id": data_fetch.CANTEEN_ROOM_SERVICE_PATIENT_TYPE_ID,
                "wallet_allowed": True,
            },
            {
                "category_key": data_fetch.CANTEEN_BILLING_CATEGORY_BULK,
                "category_name": "Bulk / Institutional Orders",
                "type_id": 1,
                "pricing_type_id": 1,
                "wallet_allowed": False,
            },
        ]

    def _bulk_subcategory_rows() -> list[dict]:
        return [
            {"id": "party", "label": "Party Orders"},
            {"id": "double_duty", "label": "Double Duty Staff"},
            {"id": "student_institutional", "label": "Student / Institutional Orders"},
        ]

    def _customer_master_type_label(key: str, fallback_name: str = "") -> str:
        key_text = str(key or "").strip().lower()
        if key_text == "cash":
            return "Cash Customer"
        if key_text == "credit":
            return "Credit"
        if key_text == "prepaid":
            return "Prepaid"
        if key_text == "room_service_patient":
            return "Room Service-Patient"
        if key_text == "room_service_delivery":
            return "Room Service-Delivery"
        if key_text == "steward":
            return "Steward"
        return str(fallback_name or "").strip() or "Cash Customer"

    def _normalize_customer_master_type_rows(type_rows: list[dict]) -> list[dict]:
        grouped = {}
        for row in (type_rows or []):
            snapshot = data_fetch.canteen_ui_type_snapshot(
                type_id=row.get("type_id"),
                type_name=row.get("type_name"),
                credit_tag=row.get("credit_tag"),
            )
            key = str(snapshot.get("key") or "").strip().lower()
            if key not in {
                "cash",
                "credit",
                "prepaid",
                "room_service_patient",
                "room_service_delivery",
                "steward",
            } or key in grouped:
                continue
            grouped[key] = {
                "type_id": _safe_int(snapshot.get("type_id"), 0),
                "type_name": _customer_master_type_label(key, snapshot.get("type_name")),
                "credit_tag": _safe_int(snapshot.get("credit_tag"), 0),
                "type_key": key,
            }

        ordered = []
        for key in (
            "cash",
            "credit",
            "prepaid",
            "room_service_patient",
            "room_service_delivery",
            "steward",
        ):
            row = grouped.get(key)
            if not row:
                continue
            ordered.append(
                {
                    "type_id": _safe_int(row.get("type_id"), 0),
                    "type_name": str(row.get("type_name") or "").strip(),
                    "credit_tag": _safe_int(row.get("credit_tag"), 0),
                    "type_key": key,
                }
            )
        return ordered

    def _normalize_pay_mode_rows(df) -> list[dict]:
        rows = []
        for row in _df_to_rows(df):
            rows.append(
                {
                    "pay_mode_id": _safe_int(row.get("PayModeID"), 0),
                    "pay_mode_name": str(row.get("PayModeName") or "").strip(),
                }
            )
        return rows

    def _build_init_payload(unit: str) -> dict:
        today = datetime.now(tz=LOCAL_TZ).date()
        types = _normalize_type_rows(data_fetch.fetch_canteen_types(unit))
        pay_modes = _normalize_pay_mode_rows(data_fetch.fetch_canteen_pay_modes(unit))
        return {
            "status": "success",
            "unit": unit,
            "allowed_units": _canteen_allowed_units(),
            "branding": _branding_for(unit),
            "brandings": {row_unit: _branding_for(row_unit) for row_unit in _canteen_allowed_units()},
            "today": today.isoformat(),
            "defaults": {
                "sale_date": today.isoformat(),
                "receipt_date": today.isoformat(),
                "report_from_date": today.isoformat(),
                "report_to_date": today.isoformat(),
                "due_from_date": today.isoformat(),
                "due_to_date": today.isoformat(),
                "menu_code": "",
            },
            "prepared_by": session.get("username") or session.get("user") or "",
            "customer_modes": [
                {"id": "walkin", "label": "Walk-In"},
                {"id": "registered", "label": "Permanent / Ledger"},
            ],
            "report_kinds": _report_kind_rows(),
            "types": types,
            "billing_types": _normalize_billing_type_rows(types),
            "billing_categories": _billing_category_rows(),
            "bulk_subcategories": _bulk_subcategory_rows(),
            "customer_master_types": _normalize_customer_master_type_rows(types),
            "pay_modes": pay_modes,
            "steward_type_id": data_fetch.CANTEEN_STEWARD_TYPE_ID,
            "rights": {
                "can_manage_menu": _can_manage_menu_master(),
                "can_process_cancellations": _can_process_cancellations(),
                "can_edit_sale_rate": _can_edit_sale_rate(),
            },
            "wallet_payment_mode": data_fetch._canteen_wallet_pay_mode_row(unit),
        }

    def _normalize_menu_rows(df) -> list[dict]:
        rows = []
        for row in _df_to_rows(df):
            rows.append(
                {
                    "menu_id": _safe_int(row.get("MenuID"), 0),
                    "code": str(row.get("Code") or "").strip(),
                    "item_name": str(row.get("ItemName") or "").strip(),
                    "rate": round(_safe_float(row.get("Rate"), 0), 2),
                    "staff_rate": round(_safe_float(row.get("Rate"), 0), 2),
                    "room_service_rate": round(_safe_float(row.get("RoomService"), 0), 2),
                    "effective_rate": round(_safe_float(row.get("EffectiveRate") if "EffectiveRate" in row else row.get("Rate"), 0), 2),
                    "updated_on": _format_display_datetime(row.get("UpdatedOn")),
                    "updated_by": str(row.get("UpdatedBy") or "").strip(),
                }
            )
        return rows

    def _normalize_customer_record(row: dict | None) -> dict | None:
        if not isinstance(row, dict):
            return None
        raw_type_name = str(row.get("TypeName") or "").strip()
        raw_type_id = _safe_int(row.get("PatientType"), 0)
        raw_credit_tag = _safe_int(row.get("CreditTag"), 0)
        billing_type = data_fetch.canteen_ui_type_snapshot(
            type_id=raw_type_id,
            type_name=raw_type_name,
            credit_tag=raw_credit_tag,
        )
        customer_type_key = str(billing_type.get("key") or "").strip().lower()
        customer = {
            "tmp_patient_id": _safe_int(row.get("TmpPatientID"), 0),
            "code": str(row.get("Code") or "").strip(),
            "name": str(row.get("Name") or "").strip(),
            "patient_type": raw_type_id,
            "type_name": raw_type_name,
            "credit_tag": raw_credit_tag,
            "billing_type_id": _safe_int(billing_type.get("type_id"), 0),
            "billing_type_name": str(billing_type.get("type_name") or "").strip(),
            "billing_credit_tag": _safe_int(billing_type.get("credit_tag"), 0),
            "customer_type_id": _safe_int(billing_type.get("type_id"), 0),
            "customer_type_key": customer_type_key,
            "customer_type_name": _customer_master_type_label(customer_type_key, billing_type.get("type_name")),
            "legacy_type_ids": billing_type.get("legacy_type_ids") or [],
            "mobile1": str(row.get("Mobile1") or "").strip(),
            "mobile2": str(row.get("Mobile2") or "").strip(),
            "card_id": _safe_int(row.get("CardID"), 0),
            "dept_id": _safe_int(row.get("DeptID"), 0),
            "ledger_code": str(row.get("LedgerCode") or "").strip(),
            "ledger_name": str(row.get("LedgerName") or "").strip(),
            "department_name": str(row.get("DepartmentName") or "").strip(),
            "card_reference": str(row.get("CardReference") or "").strip(),
            "workflow_context": str(row.get("WorkflowContext") or "").strip(),
            "reference_label": str(row.get("ReferenceLabel") or "").strip(),
            "notes": str(row.get("Notes") or "").strip(),
            "wallet_balance": round(_safe_float(row.get("WalletBalance"), 0), 2),
            "is_active": bool(row.get("IsActive", True)),
        }
        customer["display_name"] = customer["ledger_name"] or customer["name"] or customer["code"]
        customer["category_label"] = customer["customer_type_name"] or customer["billing_type_name"] or customer["type_name"] or "Customer Master"
        customer["identity_hint"] = " | ".join(
            part
            for part in [customer["code"], customer["department_name"], customer["mobile1"], f"Wallet {customer['wallet_balance']:.2f}" if customer["wallet_balance"] > 0 else ""]
            if part
        )
        customer["is_steward"] = customer["patient_type"] == data_fetch.CANTEEN_STEWARD_TYPE_ID or raw_type_name.lower().startswith("steward")
        return customer

    def _normalize_customer_rows(df) -> list[dict]:
        return [row for row in (_normalize_customer_record(row) for row in _df_to_rows(df)) if row]

    def _normalize_patient_rows(df) -> list[dict]:
        rows = []
        for row in _df_to_rows(df):
            patient_id = _safe_int(row.get("PatientId") or row.get("patient_id"), 0)
            registration_no = str(row.get("Registration_No") or row.get("RegistrationNo") or "").strip()
            patient_name = str(row.get("PatientName") or row.get("Patient") or "").strip()
            mobile = str(row.get("Mobile") or "").strip()
            quick_label = " ".join(part for part in [registration_no, patient_name] if part).strip() or patient_name or registration_no
            rows.append(
                {
                    "patient_id": patient_id,
                    "registration_no": registration_no,
                    "patient_name": patient_name,
                    "mobile": mobile,
                    "gender": str(row.get("Gender") or "").strip(),
                    "age": str(row.get("Age") or "").strip(),
                    "quick_label": quick_label,
                    "display_name": quick_label,
                    "category_label": "Patient",
                    "identity_hint": " | ".join(
                        part for part in [str(row.get("Gender") or "").strip(), str(row.get("Age") or "").strip(), mobile] if part
                    ),
                }
            )
        return rows

    def _customer_label_from_due_row(row: dict) -> str:
        return (
            str(row.get("LedgerName") or "").strip()
            or str(row.get("CustomerName") or "").strip()
            or str(row.get("RegistrationNo") or "").strip()
            or "Walk-In"
        )

    def _normalize_due_rows(df) -> list[dict]:
        rows = []
        for row in _df_to_rows(df):
            rows.append(
                {
                    "bill_id": _safe_int(row.get("BillID"), 0),
                    "bill_no": str(row.get("BillNo") or "").strip(),
                    "bill_date": _format_display_datetime(row.get("BillDate")),
                    "bill_date_iso": _format_iso_date(row.get("BillDate")),
                    "net_amount": round(_safe_float(row.get("NetAmount"), 0), 2),
                    "due_amount": round(_safe_float(row.get("DueAmount"), 0), 2),
                    "received_amount": round(_safe_float(row.get("ReceivedAmount"), 0), 2),
                    "last_receipt_date": _format_display_datetime(row.get("LastReceiptDate")),
                    "type_id": _safe_int(row.get("TypeID"), 0),
                    "type_name": str(row.get("TypeName") or "").strip(),
                    "registration_no": str(row.get("RegistrationNo") or "").strip(),
                    "tmp_patient_id": _safe_int(row.get("TmpPatientID"), 0),
                    "customer_name": str(row.get("CustomerName") or "").strip(),
                    "customer_code": str(row.get("CustomerCode") or "").strip(),
                    "ledger_code": str(row.get("LedgerCode") or "").strip(),
                    "ledger_name": str(row.get("LedgerName") or "").strip(),
                    "department_name": str(row.get("DepartmentName") or "").strip(),
                    "customer_label": _customer_label_from_due_row(row),
                    "billing_category": str(row.get("BillingCategoryName") or "").strip(),
                    "bulk_subcategory": str(row.get("BulkSubcategoryName") or "").strip(),
                    "payment_classification": str(row.get("PaymentClassification") or "").strip().title(),
                    "created_by": str(row.get("CreatedByName") or "").strip(),
                }
            )
        return rows

    def _summarize_money_rows(rows: list[dict], *keys: str) -> dict:
        summary = {"row_count": len(rows)}
        for key in keys:
            summary[key] = round(sum(_safe_float(row.get(key), 0) for row in rows), 2)
        return summary

    def _menu_item_summary_rows(rows: list[dict]) -> list[dict]:
        grouped: dict[str, dict] = {}
        for row in rows or []:
            item_name = str(row.get("item_name") or "").strip() or "Unknown Item"
            entry = grouped.setdefault(
                item_name.lower(),
                {
                    "item_name": item_name,
                    "qty": 0.0,
                    "amount": 0.0,
                    "bill_ids": set(),
                    "bill_count": 0,
                },
            )
            entry["qty"] += _safe_float(row.get("qty"), 0)
            entry["amount"] += _safe_float(row.get("amount"), 0)
            bill_id = _safe_int(row.get("bill_id"), 0)
            if bill_id > 0:
                entry["bill_ids"].add(bill_id)
            elif str(row.get("bill_no") or "").strip():
                entry["bill_ids"].add(str(row.get("bill_no") or "").strip())
        summary_rows = []
        for entry in grouped.values():
            summary_rows.append(
                {
                    "item_name": entry["item_name"],
                    "qty": round(_safe_float(entry.get("qty"), 0), 2),
                    "amount": round(_safe_float(entry.get("amount"), 0), 2),
                    "bill_count": len(entry.get("bill_ids") or []),
                }
            )
        return sorted(summary_rows, key=lambda item: (-_safe_float(item.get("qty"), 0), str(item.get("item_name") or "").lower()))

    def _normalize_report_payload(report_key: str, df):
        report_name = str(report_key or "").strip().lower()
        if report_name == "menu_wise":
            rows = []
            for row in _df_to_rows(df):
                rows.append(
                    {
                        "bill_id": _safe_int(row.get("Bill_ID"), 0),
                        "bill_no": str(row.get("BillNo") or "").strip(),
                        "item_name": str(row.get("Itemname") or "").strip(),
                        "bill_date": _format_display_datetime(row.get("BillDate")),
                        "bill_date_iso": _format_iso_date(row.get("BillDate")),
                        "qty": round(_safe_float(row.get("Quantity"), 0), 2),
                        "rate": round(_safe_float(row.get("Rate"), 0), 2),
                        "amount": round(_safe_float(row.get("Amount"), 0), 2),
                        "registration_no": str(row.get("Registration_No") or "").strip(),
                        "created_by": str(row.get("CreatedByName") or "").strip(),
                    }
                )
            item_summary_rows = _menu_item_summary_rows(rows)
            return {
                "rows": rows,
                "summary": _summarize_money_rows(rows, "qty", "amount"),
                "columns": [
                    {"key": "bill_date", "label": "Bill Date"},
                    {"key": "bill_no", "label": "Bill No"},
                    {"key": "item_name", "label": "Menu Item"},
                    {"key": "qty", "label": "Qty", "align": "right"},
                    {"key": "rate", "label": "Rate", "align": "right"},
                    {"key": "amount", "label": "Amount", "align": "right"},
                    {"key": "registration_no", "label": "Legacy Label"},
                    {"key": "created_by", "label": "Created By"},
                ],
                "extra_tables": [
                    {
                        "title": "Item Consumption Summary",
                        "sheet_name": "Item Summary",
                        "summary": _summarize_money_rows(item_summary_rows, "qty", "amount"),
                        "columns": [
                            {"key": "item_name", "label": "Menu Item"},
                            {"key": "qty", "label": "Total Qty", "align": "right"},
                            {"key": "amount", "label": "Total Amount", "align": "right"},
                            {"key": "bill_count", "label": "Bill Count", "align": "right", "format": "count"},
                        ],
                        "rows": item_summary_rows,
                    }
                ],
            }

        if report_name == "bill_register":
            rows = []
            for row in _df_to_rows(df):
                rows.append(
                    {
                        "bill_id": _safe_int(row.get("Bill_ID"), 0),
                        "bill_no": str(row.get("BillNo") or "").strip(),
                        "registration_no": str(row.get("Registration_No") or "").strip(),
                        "bill_date": _format_display_datetime(row.get("BillDate")),
                        "bill_date_iso": _format_iso_date(row.get("BillDate")),
                        "net_amount": round(_safe_float(row.get("NetAmount"), 0), 2),
                        "received_amount": round(_safe_float(row.get("received"), 0), 2),
                        "due_amount": round(_safe_float(row.get("DueAmount"), 0), 2),
                        "type_name": str(row.get("Typename") or "").strip(),
                        "billing_category": str(row.get("BillingCategoryName") or "").strip(),
                        "bulk_subcategory": str(row.get("BulkSubcategoryName") or "").strip(),
                        "payment_classification": str(row.get("PaymentClassification") or "").strip().title(),
                        "created_by": str(row.get("CreatedByName") or "").strip(),
                    }
                )
            return {
                "rows": rows,
                "summary": _summarize_money_rows(rows, "net_amount", "received_amount", "due_amount"),
                "columns": [
                    {"key": "bill_date", "label": "Bill Date"},
                    {"key": "bill_no", "label": "Bill No"},
                    {"key": "registration_no", "label": "Legacy Label"},
                    {"key": "billing_category", "label": "Category"},
                    {"key": "payment_classification", "label": "Payment"},
                    {"key": "type_name", "label": "Type"},
                    {"key": "created_by", "label": "Created By"},
                    {"key": "net_amount", "label": "Net", "align": "right"},
                    {"key": "received_amount", "label": "Received", "align": "right"},
                    {"key": "due_amount", "label": "Due", "align": "right"},
                ],
            }

        if report_name == "dues":
            rows = _normalize_due_rows(df)
            return {
                "rows": rows,
                "summary": _summarize_money_rows(rows, "net_amount", "received_amount", "due_amount"),
                "columns": [
                    {"key": "bill_date", "label": "Bill Date"},
                    {"key": "bill_no", "label": "Bill No"},
                    {"key": "customer_label", "label": "Customer"},
                    {"key": "billing_category", "label": "Category"},
                    {"key": "payment_classification", "label": "Payment"},
                    {"key": "type_name", "label": "Type"},
                    {"key": "created_by", "label": "Created By"},
                    {"key": "net_amount", "label": "Net", "align": "right"},
                    {"key": "received_amount", "label": "Received", "align": "right"},
                    {"key": "due_amount", "label": "Due", "align": "right"},
                    {"key": "last_receipt_date", "label": "Last Receipt"},
                ],
            }

        if report_name == "collections":
            rows = []
            for row in _df_to_rows(df):
                customer_label = (
                    str(row.get("LedgerName") or "").strip()
                    or str(row.get("CustomerName") or "").strip()
                    or str(row.get("RegistrationNo") or "").strip()
                    or "Walk-In"
                )
                rows.append(
                    {
                        "wallet_txn_id": _safe_int(row.get("WalletTxnID"), 0),
                        "receipt_id": _safe_int(row.get("ReceiptID"), 0),
                        "receipt_no": str(row.get("ReceiptNo") or "").strip(),
                        "receipt_date": _format_display_datetime(row.get("ReceiptDate")),
                        "receipt_date_iso": _format_iso_date(row.get("ReceiptDate")),
                        "payment_mode_id": _safe_int(row.get("PaymentModeID"), 0),
                        "type_id": _safe_int(row.get("TypeID"), 0),
                        "bill_id": _safe_int(row.get("BillIDResolved") or row.get("BillID"), 0),
                        "bill_no": str(row.get("BillNo") or "").strip(),
                        "bill_date": _format_display_datetime(row.get("BillDate")),
                        "customer_label": customer_label,
                        "type_name": str(row.get("TypeName") or "").strip(),
                        "billing_category": str(row.get("BillingCategoryName") or "").strip(),
                        "bulk_subcategory": str(row.get("BulkSubcategoryName") or "").strip(),
                        "payment_classification": str(row.get("PaymentClassification") or "").strip().title(),
                        "payment_mode": str(row.get("PaymentModeName") or "").strip(),
                        "net_amount": round(_safe_float(row.get("BillNetAmount"), 0), 2),
                        "received_amount": round(_safe_float(row.get("Amount"), 0), 2),
                        "due_amount": round(_safe_float(row.get("BillDueAmount"), 0), 2),
                        "amount": round(_safe_float(row.get("Amount"), 0), 2),
                        "collected_by": str(row.get("CollectionByName") or "").strip(),
                        "created_by": str(row.get("CreatedByName") or row.get("CollectionByName") or "").strip(),
                        "note": str(row.get("Note") or "").strip(),
                    }
                )
            seen_bill_ids = set()
            net_total = 0.0
            due_total = 0.0
            received_total = 0.0
            wallet_topup_total = 0.0
            for row in rows:
                wallet_txn_id = _safe_int(row.get("wallet_txn_id"), 0)
                row_received = _safe_float(row.get("received_amount"), 0)
                if wallet_txn_id > 0:
                    wallet_topup_total += row_received
                else:
                    received_total += row_received
                bill_id = _safe_int(row.get("bill_id"), 0)
                if bill_id > 0:
                    if bill_id in seen_bill_ids:
                        continue
                    seen_bill_ids.add(bill_id)
                net_total += _safe_float(row.get("net_amount"), 0)
                due_total += _safe_float(row.get("due_amount"), 0)
            return {
                "rows": rows,
                "summary": {
                    "row_count": len(rows),
                    "net_amount": round(net_total, 2),
                    "received_amount": round(received_total, 2),
                    "due_amount": round(due_total, 2),
                    "wallet_topup_amount": round(wallet_topup_total, 2),
                },
                "columns": [
                    {"key": "receipt_date", "label": "Receipt Date"},
                    {"key": "receipt_no", "label": "Receipt No"},
                    {"key": "bill_no", "label": "Bill No"},
                    {"key": "customer_label", "label": "Customer"},
                    {"key": "billing_category", "label": "Category"},
                    {"key": "payment_classification", "label": "Payment"},
                    {"key": "type_name", "label": "Type"},
                    {"key": "payment_mode", "label": "Payment Mode"},
                    {"key": "received_amount", "label": "Received", "align": "right"},
                    {"key": "collected_by", "label": "Collected By"},
                    {"key": "created_by", "label": "Created By"},
                    {"key": "note", "label": "Note"},
                ],
            }

        if report_name == "wallet_summary":
            rows = []
            for row in _df_to_rows(df):
                customer_label = (
                    str(row.get("LedgerName") or "").strip()
                    or str(row.get("Name") or "").strip()
                    or str(row.get("Code") or "").strip()
                    or "Customer"
                )
                rows.append(
                    {
                        "tmp_patient_id": _safe_int(row.get("TmpPatientID"), 0),
                        "customer_label": customer_label,
                        "ledger_code": str(row.get("LedgerCode") or row.get("Code") or "").strip(),
                        "type_name": str(row.get("TypeName") or "").strip(),
                        "opening_balance": round(_safe_float(row.get("OpeningBalance"), 0), 2),
                        "topup_amount": round(_safe_float(row.get("TopUpAmount"), 0), 2),
                        "consumption_amount": round(_safe_float(row.get("ConsumptionAmount"), 0), 2),
                        "closing_balance": round(_safe_float(row.get("ClosingBalance"), 0), 2),
                        "bill_count": _safe_int(row.get("BillCount"), 0),
                        "transaction_count": _safe_int(row.get("TransactionCount"), 0),
                        "department_name": str(row.get("DepartmentName") or "").strip(),
                    }
                )
            return {
                "rows": rows,
                "summary": {
                    "row_count": len(rows),
                    "opening_balance": round(sum(_safe_float(row.get("opening_balance"), 0) for row in rows), 2),
                    "topup_amount": round(sum(_safe_float(row.get("topup_amount"), 0) for row in rows), 2),
                    "consumption_amount": round(sum(_safe_float(row.get("consumption_amount"), 0) for row in rows), 2),
                    "closing_balance": round(sum(_safe_float(row.get("closing_balance"), 0) for row in rows), 2),
                    "bill_count": sum(_safe_int(row.get("bill_count"), 0) for row in rows),
                    "transaction_count": sum(_safe_int(row.get("transaction_count"), 0) for row in rows),
                },
                "columns": [
                    {"key": "customer_label", "label": "Customer"},
                    {"key": "ledger_code", "label": "Code / Ledger"},
                    {"key": "type_name", "label": "Type"},
                    {"key": "opening_balance", "label": "Opening Balance", "align": "right"},
                    {"key": "topup_amount", "label": "Total Top-up", "align": "right"},
                    {"key": "consumption_amount", "label": "Wallet Consumption", "align": "right"},
                    {"key": "closing_balance", "label": "Closing Balance", "align": "right"},
                    {"key": "bill_count", "label": "Bill Count", "align": "right", "format": "count"},
                    {"key": "transaction_count", "label": "Txn Count", "align": "right", "format": "count"},
                ],
            }

        if report_name == "wallet_ledger":
            rows = []
            for row in _df_to_rows(df):
                txn_type = str(row.get("TxnType") or "").strip().upper()
                customer_label = (
                    str(row.get("LedgerName") or "").strip()
                    or str(row.get("Name") or "").strip()
                    or str(row.get("Code") or "").strip()
                    or "Customer"
                )
                amount = round(_safe_float(row.get("Amount"), 0), 2)
                rows.append(
                    {
                        "wallet_txn_id": _safe_int(row.get("WalletTxnID"), 0),
                        "txn_date": _format_display_datetime(row.get("CreatedAt")),
                        "txn_date_iso": _format_iso_date(row.get("CreatedAt")),
                        "customer_label": customer_label,
                        "ledger_code": str(row.get("LedgerCode") or row.get("Code") or "").strip(),
                        "type_name": str(row.get("TypeName") or "").strip(),
                        "txn_type": txn_type.title(),
                        "bill_no": str(row.get("BillNo") or "").strip(),
                        "receipt_no": str(row.get("ReceiptNo") or "").strip(),
                        "topup_amount": amount if txn_type == "TOPUP" else 0.0,
                        "consumption_amount": amount if txn_type == "CONSUMPTION" else 0.0,
                        "opening_balance": round(_safe_float(row.get("OpeningBalance"), 0), 2),
                        "closing_balance": round(_safe_float(row.get("ClosingBalance"), 0), 2),
                        "payment_mode": str(row.get("PaymentModeName") or "").strip(),
                        "remarks": str(row.get("Remarks") or "").strip(),
                        "created_by": str(row.get("CreatedByUserName") or "").strip(),
                    }
                )
            return {
                "rows": rows,
                "summary": {
                    "row_count": len(rows),
                    "topup_amount": round(sum(_safe_float(row.get("topup_amount"), 0) for row in rows), 2),
                    "consumption_amount": round(sum(_safe_float(row.get("consumption_amount"), 0) for row in rows), 2),
                },
                "columns": [
                    {"key": "txn_date", "label": "Date / Time"},
                    {"key": "customer_label", "label": "Customer"},
                    {"key": "ledger_code", "label": "Code / Ledger"},
                    {"key": "txn_type", "label": "Transaction"},
                    {"key": "bill_no", "label": "Bill No"},
                    {"key": "receipt_no", "label": "Receipt No"},
                    {"key": "topup_amount", "label": "Top-up", "align": "right"},
                    {"key": "consumption_amount", "label": "Consumed", "align": "right"},
                    {"key": "opening_balance", "label": "Opening", "align": "right"},
                    {"key": "closing_balance", "label": "Closing", "align": "right"},
                    {"key": "payment_mode", "label": "Payment Mode"},
                    {"key": "remarks", "label": "Remarks"},
                    {"key": "created_by", "label": "Created By"},
                ],
            }

        rows = []
        for row in _df_to_rows(df):
            rows.append(
                {
                    "ledger_name": str(row.get("LedgerName") or "").strip(),
                    "ledger_code": str(row.get("LedgerCode") or "").strip(),
                    "tmp_patient_id": _safe_int(row.get("TmpPatientID"), 0),
                    "type_id": _safe_int(row.get("TypeID"), 0),
                    "type_name": str(row.get("TypeName") or "").strip(),
                    "bill_id": _safe_int(row.get("BillID"), 0),
                    "bill_no": str(row.get("BillNo") or "").strip(),
                    "bill_date": _format_display_datetime(row.get("BillDate")),
                    "bill_date_iso": _format_iso_date(row.get("BillDate")),
                    "registration_no": str(row.get("RegistrationNo") or "").strip(),
                    "net_amount": round(_safe_float(row.get("NetAmount"), 0), 2),
                    "received_amount": round(_safe_float(row.get("ReceivedAmount"), 0), 2),
                    "due_amount": round(_safe_float(row.get("DueAmount"), 0), 2),
                    "last_receipt_date": _format_display_datetime(row.get("LastReceiptDate")),
                    "created_by": str(row.get("CreatedByName") or "").strip(),
                }
            )
        return {
            "rows": rows,
            "summary": _summarize_money_rows(rows, "net_amount", "received_amount", "due_amount"),
            "columns": [
                {"key": "ledger_name", "label": "Ledger / Customer"},
                {"key": "ledger_code", "label": "Ledger Code"},
                {"key": "bill_date", "label": "Bill Date"},
                {"key": "bill_no", "label": "Bill No"},
                {"key": "registration_no", "label": "Legacy Label"},
                {"key": "type_name", "label": "Type"},
                {"key": "created_by", "label": "Created By"},
                {"key": "net_amount", "label": "Net", "align": "right"},
                {"key": "received_amount", "label": "Received", "align": "right"},
                {"key": "due_amount", "label": "Due", "align": "right"},
            ],
        }

    @app.route("/canteen")
    @login_required(required_section="canteen")
    def canteen_dashboard():
        unit, error = _resolve_canteen_unit()
        if error:
            return error
        allowed_units = _canteen_allowed_units()
        return (
            render_template(
                "canteen.html",
                allowed_units=allowed_units,
                default_unit=unit,
                prepared_by=session.get("username") or session.get("user") or "",
                today_iso=datetime.now(tz=LOCAL_TZ).date().isoformat(),
                brandings={row_unit: _branding_for(row_unit) for row_unit in allowed_units},
                initial_init=_build_init_payload(unit),
                can_manage_menu=_can_manage_menu_master(),
                can_process_cancellations=_can_process_cancellations(),
                can_edit_sale_rate=_can_edit_sale_rate(),
            ),
            200,
            _no_store_headers(),
        )

    @app.route("/api/canteen/init")
    @login_required(required_section="canteen")
    def api_canteen_init():
        unit, error = _resolve_canteen_unit()
        if error:
            return error
        return jsonify(_build_init_payload(unit))

    @app.route("/api/canteen/menu", methods=["GET"])
    @login_required(required_section="canteen")
    def api_canteen_menu_search():
        unit, error = _resolve_canteen_unit()
        if error:
            return error
        df = data_fetch.search_canteen_menu(
            unit,
            query=request.args.get("q") or "",
            type_id=_safe_int(request.args.get("type_id"), 0),
            type_name=request.args.get("type_name") or "",
            limit=_safe_int(request.args.get("limit"), 50),
        )
        if df is None:
            return jsonify({"status": "error", "message": "Failed to fetch canteen menu items."}), 500
        return jsonify(
            {
                "status": "success",
                "unit": unit,
                "next_code": data_fetch.fetch_canteen_next_menu_code(unit),
                "items": _normalize_menu_rows(df),
            }
        )

    @app.route("/api/canteen/menu", methods=["POST"])
    @login_required(required_section="canteen")
    def api_canteen_menu_save():
        if not _can_manage_menu_master():
            return jsonify({"status": "error", "message": "Menu Master rights are required to save canteen menu items."}), 403
        payload = _sanitize_json_payload(request.get_json(silent=True) or {})
        unit, error = _resolve_canteen_unit(payload.get("unit"))
        if error:
            return error
        result = data_fetch.upsert_canteen_menu_item(
            unit,
            menu_id=_safe_int(payload.get("menu_id"), 0),
            code=str(payload.get("code") or "").strip(),
            item_name=str(payload.get("item_name") or "").strip(),
            rate=_safe_float(payload.get("rate"), 0),
            staff_rate=_safe_float(payload.get("rate"), 0),
            room_service_rate=_safe_float(payload.get("room_service_rate"), 0),
        )
        if result.get("status") != "success":
            _audit_log_event(
                "canteen",
                "menu_save",
                status="error",
                entity_type="menu_item",
                unit=unit,
                summary="Canteen menu save failed",
                details={"message": result.get("message")},
            )
            return jsonify({"status": "error", "message": result.get("message") or "Failed to save menu item."}), 400
        _audit_log_event(
            "canteen",
            "menu_save",
            status="success",
            entity_type="menu_item",
            entity_id=str(result.get("menu_id") or ""),
            unit=unit,
            summary="Canteen menu saved",
            details={"code": result.get("code"), "menu_id": result.get("menu_id")},
        )
        return jsonify(result)

    @app.route("/api/canteen/customers", methods=["GET"])
    @login_required(required_section="canteen")
    def api_canteen_customer_search():
        unit, error = _resolve_canteen_unit()
        if error:
            return error
        df = data_fetch.search_canteen_customers(
            unit,
            query=request.args.get("q") or "",
            type_id=_safe_int(request.args.get("type_id"), 0),
            limit=_safe_int(request.args.get("limit"), 50),
            include_stewards=str(request.args.get("include_stewards") or "0").strip().lower() in {"1", "true", "yes"},
        )
        if df is None:
            return jsonify({"status": "error", "message": "Failed to fetch canteen customers."}), 500
        return jsonify({"status": "success", "unit": unit, "items": _normalize_customer_rows(df)})

    @app.route("/api/canteen/customer-lookup")
    @login_required(required_section="canteen")
    def api_canteen_customer_lookup():
        unit, error = _resolve_canteen_unit()
        if error:
            return error
        query = request.args.get("q") or ""
        query_text = str(query or "").strip()
        limit = max(1, min(_safe_int(request.args.get("limit"), 20), 40))
        type_id = _safe_int(request.args.get("type_id"), 0)
        customer_limit = min(limit, 20)
        include_patient_matches = bool(
            query_text
            and (
                (query_text.isdigit() and len(query_text) >= 5)
                or (not query_text.isdigit() and len(query_text) >= 3)
            )
        )
        patient_limit = min(limit, 8)
        if include_patient_matches:
            with ThreadPoolExecutor(max_workers=2) as pool:
                customer_future = pool.submit(
                    data_fetch.search_canteen_customers,
                    unit,
                    query=query_text,
                    type_id=type_id,
                    limit=customer_limit,
                )
                patient_future = pool.submit(
                    data_fetch.fetch_modification_patients_search,
                    unit,
                    query=query_text,
                    limit=patient_limit,
                )
                customer_df = customer_future.result()
                patient_df = patient_future.result()
        else:
            customer_df = data_fetch.search_canteen_customers(
                unit,
                query=query_text,
                type_id=type_id,
                limit=customer_limit,
            )
            patient_df = pd.DataFrame()
        if customer_df is None:
            return jsonify({"status": "error", "message": "Failed to fetch customer master suggestions."}), 500
        customer_rows = _normalize_customer_rows(customer_df)
        items = []
        for row in customer_rows:
            items.append(
                {
                    **row,
                    "lookup_kind": "customer",
                    "category_label": row.get("category_label") or row.get("customer_type_name") or "Customer Master",
                    "display_name": row.get("display_name") or row.get("name") or row.get("code") or "Customer",
                    "identity_hint": " | ".join(
                        part
                        for part in [
                            row.get("code"),
                            row.get("department_name"),
                            row.get("mobile1") or row.get("mobile2"),
                            f"Wallet {row.get('wallet_balance', 0):.2f}",
                        ]
                        if part
                    ),
                }
            )
        if patient_df is None:
            return jsonify({"status": "error", "message": "Failed to fetch patient suggestions."}), 500
        for row in _normalize_patient_rows(patient_df):
            items.append(
                {
                    **row,
                    "lookup_kind": "patient",
                }
            )
        return jsonify({"status": "success", "unit": unit, "items": items[:limit]})

    @app.route("/api/canteen/patients")
    @login_required(required_section="canteen")
    def api_canteen_patient_search():
        unit, error = _resolve_canteen_unit()
        if error:
            return error
        df = data_fetch.fetch_modification_patients_search(
            unit,
            query=request.args.get("q") or "",
            limit=_safe_int(request.args.get("limit"), 20),
        )
        if df is None:
            return jsonify({"status": "error", "message": "Failed to fetch patient search results."}), 500
        return jsonify({"status": "success", "unit": unit, "items": _normalize_patient_rows(df)})

    @app.route("/api/canteen/stewards")
    @login_required(required_section="canteen")
    def api_canteen_stewards():
        unit, error = _resolve_canteen_unit()
        if error:
            return error
        df = data_fetch.search_canteen_customers(
            unit,
            query=request.args.get("q") or "",
            type_id=data_fetch.CANTEEN_STEWARD_TYPE_ID,
            limit=_safe_int(request.args.get("limit"), 25),
            include_stewards=True,
        )
        if df is None:
            return jsonify({"status": "error", "message": "Failed to fetch steward list."}), 500
        return jsonify({"status": "success", "unit": unit, "items": _normalize_customer_rows(df)})

    @app.route("/api/canteen/customers/<int:tmp_patient_id>")
    @login_required(required_section="canteen")
    def api_canteen_customer_detail(tmp_patient_id: int):
        unit, error = _resolve_canteen_unit()
        if error:
            return error
        customer = _normalize_customer_record(data_fetch.fetch_canteen_customer_detail(unit, tmp_patient_id))
        if not customer:
            return jsonify({"status": "error", "message": "Customer not found."}), 404
        return jsonify({"status": "success", "unit": unit, "customer": customer})

    @app.route("/api/canteen/customers", methods=["POST"])
    @login_required(required_section="canteen")
    def api_canteen_customer_save():
        payload = _sanitize_json_payload(request.get_json(silent=True) or {})
        unit, error = _resolve_canteen_unit(payload.get("unit"))
        if error:
            return error
        result = data_fetch.upsert_canteen_customer(
            unit,
            tmp_patient_id=_safe_int(payload.get("tmp_patient_id"), 0),
            type_id=_safe_int(payload.get("type_id"), 0),
            code=str(payload.get("code") or "").strip(),
            name=str(payload.get("name") or "").strip(),
            mobile1=str(payload.get("mobile1") or "").strip(),
            mobile2=str(payload.get("mobile2") or "").strip(),
            ledger_code=str(payload.get("ledger_code") or "").strip(),
            ledger_name=str(payload.get("ledger_name") or "").strip(),
            department_name=str(payload.get("department_name") or "").strip(),
            card_reference=str(payload.get("card_reference") or "").strip(),
            workflow_context=str(payload.get("workflow_context") or "").strip(),
            reference_label=str(payload.get("reference_label") or "").strip(),
            notes=str(payload.get("notes") or "").strip(),
            is_active=str(payload.get("is_active") if payload.get("is_active") is not None else "true").strip().lower() in {"1", "true", "yes", "on"},
        )
        if result.get("status") != "success":
            _audit_log_event(
                "canteen",
                "customer_save",
                status="error",
                entity_type="customer",
                unit=unit,
                summary="Canteen customer save failed",
                details={"message": result.get("message")},
            )
            return jsonify({"status": "error", "message": result.get("message") or "Failed to save customer."}), 400
        result["customer"] = _normalize_customer_record(result.get("customer"))
        _audit_log_event(
            "canteen",
            "customer_save",
            status="success",
            entity_type="customer",
            entity_id=str(result.get("tmp_patient_id") or ""),
            unit=unit,
            summary="Canteen customer saved",
            details={"tmp_patient_id": result.get("tmp_patient_id"), "name": (result.get("customer") or {}).get("name")},
        )
        return jsonify(result)

    @app.route("/api/canteen/customers/<int:tmp_patient_id>/wallet/topup", methods=["POST"])
    @login_required(required_section="canteen")
    def api_canteen_customer_wallet_topup(tmp_patient_id: int):
        payload = _sanitize_json_payload(request.get_json(silent=True) or {})
        unit, error = _resolve_canteen_unit(payload.get("unit"))
        if error:
            return error
        result = data_fetch.topup_canteen_customer_wallet(
            unit,
            tmp_patient_id=tmp_patient_id,
            amount=_safe_float(payload.get("amount"), 0),
            payment_mode_id=_safe_int(payload.get("payment_mode_id"), 0),
            remarks=str(payload.get("remarks") or "").strip(),
            actor_user_id=_safe_int(session.get("accountid") or session.get("account_id"), 0),
            actor_username=str(session.get("username") or session.get("user") or "").strip(),
        )
        status_code = 200 if result.get("status") == "success" else 400
        if status_code != 200:
            _audit_log_event(
                "canteen",
                "wallet_topup",
                status="error",
                entity_type="wallet_topup",
                entity_id=str(tmp_patient_id),
                unit=unit,
                summary="Canteen wallet top-up failed",
                details={"message": result.get("message"), "amount": _safe_float(payload.get("amount"), 0)},
            )
            return jsonify({"status": "error", "message": result.get("message") or "Failed to top up wallet."}), status_code
        result["customer"] = _normalize_customer_record(result.get("customer"))
        _audit_log_event(
            "canteen",
            "wallet_topup",
            status="success",
            entity_type="wallet_topup",
            entity_id=str(result.get("transaction_id") or tmp_patient_id),
            unit=unit,
            summary="Canteen wallet topped up",
            details={
                "tmp_patient_id": tmp_patient_id,
                "amount": result.get("topup_amount"),
                "wallet_balance": result.get("wallet_balance"),
                "payment_mode_name": result.get("payment_mode_name"),
                "remarks": str(payload.get("remarks") or "").strip(),
            },
        )
        return jsonify(result)

    @app.route("/api/canteen/bill", methods=["POST"])
    @login_required(required_section="canteen")
    def api_canteen_bill_save():
        payload = _sanitize_json_payload(request.get_json(silent=True) or {})
        unit, error = _resolve_canteen_unit(payload.get("unit"))
        if error:
            return error
        result = data_fetch.save_canteen_bill(
            unit,
            customer_mode=str(payload.get("customer_mode") or "walkin").strip(),
            billing_category=str(payload.get("billing_category") or "").strip(),
            customer_kind=str(payload.get("customer_kind") or "").strip(),
            bulk_subcategory=str(payload.get("bulk_subcategory") or "").strip(),
            type_id=_safe_int(payload.get("type_id"), 0),
            customer_tmp_patient_id=_safe_int(payload.get("customer_tmp_patient_id"), 0),
            walkin_name=str(payload.get("walkin_name") or "").strip(),
            sale_date=datetime.now(tz=LOCAL_TZ).date().isoformat(),
            steward_tmp_patient_id=_safe_int(payload.get("steward_tmp_patient_id"), 0),
            discount_amount=_safe_float(payload.get("discount_amount"), 0),
            received_amount=_safe_float(payload.get("received_amount"), 0),
            zero_received_action=str(payload.get("zero_received_action") or "").strip(),
            payment_mode_id=_safe_int(payload.get("payment_mode_id"), 0),
            receipt_note=str(payload.get("receipt_note") or "").strip(),
            items=payload.get("items") or [],
            actor_user_id=_safe_int(session.get("accountid") or session.get("account_id"), 0),
            actor_username=str(session.get("username") or session.get("user") or "").strip(),
            allow_rate_override=_can_edit_sale_rate(),
        )
        if result.get("status") != "success":
            _audit_log_event(
                "canteen",
                "bill_save",
                status="error",
                entity_type="bill",
                unit=unit,
                summary="Canteen bill save failed",
                details={"message": result.get("message")},
            )
            return jsonify({"status": "error", "message": result.get("message") or "Failed to save canteen bill."}), 400
        result["print_url"] = f"/api/canteen/bill/{_safe_int(result.get('bill_id'), 0)}/print?unit={unit}"
        _audit_log_event(
            "canteen",
            "bill_save",
            status="success",
            entity_type="bill",
            entity_id=str(result.get("bill_id") or ""),
            unit=unit,
            summary="Canteen bill saved",
            details={
                "bill_no": result.get("bill_no"),
                "receipt_no": result.get("receipt_no"),
                "received_amount": result.get("received_amount"),
                "line_count": len(payload.get("items") or []),
                "payment_mode_name": result.get("payment_mode_name"),
                "wallet_payment": bool(result.get("wallet_payment")),
                "billing_category": result.get("billing_category_name"),
                "bulk_subcategory": result.get("bulk_subcategory_name"),
                "payment_classification": result.get("payment_classification"),
            },
        )
        if result.get("notification_roles"):
            _audit_log_event(
                "canteen",
                "bulk_order_notification_required",
                status="success",
                entity_type="bill",
                entity_id=str(result.get("bill_id") or ""),
                unit=unit,
                summary="Canteen bulk order notification roles resolved",
                details={
                    "bill_no": result.get("bill_no"),
                    "bulk_subcategory": result.get("bulk_subcategory_name"),
                    "notification_roles": result.get("notification_roles"),
                },
            )
            if _send_canteen_bulk_notification:
                try:
                    actor_username = str(session.get("username") or session.get("user") or "").strip()
                    notification_result = _send_canteen_bulk_notification(
                        unit,
                        {**result, "created_by": actor_username},
                        {
                            "username": actor_username,
                            "role": str(session.get("role") or "").strip(),
                            "account_id": _safe_int(session.get("accountid") or session.get("account_id"), 0),
                        },
                    )
                    result["notification_status"] = notification_result
                    _audit_log_event(
                        "canteen",
                        "bulk_order_notification_queued",
                        status="success" if str((notification_result or {}).get("status") or "").lower() in {"queued", "success"} else "warning",
                        entity_type="bill",
                        entity_id=str(result.get("bill_id") or ""),
                        unit=unit,
                        summary="Canteen bulk order notification dispatch queued",
                        details={
                            "bill_no": result.get("bill_no"),
                            "bulk_subcategory": result.get("bulk_subcategory_name"),
                            "notification_result": notification_result,
                        },
                    )
                except Exception as notification_error:
                    result["notification_status"] = {
                        "status": "error",
                        "message": str(notification_error),
                    }
                    _audit_log_event(
                        "canteen",
                        "bulk_order_notification_failed",
                        status="error",
                        entity_type="bill",
                        entity_id=str(result.get("bill_id") or ""),
                        unit=unit,
                        summary="Canteen bulk order notification dispatch could not be queued",
                        details={
                            "bill_no": result.get("bill_no"),
                            "bulk_subcategory": result.get("bulk_subcategory_name"),
                            "message": str(notification_error),
                        },
                    )
        if result.get("wallet_payment"):
            _audit_log_event(
                "canteen",
                "wallet_consumption",
                status="success",
                entity_type="wallet_consumption",
                entity_id=str(result.get("wallet_transaction_id") or result.get("bill_id") or ""),
                unit=unit,
                summary="Canteen wallet consumed on bill save",
                details={
                    "bill_id": result.get("bill_id"),
                    "bill_no": result.get("bill_no"),
                    "customer_tmp_patient_id": result.get("customer_tmp_patient_id"),
                    "wallet_amount": result.get("wallet_amount"),
                    "wallet_opening_balance": result.get("wallet_opening_balance"),
                    "wallet_closing_balance": result.get("wallet_closing_balance"),
                    "receipt_id": result.get("receipt_id"),
                    "receipt_no": result.get("receipt_no"),
                },
            )
        sms_mobile = str(result.get("customer_mobile") or payload.get("selected_mobile") or "").strip()
        if sms_mobile:
            actor = {
                "username": str(session.get("username") or session.get("user") or "").strip(),
                "role": str(session.get("role") or "").strip(),
                "account_id": _safe_int(session.get("accountid") or session.get("account_id"), 0),
            }
            sms_future = sms_gateway.queue_canteen_bill_sms(
                mobile=sms_mobile,
                bill_no=str(result.get("bill_no") or ""),
                amount=result.get("net_amount"),
                bill_date=result.get("bill_date"),
            )
            sms_future.add_done_callback(
                lambda future, unit=unit, result=dict(result), actor=actor: _audit_canteen_bill_sms_future(
                    future,
                    unit=unit,
                    result=result,
                    actor=actor,
                )
            )
            result["sms_status"] = "queued"
        else:
            result["sms_status"] = "skipped_no_mobile"
        return jsonify(result)

    @app.route("/api/canteen/bill/<int:bill_id>/print")
    @login_required(required_section="canteen")
    def api_canteen_bill_print_payload(bill_id: int):
        unit, error = _resolve_canteen_unit()
        if error:
            return error
        payload = data_fetch.fetch_canteen_bill_print_payload(unit, bill_id)
        if not payload:
            return jsonify({"status": "error", "message": "Canteen bill not found."}), 404
        payload["bill_date_display"] = _format_display_datetime(payload.get("bill_date"))
        payload["printed_by"] = str(session.get("username") or session.get("user") or "Unknown").strip() or "Unknown"
        payload["printed_at"] = datetime.now(tz=LOCAL_TZ).strftime("%d-%b-%Y %I:%M %p")
        _audit_log_event(
            "canteen",
            "bill_print_payload",
            status="success",
            entity_type="bill",
            entity_id=str(bill_id),
            unit=unit,
            summary="Canteen print payload prepared",
            details={"bill_no": payload.get("bill_no")},
        )
        return jsonify({"status": "success", "unit": unit, "bill": _sanitize_json_payload(payload)})

    @app.route("/api/canteen/dues")
    @login_required(required_section="canteen")
    def api_canteen_due_rows():
        unit, error = _resolve_canteen_unit()
        if error:
            return error
        df = data_fetch.fetch_canteen_due_rows(
            unit,
            from_date=request.args.get("from_date") or request.args.get("from"),
            to_date=request.args.get("to_date") or request.args.get("to"),
            query=request.args.get("q") or "",
            type_id=_safe_int(request.args.get("type_id"), 0),
            only_outstanding=str(request.args.get("only_outstanding") or "true").strip().lower() not in {"0", "false", "no"},
            limit=_safe_int(request.args.get("limit"), 500),
        )
        if df is None:
            return jsonify({"status": "error", "message": "Failed to fetch pending due bills."}), 500
        rows = _normalize_due_rows(df)
        return jsonify({"status": "success", "unit": unit, "summary": _summarize_money_rows(rows, "net_amount", "received_amount", "due_amount"), "items": rows})

    @app.route("/api/canteen/receipts", methods=["POST"])
    @login_required(required_section="canteen")
    def api_canteen_receipts_save():
        payload = _sanitize_json_payload(request.get_json(silent=True) or {})
        unit, error = _resolve_canteen_unit(payload.get("unit"))
        if error:
            return error
        result = data_fetch.save_canteen_receipts(
            unit,
            receipt_date=payload.get("receipt_date"),
            payment_mode_id=_safe_int(payload.get("payment_mode_id"), 0),
            note=str(payload.get("note") or "").strip(),
            bills=payload.get("bills") or [],
            actor_user_id=_safe_int(session.get("accountid") or session.get("account_id"), 0),
            actor_username=str(session.get("username") or session.get("user") or "").strip(),
        )
        if result.get("status") != "success":
            _audit_log_event(
                "canteen",
                "receipt_save",
                status="error",
                entity_type="receipt",
                unit=unit,
                summary="Canteen receipt save failed",
                details={"message": result.get("message")},
            )
            return jsonify({"status": "error", "message": result.get("message") or "Failed to save due collection."}), 400
        _audit_log_event(
            "canteen",
            "receipt_save",
            status="success",
            entity_type="receipt",
            unit=unit,
            summary="Canteen due collection posted",
            details={"receipt_count": len(result.get("receipts") or []), "receipts": result.get("receipts") or []},
        )
        return jsonify(result)

    @app.route("/api/canteen/reports")
    @login_required(required_section="canteen")
    def api_canteen_reports():
        unit, error = _resolve_canteen_unit()
        if error:
            return error
        filters = _report_request_params(default_limit=800)
        report_key = filters["report_key"]
        df = data_fetch.fetch_canteen_report_rows(
            unit,
            report_key,
            from_date=filters["from_date"],
            to_date=filters["to_date"],
            type_id=filters["type_id"],
            query=filters["query"],
            limit=filters["limit"],
        )
        if df is None:
            return jsonify({"status": "error", "message": "Failed to fetch canteen report."}), 500
        payload = _normalize_report_payload(report_key, df)
        if report_key == "bill_register":
            pending = data_fetch.fetch_canteen_cancellation_request_statuses(
                unit,
                [row.get("bill_id") for row in payload.get("rows") or []],
            )
            for row in payload.get("rows") or []:
                request_row = pending.get(_safe_int(row.get("bill_id"), 0))
                if request_row:
                    row["cancellation_request_id"] = request_row.get("request_id")
                    row["cancellation_status"] = request_row.get("status")
                    row["cancellation_requested_by"] = request_row.get("requested_by")
                    row["cancellation_requested_on"] = request_row.get("requested_on")
        elif report_key == "collections":
            payment_pending = data_fetch.fetch_canteen_payment_mode_change_request_statuses(
                unit,
                [row.get("receipt_id") for row in payload.get("rows") or []],
            )
            wallet_payment_pending = data_fetch.fetch_canteen_wallet_topup_payment_mode_change_request_statuses(
                unit,
                [row.get("wallet_txn_id") for row in payload.get("rows") or []],
            )
            receipt_cancel_pending = data_fetch.fetch_canteen_receipt_cancellation_request_statuses(
                unit,
                [row.get("receipt_id") for row in payload.get("rows") or []],
            )
            wallet_cancel_pending = data_fetch.fetch_canteen_wallet_topup_cancellation_request_statuses(
                unit,
                [row.get("wallet_txn_id") for row in payload.get("rows") or []],
            )
            for row in payload.get("rows") or []:
                receipt_id = _safe_int(row.get("receipt_id"), 0)
                wallet_txn_id = _safe_int(row.get("wallet_txn_id"), 0)
                payment_row = payment_pending.get(receipt_id)
                if not payment_row and wallet_txn_id > 0:
                    payment_row = wallet_payment_pending.get(wallet_txn_id)
                if payment_row:
                    row["payment_mode_request_id"] = payment_row.get("request_id")
                    row["payment_mode_request_status"] = payment_row.get("status")
                    row["payment_mode_requested_mode"] = payment_row.get("requested_payment_mode")
                    row["payment_mode_requested_by"] = payment_row.get("requested_by")
                    row["payment_mode_requested_on"] = payment_row.get("requested_on")
                cancel_row = receipt_cancel_pending.get(receipt_id)
                if not cancel_row and wallet_txn_id > 0:
                    cancel_row = wallet_cancel_pending.get(wallet_txn_id)
                if cancel_row:
                    if wallet_txn_id > 0:
                        row["wallet_cancellation_request_id"] = cancel_row.get("request_id")
                        row["wallet_cancellation_status"] = cancel_row.get("status")
                        row["wallet_cancellation_requested_by"] = cancel_row.get("requested_by")
                        row["wallet_cancellation_requested_on"] = cancel_row.get("requested_on")
                    else:
                        row["receipt_cancellation_request_id"] = cancel_row.get("request_id")
                        row["receipt_cancellation_status"] = cancel_row.get("status")
                        row["receipt_cancellation_requested_by"] = cancel_row.get("requested_by")
                        row["receipt_cancellation_requested_on"] = cancel_row.get("requested_on")
        return jsonify(
            {
                "status": "success",
                "unit": unit,
                "report_key": report_key,
                "count": len(payload.get("rows") or []),
                "columns": payload.get("columns") or [],
                "summary": payload.get("summary") or {},
                "items": payload.get("rows") or [],
            }
        )

    @app.route("/api/canteen/cancellation-request", methods=["POST"])
    @login_required(required_section="canteen")
    def api_canteen_cancellation_request_create():
        payload = _sanitize_json_payload(request.get_json(silent=True) or {})
        unit, error = _resolve_canteen_unit(payload.get("unit"))
        if error:
            return error
        result = data_fetch.create_canteen_cancellation_request(
            unit,
            _safe_int(payload.get("bill_id"), 0),
            reason=str(payload.get("reason") or "").strip(),
            actor_user_id=_safe_int(session.get("accountid") or session.get("account_id"), 0),
            actor_username=str(session.get("username") or session.get("user") or "").strip(),
        )
        status_code = 200 if result.get("status") == "success" else 400
        request_row = result.get("request") or {}
        _audit_log_event(
            "canteen",
            "cancellation_request",
            status="success" if status_code == 200 else "error",
            entity_type="bill",
            entity_id=str(payload.get("bill_id") or ""),
            unit=unit,
            summary="Canteen bill cancellation requested" if status_code == 200 else "Canteen bill cancellation request failed",
            details={
                "message": result.get("message"),
                "request_id": request_row.get("request_id"),
                "bill_no": request_row.get("bill_no"),
                "reason": str(payload.get("reason") or "").strip(),
            },
        )
        return jsonify(result), status_code

    @app.route("/api/canteen/cancellation-request/<int:request_id>/recall", methods=["POST"])
    @login_required(required_section="canteen")
    def api_canteen_cancellation_request_recall(request_id: int):
        payload = _sanitize_json_payload(request.get_json(silent=True) or {})
        unit, error = _resolve_canteen_unit(payload.get("unit"))
        if error:
            return error
        result = data_fetch.recall_canteen_cancellation_request(
            unit,
            request_id,
            actor_user_id=_safe_int(session.get("accountid") or session.get("account_id"), 0),
            actor_username=str(session.get("username") or session.get("user") or "").strip(),
            allow_admin=_can_process_cancellations(),
        )
        status_code = 200 if result.get("status") == "success" else 400
        request_row = result.get("request") or {}
        _audit_log_event(
            "canteen",
            "cancellation_recall",
            status="success" if status_code == 200 else "error",
            entity_type="cancellation_request",
            entity_id=str(request_id),
            unit=unit,
            summary="Canteen cancellation request recalled" if status_code == 200 else "Canteen cancellation recall failed",
            details={"message": result.get("message"), "bill_id": request_row.get("bill_id"), "bill_no": request_row.get("bill_no")},
        )
        return jsonify(result), status_code

    @app.route("/api/canteen/payment-mode-change-request", methods=["POST"])
    @login_required(required_section="canteen")
    def api_canteen_payment_mode_change_request_create():
        payload = _sanitize_json_payload(request.get_json(silent=True) or {})
        unit, error = _resolve_canteen_unit(payload.get("unit"))
        if error:
            return error
        result = data_fetch.create_canteen_payment_mode_change_request(
            unit,
            _safe_int(payload.get("receipt_id"), 0),
            requested_payment_mode_id=_safe_int(payload.get("requested_payment_mode_id"), 0),
            reason=str(payload.get("reason") or "").strip(),
            actor_user_id=_safe_int(session.get("accountid") or session.get("account_id"), 0),
            actor_username=str(session.get("username") or session.get("user") or "").strip(),
        )
        status_code = 200 if result.get("status") == "success" else 400
        request_row = result.get("request") or {}
        _audit_log_event(
            "canteen",
            "payment_mode_change_request",
            status="success" if status_code == 200 else "error",
            entity_type="receipt",
            entity_id=str(payload.get("receipt_id") or ""),
            unit=unit,
            summary="Canteen payment mode change requested" if status_code == 200 else "Canteen payment mode change request failed",
            details={
                "message": result.get("message"),
                "request_id": request_row.get("request_id"),
                "bill_no": request_row.get("bill_no"),
                "receipt_no": request_row.get("receipt_no"),
                "requested_payment_mode": request_row.get("requested_payment_mode"),
                "reason": str(payload.get("reason") or "").strip(),
            },
        )
        return jsonify(result), status_code

    @app.route("/api/canteen/payment-mode-change-request/<int:request_id>/recall", methods=["POST"])
    @login_required(required_section="canteen")
    def api_canteen_payment_mode_change_request_recall(request_id: int):
        payload = _sanitize_json_payload(request.get_json(silent=True) or {})
        unit, error = _resolve_canteen_unit(payload.get("unit"))
        if error:
            return error
        result = data_fetch.recall_canteen_payment_mode_change_request(
            unit,
            request_id,
            actor_user_id=_safe_int(session.get("accountid") or session.get("account_id"), 0),
            actor_username=str(session.get("username") or session.get("user") or "").strip(),
            allow_admin=_can_process_cancellations(),
        )
        status_code = 200 if result.get("status") == "success" else 400
        request_row = result.get("request") or {}
        _audit_log_event(
            "canteen",
            "payment_mode_change_recall",
            status="success" if status_code == 200 else "error",
            entity_type="payment_mode_change_request",
            entity_id=str(request_id),
            unit=unit,
            summary="Canteen payment mode change request recalled" if status_code == 200 else "Canteen payment mode change recall failed",
            details={
                "message": result.get("message"),
                "bill_id": request_row.get("bill_id"),
                "bill_no": request_row.get("bill_no"),
                "receipt_id": request_row.get("receipt_id"),
                "receipt_no": request_row.get("receipt_no"),
            },
        )
        return jsonify(result), status_code

    @app.route("/api/canteen/receipt-cancellation-request", methods=["POST"])
    @login_required(required_section="canteen")
    def api_canteen_receipt_cancellation_request_create():
        payload = _sanitize_json_payload(request.get_json(silent=True) or {})
        unit, error = _resolve_canteen_unit(payload.get("unit"))
        if error:
            return error
        result = data_fetch.create_canteen_receipt_cancellation_request(
            unit,
            _safe_int(payload.get("receipt_id"), 0),
            reason=str(payload.get("reason") or "").strip(),
            actor_user_id=_safe_int(session.get("accountid") or session.get("account_id"), 0),
            actor_username=str(session.get("username") or session.get("user") or "").strip(),
        )
        status_code = 200 if result.get("status") == "success" else 400
        request_row = result.get("request") or {}
        _audit_log_event(
            "canteen",
            "receipt_cancellation_request",
            status="success" if status_code == 200 else "error",
            entity_type="receipt",
            entity_id=str(payload.get("receipt_id") or ""),
            unit=unit,
            summary="Canteen receipt cancellation requested" if status_code == 200 else "Canteen receipt cancellation request failed",
            details={
                "message": result.get("message"),
                "request_id": request_row.get("request_id"),
                "bill_no": request_row.get("bill_no"),
                "receipt_no": request_row.get("receipt_no"),
                "reason": str(payload.get("reason") or "").strip(),
            },
        )
        return jsonify(result), status_code

    @app.route("/api/canteen/receipt-cancellation-request/<int:request_id>/recall", methods=["POST"])
    @login_required(required_section="canteen")
    def api_canteen_receipt_cancellation_request_recall(request_id: int):
        payload = _sanitize_json_payload(request.get_json(silent=True) or {})
        unit, error = _resolve_canteen_unit(payload.get("unit"))
        if error:
            return error
        result = data_fetch.recall_canteen_receipt_cancellation_request(
            unit,
            request_id,
            actor_user_id=_safe_int(session.get("accountid") or session.get("account_id"), 0),
            actor_username=str(session.get("username") or session.get("user") or "").strip(),
            allow_admin=_can_process_cancellations(),
        )
        status_code = 200 if result.get("status") == "success" else 400
        request_row = result.get("request") or {}
        _audit_log_event(
            "canteen",
            "receipt_cancellation_recall",
            status="success" if status_code == 200 else "error",
            entity_type="receipt_cancellation_request",
            entity_id=str(request_id),
            unit=unit,
            summary="Canteen receipt cancellation request recalled" if status_code == 200 else "Canteen receipt cancellation recall failed",
            details={
                "message": result.get("message"),
                "bill_id": request_row.get("bill_id"),
                "bill_no": request_row.get("bill_no"),
                "receipt_id": request_row.get("receipt_id"),
                "receipt_no": request_row.get("receipt_no"),
            },
        )
        return jsonify(result), status_code

    @app.route("/api/canteen/receipt-type-change-request", methods=["POST"])
    @login_required(required_section="canteen")
    def api_canteen_receipt_type_change_request_create():
        payload = _sanitize_json_payload(request.get_json(silent=True) or {})
        unit, error = _resolve_canteen_unit(payload.get("unit"))
        if error:
            return error
        result = data_fetch.create_canteen_receipt_type_change_request(
            unit,
            _safe_int(payload.get("receipt_id"), 0),
            requested_type_id=_safe_int(payload.get("requested_type_id"), 0),
            reason=str(payload.get("reason") or "").strip(),
            actor_user_id=_safe_int(session.get("accountid") or session.get("account_id"), 0),
            actor_username=str(session.get("username") or session.get("user") or "").strip(),
        )
        status_code = 200 if result.get("status") == "success" else 400
        request_row = result.get("request") or {}
        _audit_log_event(
            "canteen",
            "receipt_type_change_request",
            status="success" if status_code == 200 else "error",
            entity_type="receipt",
            entity_id=str(payload.get("receipt_id") or ""),
            unit=unit,
            summary="Canteen receipt type change requested" if status_code == 200 else "Canteen receipt type change request failed",
            details={
                "message": result.get("message"),
                "request_id": request_row.get("request_id"),
                "bill_no": request_row.get("bill_no"),
                "receipt_no": request_row.get("receipt_no"),
                "current_type_name": request_row.get("current_type_name"),
                "requested_type_name": request_row.get("requested_type_name"),
                "reason": str(payload.get("reason") or "").strip(),
            },
        )
        return jsonify(result), status_code

    @app.route("/api/canteen/receipt-type-change-request/<int:request_id>/recall", methods=["POST"])
    @login_required(required_section="canteen")
    def api_canteen_receipt_type_change_request_recall(request_id: int):
        payload = _sanitize_json_payload(request.get_json(silent=True) or {})
        unit, error = _resolve_canteen_unit(payload.get("unit"))
        if error:
            return error
        result = data_fetch.recall_canteen_receipt_type_change_request(
            unit,
            request_id,
            actor_user_id=_safe_int(session.get("accountid") or session.get("account_id"), 0),
            actor_username=str(session.get("username") or session.get("user") or "").strip(),
            allow_admin=_can_process_cancellations(),
        )
        status_code = 200 if result.get("status") == "success" else 400
        request_row = result.get("request") or {}
        _audit_log_event(
            "canteen",
            "receipt_type_change_recall",
            status="success" if status_code == 200 else "error",
            entity_type="receipt_type_change_request",
            entity_id=str(request_id),
            unit=unit,
            summary="Canteen receipt type change request recalled" if status_code == 200 else "Canteen receipt type change recall failed",
            details={
                "message": result.get("message"),
                "bill_id": request_row.get("bill_id"),
                "bill_no": request_row.get("bill_no"),
                "receipt_id": request_row.get("receipt_id"),
                "receipt_no": request_row.get("receipt_no"),
            },
        )
        return jsonify(result), status_code

    @app.route("/api/canteen/wallet-topup-cancellation-request", methods=["POST"])
    @login_required(required_section="canteen")
    def api_canteen_wallet_topup_cancellation_request_create():
        payload = _sanitize_json_payload(request.get_json(silent=True) or {})
        unit, error = _resolve_canteen_unit(payload.get("unit"))
        if error:
            return error
        result = data_fetch.create_canteen_wallet_topup_cancellation_request(
            unit,
            _safe_int(payload.get("wallet_txn_id"), 0),
            reason=str(payload.get("reason") or "").strip(),
            actor_user_id=_safe_int(session.get("accountid") or session.get("account_id"), 0),
            actor_username=str(session.get("username") or session.get("user") or "").strip(),
        )
        status_code = 200 if result.get("status") == "success" else 400
        request_row = result.get("request") or {}
        _audit_log_event(
            "canteen",
            "wallet_topup_cancellation_request",
            status="success" if status_code == 200 else "error",
            entity_type="wallet_topup",
            entity_id=str(payload.get("wallet_txn_id") or ""),
            unit=unit,
            summary="Canteen wallet top-up cancellation requested" if status_code == 200 else "Canteen wallet top-up cancellation request failed",
            details={
                "message": result.get("message"),
                "request_id": request_row.get("request_id"),
                "wallet_txn_id": request_row.get("wallet_txn_id"),
                "receipt_no": request_row.get("receipt_no"),
                "reason": str(payload.get("reason") or "").strip(),
            },
        )
        return jsonify(result), status_code

    @app.route("/api/canteen/wallet-topup-cancellation-request/<int:request_id>/recall", methods=["POST"])
    @login_required(required_section="canteen")
    def api_canteen_wallet_topup_cancellation_request_recall(request_id: int):
        payload = _sanitize_json_payload(request.get_json(silent=True) or {})
        unit, error = _resolve_canteen_unit(payload.get("unit"))
        if error:
            return error
        result = data_fetch.recall_canteen_wallet_topup_cancellation_request(
            unit,
            request_id,
            actor_user_id=_safe_int(session.get("accountid") or session.get("account_id"), 0),
            actor_username=str(session.get("username") or session.get("user") or "").strip(),
            allow_admin=_can_process_cancellations(),
        )
        status_code = 200 if result.get("status") == "success" else 400
        request_row = result.get("request") or {}
        _audit_log_event(
            "canteen",
            "wallet_topup_cancellation_recall",
            status="success" if status_code == 200 else "error",
            entity_type="wallet_topup_cancellation_request",
            entity_id=str(request_id),
            unit=unit,
            summary="Canteen wallet top-up cancellation request recalled" if status_code == 200 else "Canteen wallet top-up cancellation recall failed",
            details={
                "message": result.get("message"),
                "wallet_txn_id": request_row.get("wallet_txn_id"),
                "receipt_no": request_row.get("receipt_no"),
            },
        )
        return jsonify(result), status_code

    @app.route("/api/canteen/wallet-topup-payment-mode-change-request", methods=["POST"])
    @login_required(required_section="canteen")
    def api_canteen_wallet_topup_payment_mode_change_request_create():
        payload = _sanitize_json_payload(request.get_json(silent=True) or {})
        unit, error = _resolve_canteen_unit(payload.get("unit"))
        if error:
            return error
        result = data_fetch.create_canteen_wallet_topup_payment_mode_change_request(
            unit,
            _safe_int(payload.get("wallet_txn_id"), 0),
            requested_payment_mode_id=_safe_int(payload.get("requested_payment_mode_id"), 0),
            reason=str(payload.get("reason") or "").strip(),
            actor_user_id=_safe_int(session.get("accountid") or session.get("account_id"), 0),
            actor_username=str(session.get("username") or session.get("user") or "").strip(),
        )
        status_code = 200 if result.get("status") == "success" else 400
        request_row = result.get("request") or {}
        _audit_log_event(
            "canteen",
            "wallet_topup_payment_mode_change_request",
            status="success" if status_code == 200 else "error",
            entity_type="wallet_topup",
            entity_id=str(payload.get("wallet_txn_id") or ""),
            unit=unit,
            summary="Canteen wallet top-up payment mode change requested" if status_code == 200 else "Canteen wallet top-up payment mode change request failed",
            details={
                "message": result.get("message"),
                "request_id": request_row.get("request_id"),
                "wallet_txn_id": request_row.get("wallet_txn_id"),
                "receipt_no": request_row.get("receipt_no"),
                "requested_payment_mode": request_row.get("requested_payment_mode"),
                "reason": str(payload.get("reason") or "").strip(),
            },
        )
        return jsonify(result), status_code

    @app.route("/api/canteen/wallet-topup-payment-mode-change-request/<int:request_id>/recall", methods=["POST"])
    @login_required(required_section="canteen")
    def api_canteen_wallet_topup_payment_mode_change_request_recall(request_id: int):
        payload = _sanitize_json_payload(request.get_json(silent=True) or {})
        unit, error = _resolve_canteen_unit(payload.get("unit"))
        if error:
            return error
        result = data_fetch.recall_canteen_wallet_topup_payment_mode_change_request(
            unit,
            request_id,
            actor_user_id=_safe_int(session.get("accountid") or session.get("account_id"), 0),
            actor_username=str(session.get("username") or session.get("user") or "").strip(),
            allow_admin=_can_process_cancellations(),
        )
        status_code = 200 if result.get("status") == "success" else 400
        request_row = result.get("request") or {}
        _audit_log_event(
            "canteen",
            "wallet_topup_payment_mode_change_recall",
            status="success" if status_code == 200 else "error",
            entity_type="wallet_topup_payment_mode_change_request",
            entity_id=str(request_id),
            unit=unit,
            summary="Canteen wallet top-up payment mode change request recalled" if status_code == 200 else "Canteen wallet top-up payment mode change recall failed",
            details={
                "message": result.get("message"),
                "wallet_txn_id": request_row.get("wallet_txn_id"),
                "receipt_no": request_row.get("receipt_no"),
            },
        )
        return jsonify(result), status_code

    @app.route("/api/canteen-modifications/requests")
    @app.route("/api/canteen-cancellations/requests")
    @login_required(required_section=CANTEEN_CANCEL_SECTION)
    def api_canteen_modification_requests():
        unit_arg = str(request.args.get("unit") or "").strip().upper()
        allowed_units = set(_canteen_allowed_units())
        if not allowed_units:
            return jsonify({"status": "error", "message": "No canteen units are assigned to your login."}), 403
        if unit_arg and unit_arg not in allowed_units:
            return jsonify({"status": "error", "message": "You do not have access to the selected canteen unit."}), 403
        rows = data_fetch.fetch_canteen_modification_requests(
            unit=unit_arg,
            status=request.args.get("status") or "PENDING",
            limit=_safe_int(request.args.get("limit"), 250),
            request_type=request.args.get("request_type") or "",
        )
        if allowed_units:
            rows = [row for row in rows if str(row.get("unit") or "").upper() in allowed_units]
        return jsonify({"status": "success", "items": rows, "count": len(rows), "allowed_units": sorted(allowed_units)})

    @app.route("/api/canteen-modifications/requests/<int:request_id>/decision", methods=["POST"])
    @app.route("/api/canteen-cancellations/requests/<int:request_id>/decision", methods=["POST"])
    @login_required(required_section=CANTEEN_CANCEL_SECTION)
    def api_canteen_modification_request_decision(request_id: int):
        payload = _sanitize_json_payload(request.get_json(silent=True) or {})
        action = str(payload.get("action") or "").strip().lower()
        allowed_units = set(_canteen_allowed_units())
        if not allowed_units:
            return jsonify({"status": "error", "message": "No canteen units are assigned to your login."}), 403
        pending_rows = data_fetch.fetch_canteen_modification_requests(status="PENDING", limit=1000)
        pending_row = next((row for row in pending_rows if _safe_int(row.get("request_id"), 0) == request_id), None)
        if not pending_row:
            return jsonify({"status": "error", "message": "Pending canteen modification request was not found."}), 404
        if allowed_units and str(pending_row.get("unit") or "").upper() not in allowed_units:
            return jsonify({"status": "error", "message": "You do not have access to this canteen unit."}), 403
        result = data_fetch.decide_canteen_modification_request(
            request_id,
            action=action,
            note=str(payload.get("note") or "").strip(),
            actor_user_id=_safe_int(session.get("accountid") or session.get("account_id"), 0),
            actor_username=str(session.get("username") or session.get("user") or "").strip(),
        )
        status_code = 200 if result.get("status") == "success" else 400
        request_row = result.get("request") or {}
        request_type = str(request_row.get("request_type") or pending_row.get("request_type") or "").strip().upper()
        summary_map = {
            ("PAYMENT_MODE_CHANGE", True): "Canteen payment mode change approved",
            ("PAYMENT_MODE_CHANGE", False): "Canteen payment mode change rejected",
            ("WALLET_TOPUP_PAYMENT_MODE_CHANGE", True): "Canteen wallet top-up payment mode change approved",
            ("WALLET_TOPUP_PAYMENT_MODE_CHANGE", False): "Canteen wallet top-up payment mode change rejected",
            ("RECEIPT_TYPE_CHANGE", True): "Canteen receipt type change approved",
            ("RECEIPT_TYPE_CHANGE", False): "Canteen receipt type change rejected",
            ("RECEIPT_CANCELLATION", True): "Canteen receipt cancellation approved",
            ("RECEIPT_CANCELLATION", False): "Canteen receipt cancellation rejected",
            ("WALLET_TOPUP_CANCELLATION", True): "Canteen wallet top-up cancellation approved",
            ("WALLET_TOPUP_CANCELLATION", False): "Canteen wallet top-up cancellation rejected",
            ("CANCELLATION", True): "Canteen bill cancellation approved",
            ("CANCELLATION", False): "Canteen bill cancellation rejected",
        }
        entity_type_map = {
            "PAYMENT_MODE_CHANGE": "payment_mode_change_request",
            "WALLET_TOPUP_PAYMENT_MODE_CHANGE": "wallet_topup_payment_mode_change_request",
            "RECEIPT_TYPE_CHANGE": "receipt_type_change_request",
            "RECEIPT_CANCELLATION": "receipt_cancellation_request",
            "WALLET_TOPUP_CANCELLATION": "wallet_topup_cancellation_request",
            "CANCELLATION": "cancellation_request",
        }
        approved = action.startswith("approve")
        _audit_log_event(
            "canteen",
            "modification_decision",
            status="success" if status_code == 200 else "error",
            entity_type=entity_type_map.get(request_type, "canteen_modification_request"),
            entity_id=str(request_id),
            unit=request_row.get("unit") or "",
            summary=summary_map.get((request_type, approved), "Canteen modification decision failed") if status_code == 200 else "Canteen modification decision failed",
            details={
                "message": result.get("message"),
                "request_type": request_type,
                "action": action,
                "bill_id": request_row.get("bill_id"),
                "bill_no": request_row.get("bill_no"),
                "wallet_txn_id": request_row.get("wallet_txn_id"),
                "receipt_id": request_row.get("receipt_id"),
                "receipt_no": request_row.get("receipt_no"),
                "current_payment_mode": request_row.get("current_payment_mode"),
                "requested_payment_mode": request_row.get("requested_payment_mode"),
                "current_type_name": request_row.get("current_type_name"),
                "requested_type_name": request_row.get("requested_type_name"),
                "applied_receipt_count": result.get("applied_receipt_count"),
                "applied_wallet_cancel": request_row.get("applied_wallet_cancel"),
                "applied_type_change": request_row.get("applied_type_change"),
                "note": str(payload.get("note") or "").strip(),
            },
        )
        return jsonify(result), status_code

    def _send_canteen_report_export(export_format: str):
        unit, error = _resolve_canteen_unit()
        if error:
            return error
        filters = _report_request_params(default_limit=5000)
        report_key = filters["report_key"]
        report_label = _report_label_for(report_key)
        df = data_fetch.fetch_canteen_report_rows(
            unit,
            report_key,
            from_date=filters["from_date"],
            to_date=filters["to_date"],
            type_id=filters["type_id"],
            query=filters["query"],
            limit=filters["limit"],
        )
        if df is None:
            _audit_log_event(
                "canteen",
                "report_export",
                status="error",
                entity_type="report_export",
                unit=unit,
                summary=f"{report_label} export failed",
                details={"format": export_format, "message": "Failed to fetch canteen report rows."},
            )
            return jsonify({"status": "error", "message": "Failed to fetch canteen report for export."}), 500

        payload = _normalize_report_payload(report_key, df)
        rows = payload.get("rows") or []
        if not rows:
            return jsonify({"status": "error", "message": "No report rows matched the selected filters for export."}), 404

        exported_by = str(session.get("username") or session.get("user") or "Unknown").strip() or "Unknown"
        exported_at = datetime.now(tz=LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S")
        try:
            data, filename, mimetype = canteen_reports.build_canteen_export(
                export_format=export_format,
                branding=_branding_for(unit),
                report_label=report_label,
                columns=payload.get("columns") or [],
                rows=rows,
                summary=payload.get("summary") or {},
                extra_tables=payload.get("extra_tables") or [],
                filters={
                    "unit": unit,
                    "from_date": filters["from_date"] or "",
                    "to_date": filters["to_date"] or "",
                    "type_name": _report_type_label_for(unit, filters["type_id"]),
                    "search": filters["query"] or "",
                },
                exported_by=exported_by,
                exported_at=exported_at,
            )
        except Exception as exc:
            _audit_log_event(
                "canteen",
                "report_export",
                status="error",
                entity_type="report_export",
                unit=unit,
                summary=f"{report_label} export failed",
                details={"format": export_format, "message": str(exc)},
            )
            return jsonify({"status": "error", "message": f"Failed to prepare {report_label} export: {exc}"}), 500

        _audit_log_event(
            "canteen",
            "report_export",
            status="success",
            entity_type="report_export",
            unit=unit,
            summary=f"{report_label} exported",
            details={"format": export_format, "row_count": len(rows), "filename": filename},
        )
        return send_file(io.BytesIO(data), as_attachment=True, download_name=filename, mimetype=mimetype)

    @app.route("/api/canteen/reports/export_excel")
    @login_required(required_section="canteen")
    def api_canteen_reports_export_excel():
        return _send_canteen_report_export("xlsx")

    @app.route("/api/canteen/reports/export_pdf")
    @login_required(required_section="canteen")
    def api_canteen_reports_export_pdf():
        return _send_canteen_report_export("pdf")
