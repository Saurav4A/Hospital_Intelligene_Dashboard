from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime
from typing import Any

from . import queries
from .utils import (
    EXCEL_DETAIL_LIMIT,
    REPORT_TYPES,
    auth_status,
    classify_result,
    comparison_periods,
    date_range_from_preset,
    normalize_auth_filter,
    normalize_match_mode,
    normalize_report_type,
    normalize_result_filter,
    parse_positive_int,
    rows_to_jsonable,
    safe_text,
    validate_int_list,
)

TEST_REQUIRED_MESSAGE = "Select at least one pathology test before loading diagnostic data."


def build_filter_payload(payload: dict[str, Any] | None) -> dict[str, Any]:
    payload = payload or {}
    start, end = date_range_from_preset(payload.get("time_preset") or payload.get("preset"), payload.get("from_date"), payload.get("to_date"))
    return {
        "unit": safe_text(payload.get("unit")).upper(),
        "from_date": start.isoformat(),
        "to_date": end.isoformat(),
        "time_preset": safe_text(payload.get("time_preset") or payload.get("preset") or "last_30_days"),
        "test_ids": validate_int_list(payload.get("test_ids") or payload.get("tests")),
        "parameter_ids": validate_int_list(payload.get("parameter_ids") or payload.get("parameters")),
        "patient_id": parse_positive_int(payload.get("patient_id"), 0, minimum=0) or None,
        "patient_search": safe_text(payload.get("patient_search") or payload.get("search"), 100),
        "result_status": normalize_result_filter(payload.get("result_status")),
        "auth_status": normalize_auth_filter(payload.get("auth_status")),
        "visit_types": [safe_text(v, 20).upper() for v in (payload.get("visit_types") or payload.get("visit_type") or []) if safe_text(v)],
        "followup_gap_days": parse_positive_int(payload.get("followup_gap_days"), 90, maximum=3650),
        "report_type": normalize_report_type(payload.get("report_type")),
        "match_mode": normalize_match_mode(payload.get("match_mode")),
        "comparison_mode": safe_text(payload.get("comparison_mode") or "last_30_vs_previous_30"),
    }


def enrich_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    enriched = []
    for row in rows:
        item = dict(row)
        status = classify_result(item)
        item["result_status"] = status
        item["authorization_status"] = auth_status(item.get("result_auth_flag"))
        item["result_date"] = item.get("result_auth_datetime") or item.get("sample_datetime") or item.get("order_datetime") or item.get("visit_date")
        for dt_key in ("visit_date", "order_datetime", "sample_datetime", "sample_accept_datetime", "result_auth_datetime", "result_date", "last_visit_date", "last_test_date"):
            if dt_key in item:
                item[dt_key] = _format_display_datetime(item.get(dt_key))
        item.pop("rn", None)
        item.pop("total_rows", None)
        enriched.append(item)
    return enriched


def _summary(rows: list[dict[str, Any]], total_rows: int | None = None) -> dict[str, Any]:
    patient_ids = {r.get("patient_id") for r in rows if r.get("patient_id") not in (None, "")}
    tests = {r.get("test_id") for r in rows if r.get("test_id") not in (None, "")}
    abnormal = [r for r in rows if str(r.get("result_status") or "").lower() in {"abnormal", "high", "low", "critical"}]
    authorized = [r for r in rows if r.get("authorization_status") == "Authorized"]
    pending = [r for r in rows if r.get("authorization_status") != "Authorized"]
    return {
        "total_tests_found": len(tests),
        "patients_covered": len(patient_ids),
        "abnormal_results": len(abnormal),
        "abnormal_percentage": round((len(abnormal) / len(rows) * 100), 2) if rows else 0,
        "followup_candidates": 0,
        "authorized_reports": len(authorized),
        "pending_authorization_reports": len(pending),
        "selected_tests_count": len(tests),
        "reportable_records": int(total_rows if total_rows is not None else len(rows)),
    }


def get_test_master(unit: str, search: str = "", limit: int = 300) -> dict[str, Any]:
    rows = queries.fetch_test_master(unit, search, limit)
    return {"status": "success", "data": rows_to_jsonable(rows), "count": len(rows)}


def get_parameter_master(unit: str, test_ids: list[int]) -> dict[str, Any]:
    rows = queries.fetch_parameter_master(unit, test_ids)
    return {"status": "success", "data": rows_to_jsonable(rows), "count": len(rows)}


def search_patients(unit: str, query: str, limit: int = 50) -> dict[str, Any]:
    rows = queries.search_patients(unit, query, limit)
    return {"status": "success", "data": rows_to_jsonable(rows), "count": len(rows)}


def get_patient_test_suggestions(unit: str, payload: dict[str, Any]) -> dict[str, Any]:
    filters = build_filter_payload(payload)
    if not (filters.get("patient_id") or filters.get("patient_search")):
        return {"status": "success", "data": [], "count": 0, "patient_count": 0, "message": "Enter a patient before loading test suggestions."}
    result = queries.fetch_patient_test_suggestions(unit, filters)
    rows = rows_to_jsonable(result.get("rows") or [])
    for row in rows:
        row["last_test_date"] = _format_display_datetime(row.get("last_test_date"))
    patients = rows_to_jsonable(result.get("patients") or [])
    patient_count = len(result.get("patient_ids") or [])
    message = ""
    if patient_count > 1:
        message = "Patient search matched multiple patients. Refine the search for comparison."
    elif not rows:
        message = "No tests found for this patient in the selected date range."
    return {
        "status": "success",
        "data": rows,
        "patients": patients,
        "count": len(rows),
        "patient_count": patient_count,
        "message": message,
    }


def get_patient_history(unit: str, payload: dict[str, Any], *, page: int = 1, page_size: int = 50) -> dict[str, Any]:
    filters = build_filter_payload(payload)
    if _needs_narrow_filter(filters):
        return _empty_payload(filters, page, page_size, TEST_REQUIRED_MESSAGE)
    result = queries.fetch_diagnostic_results(unit, filters, page=page, page_size=page_size)
    rows = enrich_rows(result["rows"])
    return {
        "status": "success",
        "filters": filters,
        "rows": rows_to_jsonable(rows),
        "summary": _summary(rows, result["total"]),
        "page": page,
        "page_size": page_size,
        "total_rows": result["total"],
        "has_more": bool(result.get("has_more")),
    }


def get_abnormal_results(unit: str, payload: dict[str, Any], *, page: int = 1, page_size: int = 50) -> dict[str, Any]:
    filters = build_filter_payload(payload)
    if _needs_narrow_filter(filters):
        return _empty_payload(filters, page, page_size, TEST_REQUIRED_MESSAGE)
    filters["result_status"] = "abnormal" if filters["result_status"] == "all" else filters["result_status"]
    result = queries.fetch_diagnostic_results(unit, filters, page=page, page_size=page_size)
    rows = [r for r in enrich_rows(result["rows"]) if str(r.get("result_status")).lower() in {"abnormal", "high", "low", "critical"}]
    for row in rows:
        status = row.get("result_status")
        row["suggested_action"] = "Review and consider follow-up testing" if status == "Abnormal" else f"{status} value observed, clinical review advised"
    return {
        "status": "success",
        "filters": filters,
        "rows": rows_to_jsonable(rows),
        "summary": _summary(rows, result["total"]),
        "page": page,
        "page_size": page_size,
        "total_rows": result["total"],
        "has_more": bool(result.get("has_more")),
    }


def get_patient_test_comparison(unit: str, payload: dict[str, Any]) -> dict[str, Any]:
    filters = build_filter_payload(payload)
    if len(filters["test_ids"]) != 1 or not (filters.get("patient_id") or filters.get("patient_search")):
        return {
            "status": "success",
            "available": False,
            "message": "Select exactly one test and one patient to compare repeated test iterations.",
            "columns": [],
            "rows": [],
        }
    result = queries.fetch_diagnostic_results(unit, filters, page=1, page_size=2000, max_page_size=2000)
    rows = enrich_rows(result["rows"])
    patient_keys = {
        str(r.get("patient_id") or r.get("registration_no") or r.get("patient_name") or "").strip()
        for r in rows
        if str(r.get("patient_id") or r.get("registration_no") or r.get("patient_name") or "").strip()
    }
    if not rows:
        return {"status": "success", "available": False, "message": "No result rows found for comparison.", "columns": [], "rows": []}
    if len(patient_keys) != 1:
        return {
            "status": "success",
            "available": False,
            "message": "Comparison is available only when the patient filter resolves to one patient.",
            "columns": [],
            "rows": [],
        }

    instances: dict[str, dict[str, Any]] = {}
    parameters: dict[str, dict[str, Any]] = {}
    values: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        instance_key = "|".join(str(row.get(k) or "") for k in ("visit_id", "order_id", "sample_id", "result_date"))
        if not instance_key.strip("|"):
            instance_key = str(row.get("result_date") or row.get("visit_date") or "Unknown")
        instances.setdefault(instance_key, {
            "key": f"i{len(instances) + 1}",
            "sort": str(row.get("result_date") or row.get("visit_date") or ""),
            "label": _comparison_instance_label(row),
        })
        param_key = str(row.get("parameter_id") or row.get("parameter_name") or "parameter")
        parameters.setdefault(param_key, {
            "parameter_name": row.get("parameter_name"),
            "unit": row.get("unit"),
            "normal_range": row.get("normal_range"),
        })
        values[(param_key, instance_key)] = {
            "result": row.get("result"),
            "status": row.get("result_status"),
        }

    ordered_instances = sorted(instances.items(), key=lambda item: item[1]["sort"])
    if len(ordered_instances) < 2:
        return {
            "status": "success",
            "available": False,
            "message": "Only one iteration of the selected test was found for this patient in the selected date range.",
            "columns": [],
            "rows": [],
        }

    columns = [
        {"key": "parameter_name", "label": "Parameter"},
        {"key": "unit", "label": "Unit"},
        {"key": "normal_range", "label": "Normal range"},
    ]
    columns.extend({"key": meta["key"], "label": meta["label"]} for _key, meta in ordered_instances)

    out_rows = []
    for param_key, param in sorted(parameters.items(), key=lambda item: str(item[1].get("parameter_name") or "")):
        item = dict(param)
        for instance_key, meta in ordered_instances:
            cell = values.get((param_key, instance_key)) or {}
            result_text = safe_text(cell.get("result"), 80)
            status_text = safe_text(cell.get("status"), 40)
            item[meta["key"]] = f"{result_text} ({status_text})" if result_text and status_text else result_text
        out_rows.append(item)

    first = rows[0]
    return {
        "status": "success",
        "available": True,
        "message": f"Comparison built across {len(ordered_instances)} test iterations.",
        "patient": {
            "name": first.get("patient_name"),
            "registration_no": first.get("registration_no"),
            "age": first.get("patient_age"),
            "mobile": first.get("mobile"),
        },
        "test_name": first.get("test_name"),
        "columns": columns,
        "rows": rows_to_jsonable(out_rows),
    }


def _comparison_instance_label(row: dict[str, Any]) -> str:
    parts = [row.get("result_date") or row.get("visit_date"), row.get("visit_no")]
    return " | ".join(str(p) for p in parts if p) or "Test iteration"


def get_followup_candidates(unit: str, payload: dict[str, Any], *, page: int = 1, page_size: int = 50) -> dict[str, Any]:
    filters = build_filter_payload(payload)
    if _needs_narrow_filter(filters):
        return _empty_payload(filters, page, page_size, TEST_REQUIRED_MESSAGE)
    export_filters = dict(filters)
    export_filters["result_status"] = "all" if filters["result_status"] == "all" else filters["result_status"]
    raw = queries.fetch_diagnostic_results(unit, export_filters, page=1, page_size=1, for_export=True)["rows"]
    rows = enrich_rows(raw)
    latest: dict[tuple[Any, Any, Any], dict[str, Any]] = {}
    for row in rows:
        key = (row.get("patient_id"), row.get("test_id"), row.get("parameter_id"))
        current_dt = _parse_dt(row.get("result_date"))
        prev_dt = _parse_dt(latest.get(key, {}).get("result_date")) if key in latest else None
        if key not in latest or (current_dt and (not prev_dt or current_dt > prev_dt)):
            latest[key] = row

    now = datetime.now()
    candidates = []
    gap = int(filters["followup_gap_days"])
    for row in latest.values():
        last_dt = _parse_dt(row.get("result_date"))
        days = (now - last_dt).days if last_dt else None
        status = str(row.get("result_status") or "")
        due = days is None or days >= gap
        abnormal = status.lower() in {"abnormal", "high", "low", "critical"}
        if not due and not abnormal:
            continue
        reason_parts = []
        if abnormal:
            reason_parts.append("Previous abnormal result")
        if status == "High":
            reason_parts.append("High value observed")
        if status == "Low":
            reason_parts.append("Low value observed")
        if due:
            reason_parts.append("No repeat test in selected period")
        if not reason_parts:
            reason_parts.append("Eligible for repeat screening")
        item = {
            "patient_name": row.get("patient_name"),
            "registration_no": row.get("registration_no"),
            "patient_age": row.get("patient_age"),
            "mobile": row.get("mobile"),
            "last_visit_date": row.get("visit_date"),
            "last_test_date": row.get("result_date"),
            "test_name": row.get("test_name"),
            "parameter_name": row.get("parameter_name"),
            "last_result": row.get("result"),
            "normal_range": row.get("normal_range"),
            "result_status": status,
            "days_since_last_test": days,
            "suggested_followup_reason": "; ".join(reason_parts),
            "priority": "High" if abnormal and due else ("Medium" if abnormal or due else "Low"),
            "patient_id": row.get("patient_id"),
            "test_id": row.get("test_id"),
            "parameter_id": row.get("parameter_id"),
        }
        candidates.append(item)
    candidates.sort(key=lambda r: ({"High": 0, "Medium": 1, "Low": 2}.get(r.get("priority"), 9), -(r.get("days_since_last_test") or 0)))
    start = (page - 1) * page_size
    page_rows = candidates[start:start + page_size]
    summary = _summary(rows, len(candidates))
    summary["followup_candidates"] = len(candidates)
    return {
        "status": "success",
        "filters": filters,
        "rows": rows_to_jsonable(page_rows),
        "summary": summary,
        "page": page,
        "page_size": page_size,
        "total_rows": len(candidates),
    }


def build_report_preview(unit: str, payload: dict[str, Any], *, page: int = 1, page_size: int = 50) -> dict[str, Any]:
    filters = build_filter_payload(payload)
    if _needs_narrow_filter(filters):
        return _empty_payload(filters, page, page_size, TEST_REQUIRED_MESSAGE)
    report_type = filters["report_type"]
    if report_type == "abnormal_results":
        return get_abnormal_results(unit, filters, page=page, page_size=page_size)
    if report_type == "followup_candidates":
        return get_followup_candidates(unit, filters, page=page, page_size=page_size)
    if report_type == "time_period_comparison":
        return build_time_period_comparison(unit, payload)

    result = queries.fetch_diagnostic_results(unit, filters, page=page, page_size=page_size)
    rows = enrich_rows(result["rows"])
    if report_type == "test_wise_patient":
        rows = _test_wise_rows(rows)
    elif report_type == "multi_test_screening":
        rows = _screening_rows(rows, filters["match_mode"], filters["test_ids"])
    elif report_type == "test_launch_opportunity":
        rows = _launch_opportunity_rows(rows)
    return {
        "status": "success",
        "report_type": report_type,
        "report_name": REPORT_TYPES[report_type],
        "filters": filters,
        "rows": rows_to_jsonable(rows),
        "summary": _summary(rows, result["total"]),
        "page": page,
        "page_size": page_size,
        "total_rows": result["total"],
        "has_more": bool(result.get("has_more")),
    }


def build_export_dataset(unit: str, payload: dict[str, Any]) -> dict[str, Any]:
    filters = build_filter_payload(payload)
    if _needs_narrow_filter(filters):
        data = _empty_payload(filters, 1, 0, TEST_REQUIRED_MESSAGE)
        data["unit"] = unit
        data["report_type"] = filters["report_type"]
        data["report_name"] = REPORT_TYPES[filters["report_type"]]
        data["detail_rows"] = []
        data["abnormal_rows"] = []
        data["followup_rows"] = []
        return data
    report_type = filters["report_type"]
    if report_type == "time_period_comparison":
        return build_time_period_comparison(unit, payload)
    if report_type == "followup_candidates":
        data = get_followup_candidates(unit, filters, page=1, page_size=50000)
        data["report_type"] = report_type
        data["report_name"] = REPORT_TYPES[report_type]
        return data
    result = queries.fetch_diagnostic_results(unit, filters, page=1, page_size=1, for_export=True, export_limit=EXCEL_DETAIL_LIMIT + 1)
    raw_rows = result["rows"]
    export_truncated = len(raw_rows) > EXCEL_DETAIL_LIMIT
    if export_truncated:
        raw_rows = raw_rows[:EXCEL_DETAIL_LIMIT]
    rows = enrich_rows(raw_rows)
    abnormal = [r for r in rows if str(r.get("result_status") or "").lower() in {"abnormal", "high", "low", "critical"}]
    followup = []
    shaped = rows
    if report_type == "test_wise_patient":
        shaped = _test_wise_rows(rows)
    elif report_type == "abnormal_results":
        shaped = abnormal
    elif report_type == "multi_test_screening":
        shaped = _screening_rows(rows, filters["match_mode"], filters["test_ids"])
    elif report_type == "test_launch_opportunity":
        shaped = _launch_opportunity_rows(rows)
    export_note = ""
    if export_truncated:
        export_note = f"Export limited to the latest {EXCEL_DETAIL_LIMIT:,} parameter rows for performance. Narrow filters or split the date range for a complete detailed export."
    return {
        "status": "success",
        "unit": unit,
        "report_type": report_type,
        "report_name": REPORT_TYPES[report_type],
        "filters": filters,
        "rows": rows_to_jsonable(shaped),
        "detail_rows": rows_to_jsonable(rows),
        "abnormal_rows": rows_to_jsonable(abnormal),
        "followup_rows": rows_to_jsonable(followup),
        "summary": _summary(rows, len(shaped)),
        "total_rows": len(shaped),
        "export_truncated": export_truncated,
        "export_note": export_note,
    }


def build_time_period_comparison(unit: str, payload: dict[str, Any]) -> dict[str, Any]:
    filters = build_filter_payload(payload)
    periods = comparison_periods(filters.get("comparison_mode"), payload or {})
    rows = []
    for period in periods:
        local = dict(filters)
        local["from_date"] = period["from_date"]
        local["to_date"] = period["to_date"]
        data = queries.fetch_diagnostic_results(unit, local, page=1, page_size=1, for_export=True)
        detail = enrich_rows(data["rows"])
        abnormal_count = sum(1 for r in detail if str(r.get("result_status") or "").lower() in {"abnormal", "high", "low", "critical"})
        patient_first_dates: dict[Any, Any] = {}
        for row in detail:
            pid = row.get("patient_id") or row.get("registration_no") or row.get("patient_name")
            dt = row.get("result_date")
            if pid not in patient_first_dates or str(dt) < str(patient_first_dates[pid]):
                patient_first_dates[pid] = dt
        rows.append({
            "period": period["name"],
            "from_date": period["from_date"],
            "to_date": period["to_date"],
            "test_name": "Selected Tests",
            "total_patients": len({r.get("patient_id") or r.get("registration_no") for r in detail}),
            "total_tests": len(detail),
            "normal_results": sum(1 for r in detail if r.get("result_status") == "Normal"),
            "abnormal_results": abnormal_count,
            "high_results": sum(1 for r in detail if r.get("result_status") == "High"),
            "low_results": sum(1 for r in detail if r.get("result_status") == "Low"),
            "unclassified_results": sum(1 for r in detail if r.get("result_status") == "Unclassified"),
            "abnormal_percentage": round((abnormal_count / len(detail) * 100), 2) if detail else 0,
            "repeat_patients": max(0, len(detail) - len(patient_first_dates)),
            "new_patients": len(patient_first_dates),
            "followup_due_patients": 0,
        })
    return {
        "status": "success",
        "report_type": "time_period_comparison",
        "report_name": REPORT_TYPES["time_period_comparison"],
        "filters": filters,
        "rows": rows_to_jsonable(rows),
        "summary": _summary(rows, len(rows)),
        "page": 1,
        "page_size": len(rows),
        "total_rows": len(rows),
    }


def report_options() -> dict[str, Any]:
    return {
        "status": "success",
        "report_types": [{"key": key, "label": label} for key, label in REPORT_TYPES.items()],
        "time_presets": [
            {"key": "last_7_days", "label": "Last 7 days"},
            {"key": "last_15_days", "label": "Last 15 days"},
            {"key": "last_30_days", "label": "Last 30 days"},
            {"key": "last_60_days", "label": "Last 60 days"},
            {"key": "last_90_days", "label": "Last 90 days"},
            {"key": "last_180_days", "label": "Last 180 days"},
            {"key": "current_fy", "label": "Current financial year"},
            {"key": "previous_fy", "label": "Previous financial year"},
            {"key": "custom", "label": "Custom range"},
        ],
        "result_statuses": ["All", "Normal", "High", "Low", "Abnormal", "Critical", "Unclassified"],
        "auth_statuses": ["All", "Authorized", "Pending Authorization"],
        "visit_types": ["OPD", "IPD", "DPV", "HCV"],
        "match_modes": [
            {"key": "any", "label": "Match any selected test"},
            {"key": "all", "label": "Match all selected tests"},
            {"key": "abnormal_any", "label": "Abnormal in any selected test"},
            {"key": "abnormal_all", "label": "Abnormal in all selected tests"},
            {"key": "missing_repeat", "label": "Missing repeat test"},
        ],
    }


def settings_payload() -> dict[str, Any]:
    return {
        "status": "success",
        "settings": {
            "default_time_preset": "last_30_days",
            "default_followup_gap_days": 90,
            "active_sources": ["inhouse_pathology"],
            "planned_sources": ["inhouse_radiology", "outsourced_pathology", "outsourced_radiology"],
        },
    }


def _needs_narrow_filter(filters: dict[str, Any]) -> bool:
    return not filters.get("test_ids")


def _empty_payload(filters: dict[str, Any], page: int, page_size: int, message: str) -> dict[str, Any]:
    summary = {
        "total_tests_found": 0,
        "patients_covered": 0,
        "abnormal_results": 0,
        "abnormal_percentage": 0,
        "followup_candidates": 0,
        "authorized_reports": 0,
        "pending_authorization_reports": 0,
        "selected_tests_count": len(filters.get("test_ids") or []),
        "reportable_records": 0,
    }
    return {
        "status": "success",
        "message": message,
        "report_type": filters.get("report_type"),
        "report_name": REPORT_TYPES.get(filters.get("report_type"), "Diagnostic Report"),
        "filters": filters,
        "rows": [],
        "summary": summary,
        "page": page,
        "page_size": page_size,
        "total_rows": 0,
        "has_more": False,
    }


def _parse_dt(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "").split(".")[0])
    except Exception:
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                return datetime.strptime(str(value)[:19], fmt)
            except Exception:
                continue
    return None


def _format_display_datetime(value: Any) -> str:
    if value is None or value == "":
        return ""
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M")
    if isinstance(value, date):
        return value.strftime("%Y-%m-%d")
    text = str(value).strip()
    if not text:
        return ""
    try:
        return datetime.fromisoformat(text.replace("Z", "").split(".")[0]).strftime("%Y-%m-%d %H:%M")
    except Exception:
        pass
    if "." in text:
        text = text.split(".", 1)[0]
    return text[:16] if len(text) >= 16 else text


def _test_wise_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[Any, Any], dict[str, Any]] = {}
    for row in rows:
        key = (row.get("test_id"), row.get("patient_id") or row.get("registration_no"))
        item = grouped.setdefault(key, {
            "test_name": row.get("test_name"),
            "patient_name": row.get("patient_name"),
            "registration_no": row.get("registration_no"),
            "patient_age": row.get("patient_age"),
            "mobile": row.get("mobile"),
            "visit_date": row.get("visit_date"),
            "sample_date": row.get("sample_datetime"),
            "result_summary": "",
            "abnormal_parameter_count": 0,
            "authorization_status": row.get("authorization_status"),
            "doctor_name": row.get("doctor_name"),
            "parameter_count": 0,
        })
        item["parameter_count"] += 1
        if str(row.get("result_status") or "").lower() in {"abnormal", "high", "low", "critical"}:
            item["abnormal_parameter_count"] += 1
        item["result_summary"] = f"{item['abnormal_parameter_count']} abnormal of {item['parameter_count']} parameters"
    return list(grouped.values())


def _screening_rows(rows: list[dict[str, Any]], match_mode: str, selected_tests: list[int]) -> list[dict[str, Any]]:
    selected = set(selected_tests or [])
    by_patient: dict[Any, dict[str, Any]] = {}
    for row in rows:
        key = row.get("patient_id") or row.get("registration_no") or row.get("patient_name")
        item = by_patient.setdefault(key, {
            "patient_name": row.get("patient_name"),
            "registration_no": row.get("registration_no"),
            "patient_age": row.get("patient_age"),
            "mobile": row.get("mobile"),
            "tests_done": set(),
            "tests_abnormal": set(),
            "tests_not_repeated": set(),
            "last_test_date": row.get("result_date"),
            "risk_followup_summary": "",
        })
        item["tests_done"].add(row.get("test_name"))
        if str(row.get("result_status") or "").lower() in {"abnormal", "high", "low", "critical"}:
            item["tests_abnormal"].add(row.get("test_name"))
        if str(row.get("result_date")) > str(item.get("last_test_date")):
            item["last_test_date"] = row.get("result_date")

    out = []
    for item in by_patient.values():
        done_count = len(item["tests_done"])
        abnormal_count = len(item["tests_abnormal"])
        if match_mode == "all" and selected and done_count < len(selected):
            continue
        if match_mode == "abnormal_any" and abnormal_count == 0:
            continue
        if match_mode == "abnormal_all" and selected and abnormal_count < len(selected):
            continue
        item["risk_followup_summary"] = f"{done_count} tests done; {abnormal_count} tests abnormal"
        item["tests_done"] = ", ".join(sorted(safe_text(x) for x in item["tests_done"] if safe_text(x)))
        item["tests_abnormal"] = ", ".join(sorted(safe_text(x) for x in item["tests_abnormal"] if safe_text(x)))
        item["tests_not_repeated"] = ", ".join(sorted(safe_text(x) for x in item["tests_not_repeated"] if safe_text(x)))
        out.append(item)
    return out


def _launch_opportunity_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    opportunities = []
    for row in rows:
        if str(row.get("result_status") or "").lower() not in {"abnormal", "high", "low", "critical"}:
            continue
        opportunities.append({
            "patient_name": row.get("patient_name"),
            "registration_no": row.get("registration_no"),
            "patient_age": row.get("patient_age"),
            "mobile": row.get("mobile"),
            "previous_relevant_test": row.get("test_name"),
            "previous_result": row.get("result"),
            "previous_result_status": row.get("result_status"),
            "previous_test_date": row.get("result_date"),
            "suggested_new_test": "Related diagnostic follow-up",
            "reason_for_recommendation": "Previous abnormal or risk-signalling diagnostic history",
            "priority": "High" if row.get("result_status") in {"Critical", "Abnormal"} else "Medium",
        })
    return opportunities
