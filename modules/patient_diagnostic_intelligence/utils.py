from __future__ import annotations

import math
import re
from datetime import date, datetime, timedelta
from typing import Any


MODULE_KEY = "patient_diagnostic_intelligence"
MODULE_NAME = "Patient Diagnostic Intelligence"
DEFAULT_PAGE_SIZE = 25
MAX_PAGE_SIZE = 100
PDF_ROW_LIMIT = 1200
EXCEL_DETAIL_LIMIT = 50000


REPORT_TYPES = {
    "patient_history": "Patient Diagnostic History Report",
    "test_wise_patient": "Test-wise Patient Report",
    "parameter_wise": "Parameter-wise Result Report",
    "abnormal_results": "Abnormal Result Report",
    "followup_candidates": "Follow-up Candidate Report",
    "multi_test_screening": "Multi-Test Screening Report",
    "time_period_comparison": "Time-Period Comparison Report",
    "test_launch_opportunity": "Test Launch Opportunity Report",
}


RESULT_STATUSES = {"all", "normal", "high", "low", "abnormal", "critical", "unclassified"}
AUTH_STATUSES = {"all", "authorized", "pending"}
MATCH_MODES = {
    "any",
    "all",
    "abnormal_any",
    "abnormal_all",
    "missing_repeat",
}


def today_local() -> date:
    return datetime.now().date()


def parse_date(value: Any, default: date | None = None) -> date | None:
    if value is None or value == "":
        return default
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip()
    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(text[:10], fmt).date()
        except Exception:
            continue
    try:
        return datetime.fromisoformat(text).date()
    except Exception:
        return default


def date_range_from_preset(preset: str | None, from_date: Any = None, to_date: Any = None) -> tuple[date, date]:
    today = today_local()
    preset_norm = str(preset or "last_30_days").strip().lower()
    if preset_norm == "custom":
        start = parse_date(from_date, today - timedelta(days=29))
        end = parse_date(to_date, today)
        return normalize_date_range(start, end)

    days_map = {
        "last_7_days": 7,
        "last_15_days": 15,
        "last_30_days": 30,
        "last_60_days": 60,
        "last_90_days": 90,
        "last_180_days": 180,
    }
    if preset_norm in days_map:
        days = days_map[preset_norm]
        return today - timedelta(days=days - 1), today

    fy_start_month = 4
    fy_year = today.year if today.month >= fy_start_month else today.year - 1
    if preset_norm == "current_fy":
        return date(fy_year, fy_start_month, 1), date(fy_year + 1, fy_start_month, 1) - timedelta(days=1)
    if preset_norm == "previous_fy":
        return date(fy_year - 1, fy_start_month, 1), date(fy_year, fy_start_month, 1) - timedelta(days=1)
    return today - timedelta(days=29), today


def comparison_periods(mode: str | None, payload: dict[str, Any]) -> list[dict[str, str]]:
    today = today_local()
    mode_norm = str(mode or "last_30_vs_previous_30").strip().lower()
    periods: list[tuple[str, date, date]] = []
    if mode_norm == "this_month_vs_previous_month":
        first_this = date(today.year, today.month, 1)
        first_prev = (first_this - timedelta(days=1)).replace(day=1)
        periods = [
            ("This Month", first_this, today),
            ("Previous Month", first_prev, first_this - timedelta(days=1)),
        ]
    elif mode_norm == "current_quarter_vs_previous_quarter":
        q_month = ((today.month - 1) // 3) * 3 + 1
        q_start = date(today.year, q_month, 1)
        prev_end = q_start - timedelta(days=1)
        prev_start = date(prev_end.year, ((prev_end.month - 1) // 3) * 3 + 1, 1)
        periods = [("Current Quarter", q_start, today), ("Previous Quarter", prev_start, prev_end)]
    elif mode_norm == "current_fy_vs_previous_fy":
        cur = date_range_from_preset("current_fy")
        prev = date_range_from_preset("previous_fy")
        periods = [("Current FY", cur[0], cur[1]), ("Previous FY", prev[0], prev[1])]
    elif mode_norm == "custom_a_vs_b":
        a = normalize_date_range(parse_date(payload.get("period_a_from"), today - timedelta(days=29)), parse_date(payload.get("period_a_to"), today))
        b = normalize_date_range(parse_date(payload.get("period_b_from"), today - timedelta(days=59)), parse_date(payload.get("period_b_to"), today - timedelta(days=30)))
        periods = [("Period A", a[0], a[1]), ("Period B", b[0], b[1])]
    else:
        a_to = today
        a_from = today - timedelta(days=29)
        b_to = a_from - timedelta(days=1)
        b_from = b_to - timedelta(days=29)
        periods = [("Last 30 Days", a_from, a_to), ("Previous 30 Days", b_from, b_to)]
    return [{"name": name, "from_date": start.isoformat(), "to_date": end.isoformat()} for name, start, end in periods]


def normalize_date_range(start: date | None, end: date | None) -> tuple[date, date]:
    today = today_local()
    start = start or today - timedelta(days=29)
    end = end or today
    if start > end:
        start, end = end, start
    return start, end


def validate_int_list(values: Any, *, max_items: int = 250) -> list[int]:
    if values in (None, ""):
        return []
    if isinstance(values, str):
        raw_items = [part.strip() for part in re.split(r"[,|]", values) if part.strip()]
    elif isinstance(values, (list, tuple, set)):
        raw_items = list(values)
    else:
        raw_items = [values]

    out: list[int] = []
    seen = set()
    for item in raw_items:
        try:
            val = int(str(item).strip())
        except Exception:
            continue
        if val <= 0 or val in seen:
            continue
        out.append(val)
        seen.add(val)
        if len(out) >= max_items:
            break
    return out


def parse_positive_int(value: Any, default: int, *, minimum: int = 1, maximum: int | None = None) -> int:
    try:
        parsed = int(value)
    except Exception:
        parsed = default
    parsed = max(minimum, parsed)
    if maximum is not None:
        parsed = min(maximum, parsed)
    return parsed


def normalize_result_filter(value: Any) -> str:
    norm = str(value or "all").strip().lower().replace(" ", "_")
    return norm if norm in RESULT_STATUSES else "all"


def normalize_auth_filter(value: Any) -> str:
    norm = str(value or "all").strip().lower().replace(" ", "_")
    return norm if norm in AUTH_STATUSES else "all"


def normalize_report_type(value: Any) -> str:
    norm = str(value or "patient_history").strip().lower()
    return norm if norm in REPORT_TYPES else "patient_history"


def normalize_match_mode(value: Any) -> str:
    norm = str(value or "any").strip().lower()
    return norm if norm in MATCH_MODES else "any"


def safe_text(value: Any, limit: int | None = None) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    return text[:limit] if limit and len(text) > limit else text


def to_jsonable(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (datetime, date)):
        return value.isoformat(sep=" ") if isinstance(value, datetime) else value.isoformat()
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    return value


def rows_to_jsonable(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{key: to_jsonable(val) for key, val in row.items()} for row in rows]


def coerce_number(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        try:
            f = float(value)
            return f if math.isfinite(f) else None
        except Exception:
            return None
    text = str(value).strip()
    if not text:
        return None
    match = re.search(r"[-+]?\d+(?:\.\d+)?", text.replace(",", ""))
    if not match:
        return None
    try:
        return float(match.group(0))
    except Exception:
        return None


def classify_result(row: dict[str, Any]) -> str:
    flag = safe_text(row.get("abnormal_flag") or row.get("AbnormalFlag")).lower()
    if flag in {"critical", "c", "panic"}:
        return "Critical"
    if flag in {"abnormal", "a", "1", "true", "yes", "y", "h", "l", "high", "low"}:
        if flag in {"h", "high"}:
            return "High"
        if flag in {"l", "low"}:
            return "Low"
        return "Abnormal"

    result = coerce_number(row.get("result") or row.get("Result"))
    if result is None:
        return "Unclassified"
    low = coerce_number(row.get("param_low") or row.get("ParamLow") or row.get("low_value"))
    high = coerce_number(row.get("param_high") or row.get("ParamHigh") or row.get("high_value"))
    if low is not None and result < low:
        return "Low"
    if high is not None and result > high:
        return "High"
    if low is not None or high is not None:
        return "Normal"
    return "Unclassified"


def auth_status(value: Any) -> str:
    text = safe_text(value).lower()
    if text in {"1", "true", "yes", "y", "authorized", "authorised", "a"}:
        return "Authorized"
    return "Pending Authorization"


def filename_timestamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def clean_filename_part(value: Any) -> str:
    text = re.sub(r"[^A-Za-z0-9_-]+", "_", safe_text(value, 80))
    return text.strip("_") or "Report"
