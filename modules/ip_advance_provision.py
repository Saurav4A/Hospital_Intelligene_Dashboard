from __future__ import annotations

import io
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd


def _clean_df_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is not None and not df.empty:
        df.columns = [str(c).strip() for c in df.columns]
    return df


def default_filters(local_tz: ZoneInfo) -> dict[str, str]:
    today_local = datetime.now(tz=local_tz).date()
    current_month_start = today_local.replace(day=1)
    previous_month_end = current_month_start - timedelta(days=1)
    previous_month_start = previous_month_end.replace(day=1)
    return {
        "basis": "discharge",
        "admission_from": previous_month_start.strftime("%Y-%m-%d"),
        "admission_to": previous_month_end.strftime("%Y-%m-%d"),
        "discharge_from": current_month_start.strftime("%Y-%m-%d"),
        "discharge_to": today_local.strftime("%Y-%m-%d"),
        "as_on_date": today_local.strftime("%Y-%m-%d"),
    }


def parse_date(raw_value, default_value: str | None = None) -> str | None:
    raw = str(raw_value or "").strip()
    if not raw:
        raw = str(default_value or "").strip()
    if not raw:
        return None
    try:
        return date.fromisoformat(raw).strftime("%Y-%m-%d")
    except Exception:
        return None


def normalize_filters(args, local_tz: ZoneInfo) -> tuple[dict | None, str | None]:
    defaults = default_filters(local_tz)
    basis_raw = str((args.get("basis") if args else "") or defaults["basis"]).strip().lower()
    basis = "as_on" if basis_raw in {"as_on", "as-on", "as on", "ason"} else "discharge"

    admission_from = parse_date(args.get("admission_from") if args else None, defaults["admission_from"])
    admission_to = parse_date(args.get("admission_to") if args else None, defaults["admission_to"])
    if not admission_from or not admission_to:
        return None, "Invalid admission date range"
    if admission_from > admission_to:
        return None, "Admission From cannot be after Admission To"

    filters = {
        "basis": basis,
        "basis_label": "As On Date" if basis == "as_on" else "Discharge Window",
        "admission_from": admission_from,
        "admission_to": admission_to,
        "discharge_from": None,
        "discharge_to": None,
        "as_on_date": None,
        "cutoff_label": "As On Date" if basis == "as_on" else "Provision Cutoff",
    }

    if basis == "discharge":
        discharge_from = parse_date(args.get("discharge_from") if args else None, defaults["discharge_from"])
        discharge_to = parse_date(args.get("discharge_to") if args else None, defaults["discharge_to"])
        if not discharge_from or not discharge_to:
            return None, "Invalid discharge date range"
        if discharge_from > discharge_to:
            return None, "Discharge From cannot be after Discharge To"
        filters["discharge_from"] = discharge_from
        filters["discharge_to"] = discharge_to
        # Monthly advance provisioning is measured as of the admission window close,
        # while the discharge window only defines the crossover cohort.
        filters["cutoff_date"] = admission_to
    else:
        as_on_date = parse_date(args.get("as_on_date") if args else None, defaults["as_on_date"])
        if not as_on_date:
            return None, "Invalid As On Date"
        filters["as_on_date"] = as_on_date
        filters["cutoff_date"] = as_on_date

    return filters, None


def build_summary(df: pd.DataFrame, basis: str, cutoff_date: str) -> dict:
    summary = {
        "basis": basis,
        "cutoff_date": cutoff_date,
        "visit_count": 0,
        "receipt_count": 0,
        "total_advance_amount": 0.0,
        "average_advance_per_visit": 0.0,
        "open_visits": 0,
        "discharged_visits": 0,
        "billed_after_cutoff_visits": 0,
        "still_unbilled_visits": 0,
        "discharged_in_window_visits": 0,
        "discharged_after_window_visits": 0,
        "pending_discharge_visits": 0,
    }
    if df is None or df.empty:
        return summary

    work_df = _clean_df_columns(df.copy())
    if "AdvanceAmount" in work_df.columns:
        work_df["AdvanceAmount"] = pd.to_numeric(work_df["AdvanceAmount"], errors="coerce").fillna(0.0)
    if "ReceiptCount" in work_df.columns:
        work_df["ReceiptCount"] = pd.to_numeric(work_df["ReceiptCount"], errors="coerce").fillna(0).astype(int)

    cutoff_dt = None
    try:
        cutoff_dt = date.fromisoformat(str(cutoff_date or ""))
    except Exception:
        cutoff_dt = None

    if "DischargeDate" in work_df.columns:
        discharge_series = pd.to_datetime(work_df["DischargeDate"], errors="coerce")
    else:
        discharge_series = pd.Series(dtype="datetime64[ns]")

    if cutoff_dt is not None and not discharge_series.empty:
        discharged_visits = int(((discharge_series.notna()) & (discharge_series.dt.date <= cutoff_dt)).sum())
    else:
        discharged_visits = int(discharge_series.notna().sum()) if not discharge_series.empty else 0

    visit_count = int(len(work_df))
    total_advance = float(work_df.get("AdvanceAmount", pd.Series(dtype=float)).sum()) if "AdvanceAmount" in work_df.columns else 0.0
    receipt_count = int(work_df.get("ReceiptCount", pd.Series(dtype=int)).sum()) if "ReceiptCount" in work_df.columns else 0
    bill_status_series = work_df.get("BillStatusAsOnCutoff", pd.Series(dtype=str)).astype(str).str.strip()
    discharge_bucket_series = work_df.get("DischargeBucket", pd.Series(dtype=str)).astype(str).str.strip()

    summary.update({
        "visit_count": visit_count,
        "receipt_count": receipt_count,
        "total_advance_amount": round(total_advance, 2),
        "average_advance_per_visit": round((total_advance / visit_count), 2) if visit_count else 0.0,
        "open_visits": max(0, visit_count - discharged_visits),
        "discharged_visits": discharged_visits,
        "billed_after_cutoff_visits": int((bill_status_series == "Billed After Cutoff").sum()) if not bill_status_series.empty else 0,
        "still_unbilled_visits": int((bill_status_series == "Unbilled").sum()) if not bill_status_series.empty else 0,
        "discharged_in_window_visits": int((discharge_bucket_series == "Discharged In Window").sum()) if not discharge_bucket_series.empty else 0,
        "discharged_after_window_visits": int((discharge_bucket_series == "Discharged After Window").sum()) if not discharge_bucket_series.empty else 0,
        "pending_discharge_visits": int((discharge_bucket_series == "Still Admitted / Not Yet Discharged").sum()) if not discharge_bucket_series.empty else 0,
    })
    return summary


def build_excel_buffer(
    target_unit: str,
    filters: dict,
    df: pd.DataFrame,
    summary: dict,
    exported_by: str,
    local_tz: ZoneInfo,
):
    output = io.BytesIO()
    exported_at = datetime.now(tz=local_tz).strftime("%Y-%m-%d %H:%M:%S")
    basis_label = filters.get("basis_label") or ("As On Date" if filters.get("basis") == "as_on" else "Discharge Window")
    cutoff_label = filters.get("cutoff_label") or "Cutoff"
    cutoff_date = filters.get("cutoff_date") or ""
    details_df = _clean_df_columns(df.copy()) if df is not None else pd.DataFrame()

    if details_df.empty:
        return None

    preferred_cols = [
        "Visit_ID",
        "VisitNo",
        "AdmissionNo",
        "AdmissionDate",
        "DischargeDate",
        "VisitStatusAsOnCutoff",
        "DischargeBucket",
        "PatientName",
        "RegNo",
        "DoctorInCharge",
        "WardName",
        "PatientType",
        "PatientSubType",
        "PayCategory",
        "ReceiptCount",
        "FirstReceiptDate",
        "LastReceiptDate",
        "AdvanceAmount",
        "BillStatusAsOnCutoff",
        "FinalBillDate",
        "FinalBillNo",
        "Unit",
    ]
    details_df = details_df[[c for c in preferred_cols if c in details_df.columns] + [c for c in details_df.columns if c not in preferred_cols]]

    for col in ["AdmissionDate", "DischargeDate", "FirstReceiptDate", "LastReceiptDate", "FinalBillDate"]:
        if col in details_df.columns:
            details_df[col] = pd.to_datetime(details_df[col], errors="coerce")
    if "AdvanceAmount" in details_df.columns:
        details_df["AdvanceAmount"] = pd.to_numeric(details_df["AdvanceAmount"], errors="coerce").fillna(0.0)
    for col in ["ReceiptCount", "Visit_ID"]:
        if col in details_df.columns:
            details_df[col] = pd.to_numeric(details_df[col], errors="coerce")

    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        wb = writer.book
        title_fmt = wb.add_format({"bold": True, "font_size": 14, "font_color": "#1E3A8A", "align": "center", "valign": "vcenter"})
        subtitle_fmt = wb.add_format({"bold": True, "font_size": 10, "align": "center", "valign": "vcenter"})
        meta_fmt = wb.add_format({"font_size": 9, "align": "left", "valign": "vcenter"})
        header_fmt = wb.add_format({
            "bold": True, "bg_color": "#1E3A8A", "font_color": "white",
            "border": 1, "align": "center", "valign": "vcenter",
        })
        text_fmt = wb.add_format({"align": "left", "valign": "vcenter"})
        int_fmt = wb.add_format({"align": "right", "valign": "vcenter", "num_format": "#,##0"})
        money_fmt = wb.add_format({"align": "right", "valign": "vcenter", "num_format": "#,##,##0.00"})
        date_fmt = wb.add_format({"align": "center", "valign": "vcenter", "num_format": "yyyy-mm-dd"})

        ws_sum = wb.add_worksheet("Summary")
        writer.sheets["Summary"] = ws_sum
        ws_sum.merge_range(0, 0, 0, 3, "IP Advance Provision Report", title_fmt)
        ws_sum.merge_range(1, 0, 1, 3, f"Unit: {target_unit} | Basis: {basis_label} | {cutoff_label}: {cutoff_date}", subtitle_fmt)
        ws_sum.merge_range(2, 0, 2, 3, f"Admission Window: {filters.get('admission_from')} to {filters.get('admission_to')}", meta_fmt)
        if filters.get("basis") == "discharge":
            ws_sum.merge_range(3, 0, 3, 3, f"Discharge Window: {filters.get('discharge_from')} to {filters.get('discharge_to')}", meta_fmt)
        else:
            ws_sum.merge_range(3, 0, 3, 3, f"As On Date: {filters.get('as_on_date')}", meta_fmt)
        ws_sum.merge_range(4, 0, 4, 3, f"Exported By: {exported_by} | Exported At: {exported_at}", meta_fmt)

        metrics = [
            ("Visit Count", summary.get("visit_count", 0)),
            ("Receipt Count", summary.get("receipt_count", 0)),
            ("Total Advance Amount", summary.get("total_advance_amount", 0.0)),
            ("Average Advance / Visit", summary.get("average_advance_per_visit", 0.0)),
            ("Discharged In Window", summary.get("discharged_in_window_visits", 0)),
            ("Discharged After Window", summary.get("discharged_after_window_visits", 0)),
            ("Still Pending", summary.get("pending_discharge_visits", 0)),
            ("Billed After Cutoff", summary.get("billed_after_cutoff_visits", 0)),
            ("Still Unbilled", summary.get("still_unbilled_visits", 0)),
        ]
        ws_sum.write(6, 0, "Metric", header_fmt)
        ws_sum.write(6, 1, "Value", header_fmt)
        row_cursor = 7
        for label, value in metrics:
            ws_sum.write(row_cursor, 0, label, text_fmt)
            if "Amount" in label or "Average" in label:
                ws_sum.write(row_cursor, 1, float(value or 0), money_fmt)
            else:
                ws_sum.write(row_cursor, 1, int(value or 0), int_fmt)
            row_cursor += 1
        ws_sum.set_column(0, 0, 28)
        ws_sum.set_column(1, 1, 18)

        details_df.to_excel(writer, sheet_name="Details", index=False, startrow=6, header=False)
        ws = writer.sheets["Details"]
        last_col = len(details_df.columns) - 1
        ws.merge_range(0, 0, 0, last_col, "IP Advance Provision - Details", title_fmt)
        ws.merge_range(1, 0, 1, last_col, f"Unit: {target_unit} | Basis: {basis_label} | {cutoff_label}: {cutoff_date}", subtitle_fmt)
        ws.merge_range(2, 0, 2, last_col, f"Admission Window: {filters.get('admission_from')} to {filters.get('admission_to')}", meta_fmt)
        detail_line = (
            f"Discharge Window: {filters.get('discharge_from')} to {filters.get('discharge_to')}"
            if filters.get("basis") == "discharge"
            else f"As On Date: {filters.get('as_on_date')}"
        )
        ws.merge_range(3, 0, 3, last_col, detail_line, meta_fmt)
        ws.merge_range(4, 0, 4, last_col, f"Exported By: {exported_by} | Exported At: {exported_at}", meta_fmt)

        date_cols = {"admissiondate", "dischargedate", "firstreceiptdate", "lastreceiptdate", "finalbilldate"}
        money_cols = {"advanceamount"}
        int_cols = {"visit_id", "visitid", "receiptcount"}
        for col_idx, col_name in enumerate(details_df.columns):
            ws.write(5, col_idx, col_name, header_fmt)
            col_key = str(col_name).strip().lower().replace(" ", "").replace("_", "")
            fmt = text_fmt
            width = 16
            if col_key in date_cols:
                fmt = date_fmt
                width = 14
            elif col_key in money_cols:
                fmt = money_fmt
                width = 18
            elif col_key in int_cols:
                fmt = int_fmt
                width = 12
            elif any(token in col_key for token in ["patient", "doctor", "ward"]):
                width = 24
            elif any(token in col_key for token in ["billstatus", "visitstatus"]):
                width = 18
            ws.set_column(col_idx, col_idx, width, fmt)
        ws.freeze_panes(6, 0)

    output.seek(0)
    return output
