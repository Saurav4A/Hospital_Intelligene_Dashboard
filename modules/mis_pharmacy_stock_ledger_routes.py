from datetime import date, datetime
import io
from decimal import Decimal
from secrets import token_hex

import pandas as pd
from flask import jsonify, request, send_file, session

from modules import data_fetch


LEDGER_ROW_NUMERIC_FIELDS = {
    "qty",
    "qty_in",
    "qty_out",
    "moved_qty",
    "balance_before",
    "balance_after",
    "stock_before",
    "stock_after",
    "current_qty",
    "stock_now",
    "reconciliation_delta",
    "change_since",
    "rate",
    "mrp",
    "amount",
}

TRACE_NUMERIC_FIELD_HINTS = {
    "qty",
    "saleqty",
    "grnqty",
    "retqty",
    "issqty",
    "purqty",
    "srqty",
    "adjqty",
    "shortqty",
    "closingqty",
    "rate",
    "mrp",
    "salevalue",
    "purchasevalue",
    "issuedvalue",
    "saleretvalue",
    "issueretvalue",
    "purretvalue",
    "adjvalue",
    "gst",
    "openingqty",
    "runningbalqty",
    "amount",
}


def register_mis_pharmacy_stock_ledger_routes(
    app,
    *,
    login_required,
    allowed_units_for_session,
    clean_df_columns,
    safe_float,
    export_cache_get_bytes,
    export_cache_put_bytes,
    excel_job_get,
    excel_job_update,
    export_executor,
    local_tz,
):
    """Register Pharmacy MIS stock ledger routes."""
    _allowed_units_for_session = allowed_units_for_session
    _clean_df_columns = clean_df_columns
    _safe_float = safe_float
    _export_cache_get_bytes = export_cache_get_bytes
    _export_cache_put_bytes = export_cache_put_bytes
    _excel_job_get = excel_job_get
    _excel_job_update = excel_job_update
    EXPORT_EXECUTOR = export_executor
    LOCAL_TZ = local_tz

    def _today_str() -> str:
        return datetime.now(tz=LOCAL_TZ).strftime("%Y-%m-%d")

    def _resolve_unit(raw_unit: str):
        allowed_units = _allowed_units_for_session()
        unit = (raw_unit or "").strip().upper()
        if not allowed_units:
            return None, allowed_units, ("No unit access assigned", 403)
        if not unit:
            if len(allowed_units) == 1:
                return allowed_units[0], allowed_units, None
            return None, allowed_units, ("Please select a unit", 400)
        if unit not in allowed_units:
            return None, allowed_units, (f"Unit {unit} not permitted", 403)
        return unit, allowed_units, None

    def _coerce_int(raw_value, default: int = 0) -> int:
        try:
            return int(float(raw_value))
        except Exception:
            return int(default)

    def _is_truthy(raw_value) -> bool:
        return str(raw_value or "").strip().lower() in {"1", "true", "yes", "y", "on"}

    def _format_dt(value):
        if value is None:
            return None
        try:
            if pd.isna(value):
                return None
        except Exception:
            pass
        if isinstance(value, pd.Timestamp):
            return value.to_pydatetime().strftime("%Y-%m-%d %H:%M:%S")
        if isinstance(value, datetime):
            return value.strftime("%Y-%m-%d %H:%M:%S")
        if isinstance(value, date):
            return value.strftime("%Y-%m-%d")
        text = str(value).strip()
        return text or None

    def _json_scalar(value, field_name: str | None = None):
        if value is None:
            return None
        try:
            if pd.isna(value):
                return None
        except Exception:
            pass
        if isinstance(value, pd.Timestamp):
            return _format_dt(value)
        if isinstance(value, datetime):
            return value.strftime("%Y-%m-%d %H:%M:%S")
        if isinstance(value, date):
            return value.strftime("%Y-%m-%d")
        if isinstance(value, Decimal):
            return float(value)
        if isinstance(value, bool):
            return bool(value)
        if isinstance(value, int):
            return int(value)
        if isinstance(value, float):
            return float(value)
        key = str(field_name or "").strip().lower()
        if key in TRACE_NUMERIC_FIELD_HINTS:
            return _safe_float(value)
        return str(value).strip()

    def _comparison_label(to_date: str) -> str:
        return "Changed Since" if str(to_date or "").strip() == _today_str() else "Live Change Since"

    def _comparison_note(to_date: str) -> str:
        if str(to_date or "").strip() == _today_str():
            return "Shows how stock now compares with the stock immediately after each movement."
        return "Shows how live stock now compares with the stock after each movement in this historical view."

    def _audit_user_label(raw_value) -> str:
        text = str(raw_value or "").strip()
        low = text.lower()
        if not text or low in {"0", "0.0", "0.00", "nan", "none", "null"}:
            return "System"
        if low.startswith("1900-01-01") or low.startswith("2000-01-01"):
            return "System"
        if low.startswith("01 jan 1900") or low.startswith("01 jan 2000"):
            return "System"
        return text

    def _movement_verb(moved_qty: float) -> str:
        if moved_qty > 0:
            return "came in"
        if moved_qty < 0:
            return "went out"
        return "moved"

    def _coerce_df(value) -> pd.DataFrame:
        if isinstance(value, pd.DataFrame):
            return value
        if value is None:
            return pd.DataFrame()
        try:
            return pd.DataFrame(value)
        except Exception:
            return pd.DataFrame()

    def _ledger_row_payload(row: dict) -> dict:
        qty_in = _safe_float(row.get("QtyIn"))
        qty_out = _safe_float(row.get("QtyOut"))
        moved_qty = _safe_float(row.get("MovementQty"))
        if abs(moved_qty) <= 0.0005:
            moved_qty = qty_in if abs(qty_in) > 0.0005 else -qty_out
        balance_before = _safe_float(row.get("BalanceBefore"))
        balance_after = _safe_float(row.get("BalanceAfter"))
        current_qty = _safe_float(row.get("CurrentQty"))
        reconciliation_delta = _safe_float(row.get("ReconciliationDelta"))
        audit_user_raw = str(row.get("AuditUserID") or "").strip()
        return {
            "id": _coerce_int(row.get("ID"), 0),
            "doc_date": _format_dt(row.get("DocDate")),
            "doc_type": str(row.get("DocType") or "").strip().upper(),
            "doc_id": _coerce_int(row.get("DocId"), 0),
            "reference_no": str(row.get("ReferenceNo") or "").strip(),
            "store_id": _coerce_int(row.get("StoreID"), 0),
            "store_name": str(row.get("StoreName") or "").strip(),
            "counterparty_store_id": _coerce_int(row.get("CounterpartyStoreID"), 0),
            "counterparty_store_name": str(row.get("CounterpartyStoreName") or "").strip(),
            "item_id": _coerce_int(row.get("ItemID"), 0),
            "item_code": str(row.get("ItemCode") or "").strip(),
            "item_name": str(row.get("ItemName") or "").strip(),
            "batch_id": _coerce_int(row.get("BatchID"), 0),
            "batch_name": str(row.get("BatchName") or "").strip(),
            "expiry_date": _format_dt(row.get("ExpiryDate")),
            "qty": _safe_float(row.get("Qty")),
            "qty_in": qty_in,
            "qty_out": qty_out,
            "moved_qty": moved_qty,
            "balance_before": balance_before,
            "balance_after": balance_after,
            "stock_before": balance_before,
            "stock_after": balance_after,
            "current_qty": current_qty,
            "stock_now": current_qty,
            "reconciliation_delta": reconciliation_delta,
            "change_since": reconciliation_delta,
            "rate": _safe_float(row.get("Rate")),
            "mrp": _safe_float(row.get("MRP")),
            "amount": _safe_float(row.get("Amount")),
            "movement_family": str(row.get("MovementFamily") or "").strip().lower(),
            "movement_label": str(row.get("MovementLabel") or "").strip(),
            "movement_direction": "in" if moved_qty > 0 else ("out" if moved_qty < 0 else "neutral"),
            "audit_user_id": audit_user_raw,
            "audit_user_label": _audit_user_label(audit_user_raw),
            "audit_time": _format_dt(row.get("AuditTime")),
        }

    def _timeline_row_payload(row: dict) -> dict:
        moved_qty = _safe_float(row.get("MovementQty"))
        return {
            "id": _coerce_int(row.get("ID"), 0),
            "doc_date": _format_dt(row.get("DocDate")),
            "doc_type": str(row.get("DocType") or "").strip().upper(),
            "doc_id": _coerce_int(row.get("DocId"), 0),
            "reference_no": str(row.get("ReferenceNo") or "").strip(),
            "batch_id": _coerce_int(row.get("BatchID"), 0),
            "batch_name": str(row.get("BatchName") or "").strip(),
            "movement_family": str(row.get("MovementFamily") or "").strip().lower(),
            "movement_label": str(row.get("MovementLabel") or "").strip(),
            "moved_qty": moved_qty,
            "stock_before": _safe_float(row.get("StockBefore")),
            "stock_after": _safe_float(row.get("StockAfter")),
            "is_selected": bool(_coerce_int(row.get("IsSelected"), 0)),
        }

    def _batch_option_payload(row: dict) -> dict:
        return {
            "batch_id": _coerce_int(row.get("batch_id") or row.get("BatchID"), 0),
            "batch_name": str(row.get("batch_name") or row.get("BatchName") or "").strip(),
            "expiry_date": _format_dt(row.get("expiry_date") or row.get("ExpiryDate")),
            "item_id": _coerce_int(row.get("item_id") or row.get("ItemID"), 0),
            "item_name": str(row.get("item_name") or row.get("ItemName") or "").strip(),
            "row_count": _coerce_int(row.get("row_count") or row.get("RowCount"), 0),
        }

    def _trace_story_payload(row_payload: dict, current_payload: dict, raw_trace: dict) -> dict:
        trace = dict(row_payload or {})
        raw_stock_now = (current_payload or {}).get("stock_now")
        if raw_stock_now in {None, ""}:
            stock_now = _safe_float(trace.get("stock_now"))
        else:
            stock_now = _safe_float(raw_stock_now)
        stock_before = _safe_float(trace.get("stock_before"))
        stock_after = _safe_float(trace.get("stock_after"))
        moved_qty = _safe_float(trace.get("moved_qty"))
        change_since = _safe_float(stock_now - stock_after)
        machine_name = (
            str(raw_trace.get("UpdatedMacName") or "").strip()
            or str(raw_trace.get("InsertedMacName") or "").strip()
        )
        ip_address = (
            str(raw_trace.get("UpdatedIPAddress") or "").strip()
            or str(raw_trace.get("InsertedIPAddress") or "").strip()
        )
        trace.update({
            "stock_now": stock_now,
            "current_qty": stock_now,
            "change_since": change_since,
            "reconciliation_delta": change_since,
            "machine_name": machine_name,
            "ip_address": ip_address,
            "store_name": str(trace.get("store_name") or (current_payload or {}).get("store_name") or "").strip(),
            "item_code": str(trace.get("item_code") or (current_payload or {}).get("item_code") or "").strip(),
            "audit_user_label": _audit_user_label(trace.get("audit_user_id")),
        })
        movement_label = str(trace.get("movement_label") or trace.get("doc_type") or "Movement").strip()
        when_text = _format_dt(trace.get("doc_date")) or "this time"
        try:
            parsed = pd.to_datetime(trace.get("doc_date"))
            if not pd.isna(parsed):
                when_text = parsed.to_pydatetime().strftime("%d %b %Y, %I:%M %p")
        except Exception:
            pass
        batch_name = str(trace.get("batch_name") or "this batch").strip()
        counterparty_store_name = str(trace.get("counterparty_store_name") or "").strip()
        movement_clause = movement_label
        if str(trace.get("doc_type") or "").strip().upper() == "ISD" and counterparty_store_name:
            movement_clause = f"{movement_label} to {counterparty_store_name}"
        story = (
            f"On {when_text}, {abs(moved_qty):,.3f} units {_movement_verb(moved_qty)} through {movement_clause}. "
            f"Stock moved from {stock_before:,.3f} to {stock_after:,.3f}. "
            f"The store now holds {stock_now:,.3f} units for this item."
        )
        if batch_name:
            story = f"{story} Selected batch: {batch_name}."
        trace["story_text"] = story
        if abs(change_since) <= 0.0005:
            trace["change_note"] = "Stock now matches the stock after this movement."
        elif change_since > 0:
            trace["change_note"] = f"Stock is higher now by {change_since:,.3f} units."
        else:
            trace["change_note"] = f"Stock is lower now by {abs(change_since):,.3f} units."
        return trace

    def _generic_record_payload(row: dict) -> dict:
        out = {}
        for key, value in (row or {}).items():
            clean_key = str(key or "").strip()
            if not clean_key:
                continue
            out[clean_key] = _json_scalar(value, clean_key)
        return out

    def _fetch_ledger_page_result(
        unit: str,
        *,
        store_id: int,
        from_date: str,
        to_date: str,
        page: int,
        page_size: int,
        search_text: str,
        batch_id: int,
        movement_family: str,
        doc_type: str,
        mismatch_only: bool,
    ):
        return data_fetch.fetch_pharmacy_stock_ledger_page(
            unit,
            store_id,
            from_date,
            to_date,
            page=page,
            page_size=page_size,
            search_text=search_text,
            batch_id=batch_id,
            movement_family=movement_family,
            doc_type=doc_type,
            mismatch_only=mismatch_only,
        )

    def _fetch_ledger_all_rows(
        unit: str,
        *,
        store_id: int,
        from_date: str,
        to_date: str,
        search_text: str,
        batch_id: int,
        movement_family: str,
        doc_type: str,
        mismatch_only: bool,
    ):
        page = 1
        page_size = 10000
        frames = []
        meta = None
        doc_types = []
        batch_options = []
        while True:
            result = _fetch_ledger_page_result(
                unit,
                store_id=store_id,
                from_date=from_date,
                to_date=to_date,
                page=page,
                page_size=page_size,
                search_text=search_text,
                batch_id=batch_id,
                movement_family=movement_family,
                doc_type=doc_type,
                mismatch_only=mismatch_only,
            )
            if result is None:
                return None
            page_df = _clean_df_columns(_coerce_df(result.get("df"))).copy()
            if meta is None:
                meta = dict(result.get("meta") or {})
                doc_types = list(result.get("doc_types") or [])
                batch_options = list(result.get("batch_options") or [])
            if page_df is not None and not page_df.empty:
                frames.append(page_df)
            total_pages = max(1, _coerce_int((result.get("meta") or {}).get("total_pages"), 1))
            if page >= total_pages:
                break
            page += 1
        full_df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        return {"df": full_df, "meta": meta or {}, "doc_types": doc_types, "batch_options": batch_options}

    def _build_stock_ledger_excel(
        unit: str,
        *,
        store_id: int,
        from_date: str,
        to_date: str,
        search_text: str,
        batch_id: int,
        movement_family: str,
        doc_type: str,
        mismatch_only: bool,
        exported_by: str,
    ):
        result = _fetch_ledger_all_rows(
            unit,
            store_id=store_id,
            from_date=from_date,
            to_date=to_date,
            search_text=search_text,
            batch_id=batch_id,
            movement_family=movement_family,
            doc_type=doc_type,
            mismatch_only=mismatch_only,
        )
        if result is None:
            return None, None, "Failed to fetch stock ledger data"

        df = _clean_df_columns(_coerce_df(result.get("df"))).copy()
        if df is None or df.empty:
            return None, None, "No stock ledger data available to export"

        meta = dict(result.get("meta") or {})
        doc_types = list(result.get("doc_types") or [])
        batch_options = list(result.get("batch_options") or [])
        comparison_label = _comparison_label(to_date)
        comparison_note = _comparison_note(to_date)
        exported_at = datetime.now(tz=LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S")
        selected_batch = next((entry for entry in batch_options if _coerce_int(entry.get("batch_id"), 0) == _coerce_int(batch_id, 0)), None)

        summary_rows = [
            {"Metric": "Unit", "Value": unit},
            {"Metric": "Store ID", "Value": store_id},
            {"Metric": "Store Name", "Value": str(meta.get("store_name") or "").strip()},
            {"Metric": "From Date", "Value": from_date},
            {"Metric": "To Date", "Value": to_date},
            {"Metric": "Search", "Value": search_text or "All"},
            {"Metric": "Batch Filter", "Value": (selected_batch or {}).get("batch_name") or (batch_id or "All")},
            {"Metric": "Movement Family", "Value": movement_family or "All"},
            {"Metric": "Doc Type", "Value": doc_type or "All"},
            {"Metric": "Mismatch Only", "Value": "Yes" if mismatch_only else "No"},
            {"Metric": "Comparison Label", "Value": comparison_label},
            {"Metric": "Comparison Note", "Value": comparison_note},
            {"Metric": "Opening Qty", "Value": _safe_float(meta.get("opening_qty"))},
            {"Metric": "Inward Qty", "Value": _safe_float(meta.get("inward_qty"))},
            {"Metric": "Outward Qty", "Value": _safe_float(meta.get("outward_qty"))},
            {"Metric": "Closing Qty", "Value": _safe_float(meta.get("closing_qty"))},
            {"Metric": "Live Current Qty", "Value": _safe_float(meta.get("live_current_qty"))},
            {"Metric": "Delta Row Count", "Value": _coerce_int(meta.get("delta_row_count"), 0)},
            {"Metric": "Mismatch Count", "Value": _coerce_int(meta.get("mismatch_count"), 0)},
            {"Metric": "Filtered Rows", "Value": _coerce_int(meta.get("total_rows"), len(df.index))},
            {"Metric": "Exported By", "Value": exported_by},
            {"Metric": "Exported At", "Value": exported_at},
        ]
        summary_df = pd.DataFrame(summary_rows)
        doc_type_df = pd.DataFrame(doc_types or [])
        if not doc_type_df.empty:
            doc_type_df = doc_type_df.rename(
                columns={
                    "doc_type": "Doc Type",
                    "movement_family": "Movement Family",
                    "movement_label": "Movement Label",
                    "row_count": "Rows",
                }
            )

        details_df = pd.DataFrame([_ledger_row_payload(row) for row in df.to_dict(orient="records")])
        details_df = details_df[
            [
                "doc_date",
                "movement_label",
                "doc_type",
                "reference_no",
                "item_name",
                "item_id",
                "item_code",
                "batch_name",
                "batch_id",
                "expiry_date",
                "stock_before",
                "moved_qty",
                "stock_after",
                "stock_now",
                "change_since",
                "qty_in",
                "qty_out",
                "rate",
                "mrp",
                "amount",
                "audit_user_label",
                "audit_time",
            ]
        ].rename(
            columns={
                "doc_date": "Doc Date",
                "movement_label": "Movement",
                "doc_type": "Doc Type",
                "reference_no": "Reference No",
                "item_name": "Item Name",
                "item_id": "Item ID",
                "item_code": "Item Code",
                "batch_name": "Batch",
                "batch_id": "Batch ID",
                "expiry_date": "Expiry Date",
                "stock_before": "Stock Before",
                "moved_qty": "Moved Qty",
                "stock_after": "Stock After",
                "stock_now": "Stock Now",
                "change_since": comparison_label,
                "qty_in": "Qty In",
                "qty_out": "Qty Out",
                "rate": "Rate",
                "mrp": "MRP",
                "amount": "Amount",
                "audit_user_label": "Recorded By",
                "audit_time": "Audit Time",
            }
        )

        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            workbook = writer.book
            header_fmt = workbook.add_format({
                "bold": True,
                "font_size": 10,
                "align": "center",
                "valign": "vcenter",
                "bg_color": "#1e3a8a",
                "font_color": "white",
                "border": 1,
            })
            title_fmt = workbook.add_format({
                "bold": True,
                "font_size": 14,
                "align": "center",
                "valign": "vcenter",
                "bg_color": "#1e3a8a",
                "font_color": "white",
            })
            meta_fmt = workbook.add_format({
                "font_size": 9,
                "align": "center",
                "valign": "vcenter",
                "font_color": "#475569",
            })
            text_col_fmt = workbook.add_format({"font_size": 9, "align": "left"})
            num_col_fmt = workbook.add_format({"font_size": 9, "align": "right", "num_format": "#,##,##0.000"})
            money_col_fmt = workbook.add_format({"font_size": 9, "align": "right", "num_format": "#,##,##0.00"})

            summary_df.to_excel(writer, sheet_name="Summary", index=False, startrow=4)
            summary_ws = writer.sheets["Summary"]
            summary_ws.merge_range(0, 0, 0, 3, "Pharmacy Stock Ledger", title_fmt)
            summary_ws.merge_range(
                1,
                0,
                1,
                3,
                f"Unit: {unit} | Store: {meta.get('store_name') or store_id} | Range: {from_date} to {to_date}",
                meta_fmt,
            )
            summary_ws.merge_range(2, 0, 2, 3, f"Exported By: {exported_by} | Exported At: {exported_at}", meta_fmt)
            for idx, col_name in enumerate(summary_df.columns):
                summary_ws.write(4, idx, col_name, header_fmt)
            summary_ws.set_column(0, 0, 24, text_col_fmt)
            summary_ws.set_column(1, 1, 42, text_col_fmt)

            row_ptr = 6 + len(summary_df)
            if not doc_type_df.empty:
                doc_type_df.to_excel(writer, sheet_name="Summary", index=False, startrow=row_ptr)
                for idx, col_name in enumerate(doc_type_df.columns):
                    summary_ws.write(row_ptr, idx, col_name, header_fmt)
                summary_ws.set_column(0, 3, 22, text_col_fmt)

            details_df.to_excel(writer, sheet_name="Details", index=False, startrow=4)
            details_ws = writer.sheets["Details"]
            details_last_col = max(0, len(details_df.columns) - 1)
            details_ws.merge_range(0, 0, 0, details_last_col, "Pharmacy Stock Ledger - Detail Rows", title_fmt)
            details_ws.merge_range(1, 0, 1, details_last_col, f"Comparison: {comparison_label}", meta_fmt)
            details_ws.merge_range(2, 0, 2, details_last_col, comparison_note, meta_fmt)
            for idx, col_name in enumerate(details_df.columns):
                details_ws.write(4, idx, col_name, header_fmt)
                low = str(col_name or "").strip().lower()
                if low in {"qty in", "qty out", "stock before", "moved qty", "stock after", "stock now", comparison_label.lower()}:
                    details_ws.set_column(idx, idx, 14, num_col_fmt)
                elif low in {"rate", "mrp", "amount"}:
                    details_ws.set_column(idx, idx, 14, money_col_fmt)
                elif low in {"item name", "batch", "movement", "reference no"}:
                    details_ws.set_column(idx, idx, 24, text_col_fmt)
                else:
                    details_ws.set_column(idx, idx, 18, text_col_fmt)
            details_ws.freeze_panes(5, 0)

        output.seek(0)
        filename = f"Pharmacy_Stock_Ledger_{unit}_Store{store_id}_{from_date}_to_{to_date}.xlsx"
        return output.getvalue(), filename, None

    def _run_stock_ledger_export_job(
        job_id: str,
        unit: str,
        store_id: int,
        from_date: str,
        to_date: str,
        search_text: str,
        batch_id: int,
        movement_family: str,
        doc_type: str,
        mismatch_only: bool,
        exported_by: str,
    ):
        _excel_job_update(job_id, state="running")
        try:
            data, filename, err = _build_stock_ledger_excel(
                unit,
                store_id=store_id,
                from_date=from_date,
                to_date=to_date,
                search_text=search_text,
                batch_id=batch_id,
                movement_family=movement_family,
                doc_type=doc_type,
                mismatch_only=mismatch_only,
                exported_by=exported_by,
            )
            if not data:
                _excel_job_update(job_id, state="error", error=err or "No data available to export")
                return
            _export_cache_put_bytes("pharmacy_stock_ledger_xlsx_job", data, job_id)
            _excel_job_update(job_id, state="done", filename=filename)
        except Exception as exc:
            _excel_job_update(job_id, state="error", error=str(exc))

    @app.route('/api/mis/pharmacy/stock_ledger')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_mis_pharmacy_stock_ledger():
        unit, _, unit_error = _resolve_unit(request.args.get("unit"))
        if unit_error:
            message, code = unit_error
            return jsonify({"status": "error", "message": message}), code

        store_id = _coerce_int(request.args.get("store_id"), 0)
        from_date = (request.args.get("from") or "").strip()
        to_date = (request.args.get("to") or "").strip()
        page = _coerce_int(request.args.get("page"), 1)
        page_size = min(250, max(1, _coerce_int(request.args.get("page_size"), 100)))
        search_text = (request.args.get("search") or request.args.get("search_text") or "").strip()
        batch_id = _coerce_int(request.args.get("batch_id"), 0)
        movement_family = (request.args.get("movement_family") or "").strip().lower()
        doc_type = (request.args.get("doc_type") or "").strip().upper()
        mismatch_only = _is_truthy(request.args.get("mismatch_only"))

        if store_id <= 0:
            return jsonify({"status": "error", "message": "Store is required"}), 400
        if not from_date or not to_date:
            return jsonify({"status": "error", "message": "Valid from/to dates are required"}), 400

        result = _fetch_ledger_page_result(
            unit,
            store_id=store_id,
            from_date=from_date,
            to_date=to_date,
            page=page,
            page_size=page_size,
            search_text=search_text,
            batch_id=batch_id,
            movement_family=movement_family,
            doc_type=doc_type,
            mismatch_only=mismatch_only,
        )
        if result is None:
            return jsonify({"status": "error", "message": "Failed to load stock ledger"}), 500

        df = _clean_df_columns(_coerce_df(result.get("df"))).copy()
        rows = [_ledger_row_payload(row) for row in df.to_dict(orient="records")] if not df.empty else []
        meta = dict(result.get("meta") or {})
        store_name = str(meta.get("store_name") or "").strip()
        comparison_label = _comparison_label(to_date)

        return jsonify({
            "status": "success",
            "unit": unit,
            "store_id": store_id,
            "store_name": store_name,
            "from_date": from_date,
            "to_date": to_date,
            "page": _coerce_int(meta.get("page"), page),
            "page_size": _coerce_int(meta.get("page_size"), page_size),
            "total_rows": _coerce_int(meta.get("total_rows"), len(rows)),
            "total_pages": _coerce_int(meta.get("total_pages"), 1),
            "selected_batch_id": batch_id,
            "comparison_label": comparison_label,
            "comparison_note": _comparison_note(to_date),
            "summary": {
                "opening_qty": _safe_float(meta.get("opening_qty")),
                "inward_qty": _safe_float(meta.get("inward_qty")),
                "outward_qty": _safe_float(meta.get("outward_qty")),
                "closing_qty": _safe_float(meta.get("closing_qty")),
                "live_current_qty": _safe_float(meta.get("live_current_qty")),
                "delta_row_count": _coerce_int(meta.get("delta_row_count"), 0),
                "mismatch_count": _coerce_int(meta.get("mismatch_count"), 0),
            },
            "doc_types": list(result.get("doc_types") or []),
            "batch_options": [_batch_option_payload(row) for row in list(result.get("batch_options") or [])],
            "rows": rows,
        })

    @app.route('/api/mis/pharmacy/stock_ledger/trace')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_mis_pharmacy_stock_ledger_trace():
        unit, _, unit_error = _resolve_unit(request.args.get("unit"))
        if unit_error:
            message, code = unit_error
            return jsonify({"status": "error", "message": message}), code

        store_id = _coerce_int(request.args.get("store_id"), 0)
        item_id = _coerce_int(request.args.get("item_id"), 0)
        batch_id = _coerce_int(request.args.get("batch_id"), 0)
        doc_id = _coerce_int(request.args.get("doc_id"), 0)
        ledger_id = _coerce_int(request.args.get("ledger_id"), 0)
        doc_type = (request.args.get("doc_type") or "").strip().upper()
        doc_date = (request.args.get("doc_date") or "").strip()
        qty = request.args.get("qty")

        if store_id <= 0 or item_id <= 0 or (ledger_id <= 0 and (doc_id <= 0 or not doc_type)):
            return jsonify({"status": "error", "message": "Missing trace identifiers"}), 400

        result = data_fetch.fetch_pharmacy_stock_ledger_trace(
            unit,
            store_id,
            item_id,
            batch_id,
            doc_date,
            doc_id,
            doc_type,
            qty,
            ledger_id=ledger_id,
        )
        if result is None:
            return jsonify({"status": "error", "message": "Failed to load ledger trace"}), 500

        ledger_df = _clean_df_columns(_coerce_df(result.get("ledger_row"))).copy()
        current_df = _clean_df_columns(_coerce_df(result.get("current_stock"))).copy()
        timeline_df = _clean_df_columns(_coerce_df(result.get("timeline_rows"))).copy()
        fallback_df = _clean_df_columns(_coerce_df(result.get("fallback_rows"))).copy()

        ledger_row = _generic_record_payload(ledger_df.iloc[0].to_dict()) if not ledger_df.empty else {}
        current_row = _generic_record_payload(current_df.iloc[0].to_dict()) if not current_df.empty else {}
        row_payload = _ledger_row_payload(ledger_df.iloc[0].to_dict()) if not ledger_df.empty else {}
        current_payload = {
            "store_name": str(current_df.iloc[0].get("StoreName") or "").strip(),
            "item_code": str(current_df.iloc[0].get("ItemCode") or "").strip(),
            "stock_now": _safe_float(current_df.iloc[0].get("CurrentQty")) if not current_df.empty else _safe_float(row_payload.get("stock_now")),
            "opening_qty": _safe_float(current_df.iloc[0].get("OpeningQty")) if not current_df.empty else 0.0,
            "selected_batch_qty": _safe_float(current_df.iloc[0].get("SelectedBatchQty")) if not current_df.empty else 0.0,
            "updated_on": _format_dt(current_df.iloc[0].get("UpdatedON")) if not current_df.empty else None,
        }
        trace_payload = _trace_story_payload(row_payload, current_payload, ledger_row)
        timeline_rows = [_timeline_row_payload(row) for row in timeline_df.to_dict(orient="records")] if not timeline_df.empty else []
        fallback_rows = [_generic_record_payload(row) for row in fallback_df.to_dict(orient="records")] if not fallback_df.empty else []

        return jsonify({
            "status": "success",
            "trace": trace_payload,
            "current_snapshot": current_payload,
            "timeline_rows": timeline_rows,
            "ledger_row": ledger_row,
            "current_stock": current_row,
            "fallback_rows": fallback_rows,
        })

    @app.route('/api/mis/pharmacy/stock_ledger/export_excel_job', methods=["POST"])
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_mis_pharmacy_stock_ledger_export_excel_job():
        payload = request.get_json(silent=True) or {}
        unit, _, unit_error = _resolve_unit(payload.get("unit") or request.args.get("unit"))
        if unit_error:
            message, code = unit_error
            return jsonify({"status": "error", "message": message}), code

        store_id = _coerce_int(payload.get("store_id") or request.args.get("store_id"), 0)
        from_date = (payload.get("from") or payload.get("from_date") or request.args.get("from") or "").strip()
        to_date = (payload.get("to") or payload.get("to_date") or request.args.get("to") or "").strip()
        search_text = (payload.get("search") or payload.get("search_text") or request.args.get("search") or "").strip()
        batch_id = _coerce_int(payload.get("batch_id") or request.args.get("batch_id"), 0)
        movement_family = (payload.get("movement_family") or request.args.get("movement_family") or "").strip().lower()
        doc_type = (payload.get("doc_type") or request.args.get("doc_type") or "").strip().upper()
        mismatch_only = _is_truthy(payload.get("mismatch_only") or request.args.get("mismatch_only"))

        if store_id <= 0:
            return jsonify({"status": "error", "message": "Store is required"}), 400
        if not from_date or not to_date:
            return jsonify({"status": "error", "message": "Valid from/to dates are required"}), 400

        exported_by = session.get("username") or session.get("user") or "Unknown"
        job_id = token_hex(16)
        _excel_job_update(job_id, state="queued", filename=None)
        EXPORT_EXECUTOR.submit(
            _run_stock_ledger_export_job,
            job_id,
            unit,
            store_id,
            from_date,
            to_date,
            search_text,
            batch_id,
            movement_family,
            doc_type,
            mismatch_only,
            exported_by,
        )
        return jsonify({"status": "queued", "job_id": job_id})

    @app.route('/api/mis/pharmacy/stock_ledger/export_excel_job_status')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_mis_pharmacy_stock_ledger_export_excel_job_status():
        job_id = (request.args.get("job_id") or "").strip()
        if not job_id:
            return jsonify({"status": "error", "message": "Missing job id"}), 400
        entry = _excel_job_get(job_id)
        if not entry:
            return jsonify({"status": "error", "message": "Job not found"}), 404
        return jsonify({
            "status": "success",
            "state": entry.get("state"),
            "error": entry.get("error"),
            "filename": entry.get("filename"),
        })

    @app.route('/api/mis/pharmacy/stock_ledger/export_excel_job_result')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_mis_pharmacy_stock_ledger_export_excel_job_result():
        job_id = (request.args.get("job_id") or "").strip()
        if not job_id:
            return "Missing job id", 400
        entry = _excel_job_get(job_id)
        if not entry:
            return "Job not found", 404
        if entry.get("state") != "done":
            return "Job not ready", 409
        data = _export_cache_get_bytes("pharmacy_stock_ledger_xlsx_job", job_id)
        if not data:
            return "Export expired", 404
        filename = entry.get("filename") or "Pharmacy_Stock_Ledger.xlsx"
        return send_file(
            io.BytesIO(data),
            as_attachment=True,
            download_name=filename,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
