from flask import jsonify, render_template, request, send_file, session
from datetime import date, datetime

from modules import data_fetch


def register_purchase_grn_routes(
    app,
    *,
    login_required,
    allowed_purchase_units_for_session,
    clean_df_columns,
    sanitize_json_payload,
    safe_float,
    safe_int,
    audit_log_event,
    local_tz,
):
    """Register Sharpsight-only GRN routes."""

    _allowed_purchase_units_for_session = allowed_purchase_units_for_session
    _clean_df_columns = clean_df_columns
    _sanitize_json_payload = sanitize_json_payload
    _safe_float = safe_float
    _safe_int = safe_int
    _audit_log_event = audit_log_event
    LOCAL_TZ = local_tz

    SHARPSIGHT_UNIT = "SHARPSIGHT"
    DIRECT_PURCHASE_TYPE_ID = 3
    AGAINST_PO_TYPE_ID = 1
    ALLOWED_TYPE_IDS = {DIRECT_PURCHASE_TYPE_ID, AGAINST_PO_TYPE_ID}

    def _session_role() -> str:
        return str(session.get("role") or "").strip()

    def _role_base(role_name: str | None = None) -> str:
        return str(role_name or _session_role()).split(":", 1)[0].strip()

    def _is_departmental_head(role_name: str | None = None) -> bool:
        return _role_base(role_name).lower().startswith("departmental head")

    def _can_backdate_grn() -> bool:
        return _role_base() == "IT"

    def _can_authorize_grn() -> bool:
        role = _session_role()
        return _role_base(role) == "IT" or _is_departmental_head(role)

    def _grn_unit():
        allowed_units = [str(unit or "").strip().upper() for unit in (_allowed_purchase_units_for_session() or [])]
        if SHARPSIGHT_UNIT not in allowed_units:
            return None, (
                jsonify(
                    {
                        "status": "error",
                        "message": "Sharpsight GRN is not available for your current purchase access.",
                    }
                ),
                403,
            )
        return SHARPSIGHT_UNIT, None

    def _col_map(df):
        return {str(col).strip().lower(): col for col in (df.columns if df is not None else [])}

    def _cell(row, cols: dict, *names, default=None):
        for name in names:
            col = cols.get(str(name or "").strip().lower())
            if col is not None:
                return row.get(col)
        return default

    def _boolish(value) -> bool:
        text = str("" if value is None else value).strip().lower()
        if text in {"", "0", "false", "no", "n", "nan", "none", "nat"}:
            return False
        if text in {"1", "true", "yes", "y", "-1"}:
            return True
        try:
            return bool(int(float(text)))
        except Exception:
            return bool(value)

    def _iso_date(value) -> str:
        text = str("" if value is None else value).strip()
        if not text or text.lower() in {"nan", "nat", "none"}:
            return ""
        if isinstance(value, datetime):
            return value.date().isoformat()
        if isinstance(value, date):
            return value.isoformat()
        try:
            return datetime.fromisoformat(text.replace("Z", "")).date().isoformat()
        except Exception:
            pass
        for fmt in (
            "%Y-%m-%d",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M:%S.%f",
            "%d-%m-%Y",
            "%d/%m/%Y",
            "%d-%b-%Y",
            "%d-%b-%Y %H:%M:%S",
        ):
            try:
                return datetime.strptime(text, fmt).date().isoformat()
            except Exception:
                continue
        return text[:10]

    def _clean_text(value) -> str:
        text = str("" if value is None else value).strip()
        return "" if text.lower() in {"nan", "nat", "none"} else text

    def _blank_if_sentinel(value) -> str:
        iso = _iso_date(value)
        return "" if iso in {"1900-01-01", "1900-01-02"} else iso

    def _round_money(value, places: int = 2) -> float:
        try:
            return round(float(value or 0), places)
        except Exception:
            return 0.0

    def _split_display_gst(cgst_pct: float, sgst_pct: float, cgst_amt: float, sgst_amt: float):
        if (cgst_pct > 0 and sgst_pct <= 0) or (sgst_pct > 0 and cgst_pct <= 0):
            total_pct = cgst_pct + sgst_pct
            cgst_pct = _round_money(total_pct / 2.0, 4)
            sgst_pct = _round_money(total_pct - cgst_pct, 4)
        if (cgst_amt > 0 and sgst_amt <= 0) or (sgst_amt > 0 and cgst_amt <= 0):
            total_amt = cgst_amt + sgst_amt
            cgst_amt = _round_money(total_amt / 2.0)
            sgst_amt = _round_money(total_amt - cgst_amt)
        return cgst_pct, sgst_pct, cgst_amt, sgst_amt

    def _build_row(values: dict) -> dict:
        qty = max(0.0, _safe_float(values.get("qty"), 0.0))
        free_qty = max(0.0, _safe_float(values.get("free_qty"), 0.0))
        rate = max(0.0, _safe_float(values.get("rate"), 0.0))
        mrp = max(0.0, _safe_float(values.get("mrp"), 0.0))
        discount_pct = max(0.0, _safe_float(values.get("discount_pct"), 0.0))
        gst_pct = max(0.0, _safe_float(values.get("gst_pct"), 0.0))
        for_amt = max(0.0, _safe_float(values.get("for_amt"), 0.0))

        gross_amt = _round_money(qty * rate)
        discount_amt = _round_money(min(gross_amt, gross_amt * discount_pct / 100.0))
        taxable_amt = _round_money(max(gross_amt - discount_amt, 0.0))
        gst_amt = _round_money(taxable_amt * gst_pct / 100.0)
        cgst_pct = _round_money(gst_pct / 2.0, 4)
        sgst_pct = _round_money(gst_pct - cgst_pct, 4)
        cgst_amt = _round_money(gst_amt / 2.0)
        sgst_amt = _round_money(gst_amt - cgst_amt)
        net_amt = _round_money(taxable_amt + gst_amt + for_amt)
        lending_rate = _round_money(net_amt / qty, 4) if qty > 0 else 0.0
        unit_for = _round_money(for_amt / qty, 4) if qty > 0 else 0.0
        unit_discount = _round_money(discount_amt / qty, 4) if qty > 0 else 0.0

        payload = dict(values or {})
        payload.update(
            {
                "qty": qty,
                "free_qty": free_qty,
                "rate": rate,
                "mrp": mrp,
                "discount_pct": discount_pct,
                "discount_amt": discount_amt,
                "gst_pct": gst_pct,
                "gst_amt": gst_amt,
                "cgst_pct": cgst_pct,
                "sgst_pct": sgst_pct,
                "cgst_amt": cgst_amt,
                "sgst_amt": sgst_amt,
                "gross_amt": gross_amt,
                "taxable_amt": taxable_amt,
                "for_amt": for_amt,
                "net_amt": net_amt,
                "lending_rate": lending_rate,
                "unit_for": unit_for,
                "unit_discount": unit_discount,
            }
        )
        return payload

    def _compute_totals(items: list[dict]) -> dict:
        gross_total = 0.0
        discount_total = 0.0
        taxable_total = 0.0
        cgst_total = 0.0
        sgst_total = 0.0
        for_total = 0.0
        grand_total = 0.0
        rows = []

        for index, item in enumerate(items or [], start=1):
            row = _build_row(item)
            row["row_no"] = index
            gross_total += row.get("gross_amt", 0.0)
            discount_total += row.get("discount_amt", 0.0)
            taxable_total += row.get("taxable_amt", 0.0)
            cgst_total += row.get("cgst_amt", 0.0)
            sgst_total += row.get("sgst_amt", 0.0)
            for_total += row.get("for_amt", 0.0)
            grand_total += row.get("net_amt", 0.0)
            rows.append(row)

        tax_total = _round_money(cgst_total + sgst_total)
        return {
            "rows": rows,
            "gross_total": _round_money(gross_total),
            "discount_total": _round_money(discount_total),
            "taxable_total": _round_money(taxable_total),
            "cgst_total": _round_money(cgst_total),
            "sgst_total": _round_money(sgst_total),
            "tax_total": tax_total,
            "for_total": _round_money(for_total),
            "grand_total": _round_money(grand_total),
        }

    def _normalize_grn_types(df):
        rows = []
        if df is None or df.empty:
            return rows
        df = _clean_df_columns(df)
        cols = _col_map(df)
        for _, row in df.iterrows():
            type_id = _safe_int(_cell(row, cols, "id"), 0)
            if type_id not in ALLOWED_TYPE_IDS:
                continue
            rows.append({"id": type_id, "name": str(_cell(row, cols, "name") or "").strip()})
        priority = {DIRECT_PURCHASE_TYPE_ID: 0, AGAINST_PO_TYPE_ID: 1}
        rows.sort(key=lambda rec: (priority.get(rec["id"], 99), rec["name"].lower()))
        return rows

    def _normalize_stores(df):
        rows = []
        if df is None or df.empty:
            return rows
        df = _clean_df_columns(df)
        cols = _col_map(df)
        for _, row in df.iterrows():
            rows.append(
                {
                    "id": _safe_int(_cell(row, cols, "id"), 0),
                    "name": str(_cell(row, cols, "store name", "name") or "").strip(),
                    "code": str(_cell(row, cols, "store code", "code") or "").strip(),
                }
            )
        return [row for row in rows if row["id"] > 0]

    def _normalize_suppliers(df):
        rows = []
        if df is None or df.empty:
            return rows
        df = _clean_df_columns(df)
        cols = _col_map(df)
        for _, row in df.iterrows():
            rows.append(
                {
                    "id": _safe_int(_cell(row, cols, "id"), 0),
                    "name": str(_cell(row, cols, "name") or "").strip(),
                    "code": str(_cell(row, cols, "code") or "").strip(),
                }
            )
        rows = [row for row in rows if row["id"] > 0 and row["name"]]
        rows.sort(key=lambda rec: rec["name"].lower())
        return rows

    def _normalize_pack_sizes(df):
        rows = []
        if df is None or df.empty:
            return rows
        df = _clean_df_columns(df)
        cols = _col_map(df)
        for _, row in df.iterrows():
            rows.append({"id": _safe_int(_cell(row, cols, "id"), 0), "name": str(_cell(row, cols, "name") or "").strip()})
        return [row for row in rows if row["id"] > 0]

    def _normalize_items(df):
        rows = []
        if df is None or df.empty:
            return rows
        df = _clean_df_columns(df)
        cols = _col_map(df)
        for _, row in df.iterrows():
            item_id = _safe_int(_cell(row, cols, "id", "itemid"), 0)
            if item_id <= 0:
                continue
            item_name = str(_cell(row, cols, "itemname", "name") or "").strip()
            item_code = str(_cell(row, cols, "code", "itemcode") or "").strip()
            unit_name = str(_cell(row, cols, "unitname", "unit name") or "").strip()
            rows.append(
                {
                    "item_id": item_id,
                    "item_code": item_code,
                    "item_name": item_name,
                    "display_name": item_name,
                    "unit_id": _safe_int(_cell(row, cols, "unitid"), 0),
                    "unit_name": unit_name,
                    "pack_size_id": _safe_int(_cell(row, cols, "packsizeid", "packsize"), 0),
                    "rate": _safe_float(_cell(row, cols, "rate"), 0.0),
                    "mrp": _safe_float(_cell(row, cols, "mrp"), 0.0),
                    "gst_pct": _safe_float(_cell(row, cols, "gstpct"), 0.0),
                    "store_stock": _safe_float(_cell(row, cols, "storestock"), 0.0),
                    "batch_required": _boolish(_cell(row, cols, "batchreq", "batchrequired")),
                    "expiry_required": _boolish(_cell(row, cols, "expreq", "expirydtrequired")),
                    "quality_required": _boolish(_cell(row, cols, "chkqualityctrl")),
                }
            )
        rows.sort(key=lambda rec: (rec["item_name"].lower(), rec["item_code"].lower()))
        return rows

    def _normalize_po_list(df):
        rows = []
        if df is None or df.empty:
            return rows
        df = _clean_df_columns(df)
        cols = _col_map(df)
        for _, row in df.iterrows():
            po_id = _safe_int(_cell(row, cols, "id"), 0)
            if po_id <= 0:
                continue
            po_no = str(_cell(row, cols, "pono") or f"PO-{po_id}").strip()
            rows.append({"po_id": po_id, "po_no": po_no})
        return rows

    def _normalize_po_header(df, po_id: int):
        if df is None or df.empty:
            return {
                "po_id": int(po_id),
                "po_no": f"PO-{int(po_id)}",
                "po_date": "",
                "supplier_id": 0,
                "supplier_name": "",
                "amount": 0.0,
            }
        df = _clean_df_columns(df)
        row = df.iloc[0]
        cols = _col_map(df)
        return {
            "po_id": _safe_int(_cell(row, cols, "id"), po_id),
            "po_no": str(_cell(row, cols, "pono") or f"PO-{int(po_id)}").strip(),
            "po_date": _iso_date(_cell(row, cols, "podate")),
            "supplier_id": _safe_int(_cell(row, cols, "supplierid"), 0),
            "supplier_name": str(_cell(row, cols, "suppliername") or "").strip(),
            "amount": _safe_float(_cell(row, cols, "amount"), 0.0),
        }

    def _normalize_po_items(df):
        rows = []
        if df is None or df.empty:
            return rows
        df = _clean_df_columns(df)
        cols = _col_map(df)
        for _, row in df.iterrows():
            item_id = _safe_int(_cell(row, cols, "itemid", "id"), 0)
            if item_id <= 0:
                continue
            gst_pct = _safe_float(_cell(row, cols, "gstpct"), 0.0)
            if gst_pct <= 0:
                gst_pct = _safe_float(_cell(row, cols, "cgstpct"), 0.0) + _safe_float(_cell(row, cols, "sgstpct"), 0.0)
            base = {
                "item_id": item_id,
                "item_code": str(_cell(row, cols, "code", "item code") or "").strip(),
                "item_name": str(_cell(row, cols, "itemname", "item name") or "").strip(),
                "display_name": "",
                "unit_id": _safe_int(_cell(row, cols, "unitid"), 0),
                "unit_name": str(_cell(row, cols, "unit name", "unitname") or "").strip(),
                "pack_size_id": _safe_int(_cell(row, cols, "packsize", "packsizeid"), 0),
                "qty": _safe_float(_cell(row, cols, "outstandingqty", "poosbal", "poqty", "qty"), 0.0),
                "free_qty": _safe_float(_cell(row, cols, "freeqty"), 0.0),
                "rate": _safe_float(_cell(row, cols, "rate"), 0.0),
                "mrp": _safe_float(_cell(row, cols, "mrp"), 0.0),
                "discount_pct": _safe_float(_cell(row, cols, "discount"), 0.0),
                "gst_pct": gst_pct,
                "for_amt": _safe_float(_cell(row, cols, "foramt"), 0.0),
                "store_stock": _safe_float(_cell(row, cols, "storestock"), 0.0),
                "po_detail_id": _safe_int(_cell(row, cols, "dtlid", "detailid"), 0),
                "po_os_bal": _safe_float(_cell(row, cols, "outstandingqty", "poosbal"), 0.0),
                "po_qty": _safe_float(_cell(row, cols, "poqty", "qty"), 0.0),
                "batch_required": _boolish(_cell(row, cols, "batchrequired")),
                "expiry_required": _boolish(_cell(row, cols, "expirydtrequired")),
            }
            normalized = _build_row(base)
            normalized["display_name"] = normalized.get("item_name") or ""
            rows.append(normalized)
        return rows

    def _normalize_grn_list(df):
        rows = []
        if df is None or df.empty:
            return rows
        df = _clean_df_columns(df)
        cols = _col_map(df)
        for _, row in df.iterrows():
            grn_id = _safe_int(_cell(row, cols, "grnid", "id"), 0)
            if grn_id <= 0:
                continue
            rows.append(
                {
                    "grn_id": grn_id,
                    "grn_no": str(_cell(row, cols, "grnno") or f"GRN-{grn_id}").strip(),
                    "grn_date": _iso_date(_cell(row, cols, "grndate")),
                    "supplier_name": str(_cell(row, cols, "suppliername") or "").strip(),
                    "invoice_no": str(_cell(row, cols, "invoiceno") or "").strip(),
                    "invoice_date": _iso_date(_cell(row, cols, "invoicedate")),
                    "amount": _safe_float(_cell(row, cols, "amount"), 0.0),
                    "authorise": _boolish(_cell(row, cols, "authorise")),
                    "authorized_by": _clean_text(_cell(row, cols, "authorizedby")),
                    "authorized_date": _iso_date(_cell(row, cols, "authorizeddate")),
                    "status_label": "Authorized" if _boolish(_cell(row, cols, "authorise")) else "Pending Authorization",
                }
            )
        return rows

    def _grn_report_type_label(grn_type_id, grn_type_name) -> str:
        type_id = _safe_int(grn_type_id, 0)
        type_name = str(grn_type_name or "").strip()
        if type_id == AGAINST_PO_TYPE_ID:
            return "Against PO"
        if type_id == DIRECT_PURCHASE_TYPE_ID:
            return "Direct Purchase"
        lowered = type_name.lower()
        if "po" in lowered:
            return "Against PO"
        if "direct" in lowered:
            return "Direct Purchase"
        return type_name or "-"

    def _normalize_grn_summary_report(df):
        rows = []
        if df is None or df.empty:
            return rows
        df = _clean_df_columns(df)
        cols = _col_map(df)
        for _, row in df.iterrows():
            grn_id = _safe_int(_cell(row, cols, "id", "grnid"), 0)
            grn_type_id = _safe_int(_cell(row, cols, "grntypeid"), 0)
            po_no = str(_cell(row, cols, "pono") or "").strip()
            rows.append(
                {
                    "grn_id": grn_id,
                    "grn_no": str(_cell(row, cols, "grnno") or (f"GRN-{grn_id}" if grn_id else "")).strip(),
                    "supplier_name": str(_cell(row, cols, "suppliername") or "").strip(),
                    "grn_date": _iso_date(_cell(row, cols, "grndate")),
                    "grn_type": _grn_report_type_label(grn_type_id, _cell(row, cols, "grntypename")),
                    "po_no": po_no if grn_type_id == AGAINST_PO_TYPE_ID or po_no else "",
                }
            )
        return rows

    def _normalize_grn_header(df):
        if df is None or df.empty:
            return None
        df = _clean_df_columns(df)
        row = df.iloc[0]
        cols = _col_map(df)
        cgst_total = _safe_float(_cell(row, cols, "totalexciseamt"), 0.0)
        sgst_total = _safe_float(_cell(row, cols, "totaltaxamt"), 0.0)
        stock_posted_count = _safe_int(_cell(row, cols, "stockpostingcount"), 0)
        prepared_by = str(_cell(row, cols, "preparedby") or "").strip()
        inserted_by_user_id = str(_cell(row, cols, "insertedbyuserid") or "").strip()
        return {
            "grn_id": _safe_int(_cell(row, cols, "id"), 0),
            "grn_no": str(_cell(row, cols, "grnno") or "").strip(),
            "grn_date": _iso_date(_cell(row, cols, "grndate")),
            "grn_type_id": _safe_int(_cell(row, cols, "grntypeid"), 0),
            "grn_type_name": str(_cell(row, cols, "grntypename") or "").strip(),
            "po_id": _safe_int(_cell(row, cols, "poid"), 0),
            "po_no": str(_cell(row, cols, "pono") or "").strip(),
            "supplier_id": _safe_int(_cell(row, cols, "supplierid"), 0),
            "supplier_name": str(_cell(row, cols, "suppliername") or "").strip(),
            "supplier_code": str(_cell(row, cols, "suppliercode") or "").strip(),
            "store_id": _safe_int(_cell(row, cols, "storeid"), 0),
            "store_name": str(_cell(row, cols, "storename") or "").strip(),
            "store_code": str(_cell(row, cols, "storecode") or "").strip(),
            "dc_no": str(_cell(row, cols, "dcno") or "").strip(),
            "dc_date": _iso_date(_cell(row, cols, "dcdate")),
            "invoice_no": str(_cell(row, cols, "invoiceno") or "").strip(),
            "invoice_date": _iso_date(_cell(row, cols, "invoicedate")),
            "transporter": str(_cell(row, cols, "transporter") or "").strip(),
            "vehicle_no": str(_cell(row, cols, "vehicleno") or "").strip(),
            "prepared_by": prepared_by,
            "pur_reg_no": str(_cell(row, cols, "purregno") or "").strip(),
            "notes": str(_cell(row, cols, "notes") or "").strip(),
            "amount": _safe_float(_cell(row, cols, "amount"), 0.0),
            "status": str(_cell(row, cols, "status") or "").strip(),
            "authorise": _boolish(_cell(row, cols, "authorise")),
            "authorized_by": _clean_text(_cell(row, cols, "authorizedby")),
            "authorized_date": _iso_date(_cell(row, cols, "authorizeddate")),
            "canceled": _boolish(_cell(row, cols, "canceled")),
            "stock_posted_count": stock_posted_count,
            "stock_posted": stock_posted_count > 0,
            "status_label": _grn_status_label(_cell(row, cols, "status"), _cell(row, cols, "authorise"), _cell(row, cols, "canceled")),
            "created_by": prepared_by or (f"User ID {inserted_by_user_id}" if inserted_by_user_id else ""),
            "created_on": _iso_date(_cell(row, cols, "insertedon", "updatedon", "grndate")),
            "inserted_by_user_id": inserted_by_user_id,
            "inserted_on": _iso_date(_cell(row, cols, "insertedon")),
            "inserted_ip": str(_cell(row, cols, "insertedipaddress") or "").strip(),
            "updated_by": str(_cell(row, cols, "updatedby") or "").strip(),
            "updated_on": _iso_date(_cell(row, cols, "updatedon")),
            "updated_ip": str(_cell(row, cols, "updatedipaddress") or "").strip(),
            "totals": {
                "cgst_total": cgst_total,
                "sgst_total": sgst_total,
                "tax_total": _round_money(cgst_total + sgst_total),
                "discount_total": _safe_float(_cell(row, cols, "totaldisc"), 0.0),
                "for_total": _safe_float(_cell(row, cols, "totalfore"), 0.0),
                "grand_total": _safe_float(_cell(row, cols, "amount"), 0.0),
            },
        }

    def _normalize_grn_detail(df):
        rows = []
        if df is None or df.empty:
            return rows
        df = _clean_df_columns(df)
        cols = _col_map(df)
        for _, row in df.iterrows():
            item_id = _safe_int(_cell(row, cols, "itemid"), 0)
            if item_id <= 0:
                continue
            cgst_pct = _safe_float(_cell(row, cols, "excise"), 0.0)
            sgst_pct = _safe_float(_cell(row, cols, "taxrate"), 0.0)
            cgst_amt = _safe_float(_cell(row, cols, "excisetaxamt"), 0.0)
            sgst_amt = _safe_float(_cell(row, cols, "taxamount"), 0.0)
            cgst_pct, sgst_pct, cgst_amt, sgst_amt = _split_display_gst(cgst_pct, sgst_pct, cgst_amt, sgst_amt)
            item_name = str(_cell(row, cols, "item name", "itemname") or "").strip()
            item_code = str(_cell(row, cols, "item code", "code") or "").strip()
            entry = {
                "item_id": item_id,
                "item_code": item_code,
                "item_name": item_name,
                "display_name": item_name,
                "unit_id": _safe_int(_cell(row, cols, "unitid"), 0),
                "unit_name": str(_cell(row, cols, "unit name", "unitname") or "").strip(),
                "pack_size_id": _safe_int(_cell(row, cols, "packsize", "packsizeid"), 0),
                "qty": _safe_float(_cell(row, cols, "qty"), 0.0),
                "free_qty": _safe_float(_cell(row, cols, "freeqty"), 0.0),
                "rate": _safe_float(_cell(row, cols, "rate"), 0.0),
                "mrp": _safe_float(_cell(row, cols, "mrp"), 0.0),
                "discount_pct": _safe_float(_cell(row, cols, "discount"), 0.0),
                "gst_pct": _round_money(cgst_pct + sgst_pct, 4),
                "for_amt": _safe_float(_cell(row, cols, "for"), 0.0),
                "batch_name": str(_cell(row, cols, "batch no", "batchname") or "").strip(),
                "batch_id": _safe_int(_cell(row, cols, "batchid"), 0),
                "expiry_date": _blank_if_sentinel(_cell(row, cols, "expiry date")),
                "po_detail_id": _safe_int(_cell(row, cols, "podtlid"), 0),
                "po_os_bal": _safe_float(_cell(row, cols, "poosbal"), 0.0),
                "batch_required": _boolish(_cell(row, cols, "batchreq")),
                "expiry_required": _boolish(_cell(row, cols, "expreq")),
                "actual_lending_rate": _safe_float(_cell(row, cols, "actuallendingrate"), 0.0),
            }
            entry = _build_row(entry)
            entry["batch_name"] = str(_cell(row, cols, "batch no", "batchname") or "").strip()
            entry["batch_id"] = _safe_int(_cell(row, cols, "batchid"), 0)
            entry["expiry_date"] = _blank_if_sentinel(_cell(row, cols, "expiry date"))
            entry["po_detail_id"] = _safe_int(_cell(row, cols, "podtlid"), 0)
            entry["po_os_bal"] = _safe_float(_cell(row, cols, "poosbal"), 0.0)
            entry["actual_lending_rate"] = _safe_float(_cell(row, cols, "actuallendingrate"), 0.0)
            entry["net_amt"] = _safe_float(_cell(row, cols, "amount"), entry.get("net_amt"))
            rows.append(entry)
        return rows

    def _normalize_request_items(raw_items: list, grn_type_id: int) -> tuple[list[dict], list[str]]:
        items = []
        errors = []
        for index, raw_item in enumerate(raw_items or [], start=1):
            item_id = _safe_int((raw_item or {}).get("item_id"), 0)
            pack_size_id = _safe_int((raw_item or {}).get("pack_size_id"), 0)
            qty = max(0.0, _safe_float((raw_item or {}).get("qty"), 0.0))
            po_detail_id = _safe_int((raw_item or {}).get("po_detail_id"), 0)
            po_os_bal = max(0.0, _safe_float((raw_item or {}).get("po_os_bal"), 0.0))
            expiry_date = _iso_date((raw_item or {}).get("expiry_date")) if (raw_item or {}).get("expiry_date") else ""
            if item_id <= 0:
                errors.append(f"Row {index}: select a valid item.")
            if pack_size_id <= 0:
                errors.append(f"Row {index}: select a pack size.")
            if qty <= 0:
                errors.append(f"Row {index}: enter a quantity above 0.")
            if not str((raw_item or {}).get("batch_name") or "").strip():
                errors.append(f"Row {index}: batch name is required.")
            if _boolish((raw_item or {}).get("expiry_required")) and not expiry_date:
                errors.append(f"Row {index}: expiry date is required for this item.")
            if grn_type_id == AGAINST_PO_TYPE_ID and po_detail_id > 0:
                if po_os_bal > 0 and qty - po_os_bal > 0.0001:
                    errors.append(f"Row {index}: quantity cannot exceed PO balance ({po_os_bal:g}).")
            items.append(
                {
                    "item_id": item_id,
                    "item_name": str((raw_item or {}).get("item_name") or "").strip(),
                    "item_code": str((raw_item or {}).get("item_code") or "").strip(),
                    "display_name": str((raw_item or {}).get("display_name") or "").strip(),
                    "unit_id": _safe_int((raw_item or {}).get("unit_id"), 0),
                    "unit_name": str((raw_item or {}).get("unit_name") or "").strip(),
                    "pack_size_id": pack_size_id,
                    "qty": qty,
                    "free_qty": max(0.0, _safe_float((raw_item or {}).get("free_qty"), 0.0)),
                    "rate": max(0.0, _safe_float((raw_item or {}).get("rate"), 0.0)),
                    "mrp": max(0.0, _safe_float((raw_item or {}).get("mrp"), 0.0)),
                    "discount_pct": max(0.0, _safe_float((raw_item or {}).get("discount_pct"), 0.0)),
                    "gst_pct": max(0.0, _safe_float((raw_item or {}).get("gst_pct"), 0.0)),
                    "for_amt": max(0.0, _safe_float((raw_item or {}).get("for_amt"), 0.0)),
                    "store_stock": max(0.0, _safe_float((raw_item or {}).get("store_stock"), 0.0)),
                    "po_detail_id": po_detail_id,
                    "po_os_bal": po_os_bal,
                    "po_qty": max(0.0, _safe_float((raw_item or {}).get("po_qty"), 0.0)),
                    "batch_required": _boolish((raw_item or {}).get("batch_required")),
                    "expiry_required": _boolish((raw_item or {}).get("expiry_required")),
                    "batch_name": str((raw_item or {}).get("batch_name") or "").strip(),
                    "expiry_date": expiry_date or "1900-01-01",
                }
            )
        return items, errors

    def _grn_status_label(status_code, authorise=False, canceled=False) -> str:
        if canceled:
            return "Cancelled"
        if _boolish(authorise):
            return "Authorized"
        code = str(status_code or "").strip().upper()
        return {
            "A": "Approved",
            "PA": "Pending Authorization",
            "P": "Pending",
            "D": "Draft",
            "C": "Cancelled",
            "R": "Rejected",
        }.get(code, code or "Saved")

    def _parse_datetime_value(value):
        if isinstance(value, datetime):
            return value
        if isinstance(value, date):
            return datetime.combine(value, datetime.min.time())
        text = str(value or "").strip()
        if not text:
            return None
        try:
            return datetime.fromisoformat(text.replace("Z", ""))
        except Exception:
            pass
        for fmt in (
            "%Y-%m-%d",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M:%S.%f",
            "%d-%m-%Y",
            "%d/%m/%Y",
            "%d-%b-%Y",
            "%d-%b-%Y %H:%M:%S",
            "%m/%d/%Y %H:%M:%S",
        ):
            try:
                return datetime.strptime(text, fmt)
            except Exception:
                continue
        return None

    def _format_display_date(value) -> str:
        parsed = _parse_datetime_value(value)
        return parsed.strftime("%d-%b-%Y") if parsed else "-"

    def _format_display_datetime(value) -> str:
        parsed = _parse_datetime_value(value)
        return parsed.strftime("%d-%b-%Y %I:%M %p") if parsed else "-"

    def _format_indian_currency(value) -> str:
        try:
            number = float(value or 0)
        except Exception:
            return "0.00"
        sign = "-" if number < 0 else ""
        number = abs(number)
        text = f"{number:.2f}"
        integer_part, decimal_part = text.split(".", 1)
        if len(integer_part) <= 3:
            grouped = integer_part
        else:
            last_three = integer_part[-3:]
            remaining = integer_part[:-3]
            groups = []
            while remaining:
                groups.append(remaining[-2:])
                remaining = remaining[:-2]
            grouped = ",".join(reversed(groups)) + "," + last_three
        return f"{sign}{grouped}.{decimal_part}"

    def _format_number(value, places: int = 2) -> str:
        try:
            number = float(value or 0)
        except Exception:
            number = 0.0
        text = f"{number:.{places}f}".rstrip("0").rstrip(".")
        return text or "0"

    def _grn_amount_in_words(amount_value) -> str:
        from decimal import Decimal, ROUND_HALF_UP

        try:
            amount = Decimal(str(amount_value or 0)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        except Exception:
            amount = Decimal("0.00")

        units = ["", "One", "Two", "Three", "Four", "Five", "Six", "Seven", "Eight", "Nine"]
        teens = ["Ten", "Eleven", "Twelve", "Thirteen", "Fourteen", "Fifteen", "Sixteen", "Seventeen", "Eighteen", "Nineteen"]
        tens = ["", "", "Twenty", "Thirty", "Forty", "Fifty", "Sixty", "Seventy", "Eighty", "Ninety"]

        def two_digits(number: int) -> str:
            if number < 10:
                return units[number]
            if number < 20:
                return teens[number - 10]
            return f"{tens[number // 10]} {units[number % 10]}".strip()

        def three_digits(number: int) -> str:
            if number >= 100:
                return f"{units[number // 100]} Hundred {two_digits(number % 100)}".strip()
            return two_digits(number)

        def indian_words(number: int) -> str:
            if number == 0:
                return "Zero"
            parts = []
            crore = number // 10000000
            if crore:
                parts.append(f"{three_digits(crore)} Crore")
            number %= 10000000
            lakh = number // 100000
            if lakh:
                parts.append(f"{two_digits(lakh)} Lakh")
            number %= 100000
            thousand = number // 1000
            if thousand:
                parts.append(f"{two_digits(thousand)} Thousand")
            number %= 1000
            if number:
                parts.append(three_digits(number))
            return " ".join([part for part in parts if part]).strip()

        rupees = int(amount)
        paise = int((amount - Decimal(rupees)) * 100)
        rupee_words = indian_words(rupees)
        if paise > 0:
            return f"Rupees {rupee_words} and Paise {indian_words(paise)} Only"
        return f"Rupees {rupee_words} Only"

    def _build_grn_pdf_buffer(unit: str, header: dict, items: list[dict], printed_by: str, printed_at):
        import io
        from xml.sax.saxutils import escape
        from reportlab.lib import colors
        from reportlab.lib.enums import TA_CENTER, TA_RIGHT
        from reportlab.lib.pagesizes import A4, landscape
        from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
        from reportlab.lib.units import mm
        from reportlab.platypus import LongTable, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

        buffer = io.BytesIO()
        doc = SimpleDocTemplate(
            buffer,
            pagesize=landscape(A4),
            topMargin=7 * mm,
            bottomMargin=11 * mm,
            leftMargin=9 * mm,
            rightMargin=9 * mm,
        )
        doc.title = f"GRN Details - {header.get('grn_no') or header.get('grn_id') or ''}"
        doc.author = printed_by or "Hospital Intelligence Dashboard"
        styles = getSampleStyleSheet()

        company_style = ParagraphStyle(
            "GrnPdfCompany",
            parent=styles["Heading1"],
            alignment=TA_CENTER,
            fontName="Helvetica-Bold",
            fontSize=14.5,
            leading=16,
            textColor=colors.black,
            spaceAfter=1,
        )
        subtitle_style = ParagraphStyle(
            "GrnPdfSubtitle",
            parent=styles["Normal"],
            alignment=TA_CENTER,
            fontSize=8.1,
            leading=9.4,
            textColor=colors.HexColor("#4b5563"),
            spaceAfter=1,
        )
        doc_title_style = ParagraphStyle(
            "GrnPdfTitle",
            parent=styles["Normal"],
            alignment=TA_CENTER,
            fontName="Helvetica-Bold",
            fontSize=10.3,
            leading=11.5,
            textColor=colors.black,
        )
        strip_label_style = ParagraphStyle(
            "GrnPdfStripLabel",
            parent=styles["Normal"],
            fontSize=7.1,
            fontName="Helvetica-Bold",
            textColor=colors.HexColor("#475569"),
        )
        strip_value_style = ParagraphStyle(
            "GrnPdfStripValue",
            parent=styles["Normal"],
            fontSize=7.7,
            fontName="Helvetica-Bold",
            textColor=colors.black,
        )
        meta_label_style = ParagraphStyle("GrnPdfMetaLabel", parent=styles["Normal"], fontSize=7.0, fontName="Helvetica-Bold", textColor=colors.HexColor("#6b7280"))
        meta_value_style = ParagraphStyle(
            "GrnPdfMetaValue",
            parent=styles["Normal"],
            fontSize=7.8,
            leading=9.3,
            textColor=colors.black,
        )
        remark_style = ParagraphStyle("GrnPdfRemark", parent=styles["Normal"], fontSize=7.7, leading=9.2, textColor=colors.black)
        helper_style = ParagraphStyle("GrnPdfHelper", parent=styles["Normal"], fontSize=7.0, leading=8.3, textColor=colors.HexColor("#6b7280"))
        item_head_style = ParagraphStyle(
            "GrnPdfItemHead",
            parent=styles["Normal"],
            alignment=TA_CENTER,
            fontName="Helvetica-Bold",
            fontSize=7.0,
            leading=8.1,
            textColor=colors.black,
        )
        item_text_style = ParagraphStyle(
            "GrnPdfItemText",
            parent=styles["Normal"],
            fontSize=7.3,
            leading=8.7,
            textColor=colors.black,
        )
        item_bold_style = ParagraphStyle("GrnPdfItemBold", parent=item_text_style, fontName="Helvetica-Bold")
        item_center_style = ParagraphStyle("GrnPdfItemCenter", parent=item_text_style, alignment=TA_CENTER)
        item_num_style = ParagraphStyle("GrnPdfItemNum", parent=item_text_style, alignment=TA_RIGHT)
        total_label_style = ParagraphStyle("GrnPdfTotalLabel", parent=styles["Normal"], fontSize=7.9, fontName="Helvetica-Bold", textColor=colors.black)
        total_value_style = ParagraphStyle("GrnPdfTotalValue", parent=styles["Normal"], fontSize=7.9, alignment=TA_RIGHT, textColor=colors.black)

        totals = header.get("totals") or _compute_totals(items)
        grn_no = str(header.get("grn_no") or f"GRN-{header.get('grn_id') or ''}").strip()
        status_label = _grn_status_label(header.get("status"), header.get("authorise"), header.get("canceled"))
        created_by = str(header.get("created_by") or header.get("prepared_by") or "").strip()
        created_on = header.get("created_on") or header.get("inserted_on") or header.get("updated_on") or header.get("grn_date")
        authorized_by = str(header.get("authorized_by") or "").strip()
        authorized_on = header.get("authorized_date")
        amount_words = _grn_amount_in_words(totals.get("grand_total", header.get("amount") or 0))
        notes_text = str(header.get("notes") or "").strip()
        supplier_text = str(header.get("supplier_name") or "-").strip()
        store_text = " ".join(
            filter(
                None,
                [
                    str(header.get("store_name") or "").strip(),
                    f"({header.get('store_code')})" if header.get("store_code") else "",
                ],
            )
        ) or "-"

        def para(text, style):
            text = "-" if text in (None, "") else str(text)
            return Paragraph(escape(text).replace("\n", "<br/>"), style)

        header_block = Table(
            [
                [Paragraph("Asarfi Hospital Pvt Ltd", company_style)],
                [Paragraph("Sharpsight Purchase Module", subtitle_style)],
                [Paragraph("GRN Details", doc_title_style)],
            ],
            colWidths=[doc.width],
        )
        header_block.setStyle(
            TableStyle(
                [
                    ("LEFTPADDING", (0, 0), (-1, -1), 0),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 0),
                    ("TOPPADDING", (0, 0), (-1, -1), 0),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
                    ("ALIGN", (0, 0), (-1, -1), "CENTER"),
                ]
            )
        )

        title_rule = Table([[""]], colWidths=[doc.width])
        title_rule.setStyle(TableStyle([("LINEABOVE", (0, 0), (-1, 0), 1, colors.black)]))

        strip_table = Table(
            [
                [
                    para("GRN No.", strip_label_style), para(grn_no, strip_value_style),
                    para("GRN Date", strip_label_style), para(_format_display_date(header.get("grn_date")), strip_value_style),
                    para("Status", strip_label_style), para(status_label, strip_value_style),
                    para("Printed On", strip_label_style), para(_format_display_datetime(printed_at), strip_value_style),
                ]
            ],
            colWidths=[18 * mm, 34 * mm, 17 * mm, 24 * mm, 14 * mm, 30 * mm, 18 * mm, doc.width - (18 * mm + 34 * mm + 17 * mm + 24 * mm + 14 * mm + 30 * mm + 18 * mm)],
        )
        strip_table.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#f3f4f6")),
                    ("BOX", (0, 0), (-1, -1), 0.7, colors.HexColor("#9ca3af")),
                    ("INNERGRID", (0, 0), (-1, -1), 0.45, colors.HexColor("#d1d5db")),
                    ("LEFTPADDING", (0, 0), (-1, -1), 4),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 4),
                    ("TOPPADDING", (0, 0), (-1, -1), 3),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
                    ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ]
            )
        )

        meta_widths = [18 * mm, 52 * mm, 18 * mm, 35 * mm, 20 * mm, 38 * mm, 18 * mm, doc.width - (18 * mm + 52 * mm + 18 * mm + 35 * mm + 20 * mm + 38 * mm + 18 * mm)]
        meta_rows = [
            [
                para("Supplier", meta_label_style), para(supplier_text, meta_value_style),
                para("Store", meta_label_style), para(store_text, meta_value_style),
                para("GRN Type", meta_label_style), para(header.get("grn_type_name") or "-", meta_value_style),
                para("Approved PO", meta_label_style), para(header.get("po_no") or "-", meta_value_style),
            ],
            [
                para("Invoice No.", meta_label_style), para(header.get("invoice_no") or "-", meta_value_style),
                para("Invoice Date", meta_label_style), para(_format_display_date(header.get("invoice_date")), meta_value_style),
                para("DC No.", meta_label_style), para(header.get("dc_no") or "-", meta_value_style),
                para("DC Date", meta_label_style), para(_format_display_date(header.get("dc_date")), meta_value_style),
            ],
            [
                para("Transporter", meta_label_style), para(header.get("transporter") or "-", meta_value_style),
                para("Vehicle No.", meta_label_style), para(header.get("vehicle_no") or "-", meta_value_style),
                para("Pur Reg No.", meta_label_style), para(header.get("pur_reg_no") or "-", meta_value_style),
                para("Supplier Code", meta_label_style), para(header.get("supplier_code") or "-", meta_value_style),
            ],
            [
                para("Prepared By", meta_label_style), para(header.get("prepared_by") or "-", meta_value_style),
                para("Created By", meta_label_style), para(created_by or "-", meta_value_style),
                para("Created On", meta_label_style), para(_format_display_datetime(created_on), meta_value_style),
                para("Printed By", meta_label_style), para(printed_by or "-", meta_value_style),
            ],
        ]
        if authorized_by or authorized_on:
            meta_rows.append(
                [
                    para("Authorized By", meta_label_style), para(authorized_by or "-", meta_value_style),
                    para("Authorized On", meta_label_style), para(_format_display_datetime(authorized_on), meta_value_style),
                    para("Store Code", meta_label_style), para(header.get("store_code") or "-", meta_value_style),
                    para("Unit", meta_label_style), para(unit, meta_value_style),
                ]
            )
        else:
            meta_rows.append(
                [
                    para("Printed On", meta_label_style), para(_format_display_datetime(printed_at), meta_value_style),
                    para("Store Code", meta_label_style), para(header.get("store_code") or "-", meta_value_style),
                    para("Unit", meta_label_style), para(unit, meta_value_style),
                    para("Copy Type", meta_label_style), para("System Copy", meta_value_style),
                ]
            )
        meta_table = Table(meta_rows, colWidths=meta_widths)
        meta_table.setStyle(
            TableStyle(
                [
                    ("BOX", (0, 0), (-1, -1), 0.6, colors.HexColor("#c7cdd4")),
                    ("INNERGRID", (0, 0), (-1, -1), 0.35, colors.HexColor("#e5e7eb")),
                    ("LEFTPADDING", (0, 0), (-1, -1), 4),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 4),
                    ("TOPPADDING", (0, 0), (-1, -1), 3),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
                    ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ]
            )
        )

        item_header = [
            Paragraph("Item Name", item_head_style),
            Paragraph("Batch", item_head_style),
            Paragraph("Expiry", item_head_style),
            Paragraph("UOM", item_head_style),
            Paragraph("Pack", item_head_style),
            Paragraph("Qty", item_head_style),
            Paragraph("Free", item_head_style),
            Paragraph("Rate", item_head_style),
            Paragraph("MRP", item_head_style),
            Paragraph("Disc %", item_head_style),
            Paragraph("GST %", item_head_style),
            Paragraph("GST Amt", item_head_style),
            Paragraph("Net Amt", item_head_style),
        ]
        item_rows = [item_header]
        for row in items or []:
            item_rows.append(
                [
                    Paragraph(escape(str(row.get("item_name") or row.get("display_name") or "-")), item_bold_style),
                    Paragraph(escape(str(row.get("batch_name") or "-")), item_text_style),
                    Paragraph(escape(_format_display_date(row.get("expiry_date")) if row.get("expiry_date") else "-"), item_center_style),
                    Paragraph(escape(str(row.get("unit_name") or "-")), item_center_style),
                    Paragraph(escape(str(row.get("pack_size_name") or row.get("pack_size_label") or row.get("pack_size_id") or "-")), item_center_style),
                    Paragraph(_format_number(row.get("qty") or 0), item_num_style),
                    Paragraph(_format_number(row.get("free_qty") or 0), item_num_style),
                    Paragraph(_format_indian_currency(row.get("rate") or 0), item_num_style),
                    Paragraph(_format_indian_currency(row.get("mrp") or 0), item_num_style),
                    Paragraph(_format_number(row.get("discount_pct") or 0), item_num_style),
                    Paragraph(_format_number(row.get("gst_pct") or 0), item_num_style),
                    Paragraph(_format_indian_currency(row.get("gst_amt") or 0), item_num_style),
                    Paragraph(_format_indian_currency(row.get("net_amt") or 0), item_num_style),
                ]
            )
        item_table = LongTable(
            item_rows,
            repeatRows=1,
            colWidths=[70 * mm, 24 * mm, 18 * mm, 14 * mm, 14 * mm, 12 * mm, 12 * mm, 18 * mm, 18 * mm, 13 * mm, 13 * mm, 18 * mm, doc.width - (70 * mm + 24 * mm + 18 * mm + 14 * mm + 14 * mm + 12 * mm + 12 * mm + 18 * mm + 18 * mm + 13 * mm + 13 * mm + 18 * mm)],
        )
        item_table.setStyle(
            TableStyle(
                [
                    ("LINEABOVE", (0, 0), (-1, 0), 0.9, colors.black),
                    ("LINEBELOW", (0, 0), (-1, 0), 0.9, colors.black),
                    ("LINEBELOW", (0, 1), (-1, -1), 0.25, colors.HexColor("#d1d5db")),
                    ("LEFTPADDING", (0, 0), (-1, -1), 3),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 3),
                    ("TOPPADDING", (0, 0), (-1, -1), 3),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
                    ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ]
            )
        )

        remarks_text = notes_text or "-"
        left_footer_box = Table(
            [
                [Paragraph(f"<b>Remarks :</b> {escape(remarks_text)}", remark_style)],
                [Paragraph(f"<b>Amount in words :</b> {escape(amount_words)}", remark_style)],
                [Paragraph("Printed copy for internal verification, stock follow-up, and audit reference.", helper_style)],
            ],
            colWidths=[doc.width * 0.72],
        )
        left_footer_box.setStyle(
            TableStyle(
                [
                    ("LEFTPADDING", (0, 0), (-1, -1), 0),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 8),
                    ("TOPPADDING", (0, 0), (-1, -1), 2),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
                    ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ]
            )
        )

        totals_table = Table(
            [
                [Paragraph("Gross Amount", total_label_style), Paragraph(_format_indian_currency(totals.get("gross_total", 0)), total_value_style)],
                [Paragraph("Discount", total_label_style), Paragraph(_format_indian_currency(totals.get("discount_total", 0)), total_value_style)],
                [Paragraph("CGST", total_label_style), Paragraph(_format_indian_currency(totals.get("cgst_total", 0)), total_value_style)],
                [Paragraph("SGST", total_label_style), Paragraph(_format_indian_currency(totals.get("sgst_total", 0)), total_value_style)],
                [Paragraph("Total GST", total_label_style), Paragraph(_format_indian_currency(totals.get("tax_total", 0)), total_value_style)],
                [Paragraph("F.O.R.", total_label_style), Paragraph(_format_indian_currency(totals.get("for_total", 0)), total_value_style)],
                [Paragraph("Total Net Amount", ParagraphStyle("GrnPdfGrandLabel", parent=total_label_style, fontSize=8.6)), Paragraph(_format_indian_currency(totals.get("grand_total", header.get("amount") or 0)), ParagraphStyle("GrnPdfGrandValue", parent=total_value_style, fontSize=8.8, fontName="Helvetica-Bold"))],
            ],
            colWidths=[34 * mm, doc.width * 0.28 - 34 * mm],
        )
        totals_table.setStyle(
            TableStyle(
                [
                    ("LINEABOVE", (0, 0), (-1, 0), 0.9, colors.black),
                    ("LINEBELOW", (0, -1), (-1, -1), 0.9, colors.black),
                    ("LEFTPADDING", (0, 0), (-1, -1), 3),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 3),
                    ("TOPPADDING", (0, 0), (-1, -1), 3),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
                    ("ALIGN", (0, 0), (-1, -1), "RIGHT"),
                ]
            )
        )

        footer_table = Table([[left_footer_box, totals_table]], colWidths=[doc.width * 0.72, doc.width * 0.28])
        footer_table.setStyle(
            TableStyle(
                [
                    ("VALIGN", (0, 0), (-1, -1), "TOP"),
                    ("LEFTPADDING", (0, 0), (-1, -1), 0),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 0),
                    ("TOPPADDING", (0, 0), (-1, -1), 0),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
                ]
            )
        )

        elements = [
            header_block,
            Spacer(1, 1.5 * mm),
            title_rule,
            Spacer(1, 2 * mm),
            strip_table,
            Spacer(1, 1.6 * mm),
            meta_table,
            Spacer(1, 2 * mm),
            item_table,
            Spacer(1, 2 * mm),
            footer_table,
        ]

        def draw_footer(canvas, doc_obj):
            page_width, _ = landscape(A4)
            footer_y = 6 * mm
            canvas.saveState()
            canvas.setStrokeColor(colors.HexColor("#9ca3af"))
            canvas.line(doc_obj.leftMargin, footer_y + 3.4 * mm, page_width - doc_obj.rightMargin, footer_y + 3.4 * mm)
            canvas.setFont("Helvetica", 7)
            canvas.setFillColor(colors.HexColor("#4b5563"))
            canvas.drawString(doc_obj.leftMargin, footer_y, f"Printed By: {printed_by or '-'} | Printed On: {_format_display_datetime(printed_at)}")
            canvas.drawCentredString(page_width / 2, footer_y, f"Page {canvas.getPageNumber()}")
            canvas.drawRightString(page_width - doc_obj.rightMargin, footer_y, "Copyright (c) ASARFI HOSPITAL")
            canvas.restoreState()

        doc.build(elements, onFirstPage=draw_footer, onLaterPages=draw_footer)
        buffer.seek(0)
        return buffer

    def _build_grn_summary_report_pdf_buffer(
        unit: str,
        rows: list[dict],
        printed_by: str,
        printed_at,
        *,
        from_date: str = "",
        to_date: str = "",
        supplier_name: str = "",
    ):
        import io
        from xml.sax.saxutils import escape
        from reportlab.lib import colors
        from reportlab.lib.enums import TA_CENTER
        from reportlab.lib.pagesizes import A4
        from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
        from reportlab.lib.units import mm
        from reportlab.platypus import LongTable, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

        buffer = io.BytesIO()
        doc = SimpleDocTemplate(
            buffer,
            pagesize=A4,
            topMargin=8 * mm,
            bottomMargin=12 * mm,
            leftMargin=10 * mm,
            rightMargin=10 * mm,
        )
        doc.title = "GRN Summary Report"
        doc.author = printed_by or "Hospital Intelligence Dashboard"
        styles = getSampleStyleSheet()

        company_style = ParagraphStyle(
            "GrnReportCompany",
            parent=styles["Heading1"],
            alignment=TA_CENTER,
            fontName="Helvetica-Bold",
            fontSize=14.5,
            leading=16,
            textColor=colors.black,
            spaceAfter=1,
        )
        subtitle_style = ParagraphStyle(
            "GrnReportSubtitle",
            parent=styles["Normal"],
            alignment=TA_CENTER,
            fontSize=8.1,
            leading=9.4,
            textColor=colors.HexColor("#4b5563"),
            spaceAfter=1,
        )
        title_style = ParagraphStyle(
            "GrnReportTitle",
            parent=styles["Normal"],
            alignment=TA_CENTER,
            fontName="Helvetica-Bold",
            fontSize=10.5,
            leading=11.8,
            textColor=colors.black,
        )
        label_style = ParagraphStyle(
            "GrnReportLabel",
            parent=styles["Normal"],
            fontSize=7.1,
            fontName="Helvetica-Bold",
            textColor=colors.HexColor("#475569"),
        )
        value_style = ParagraphStyle(
            "GrnReportValue",
            parent=styles["Normal"],
            fontSize=7.8,
            leading=9.2,
            textColor=colors.black,
        )
        head_style = ParagraphStyle(
            "GrnReportHead",
            parent=styles["Normal"],
            alignment=TA_CENTER,
            fontName="Helvetica-Bold",
            fontSize=7.5,
            leading=8.7,
            textColor=colors.black,
        )
        cell_style = ParagraphStyle(
            "GrnReportCell",
            parent=styles["Normal"],
            fontSize=7.7,
            leading=9.2,
            textColor=colors.black,
        )
        strong_cell_style = ParagraphStyle("GrnReportStrongCell", parent=cell_style, fontName="Helvetica-Bold")
        helper_style = ParagraphStyle(
            "GrnReportHelper",
            parent=styles["Normal"],
            fontSize=7.1,
            leading=8.4,
            textColor=colors.HexColor("#4b5563"),
        )

        def para(text, style):
            text = "-" if text in (None, "") else str(text)
            return Paragraph(escape(text).replace("\n", "<br/>"), style)

        header_block = Table(
            [
                [Paragraph("Asarfi Hospital Pvt Ltd", company_style)],
                [Paragraph("Sharpsight Purchase Module", subtitle_style)],
                [Paragraph("GRN Summary Report", title_style)],
            ],
            colWidths=[doc.width],
        )
        header_block.setStyle(
            TableStyle(
                [
                    ("LEFTPADDING", (0, 0), (-1, -1), 0),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 0),
                    ("TOPPADDING", (0, 0), (-1, -1), 0),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
                    ("ALIGN", (0, 0), (-1, -1), "CENTER"),
                ]
            )
        )

        title_rule = Table([[""]], colWidths=[doc.width])
        title_rule.setStyle(TableStyle([("LINEABOVE", (0, 0), (-1, 0), 1, colors.black)]))

        filter_table = Table(
            [
                [
                    para("Unit", label_style), para(unit, value_style),
                    para("Report Rows", label_style), para(str(len(rows or [])), value_style),
                ],
                [
                    para("From Date", label_style), para(_format_display_date(from_date) if from_date else "-", value_style),
                    para("To Date", label_style), para(_format_display_date(to_date) if to_date else "-", value_style),
                ],
                [
                    para("Supplier", label_style), para(supplier_name or "All Suppliers", value_style),
                    para("Report Type", label_style), para("GRN Header Summary", value_style),
                ],
                [
                    para("Printed By", label_style), para(printed_by or "-", value_style),
                    para("Printed On", label_style), para(_format_display_datetime(printed_at), value_style),
                ],
            ],
            colWidths=[22 * mm, 73 * mm, 22 * mm, doc.width - (22 * mm + 73 * mm + 22 * mm)],
        )
        filter_table.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#f3f4f6")),
                    ("BOX", (0, 0), (-1, -1), 0.7, colors.HexColor("#9ca3af")),
                    ("INNERGRID", (0, 0), (-1, -1), 0.45, colors.HexColor("#d1d5db")),
                    ("LEFTPADDING", (0, 0), (-1, -1), 4),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 4),
                    ("TOPPADDING", (0, 0), (-1, -1), 3),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
                    ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ]
            )
        )

        table_rows = [
            [
                Paragraph("GRN No", head_style),
                Paragraph("Supplier Name", head_style),
                Paragraph("GRN Date", head_style),
                Paragraph("GRN Type", head_style),
                Paragraph("PO-Number", head_style),
            ]
        ]
        for row in rows or []:
            table_rows.append(
                [
                    Paragraph(escape(str(row.get("grn_no") or "-")), strong_cell_style),
                    Paragraph(escape(str(row.get("supplier_name") or "-")), cell_style),
                    Paragraph(escape(_format_display_date(row.get("grn_date"))), cell_style),
                    Paragraph(escape(str(row.get("grn_type") or "-")), cell_style),
                    Paragraph(escape(str(row.get("po_no") or "-")), cell_style),
                ]
            )
        if len(table_rows) == 1:
            table_rows.append([Paragraph("No GRNs found for the selected filters.", cell_style), "", "", "", ""])

        report_table = LongTable(
            table_rows,
            repeatRows=1,
            colWidths=[28 * mm, doc.width - (28 * mm + 26 * mm + 31 * mm + 27 * mm), 26 * mm, 31 * mm, 27 * mm],
        )
        report_style = [
            ("LINEABOVE", (0, 0), (-1, 0), 0.9, colors.black),
            ("LINEBELOW", (0, 0), (-1, 0), 0.9, colors.black),
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#e5e7eb")),
            ("LINEBELOW", (0, 1), (-1, -1), 0.25, colors.HexColor("#d1d5db")),
            ("LEFTPADDING", (0, 0), (-1, -1), 4),
            ("RIGHTPADDING", (0, 0), (-1, -1), 4),
            ("TOPPADDING", (0, 0), (-1, -1), 3),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ]
        if len(table_rows) == 2 and not rows:
            report_style.append(("SPAN", (0, 1), (-1, 1)))
        report_table.setStyle(TableStyle(report_style))

        note_table = Table(
            [[Paragraph("Printed copy for purchase review, supplier follow-up, and audit reference.", helper_style)]],
            colWidths=[doc.width],
        )
        note_table.setStyle(
            TableStyle(
                [
                    ("LINEABOVE", (0, 0), (-1, 0), 0.7, colors.HexColor("#9ca3af")),
                    ("LEFTPADDING", (0, 0), (-1, -1), 0),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 0),
                    ("TOPPADDING", (0, 0), (-1, -1), 5),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
                ]
            )
        )

        elements = [
            header_block,
            Spacer(1, 1.5 * mm),
            title_rule,
            Spacer(1, 2 * mm),
            filter_table,
            Spacer(1, 2.2 * mm),
            report_table,
            Spacer(1, 2.5 * mm),
            note_table,
        ]

        def draw_footer(canvas, doc_obj):
            page_width, _ = A4
            footer_y = 6 * mm
            canvas.saveState()
            canvas.setStrokeColor(colors.HexColor("#9ca3af"))
            canvas.line(doc_obj.leftMargin, footer_y + 3.4 * mm, page_width - doc_obj.rightMargin, footer_y + 3.4 * mm)
            canvas.setFont("Helvetica", 7)
            canvas.setFillColor(colors.HexColor("#4b5563"))
            canvas.drawString(doc_obj.leftMargin, footer_y, f"Printed By: {printed_by or '-'} | Printed On: {_format_display_datetime(printed_at)}")
            canvas.drawCentredString(page_width / 2, footer_y, f"Page {canvas.getPageNumber()}")
            canvas.drawRightString(page_width - doc_obj.rightMargin, footer_y, "Copyright (c) ASARFI HOSPITAL")
            canvas.restoreState()

        doc.build(elements, onFirstPage=draw_footer, onLaterPages=draw_footer)
        buffer.seek(0)
        return buffer

    @app.route("/purchase/grn")
    @login_required(allowed_roles={"IT", "Management", "Departmental Head", "Executive"})
    def purchase_grn():
        unit, error = _grn_unit()
        if error:
            return error
        return (
            render_template(
                "purchase_grn.html",
                unit=unit,
                prepared_by=session.get("username") or session.get("user") or "",
                can_authorize_grn=_can_authorize_grn(),
                can_backdate_grn=_can_backdate_grn(),
                today_iso=datetime.now(tz=LOCAL_TZ).date().isoformat(),
            ),
            200,
            {
                "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
                "Pragma": "no-cache",
                "Expires": "0",
            },
        )

    @app.route("/api/purchase/grn/init")
    @login_required(allowed_roles={"IT", "Management", "Departmental Head", "Executive"})
    def api_purchase_grn_init():
        unit, error = _grn_unit()
        if error:
            return error

        grn_type_df = data_fetch.fetch_purchase_grn_types(unit)
        store_df = data_fetch.fetch_purchase_grn_stores(unit, user_id=_safe_int(session.get("accountid") or session.get("account_id"), 0))
        supplier_df = data_fetch.fetch_purchase_grn_suppliers(unit)
        pack_df = data_fetch.fetch_purchase_pack_sizes(unit)
        stores = _normalize_stores(store_df)
        default_store = next(
            (
                row for row in stores
                if str(row.get("code") or "").strip().upper() == "EYE"
                or str(row.get("name") or "").strip().lower() == "eye pharmacy"
                or "eye pharmacy" in str(row.get("name") or "").strip().lower()
            ),
            stores[0] if stores else None,
        )

        return jsonify(
            {
                "status": "success",
                "unit": unit,
                "today": datetime.now(tz=LOCAL_TZ).date().isoformat(),
                "can_backdate_grn": _can_backdate_grn(),
                "permissions": {
                    "can_authorize": _can_authorize_grn(),
                },
                "prepared_by": session.get("username") or session.get("user") or "",
                "grn_types": _normalize_grn_types(grn_type_df),
                "stores": stores,
                "default_store_id": _safe_int((default_store or {}).get("id"), 0),
                "suppliers": _normalize_suppliers(supplier_df),
                "pack_sizes": _normalize_pack_sizes(pack_df),
            }
        )

    @app.route("/api/purchase/grn/items")
    @login_required(allowed_roles={"IT", "Management", "Departmental Head", "Executive"})
    def api_purchase_grn_items():
        unit, error = _grn_unit()
        if error:
            return error

        supplier_id = _safe_int(request.args.get("supplier_id"), 0)
        store_id = _safe_int(request.args.get("store_id"), 0)
        query = str(request.args.get("q") or "").strip().lower()

        df = data_fetch.fetch_purchase_grn_items(unit, supplier_id=supplier_id, store_id=store_id or None)
        if df is None:
            return jsonify({"status": "error", "message": "Failed to fetch GRN items."}), 500
        items = _normalize_items(df)
        if query:
            items = [
                item
                for item in items
                if query in str(item.get("item_name") or "").lower()
                or query in str(item.get("item_code") or "").lower()
                or query in str(item.get("display_name") or "").lower()
            ]
        return jsonify({"status": "success", "unit": unit, "items": items})

    @app.route("/api/purchase/grn/po_list")
    @login_required(allowed_roles={"IT", "Management", "Departmental Head", "Executive"})
    def api_purchase_grn_po_list():
        unit, error = _grn_unit()
        if error:
            return error

        query = str(request.args.get("q") or "").strip().lower()
        df = data_fetch.fetch_purchase_grn_po_list(unit)
        if df is None:
            return jsonify({"status": "error", "message": "Failed to fetch approved PO list."}), 500
        rows = _normalize_po_list(df)
        if query:
            rows = [row for row in rows if query in row.get("po_no", "").lower()]
        return jsonify({"status": "success", "unit": unit, "items": rows})

    @app.route("/api/purchase/grn/po_items")
    @login_required(allowed_roles={"IT", "Management", "Departmental Head", "Executive"})
    def api_purchase_grn_po_items():
        unit, error = _grn_unit()
        if error:
            return error

        po_id = _safe_int(request.args.get("po_id"), 0)
        store_id = _safe_int(request.args.get("store_id"), 0)
        if po_id <= 0:
            return jsonify({"status": "error", "message": "PO is required."}), 400

        items_df = data_fetch.fetch_purchase_grn_po_items(unit, po_id=po_id, store_id=store_id or None)
        if items_df is None:
            return jsonify({"status": "error", "message": "Failed to fetch PO-linked GRN rows."}), 500
        po_header_df = data_fetch.fetch_purchase_po_header(unit, po_id=po_id)
        return jsonify(
            {
                "status": "success",
                "unit": unit,
                "po": _normalize_po_header(po_header_df, po_id),
                "items": _normalize_po_items(items_df),
            }
        )

    @app.route("/api/purchase/grn/list")
    @login_required(allowed_roles={"IT", "Management", "Departmental Head", "Executive"})
    def api_purchase_grn_list():
        unit, error = _grn_unit()
        if error:
            return error

        grn_no = str(request.args.get("grn_no") or "").strip()
        supplier_name = str(request.args.get("supplier_name") or "").strip()
        pending_only = _boolish(request.args.get("pending_only"))
        page = max(1, _safe_int(request.args.get("page"), 1))
        page_size = max(1, min(20, _safe_int(request.args.get("page_size"), 6)))
        df = data_fetch.fetch_purchase_grn_list(unit, grn_no=grn_no, supplier_name=supplier_name)
        if df is None:
            _audit_log_event(
                "purchase",
                "grn_find",
                status="error",
                entity_type="grn",
                unit=unit,
                summary="GRN search failed",
                details={"grn_no": grn_no, "supplier_name": supplier_name},
            )
            return jsonify({"status": "error", "message": "Failed to fetch GRN list."}), 500

        rows = _normalize_grn_list(df)
        if pending_only:
            rows = [row for row in rows if not row.get("authorise")]
        total_count = len(rows)
        total_pages = max(1, (total_count + page_size - 1) // page_size)
        if page > total_pages:
            page = total_pages
        start_idx = (page - 1) * page_size
        end_idx = start_idx + page_size
        paged_rows = rows[start_idx:end_idx]
        _audit_log_event(
            "purchase",
            "grn_find",
            status="success",
            entity_type="grn",
            unit=unit,
            summary="GRN search completed",
            details={"grn_no": grn_no, "supplier_name": supplier_name, "pending_only": pending_only, "count": total_count, "page": page, "page_size": page_size},
        )
        return jsonify(
            {
                "status": "success",
                "unit": unit,
                "items": paged_rows,
                "pagination": {
                    "page": page,
                    "page_size": page_size,
                    "total_count": total_count,
                    "total_pages": total_pages,
                    "has_prev": page > 1,
                    "has_next": page < total_pages,
                },
            }
        )

    @app.route("/api/purchase/grn/report_summary")
    @login_required(allowed_roles={"IT", "Management", "Departmental Head", "Executive"})
    def api_purchase_grn_report_summary():
        unit, error = _grn_unit()
        if error:
            return error

        from_date = str(request.args.get("from_date") or "").strip()
        to_date = str(request.args.get("to_date") or "").strip()
        supplier_name = str(request.args.get("supplier_name") or "").strip()
        df = data_fetch.fetch_purchase_grn_summary_report(
            unit,
            from_date=from_date,
            to_date=to_date,
            supplier_name=supplier_name,
        )
        if df is None:
            _audit_log_event(
                "purchase",
                "grn_report_summary",
                status="error",
                entity_type="grn",
                unit=unit,
                summary="GRN summary report failed",
                details={"from_date": from_date, "to_date": to_date, "supplier_name": supplier_name},
            )
            return jsonify({"status": "error", "message": "Failed to fetch GRN summary report."}), 500

        rows = _normalize_grn_summary_report(df)
        _audit_log_event(
            "purchase",
            "grn_report_summary",
            status="success",
            entity_type="grn",
            unit=unit,
            summary="GRN summary report fetched",
            details={"from_date": from_date, "to_date": to_date, "supplier_name": supplier_name, "count": len(rows)},
        )
        return jsonify({"status": "success", "unit": unit, "items": rows, "total_count": len(rows)})

    @app.route("/api/purchase/grn/report_summary/pdf")
    @login_required(allowed_roles={"IT", "Management", "Departmental Head", "Executive"})
    def api_purchase_grn_report_summary_pdf():
        unit, error = _grn_unit()
        if error:
            return error

        from_date = str(request.args.get("from_date") or "").strip()
        to_date = str(request.args.get("to_date") or "").strip()
        supplier_name = str(request.args.get("supplier_name") or "").strip()
        df = data_fetch.fetch_purchase_grn_summary_report(
            unit,
            from_date=from_date,
            to_date=to_date,
            supplier_name=supplier_name,
        )
        if df is None:
            _audit_log_event(
                "purchase",
                "grn_report_summary_pdf",
                status="error",
                entity_type="grn",
                unit=unit,
                summary="GRN summary PDF failed",
                details={"from_date": from_date, "to_date": to_date, "supplier_name": supplier_name},
            )
            return jsonify({"status": "error", "message": "Failed to fetch GRN summary report."}), 500

        rows = _normalize_grn_summary_report(df)
        printed_by = str(session.get("username") or session.get("user") or "Unknown").strip() or "Unknown"
        printed_at = datetime.now(tz=LOCAL_TZ)
        pdf_buffer = _build_grn_summary_report_pdf_buffer(
            unit,
            rows,
            printed_by,
            printed_at,
            from_date=from_date,
            to_date=to_date,
            supplier_name=supplier_name,
        )
        date_part = f"{from_date or 'start'}_to_{to_date or 'today'}".replace("/", "-").replace("\\", "-")
        _audit_log_event(
            "purchase",
            "grn_report_summary_pdf",
            status="success",
            entity_type="grn",
            unit=unit,
            summary="GRN summary PDF generated",
            details={"from_date": from_date, "to_date": to_date, "supplier_name": supplier_name, "count": len(rows), "printed_by": printed_by},
        )
        return send_file(
            pdf_buffer,
            mimetype="application/pdf",
            as_attachment=False,
            download_name=f"GRN_Summary_Report_{date_part}.pdf",
        )

    @app.route("/api/purchase/grn/<int:grn_id>")
    @login_required(allowed_roles={"IT", "Management", "Departmental Head", "Executive"})
    def api_purchase_grn_detail(grn_id: int):
        unit, error = _grn_unit()
        if error:
            return error

        header_df = data_fetch.fetch_purchase_grn_header(unit, grn_id)
        if header_df is None:
            _audit_log_event(
                "purchase",
                "grn_view",
                status="error",
                entity_type="grn",
                entity_id=str(grn_id),
                unit=unit,
                summary="Failed to fetch GRN header",
            )
            return jsonify({"status": "error", "message": "Failed to fetch GRN."}), 500
        if header_df.empty:
            _audit_log_event(
                "purchase",
                "grn_view",
                status="error",
                entity_type="grn",
                entity_id=str(grn_id),
                unit=unit,
                summary="GRN not found",
            )
            return jsonify({"status": "error", "message": "GRN not found."}), 404

        detail_df = data_fetch.fetch_purchase_grn_detail(unit, grn_id)
        if detail_df is None:
            _audit_log_event(
                "purchase",
                "grn_view",
                status="error",
                entity_type="grn",
                entity_id=str(grn_id),
                unit=unit,
                summary="Failed to fetch GRN items",
            )
            return jsonify({"status": "error", "message": "Failed to fetch GRN items."}), 500

        header = _normalize_grn_header(header_df) or {}
        items = _normalize_grn_detail(detail_df)
        pack_size_df = data_fetch.fetch_purchase_pack_sizes(unit)
        pack_size_lookup = {row.get("id"): row.get("name") for row in _normalize_pack_sizes(pack_size_df)}
        for item in items:
            item["pack_size_name"] = pack_size_lookup.get(_safe_int(item.get("pack_size_id"), 0), str(item.get("pack_size_id") or "-"))
        totals = _compute_totals(items)
        header["totals"] = {
            "gross_total": totals.get("gross_total", 0.0),
            "discount_total": totals.get("discount_total", 0.0),
            "taxable_total": totals.get("taxable_total", 0.0),
            "cgst_total": totals.get("cgst_total", 0.0),
            "sgst_total": totals.get("sgst_total", 0.0),
            "tax_total": totals.get("tax_total", 0.0),
            "for_total": totals.get("for_total", 0.0),
            "grand_total": header.get("amount") or totals.get("grand_total", 0.0),
        }
        can_edit = not header.get("authorise") and not header.get("canceled") and not header.get("stock_posted")

        _audit_log_event(
            "purchase",
            "grn_view",
            status="success",
            entity_type="grn",
            entity_id=str(grn_id),
            unit=unit,
            summary="GRN loaded",
            details={"grn_no": header.get("grn_no"), "item_count": len(items)},
        )
        return jsonify(
            {
                "status": "success",
                "unit": unit,
                "header": header,
                "items": items,
                "read_only": not can_edit,
                "permissions": {"can_authorize": _can_authorize_grn(), "can_edit": True},
            }
        )

    @app.route("/api/purchase/grn/<int:grn_id>/print")
    @login_required(allowed_roles={"IT", "Management", "Departmental Head", "Executive"})
    def api_purchase_grn_print(grn_id: int):
        unit, error = _grn_unit()
        if error:
            return error

        header_df = data_fetch.fetch_purchase_grn_header(unit, grn_id)
        if header_df is None:
            _audit_log_event(
                "purchase",
                "grn_print",
                status="error",
                entity_type="grn",
                entity_id=str(grn_id),
                unit=unit,
                summary="Failed to fetch GRN header for print",
            )
            return jsonify({"status": "error", "message": "Failed to fetch GRN."}), 500
        if header_df.empty:
            _audit_log_event(
                "purchase",
                "grn_print",
                status="error",
                entity_type="grn",
                entity_id=str(grn_id),
                unit=unit,
                summary="GRN not found for print",
            )
            return jsonify({"status": "error", "message": "GRN not found."}), 404

        detail_df = data_fetch.fetch_purchase_grn_detail(unit, grn_id)
        if detail_df is None:
            _audit_log_event(
                "purchase",
                "grn_print",
                status="error",
                entity_type="grn",
                entity_id=str(grn_id),
                unit=unit,
                summary="Failed to fetch GRN items for print",
            )
            return jsonify({"status": "error", "message": "Failed to fetch GRN items."}), 500

        header = _normalize_grn_header(header_df) or {}
        header_df = _clean_df_columns(header_df)
        row = header_df.iloc[0]
        cols = _col_map(header_df)
        prepared_by = str(_cell(row, cols, "preparedby") or header.get("prepared_by") or "").strip()
        inserted_by_user_id = str(_cell(row, cols, "insertedbyuserid") or header.get("inserted_by_user_id") or "").strip()
        created_by = prepared_by or (f"User ID {inserted_by_user_id}" if inserted_by_user_id else "")
        if prepared_by and inserted_by_user_id:
            created_by = f"{prepared_by} (User ID {inserted_by_user_id})"
        header.update(
            {
                "status": str(_cell(row, cols, "status") or header.get("status") or "").strip(),
                "authorise": _boolish(_cell(row, cols, "authorise")),
                "canceled": _boolish(_cell(row, cols, "canceled")),
                "prepared_by": prepared_by,
                "created_by": created_by,
                "created_on": _cell(row, cols, "insertedon", "updatedon", "grndate"),
                "inserted_by_user_id": inserted_by_user_id,
                "inserted_on": _cell(row, cols, "insertedon"),
                "inserted_ip": str(_cell(row, cols, "insertedipaddress") or "").strip(),
                "updated_by": str(_cell(row, cols, "updatedby") or "").strip(),
                "updated_on": _cell(row, cols, "updatedon"),
                "updated_ip": str(_cell(row, cols, "updatedipaddress") or "").strip(),
                "authorized_by": _clean_text(_cell(row, cols, "authorizedby")),
                "authorized_date": _cell(row, cols, "authorizeddate"),
            }
        )

        items = _normalize_grn_detail(detail_df)
        totals = _compute_totals(items)
        header["totals"] = {
            "gross_total": totals.get("gross_total", 0.0),
            "discount_total": totals.get("discount_total", 0.0),
            "taxable_total": totals.get("taxable_total", 0.0),
            "cgst_total": totals.get("cgst_total", 0.0),
            "sgst_total": totals.get("sgst_total", 0.0),
            "tax_total": totals.get("tax_total", 0.0),
            "for_total": totals.get("for_total", 0.0),
            "grand_total": header.get("amount") or totals.get("grand_total", 0.0),
        }

        printed_by = str(session.get("username") or session.get("user") or "Unknown").strip() or "Unknown"
        printed_at = datetime.now(tz=LOCAL_TZ)
        pdf_buffer = _build_grn_pdf_buffer(unit, header, items, printed_by, printed_at)
        safe_grn_no = str(header.get("grn_no") or f"GRN-{grn_id}").replace("/", "-").replace("\\", "-")

        _audit_log_event(
            "purchase",
            "grn_print",
            status="success",
            entity_type="grn",
            entity_id=str(grn_id),
            unit=unit,
            summary="GRN PDF generated",
            details={"grn_no": header.get("grn_no"), "item_count": len(items), "printed_by": printed_by},
        )
        return send_file(
            pdf_buffer,
            mimetype="application/pdf",
            as_attachment=False,
            download_name=f"{safe_grn_no}_Details.pdf",
        )

    @app.route("/api/purchase/grn/authorize", methods=["POST"])
    @login_required(allowed_roles={"IT", "Management", "Departmental Head", "Executive"})
    def api_purchase_grn_authorize():
        unit, error = _grn_unit()
        if error:
            return error

        if not _can_authorize_grn():
            _audit_log_event(
                "purchase",
                "grn_authorize",
                status="error",
                entity_type="grn",
                unit=unit,
                summary="GRN authorization denied",
                details={"role": _session_role()},
            )
            return jsonify({"status": "error", "message": "Only IT and Departmental Heads can authorize GRNs."}), 403

        payload = _sanitize_json_payload(request.get_json(silent=True) or {})
        grn_id = _safe_int(payload.get("grn_id"), 0)
        remarks = str(payload.get("remarks") or "").strip()
        if grn_id <= 0:
            _audit_log_event("purchase", "grn_authorize", status="error", entity_type="grn", unit=unit, summary="GRN ID is required")
            return jsonify({"status": "error", "message": "GRN ID is required."}), 400

        header_df = data_fetch.fetch_purchase_grn_header(unit, grn_id)
        if header_df is None:
            _audit_log_event(
                "purchase",
                "grn_authorize",
                status="error",
                entity_type="grn",
                entity_id=str(grn_id),
                unit=unit,
                summary="Failed to fetch GRN for authorization",
            )
            return jsonify({"status": "error", "message": "Failed to fetch GRN."}), 500
        if header_df.empty:
            _audit_log_event(
                "purchase",
                "grn_authorize",
                status="error",
                entity_type="grn",
                entity_id=str(grn_id),
                unit=unit,
                summary="GRN not found for authorization",
            )
            return jsonify({"status": "error", "message": "GRN not found."}), 404

        header = _normalize_grn_header(header_df) or {}
        if header.get("canceled"):
            return jsonify({"status": "error", "message": "Cancelled GRNs cannot be authorized."}), 400
        if header.get("authorise"):
            return jsonify({"status": "error", "message": "This GRN is already authorized."}), 400

        now = datetime.now(tz=LOCAL_TZ)
        actor_user_id = _safe_int(session.get("accountid") or session.get("account_id"), 0)
        actor_username = str(session.get("username") or session.get("user") or actor_user_id or "web").strip() or "web"
        result = data_fetch.authorize_purchase_grn(
            unit,
            grn_id,
            actor_user_id=actor_user_id,
            actor_username=actor_username,
            actor_ip=request.remote_addr or "",
            acted_on=now.strftime("%Y-%m-%d %H:%M:%S"),
        )
        if result.get("error"):
            _audit_log_event(
                "purchase",
                "grn_authorize",
                status="error",
                entity_type="grn",
                entity_id=str(grn_id),
                unit=unit,
                summary="GRN authorization failed",
                details={"error": result.get("error"), "remarks": remarks},
            )
            return jsonify({"status": "error", "message": str(result.get("error") or "Failed to authorize GRN.")}), 500

        _audit_log_event(
            "purchase",
            "grn_authorize",
            status="success",
            entity_type="grn",
            entity_id=str(grn_id),
            unit=unit,
            summary="GRN authorized",
            details={
                "grn_no": result.get("grn_no") or header.get("grn_no"),
                "posted_rows": result.get("posted_rows"),
                "authorized_by": actor_username,
                "remarks": remarks,
            },
        )
        return jsonify(
            {
                "status": "success",
                "unit": unit,
                "grn_id": grn_id,
                "grn_no": result.get("grn_no") or header.get("grn_no"),
                "posted_rows": result.get("posted_rows", 0),
                "authorized_by": actor_username,
                "authorized_on": now.isoformat(),
            }
        )

    @app.route("/api/purchase/grn", methods=["POST"])
    @login_required(allowed_roles={"IT", "Management", "Departmental Head", "Executive"})
    def api_purchase_grn_save():
        unit, error = _grn_unit()
        if error:
            return error

        payload = _sanitize_json_payload(request.get_json(silent=True) or {})
        header = payload.get("header") or {}
        raw_items = payload.get("items") or []
        requested_grn_id = _safe_int(header.get("grn_id"), 0)
        is_update = requested_grn_id > 0
        audit_action = "grn_update" if is_update else "grn_create"
        success_summary = "GRN updated" if is_update else "GRN created"
        error_summary_prefix = "GRN update" if is_update else "GRN save"

        grn_type_id = _safe_int(header.get("grn_type_id"), 0)
        supplier_id = _safe_int(header.get("supplier_id"), 0)
        store_id = _safe_int(header.get("store_id"), 0)
        po_id = _safe_int(header.get("po_id"), 0)

        if grn_type_id not in ALLOWED_TYPE_IDS:
            _audit_log_event("purchase", audit_action, status="error", entity_type="grn", unit=unit, summary="Invalid GRN type")
            return jsonify({"status": "error", "message": "Choose Direct Purchase or Against PO."}), 400
        if supplier_id <= 0:
            _audit_log_event("purchase", audit_action, status="error", entity_type="grn", unit=unit, summary="Supplier is required")
            return jsonify({"status": "error", "message": "Supplier is required."}), 400
        if store_id <= 0:
            _audit_log_event("purchase", audit_action, status="error", entity_type="grn", unit=unit, summary="Store is required")
            return jsonify({"status": "error", "message": "Store is required."}), 400
        if grn_type_id == AGAINST_PO_TYPE_ID and po_id <= 0:
            _audit_log_event("purchase", audit_action, status="error", entity_type="grn", unit=unit, summary="Against PO GRN requires a PO")
            return jsonify({"status": "error", "message": "Select an approved PO for Against PO GRN."}), 400
        if not raw_items:
            _audit_log_event("purchase", audit_action, status="error", entity_type="grn", unit=unit, summary="At least one item is required")
            return jsonify({"status": "error", "message": "At least one item is required."}), 400

        if is_update:
            existing_header_df = data_fetch.fetch_purchase_grn_header(unit, requested_grn_id)
            if existing_header_df is None:
                _audit_log_event("purchase", audit_action, status="error", entity_type="grn", entity_id=str(requested_grn_id), unit=unit, summary="Failed to fetch GRN for edit")
                return jsonify({"status": "error", "message": "Failed to fetch the existing GRN for editing."}), 500
            if existing_header_df.empty:
                _audit_log_event("purchase", audit_action, status="error", entity_type="grn", entity_id=str(requested_grn_id), unit=unit, summary="GRN not found for edit")
                return jsonify({"status": "error", "message": "GRN not found."}), 404
            existing_header = _normalize_grn_header(existing_header_df) or {}
            if existing_header.get("canceled"):
                return jsonify({"status": "error", "message": "Cancelled GRNs cannot be edited."}), 400
            if existing_header.get("authorise"):
                return jsonify({"status": "error", "message": "Authorized GRNs cannot be edited."}), 400
            if existing_header.get("stock_posted"):
                return jsonify({"status": "error", "message": "This pending GRN already has stock posted and can no longer be edited."}), 400

        items, item_errors = _normalize_request_items(raw_items, grn_type_id)
        if item_errors:
            _audit_log_event(
                "purchase",
                audit_action,
                status="error",
                entity_type="grn",
                unit=unit,
                summary="GRN validation failed",
                details={"errors": item_errors},
            )
            return jsonify({"status": "error", "message": "Please fix the highlighted GRN rows.", "errors": item_errors}), 400

        now = datetime.now(tz=LOCAL_TZ)
        can_backdate_grn = _can_backdate_grn()
        today_iso = now.date().isoformat()
        requested_grn_date = _iso_date(header.get("grn_date")) or today_iso
        account_id = _safe_int(session.get("accountid") or session.get("account_id"), 0)

        if not can_backdate_grn and requested_grn_date != today_iso:
            _audit_log_event("purchase", audit_action, status="error", entity_type="grn", unit=unit, summary="Backdated GRN date denied")
            return jsonify({"status": "error", "message": "GRN date must be the current date. Backdated GRN entry is allowed only for IT."}), 400

        save_header = {
            "grn_id": requested_grn_id,
            "grn_type_id": grn_type_id,
            "po_id": po_id if grn_type_id == AGAINST_PO_TYPE_ID else 0,
            "pr_id": 0,
            "supplier_id": supplier_id,
            "dc_no": str(header.get("dc_no") or "").strip(),
            "dc_date": _iso_date(header.get("dc_date")) or now.date().isoformat(),
            "grn_date": requested_grn_date if can_backdate_grn else today_iso,
            "invoice_no": str(header.get("invoice_no") or "").strip(),
            "invoice_date": _iso_date(header.get("invoice_date")) or now.date().isoformat(),
            "transporter": str(header.get("transporter") or "").strip(),
            "vehicle_no": str(header.get("vehicle_no") or "").strip(),
            "octroi_amount": 0,
            "prepared_by": str(header.get("prepared_by") or session.get("username") or session.get("user") or "").strip(),
            "pur_reg_no": str(header.get("pur_reg_no") or "").strip(),
            "notes": str(header.get("notes") or "").strip(),
            "updated_by": account_id if account_id > 0 else 1,
            "updated_on": now.strftime("%Y-%m-%d %H:%M:%S"),
            "inserted_by": str(session.get("username") or session.get("user") or account_id or "web").strip(),
            "inserted_on": now.strftime("%Y-%m-%d %H:%M:%S"),
            "inserted_mac_name": "",
            "inserted_mac_id": "WEB",
            "inserted_ip": request.remote_addr or "",
            "store_id": store_id,
            "day_book_id": 0,
            "rc_id": 0,
            "status": "PA",
        }

        result = data_fetch.save_purchase_grn(unit, save_header, items)
        if result.get("error"):
            _audit_log_event(
                "purchase",
                audit_action,
                status="error",
                entity_type="grn",
                entity_id=str(requested_grn_id or ""),
                unit=unit,
                summary=f"{error_summary_prefix} failed",
                details={"error": result.get("error"), "item_count": len(items), "grn_id": requested_grn_id or None},
            )
            return jsonify({"status": "error", "message": str(result.get("error") or "Failed to save GRN.")}), 500

        totals = result.get("totals") or _compute_totals(items)
        _audit_log_event(
            "purchase",
            audit_action,
            status="success",
            entity_type="grn",
            entity_id=str(result.get("grn_id") or ""),
            unit=unit,
            summary=success_summary,
            details={"grn_no": result.get("grn_no"), "item_count": len(items), "totals": totals, "operation": result.get("operation") or ("update" if is_update else "create")},
        )
        return jsonify(
            {
                "status": "success",
                "unit": unit,
                "grn_id": result.get("grn_id"),
                "grn_no": result.get("grn_no"),
                "totals": totals,
                "authorization_pending": True,
                "updated": bool(result.get("operation") == "update" or is_update),
            }
        )
