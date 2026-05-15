from flask import jsonify, request, send_file, session
from datetime import date, datetime, timedelta
import io
import pandas as pd
import re
import time
from secrets import token_hex

from modules import data_fetch


def register_purchase_po_routes(
    app,
    *,
    login_required,
    allowed_purchase_units_for_session,
    get_purchase_unit,
    clean_df_columns,
    sanitize_json_payload,
    safe_float,
    safe_int,
    get_login_db_connection,
    audit_log_event,
    saved_po_print_format,
    resolve_purchasing_department_name,
    fetch_latest_purchase_otp_request,
    fetch_purchase_approval_meta,
    ensure_purchase_po_valuation_table,
    export_cache_get_bytes,
    export_cache_put_bytes,
    excel_job_get,
    excel_job_update,
    create_purchase_otp_request,
    validate_purchase_otp,
    insert_purchase_otp_request,
    mark_purchase_otp_used,
    mark_central_otp_used,
    fetch_purchase_otp_request_by_id,
    send_purchase_po_approval_email,
    build_po_pdf_buffer,
    resolve_po_print_format_for_persistence,
    resolve_po_print_format_for_render,
    purchase_normalize_email,
    purchase_is_valid_email,
    send_graph_mail_with_attachment,
    build_po_supplier_dispatch_email_body,
    build_po_cancellation_email_body,
    normalize_po_header_for_audit,
    diff_simple_fields,
    diff_po_items,
    ensure_item_masters_for_po,
    sync_purchase_po_valuation_rows,
    po_val_should_sync,
    po_val_sync_key,
    po_val_sync_cache_put,
    fetch_purchasing_departments,
    format_indian_currency,
    is_truthy,
    otp_po_request_type,
    po_valuation_start_date,
    export_executor,
    po_val_sync_cache,
    po_val_sync_lock,
    local_tz,
):
    """Register Purchase PO and valuation routes."""
    _allowed_purchase_units_for_session = allowed_purchase_units_for_session
    _get_purchase_unit = get_purchase_unit
    _clean_df_columns = clean_df_columns
    _sanitize_json_payload = sanitize_json_payload
    _safe_float = safe_float
    _safe_int = safe_int
    _get_login_db_connection = get_login_db_connection
    _audit_log_event = audit_log_event
    _saved_po_print_format = saved_po_print_format
    _resolve_purchasing_department_name = resolve_purchasing_department_name
    _fetch_latest_purchase_otp_request = fetch_latest_purchase_otp_request
    _fetch_purchase_approval_meta = fetch_purchase_approval_meta
    _ensure_purchase_po_valuation_table = ensure_purchase_po_valuation_table
    _export_cache_get_bytes = export_cache_get_bytes
    _export_cache_put_bytes = export_cache_put_bytes
    _excel_job_get = excel_job_get
    _excel_job_update = excel_job_update
    _create_purchase_otp_request = create_purchase_otp_request
    _validate_purchase_otp = validate_purchase_otp
    _insert_purchase_otp_request = insert_purchase_otp_request
    _mark_purchase_otp_used = mark_purchase_otp_used
    _mark_central_otp_used = mark_central_otp_used
    _fetch_purchase_otp_request_by_id = fetch_purchase_otp_request_by_id
    _send_purchase_po_approval_email = send_purchase_po_approval_email
    _build_po_pdf_buffer = build_po_pdf_buffer
    _resolve_po_print_format_for_persistence = resolve_po_print_format_for_persistence
    _resolve_po_print_format_for_render = resolve_po_print_format_for_render
    _purchase_normalize_email = purchase_normalize_email
    _purchase_is_valid_email = purchase_is_valid_email
    _send_graph_mail_with_attachment = send_graph_mail_with_attachment
    _build_po_supplier_dispatch_email_body = build_po_supplier_dispatch_email_body
    _build_po_cancellation_email_body = build_po_cancellation_email_body
    _normalize_po_header_for_audit = normalize_po_header_for_audit
    _diff_simple_fields = diff_simple_fields
    _diff_po_items = diff_po_items
    _ensure_item_masters_for_po = ensure_item_masters_for_po
    _sync_purchase_po_valuation_rows = sync_purchase_po_valuation_rows
    _po_val_should_sync = po_val_should_sync
    _po_val_sync_key = po_val_sync_key
    _po_val_sync_cache_put = po_val_sync_cache_put
    _fetch_purchasing_departments = fetch_purchasing_departments
    _format_indian_currency = format_indian_currency
    _is_truthy = is_truthy
    OTP_PO_REQUEST_TYPE = otp_po_request_type
    PO_VALUATION_START_DATE = po_valuation_start_date
    EXPORT_EXECUTOR = export_executor
    PO_VAL_SYNC_CACHE = po_val_sync_cache
    PO_VAL_SYNC_LOCK = po_val_sync_lock
    LOCAL_TZ = local_tz
    PO_CANCELLATION_INTERNAL_RECIPIENTS = ("accounts@asarfihospital.com", "us@asarfihospital.com")

    def _find_duplicate_purchase_item_refs(
        items: list,
        *,
        id_keys: tuple[str, ...] = ('item_id', 'id', 'ItemID', 'ItemId'),
        name_keys: tuple[str, ...] = ('item_name', 'name', 'ItemName'),
        qty_keys: tuple[str, ...] = ('qty', 'Qty', 'item_qty', 'ItemQty'),
        require_positive_qty: bool = False,
    ) -> dict:
        duplicate_ids = []
        duplicate_names = []
        seen_ids = set()
        seen_names = set()

        for raw_item in (items or []):
            item = raw_item or {}
            item_id = 0
            for key in id_keys:
                try:
                    item_id = int(item.get(key) or 0)
                except Exception:
                    item_id = 0
                if item_id:
                    break

            qty = 0.0
            for key in qty_keys:
                if key in item:
                    try:
                        qty = float(item.get(key) or 0)
                    except Exception:
                        qty = 0.0
                    break
            if require_positive_qty and qty <= 0:
                continue

            if item_id > 0:
                if item_id in seen_ids and item_id not in duplicate_ids:
                    duplicate_ids.append(item_id)
                seen_ids.add(item_id)
                continue

            name_raw = ''
            for key in name_keys:
                if key in item and item.get(key) is not None:
                    name_raw = str(item.get(key) or '').strip()
                    if name_raw:
                        break
            name_key = name_raw.lower()
            if not name_key:
                continue
            if name_key in seen_names and name_raw not in duplicate_names:
                duplicate_names.append(name_raw)
            seen_names.add(name_key)

        return {'duplicate_ids': duplicate_ids, 'duplicate_names': duplicate_names}

    def _default_po_terms_for_unit(unit_name: str) -> tuple[str, str]:
        delivery = "Deliveries shall be effected strictly between 9:00 AM and 6:00 PM."
        payment_default = "\n".join([
            "Billing shall strictly comply with the PO-approved quantity and rate.",
            "Bonuses, discounts, and rates shall be only as commercially approved with the authorized representative.",
        ])
        payment_store = "\n".join([
            "Billing shall strictly comply with the PO-approved quantity and rate.",
            "Commercial terms, including discounts and bonuses, shall be only as approved with the authorized representative.",
        ])
        unit_key = str(unit_name or "").strip().upper().replace(" ", "")
        if unit_key in {"AHLSTORE", "CANCERUNITSTORE", "BALLIASTORE"}:
            return delivery, payment_store
        return delivery, payment_default

    def _apply_default_po_terms(unit_name: str, header: dict, existing_header: dict | None = None) -> dict:
        normalized = dict(header or {})
        existing_header = existing_header or {}
        saved_delivery = str(
            existing_header.get("DeliveryTerms")
            or existing_header.get("delivery_terms")
            or ""
        ).strip()
        saved_payment = str(
            existing_header.get("PaymentsTerms")
            or existing_header.get("payment_terms")
            or ""
        ).strip()
        current_delivery = str(normalized.get("delivery_terms") or "").strip()
        current_payment = str(normalized.get("payment_terms") or "").strip()
        default_delivery, default_payment = _default_po_terms_for_unit(unit_name)
        if not current_delivery:
            normalized["delivery_terms"] = saved_delivery or default_delivery
        if not current_payment:
            normalized["payment_terms"] = saved_payment or default_payment
        return normalized


    def _format_duplicate_item_labels(items: list, duplicate_ids: list[int]) -> list[str]:
        labels = []
        for dup_id in duplicate_ids or []:
            name = ''
            for item in items or []:
                try:
                    item_id = int((item or {}).get('item_id') or (item or {}).get('id') or 0)
                except Exception:
                    item_id = 0
                if item_id != dup_id:
                    continue
                name = str((item or {}).get('item_name') or (item or {}).get('name') or '').strip()
                if name:
                    break
            labels.append(f'{dup_id} ({name})' if name else str(dup_id))
        return labels

    def _purchase_item_row_has_input(item: dict) -> bool:
        row = item or {}
        item_id = _safe_int(row.get("item_id") or row.get("id") or row.get("ItemID") or row.get("ItemId"))
        if item_id > 0:
            return True
        name = str(row.get("item_name") or row.get("name") or row.get("ItemName") or "").strip()
        if name:
            return True
        numeric_values = (
            row.get("qty"),
            row.get("Qty"),
            row.get("free_qty"),
            row.get("FreeQty"),
            row.get("rate"),
            row.get("Rate"),
            row.get("discount_pct"),
            row.get("Discount"),
            row.get("mrp"),
            row.get("MRP"),
            row.get("gst_pct"),
            row.get("cgst_pct"),
            row.get("sgst_pct"),
            row.get("igst_pct"),
            row.get("for_amt"),
            row.get("net_amt"),
        )
        for value in numeric_values:
            try:
                if abs(float(value or 0)) > 0:
                    return True
            except Exception:
                continue
        return False

    def _normalize_purchase_item_rows(items: list) -> tuple[list, list[str]]:
        normalized = []
        errors = []
        for row_no, raw_item in enumerate(items or [], start=1):
            item = dict(raw_item or {})
            item_id = _safe_int(item.get("item_id") or item.get("id") or item.get("ItemID") or item.get("ItemId"))
            name = str(item.get("item_name") or item.get("name") or item.get("ItemName") or "").strip()
            item_code = str(item.get("item_code") or item.get("store_name") or item.get("ItemCode") or "").strip()
            unit_name = str(item.get("unit") or item.get("UnitName") or "").strip()
            item["item_id"] = item_id
            item["item_name"] = name
            item["item_code"] = item_code
            item["unit"] = unit_name
            item["_source_row_no"] = row_no
            if not _purchase_item_row_has_input(item):
                continue
            if item_id <= 0 and not name:
                errors.append(f"Row {row_no}: item name is required.")
                continue
            normalized.append(item)
        return normalized, errors

    def _fetch_existing_purchase_item_ids(unit: str, item_ids: list[int]) -> set[int]:
        normalized_ids = sorted({_safe_int(item_id) for item_id in (item_ids or []) if _safe_int(item_id) > 0})
        if not normalized_ids:
            return set()
        df = data_fetch.fetch_item_master_rate_mrp(unit, normalized_ids)
        if df is None or df.empty:
            return set()
        df = _clean_df_columns(df)
        cols = {str(c).strip().lower(): c for c in df.columns}
        id_col = cols.get("itemid") or cols.get("id")
        existing_ids = set()
        if not id_col:
            return existing_ids
        for _, row in df.iterrows():
            item_id = _safe_int(row.get(id_col))
            if item_id > 0:
                existing_ids.add(item_id)
        return existing_ids

    def _normalize_invalid_purchase_item_refs(unit: str, items: list) -> list[str]:
        existing_ids = _fetch_existing_purchase_item_ids(
            unit,
            [_safe_int((item or {}).get("item_id")) for item in (items or [])],
        )
        errors = []
        for item in items or []:
            item_id = _safe_int((item or {}).get("item_id"))
            if item_id <= 0 or item_id in existing_ids:
                continue
            row_no = _safe_int((item or {}).get("_source_row_no"))
            name = str((item or {}).get("item_name") or "").strip()
            if name:
                item["item_id"] = 0
                continue
            errors.append(f"Row {row_no}: item reference {item_id} is not available in Item Master.")
        return errors

    def _find_missing_purchase_item_refs(unit: str, items: list) -> list[str]:
        existing_ids = _fetch_existing_purchase_item_ids(
            unit,
            [_safe_int((item or {}).get("item_id")) for item in (items or [])],
        )
        errors = []
        for item in items or []:
            item_id = _safe_int((item or {}).get("item_id"))
            if item_id <= 0 or item_id in existing_ids:
                continue
            row_no = _safe_int((item or {}).get("_source_row_no"))
            name = str((item or {}).get("item_name") or "").strip()
            label = f" ({name})" if name else ""
            errors.append(f"Row {row_no}{label}: item reference {item_id} could not be verified in Item Master.")
        return errors

    def _po_status_label(status_code) -> str:
        code = str(status_code or "").strip().upper()
        return {
            "D": "Draft",
            "P": "Pending Approval",
            "A": "Approved",
            "C": "Cancelled",
        }.get(code, "Draft")

    def _is_it_user() -> bool:
        return str(session.get("role") or "").strip().upper() == "IT"

    def _format_snapshot_date(value) -> str:
        if isinstance(value, datetime):
            return value.strftime("%d-%b-%Y %I:%M %p")
        if isinstance(value, date):
            return value.strftime("%d-%b-%Y")
        return str(value or "").strip()[:19]

    def _format_snapshot_qty(value) -> str:
        qty_val = _safe_float(value)
        return f"{qty_val:.3f}".rstrip("0").rstrip(".")

    def _build_po_cancellation_snapshot_text_bytes(
        unit: str,
        header_row: dict,
        items: list[dict],
        *,
        cancelled_by: str,
        cancelled_at: datetime,
    ) -> bytes:
        po_no_val = str(header_row.get("PONo") or f"PO-{_safe_int(header_row.get('ID'))}").strip()
        purchasing_dept_name = _resolve_purchasing_department_name(_safe_int(header_row.get("PurchasingDeptId"))) or ""
        lines = [
            "Purchase Order Cancellation Snapshot",
            "===================================",
            f"Unit: {unit}",
            f"PO No: {po_no_val}",
            f"PO Date: {_format_snapshot_date(header_row.get('PODate'))}",
            "Status: Cancelled",
            f"Cancelled By: {cancelled_by or '-'}",
            f"Cancelled At: {_format_snapshot_date(cancelled_at)}",
            f"Supplier: {str(header_row.get('SupplierName') or '').strip() or '-'}",
            f"Supplier Email: {str(header_row.get('SupplierEmail') or '').strip() or '-'}",
            f"Purchasing Dept: {purchasing_dept_name or '-'}",
            f"Reference No: {str(header_row.get('RefNo') or '').strip() or '-'}",
            f"Subject: {str(header_row.get('Subject') or '').strip() or '-'}",
            f"Amount: {_format_indian_currency(header_row.get('Amount') or 0)}",
            "",
            "Items",
            "-----",
        ]
        if not items:
            lines.append("No items found.")
        for idx, row in enumerate(items or [], start=1):
            item_name = str(row.get("ItemName") or row.get("item_name") or "").strip() or f"Item {idx}"
            pack_name = str(row.get("PackSizeName") or row.get("pack_size_name") or row.get("PackSizeId") or "").strip()
            unit_name = str(row.get("UnitName") or row.get("unit") or "").strip()
            qty = _format_snapshot_qty(row.get("Qty") or row.get("qty"))
            rate = _format_indian_currency(row.get("Rate") or row.get("rate") or 0)
            net_amt = _format_indian_currency(row.get("NetAmount") or row.get("net_amt") or 0)
            line = f"{idx}. {item_name} | Qty: {qty or '0'} | Rate: {rate} | Net: {net_amt}"
            if pack_name:
                line += f" | Pack: {pack_name}"
            if unit_name:
                line += f" | Unit: {unit_name}"
            lines.append(line)
        return "\r\n".join(lines).encode("utf-8")

    def _build_po_cancellation_snapshot_image_bytes(
        unit: str,
        header_row: dict,
        items: list[dict],
        *,
        cancelled_by: str,
        cancelled_at: datetime,
    ) -> bytes:
        try:
            from PIL import Image, ImageDraw, ImageFont
        except Exception:
            return b""

        import os

        po_no_val = str(header_row.get("PONo") or f"PO-{_safe_int(header_row.get('ID'))}").strip()
        purchasing_dept_name = _resolve_purchasing_department_name(_safe_int(header_row.get("PurchasingDeptId"))) or "-"
        supplier_name = str(header_row.get("SupplierName") or "").strip() or "-"
        supplier_email = str(header_row.get("SupplierEmail") or "").strip() or "-"
        subject_val = str(header_row.get("Subject") or "").strip() or "-"
        ref_no_val = str(header_row.get("RefNo") or "").strip() or "-"
        amount_val = _format_indian_currency(header_row.get("Amount") or 0)

        info_rows = [
            f"Unit: {unit}",
            f"PO No: {po_no_val}",
            f"PO Date: {_format_snapshot_date(header_row.get('PODate'))}",
            "Status: Cancelled",
            f"Cancelled By: {cancelled_by or '-'}",
            f"Cancelled At: {_format_snapshot_date(cancelled_at)}",
            f"Supplier: {supplier_name}",
            f"Supplier Email: {supplier_email}",
            f"Purchasing Dept: {purchasing_dept_name}",
            f"Reference No: {ref_no_val}",
            f"Amount: {amount_val}",
            f"Subject: {subject_val}",
        ]

        item_lines = []
        for idx, row in enumerate(items or [], start=1):
            item_name = str(row.get("ItemName") or row.get("item_name") or "").strip() or f"Item {idx}"
            pack_name = str(row.get("PackSizeName") or row.get("pack_size_name") or row.get("PackSizeId") or "").strip()
            unit_name = str(row.get("UnitName") or row.get("unit") or "").strip()
            qty = _format_snapshot_qty(row.get("Qty") or row.get("qty")) or "0"
            rate = _format_indian_currency(row.get("Rate") or row.get("rate") or 0)
            net_amt = _format_indian_currency(row.get("NetAmount") or row.get("net_amt") or 0)
            line = f"{idx}. {item_name} | Qty: {qty} | Rate: {rate} | Net: {net_amt}"
            if pack_name:
                line += f" | Pack: {pack_name}"
            if unit_name:
                line += f" | Unit: {unit_name}"
            item_lines.append(line)
        if not item_lines:
            item_lines.append("No items found.")
        elif len(item_lines) > 12:
            extra_count = len(item_lines) - 12
            item_lines = item_lines[:12] + [f"... and {extra_count} more item(s)."]

        width = 1600
        margin = 58
        content_width = width - (margin * 2)

        def _load_font(size: int, *, bold: bool = False):
            windir = os.environ.get("WINDIR") or r"C:\Windows"
            candidates = []
            if bold:
                candidates.extend(
                    [
                        os.path.join(windir, "Fonts", "arialbd.ttf"),
                        os.path.join(windir, "Fonts", "calibrib.ttf"),
                        "arialbd.ttf",
                    ]
                )
            else:
                candidates.extend(
                    [
                        os.path.join(windir, "Fonts", "arial.ttf"),
                        os.path.join(windir, "Fonts", "calibri.ttf"),
                        "arial.ttf",
                    ]
                )
            candidates.extend(["DejaVuSans-Bold.ttf" if bold else "DejaVuSans.ttf"])
            for candidate in candidates:
                try:
                    return ImageFont.truetype(candidate, size=size)
                except Exception:
                    continue
            return ImageFont.load_default()

        title_font = _load_font(46, bold=True)
        sub_font = _load_font(24, bold=False)
        badge_font = _load_font(22, bold=True)
        section_font = _load_font(24, bold=True)
        body_font = _load_font(22, bold=False)
        footer_font = _load_font(18, bold=False)

        probe = Image.new("RGB", (width, 200), "#ffffff")
        probe_draw = ImageDraw.Draw(probe)

        def _line_height(font) -> int:
            bbox = probe_draw.textbbox((0, 0), "Ag", font=font)
            return max(26, (bbox[3] - bbox[1]) + 8)

        def _wrap_text(text: str, font, max_width: int) -> list[str]:
            words = str(text or "").split()
            if not words:
                return [""]
            lines = []
            current = words[0]
            for word in words[1:]:
                trial = f"{current} {word}"
                bbox = probe_draw.textbbox((0, 0), trial, font=font)
                if (bbox[2] - bbox[0]) <= max_width:
                    current = trial
                else:
                    lines.append(current)
                    current = word
            lines.append(current)
            return lines

        info_line_height = _line_height(body_font)
        item_line_height = _line_height(body_font)
        footer_line_height = _line_height(footer_font)

        wrapped_info = []
        for row in info_rows:
            wrapped_info.extend(_wrap_text(row, body_font, content_width))

        wrapped_items = []
        for row in item_lines:
            wrapped_items.extend(_wrap_text(row, body_font, content_width))

        header_height = 164
        section_gap = 26
        footer_height = 72
        total_height = (
            header_height
            + (len(wrapped_info) * info_line_height)
            + section_gap
            + 42
            + (len(wrapped_items) * item_line_height)
            + footer_height
            + margin
        )
        total_height = max(total_height, 950)

        image = Image.new("RGB", (width, total_height), "#fffdfd")
        draw = ImageDraw.Draw(image)
        draw.rectangle([(0, 0), (width, header_height)], fill="#991b1b")
        draw.rectangle([(0, header_height), (width, total_height)], fill="#fffdfd")
        draw.text((margin, 34), "PURCHASE ORDER CANCELLED", font=title_font, fill="#ffffff")
        draw.text((margin, 92), "This PO stands cancelled. Attached PDF carries the cancellation watermark.", font=sub_font, fill="#fee2e2")
        badge_box = (width - 338, 34, width - 58, 92)
        draw.rectangle(badge_box, fill="#fecaca")
        draw.text((badge_box[0] + 24, badge_box[1] + 14), "STATUS: CANCELLED", font=badge_font, fill="#7f1d1d")

        y = header_height + 26
        for line in wrapped_info:
            draw.text((margin, y), line, font=body_font, fill="#111827")
            y += info_line_height

        y += section_gap
        draw.line((margin, y, width - margin, y), fill="#fecaca", width=3)
        y += 18
        draw.text((margin, y), "Items Snapshot", font=section_font, fill="#991b1b")
        y += 42

        for line in wrapped_items:
            draw.text((margin, y), line, font=body_font, fill="#1f2937")
            y += item_line_height

        footer_top = total_height - footer_height
        draw.rectangle([(0, footer_top), (width, total_height)], fill="#fef2f2")
        footer_text = f"Generated on {_format_snapshot_date(cancelled_at)} by {cancelled_by or '-'}"
        draw.text((margin, footer_top + 20), footer_text, font=footer_font, fill="#7f1d1d")

        out = io.BytesIO()
        image.save(out, format="JPEG", quality=90, optimize=True)
        return out.getvalue()

    @app.route('/api/purchase/po_lookup')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_purchase_po_lookup():
        unit, error = _get_purchase_unit()
        if error:
            return error
        try:
            data_fetch.ensure_po_cmc_amc_warranty_column(unit)
        except Exception:
            pass
        try:
            data_fetch.ensure_po_approval_date_column(unit)
        except Exception:
            pass
        query = (request.args.get("q") or "").strip()
        if not query:
            return jsonify({"status": "error", "message": "Please enter a PO number or ID"}), 400

        po_id = None
        po_no = None
        if query.isdigit():
            po_id = int(query)
        else:
            po_no = query
            if po_no.upper().startswith("PO-"):
                po_no = po_no.upper()

        header_df = data_fetch.fetch_purchase_po_header(unit, po_id=po_id, po_no=po_no)
        if header_df is None:
            return jsonify({"status": "error", "message": "Failed to fetch PO"}), 500
        if header_df.empty:
            return jsonify({"status": "error", "message": "PO not found"}), 404

        header_df = _clean_df_columns(header_df)
        header_row = header_df.iloc[0].to_dict()
        po_id_val = _safe_int(header_row.get("ID"))
        if po_id_val > 0 and not str(header_row.get("PONo") or "").strip():
            try:
                po_no_fix = data_fetch.ensure_purchase_po_number(unit, po_id_val, preferred_po_no=po_no)
                ensured_po_no = str(po_no_fix.get("po_no") or "").strip()
                if ensured_po_no:
                    header_row["PONo"] = ensured_po_no
            except Exception:
                pass
        status_code = str(header_row.get("Status") or "").strip().upper()
        status_label = _po_status_label(status_code)
        cmc_amc_warranty_notes = header_row.get("CmcAmcWarrantyNotes") or header_row.get("CMCAMCWarrantyNotes") or header_row.get("OtherTerms")

        header_payload = _sanitize_json_payload({
            "po_id": header_row.get("ID"),
            "po_no": header_row.get("PONo"),
            "po_date": header_row.get("PODate"),
            "po_approval_date": header_row.get("POApprovalDate"),
            "supplier_id": header_row.get("SupplierID"),
            "supplier_name": header_row.get("SupplierName"),
            "supplier_code": header_row.get("SupplierCode"),
            "supplier_email": header_row.get("SupplierEmail"),
            "supplier_gstin": header_row.get("SupplierGSTIN"),
            "ref_no": header_row.get("RefNo"),
            "subject": header_row.get("Subject"),
            "credit_days": header_row.get("CreditDays"),
            "notes": header_row.get("Notes"),
            "special_notes": header_row.get("SpecialNotes"),
            "senior_approval_authority_name": header_row.get("SeniorApprovalAuthorityName"),
            "senior_approval_authority_designation": header_row.get("SeniorApprovalAuthorityDesignation"),
            "prepared_by": header_row.get("Preparedby"),
            "delivery_terms": header_row.get("DeliveryTerms"),
            "payment_terms": header_row.get("PaymentsTerms"),
            "cmc_amc_warranty": cmc_amc_warranty_notes,
            "other_terms": header_row.get("OtherTerms") or cmc_amc_warranty_notes,
            "freight_charges": header_row.get("Custom1") or 0,
            "packing_charges": header_row.get("Custom2") or 0,
            "discount_total": header_row.get("Discount") or 0,
            "overall_discount_mode": header_row.get("OverallDiscountMode"),
            "overall_discount_value": header_row.get("OverallDiscountValue") or 0,
            "print_format": _saved_po_print_format(header_row),
            "against": header_row.get("Against"),
            "against_id": header_row.get("AgainstId"),
            "indent_id": header_row.get("PurchaseIndentId"),
            "purchasing_dept_id": header_row.get("PurchasingDeptId"),
            "purchasing_dept_name": _resolve_purchasing_department_name(_safe_int(header_row.get("PurchasingDeptId"))),
            "status_code": status_code or "D",
            "status_label": status_label,
        })


        if status_code == "P":
            otp_rec = _fetch_latest_purchase_otp_request(int(header_row.get("ID") or 0))
            if otp_rec:
                header_payload["otp_request_id"] = otp_rec.get("request_id")

        items_df = data_fetch.fetch_purchase_po_items(unit, po_id=header_row.get("ID"))
        if items_df is None:
            return jsonify({"status": "error", "message": "Failed to fetch PO items"}), 500
        items_payload = []
        if not items_df.empty:
            items_df = _clean_df_columns(items_df)
            for row in items_df.to_dict(orient="records"):
                items_payload.append(_sanitize_json_payload({
                    "detail_id": row.get("DetailID"),
                    "item_id": row.get("ItemID"),
                    "item_name": row.get("ItemName"),
                    "item_code": row.get("ItemCode"),
                    "store_name": row.get("StoreName"),
                    "unit": row.get("UnitName"),
                    "technical_specs": row.get("TechnicalSpecs"),
                    "pack_size_id": row.get("PackSizeId"),
                    "qty": row.get("Qty"),
                    "free_qty": row.get("FreeQty"),
                    "rate": row.get("Rate"),
                    "discount_pct": row.get("Discount"),
                    "mrp": row.get("MRP"),
                    "for_amt": row.get("Fore"),
                    "net_amt": row.get("NetAmount"),
                    "tax_pct": row.get("Tax"),
                    "tax_amt": row.get("TaxAmount"),
                    "excise_pct": row.get("Excisetax"),
                    "excise_amt": row.get("ExciseTaxamt"),
                    "unit_discount": row.get("UnitDiscount"),
                    "custom1": row.get("Custom1"),
                    "custom2": row.get("Custom2"),
                }))

        return jsonify({
            "status": "success",
            "header": header_payload,
            "items": items_payload,
            "unit": unit,
        })


    @app.route('/api/purchase/po_list')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_purchase_po_list():
        unit, error = _get_purchase_unit()
        if error:
            return error
        status = request.args.get("status") or "open"
        limit_raw = request.args.get("limit") or "200"
        query = request.args.get("q")
        item_query = request.args.get("item_q")
        try:
            limit = int(limit_raw)
        except Exception:
            limit = 200
        df = data_fetch.fetch_purchase_po_list(unit, status=status, limit=limit, query=query, item_query=item_query)
        if df is None:
            return jsonify({"status": "error", "message": "Failed to fetch PO list"}), 500
        if df.empty:
            return jsonify({"status": "success", "items": [], "unit": unit})
        df = _clean_df_columns(df)
        rows = _sanitize_json_payload(df.to_dict(orient="records"))
        return jsonify({"status": "success", "items": rows, "unit": unit})


    def _purchase_po_valuation_floor_date() -> date:
        try:
            return datetime.strptime(PO_VALUATION_START_DATE, "%Y-%m-%d").date()
        except Exception:
            return datetime.now(tz=LOCAL_TZ).date()


    def _purchase_po_valuation_parse_date(raw_value) -> date | None:
        raw = str(raw_value or "").strip()
        if not raw:
            return None
        for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%Y/%m/%d", "%m/%d/%Y", "%m-%d-%Y"):
            try:
                return datetime.strptime(raw, fmt).date()
            except Exception:
                continue
        try:
            parsed = pd.to_datetime(raw, errors="coerce", dayfirst=True)
            if pd.isna(parsed):
                return None
            return parsed.date()
        except Exception:
            return None


    def _purchase_po_valuation_month_end(target: date) -> date:
        start = date(target.year, target.month, 1)
        if target.month == 12:
            nxt = date(target.year + 1, 1, 1)
        else:
            nxt = date(target.year, target.month + 1, 1)
        return nxt - timedelta(days=1)


    def _purchase_po_valuation_shift_month(target: date, month_offset: int) -> date:
        month_index = (target.year * 12) + (target.month - 1) + int(month_offset or 0)
        year = month_index // 12
        month = (month_index % 12) + 1
        day = min(target.day, _purchase_po_valuation_month_end(date(year, month, 1)).day)
        return date(year, month, day)


    def _purchase_po_valuation_fy_start_year(target: date | None = None) -> int:
        target = target or datetime.now(tz=LOCAL_TZ).date()
        return target.year if target.month >= 4 else (target.year - 1)


    def _purchase_po_valuation_fy_label(start_year: int) -> str:
        return f"FY {start_year}-{str(start_year + 1)[-2:]}"


    def _purchase_po_valuation_parse_fy_start_year(raw_value, fallback_year: int) -> int:
        raw = str(raw_value or "").strip()
        if not raw:
            return int(fallback_year)
        match = re.search(r"(\d{4})", raw)
        if match:
            return _safe_int(match.group(1), fallback_year)
        return int(fallback_year)


    def _purchase_po_valuation_fiscal_quarter_bounds(start_year: int, quarter_no: int) -> tuple[date, date]:
        quarter_no = max(1, min(4, _safe_int(quarter_no, 1)))
        if quarter_no == 1:
            return date(start_year, 4, 1), date(start_year, 6, 30)
        if quarter_no == 2:
            return date(start_year, 7, 1), date(start_year, 9, 30)
        if quarter_no == 3:
            return date(start_year, 10, 1), date(start_year, 12, 31)
        return date(start_year + 1, 1, 1), date(start_year + 1, 3, 31)


    def _purchase_po_valuation_fiscal_quarter_for_date(target: date) -> tuple[int, int]:
        start_year = _purchase_po_valuation_fy_start_year(target)
        if 4 <= target.month <= 6:
            return start_year, 1
        if 7 <= target.month <= 9:
            return start_year, 2
        if 10 <= target.month <= 12:
            return start_year, 3
        return start_year, 4


    def _purchase_po_valuation_fiscal_quarter_label(start_year: int, quarter_no: int) -> str:
        return f"Q{quarter_no} {_purchase_po_valuation_fy_label(start_year)}"


    def _purchase_po_valuation_date_text(value) -> str:
        if isinstance(value, datetime):
            return value.date().isoformat()
        if isinstance(value, date):
            return value.isoformat()
        parsed = _purchase_po_valuation_parse_date(value)
        return parsed.isoformat() if parsed else ""


    def _purchase_po_valuation_pretty_date(value) -> str:
        parsed = _purchase_po_valuation_parse_date(value)
        return parsed.strftime("%d %b %Y") if parsed else "-"


    def _resolve_purchase_po_valuation_period(period_mode: str, period_value: str, from_raw: str, to_raw: str) -> dict:
        today = datetime.now(tz=LOCAL_TZ).date()
        floor_dt = _purchase_po_valuation_floor_date()
        mode = str(period_mode or "").strip().lower()
        if mode not in {"day", "month", "quarter", "year", "custom"}:
            mode = "custom" if (str(from_raw or "").strip() or str(to_raw or "").strip()) else "month"

        current_fy = _purchase_po_valuation_fy_start_year(today)
        period_token = ""
        label = ""
        trend_grain = "day"

        if mode == "day":
            selected = _purchase_po_valuation_parse_date(period_value or from_raw or today.isoformat()) or today
            from_dt = selected
            to_dt = selected
            prev_from_dt = selected - timedelta(days=1)
            prev_to_dt = prev_from_dt
            period_token = selected.isoformat()
            label = selected.strftime("%d %b %Y")
            quarter_fy = _purchase_po_valuation_fy_start_year(selected)
            quarter_no = _purchase_po_valuation_fiscal_quarter_for_date(selected)[1]
            year_fy = quarter_fy
        elif mode == "month":
            month_match = re.match(r"^\s*(\d{4})-(\d{2})\s*$", str(period_value or "").strip())
            if month_match:
                selected = date(_safe_int(month_match.group(1), today.year), _safe_int(month_match.group(2), today.month), 1)
            else:
                parsed_month = _purchase_po_valuation_parse_date(period_value or from_raw or today.isoformat()) or today
                selected = date(parsed_month.year, parsed_month.month, 1)
            from_dt = selected
            to_dt = _purchase_po_valuation_month_end(selected)
            prev_from_dt = _purchase_po_valuation_shift_month(selected, -1)
            prev_to_dt = _purchase_po_valuation_month_end(prev_from_dt)
            period_token = f"{selected.year:04d}-{selected.month:02d}"
            label = selected.strftime("%B %Y")
            quarter_fy, quarter_no = _purchase_po_valuation_fiscal_quarter_for_date(selected)
            year_fy = quarter_fy
        elif mode == "quarter":
            quarter_match = re.match(r"^\s*(\d{4})(?:-\d{2,4})?-FQ([1-4])\s*$", str(period_value or "").strip(), flags=re.IGNORECASE)
            if quarter_match:
                quarter_fy = _safe_int(quarter_match.group(1), current_fy)
                quarter_no = _safe_int(quarter_match.group(2), 1)
            else:
                quarter_fy, quarter_no = _purchase_po_valuation_fiscal_quarter_for_date(today)
            from_dt, to_dt = _purchase_po_valuation_fiscal_quarter_bounds(quarter_fy, quarter_no)
            if quarter_no == 1:
                prev_from_dt, prev_to_dt = _purchase_po_valuation_fiscal_quarter_bounds(quarter_fy - 1, 4)
            else:
                prev_from_dt, prev_to_dt = _purchase_po_valuation_fiscal_quarter_bounds(quarter_fy, quarter_no - 1)
            period_token = f"{quarter_fy}-FQ{quarter_no}"
            label = _purchase_po_valuation_fiscal_quarter_label(quarter_fy, quarter_no)
            year_fy = quarter_fy
            trend_grain = "month"
        elif mode == "year":
            year_fy = _purchase_po_valuation_parse_fy_start_year(period_value, current_fy)
            from_dt = date(year_fy, 4, 1)
            to_dt = date(year_fy + 1, 3, 31)
            prev_from_dt = date(year_fy - 1, 4, 1)
            prev_to_dt = date(year_fy, 3, 31)
            period_token = f"{year_fy}-{year_fy + 1}"
            label = _purchase_po_valuation_fy_label(year_fy)
            quarter_no = 1
            trend_grain = "fiscal_quarter"
        else:
            from_dt = _purchase_po_valuation_parse_date(from_raw or period_value or today.isoformat()) or today
            to_dt = _purchase_po_valuation_parse_date(to_raw or from_raw or period_value or today.isoformat()) or from_dt
            if to_dt < from_dt:
                to_dt = from_dt
            span_days = max(1, (to_dt - from_dt).days + 1)
            prev_to_dt = from_dt - timedelta(days=1)
            prev_from_dt = prev_to_dt - timedelta(days=span_days - 1)
            period_token = ""
            label = f"{from_dt.strftime('%d %b %Y')} to {to_dt.strftime('%d %b %Y')}"
            quarter_fy, quarter_no = _purchase_po_valuation_fiscal_quarter_for_date(from_dt)
            year_fy = quarter_fy
            trend_grain = "day" if span_days <= 62 else "month"

        if from_dt < floor_dt:
            from_dt = floor_dt
        if to_dt < from_dt:
            to_dt = from_dt

        comparison_available = True
        if prev_from_dt < floor_dt or prev_to_dt < floor_dt:
            comparison_available = False
            prev_from_dt = None
            prev_to_dt = None

        days_in_range = max(1, (to_dt - from_dt).days + 1)
        mode_labels = {
            "day": "Day",
            "month": "Month",
            "quarter": "Quarter",
            "year": "Year",
            "custom": "Custom Date Range",
        }

        return {
            "mode": mode,
            "mode_label": mode_labels.get(mode, "Month"),
            "value": period_token,
            "label": label,
            "from_date": from_dt,
            "to_date": to_dt,
            "from": from_dt.isoformat(),
            "to": to_dt.isoformat(),
            "from_display": from_dt.strftime("%d %b %Y"),
            "to_display": to_dt.strftime("%d %b %Y"),
            "range_label": f"{from_dt.strftime('%d %b %Y')} to {to_dt.strftime('%d %b %Y')}",
            "days_in_range": days_in_range,
            "previous_from_date": prev_from_dt,
            "previous_to_date": prev_to_dt,
            "previous_from": prev_from_dt.isoformat() if prev_from_dt else "",
            "previous_to": prev_to_dt.isoformat() if prev_to_dt else "",
            "previous_label": (
                f"{prev_from_dt.strftime('%d %b %Y')} to {prev_to_dt.strftime('%d %b %Y')}"
                if (prev_from_dt and prev_to_dt)
                else "No prior baseline"
            ),
            "comparison_available": comparison_available,
            "trend_grain": trend_grain,
            "available_from": floor_dt.isoformat(),
            "fiscal_year_start": year_fy,
            "fiscal_year_label": _purchase_po_valuation_fy_label(year_fy),
            "controls": {
                "day": from_dt.isoformat() if mode == "day" else today.isoformat(),
                "month": f"{from_dt.year:04d}-{from_dt.month:02d}" if mode == "month" else f"{today.year:04d}-{today.month:02d}",
                "quarter_fy": str(quarter_fy),
                "quarter": f"FQ{quarter_no}",
                "year_fy": str(year_fy),
                "custom_from": from_dt.isoformat(),
                "custom_to": to_dt.isoformat(),
            },
        }


    def _purchase_po_valuation_request_context(source=None):
        allowed_units = _allowed_purchase_units_for_session()
        if not allowed_units:
            return None, (jsonify({"status": "error", "message": "No unit access assigned"}), 403)

        src = source if source is not None else request.args

        def _src_value(key: str, default=""):
            if isinstance(src, dict):
                return src.get(key, default)
            try:
                return src.get(key, default)
            except Exception:
                return default

        requested_unit = str(_src_value("unit") or "").strip().upper()
        if requested_unit and requested_unit not in allowed_units:
            return None, (jsonify({"status": "error", "message": f"Unit {requested_unit} not permitted"}), 403)

        scope = str(_src_value("scope") or "").strip().lower()
        if scope not in {"current_unit", "all_permitted_units"}:
            scope = "current_unit"

        current_unit = requested_unit or (allowed_units[0] if len(allowed_units) == 1 else "")
        if scope == "all_permitted_units":
            units_to_use = allowed_units[:]
        else:
            if not current_unit:
                return None, (jsonify({"status": "error", "message": "Please select a unit"}), 400)
            units_to_use = [current_unit]

        try:
            period_meta = _resolve_purchase_po_valuation_period(
                str(_src_value("period_mode") or "").strip(),
                str(_src_value("period_value") or "").strip(),
                str(_src_value("from") or "").strip(),
                str(_src_value("to") or "").strip(),
            )
        except Exception as exc:
            return None, (jsonify({"status": "error", "message": str(exc) or "Invalid period selection"}), 400)

        return {
            "allowed_units": allowed_units,
            "requested_unit": requested_unit,
            "current_unit": current_unit,
            "scope": scope,
            "units_to_use": units_to_use,
            "purchasing_dept_id": _safe_int(_src_value("purchasing_dept_id"), 0),
            "store_filter": str(_src_value("store_name") or "").strip(),
            "force_sync": str(_src_value("sync") or "").strip().lower() in {"1", "true", "yes", "y"},
            "period_meta": period_meta,
        }, None


    def _purchase_po_valuation_query_line_rows(
        units_to_use: list[str],
        from_iso: str,
        to_iso: str,
        purchasing_dept_id: int = 0,
    ) -> pd.DataFrame:
        columns = [
            "Unit",
            "POID",
            "PONo",
            "PODate",
            "StoreName",
            "SupplierName",
            "PurchasingDeptId",
            "PurchasingDeptName",
            "DeptName",
            "CategoryName",
            "SubCategoryName",
            "Qty",
            "LineValue",
        ]
        if not units_to_use:
            return pd.DataFrame(columns=columns)

        with _get_login_db_connection() as conn:
            _ensure_purchase_po_valuation_table(conn)
            placeholders = ",".join("?" * len(units_to_use))
            where_clauses = [
                "PODate >= ?",
                "PODate <= ?",
                f"Unit IN ({placeholders})",
                "ISNULL(LTRIM(RTRIM(Status)), '') = 'A'",
            ]
            params = [from_iso, to_iso] + list(units_to_use)
            if purchasing_dept_id > 0:
                where_clauses.append("ISNULL(PurchasingDeptId, 0) = ?")
                params.append(purchasing_dept_id)
            sql = f"""
                SELECT
                    UPPER(LTRIM(RTRIM(Unit))) AS Unit,
                    ISNULL(POID, 0) AS POID,
                    LTRIM(RTRIM(ISNULL(PONo, ''))) AS PONo,
                    PODate,
                    COALESCE(NULLIF(LTRIM(RTRIM(StoreName)), ''), 'Unknown') AS StoreName,
                    COALESCE(NULLIF(LTRIM(RTRIM(SupplierName)), ''), 'Unknown') AS SupplierName,
                    ISNULL(PurchasingDeptId, 0) AS PurchasingDeptId,
                    COALESCE(
                        NULLIF(LTRIM(RTRIM(PurchasingDeptName)), ''),
                        CASE
                            WHEN ISNULL(PurchasingDeptId, 0) > 0
                                THEN CONCAT('Dept ', CONVERT(NVARCHAR(20), PurchasingDeptId))
                            ELSE 'Unmapped'
                        END
                    ) AS PurchasingDeptName,
                    COALESCE(NULLIF(LTRIM(RTRIM(DeptName)), ''), 'Unknown') AS DeptName,
                    COALESCE(NULLIF(LTRIM(RTRIM(CategoryName)), ''), 'Unknown') AS CategoryName,
                    COALESCE(NULLIF(LTRIM(RTRIM(SubCategoryName)), ''), 'Unknown') AS SubCategoryName,
                    CAST(ISNULL(Qty, 0) AS FLOAT) AS Qty,
                    CAST(ISNULL(LineValue, 0) AS FLOAT) AS LineValue
                FROM dbo.HID_Purchase_PO_Valuation
                WHERE {" AND ".join(where_clauses)}
                ORDER BY PODate DESC, POID DESC, StoreName, SupplierName
            """
            df = pd.read_sql(sql, conn, params=params)

        if df is None or df.empty:
            return pd.DataFrame(columns=columns)

        df = _clean_df_columns(df)
        for col, fallback in {
            "Unit": "",
            "PONo": "",
            "StoreName": "Unknown",
            "SupplierName": "Unknown",
            "PurchasingDeptName": "Unmapped",
            "DeptName": "Unknown",
            "CategoryName": "Unknown",
            "SubCategoryName": "Unknown",
        }.items():
            if col not in df.columns:
                df[col] = fallback
            df[col] = df[col].apply(lambda value, fb=fallback: (str(value or "").strip() or fb))

        for col in ["POID", "PurchasingDeptId"]:
            if col not in df.columns:
                df[col] = 0
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

        for col in ["Qty", "LineValue"]:
            if col not in df.columns:
                df[col] = 0.0
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0).astype(float)

        if "PODate" not in df.columns:
            df["PODate"] = None
        df["PODate"] = pd.to_datetime(df["PODate"], errors="coerce").dt.date
        df = df[df["POID"] > 0].copy()
        if df.empty:
            return pd.DataFrame(columns=columns)

        df["POKey"] = df["Unit"].astype(str) + "::" + df["POID"].astype(str)
        df["StoreNameUpper"] = df["StoreName"].astype(str).str.upper()
        return df


    def _purchase_po_valuation_group_rows(df: pd.DataFrame, group_cols: list[str]) -> list[dict]:
        if df is None or df.empty:
            return []
        grouped = (
            df.groupby(group_cols, dropna=False)
            .agg(
                POCount=("POKey", "nunique"),
                TotalValue=("LineValue", "sum"),
                TotalQty=("Qty", "sum"),
                LastPODate=("PODate", "max"),
            )
            .reset_index()
        )
        grouped["AvgValuePerPO"] = grouped.apply(
            lambda row: (_safe_float(row.get("TotalValue")) / _safe_int(row.get("POCount"), 0))
            if _safe_int(row.get("POCount"), 0) > 0
            else 0.0,
            axis=1,
        )
        grouped = grouped.sort_values(["TotalValue", "POCount"], ascending=[False, False], kind="stable")
        rows = []
        for _, row in grouped.iterrows():
            entry = {}
            for col in group_cols:
                entry[col] = _safe_int(row.get(col), 0) if col == "PurchasingDeptId" else row.get(col)
            entry["POCount"] = _safe_int(row.get("POCount"), 0)
            entry["TotalValue"] = round(_safe_float(row.get("TotalValue")), 2)
            entry["TotalQty"] = round(_safe_float(row.get("TotalQty")), 3)
            entry["AvgValuePerPO"] = round(_safe_float(row.get("AvgValuePerPO")), 2)
            entry["LastPODate"] = _purchase_po_valuation_date_text(row.get("LastPODate"))
            rows.append(entry)
        return rows


    def _purchase_po_valuation_store_options(df: pd.DataFrame) -> list[dict]:
        if df is None or df.empty:
            return []
        grouped = (
            df.groupby(["StoreName"], dropna=False)
            .agg(TotalValue=("LineValue", "sum"))
            .reset_index()
            .sort_values(["TotalValue", "StoreName"], ascending=[False, True], kind="stable")
        )
        return [
            {
                "StoreName": row.get("StoreName"),
                "TotalValue": round(_safe_float(row.get("TotalValue")), 2),
            }
            for _, row in grouped.iterrows()
        ]


    def _purchase_po_valuation_detail_rows(df: pd.DataFrame) -> list[dict]:
        if df is None or df.empty:
            return []
        detail_rows = []
        group_cols = [
            "Unit",
            "POKey",
            "POID",
            "PONo",
            "PODate",
            "SupplierName",
            "PurchasingDeptId",
            "PurchasingDeptName",
        ]
        for group_key, group_df in df.groupby(group_cols, dropna=False, sort=False):
            unit, _, poid, po_no, po_date, supplier_name, purchasing_dept_id, purchasing_dept_name = group_key
            stores = sorted({str(value or "").strip() for value in group_df["StoreName"].tolist() if str(value or "").strip()})
            depts = sorted({str(value or "").strip() for value in group_df["DeptName"].tolist() if str(value or "").strip() and str(value or "").strip().lower() != "unknown"})
            categories = sorted({str(value or "").strip() for value in group_df["CategoryName"].tolist() if str(value or "").strip() and str(value or "").strip().lower() != "unknown"})
            subcategories = sorted({str(value or "").strip() for value in group_df["SubCategoryName"].tolist() if str(value or "").strip() and str(value or "").strip().lower() != "unknown"})
            store_display = stores[0] if len(stores) == 1 else (f"Multiple ({len(stores)})" if stores else "Unknown")
            detail_rows.append(
                {
                    "POID": _safe_int(poid, 0),
                    "PONo": str(po_no or "").strip(),
                    "PODate": _purchase_po_valuation_date_text(po_date),
                    "Unit": str(unit or "").strip(),
                    "StoreName": store_display,
                    "StoreList": ", ".join(stores) if stores else store_display,
                    "StoreCount": len(stores) if stores else 1,
                    "PurchasingDeptId": _safe_int(purchasing_dept_id, 0),
                    "PurchasingDeptName": str(purchasing_dept_name or "").strip() or "Unmapped",
                    "SupplierName": str(supplier_name or "").strip() or "Unknown",
                    "Qty": round(_safe_float(group_df["Qty"].sum()), 3),
                    "GrossAmount": round(_safe_float(group_df["LineValue"].sum()), 2),
                    "DeptNames": ", ".join(depts) if depts else "-",
                    "CategoryNames": ", ".join(categories) if categories else "-",
                    "SubCategoryNames": ", ".join(subcategories) if subcategories else "-",
                }
            )
        detail_rows.sort(
            key=lambda row: (
                -_safe_float(row.get("GrossAmount")),
                str(row.get("PODate") or ""),
                str(row.get("PONo") or ""),
            )
        )
        return detail_rows


    def _purchase_po_valuation_bucket_specs(from_dt: date, to_dt: date, grain: str) -> list[dict]:
        specs = []
        if grain == "fiscal_quarter":
            fy_start, quarter_no = _purchase_po_valuation_fiscal_quarter_for_date(from_dt)
            bucket_start, bucket_end = _purchase_po_valuation_fiscal_quarter_bounds(fy_start, quarter_no)
            while bucket_start <= to_dt:
                specs.append(
                    {
                        "key": f"{fy_start}-FQ{quarter_no}",
                        "label": _purchase_po_valuation_fiscal_quarter_label(fy_start, quarter_no),
                        "start": bucket_start,
                        "end": bucket_end,
                    }
                )
                if quarter_no == 4:
                    fy_start += 1
                    quarter_no = 1
                else:
                    quarter_no += 1
                bucket_start, bucket_end = _purchase_po_valuation_fiscal_quarter_bounds(fy_start, quarter_no)
            return specs

        if grain == "month":
            current = date(from_dt.year, from_dt.month, 1)
            while current <= to_dt:
                specs.append(
                    {
                        "key": f"{current.year:04d}-{current.month:02d}",
                        "label": current.strftime("%b %Y"),
                        "start": current,
                        "end": _purchase_po_valuation_month_end(current),
                    }
                )
                current = _purchase_po_valuation_shift_month(current, 1)
            return specs

        current = from_dt
        while current <= to_dt:
            specs.append(
                {
                    "key": current.isoformat(),
                    "label": current.strftime("%d %b"),
                    "start": current,
                    "end": current,
                }
            )
            current += timedelta(days=1)
        return specs


    def _purchase_po_valuation_bucket_key(target: date, grain: str) -> str:
        if grain == "fiscal_quarter":
            fy_start, quarter_no = _purchase_po_valuation_fiscal_quarter_for_date(target)
            return f"{fy_start}-FQ{quarter_no}"
        if grain == "month":
            return f"{target.year:04d}-{target.month:02d}"
        return target.isoformat()


    def _purchase_po_valuation_trend_rows(df: pd.DataFrame, period_meta: dict) -> list[dict]:
        from_dt = period_meta.get("from_date")
        to_dt = period_meta.get("to_date")
        grain = str(period_meta.get("trend_grain") or "day")
        if not isinstance(from_dt, date) or not isinstance(to_dt, date):
            return []
        specs = _purchase_po_valuation_bucket_specs(from_dt, to_dt, grain)
        if not specs:
            return []
        totals = {}
        if df is not None and not df.empty:
            for _, row in df.iterrows():
                po_date = row.get("PODate")
                if not isinstance(po_date, date):
                    continue
                key = _purchase_po_valuation_bucket_key(po_date, grain)
                entry = totals.setdefault(key, {"value": 0.0, "po_keys": set()})
                entry["value"] += _safe_float(row.get("LineValue"))
                entry["po_keys"].add(str(row.get("POKey") or ""))
        trend_rows = []
        for spec in specs:
            entry = totals.get(spec["key"], {"value": 0.0, "po_keys": set()})
            trend_rows.append(
                {
                    "BucketKey": spec["key"],
                    "BucketLabel": spec["label"],
                    "BucketStart": spec["start"].isoformat(),
                    "BucketEnd": min(spec["end"], to_dt).isoformat(),
                    "POCount": len([po_key for po_key in entry["po_keys"] if po_key]),
                    "TotalValue": round(_safe_float(entry["value"]), 2),
                }
            )
        return trend_rows


    def _build_purchase_po_valuation_payload(resolved_context: dict, status_callback=None) -> dict:
        def _notify(stage: str, message: str):
            if callable(status_callback):
                try:
                    status_callback(stage, message)
                except Exception:
                    pass

        units_to_use = list(resolved_context.get("units_to_use") or [])
        purchasing_dept_id = _safe_int(resolved_context.get("purchasing_dept_id"), 0)
        store_filter = str(resolved_context.get("store_filter") or "").strip()
        store_filter_upper = store_filter.upper()
        period_meta = dict(resolved_context.get("period_meta") or {})
        sync_from = str(period_meta.get("from") or "")
        if period_meta.get("comparison_available") and str(period_meta.get("previous_from") or ""):
            sync_from = min(sync_from, str(period_meta.get("previous_from") or ""))

        did_sync = False
        cache_ts = None
        cache_age_seconds = None
        cache_key = _po_val_sync_key(units_to_use)
        with PO_VAL_SYNC_LOCK:
            cache_entry = PO_VAL_SYNC_CACHE.get(cache_key)
        if cache_entry:
            cache_ts = cache_entry.get("ts")
            if cache_ts:
                cache_age_seconds = max(0, int(time.time() - float(cache_ts)))

        _notify("loading_data", "Loading filtered PO valuation data...")
        if _po_val_should_sync(units_to_use, sync_from, force=bool(resolved_context.get("force_sync"))):
            try:
                _sync_purchase_po_valuation_rows(units_to_use, sync_from)
                _po_val_sync_cache_put(units_to_use, sync_from)
                did_sync = True
                cache_ts = time.time()
                cache_age_seconds = 0
            except Exception as exc:
                print(f"PO valuation sync failed: {exc}")

        current_base_df = _purchase_po_valuation_query_line_rows(
            units_to_use,
            str(period_meta.get("from") or ""),
            str(period_meta.get("to") or ""),
            purchasing_dept_id=purchasing_dept_id,
        )
        if store_filter_upper and current_base_df is not None and not current_base_df.empty:
            current_df = current_base_df[current_base_df["StoreNameUpper"] == store_filter_upper].copy()
        else:
            current_df = current_base_df.copy() if current_base_df is not None else pd.DataFrame()

        previous_df = pd.DataFrame()
        if period_meta.get("comparison_available") and str(period_meta.get("previous_from") or "") and str(period_meta.get("previous_to") or ""):
            previous_base_df = _purchase_po_valuation_query_line_rows(
                units_to_use,
                str(period_meta.get("previous_from") or ""),
                str(period_meta.get("previous_to") or ""),
                purchasing_dept_id=purchasing_dept_id,
            )
            if store_filter_upper and previous_base_df is not None and not previous_base_df.empty:
                previous_df = previous_base_df[previous_base_df["StoreNameUpper"] == store_filter_upper].copy()
            else:
                previous_df = previous_base_df.copy() if previous_base_df is not None else pd.DataFrame()

        _notify("computing_summaries", "Computing KPI cards and ranked summaries...")
        unit_rows = _purchase_po_valuation_group_rows(current_df, ["Unit"])
        store_rows = _purchase_po_valuation_group_rows(current_df, ["Unit", "StoreName"])
        supplier_rows = _purchase_po_valuation_group_rows(current_df, ["Unit", "SupplierName"])
        purchasing_dept_rows = _purchase_po_valuation_group_rows(current_df, ["Unit", "PurchasingDeptId", "PurchasingDeptName"])
        dept_rows = _purchase_po_valuation_group_rows(current_df[current_df["DeptName"].str.lower() != "unknown"], ["Unit", "DeptName"]) if current_df is not None and not current_df.empty else []
        category_rows = _purchase_po_valuation_group_rows(current_df[current_df["CategoryName"].str.lower() != "unknown"], ["Unit", "CategoryName"]) if current_df is not None and not current_df.empty else []
        subcategory_rows = _purchase_po_valuation_group_rows(current_df[current_df["SubCategoryName"].str.lower() != "unknown"], ["Unit", "SubCategoryName"]) if current_df is not None and not current_df.empty else []
        detail_rows = _purchase_po_valuation_detail_rows(current_df)
        trend_rows = _purchase_po_valuation_trend_rows(current_df, period_meta)
        store_options = _purchase_po_valuation_store_options(current_base_df)

        total_value = round(_safe_float(current_df["LineValue"].sum()), 2) if current_df is not None and not current_df.empty else 0.0
        total_qty = round(_safe_float(current_df["Qty"].sum()), 3) if current_df is not None and not current_df.empty else 0.0
        po_count = int(current_df["POKey"].nunique()) if current_df is not None and not current_df.empty else 0
        supplier_count = int(current_df["SupplierName"].nunique()) if current_df is not None and not current_df.empty else 0
        store_count = int(current_df["StoreName"].nunique()) if current_df is not None and not current_df.empty else 0
        purchasing_dept_count = int(current_df["PurchasingDeptName"].nunique()) if current_df is not None and not current_df.empty else 0
        avg_po_value = round((total_value / po_count), 2) if po_count > 0 else 0.0
        avg_daily_spend = round((total_value / max(1, _safe_int(period_meta.get("days_in_range"), 1))), 2)

        previous_total_value = None
        delta_value = None
        delta_pct = None
        if previous_df is not None and not previous_df.empty:
            previous_total_value = round(_safe_float(previous_df["LineValue"].sum()), 2)
            delta_value = round(total_value - previous_total_value, 2)
            if previous_total_value > 0:
                delta_pct = round((delta_value / previous_total_value) * 100.0, 2)
        elif period_meta.get("comparison_available"):
            previous_total_value = 0.0
            delta_value = round(total_value, 2)

        top_contributor = None
        if str(resolved_context.get("scope") or "") == "all_permitted_units" and len(unit_rows) > 1:
            top_unit = unit_rows[0] if unit_rows else None
            if top_unit:
                top_contributor = {
                    "Label": str(top_unit.get("Unit") or "").strip() or "All Units",
                    "Type": "Unit",
                    "TotalValue": top_unit.get("TotalValue"),
                    "POCount": top_unit.get("POCount"),
                }
        if not top_contributor and store_rows:
            top_store = store_rows[0]
            top_contributor = {
                "Label": str(top_store.get("StoreName") or "").strip() or "Unknown",
                "Type": "Store",
                "TotalValue": top_store.get("TotalValue"),
                "POCount": top_store.get("POCount"),
            }

        purchasing_dept_options = []
        seen_dept_ids = set()
        for dept in (_fetch_purchasing_departments(include_inactive=False) or []):
            dept_id = _safe_int(dept.get("Id"), 0)
            dept_name = str(dept.get("PurchasingDeptName") or "").strip()
            if dept_id > 0 and dept_name and dept_id not in seen_dept_ids:
                purchasing_dept_options.append({"id": dept_id, "name": dept_name})
                seen_dept_ids.add(dept_id)
        purchasing_dept_options.sort(key=lambda row: str(row.get("name") or "").lower())

        scope = str(resolved_context.get("scope") or "current_unit")
        current_unit = str(resolved_context.get("current_unit") or "").strip().upper()
        unit_context_label = current_unit if scope == "current_unit" else (
            f"All permitted units ({len(units_to_use)})" if len(units_to_use) != 1 else (units_to_use[0] if units_to_use else "All permitted units")
        )

        summary_payload = {
            "total_value": total_value,
            "total_qty": total_qty,
            "po_count": po_count,
            "supplier_count": supplier_count,
            "store_count": store_count,
            "purchasing_dept_count": purchasing_dept_count,
            "avg_po_value": avg_po_value,
            "avg_daily_spend": avg_daily_spend,
            "previous_total_value": previous_total_value,
            "delta_value": delta_value,
            "delta_pct": delta_pct,
            "top_unit": unit_rows[0] if unit_rows else None,
            "top_store": store_rows[0] if store_rows else None,
            "top_supplier": supplier_rows[0] if supplier_rows else None,
            "top_purchasing_dept": purchasing_dept_rows[0] if purchasing_dept_rows else None,
            "top_contributor": top_contributor,
        }

        period_meta_payload = {
            "mode": period_meta.get("mode"),
            "mode_label": period_meta.get("mode_label"),
            "value": period_meta.get("value"),
            "label": period_meta.get("label"),
            "from": period_meta.get("from"),
            "to": period_meta.get("to"),
            "from_display": period_meta.get("from_display"),
            "to_display": period_meta.get("to_display"),
            "range_label": period_meta.get("range_label"),
            "days_in_range": period_meta.get("days_in_range"),
            "previous_from": period_meta.get("previous_from"),
            "previous_to": period_meta.get("previous_to"),
            "previous_label": period_meta.get("previous_label"),
            "comparison_available": period_meta.get("comparison_available"),
            "trend_grain": period_meta.get("trend_grain"),
            "available_from": period_meta.get("available_from"),
            "fiscal_year_start": period_meta.get("fiscal_year_start"),
            "fiscal_year_label": period_meta.get("fiscal_year_label"),
            "controls": period_meta.get("controls") or {},
            "scope": scope,
            "scope_label": "All Permitted Units" if scope == "all_permitted_units" else "Current Unit",
            "unit_context_label": unit_context_label,
        }

        payload = {
            "status": "success",
            "rows": store_rows,
            "unit_rows": unit_rows,
            "supplier_rows": supplier_rows,
            "purchasing_dept_rows": purchasing_dept_rows,
            "dept_rows": dept_rows,
            "category_rows": category_rows,
            "subcategory_rows": subcategory_rows,
            "detail_rows": detail_rows,
            "store_detail_rows": [],
            "supplier_detail_rows": [],
            "store_options": store_options,
            "purchasing_dept_options": purchasing_dept_options,
            "trend_rows": trend_rows,
            "summary": summary_payload,
            "period_meta": period_meta_payload,
            "filters": {
                "selected_unit": current_unit if scope == "current_unit" else "",
                "current_unit": current_unit,
                "scope": scope,
                "unit_context_label": unit_context_label,
                "purchasing_dept_id": purchasing_dept_id if purchasing_dept_id > 0 else 0,
                "store_name": store_filter,
            },
            "from": period_meta.get("from"),
            "to": period_meta.get("to"),
            "units": units_to_use,
            "allowed_units": list(resolved_context.get("allowed_units") or []),
            "sync_status": "synced" if did_sync else "cached",
            "sync_cache_age_seconds": cache_age_seconds,
            "sync_cache_ts": cache_ts,
        }
        return _sanitize_json_payload(payload)


    @app.route('/api/purchase/po_valuation_storewise')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_purchase_po_valuation_storewise():
        resolved_context, error = _purchase_po_valuation_request_context()
        if error:
            return error
        try:
            payload = _build_purchase_po_valuation_payload(resolved_context)
            return jsonify(payload)
        except Exception as exc:
            print(f"PO valuation fetch failed: {exc}")
            return jsonify({"status": "error", "message": "Failed to load PO valuation"}), 500


    def _purchase_po_valuation_pdf_filename(payload: dict) -> str:
        period_meta = payload.get("period_meta") or {}
        filters = payload.get("filters") or {}
        current_unit = str(filters.get("current_unit") or "").strip().upper()
        if str(filters.get("scope") or "") == "all_permitted_units":
            unit_token = "ALL_UNITS"
        else:
            unit_token = current_unit or "UNIT"
        from_iso = str(period_meta.get("from") or "NA")
        to_iso = str(period_meta.get("to") or "NA")
        filename = f"PO_Valuation_{unit_token}_{from_iso}_to_{to_iso}.pdf"
        return re.sub(r"[^A-Za-z0-9_.-]+", "_", filename)


    def _build_purchase_po_valuation_pdf(payload: dict, exported_by: str) -> bytes:
        from html import escape
        from reportlab.lib.pagesizes import A4, landscape
        from reportlab.lib import colors
        from reportlab.lib.units import mm
        from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, PageBreak
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_RIGHT

        styles = getSampleStyleSheet()

        def _p(text: str, style):
            return Paragraph(escape(str(text or "")), style)

        title_style = ParagraphStyle(
            "po_val_pdf_title",
            parent=styles["Title"],
            fontName="Helvetica-Bold",
            fontSize=18,
            leading=21,
            textColor=colors.HexColor("#0f172a"),
            alignment=TA_LEFT,
        )
        sub_style = ParagraphStyle(
            "po_val_pdf_sub",
            parent=styles["Normal"],
            fontName="Helvetica",
            fontSize=10,
            leading=13,
            textColor=colors.HexColor("#475569"),
            alignment=TA_LEFT,
        )
        section_style = ParagraphStyle(
            "po_val_pdf_section",
            parent=styles["Heading3"],
            fontName="Helvetica-Bold",
            fontSize=12,
            leading=14,
            textColor=colors.HexColor("#0f172a"),
            alignment=TA_LEFT,
            spaceBefore=8,
            spaceAfter=6,
        )
        note_style = ParagraphStyle(
            "po_val_pdf_note",
            parent=styles["Normal"],
            fontName="Helvetica",
            fontSize=8.5,
            leading=11,
            textColor=colors.HexColor("#64748b"),
            alignment=TA_LEFT,
        )
        body_left_style = ParagraphStyle(
            "po_val_pdf_cell_left",
            parent=styles["Normal"],
            fontName="Helvetica",
            fontSize=7.8,
            leading=9.2,
            textColor=colors.HexColor("#0f172a"),
            alignment=TA_LEFT,
            wordWrap="CJK",
        )
        body_right_style = ParagraphStyle(
            "po_val_pdf_cell_right",
            parent=body_left_style,
            alignment=TA_RIGHT,
        )
        body_center_style = ParagraphStyle(
            "po_val_pdf_cell_center",
            parent=body_left_style,
            alignment=TA_CENTER,
        )

        def _styled_table(data, col_widths, align_right_cols=None, align_center_cols=None):
            align_right_cols = align_right_cols or []
            align_center_cols = align_center_cols or []
            if not data:
                data = [["-"]]
            prepared = [list(data[0])]
            for row in data[1:]:
                out_row = []
                for idx, cell in enumerate(row):
                    cell_text = str(cell if cell is not None else "")
                    if idx in align_right_cols:
                        out_row.append(Paragraph(escape(cell_text), body_right_style))
                    elif idx in align_center_cols:
                        out_row.append(Paragraph(escape(cell_text), body_center_style))
                    else:
                        out_row.append(Paragraph(escape(cell_text), body_left_style))
                prepared.append(out_row)
            table = Table(prepared, colWidths=[width * mm for width in col_widths], repeatRows=1)
            style_cmds = [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#0f172a")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTSIZE", (0, 0), (-1, -1), 8),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f8fafc")]),
                ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#cbd5e1")),
                ("LEFTPADDING", (0, 0), (-1, -1), 4),
                ("RIGHTPADDING", (0, 0), (-1, -1), 4),
                ("TOPPADDING", (0, 0), (-1, -1), 3),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ]
            for idx in align_right_cols:
                style_cmds.append(("ALIGN", (idx, 1), (idx, -1), "RIGHT"))
                style_cmds.append(("ALIGN", (idx, 0), (idx, 0), "RIGHT"))
            for idx in align_center_cols:
                style_cmds.append(("ALIGN", (idx, 1), (idx, -1), "CENTER"))
                style_cmds.append(("ALIGN", (idx, 0), (idx, 0), "CENTER"))
            table.setStyle(TableStyle(style_cmds))
            return table

        period_meta = payload.get("period_meta") or {}
        filters = payload.get("filters") or {}
        summary = payload.get("summary") or {}
        detail_rows = list(payload.get("detail_rows") or [])
        exported_at = datetime.now(tz=LOCAL_TZ).strftime("%d-%b-%Y %I:%M %p")

        buffer = io.BytesIO()
        doc = SimpleDocTemplate(
            buffer,
            pagesize=landscape(A4),
            leftMargin=10 * mm,
            rightMargin=10 * mm,
            topMargin=11 * mm,
            bottomMargin=16 * mm,
        )
        elements = [
            _p("PO Valuation Expenditure Report", title_style),
            _p("Professional budgeting export for purchasing department review and management presentation", sub_style),
            Spacer(1, 5),
        ]

        dept_label = "All purchasing departments"
        selected_dept_id = _safe_int(filters.get("purchasing_dept_id"), 0)
        for dept in (payload.get("purchasing_dept_options") or []):
            if _safe_int(dept.get("id"), 0) == selected_dept_id:
                dept_label = str(dept.get("name") or dept_label)
                break
        store_label = str(filters.get("store_name") or "").strip() or "All stores"
        prev_total = summary.get("previous_total_value")
        prev_label = (
            f"Rs. {_format_indian_currency(_safe_float(prev_total))}"
            if prev_total is not None
            else "No prior baseline"
        )
        meta_rows = [
            ["Period", str(period_meta.get("mode_label") or "-"), "Resolved Range", str(period_meta.get("range_label") or "-"), "Scope", str(period_meta.get("scope_label") or "-")],
            ["Unit Context", str(period_meta.get("unit_context_label") or "-"), "Purchasing Dept", dept_label, "Store", store_label],
            ["Exported By", exported_by or "-", "Exported At", exported_at, "Previous Period", prev_label],
        ]
        elements.append(_styled_table([["Field", "Value", "Field", "Value", "Field", "Value"]] + meta_rows, [24, 44, 26, 54, 24, 66]))
        elements.append(Spacer(1, 8))

        elements.append(_p("Executive KPI Summary", section_style))
        kpi_rows = [
            ["Metric", "Value", "Metric", "Value"],
            ["Total Expenditure", f"Rs. {_format_indian_currency(_safe_float(summary.get('total_value')))}", "PO Count", str(_safe_int(summary.get("po_count"), 0))],
            ["Average PO Value", f"Rs. {_format_indian_currency(_safe_float(summary.get('avg_po_value')))}", "Average Spend / Day", f"Rs. {_format_indian_currency(_safe_float(summary.get('avg_daily_spend')))}"],
            [
                "Previous Period Delta",
                (
                    f"Rs. {_format_indian_currency(_safe_float(summary.get('delta_value')))}"
                    if summary.get("delta_value") is not None
                    else "No prior baseline"
                ),
                "Delta %",
                (
                    f"{_safe_float(summary.get('delta_pct')):.2f}%"
                    if summary.get("delta_pct") is not None
                    else "-"
                ),
            ],
            [
                "Top Contributor",
                str((summary.get("top_contributor") or {}).get("Label") or "-"),
                "Contributor Type",
                str((summary.get("top_contributor") or {}).get("Type") or "-"),
            ],
        ]
        elements.append(_styled_table(kpi_rows, [34, 58, 34, 58], align_right_cols=[1, 3]))
        elements.append(Spacer(1, 6))

        def _append_ranked_section(title: str, rows: list[dict], label_fn, limit: int = 20):
            if not rows:
                return
            render_rows = rows[:limit]
            table_rows = [["#", "Label", "POs", "Qty", "Expenditure"]]
            for idx, row in enumerate(render_rows, start=1):
                table_rows.append(
                    [
                        str(idx),
                        label_fn(row),
                        str(_safe_int(row.get("POCount"), 0)),
                        f"{_safe_float(row.get('TotalQty')):.2f}",
                        f"Rs. {_format_indian_currency(_safe_float(row.get('TotalValue')))}",
                    ]
                )
            elements.append(_p(title, section_style))
            if len(rows) > limit:
                elements.append(_p(f"Showing top {limit} rows by expenditure.", note_style))
                elements.append(Spacer(1, 3))
            elements.append(_styled_table(table_rows, [10, 90, 16, 22, 34], align_right_cols=[2, 3, 4], align_center_cols=[0]))
            elements.append(Spacer(1, 6))

        unit_rows = list(payload.get("unit_rows") or [])
        if len(unit_rows) > 1:
            _append_ranked_section("Unit-wise Summary", unit_rows, lambda row: str(row.get("Unit") or "-"))
        _append_ranked_section("Store-wise Summary", list(payload.get("rows") or []), lambda row: f"{row.get('StoreName') or '-'} ({row.get('Unit') or '-'})")
        _append_ranked_section("Purchasing Department Summary", list(payload.get("purchasing_dept_rows") or []), lambda row: f"{row.get('PurchasingDeptName') or '-'} ({row.get('Unit') or '-'})")
        _append_ranked_section("Supplier Summary", list(payload.get("supplier_rows") or []), lambda row: f"{row.get('SupplierName') or '-'} ({row.get('Unit') or '-'})")
        _append_ranked_section("Department Summary", list(payload.get("dept_rows") or []), lambda row: f"{row.get('DeptName') or '-'} ({row.get('Unit') or '-'})")
        _append_ranked_section("Category Summary", list(payload.get("category_rows") or []), lambda row: f"{row.get('CategoryName') or '-'} ({row.get('Unit') or '-'})")

        elements.append(PageBreak())
        elements.append(_p("PO Detail Appendix", section_style))
        max_detail_rows = 1500
        render_details = detail_rows[:max_detail_rows]
        if len(detail_rows) > max_detail_rows:
            elements.append(_p(f"Appendix truncated to first {max_detail_rows} PO rows out of {len(detail_rows)}.", note_style))
            elements.append(Spacer(1, 4))
        if render_details:
            detail_table_rows = [["PO Date", "PO No", "Unit", "Store", "Purchasing Dept", "Supplier", "Qty", "Gross Amount"]]
            for row in render_details:
                detail_table_rows.append(
                    [
                        str(row.get("PODate") or "-"),
                        str(row.get("PONo") or "-"),
                        str(row.get("Unit") or "-"),
                        str(row.get("StoreName") or "-"),
                        str(row.get("PurchasingDeptName") or "-"),
                        str(row.get("SupplierName") or "-"),
                        f"{_safe_float(row.get('Qty')):.2f}",
                        f"Rs. {_format_indian_currency(_safe_float(row.get('GrossAmount')))}",
                    ]
                )
            elements.append(_styled_table(detail_table_rows, [18, 24, 16, 36, 32, 58, 18, 26], align_right_cols=[6, 7]))
        else:
            elements.append(_p("No PO details were found for the selected filters.", note_style))

        footer_line_1 = f"PO Valuation Expenditure Report | {period_meta.get('range_label') or '-'}"
        footer_line_2 = f"Exported By: {exported_by or '-'} | Exported At: {exported_at}"

        def _on_page(canvas, doc_obj):
            canvas.saveState()
            canvas.setStrokeColor(colors.HexColor("#cbd5e1"))
            canvas.setLineWidth(0.5)
            canvas.line(doc_obj.leftMargin, 11 * mm, doc_obj.pagesize[0] - doc_obj.rightMargin, 11 * mm)
            canvas.setFont("Helvetica", 8)
            canvas.setFillColor(colors.HexColor("#475569"))
            canvas.drawString(doc_obj.leftMargin, 7 * mm, footer_line_1)
            canvas.drawString(doc_obj.leftMargin, 4.2 * mm, footer_line_2)
            canvas.drawRightString(doc_obj.pagesize[0] - doc_obj.rightMargin, 4.2 * mm, f"Page {doc_obj.page}")
            canvas.restoreState()

        doc.build(elements, onFirstPage=_on_page, onLaterPages=_on_page)
        return buffer.getvalue()


    def _run_purchase_po_valuation_pdf_job(job_id: str, resolved_context: dict, exported_by: str):
        try:
            def _job_stage(stage: str, message: str):
                _excel_job_update(job_id, state="running", stage=stage, message=message, filename=None, format="pdf")

            _job_stage("loading_data", "Loading filtered PO valuation data...")
            payload = _build_purchase_po_valuation_payload(resolved_context, status_callback=_job_stage)
            _job_stage("rendering_pdf", "Rendering professional PDF export...")
            pdf_bytes = _build_purchase_po_valuation_pdf(payload, exported_by)
            filename = _purchase_po_valuation_pdf_filename(payload)
            _export_cache_put_bytes("purchase_po_valuation_pdf_job", pdf_bytes, job_id)
            _excel_job_update(job_id, state="done", stage="ready", message="PDF export ready.", filename=filename, format="pdf")
        except Exception as exc:
            _excel_job_update(job_id, state="error", stage="error", message=str(exc), error=str(exc), filename=None, format="pdf")


    @app.route('/api/purchase/po_valuation_export_pdf_job', methods=['POST'])
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_purchase_po_valuation_export_pdf_job():
        source = request.get_json(silent=True) if request.is_json else (request.form.to_dict(flat=True) if request.form else {})
        resolved_context, error = _purchase_po_valuation_request_context(source or {})
        if error:
            return error
        exported_by = (session.get("username") or session.get("user") or "Unknown").strip() or "Unknown"
        job_id = token_hex(16)
        _excel_job_update(job_id, state="queued", stage="queued", message="PDF export queued.", filename=None, format="pdf")
        EXPORT_EXECUTOR.submit(_run_purchase_po_valuation_pdf_job, job_id, resolved_context, exported_by)
        return jsonify({"status": "queued", "job_id": job_id})


    @app.route('/api/purchase/po_valuation_export_pdf_job_status')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_purchase_po_valuation_export_pdf_job_status():
        job_id = (request.args.get("job_id") or "").strip()
        if not job_id:
            return jsonify({"status": "error", "message": "Missing job id"}), 400
        entry = _excel_job_get(job_id)
        if not entry:
            return jsonify({"status": "error", "message": "Job not found"}), 404
        return jsonify({
            "status": "success",
            "state": entry.get("state"),
            "stage": entry.get("stage"),
            "message": entry.get("message"),
            "error": entry.get("error"),
            "filename": entry.get("filename"),
            "format": entry.get("format"),
        })


    @app.route('/api/purchase/po_valuation_export_pdf_job_result')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_purchase_po_valuation_export_pdf_job_result():
        job_id = (request.args.get("job_id") or "").strip()
        if not job_id:
            return "Missing job id", 400
        entry = _excel_job_get(job_id)
        if not entry:
            return "Job not found", 404
        if entry.get("state") != "done":
            return "Job not ready", 409
        data = _export_cache_get_bytes("purchase_po_valuation_pdf_job", job_id)
        if not data:
            return "Export expired", 404
        return send_file(
            io.BytesIO(data),
            mimetype="application/pdf",
            as_attachment=True,
            download_name=str(entry.get("filename") or "PO_Valuation.pdf"),
        )


    @app.route('/api/purchase/po', methods=['POST'])
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_purchase_create_po():
        unit, error = _get_purchase_unit()
        if error:
            return error
        try:
            data_fetch.ensure_po_purchasing_dept_column(unit)
        except Exception:
            pass
        try:
            data_fetch.ensure_po_special_notes_column(unit)
        except Exception:
            pass
        try:
            data_fetch.ensure_po_cmc_amc_warranty_column(unit)
        except Exception:
            pass
        try:
            data_fetch.ensure_po_overall_discount_mode_column(unit)
        except Exception:
            pass
        try:
            data_fetch.ensure_po_overall_discount_value_column(unit)
        except Exception:
            pass
        try:
            data_fetch.ensure_po_print_format_column(unit)
        except Exception:
            pass
        try:
            data_fetch.ensure_po_approval_date_column(unit)
        except Exception:
            pass
        try:
            data_fetch.ensure_po_senior_approval_authority_column(unit)
        except Exception:
            pass
        try:
            data_fetch.ensure_po_senior_approval_designation_column(unit)
        except Exception:
            pass
        if not data_fetch.ensure_po_detail_unit_name_column(unit):
            return jsonify({"status": "error", "message": "PO detail unit field is not available in the selected unit database."}), 500

        payload = request.get_json(silent=True) or {}
        header = _apply_default_po_terms(unit, payload.get("header") or {})
        items, item_row_errors = _normalize_purchase_item_rows(payload.get("items") or [])

        supplier_id = int(header.get("supplier_id") or 0)
        if not supplier_id:
            _audit_log_event(
                "purchase",
                "po_create",
                status="error",
                entity_type="po",
                unit=unit,
                summary="Supplier is required",
                details={"po_no": header.get("po_no"), "item_count": len(items)},
            )
            return jsonify({"status": "error", "message": "Supplier is required"}), 400
        if not items:
            _audit_log_event(
                "purchase",
                "po_create",
                status="error",
                entity_type="po",
                unit=unit,
                summary="At least one item is required",
                details={"po_no": header.get("po_no")},
            )
            return jsonify({"status": "error", "message": "At least one item is required"}), 400
        if item_row_errors:
            _audit_log_event(
                "purchase",
                "po_create",
                status="error",
                entity_type="po",
                unit=unit,
                summary="Invalid PO item rows",
                details={"errors": item_row_errors, "po_no": header.get("po_no")},
            )
            return jsonify({"status": "error", "message": "Invalid PO item rows", "errors": item_row_errors}), 400
        invalid_item_ref_errors = _normalize_invalid_purchase_item_refs(unit, items)
        if invalid_item_ref_errors:
            _audit_log_event(
                "purchase",
                "po_create",
                status="error",
                entity_type="po",
                unit=unit,
                summary="Invalid PO item references",
                details={"errors": invalid_item_ref_errors, "po_no": header.get("po_no")},
            )
            return jsonify({"status": "error", "message": "Invalid PO item references", "errors": invalid_item_ref_errors}), 400
        dup_check = _find_duplicate_purchase_item_refs(items, require_positive_qty=False)
        if dup_check["duplicate_ids"] or dup_check["duplicate_names"]:
            dup_labels = _format_duplicate_item_labels(items, dup_check["duplicate_ids"])
            name_labels = dup_check["duplicate_names"] or []
            label_parts = []
            if dup_labels:
                label_parts.append(", ".join(dup_labels[:8]) + (f" (+{len(dup_labels) - 8} more)" if len(dup_labels) > 8 else ""))
            if name_labels:
                label_parts.append(", ".join(name_labels[:5]) + (f" (+{len(name_labels) - 5} more)" if len(name_labels) > 5 else ""))
            msg_detail = " | ".join([part for part in label_parts if part])
            msg = f"Duplicate items are not allowed in PO. Remove repeated item(s): {msg_detail}."
            _audit_log_event(
                "purchase",
                "po_create",
                status="error",
                entity_type="po",
                unit=unit,
                summary="Duplicate PO items detected",
                details={"duplicate_ids": dup_labels, "duplicate_names": name_labels},
            )
            return jsonify(
                {
                    "status": "error",
                    "message": msg,
                    "duplicate_ids": dup_labels,
                    "duplicate_names": name_labels,
                }
            ), 400

        for item in items:
            if item.get("gst_pct") is None:
                cgst_pct = _safe_float(item.get("cgst_pct"))
                sgst_pct = _safe_float(item.get("sgst_pct"))
                igst_pct = _safe_float(item.get("igst_pct"))
                item["gst_pct"] = (cgst_pct + sgst_pct) if (cgst_pct or sgst_pct) else igst_pct

        cgst_total = 0.0
        sgst_total = 0.0
        for item in items:
            cgst_amt = float(item.get("cgst_amt") or 0)
            sgst_amt = float(item.get("sgst_amt") or 0)
            igst_amt = float(item.get("igst_amt") or 0)
            if igst_amt and not (cgst_amt or sgst_amt):
                cgst_amt = igst_amt / 2
                sgst_amt = igst_amt / 2
            cgst_total += cgst_amt
            sgst_total += sgst_amt

        totals = header.get("totals") or {}
        tax_total = float(totals.get("tax_total") or 0)
        discount_total = float(totals.get("discount_total") or 0)
        amount_total = float(totals.get("grand_total") or 0)
        for_total = float(totals.get("for_total") or 0)
        excise_total = float(totals.get("excise_total") or 0)

        tax_total = sgst_total
        excise_total = cgst_total

        status_raw = str(header.get("status") or "Draft").strip().lower()
        status_map = {
            "draft": "D",
            "pending approval": "P",
            "approved": "A",
        }
        status_code = status_map.get(status_raw, "D")
        purchasing_dept_id = _safe_int(header.get("purchasing_dept_id"))
        selected_po_print_format = _resolve_po_print_format_for_persistence(
            unit,
            header.get("print_format"),
            existing_format=None,
            purchasing_dept_id=purchasing_dept_id,
        )
        if status_code == "P" and purchasing_dept_id <= 0:
            _audit_log_event(
                "purchase",
                "po_create",
                status="error",
                entity_type="po",
                unit=unit,
                summary="Purchasing department is required for approval",
                details={"po_no": header.get("po_no")},
            )
            return jsonify({"status": "error", "message": "Please select Purchasing Dept before submitting for approval."}), 400

        now = datetime.now(tz=LOCAL_TZ)
        created_items, item_errors = _ensure_item_masters_for_po(unit, items, now.strftime("%Y-%m-%d %H:%M:%S"))
        if item_errors:
            _audit_log_event(
                "purchase",
                "po_create",
                status="error",
                entity_type="po",
                unit=unit,
                summary="Item master creation failed",
                details={"errors": item_errors, "po_no": header.get("po_no")},
            )
            return jsonify({"status": "error", "message": "Item master creation failed", "errors": item_errors}), 400
        missing_item_ref_errors = _find_missing_purchase_item_refs(unit, items)
        if missing_item_ref_errors:
            _audit_log_event(
                "purchase",
                "po_create",
                status="error",
                entity_type="po",
                unit=unit,
                summary="PO item references could not be verified",
                details={"errors": missing_item_ref_errors, "po_no": header.get("po_no")},
            )
            return jsonify({"status": "error", "message": "PO item references could not be verified", "errors": missing_item_ref_errors}), 400

        def _num_or_text(val):
            try:
                if val is None or val == "":
                    return None
                return float(val)
            except Exception:
                return val

        cmc_amc_warranty_value = header.get("cmc_amc_warranty")
        if cmc_amc_warranty_value is None:
            cmc_amc_warranty_value = header.get("other_terms")

        po_params = {
            "pId": int(header.get("po_id") or 0),
            "pSupplierid": supplier_id,
            "pTenderid": int(header.get("tender_id") or 0),
            "pPono": header.get("po_no"),
            "pPodate": header.get("po_date"),
            "pDeliveryterms": header.get("delivery_terms"),
            "pPaymentsterms": header.get("payment_terms"),
            "pOtherterms": cmc_amc_warranty_value,
            "pTaxid": int(header.get("tax_id") or 0),
            "pTax": tax_total,
            "pDiscount": discount_total,
            "pAmount": amount_total,
            "pCreditdays": int(header.get("credit_days") or 0),
            "pPocomplete": int(bool(header.get("po_complete"))),
            "pNotes": header.get("notes"),
            "pSpecialNotes": header.get("special_notes"),
            "pCmcAmcWarrantyNotes": cmc_amc_warranty_value,
            "SeniorApprovalAuthorityName": header.get("senior_approval_authority_name"),
            "SeniorApprovalAuthorityDesignation": header.get("senior_approval_authority_designation"),
            "pPreparedby": header.get("prepared_by"),
            "pCustom1": _num_or_text(header.get("freight_charges")),
            "pCustom2": _num_or_text(header.get("packing_charges")),
            "pUpdatedby": int(header.get("updated_by") or 0),
            "pUpdatedon": header.get("updated_on"),
            "pSignauthorityperson": header.get("sign_authority_person"),
            "pSignauthoritypdesig": header.get("sign_authority_desig"),
            "pRefno": header.get("ref_no"),
            "pSubject": header.get("subject"),
            "pAuthorizationid": int(header.get("authorization_id") or 0),
            "pPurchaseIndentId": int(header.get("indent_id") or 0),
            "pInsertedByUserID": session.get("username") or session.get("user"),
            "pInsertedON": now.strftime("%Y-%m-%d %H:%M:%S"),
            "pInsertedMacName": header.get("mac_name"),
            "pInsertedMacID": header.get("mac_id"),
            "pInsertedIPAddress": request.remote_addr,
            "Against": header.get("against_name") or header.get("against"),
            "QuotationId": int(header.get("quotation_id") or 0),
            "TotalFORe": for_total,
            "TotalExciseAmt": excise_total,
            "AgainstId": int(header.get("against_id") or 0),
            "Status": status_code,
            "PurchasingDeptId": purchasing_dept_id if purchasing_dept_id > 0 else None,
            "OverallDiscountMode": header.get("overall_discount_mode"),
            "OverallDiscountValue": _safe_float(header.get("overall_discount_value")),
            "POPrintFormat": selected_po_print_format,
        }

        detail_errors = []
        detail_params = []
        po_id = 0
        po_no = ""
        po_write_conn = data_fetch.get_sql_connection(unit)
        if not po_write_conn:
            _audit_log_event(
                "purchase",
                "po_create",
                status="error",
                entity_type="po",
                unit=unit,
                summary="PO creation failed",
                details={"error": f"Could not connect to {unit}", "po_no": header.get("po_no")},
            )
            return jsonify({"status": "error", "message": f"Could not connect to {unit}"}), 500

        try:
            try:
                po_write_conn.autocommit = False
            except Exception:
                try:
                    po_write_conn.driver_connection.autocommit = False
                except Exception:
                    pass

            mst_result = data_fetch.add_iv_po_mst_with_autonumber(
                unit,
                po_params,
                conn=po_write_conn,
                manage_transaction=False,
            )
            if mst_result.get("error"):
                detail_errors.append(str(mst_result.get("error")))
            else:
                po_id = _safe_int(mst_result.get("po_id"))
                po_no = str(mst_result.get("po_no") or "").strip()
                if po_id <= 0:
                    detail_errors.append("Failed to create PO header.")

            if po_id > 0:
                for row_idx, item in enumerate(items, start=1):
                    try:
                        cgst_pct = float(item.get("cgst_pct") or 0)
                        sgst_pct = float(item.get("sgst_pct") or 0)
                        igst_pct = float(item.get("igst_pct") or 0)
                        cgst_amt = float(item.get("cgst_amt") or 0)
                        sgst_amt = float(item.get("sgst_amt") or 0)
                        igst_amt = float(item.get("igst_amt") or 0)

                        if igst_pct and not (cgst_pct or sgst_pct):
                            cgst_pct = igst_pct / 2
                            sgst_pct = igst_pct / 2
                        if igst_amt and not (cgst_amt or sgst_amt):
                            cgst_amt = igst_amt / 2
                            sgst_amt = igst_amt / 2

                        item_params = {
                            "pId": int(item.get("id") or 0),
                            "pPoid": int(po_id),
                            "pItemid": int(item.get("item_id") or 0),
                            "pQty": float(item.get("qty") or 0),
                            "pPackSizeId": int(item.get("pack_size_id") or 0),
                            "UnitName": str(item.get("unit") or "").strip(),
                            "pRate": float(item.get("rate") or 0),
                            "pFreeqty": float(item.get("free_qty") or 0),
                            "pDiscount": float(item.get("discount_pct") or 0),
                            "pTax": sgst_pct,
                            "pTaxamount": sgst_amt,
                            "pMRP": float(item.get("mrp") or 0),
                            "pVATOn": item.get("vat_on"),
                            "pVAT": item.get("vat"),
                            "pCustom1": item.get("custom1"),
                            "pCustom2": item.get("custom2"),
                            "pInsertedByUserID": session.get("username") or session.get("user"),
                            "pInsertedON": now.strftime("%Y-%m-%d %H:%M:%S"),
                            "pInsertedMacName": item.get("mac_name"),
                            "pInsertedMacID": item.get("mac_id"),
                            "pInsertedIPAddress": request.remote_addr,
                            "Fore": float(item.get("for_amt") or 0),
                            "Excisetax": cgst_pct,
                            "ExciseTaxamt": cgst_amt,
                            "NetAmount": float(item.get("net_amt") or 0),
                            "lendingRate": float(item.get("lending_rate") or 0),
                            "UnitFor": float(item.get("unit_for") or 0),
                            "UnitDiscount": float(item.get("unit_discount") or 0),
                            "_row_no": row_idx,
                            "_item_name": item.get("item_name"),
                        }
                        detail_params.append(item_params)
                    except Exception as e:
                        detail_errors.append(f"Row {row_idx}: {e}")

            if not detail_errors and detail_params:
                bulk_result = data_fetch.add_iv_po_dtl_many(
                    unit,
                    detail_params,
                    conn=po_write_conn,
                    manage_transaction=False,
                )
                if bulk_result.get("error"):
                    detail_errors.extend(list(bulk_result.get("errors") or [str(bulk_result.get("error"))]))
                else:
                    detail_errors.extend(list(bulk_result.get("errors") or []))

            if detail_errors:
                try:
                    po_write_conn.rollback()
                except Exception:
                    pass
                _audit_log_event(
                    "purchase",
                    "po_create",
                    status="error",
                    entity_type="po",
                    unit=unit,
                    summary="PO save failed and was rolled back",
                    details={"errors": detail_errors, "po_no": po_no or header.get("po_no")},
                )
                return jsonify({
                    "status": "error",
                    "message": "PO save failed. No PO data was saved.",
                    "errors": detail_errors,
                }), 500

            po_write_conn.commit()
        except Exception as e:
            try:
                po_write_conn.rollback()
            except Exception:
                pass
            detail_errors = detail_errors or [str(e)]
            _audit_log_event(
                "purchase",
                "po_create",
                status="error",
                entity_type="po",
                unit=unit,
                summary="PO save failed and was rolled back",
                details={"errors": detail_errors, "po_no": po_no or header.get("po_no")},
            )
            return jsonify({
                "status": "error",
                "message": "PO save failed. No PO data was saved.",
                "errors": detail_errors,
            }), 500
        finally:
            try:
                po_write_conn.close()
            except Exception:
                pass

        if not po_no:
            try:
                saved_header_df = data_fetch.fetch_purchase_po_header(unit, po_id=po_id)
                if saved_header_df is not None and not saved_header_df.empty:
                    saved_header_df = _clean_df_columns(saved_header_df)
                    saved_po_no = str(saved_header_df.iloc[0].get("PONo") or "").strip()
                    if saved_po_no:
                        po_no = saved_po_no
            except Exception:
                pass
        if not po_no:
            po_no = str(header.get("po_no") or "").strip() or f"PO-{po_id}"
        try:
            po_no_fix = data_fetch.ensure_purchase_po_number(unit, int(po_id), preferred_po_no=po_no)
            ensured_po_no = str(po_no_fix.get("po_no") or "").strip()
            if ensured_po_no:
                po_no = ensured_po_no
        except Exception:
            pass

        otp_request_id = None
        if status_code == "P":
            otp_result = _create_purchase_otp_request(
                unit=unit,
                po_no=po_no,
                po_id=po_id,
                amount=amount_total,
                supplier_name=header.get("supplier_name") or "",
                reason=header.get("subject") or header.get("notes") or "",
                requested_by=session.get("username") or session.get("user") or "",
                purchasing_dept_id=purchasing_dept_id if purchasing_dept_id > 0 else None,
            )
            if otp_result.get("error"):
                _audit_log_event(
                    "purchase",
                    "po_create",
                    status="error",
                    entity_type="po",
                    entity_id=str(po_id),
                    unit=unit,
                    summary="OTP request failed",
                    details={"error": otp_result.get("error"), "po_no": po_no},
                )
                return jsonify({"status": "error", "message": otp_result["error"]}), 500
            otp_request_id = otp_result.get("request_id")
            if otp_request_id:
                _insert_purchase_otp_request(po_id, po_no, unit, int(otp_request_id), session.get("username") or session.get("user"))

        response = {"status": "success", "po_id": po_id, "po_no": po_no}
        if otp_request_id:
            response["request_id"] = otp_request_id
        if created_items:
            response["created_items"] = created_items
        _audit_log_event(
            "purchase",
            "po_create",
            status="success",
            entity_type="po",
            entity_id=str(po_id),
            unit=unit,
            summary="PO created",
            details={
                "po_no": po_no,
                "header": _normalize_po_header_for_audit(header, totals, status_code),
                "item_count": len(items),
                "created_items": created_items,
                "otp_request_id": otp_request_id,
            },
            request_id=str(otp_request_id) if otp_request_id else None,
        )
        return jsonify(response)


    @app.route('/api/purchase/po_update', methods=['POST'])
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_purchase_update_po():
        unit, error = _get_purchase_unit()
        if error:
            return error
        try:
            data_fetch.ensure_po_purchasing_dept_column(unit)
        except Exception:
            pass
        try:
            data_fetch.ensure_po_special_notes_column(unit)
        except Exception:
            pass
        try:
            data_fetch.ensure_po_cmc_amc_warranty_column(unit)
        except Exception:
            pass
        try:
            data_fetch.ensure_po_overall_discount_mode_column(unit)
        except Exception:
            pass
        try:
            data_fetch.ensure_po_overall_discount_value_column(unit)
        except Exception:
            pass
        try:
            data_fetch.ensure_po_print_format_column(unit)
        except Exception:
            pass
        try:
            data_fetch.ensure_po_approval_date_column(unit)
        except Exception:
            pass
        try:
            data_fetch.ensure_po_senior_approval_authority_column(unit)
        except Exception:
            pass
        try:
            data_fetch.ensure_po_senior_approval_designation_column(unit)
        except Exception:
            pass
        if not data_fetch.ensure_po_detail_unit_name_column(unit):
            return jsonify({"status": "error", "message": "PO detail unit field is not available in the selected unit database."}), 500

        payload = request.get_json(silent=True) or {}
        header = payload.get("header") or {}
        items, item_row_errors = _normalize_purchase_item_rows(payload.get("items") or [])

        po_id = int(header.get("po_id") or 0)
        if not po_id:
            _audit_log_event(
                "purchase",
                "po_update",
                status="error",
                entity_type="po",
                unit=unit,
                summary="PO ID is required for update",
                details={"po_no": header.get("po_no")},
            )
            return jsonify({"status": "error", "message": "PO ID is required for update"}), 400
        if not items:
            _audit_log_event(
                "purchase",
                "po_update",
                status="error",
                entity_type="po",
                entity_id=str(po_id),
                unit=unit,
                summary="At least one item is required",
                details={"po_no": header.get("po_no")},
            )
            return jsonify({"status": "error", "message": "At least one item is required"}), 400
        if item_row_errors:
            _audit_log_event(
                "purchase",
                "po_update",
                status="error",
                entity_type="po",
                entity_id=str(po_id),
                unit=unit,
                summary="Invalid PO item rows",
                details={"errors": item_row_errors, "po_no": header.get("po_no")},
            )
            return jsonify({"status": "error", "message": "Invalid PO item rows", "errors": item_row_errors}), 400
        invalid_item_ref_errors = _normalize_invalid_purchase_item_refs(unit, items)
        if invalid_item_ref_errors:
            _audit_log_event(
                "purchase",
                "po_update",
                status="error",
                entity_type="po",
                entity_id=str(po_id),
                unit=unit,
                summary="Invalid PO item references",
                details={"errors": invalid_item_ref_errors, "po_no": header.get("po_no")},
            )
            return jsonify({"status": "error", "message": "Invalid PO item references", "errors": invalid_item_ref_errors}), 400
        dup_check = _find_duplicate_purchase_item_refs(items, require_positive_qty=False)
        if dup_check["duplicate_ids"] or dup_check["duplicate_names"]:
            dup_labels = _format_duplicate_item_labels(items, dup_check["duplicate_ids"])
            name_labels = dup_check["duplicate_names"] or []
            label_parts = []
            if dup_labels:
                label_parts.append(", ".join(dup_labels[:8]) + (f" (+{len(dup_labels) - 8} more)" if len(dup_labels) > 8 else ""))
            if name_labels:
                label_parts.append(", ".join(name_labels[:5]) + (f" (+{len(name_labels) - 5} more)" if len(name_labels) > 5 else ""))
            msg_detail = " | ".join([part for part in label_parts if part])
            msg = f"Duplicate items are not allowed in PO. Remove repeated item(s): {msg_detail}."
            _audit_log_event(
                "purchase",
                "po_update",
                status="error",
                entity_type="po",
                entity_id=str(po_id),
                unit=unit,
                summary="Duplicate PO items detected",
                details={"duplicate_ids": dup_labels, "duplicate_names": name_labels},
            )
            return jsonify(
                {
                    "status": "error",
                    "message": msg,
                    "duplicate_ids": dup_labels,
                    "duplicate_names": name_labels,
                }
            ), 400

        for item in items:
            if item.get("gst_pct") is None:
                cgst_pct = _safe_float(item.get("cgst_pct"))
                sgst_pct = _safe_float(item.get("sgst_pct"))
                igst_pct = _safe_float(item.get("igst_pct"))
                item["gst_pct"] = (cgst_pct + sgst_pct) if (cgst_pct or sgst_pct) else igst_pct

        header_df = data_fetch.fetch_purchase_po_header(unit, po_id=po_id)
        if header_df is None:
            _audit_log_event(
                "purchase",
                "po_update",
                status="error",
                entity_type="po",
                entity_id=str(po_id),
                unit=unit,
                summary="Failed to fetch PO for update",
            )
            return jsonify({"status": "error", "message": "Failed to fetch PO for update"}), 500
        if header_df.empty:
            _audit_log_event(
                "purchase",
                "po_update",
                status="error",
                entity_type="po",
                entity_id=str(po_id),
                unit=unit,
                summary="PO not found",
            )
            return jsonify({"status": "error", "message": "PO not found"}), 404
        header_df = _clean_df_columns(header_df)
        existing_header = header_df.iloc[0].to_dict()
        header = _apply_default_po_terms(unit, header, existing_header)
        current_status = str(header_df.iloc[0].get("Status") or "").strip().upper()
        if current_status not in {"D", "P"}:
            _audit_log_event(
                "purchase",
                "po_update",
                status="error",
                entity_type="po",
                entity_id=str(po_id),
                unit=unit,
                summary="Only Draft or Pending Approval POs can be updated",
                details={"current_status": current_status},
            )
            return jsonify({"status": "error", "message": "Only Draft or Pending Approval POs can be updated"}), 400

        existing_items = []
        try:
            items_df = data_fetch.fetch_purchase_po_items(unit, po_id)
            if items_df is not None and not items_df.empty:
                items_df = _clean_df_columns(items_df)
                existing_items = items_df.to_dict(orient="records")
        except Exception:
            existing_items = []

        cgst_total = 0.0
        sgst_total = 0.0
        for item in items:
            cgst_amt = float(item.get("cgst_amt") or 0)
            sgst_amt = float(item.get("sgst_amt") or 0)
            igst_amt = float(item.get("igst_amt") or 0)
            if igst_amt and not (cgst_amt or sgst_amt):
                cgst_amt = igst_amt / 2
                sgst_amt = igst_amt / 2
            cgst_total += cgst_amt
            sgst_total += sgst_amt

        totals = header.get("totals") or {}
        tax_total = float(totals.get("tax_total") or 0)
        discount_total = float(totals.get("discount_total") or 0)
        amount_total = float(totals.get("grand_total") or 0)
        for_total = float(totals.get("for_total") or 0)

        tax_total = sgst_total
        excise_total = cgst_total

        status_raw = str(header.get("status") or "Draft").strip().lower()
        status_map = {
            "draft": "D",
            "pending approval": "P",
            "approved": "A",
        }
        status_code = status_map.get(status_raw, "D")
        purchasing_dept_id = _safe_int(header.get("purchasing_dept_id"))
        selected_po_print_format = _resolve_po_print_format_for_persistence(
            unit,
            header.get("print_format"),
            existing_format=existing_header.get("POPrintFormat"),
            purchasing_dept_id=purchasing_dept_id,
        )
        if status_code == "P" and purchasing_dept_id <= 0:
            _audit_log_event(
                "purchase",
                "po_update",
                status="error",
                entity_type="po",
                entity_id=str(po_id),
                unit=unit,
                summary="Purchasing department is required for approval",
                details={"po_no": header.get("po_no")},
            )
            return jsonify({"status": "error", "message": "Please select Purchasing Dept before submitting for approval."}), 400

        now = datetime.now(tz=LOCAL_TZ)
        created_items, item_errors = _ensure_item_masters_for_po(unit, items, now.strftime("%Y-%m-%d %H:%M:%S"))
        if item_errors:
            _audit_log_event(
                "purchase",
                "po_update",
                status="error",
                entity_type="po",
                entity_id=str(po_id),
                unit=unit,
                summary="Item master creation failed",
                details={"errors": item_errors, "po_no": header.get("po_no")},
            )
            return jsonify({"status": "error", "message": "Item master creation failed", "errors": item_errors}), 400
        missing_item_ref_errors = _find_missing_purchase_item_refs(unit, items)
        if missing_item_ref_errors:
            _audit_log_event(
                "purchase",
                "po_update",
                status="error",
                entity_type="po",
                entity_id=str(po_id),
                unit=unit,
                summary="PO item references could not be verified",
                details={"errors": missing_item_ref_errors, "po_no": header.get("po_no")},
            )
            return jsonify({"status": "error", "message": "PO item references could not be verified", "errors": missing_item_ref_errors}), 400

        def _num_or_text(val):
            try:
                if val is None or val == "":
                    return None
                return float(val)
            except Exception:
                return val

        cmc_amc_warranty_value = header.get("cmc_amc_warranty")
        if cmc_amc_warranty_value is None:
            cmc_amc_warranty_value = header.get("other_terms")

        po_params = {
            "pId": po_id,
            "pSupplierid": int(header.get("supplier_id") or 0),
            "pTenderid": int(header.get("tender_id") or 0),
            "pPono": header.get("po_no") or existing_header.get("PONo"),
            "pPodate": header.get("po_date"),
            "pDeliveryterms": header.get("delivery_terms"),
            "pPaymentsterms": header.get("payment_terms"),
            "pOtherterms": cmc_amc_warranty_value,
            "pTaxid": int(header.get("tax_id") or 0),
            "pTax": tax_total,
            "pDiscount": discount_total,
            "pAmount": amount_total,
            "pCreditdays": int(header.get("credit_days") or 0),
            "pPocomplete": int(bool(header.get("po_complete"))),
            "pNotes": header.get("notes"),
            "pSpecialNotes": header.get("special_notes"),
            "pCmcAmcWarrantyNotes": cmc_amc_warranty_value,
            "SeniorApprovalAuthorityName": header.get("senior_approval_authority_name"),
            "SeniorApprovalAuthorityDesignation": header.get("senior_approval_authority_designation"),
            "pPreparedby": header.get("prepared_by"),
            "pCustom1": _num_or_text(header.get("freight_charges")),
            "pCustom2": _num_or_text(header.get("packing_charges")),
            "pSignauthorityperson": header.get("sign_authority_person"),
            "pSignauthoritypdesig": header.get("sign_authority_desig"),
            "pRefno": header.get("ref_no"),
            "pSubject": header.get("subject"),
            "pAuthorizationid": int(header.get("authorization_id") or 0),
            "pPurchaseIndentId": int(header.get("indent_id") or 0),
            "Against": header.get("against_name") or header.get("against"),
            "QuotationId": int(header.get("quotation_id") or 0),
            "TotalFORe": for_total,
            "TotalExciseAmt": excise_total,
            "AgainstId": int(header.get("against_id") or 0),
            "Status": status_code,
            "PurchasingDeptId": purchasing_dept_id if purchasing_dept_id > 0 else None,
            "OverallDiscountMode": header.get("overall_discount_mode"),
            "OverallDiscountValue": _safe_float(header.get("overall_discount_value")),
            "POPrintFormat": selected_po_print_format,
            "pUpdatedby": int(header.get("updated_by") or 0),
            "pUpdatedon": now.strftime("%Y-%m-%d %H:%M:%S"),
        }

        po_no = str(header.get("po_no") or existing_header.get("PONo") or "").strip()
        if not po_no:
            po_no = f"PO-{po_id}"

        detail_errors = []
        detail_params = []
        po_write_conn = data_fetch.get_sql_connection(unit)
        if not po_write_conn:
            _audit_log_event(
                "purchase",
                "po_update",
                status="error",
                entity_type="po",
                entity_id=str(po_id),
                unit=unit,
                summary="PO update failed",
                details={"error": f"Could not connect to {unit}", "po_no": po_no},
            )
            return jsonify({"status": "error", "message": f"Could not connect to {unit}"}), 500

        try:
            try:
                po_write_conn.autocommit = False
            except Exception:
                try:
                    po_write_conn.driver_connection.autocommit = False
                except Exception:
                    pass

            mst_result = data_fetch.update_iv_po_mst(
                unit,
                po_params,
                conn=po_write_conn,
                manage_transaction=False,
            )
            if mst_result.get("error"):
                detail_errors.append(str(mst_result.get("error")))

            if not detail_errors:
                clear_result = data_fetch.clear_iv_po_dtl(
                    unit,
                    po_id,
                    conn=po_write_conn,
                    manage_transaction=False,
                )
                if clear_result.get("error"):
                    detail_errors.append(str(clear_result.get("error")))

            if not detail_errors:
                for row_idx, item in enumerate(items, start=1):
                    try:
                        cgst_pct = float(item.get("cgst_pct") or 0)
                        sgst_pct = float(item.get("sgst_pct") or 0)
                        igst_pct = float(item.get("igst_pct") or 0)
                        cgst_amt = float(item.get("cgst_amt") or 0)
                        sgst_amt = float(item.get("sgst_amt") or 0)
                        igst_amt = float(item.get("igst_amt") or 0)

                        if igst_pct and not (cgst_pct or sgst_pct):
                            cgst_pct = igst_pct / 2
                            sgst_pct = igst_pct / 2
                        if igst_amt and not (cgst_amt or sgst_amt):
                            cgst_amt = igst_amt / 2
                            sgst_amt = igst_amt / 2

                        item_params = {
                            "pId": int(item.get("id") or 0),
                            "pPoid": int(po_id),
                            "pItemid": int(item.get("item_id") or 0),
                            "pQty": float(item.get("qty") or 0),
                            "pPackSizeId": int(item.get("pack_size_id") or 0),
                            "UnitName": str(item.get("unit") or "").strip(),
                            "pRate": float(item.get("rate") or 0),
                            "pFreeqty": float(item.get("free_qty") or 0),
                            "pDiscount": float(item.get("discount_pct") or 0),
                            "pTax": sgst_pct,
                            "pTaxamount": sgst_amt,
                            "pMRP": float(item.get("mrp") or 0),
                            "pVATOn": item.get("vat_on"),
                            "pVAT": item.get("vat"),
                            "pCustom1": item.get("custom1"),
                            "pCustom2": item.get("custom2"),
                            "pInsertedByUserID": session.get("username") or session.get("user"),
                            "pInsertedON": now.strftime("%Y-%m-%d %H:%M:%S"),
                            "pInsertedMacName": item.get("mac_name"),
                            "pInsertedMacID": item.get("mac_id"),
                            "pInsertedIPAddress": request.remote_addr,
                            "Fore": float(item.get("for_amt") or 0),
                            "Excisetax": cgst_pct,
                            "ExciseTaxamt": cgst_amt,
                            "NetAmount": float(item.get("net_amt") or 0),
                            "lendingRate": float(item.get("lending_rate") or 0),
                            "UnitFor": float(item.get("unit_for") or 0),
                            "UnitDiscount": float(item.get("unit_discount") or 0),
                            "_row_no": row_idx,
                            "_item_name": item.get("item_name"),
                        }
                        detail_params.append(item_params)
                    except Exception as e:
                        detail_errors.append(f"Row {row_idx}: {e}")

            if not detail_errors and detail_params:
                bulk_result = data_fetch.add_iv_po_dtl_many(
                    unit,
                    detail_params,
                    conn=po_write_conn,
                    manage_transaction=False,
                )
                if bulk_result.get("error"):
                    detail_errors.extend(list(bulk_result.get("errors") or [str(bulk_result.get("error"))]))
                else:
                    detail_errors.extend(list(bulk_result.get("errors") or []))

            if detail_errors:
                try:
                    po_write_conn.rollback()
                except Exception:
                    pass
                old_header = _normalize_po_header_for_audit(existing_header)
                new_header = _normalize_po_header_for_audit(header, totals, status_code)
                _audit_log_event(
                    "purchase",
                    "po_update",
                    status="error",
                    entity_type="po",
                    entity_id=str(po_id),
                    unit=unit,
                    summary="PO update failed and was rolled back",
                    details={
                        "po_no": po_no,
                        "header_changes": _diff_simple_fields(old_header, new_header, ["supplier_id", "supplier_name", "purchasing_dept_id", "senior_approval_authority_name", "senior_approval_authority_designation", "po_date", "status", "amount", "tax_total", "discount_total", "cmc_amc_warranty"]),
                        "items": _diff_po_items(existing_items, items),
                        "errors": detail_errors,
                    },
                )
                return jsonify({
                    "status": "error",
                    "message": "PO update failed. No PO changes were saved.",
                    "errors": detail_errors,
                }), 500

            po_write_conn.commit()
        except Exception as e:
            try:
                po_write_conn.rollback()
            except Exception:
                pass
            detail_errors = detail_errors or [str(e)]
            old_header = _normalize_po_header_for_audit(existing_header)
            new_header = _normalize_po_header_for_audit(header, totals, status_code)
            _audit_log_event(
                "purchase",
                "po_update",
                status="error",
                entity_type="po",
                entity_id=str(po_id),
                unit=unit,
                summary="PO update failed and was rolled back",
                details={
                    "po_no": po_no,
                    "header_changes": _diff_simple_fields(old_header, new_header, ["supplier_id", "supplier_name", "purchasing_dept_id", "senior_approval_authority_name", "senior_approval_authority_designation", "po_date", "status", "amount", "tax_total", "discount_total", "cmc_amc_warranty"]),
                    "items": _diff_po_items(existing_items, items),
                    "errors": detail_errors,
                },
            )
            return jsonify({
                "status": "error",
                "message": "PO update failed. No PO changes were saved.",
                "errors": detail_errors,
            }), 500
        finally:
            try:
                po_write_conn.close()
            except Exception:
                pass

        try:
            po_no_fix = data_fetch.ensure_purchase_po_number(unit, int(po_id), preferred_po_no=po_no)
            ensured_po_no = str(po_no_fix.get("po_no") or "").strip()
            if ensured_po_no:
                po_no = ensured_po_no
        except Exception:
            pass

        otp_request_id = None
        if status_code == "P":
            otp_result = _create_purchase_otp_request(
                unit=unit,
                po_no=po_no,
                po_id=po_id,
                amount=amount_total,
                supplier_name=header.get("supplier_name") or "",
                reason=header.get("subject") or header.get("notes") or "",
                requested_by=session.get("username") or session.get("user") or "",
                purchasing_dept_id=purchasing_dept_id if purchasing_dept_id > 0 else None,
            )
            if otp_result.get("error"):
                _audit_log_event(
                    "purchase",
                    "po_update",
                    status="error",
                    entity_type="po",
                    entity_id=str(po_id),
                    unit=unit,
                    summary="OTP request failed",
                    details={"error": otp_result.get("error"), "po_no": po_no},
                )
                return jsonify({"status": "error", "message": otp_result["error"]}), 500
            otp_request_id = otp_result.get("request_id")
            if otp_request_id:
                _insert_purchase_otp_request(po_id, po_no, unit, int(otp_request_id), session.get("username") or session.get("user"))

        response = {"status": "success", "po_id": po_id, "po_no": po_no}
        if otp_request_id:
            response["request_id"] = otp_request_id
        if created_items:
            response["created_items"] = created_items
        old_header = _normalize_po_header_for_audit(existing_header)
        new_header = _normalize_po_header_for_audit(header, totals, status_code)
        _audit_log_event(
            "purchase",
            "po_update",
            status="success",
            entity_type="po",
            entity_id=str(po_id),
            unit=unit,
            summary="PO updated",
            details={
                "po_no": po_no,
                "header_changes": _diff_simple_fields(old_header, new_header, ["supplier_id", "supplier_name", "purchasing_dept_id", "senior_approval_authority_name", "senior_approval_authority_designation", "po_date", "status", "amount", "tax_total", "discount_total", "cmc_amc_warranty"]),
                "items": _diff_po_items(existing_items, items),
                "otp_request_id": otp_request_id,
                "created_items": created_items,
            },
            request_id=str(otp_request_id) if otp_request_id else None,
        )
        return jsonify(response)


    @app.route('/api/purchase/po_approve', methods=['POST'])
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_purchase_po_approve():
        unit, error = _get_purchase_unit()
        if error:
            return error

        payload = request.get_json(silent=True) or {}
        po_id = int(payload.get("po_id") or 0)
        request_id = int(payload.get("request_id") or 0)
        otp = (payload.get("otp") or "").strip()
        auto_email_requested = _is_truthy(payload.get("auto_email_supplier_pdf"))
        supplier_email_override = _purchase_normalize_email(
            payload.get("supplier_email") or payload.get("to_email") or payload.get("email")
        )
        raw_auto_email_format = str(payload.get("auto_email_format") or payload.get("format") or "").strip().lower()
        auto_email_print_format = "standard"

        if not po_id or not request_id or not otp:
            _audit_log_event(
                "purchase",
                "po_approve",
                status="error",
                entity_type="po",
                entity_id=str(po_id) if po_id else None,
                unit=unit,
                summary="PO ID, request ID, and OTP are required",
            )
            return jsonify({"status": "error", "message": "PO ID, request ID, and OTP are required"}), 400

        header_df = data_fetch.fetch_purchase_po_header(unit, po_id=po_id)
        if header_df is None:
            _audit_log_event(
                "purchase",
                "po_approve",
                status="error",
                entity_type="po",
                entity_id=str(po_id),
                unit=unit,
                summary="Failed to fetch PO",
            )
            return jsonify({"status": "error", "message": "Failed to fetch PO"}), 500
        if header_df.empty:
            _audit_log_event(
                "purchase",
                "po_approve",
                status="error",
                entity_type="po",
                entity_id=str(po_id),
                unit=unit,
                summary="PO not found",
            )
            return jsonify({"status": "error", "message": "PO not found"}), 404
        header_df = _clean_df_columns(header_df)
        header_row = header_df.iloc[0].to_dict()
        status_code = str(header_row.get("Status") or "").strip().upper()
        if status_code != "P":
            _audit_log_event(
                "purchase",
                "po_approve",
                status="error",
                entity_type="po",
                entity_id=str(po_id),
                unit=unit,
                summary="Only Pending Approval POs can be approved",
                details={"current_status": status_code},
            )
            return jsonify({"status": "error", "message": "Only Pending Approval POs can be approved"}), 400

        otp_result = _validate_purchase_otp(request_id, otp)
        if otp_result.get("error"):
            _audit_log_event(
                "purchase",
                "po_approve",
                status="error",
                entity_type="po",
                entity_id=str(po_id),
                unit=unit,
                summary="OTP validation failed",
                details={"error": otp_result.get("error")},
                request_id=str(request_id),
            )
            return jsonify({"status": "error", "message": otp_result["error"]}), 500
        if not otp_result.get("valid"):
            _audit_log_event(
                "purchase",
                "po_approve",
                status="error",
                entity_type="po",
                entity_id=str(po_id),
                unit=unit,
                summary="Invalid OTP",
                details={"message": otp_result.get("message")},
                request_id=str(request_id),
            )
            return jsonify({"status": "error", "message": otp_result.get("message") or "Invalid OTP"}), 400

        req_type = str(otp_result.get("request_type") or "").strip().upper()
        if req_type and req_type != OTP_PO_REQUEST_TYPE:
            _audit_log_event(
                "purchase",
                "po_approve",
                status="error",
                entity_type="po",
                entity_id=str(po_id),
                unit=unit,
                summary="OTP request type mismatch",
                details={"request_type": req_type},
                request_id=str(request_id),
            )
            return jsonify({"status": "error", "message": "OTP request type mismatch"}), 400

        otp_po_no = str(otp_result.get("bill_no") or "").strip().upper()
        if po_id > 0 and not str(header_row.get("PONo") or "").strip():
            try:
                po_no_fix = data_fetch.ensure_purchase_po_number(unit, po_id, preferred_po_no=otp_po_no or None)
                ensured_po_no = str(po_no_fix.get("po_no") or "").strip()
                if ensured_po_no:
                    header_row["PONo"] = ensured_po_no
            except Exception:
                pass
        po_no = str(header_row.get("PONo") or "").strip().upper()
        if po_no and otp_po_no and po_no != otp_po_no:
            _audit_log_event(
                "purchase",
                "po_approve",
                status="error",
                entity_type="po",
                entity_id=str(po_id),
                unit=unit,
                summary="OTP does not match this PO",
                details={"po_no": po_no, "otp_po_no": otp_po_no},
                request_id=str(request_id),
            )
            return jsonify({"status": "error", "message": "OTP does not match this PO"}), 400

        status_result = data_fetch.update_po_status(unit, po_id, "A")
        if status_result.get("error"):
            _audit_log_event(
                "purchase",
                "po_approve",
                status="error",
                entity_type="po",
                entity_id=str(po_id),
                unit=unit,
                summary="PO approval failed",
                details={"error": status_result.get("error")},
                request_id=str(request_id),
            )
            return jsonify({"status": "error", "message": status_result["error"]}), 500

        try:
            approved_header_df = data_fetch.fetch_purchase_po_header(unit, po_id=po_id)
            if approved_header_df is not None and not approved_header_df.empty:
                approved_header_df = _clean_df_columns(approved_header_df)
                header_row = approved_header_df.iloc[0].to_dict()
        except Exception:
            header_row["Status"] = "A"

        user = session.get("username") or session.get("user") or ""
        _mark_central_otp_used(request_id, user)
        _mark_purchase_otp_used(po_id, request_id, user)
        try:
            req_meta = _fetch_purchase_otp_request_by_id(request_id)
            mail_result = _send_purchase_po_approval_email(unit, po_id, header_row, req_meta)
            if mail_result.get("status") == "error":
                print(f"PO approval email failed: {mail_result.get('message')}")
        except Exception as e:
            print(f"PO approval email error: {e}")

        auto_email_result = {
            "requested": bool(auto_email_requested),
            "status": "skipped",
            "message": "Auto email option not selected",
        }
        if auto_email_requested:
            po_no_val = str(header_row.get("PONo") or f"PO-{po_id}")
            supplier_id = _safe_int(header_row.get("SupplierID"))
            supplier_email = _purchase_normalize_email(header_row.get("SupplierEmail"))
            if _purchase_is_valid_email(supplier_email_override):
                supplier_email = supplier_email_override
                header_row["SupplierEmail"] = supplier_email
            if not _purchase_is_valid_email(supplier_email):
                auto_email_result = {
                    "requested": True,
                    "status": "skipped",
                    "message": "Supplier email is missing or invalid",
                    "recipient": supplier_email,
                }
                _audit_log_event(
                    "purchase",
                    "po_auto_email_on_approve",
                    status="error",
                    entity_type="po",
                    entity_id=str(po_id),
                    unit=unit,
                    summary="Supplier auto-email skipped due to invalid email",
                    details={
                        "po_no": po_no_val,
                        "supplier_id": supplier_id,
                        "recipient": supplier_email,
                    },
                    request_id=str(request_id),
                )
            else:
                try:
                    items_df = data_fetch.fetch_purchase_po_items(unit, po_id)
                    if items_df is None:
                        raise RuntimeError("Failed to fetch PO items for supplier email")
                    items_df = _clean_df_columns(items_df)
                    items = items_df.to_dict(orient="records") if not items_df.empty else []

                    approval_meta = _fetch_purchase_approval_meta(po_id)
                    header_pdf = dict(header_row)
                    header_pdf["Status"] = "A"
                    header_pdf["SupplierEmail"] = supplier_email
                    header_pdf["PurchasingDeptName"] = _resolve_purchasing_department_name(
                        _safe_int(header_row.get("PurchasingDeptId"))
                    )
                    pdf_buffer = _build_po_pdf_buffer(
                        unit,
                        header_pdf,
                        items,
                        approval_meta,
                        print_format=auto_email_print_format,
                    )
                    pdf_bytes = pdf_buffer.getvalue() if pdf_buffer else b""

                    po_date_val = header_row.get("PODate")
                    if isinstance(po_date_val, (datetime, date)):
                        po_date_text = po_date_val.strftime("%d-%b-%Y")
                    else:
                        po_date_text = str(po_date_val or "")[:10]
                    snapshot = {
                        "unit": unit,
                        "po_no": po_no_val,
                        "po_date": po_date_text,
                        "supplier": str(header_row.get("SupplierName") or ""),
                        "amount": _format_indian_currency(header_row.get("Amount") or 0),
                        "subject": str(header_row.get("Subject") or ""),
                    }
                    mail_subject = f"Purchase Order {po_no_val}"
                    mail_body = _build_po_supplier_dispatch_email_body(snapshot)
                    mail_filename = f"PO_{po_no_val}.pdf"
                    mail_result_supplier = _send_graph_mail_with_attachment(
                        subject=mail_subject,
                        body_html=mail_body,
                        to_recipients=[supplier_email],
                        filename=mail_filename,
                        content_bytes=pdf_bytes,
                    )
                    if str(mail_result_supplier.get("status") or "").strip().lower() == "success":
                        auto_email_result = {
                            "requested": True,
                            "status": "success",
                            "recipient": supplier_email,
                        }
                        _audit_log_event(
                            "purchase",
                            "po_auto_email_on_approve",
                            status="success",
                            entity_type="po",
                            entity_id=str(po_id),
                            unit=unit,
                            summary="Supplier PO PDF auto-emailed on approval",
                            details={
                                "po_no": po_no_val,
                                "supplier_id": supplier_id,
                                "recipient": supplier_email,
                            },
                            request_id=str(request_id),
                        )
                    else:
                        err_msg = str(mail_result_supplier.get("message") or "Failed to send supplier email")
                        auto_email_result = {
                            "requested": True,
                            "status": "error",
                            "recipient": supplier_email,
                            "message": err_msg,
                        }
                        _audit_log_event(
                            "purchase",
                            "po_auto_email_on_approve",
                            status="error",
                            entity_type="po",
                            entity_id=str(po_id),
                            unit=unit,
                            summary="Supplier PO PDF auto-email failed",
                            details={
                                "po_no": po_no_val,
                                "supplier_id": supplier_id,
                                "recipient": supplier_email,
                                "mail_result": mail_result_supplier,
                            },
                            request_id=str(request_id),
                        )
                except Exception as e:
                    auto_email_result = {
                        "requested": True,
                        "status": "error",
                        "recipient": supplier_email,
                        "message": str(e),
                    }
                    _audit_log_event(
                        "purchase",
                        "po_auto_email_on_approve",
                        status="error",
                        entity_type="po",
                        entity_id=str(po_id),
                        unit=unit,
                        summary="Supplier PO PDF auto-email error",
                        details={
                            "po_no": po_no_val,
                            "supplier_id": supplier_id,
                            "recipient": supplier_email,
                            "error": str(e),
                        },
                        request_id=str(request_id),
                    )

        _audit_log_event(
            "purchase",
            "po_approve",
            status="success",
            entity_type="po",
            entity_id=str(po_id),
            unit=unit,
            summary="PO approved",
            details={
                "po_no": header_row.get("PONo"),
                "auto_email": auto_email_result if auto_email_result.get("requested") else None,
            },
            request_id=str(request_id),
        )
        return jsonify(
            {
                "status": "success",
                "po_id": po_id,
                "po_no": header_row.get("PONo"),
                "po_approval_date": header_row.get("POApprovalDate"),
                "auto_email": auto_email_result,
            }
        )


    @app.route('/api/purchase/po_status_override', methods=['POST'])
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_purchase_po_status_override():
        unit, error = _get_purchase_unit()
        if error:
            return error

        payload = request.get_json(silent=True) or {}
        po_id = _safe_int(payload.get("po_id"))
        target_status_raw = str(payload.get("target_status") or payload.get("status") or "").strip().lower()
        target_status = {
            "draft": "D",
            "d": "D",
            "cancel": "C",
            "cancelled": "C",
            "canceled": "C",
            "c": "C",
        }.get(target_status_raw)

        if not _is_it_user():
            _audit_log_event(
                "purchase",
                "po_status_override",
                status="error",
                entity_type="po",
                entity_id=str(po_id) if po_id > 0 else None,
                unit=unit,
                summary="Only IT can change PO status to Draft or Cancelled",
                details={"target_status": target_status_raw},
            )
            return jsonify({"status": "error", "message": "Only IT users can change PO status to Draft or Cancelled."}), 403

        if po_id <= 0:
            _audit_log_event(
                "purchase",
                "po_status_override",
                status="error",
                entity_type="po",
                unit=unit,
                summary="PO ID is required",
                details={"target_status": target_status_raw},
            )
            return jsonify({"status": "error", "message": "PO ID is required"}), 400

        if target_status not in {"D", "C"}:
            _audit_log_event(
                "purchase",
                "po_status_override",
                status="error",
                entity_type="po",
                entity_id=str(po_id),
                unit=unit,
                summary="Invalid PO target status",
                details={"target_status": target_status_raw},
            )
            return jsonify({"status": "error", "message": "Target status must be Draft or Cancelled"}), 400

        header_df = data_fetch.fetch_purchase_po_header(unit, po_id=po_id)
        if header_df is None:
            _audit_log_event(
                "purchase",
                "po_status_override",
                status="error",
                entity_type="po",
                entity_id=str(po_id),
                unit=unit,
                summary="Failed to fetch PO for status change",
                details={"target_status": target_status},
            )
            return jsonify({"status": "error", "message": "Failed to fetch PO"}), 500
        if header_df.empty:
            _audit_log_event(
                "purchase",
                "po_status_override",
                status="error",
                entity_type="po",
                entity_id=str(po_id),
                unit=unit,
                summary="PO not found",
                details={"target_status": target_status},
            )
            return jsonify({"status": "error", "message": "PO not found"}), 404

        header_df = _clean_df_columns(header_df)
        header_row = header_df.iloc[0].to_dict()
        current_status = str(header_row.get("Status") or "").strip().upper()
        target_status_label = _po_status_label(target_status)
        po_no_val = str(header_row.get("PONo") or "").strip()
        if not po_no_val:
            try:
                po_no_fix = data_fetch.ensure_purchase_po_number(unit, po_id)
                ensured_po_no = str(po_no_fix.get("po_no") or "").strip()
                if ensured_po_no:
                    po_no_val = ensured_po_no
                    header_row["PONo"] = ensured_po_no
            except Exception:
                pass
        if not po_no_val:
            po_no_val = f"PO-{po_id}"

        if current_status == target_status:
            return jsonify(
                {
                    "status": "success",
                    "po_id": po_id,
                    "po_no": po_no_val,
                    "target_status_code": target_status,
                    "target_status_label": target_status_label,
                    "changed": False,
                    "message": f"PO is already {target_status_label.lower()}.",
                }
            )

        status_result = data_fetch.update_po_status(unit, po_id, target_status)
        if status_result.get("error"):
            _audit_log_event(
                "purchase",
                "po_status_override",
                status="error",
                entity_type="po",
                entity_id=str(po_id),
                unit=unit,
                summary="PO status update failed",
                details={
                    "po_no": po_no_val,
                    "from_status": current_status,
                    "to_status": target_status,
                    "error": status_result.get("error"),
                },
            )
            return jsonify({"status": "error", "message": status_result["error"]}), 500

        actor = str(session.get("username") or session.get("user") or "").strip()
        changed_at = datetime.now(tz=LOCAL_TZ)
        response = {
            "status": "success",
            "po_id": po_id,
            "po_no": po_no_val,
            "target_status_code": target_status,
            "target_status_label": target_status_label,
            "changed": True,
        }

        if target_status == "D":
            response["message"] = "PO status changed to Draft. You can edit this PO now."
            _audit_log_event(
                "purchase",
                "po_change_to_draft",
                status="success",
                entity_type="po",
                entity_id=str(po_id),
                unit=unit,
                summary="PO status changed to Draft",
                details={
                    "po_no": po_no_val,
                    "from_status": current_status,
                    "to_status": target_status,
                },
            )
            return jsonify(response)

        mail_result = {"status": "skipped", "message": "Cancellation email not sent"}
        supplier_email = _purchase_normalize_email(header_row.get("SupplierEmail"))
        recipients = []
        try:
            items_df = data_fetch.fetch_purchase_po_items(unit, po_id)
            if items_df is None:
                raise RuntimeError("Failed to fetch PO items for cancellation mail")
            items_df = _clean_df_columns(items_df)
            items_rows = items_df.to_dict(orient="records") if not items_df.empty else []

            approval_meta = _fetch_purchase_approval_meta(po_id)
            header_pdf = dict(header_row)
            header_pdf["Status"] = "C"
            header_pdf["PurchasingDeptName"] = _resolve_purchasing_department_name(_safe_int(header_row.get("PurchasingDeptId")))
            print_format = _resolve_po_print_format_for_render(
                unit,
                header_row=header_pdf,
                purchasing_dept_id=_safe_int(header_row.get("PurchasingDeptId")),
            )
            pdf_buffer = _build_po_pdf_buffer(unit, header_pdf, items_rows, approval_meta, print_format=print_format)
            pdf_bytes = pdf_buffer.getvalue() if pdf_buffer else b""

            snapshot = {
                "unit": unit,
                "po_no": po_no_val,
                "po_date": _format_snapshot_date(header_row.get("PODate")),
                "supplier": str(header_row.get("SupplierName") or ""),
                "amount": _format_indian_currency(header_row.get("Amount") or 0),
                "cancelled_by": actor or "-",
                "cancelled_at": _format_snapshot_date(changed_at),
                "subject": str(header_row.get("Subject") or ""),
            }
            snapshot_bytes = _build_po_cancellation_snapshot_image_bytes(
                unit,
                header_row,
                items_rows,
                cancelled_by=actor,
                cancelled_at=changed_at,
            )
            snapshot_attachment = {
                "filename": f"PO_{po_no_val}_cancel_snapshot.jpg",
                "content_type": "image/jpeg",
                "content_bytes": snapshot_bytes,
            }
            if not snapshot_bytes:
                snapshot_attachment = {
                    "filename": f"PO_{po_no_val}_cancel_snapshot.txt",
                    "content_type": "text/plain",
                    "content_bytes": _build_po_cancellation_snapshot_text_bytes(
                        unit,
                        header_row,
                        items_rows,
                        cancelled_by=actor,
                        cancelled_at=changed_at,
                    ),
                }

            recipients = []
            if _purchase_is_valid_email(supplier_email):
                recipients.append(supplier_email)
            recipients.extend(PO_CANCELLATION_INTERNAL_RECIPIENTS)
            recipients = [addr for addr in dict.fromkeys([str(addr or "").strip().lower() for addr in recipients]) if addr]

            mail_subject = f"PO Cancelled - {po_no_val}"
            mail_body = _build_po_cancellation_email_body(snapshot)
            mail_result = _send_graph_mail_with_attachment(
                subject=mail_subject,
                body_html=mail_body,
                to_recipients=recipients,
                attachments=[
                    snapshot_attachment,
                    {
                        "filename": f"PO_{po_no_val}_cancelled.pdf",
                        "content_type": "application/pdf",
                        "content_bytes": pdf_bytes,
                    },
                ],
            )
        except Exception as exc:
            mail_result = {"status": "error", "message": str(exc)}

        response["mail_status"] = {
            "status": str(mail_result.get("status") or ""),
            "message": str(mail_result.get("message") or ""),
            "recipients": recipients,
            "supplier_email": supplier_email,
        }

        if str(mail_result.get("status") or "").strip().lower() == "success":
            if _purchase_is_valid_email(supplier_email):
                response["message"] = "PO cancelled and notification email sent to vendor and internal recipients."
            else:
                response["message"] = "PO cancelled. Supplier email was missing or invalid, so notification went to internal recipients only."
        else:
            response["message"] = f"PO cancelled, but notification email failed: {mail_result.get('message') or 'dispatch failed'}"

        _audit_log_event(
            "purchase",
            "po_cancel",
            status="success" if str(mail_result.get("status") or "").strip().lower() == "success" else "partial",
            entity_type="po",
            entity_id=str(po_id),
            unit=unit,
            summary="PO cancelled",
            details={
                "po_no": po_no_val,
                "from_status": current_status,
                "to_status": target_status,
                "cancelled_by": actor,
                "cancelled_at": changed_at.isoformat(timespec="seconds"),
                "mail_result": response.get("mail_status"),
            },
        )
        return jsonify(response)


    @app.route('/api/purchase/po_resend_otp', methods=['POST'])
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_purchase_po_resend_otp():
        unit, error = _get_purchase_unit()
        if error:
            return error
        try:
            data_fetch.ensure_po_purchasing_dept_column(unit)
        except Exception:
            pass

        payload = request.get_json(silent=True) or {}
        po_id = int(payload.get("po_id") or 0)
        if not po_id:
            _audit_log_event(
                "purchase",
                "po_resend_otp",
                status="error",
                entity_type="po",
                unit=unit,
                summary="PO ID is required",
            )
            return jsonify({"status": "error", "message": "PO ID is required"}), 400

        header_df = data_fetch.fetch_purchase_po_header(unit, po_id=po_id)
        if header_df is None:
            _audit_log_event(
                "purchase",
                "po_resend_otp",
                status="error",
                entity_type="po",
                entity_id=str(po_id),
                unit=unit,
                summary="Failed to fetch PO",
            )
            return jsonify({"status": "error", "message": "Failed to fetch PO"}), 500
        if header_df.empty:
            _audit_log_event(
                "purchase",
                "po_resend_otp",
                status="error",
                entity_type="po",
                entity_id=str(po_id),
                unit=unit,
                summary="PO not found",
            )
            return jsonify({"status": "error", "message": "PO not found"}), 404
        header_df = _clean_df_columns(header_df)
        header_row = header_df.iloc[0].to_dict()
        status_code = str(header_row.get("Status") or "").strip().upper()
        if status_code != "P":
            _audit_log_event(
                "purchase",
                "po_resend_otp",
                status="error",
                entity_type="po",
                entity_id=str(po_id),
                unit=unit,
                summary="OTP can be resent only for Pending Approval POs",
                details={"current_status": status_code},
            )
            return jsonify({"status": "error", "message": "OTP can be resent only for Pending Approval POs"}), 400

        po_no = header_row.get("PONo") or f"PO-{po_id}"
        amount_total = header_row.get("Amount") or 0
        purchasing_dept_id = _safe_int(header_row.get("PurchasingDeptId"))
        otp_result = _create_purchase_otp_request(
            unit=unit,
            po_no=po_no,
            po_id=po_id,
            amount=amount_total,
            supplier_name=header_row.get("SupplierName") or "",
            reason=header_row.get("Subject") or header_row.get("Notes") or "",
            requested_by=session.get("username") or session.get("user") or "",
            purchasing_dept_id=purchasing_dept_id if purchasing_dept_id > 0 else None,
        )
        if otp_result.get("error"):
            _audit_log_event(
                "purchase",
                "po_resend_otp",
                status="error",
                entity_type="po",
                entity_id=str(po_id),
                unit=unit,
                summary="OTP request failed",
                details={"error": otp_result.get("error"), "po_no": po_no},
            )
            return jsonify({"status": "error", "message": otp_result["error"]}), 500
        otp_request_id = otp_result.get("request_id")
        if otp_request_id:
            _insert_purchase_otp_request(po_id, po_no, unit, int(otp_request_id), session.get("username") or session.get("user"))

        _audit_log_event(
            "purchase",
            "po_resend_otp",
            status="success",
            entity_type="po",
            entity_id=str(po_id),
            unit=unit,
            summary="OTP resent",
            details={"po_no": po_no, "request_id": otp_request_id},
            request_id=str(otp_request_id) if otp_request_id else None,
        )
        return jsonify({"status": "success", "po_id": po_id, "po_no": po_no, "request_id": otp_request_id})


    @app.route('/api/purchase/po_print')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_purchase_po_print():
        unit, error = _get_purchase_unit()
        if error:
            return error
        po_id_raw = request.args.get("po_id")
        po_no = (request.args.get("po_no") or "").strip()
        po_id = int(po_id_raw) if str(po_id_raw or "").isdigit() else None
        raw_format = (request.args.get("format") or "").strip().lower()
        if not po_id and not po_no:
            return jsonify({"status": "error", "message": "PO ID or PO number is required"}), 400

        header_df = data_fetch.fetch_purchase_po_header(unit, po_id=po_id, po_no=po_no)
        if header_df is None:
            return jsonify({"status": "error", "message": "Failed to fetch PO"}), 500
        if header_df.empty:
            return jsonify({"status": "error", "message": "PO not found"}), 404
        header_df = _clean_df_columns(header_df)
        header = header_df.iloc[0].to_dict()
        header["PurchasingDeptName"] = _resolve_purchasing_department_name(_safe_int(header.get("PurchasingDeptId")))
        print_format = _resolve_po_print_format_for_render(
            unit,
            requested_format=raw_format,
            header_row=header,
            purchasing_dept_id=_safe_int(header.get("PurchasingDeptId")),
        )
        po_id = int(header.get("ID") or po_id or 0)
        if po_id > 0 and not str(header.get("PONo") or "").strip():
            try:
                po_no_fix = data_fetch.ensure_purchase_po_number(unit, po_id, preferred_po_no=po_no)
                ensured_po_no = str(po_no_fix.get("po_no") or "").strip()
                if ensured_po_no:
                    header["PONo"] = ensured_po_no
            except Exception:
                pass
        items_df = data_fetch.fetch_purchase_po_items(unit, po_id)
        if items_df is None:
            return jsonify({"status": "error", "message": "Failed to fetch PO items"}), 500
        items_df = _clean_df_columns(items_df)
        items = items_df.to_dict(orient="records") if not items_df.empty else []
        approval_meta = _fetch_purchase_approval_meta(po_id)

        pdf_buffer = _build_po_pdf_buffer(unit, header, items, approval_meta, print_format=print_format)
        filename = f"PO_{header.get('PONo') or po_no or po_id}.pdf"
        return send_file(
            pdf_buffer,
            mimetype="application/pdf",
            as_attachment=False,
            download_name=filename,
        )


    @app.route('/api/purchase/po_email_pdf', methods=['POST'])
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_purchase_po_email_pdf():
        unit, error = _get_purchase_unit()
        if error:
            return error

        payload = request.get_json(silent=True) or {}
        po_id = _safe_int(payload.get("po_id"))
        po_no = str(payload.get("po_no") or "").strip()
        to_email = _purchase_normalize_email(payload.get("to_email") or payload.get("email"))
        raw_format = str(payload.get("format") or "").strip().lower()

        if po_id <= 0 and not po_no:
            _audit_log_event(
                "purchase",
                "po_email_pdf",
                status="error",
                entity_type="po",
                unit=unit,
                summary="PO ID or PO number is required",
            )
            return jsonify({"status": "error", "message": "PO ID or PO number is required"}), 400
        if not _purchase_is_valid_email(to_email):
            _audit_log_event(
                "purchase",
                "po_email_pdf",
                status="error",
                entity_type="po",
                entity_id=str(po_id) if po_id > 0 else None,
                unit=unit,
                summary="Invalid recipient email",
                details={"to_email": to_email, "po_no": po_no},
            )
            return jsonify({"status": "error", "message": "Enter a valid recipient email address"}), 400

        header_df = data_fetch.fetch_purchase_po_header(
            unit,
            po_id=po_id if po_id > 0 else None,
            po_no=po_no,
        )
        if header_df is None:
            _audit_log_event(
                "purchase",
                "po_email_pdf",
                status="error",
                entity_type="po",
                entity_id=str(po_id) if po_id > 0 else None,
                unit=unit,
                summary="Failed to fetch PO",
                details={"po_no": po_no},
            )
            return jsonify({"status": "error", "message": "Failed to fetch PO"}), 500
        if header_df.empty:
            _audit_log_event(
                "purchase",
                "po_email_pdf",
                status="error",
                entity_type="po",
                entity_id=str(po_id) if po_id > 0 else None,
                unit=unit,
                summary="PO not found",
                details={"po_no": po_no},
            )
            return jsonify({"status": "error", "message": "PO not found"}), 404

        header_df = _clean_df_columns(header_df)
        header_row = header_df.iloc[0].to_dict()
        print_format = _resolve_po_print_format_for_render(
            unit,
            requested_format=raw_format,
            header_row=header_row,
            purchasing_dept_id=_safe_int(header_row.get("PurchasingDeptId")),
        )
        po_id_val = _safe_int(header_row.get("ID"), po_id)
        if po_id_val > 0 and not str(header_row.get("PONo") or "").strip():
            try:
                po_no_fix = data_fetch.ensure_purchase_po_number(unit, po_id_val, preferred_po_no=po_no)
                ensured_po_no = str(po_no_fix.get("po_no") or "").strip()
                if ensured_po_no:
                    header_row["PONo"] = ensured_po_no
            except Exception:
                pass
        po_no_val = str(header_row.get("PONo") or po_no or f"PO-{po_id_val}")
        status_code = str(header_row.get("Status") or "").strip().upper()
        if status_code != "A":
            _audit_log_event(
                "purchase",
                "po_email_pdf",
                status="error",
                entity_type="po",
                entity_id=str(po_id_val),
                unit=unit,
                summary="Only approved POs can be emailed",
                details={"po_no": po_no_val, "status": status_code},
            )
            return jsonify({"status": "error", "message": "Only approved POs can be emailed"}), 400

        supplier_id = _safe_int(header_row.get("SupplierID"))
        if supplier_id <= 0:
            _audit_log_event(
                "purchase",
                "po_email_pdf",
                status="error",
                entity_type="po",
                entity_id=str(po_id_val),
                unit=unit,
                summary="Supplier not linked to PO",
                details={"po_no": po_no_val},
            )
            return jsonify({"status": "error", "message": "Supplier not linked to PO"}), 400

        email_update_result = data_fetch.update_iv_supplier_email(unit, supplier_id, to_email)
        if email_update_result.get("error"):
            err_msg = str(email_update_result.get("error") or "Failed to update supplier email")
            status_code = 404 if "not found" in err_msg.lower() else 500
            _audit_log_event(
                "purchase",
                "po_email_pdf",
                status="error",
                entity_type="po",
                entity_id=str(po_id_val),
                unit=unit,
                summary="Failed to update supplier email before dispatch",
                details={"po_no": po_no_val, "supplier_id": supplier_id, "to_email": to_email, "error": err_msg},
            )
            return jsonify({"status": "error", "message": err_msg}), status_code

        items_df = data_fetch.fetch_purchase_po_items(unit, po_id_val)
        if items_df is None:
            _audit_log_event(
                "purchase",
                "po_email_pdf",
                status="error",
                entity_type="po",
                entity_id=str(po_id_val),
                unit=unit,
                summary="Failed to fetch PO items",
                details={"po_no": po_no_val},
            )
            return jsonify({"status": "error", "message": "Failed to fetch PO items"}), 500
        items_df = _clean_df_columns(items_df)
        items = items_df.to_dict(orient="records") if not items_df.empty else []

        approval_meta = _fetch_purchase_approval_meta(po_id_val)
        header_pdf = dict(header_row)
        header_pdf["PurchasingDeptName"] = _resolve_purchasing_department_name(_safe_int(header_row.get("PurchasingDeptId")))
        header_pdf["SupplierEmail"] = to_email
        pdf_buffer = _build_po_pdf_buffer(unit, header_pdf, items, approval_meta, print_format=print_format)
        pdf_bytes = pdf_buffer.getvalue() if pdf_buffer else b""

        po_date_val = header_row.get("PODate")
        if isinstance(po_date_val, (datetime, date)):
            po_date_text = po_date_val.strftime("%d-%b-%Y")
        else:
            po_date_text = str(po_date_val or "")[:10]
        snapshot = {
            "unit": unit,
            "po_no": po_no_val,
            "po_date": po_date_text,
            "supplier": str(header_row.get("SupplierName") or ""),
            "amount": _format_indian_currency(header_row.get("Amount") or 0),
            "subject": str(header_row.get("Subject") or ""),
        }
        mail_subject = f"Purchase Order {po_no_val}"
        mail_body = _build_po_supplier_dispatch_email_body(snapshot)
        mail_filename = f"PO_{po_no_val}.pdf"
        mail_result = _send_graph_mail_with_attachment(
            subject=mail_subject,
            body_html=mail_body,
            to_recipients=[to_email],
            filename=mail_filename,
            content_bytes=pdf_bytes,
        )
        if str(mail_result.get("status") or "").strip().lower() != "success":
            err_msg = str(mail_result.get("message") or "Failed to send email")
            _audit_log_event(
                "purchase",
                "po_email_pdf",
                status="error",
                entity_type="po",
                entity_id=str(po_id_val),
                unit=unit,
                summary="PO email send failed",
                details={
                    "po_no": po_no_val,
                    "supplier_id": supplier_id,
                    "recipient": to_email,
                    "mail_result": mail_result,
                },
            )
            return jsonify({"status": "error", "message": err_msg, "mail_status": mail_result}), 500

        _audit_log_event(
            "purchase",
            "po_email_pdf",
            status="success",
            entity_type="po",
            entity_id=str(po_id_val),
            unit=unit,
            summary="PO PDF emailed to supplier",
            details={
                "po_no": po_no_val,
                "supplier_id": supplier_id,
                "recipient": to_email,
                "mail_result": mail_result,
            },
        )
        return jsonify({
            "status": "success",
            "message": f"PO PDF emailed to {to_email}",
            "po_id": po_id_val,
            "po_no": po_no_val,
            "recipient": to_email,
            "mail_status": mail_result,
        })


