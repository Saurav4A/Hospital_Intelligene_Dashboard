from datetime import date, datetime
from decimal import Decimal, ROUND_HALF_UP
from io import BytesIO
from xml.sax.saxutils import escape

from flask import jsonify, render_template, request, send_file, session, url_for

from modules import data_fetch


PRINT_PROFILE = {
    "name": "ASARFI MEDICAL",
    "subtitle": "A UNIT OF ASARFI HOSPITAL LIMITED",
    "address": "Baramuri PO - Bishunpur Polytechnic, Dhanbad, Jh - 828130",
    "licence": "DL No. FORM 20/21/20F JH-DH1-144833/34/35",
    "gst_no": "GST No.-20AAFCA4125L1Z2",
    "tin_no": "Tin No. 20861605701",
    "phone": "9234302735",
    "notes": [
        "Goods sold will not be taken back after 15 days.",
        "Consult your doctor before using medicine.",
        "All disputes subject to Dhanbad jurisdiction only.",
    ],
}


def register_pharmacy_sales_routes(
    app,
    *,
    login_required,
    has_section_access,
    allowed_units_for_session,
    clean_df_columns,
    sanitize_json_payload,
    safe_float,
    safe_int,
    audit_log_event,
    local_tz,
):
    _has_section_access = has_section_access
    _allowed_units_for_session = allowed_units_for_session
    _clean_df_columns = clean_df_columns
    _sanitize_json_payload = sanitize_json_payload
    _safe_float = safe_float
    _safe_int = safe_int
    _audit_log_event = audit_log_event
    LOCAL_TZ = local_tz
    SHARPSIGHT_UNIT = data_fetch.PHARMACY_SALE_ALLOWED_UNIT
    STORE_ID = data_fetch.PHARMACY_SALE_STORE_ID
    STORE_NAME = "Eye Pharmacy"
    STORE_CODE = "EYE"
    MRP_EDIT_SECTION = "pharmacy_sales_mrp_edit"

    def _sales_unit():
        return SHARPSIGHT_UNIT, None

    def _can_edit_mrp() -> bool:
        return bool(_has_section_access(MRP_EDIT_SECTION))

    def _format_date(value) -> str:
        if value is None:
            return ""
        raw_text = str(value).strip()
        if raw_text.lower() in {"", "nan", "nat", "none"}:
            return ""
        if isinstance(value, datetime):
            return value.date().isoformat()
        if isinstance(value, date):
            return value.isoformat()
        txt = raw_text
        return txt[:10] if txt else ""

    def _format_display_datetime(value) -> str:
        if value is None:
            return "-"
        raw_text = str(value).strip()
        if raw_text.lower() in {"", "nan", "nat", "none"}:
            return "-"
        if isinstance(value, datetime):
            return value.strftime("%d-%b-%Y %I:%M %p")
        if isinstance(value, date):
            return value.strftime("%d-%b-%Y")
        txt = raw_text
        if not txt:
            return "-"
        for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                return datetime.strptime(txt[:26], fmt).strftime("%d-%b-%Y %I:%M %p" if "H" in fmt else "%d-%b-%Y")
            except Exception:
                continue
        return txt

    def _format_amount(value) -> str:
        try:
            return f"{float(value or 0):.2f}"
        except Exception:
            return "0.00"

    def _amount_in_words(amount_value) -> str:
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

    def _normalize_rows(df, mapping: dict) -> list[dict]:
        rows = []
        if df is None or df.empty:
            return rows
        df = _clean_df_columns(df)
        cols = {str(col).strip().lower(): col for col in df.columns}
        for _, row in df.iterrows():
            rec = {}
            for out_key, source in mapping.items():
                col = cols.get(source.lower())
                rec[out_key] = row.get(col) if col is not None else None
            rows.append(rec)
        return rows

    def _receipt_allowed_for_row(row: dict | None) -> bool:
        rec = row if isinstance(row, dict) else {}
        return data_fetch._pharmacy_sale_receipt_allowed(rec.get("type_of_visit"), rec.get("tmp_patient_id"))

    def _normalize_report_rows(df) -> list[dict]:
        rows = _normalize_rows(df, {
            "bill_id": "billid", "bill_no": "billno", "bill_date": "billdate", "sale_date": "saledate",
            "gross_amount": "grossamount", "discount_amount": "discountamount", "net_amount": "netamount", "due_amount": "dueamount",
            "received_amount": "receivedamount", "receipt_count": "receiptcount", "last_receipt_date": "lastreceiptdate",
            "registration_no": "registrationno", "sharpsight_uhid": "sharpsightuhid", "tmp_patient_id": "tmppatientid",
            "visit_id": "visitid", "visit_no": "visitno", "admission_no": "admissionno", "type_of_visit": "typeofvisit", "visit_date": "visitdate",
            "patient_name": "patientname", "prescribed_by": "prescribedby", "drug_sale_id": "drugsaleid", "drug_sale_code": "drugsalecode",
            "generated_by": "generatedby", "line_count": "linecount", "total_qty": "totalqty", "remarks": "remarks",
        })
        for row in rows:
            row["bill_id"] = _safe_int(row.get("bill_id"), 0)
            row["visit_id"] = _safe_int(row.get("visit_id"), 0)
            row["tmp_patient_id"] = _safe_int(row.get("tmp_patient_id"), 0)
            row["drug_sale_id"] = _safe_int(row.get("drug_sale_id"), 0)
            row["line_count"] = _safe_int(row.get("line_count"), 0)
            row["gross_amount"] = _safe_float(row.get("gross_amount"), 0)
            row["discount_amount"] = _safe_float(row.get("discount_amount"), 0)
            row["net_amount"] = _safe_float(row.get("net_amount"), 0)
            row["due_amount"] = _safe_float(row.get("due_amount"), 0)
            row["received_amount"] = _safe_float(row.get("received_amount"), 0)
            row["receipt_count"] = _safe_int(row.get("receipt_count"), 0)
            row["total_qty"] = _safe_float(row.get("total_qty"), 0)
            row["bill_date"] = _format_display_datetime(row.get("bill_date"))
            row["sale_date"] = _format_date(row.get("sale_date"))
            row["visit_date"] = _format_date(row.get("visit_date"))
            row["last_receipt_date"] = _format_display_datetime(row.get("last_receipt_date"))
            row["registration_no"] = str(row.get("registration_no") or "").strip()
            row["sharpsight_uhid"] = str(row.get("sharpsight_uhid") or "").strip()
            row["patient_name"] = str(row.get("patient_name") or "").strip()
            row["visit_no"] = str(row.get("visit_no") or "").strip()
            row["admission_no"] = str(row.get("admission_no") or "").strip()
            row["type_of_visit"] = str(row.get("type_of_visit") or "").strip()
            row["prescribed_by"] = str(row.get("prescribed_by") or "").strip()
            row["drug_sale_code"] = str(row.get("drug_sale_code") or "").strip()
            row["generated_by"] = str(row.get("generated_by") or "").strip()
            row["remarks"] = str(row.get("remarks") or "").strip()
            row["identity_label"] = "Self Walk-In" if row["tmp_patient_id"] > 0 else (row["registration_no"] or "-")
            if row["sharpsight_uhid"]:
                row["identity_label"] = f"{row['identity_label']} | UHID {row['sharpsight_uhid']}" if row["registration_no"] else f"UHID {row['sharpsight_uhid']}"
            row["visit_label"] = "Self Walk-In" if row["tmp_patient_id"] > 0 else (row["admission_no"] or row["visit_no"] or "-")
            row["receipt_allowed"] = _receipt_allowed_for_row(row)
        return rows

    def _normalize_issue_report_rows(df) -> list[dict]:
        rows = _normalize_rows(df, {
            "issue_id": "issueid", "issue_no": "issueno", "issue_date": "issuedate", "sale_date": "issuesaledate",
            "gross_amount": "grossamount", "discount_amount": "discountamount", "net_amount": "netamount", "due_amount": "dueamount",
            "registration_no": "registrationno", "sharpsight_uhid": "sharpsightuhid",
            "visit_id": "visitid", "visit_no": "visitno", "admission_no": "admissionno", "type_of_visit": "typeofvisit", "visit_date": "visitdate",
            "patient_name": "patientname", "prescribed_by": "prescribedby", "order_id": "orderid", "order_no": "orderno",
            "generated_by": "generatedby", "line_count": "linecount", "total_qty": "totalqty",
        })
        for row in rows:
            row["issue_id"] = _safe_int(row.get("issue_id"), 0)
            row["visit_id"] = _safe_int(row.get("visit_id"), 0)
            row["order_id"] = _safe_int(row.get("order_id"), 0)
            row["line_count"] = _safe_int(row.get("line_count"), 0)
            row["gross_amount"] = _safe_float(row.get("gross_amount"), 0)
            row["discount_amount"] = _safe_float(row.get("discount_amount"), 0)
            row["net_amount"] = _safe_float(row.get("net_amount"), 0)
            row["due_amount"] = _safe_float(row.get("due_amount"), 0)
            row["total_qty"] = _safe_float(row.get("total_qty"), 0)
            row["issue_date"] = _format_display_datetime(row.get("issue_date"))
            row["sale_date"] = _format_date(row.get("sale_date"))
            row["visit_date"] = _format_date(row.get("visit_date"))
            row["registration_no"] = str(row.get("registration_no") or "").strip()
            row["sharpsight_uhid"] = str(row.get("sharpsight_uhid") or "").strip()
            row["patient_name"] = str(row.get("patient_name") or "").strip()
            row["visit_no"] = str(row.get("visit_no") or "").strip()
            row["admission_no"] = str(row.get("admission_no") or "").strip()
            row["type_of_visit"] = str(row.get("type_of_visit") or "").strip()
            row["prescribed_by"] = str(row.get("prescribed_by") or "").strip()
            row["order_no"] = str(row.get("order_no") or "").strip()
            row["generated_by"] = str(row.get("generated_by") or "").strip()
            row["identity_label"] = row["registration_no"] or "-"
            if row["sharpsight_uhid"]:
                row["identity_label"] = f"{row['identity_label']} | UHID {row['sharpsight_uhid']}" if row["registration_no"] else f"UHID {row['sharpsight_uhid']}"
            row["visit_label"] = row["admission_no"] or row["visit_no"] or "-"
        return rows

    def _summarize_report_rows(rows: list[dict]) -> tuple[dict, list[dict]]:
        summary = {
            "bill_count": len(rows),
            "line_count": 0,
            "total_qty": 0.0,
            "gross_amount": 0.0,
            "discount_amount": 0.0,
            "net_amount": 0.0,
            "due_amount": 0.0,
        }
        daily_map: dict[str, dict] = {}
        for row in rows:
            summary["line_count"] += _safe_int(row.get("line_count"), 0)
            summary["total_qty"] += _safe_float(row.get("total_qty"), 0)
            summary["gross_amount"] += _safe_float(row.get("gross_amount"), 0)
            summary["discount_amount"] += _safe_float(row.get("discount_amount"), 0)
            summary["net_amount"] += _safe_float(row.get("net_amount"), 0)
            summary["due_amount"] += _safe_float(row.get("due_amount"), 0)

            sale_date = _format_date(row.get("sale_date")) or "-"
            bucket = daily_map.setdefault(sale_date, {
                "sale_date": sale_date,
                "bill_count": 0,
                "line_count": 0,
                "total_qty": 0.0,
                "gross_amount": 0.0,
                "discount_amount": 0.0,
                "net_amount": 0.0,
                "due_amount": 0.0,
            })
            bucket["bill_count"] += 1
            bucket["line_count"] += _safe_int(row.get("line_count"), 0)
            bucket["total_qty"] += _safe_float(row.get("total_qty"), 0)
            bucket["gross_amount"] += _safe_float(row.get("gross_amount"), 0)
            bucket["discount_amount"] += _safe_float(row.get("discount_amount"), 0)
            bucket["net_amount"] += _safe_float(row.get("net_amount"), 0)
            bucket["due_amount"] += _safe_float(row.get("due_amount"), 0)

        daily_rows = sorted(daily_map.values(), key=lambda rec: rec.get("sale_date") or "", reverse=True)
        return summary, daily_rows

    def _normalize_receipt_context(payload: dict | None) -> dict | None:
        if not isinstance(payload, dict):
            return None
        out = dict(payload)
        out["bill_id"] = _safe_int(out.get("bill_id"), 0)
        out["visit_id"] = _safe_int(out.get("visit_id"), 0)
        out["patient_id"] = _safe_int(out.get("patient_id"), 0)
        out["tmp_patient_id"] = _safe_int(out.get("tmp_patient_id"), 0)
        out["gross_amount"] = _safe_float(out.get("gross_amount"), 0)
        out["discount_amount"] = _safe_float(out.get("discount_amount"), 0)
        out["net_amount"] = _safe_float(out.get("net_amount"), 0)
        out["due_amount"] = _safe_float(out.get("due_amount"), 0)
        out["received_amount"] = _safe_float(out.get("received_amount"), 0)
        out["receipt_count"] = _safe_int(out.get("receipt_count"), 0)
        out["bill_date"] = _format_display_datetime(out.get("bill_date"))
        out["visit_date"] = _format_date(out.get("visit_date"))
        out["last_receipt_date"] = _format_display_datetime(out.get("last_receipt_date"))
        out["registration_no"] = str(out.get("registration_no") or "").strip()
        out["sharpsight_uhid"] = str(out.get("sharpsight_uhid") or "").strip()
        out["patient_name"] = str(out.get("patient_name") or "").strip()
        out["prescribed_by"] = str(out.get("prescribed_by") or "").strip()
        out["bill_no"] = str(out.get("bill_no") or "").strip()
        out["visit_no"] = str(out.get("visit_no") or "").strip()
        out["admission_no"] = str(out.get("admission_no") or "").strip()
        out["type_of_visit"] = str(out.get("type_of_visit") or "").strip()
        out["generated_by"] = str(out.get("generated_by") or "").strip()
        out["receipt_allowed"] = bool(out.get("receipt_allowed"))
        receipt_rows = []
        for row in out.get("receipts") or []:
            receipt_rows.append(
                {
                    "receipt_id": _safe_int(row.get("receipt_id"), 0),
                    "receipt_no": str(row.get("receipt_no") or "").strip(),
                    "receipt_date": _format_display_datetime(row.get("receipt_date")),
                    "amount": _safe_float(row.get("amount"), 0),
                    "payment_mode_id": _safe_int(row.get("payment_mode_id"), 0),
                    "payment_mode_name": str(row.get("payment_mode_name") or "").strip(),
                    "received_by": str(row.get("received_by") or "").strip(),
                    "receipt_note": str(row.get("receipt_note") or "").strip(),
                }
            )
        out["receipts"] = receipt_rows
        out["identity_label"] = "Self Walk-In" if out["tmp_patient_id"] > 0 else (out["registration_no"] or "-")
        if out["sharpsight_uhid"]:
            out["identity_label"] = f"{out['identity_label']} | UHID {out['sharpsight_uhid']}" if out["registration_no"] else f"UHID {out['sharpsight_uhid']}"
        out["visit_label"] = "Self Walk-In" if out["tmp_patient_id"] > 0 else (out["admission_no"] or out["visit_no"] or "-")
        return out

    def _build_invoice_pdf(payload: dict, printed_by: str, printed_at):
        from reportlab.lib import colors
        from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
        from reportlab.lib.pagesizes import A4
        from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
        from reportlab.lib.units import mm
        from reportlab.platypus import LongTable, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

        doc = SimpleDocTemplate(BytesIO(), pagesize=A4, leftMargin=10 * mm, rightMargin=10 * mm, topMargin=12 * mm, bottomMargin=14 * mm)
        buffer = doc.filename
        content_width = doc.width
        styles = getSampleStyleSheet()
        title = ParagraphStyle("ptitle", parent=styles["Normal"], fontName="Helvetica-Bold", fontSize=13, leading=15, alignment=TA_CENTER)
        sub = ParagraphStyle("psub", parent=styles["Normal"], fontName="Helvetica", fontSize=8.5, leading=10, alignment=TA_CENTER)
        meta = ParagraphStyle("pmeta", parent=styles["Normal"], fontName="Helvetica", fontSize=8.3, leading=10.2, alignment=TA_LEFT)
        meta_block = ParagraphStyle("pmeta_block", parent=meta, fontSize=8.5, leading=10.8)
        meta_right = ParagraphStyle("pmeta_right", parent=meta, alignment=TA_RIGHT)
        note = ParagraphStyle("pnote", parent=meta, fontSize=7.9, leading=9.3)

        def _as_text(value, fallback: str = "-") -> str:
            text = str(value or "").strip()
            return text if text else fallback

        def _meta_inline(label: str, value, *, align: str = "left"):
            style = meta_right if align == "right" else meta_block
            return Paragraph(
                f"<font color='#475569'><b>{escape(label)}</b></font> {escape(_as_text(value))}",
                style,
            )

        def _registration_block():
            reg_no = _as_text(payload.get("registration_no"))
            sharpsight_uhid = str(payload.get("sharpsight_uhid") or "").strip()
            lines = [
                "<font color='#475569'><b>Registration</b></font>",
                f"<b>{escape(reg_no)}</b>",
            ]
            if sharpsight_uhid and not _safe_int(payload.get("tmp_patient_id"), 0):
                lines.append(f"<font color='#64748b'>SharpSight UHID:</font> {escape(sharpsight_uhid)}")
            return Paragraph("<br/>".join(lines), meta_block)

        elements = []
        elements.append(Paragraph(escape(PRINT_PROFILE["name"]), title))
        elements.append(Paragraph(escape(PRINT_PROFILE["subtitle"]), sub))
        elements.append(Paragraph(escape(PRINT_PROFILE["address"]), sub))
        elements.append(Spacer(1, 3 * mm))
        header_info = [
            [Paragraph(f"<b>{escape(PRINT_PROFILE['tin_no'])}</b>", meta), Paragraph(f"<b>Retail Invoice</b>", sub), Paragraph(f"<b>Ph. No.:</b> {escape(PRINT_PROFILE['phone'])}", meta_right)],
            [Paragraph(f"<b>{escape(PRINT_PROFILE['licence'])}</b>", meta), Paragraph(f"<b>{escape(PRINT_PROFILE['gst_no'])}</b>", meta), Paragraph("", meta_right)],
        ]
        header_table = Table(header_info, colWidths=[content_width * 0.37, content_width * 0.33, content_width * 0.30], hAlign="LEFT")
        header_table.setStyle(TableStyle([
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("LEFTPADDING", (0, 0), (-1, -1), 0),
            ("RIGHTPADDING", (0, 0), (-1, -1), 0),
            ("TOPPADDING", (0, 0), (-1, -1), 0),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
        ]))
        elements.append(header_table)
        elements.append(Spacer(1, 2 * mm))
        is_walkin_bill = _safe_int(payload.get("tmp_patient_id"), 0) > 0
        if is_walkin_bill:
            metadata = [
                [_meta_inline("Patient:", payload.get("patient_name")), _meta_inline("Prescribed By:", payload.get("prescribed_by")), _meta_inline("Bill No.:", payload.get("bill_no"))],
                [_meta_inline("Route:", "Self Walk-In"), _meta_inline("Bill Date:", _format_display_datetime(payload.get("bill_date"))), _meta_inline("Generated By:", payload.get("generated_by") or printed_by)],
                [_meta_inline("Drug Sale:", payload.get("drug_sale_code") or payload.get("drug_sale_id")), _meta_inline("Received:", _format_amount(payload.get("received_amount"))), _meta_inline("Pending Due:", _format_amount(payload.get("due_amount")))],
            ]
        else:
            metadata = [
                [_registration_block(), _meta_inline("Prescribed By:", payload.get("prescribed_by")), _meta_inline("Bill No.:", payload.get("bill_no"))],
                [_meta_inline("Patient:", payload.get("patient_name")), _meta_inline("Visit No.:", payload.get("visit_no") or payload.get("admission_no")), _meta_inline("Bill Date:", _format_display_datetime(payload.get("bill_date")))],
                [_meta_inline("Drug Sale:", payload.get("drug_sale_code") or payload.get("drug_sale_id")), _meta_inline("Visit Type:", payload.get("type_of_visit")), _meta_inline("Generated By:", payload.get("generated_by") or printed_by)],
            ]
        meta_table = Table(metadata, colWidths=[content_width * 0.39, content_width * 0.33, content_width * 0.28], hAlign="LEFT")
        meta_table.setStyle(TableStyle([
            ("BOX", (0, 0), (-1, -1), 0.5, colors.HexColor("#475569")),
            ("INNERGRID", (0, 0), (-1, -1), 0.35, colors.HexColor("#cbd5e1")),
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#f8fafc")),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("LEFTPADDING", (0, 0), (-1, -1), 6),
            ("RIGHTPADDING", (0, 0), (-1, -1), 6),
            ("TOPPADDING", (0, 0), (-1, -1), 5),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ]))
        elements.append(meta_table)
        elements.append(Spacer(1, 3 * mm))

        item_rows = [["Sl", "Item", "Batch", "Expiry", "Qty", "MRP", "Total"]]
        for idx, item in enumerate(payload.get("items") or [], start=1):
            item_rows.append([
                str(idx),
                Paragraph(escape(str(item.get("item_name") or "")), meta),
                Paragraph(escape(str(item.get("batch_name") or "")), meta),
                _format_date(item.get("expiry_date")) or "-",
                _format_amount(item.get("quantity")),
                _format_amount(item.get("mrp")),
                _format_amount(item.get("line_total")),
            ])
        item_table = LongTable(item_rows, colWidths=[content_width * 0.06, content_width * 0.34, content_width * 0.18, content_width * 0.125, content_width * 0.095, content_width * 0.095, content_width * 0.105], repeatRows=1, hAlign="LEFT")
        item_table.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#0f172a")),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, -1), 8),
            ("GRID", (0, 0), (-1, -1), 0.35, colors.HexColor("#cbd5e1")),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f8fafc")]),
            ("ALIGN", (0, 0), (0, -1), "CENTER"),
            ("ALIGN", (4, 0), (-1, 0), "CENTER"),
            ("ALIGN", (4, 1), (-1, -1), "RIGHT"),
            ("LEFTPADDING", (0, 0), (-1, -1), 5),
            ("RIGHTPADDING", (0, 0), (-1, -1), 5),
            ("TOPPADDING", (0, 0), (-1, -1), 4),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ]))
        elements.append(item_table)
        elements.append(Spacer(1, 2.5 * mm))

        due_amount = payload.get("due_amount") or payload.get("net_amount") or 0
        received_amount = payload.get("received_amount") or 0
        totals_table = Table(
            [
                [Paragraph(f"<b>Amount In Words:</b> {escape(_amount_in_words(due_amount))}", note), Paragraph(f"<b>Gross Amount:</b> {_format_amount(payload.get('gross_amount'))}", meta_right)],
                [Paragraph(f"<b>Remarks:</b> {escape(payload.get('remarks') or '-')}", note), Paragraph(f"<b>Discount:</b> {_format_amount(payload.get('discount_amount'))}", meta_right)],
                [Paragraph("", note), Paragraph(f"<b>Received Amount:</b> {_format_amount(received_amount)}", meta_right)],
                [Paragraph("", note), Paragraph(f"<b>Due / Net Amount:</b> {_format_amount(due_amount)}", meta_right)],
            ],
            colWidths=[content_width * 0.70, content_width * 0.30],
            hAlign="LEFT",
        )
        totals_table.setStyle(TableStyle([
            ("BOX", (0, 0), (-1, -1), 0.5, colors.HexColor("#475569")),
            ("INNERGRID", (0, 0), (-1, -1), 0.35, colors.HexColor("#cbd5e1")),
            ("BACKGROUND", (1, 0), (1, -1), colors.HexColor("#f8fafc")),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("LEFTPADDING", (0, 0), (-1, -1), 6),
            ("RIGHTPADDING", (0, 0), (-1, -1), 6),
            ("TOPPADDING", (0, 0), (-1, -1), 5),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ]))
        elements.append(totals_table)
        elements.append(Spacer(1, 3 * mm))

        for text in PRINT_PROFILE["notes"]:
            elements.append(Paragraph(escape(text), note))

        def _on_page(canvas, doc_obj):
            canvas.saveState()
            canvas.setStrokeColor(colors.HexColor("#cbd5e1"))
            canvas.line(doc_obj.leftMargin, 10 * mm, doc_obj.pagesize[0] - doc_obj.rightMargin, 10 * mm)
            canvas.setFont("Helvetica", 7.5)
            canvas.drawString(doc_obj.leftMargin, 6 * mm, f"Printed By: {printed_by or '-'} | Printed On: {_format_display_datetime(printed_at)}")
            canvas.drawRightString(doc_obj.pagesize[0] - doc_obj.rightMargin, 6 * mm, f"Page {canvas.getPageNumber()}")
            canvas.restoreState()

        doc.build(elements, onFirstPage=_on_page, onLaterPages=_on_page)
        buffer.seek(0)
        return buffer

    def _build_issue_pdf(payload: dict, printed_by: str, printed_at):
        from reportlab.lib import colors
        from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
        from reportlab.lib.pagesizes import A4
        from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
        from reportlab.lib.units import mm
        from reportlab.platypus import LongTable, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

        doc = SimpleDocTemplate(BytesIO(), pagesize=A4, leftMargin=10 * mm, rightMargin=10 * mm, topMargin=12 * mm, bottomMargin=14 * mm)
        buffer = doc.filename
        content_width = doc.width
        styles = getSampleStyleSheet()
        title = ParagraphStyle("pititle", parent=styles["Normal"], fontName="Helvetica-Bold", fontSize=13, leading=15, alignment=TA_CENTER)
        sub = ParagraphStyle("pisub", parent=styles["Normal"], fontName="Helvetica", fontSize=8.5, leading=10, alignment=TA_CENTER)
        meta = ParagraphStyle("pimeta", parent=styles["Normal"], fontName="Helvetica", fontSize=8.3, leading=10.2, alignment=TA_LEFT)
        meta_block = ParagraphStyle("pimeta_block", parent=meta, fontSize=8.5, leading=10.8)
        meta_right = ParagraphStyle("pimeta_right", parent=meta, alignment=TA_RIGHT)
        note = ParagraphStyle("pinote", parent=meta, fontSize=7.9, leading=9.3)

        def _as_text(value, fallback: str = "-") -> str:
            text = str(value or "").strip()
            return text if text else fallback

        def _meta_inline(label: str, value, *, align: str = "left"):
            style = meta_right if align == "right" else meta_block
            return Paragraph(
                f"<font color='#475569'><b>{escape(label)}</b></font> {escape(_as_text(value))}",
                style,
            )

        def _registration_block():
            reg_no = _as_text(payload.get("registration_no"))
            sharpsight_uhid = str(payload.get("sharpsight_uhid") or "").strip()
            lines = [
                "<font color='#475569'><b>Registration</b></font>",
                f"<b>{escape(reg_no)}</b>",
            ]
            if sharpsight_uhid:
                lines.append(f"<font color='#64748b'>SharpSight UHID:</font> {escape(sharpsight_uhid)}")
            return Paragraph("<br/>".join(lines), meta_block)

        elements = []
        elements.append(Paragraph(escape(PRINT_PROFILE["name"]), title))
        elements.append(Paragraph(escape(PRINT_PROFILE["subtitle"]), sub))
        elements.append(Paragraph(escape(PRINT_PROFILE["address"]), sub))
        elements.append(Spacer(1, 3 * mm))
        header_info = [
            [Paragraph(f"<b>{escape(PRINT_PROFILE['tin_no'])}</b>", meta), Paragraph(f"<b>Consumption Issue Report</b>", sub), Paragraph(f"<b>Ph. No.:</b> {escape(PRINT_PROFILE['phone'])}", meta_right)],
            [Paragraph(f"<b>{escape(PRINT_PROFILE['licence'])}</b>", meta), Paragraph(f"<b>{escape(PRINT_PROFILE['gst_no'])}</b>", meta), Paragraph("", meta_right)],
        ]
        header_table = Table(header_info, colWidths=[content_width * 0.37, content_width * 0.33, content_width * 0.30], hAlign="LEFT")
        header_table.setStyle(TableStyle([
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("LEFTPADDING", (0, 0), (-1, -1), 0),
            ("RIGHTPADDING", (0, 0), (-1, -1), 0),
            ("TOPPADDING", (0, 0), (-1, -1), 0),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
        ]))
        elements.append(header_table)
        elements.append(Spacer(1, 2 * mm))
        metadata = [
            [_registration_block(), _meta_inline("Prescribed By:", payload.get("prescribed_by")), _meta_inline("Issue No.:", payload.get("issue_no"))],
            [_meta_inline("Patient:", payload.get("patient_name")), _meta_inline("Visit No.:", payload.get("visit_no") or payload.get("admission_no")), _meta_inline("Issue Date:", _format_display_datetime(payload.get("issue_date") or payload.get("bill_date")))],
            [_meta_inline("Order No.:", payload.get("order_no") or payload.get("order_id")), _meta_inline("Visit Type:", payload.get("type_of_visit")), _meta_inline("Generated By:", payload.get("generated_by") or printed_by)],
        ]
        meta_table = Table(metadata, colWidths=[content_width * 0.39, content_width * 0.33, content_width * 0.28], hAlign="LEFT")
        meta_table.setStyle(TableStyle([
            ("BOX", (0, 0), (-1, -1), 0.5, colors.HexColor("#475569")),
            ("INNERGRID", (0, 0), (-1, -1), 0.35, colors.HexColor("#cbd5e1")),
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#f8fafc")),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("LEFTPADDING", (0, 0), (-1, -1), 6),
            ("RIGHTPADDING", (0, 0), (-1, -1), 6),
            ("TOPPADDING", (0, 0), (-1, -1), 5),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ]))
        elements.append(meta_table)
        elements.append(Spacer(1, 3 * mm))

        item_rows = [["Sl", "Item", "Batch", "Expiry", "Qty", "MRP", "Total"]]
        for idx, item in enumerate(payload.get("items") or [], start=1):
            item_rows.append([
                str(idx),
                Paragraph(escape(str(item.get("item_name") or "")), meta),
                Paragraph(escape(str(item.get("batch_name") or "")), meta),
                _format_date(item.get("expiry_date")) or "-",
                _format_amount(item.get("quantity")),
                _format_amount(item.get("mrp")),
                _format_amount(item.get("line_total")),
            ])
        item_table = LongTable(item_rows, colWidths=[content_width * 0.06, content_width * 0.34, content_width * 0.18, content_width * 0.125, content_width * 0.095, content_width * 0.095, content_width * 0.105], repeatRows=1, hAlign="LEFT")
        item_table.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#0f172a")),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, -1), 8),
            ("GRID", (0, 0), (-1, -1), 0.35, colors.HexColor("#cbd5e1")),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f8fafc")]),
            ("ALIGN", (0, 0), (0, -1), "CENTER"),
            ("ALIGN", (4, 0), (-1, 0), "CENTER"),
            ("ALIGN", (4, 1), (-1, -1), "RIGHT"),
            ("LEFTPADDING", (0, 0), (-1, -1), 5),
            ("RIGHTPADDING", (0, 0), (-1, -1), 5),
            ("TOPPADDING", (0, 0), (-1, -1), 4),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ]))
        elements.append(item_table)
        elements.append(Spacer(1, 2.5 * mm))

        due_amount = payload.get("due_amount") or payload.get("net_amount") or 0
        totals_table = Table(
            [
                [Paragraph(f"<b>Amount In Words:</b> {escape(_amount_in_words(due_amount))}", note), Paragraph(f"<b>Gross Amount:</b> {_format_amount(payload.get('gross_amount'))}", meta_right)],
                [Paragraph(f"<b>Remarks:</b> {escape(payload.get('remarks') or '-')}", note), Paragraph(f"<b>Discount:</b> {_format_amount(payload.get('discount_amount'))}", meta_right)],
                [Paragraph("", note), Paragraph(f"<b>Total Consumption:</b> {_format_amount(due_amount)}", meta_right)],
            ],
            colWidths=[content_width * 0.70, content_width * 0.30],
            hAlign="LEFT",
        )
        totals_table.setStyle(TableStyle([
            ("BOX", (0, 0), (-1, -1), 0.5, colors.HexColor("#475569")),
            ("INNERGRID", (0, 0), (-1, -1), 0.35, colors.HexColor("#cbd5e1")),
            ("BACKGROUND", (1, 0), (1, -1), colors.HexColor("#f8fafc")),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("LEFTPADDING", (0, 0), (-1, -1), 6),
            ("RIGHTPADDING", (0, 0), (-1, -1), 6),
            ("TOPPADDING", (0, 0), (-1, -1), 5),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ]))
        elements.append(totals_table)
        elements.append(Spacer(1, 3 * mm))

        for text in PRINT_PROFILE["notes"]:
            elements.append(Paragraph(escape(text), note))

        def _on_page(canvas, doc_obj):
            canvas.saveState()
            canvas.setStrokeColor(colors.HexColor("#cbd5e1"))
            canvas.line(doc_obj.leftMargin, 10 * mm, doc_obj.pagesize[0] - doc_obj.rightMargin, 10 * mm)
            canvas.setFont("Helvetica", 7.5)
            canvas.drawString(doc_obj.leftMargin, 6 * mm, f"Printed By: {printed_by or '-'} | Printed On: {_format_display_datetime(printed_at)}")
            canvas.drawRightString(doc_obj.pagesize[0] - doc_obj.rightMargin, 6 * mm, f"Page {canvas.getPageNumber()}")
            canvas.restoreState()

        doc.build(elements, onFirstPage=_on_page, onLaterPages=_on_page)
        buffer.seek(0)
        return buffer

    def _build_report_pdf(*, title_text: str, subtitle_text: str, table_headers: list[str], table_rows: list[list], printed_by: str, printed_at, summary_pairs: list[tuple[str, str]] | None = None):
        from reportlab.lib import colors
        from reportlab.lib.enums import TA_CENTER, TA_LEFT
        from reportlab.lib.pagesizes import A4, landscape
        from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
        from reportlab.lib.units import mm
        from reportlab.platypus import LongTable, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

        doc = SimpleDocTemplate(BytesIO(), pagesize=landscape(A4), leftMargin=8 * mm, rightMargin=8 * mm, topMargin=10 * mm, bottomMargin=12 * mm)
        buffer = doc.filename
        styles = getSampleStyleSheet()
        title = ParagraphStyle("rtitle", parent=styles["Normal"], fontName="Helvetica-Bold", fontSize=13, leading=15, alignment=TA_CENTER)
        sub = ParagraphStyle("rsub", parent=styles["Normal"], fontName="Helvetica", fontSize=8.4, leading=10, alignment=TA_CENTER)
        cell = ParagraphStyle("rcell", parent=styles["Normal"], fontName="Helvetica", fontSize=7.4, leading=9, alignment=TA_LEFT)
        note = ParagraphStyle("rnote", parent=styles["Normal"], fontName="Helvetica", fontSize=7.2, leading=8.6, alignment=TA_LEFT)

        elements = [
            Paragraph(escape(PRINT_PROFILE["name"]), title),
            Paragraph(escape(title_text), sub),
            Paragraph(escape(subtitle_text), sub),
            Spacer(1, 3 * mm),
        ]

        if summary_pairs:
            summary_data = []
            row = []
            for idx, (label, value) in enumerate(summary_pairs, start=1):
                row.append(Paragraph(f"<b>{escape(label)}:</b> {escape(str(value or '-'))}", cell))
                if idx % 4 == 0:
                    summary_data.append(row)
                    row = []
            if row:
                while len(row) < 4:
                    row.append(Paragraph("", cell))
                summary_data.append(row)
            summary_table = Table(summary_data, colWidths=[doc.width / 4.0] * 4, hAlign="LEFT")
            summary_table.setStyle(TableStyle([
                ("BOX", (0, 0), (-1, -1), 0.45, colors.HexColor("#475569")),
                ("INNERGRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#cbd5e1")),
                ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#f8fafc")),
                ("LEFTPADDING", (0, 0), (-1, -1), 5),
                ("RIGHTPADDING", (0, 0), (-1, -1), 5),
                ("TOPPADDING", (0, 0), (-1, -1), 4),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
            ]))
            elements.extend([summary_table, Spacer(1, 3 * mm)])

        table_data = [table_headers]
        for row in table_rows:
            table_data.append([Paragraph(escape(str(col if col is not None else "-")), cell) for col in row])

        col_count = max(len(table_headers), 1)
        table = LongTable(table_data, colWidths=[doc.width / col_count] * col_count, repeatRows=1, hAlign="LEFT")
        table.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#0f172a")),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, -1), 7.2),
            ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#cbd5e1")),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f8fafc")]),
            ("LEFTPADDING", (0, 0), (-1, -1), 4),
            ("RIGHTPADDING", (0, 0), (-1, -1), 4),
            ("TOPPADDING", (0, 0), (-1, -1), 3),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
        ]))
        elements.append(table)

        def _on_page(canvas, doc_obj):
            canvas.saveState()
            canvas.setStrokeColor(colors.HexColor("#cbd5e1"))
            canvas.line(doc_obj.leftMargin, 8 * mm, doc_obj.pagesize[0] - doc_obj.rightMargin, 8 * mm)
            canvas.setFont("Helvetica", 7.2)
            canvas.drawString(doc_obj.leftMargin, 4.5 * mm, f"Printed By: {printed_by or '-'} | Printed On: {_format_display_datetime(printed_at)}")
            canvas.drawRightString(doc_obj.pagesize[0] - doc_obj.rightMargin, 4.5 * mm, f"Page {canvas.getPageNumber()}")
            canvas.restoreState()

        doc.build(elements, onFirstPage=_on_page, onLaterPages=_on_page)
        buffer.seek(0)
        return buffer

    @app.route("/pharmacy/sales")
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"}, required_section="pharmacy_sales")
    def pharmacy_sales_dashboard():
        unit, error = _sales_unit()
        if error:
            return error
        return (
            render_template(
                "pharmacy_sales.html",
                unit=unit,
                store_id=STORE_ID,
                store_name=STORE_NAME,
                store_code=STORE_CODE,
                can_edit_mrp=_can_edit_mrp(),
                prepared_by=session.get("username") or session.get("user") or "",
                today_iso=datetime.now(tz=LOCAL_TZ).date().isoformat(),
            ),
            200,
            {"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0", "Pragma": "no-cache", "Expires": "0"},
        )

    @app.route("/api/pharmacy/sales/init")
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"}, required_section="pharmacy_sales")
    def api_pharmacy_sales_init():
        unit, error = _sales_unit()
        if error:
            return error
        payment_modes_df = data_fetch.fetch_pharmacy_sale_payment_modes(unit)
        payment_modes = []
        if payment_modes_df is not None:
            payment_modes = _normalize_rows(payment_modes_df, {"payment_mode_id": "paymentmodeid", "payment_mode_name": "paymentmodename"})
            for row in payment_modes:
                row["payment_mode_id"] = _safe_int(row.get("payment_mode_id"), 0)
                row["payment_mode_name"] = str(row.get("payment_mode_name") or "").strip()
        return jsonify({
            "status": "success",
            "unit": unit,
            "today": datetime.now(tz=LOCAL_TZ).date().isoformat(),
            "store": {"id": STORE_ID, "name": STORE_NAME, "code": STORE_CODE},
            "prepared_by": session.get("username") or session.get("user") or "",
            "permissions": {"can_edit_mrp": _can_edit_mrp()},
            "visit_buckets": [{"id": "OPD", "label": "OPD / DPV"}, {"id": "IPD", "label": "IPD"}],
            "patient_modes": [{"id": "visit", "label": "Linked Visit"}, {"id": "walkin", "label": "Self Walk-In"}],
            "walkin_supported": True,
            "payment_modes": payment_modes,
        })

    @app.route("/api/pharmacy/sales/doctors")
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"}, required_section="pharmacy_sales")
    def api_pharmacy_sales_doctors():
        unit, error = _sales_unit()
        if error:
            return error
        df = data_fetch.search_pharmacy_sale_doctors(unit, request.args.get("q"), _safe_int(request.args.get("limit"), 20))
        if df is None:
            return jsonify({"status": "error", "message": "Failed to fetch doctor names."}), 500
        rows = _normalize_rows(df, {"doctor_id": "doctorid", "doctor_name": "doctorname"})
        for row in rows:
            row["doctor_id"] = _safe_int(row.get("doctor_id"), 0)
            row["doctor_name"] = str(row.get("doctor_name") or "").strip()
        return jsonify({"status": "success", "unit": unit, "items": rows})

    @app.route("/api/pharmacy/sales/patients")
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"}, required_section="pharmacy_sales")
    def api_pharmacy_sales_patients():
        unit, error = _sales_unit()
        if error:
            return error
        df = data_fetch.search_pharmacy_sale_patients(unit, request.args.get("bucket"), request.args.get("q"), _safe_int(request.args.get("limit"), 25))
        if df is None:
            return jsonify({"status": "error", "message": "Failed to fetch patients."}), 500
        rows = _normalize_rows(df, {
            "visit_id": "visitid", "patient_id": "patientid", "registration_no": "registrationno", "visit_no": "visitno",
            "sharpsight_uhid": "sharpsightuhid", "admission_no": "admissionno", "visit_date": "visitdate", "type_of_visit": "typeofvisit", "visit_bucket": "visitbucket",
            "patient_name": "patientname", "consultant_name": "consultantname", "payer_type_id": "payertypeid", "agreement_type_id": "agreementtypeid",
            "has_corp_bill": "hascorpbill", "transaction_mode": "transactionmode", "admission_status": "admissionstatus",
        })
        for row in rows:
            row["visit_id"] = _safe_int(row.get("visit_id"), 0)
            row["patient_id"] = _safe_int(row.get("patient_id"), 0)
            row["visit_date"] = _format_date(row.get("visit_date"))
            row["registration_no"] = str(row.get("registration_no") or "").strip()
            row["sharpsight_uhid"] = str(row.get("sharpsight_uhid") or "").strip()
            row["patient_name"] = str(row.get("patient_name") or "").strip()
            row["visit_no"] = str(row.get("visit_no") or "").strip()
            row["admission_no"] = str(row.get("admission_no") or "").strip()
            row["has_corp_bill"] = _safe_int(row.get("has_corp_bill"), 0)
            row["transaction_mode"] = str(row.get("transaction_mode") or "sale").strip().lower() or "sale"
            row["admission_status"] = str(row.get("admission_status") or "").strip()
        return jsonify({"status": "success", "unit": unit, "items": rows})

    @app.route("/api/pharmacy/sales/items")
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"}, required_section="pharmacy_sales")
    def api_pharmacy_sales_items():
        unit, error = _sales_unit()
        if error:
            return error
        df = data_fetch.search_pharmacy_sale_items(unit, request.args.get("q"), _safe_int(request.args.get("limit"), 30), STORE_ID)
        if df is None:
            return jsonify({"status": "error", "message": "Failed to fetch items."}), 500
        rows = _normalize_rows(df, {
            "item_id": "itemid", "item_code": "itemcode", "item_name": "itemname", "descriptive_name": "descriptivename",
            "sales_price": "salesprice", "standard_rate": "standardrate", "pack_size_id": "packsizeid", "stock_qty": "stockqty",
        })
        for row in rows:
            row["item_id"] = _safe_int(row.get("item_id"), 0)
            row["stock_qty"] = _safe_float(row.get("stock_qty"), 0)
            row["sales_price"] = _safe_float(row.get("sales_price"), 0)
            row["standard_rate"] = _safe_float(row.get("standard_rate"), 0)
            row["display_name"] = f"{row.get('item_name') or ''}".strip()
            if row.get("item_code"):
                row["display_name"] = f"{row['display_name']} ({str(row['item_code']).strip()})".strip()
        return jsonify({"status": "success", "unit": unit, "items": rows})

    @app.route("/api/pharmacy/sales/items/<int:item_id>/batches")
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"}, required_section="pharmacy_sales")
    def api_pharmacy_sales_batches(item_id: int):
        unit, error = _sales_unit()
        if error:
            return error
        df = data_fetch.fetch_pharmacy_sale_batches(unit, item_id, STORE_ID)
        if df is None:
            return jsonify({"status": "error", "message": "Failed to fetch batches."}), 500
        rows = _normalize_rows(df, {
            "item_id": "itemid", "batch_id": "batchid", "batch_name": "batchname", "expiry_date": "expirydate",
            "stock_qty": "stockqty", "mrp": "mrp", "rate": "rate", "lending_rate": "lendingrate",
            "actual_lending_rate": "actuallendingrate", "sales_price": "salesprice",
        })
        for row in rows:
            row["batch_id"] = _safe_int(row.get("batch_id"), 0)
            row["stock_qty"] = _safe_float(row.get("stock_qty"), 0)
            row["mrp"] = _safe_float(row.get("mrp"), 0)
            row["rate"] = _safe_float(row.get("rate"), 0)
            row["sales_price"] = _safe_float(row.get("sales_price"), 0)
            row["expiry_date"] = _format_date(row.get("expiry_date"))
        return jsonify({"status": "success", "unit": unit, "item_id": item_id, "items": rows})

    @app.route("/api/pharmacy/sales/reports/bills")
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"}, required_section="pharmacy_sales")
    def api_pharmacy_sales_report_bills():
        unit, error = _sales_unit()
        if error:
            return error
        df = data_fetch.fetch_pharmacy_sale_report_rows(
            unit,
            from_date=request.args.get("from_date") or request.args.get("from"),
            to_date=request.args.get("to_date") or request.args.get("to"),
            query=request.args.get("q") or "",
            limit=_safe_int(request.args.get("limit"), 250),
        )
        if df is None:
            return jsonify({"status": "error", "message": "Failed to fetch pharmacy bills."}), 500
        rows = _normalize_report_rows(df)
        for row in rows:
            row["print_url"] = url_for("api_pharmacy_sales_print", bill_id=row["bill_id"])
        return jsonify({"status": "success", "unit": unit, "count": len(rows), "items": rows})

    @app.route("/api/pharmacy/sales/reports/register")
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"}, required_section="pharmacy_sales")
    def api_pharmacy_sales_report_register():
        unit, error = _sales_unit()
        if error:
            return error
        df = data_fetch.fetch_pharmacy_sale_report_rows(
            unit,
            from_date=request.args.get("from_date") or request.args.get("from"),
            to_date=request.args.get("to_date") or request.args.get("to"),
            query=request.args.get("q") or "",
            limit=_safe_int(request.args.get("limit"), 2000),
        )
        if df is None:
            return jsonify({"status": "error", "message": "Failed to fetch pharmacy sale register."}), 500
        rows = _normalize_report_rows(df)
        for row in rows:
            row["print_url"] = url_for("api_pharmacy_sales_print", bill_id=row["bill_id"])
        summary, daily_rows = _summarize_report_rows(rows)
        return jsonify({"status": "success", "unit": unit, "count": len(rows), "summary": summary, "daily_summary": daily_rows, "items": rows})

    @app.route("/api/pharmacy/sales/reports/issues")
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"}, required_section="pharmacy_sales")
    def api_pharmacy_sales_report_issues():
        unit, error = _sales_unit()
        if error:
            return error
        df = data_fetch.fetch_pharmacy_issue_report_rows(
            unit,
            from_date=request.args.get("from_date") or request.args.get("from"),
            to_date=request.args.get("to_date") or request.args.get("to"),
            query=request.args.get("q") or "",
            limit=_safe_int(request.args.get("limit"), 250),
        )
        if df is None:
            return jsonify({"status": "error", "message": "Failed to fetch pharmacy issues."}), 500
        rows = _normalize_issue_report_rows(df)
        for row in rows:
            row["print_url"] = url_for("api_pharmacy_issue_print", issue_id=row["issue_id"])
        return jsonify({"status": "success", "unit": unit, "count": len(rows), "items": rows})

    @app.route("/api/pharmacy/sales/reports/issue-register")
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"}, required_section="pharmacy_sales")
    def api_pharmacy_sales_report_issue_register():
        unit, error = _sales_unit()
        if error:
            return error
        df = data_fetch.fetch_pharmacy_issue_report_rows(
            unit,
            from_date=request.args.get("from_date") or request.args.get("from"),
            to_date=request.args.get("to_date") or request.args.get("to"),
            query=request.args.get("q") or "",
            limit=_safe_int(request.args.get("limit"), 2000),
        )
        if df is None:
            return jsonify({"status": "error", "message": "Failed to fetch pharmacy issue register."}), 500
        rows = _normalize_issue_report_rows(df)
        for row in rows:
            row["print_url"] = url_for("api_pharmacy_issue_print", issue_id=row["issue_id"])
        summary, daily_rows = _summarize_report_rows(rows)
        return jsonify({"status": "success", "unit": unit, "count": len(rows), "summary": summary, "daily_summary": daily_rows, "items": rows})

    @app.route("/api/pharmacy/sales/reports/bills/print")
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"}, required_section="pharmacy_sales")
    def api_pharmacy_sales_report_bills_print():
        unit, error = _sales_unit()
        if error:
            return error
        from_date = request.args.get("from_date") or request.args.get("from")
        to_date = request.args.get("to_date") or request.args.get("to")
        query_text = str(request.args.get("q") or "").strip()
        df = data_fetch.fetch_pharmacy_sale_report_rows(unit, from_date=from_date, to_date=to_date, query=query_text, limit=_safe_int(request.args.get("limit"), 500))
        if df is None:
            return jsonify({"status": "error", "message": "Failed to fetch existing bills."}), 500
        rows = _normalize_report_rows(df)
        printed_by = str(session.get("username") or session.get("user") or "Unknown").strip() or "Unknown"
        printed_at = datetime.now(tz=LOCAL_TZ)
        pdf_buffer = _build_report_pdf(
            title_text="Existing Pharmacy Bills",
            subtitle_text=f"Range: {_format_date(from_date) or '-'} to {_format_date(to_date) or '-'}" + (f" | Filter: {query_text}" if query_text else ""),
            table_headers=["Bill Date", "Bill No", "Patient", "Reg / UHID", "Visit", "Net", "Generated By"],
            table_rows=[
                [row.get("bill_date") or "-", row.get("bill_no") or "-", row.get("patient_name") or "-", row.get("identity_label") or "-", row.get("visit_label") or "-", _format_amount(row.get("net_amount")), row.get("generated_by") or "-"]
                for row in rows
            ],
            printed_by=printed_by,
            printed_at=printed_at,
            summary_pairs=[("Rows", str(len(rows))), ("Gross", _format_amount(sum(_safe_float(r.get("gross_amount"), 0) for r in rows))), ("Discount", _format_amount(sum(_safe_float(r.get("discount_amount"), 0) for r in rows))), ("Net", _format_amount(sum(_safe_float(r.get("net_amount"), 0) for r in rows)))],
        )
        return send_file(pdf_buffer, mimetype="application/pdf", as_attachment=False, download_name="pharmacy-bills-list.pdf")

    @app.route("/api/pharmacy/sales/reports/register/print")
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"}, required_section="pharmacy_sales")
    def api_pharmacy_sales_report_register_print():
        unit, error = _sales_unit()
        if error:
            return error
        from_date = request.args.get("from_date") or request.args.get("from")
        to_date = request.args.get("to_date") or request.args.get("to")
        query_text = str(request.args.get("q") or "").strip()
        df = data_fetch.fetch_pharmacy_sale_report_rows(unit, from_date=from_date, to_date=to_date, query=query_text, limit=_safe_int(request.args.get("limit"), 2500))
        if df is None:
            return jsonify({"status": "error", "message": "Failed to fetch sale register."}), 500
        rows = _normalize_report_rows(df)
        summary, _ = _summarize_report_rows(rows)
        printed_by = str(session.get("username") or session.get("user") or "Unknown").strip() or "Unknown"
        printed_at = datetime.now(tz=LOCAL_TZ)
        pdf_buffer = _build_report_pdf(
            title_text="Pharmacy Sale Register",
            subtitle_text=f"Range: {_format_date(from_date) or '-'} to {_format_date(to_date) or '-'}" + (f" | Filter: {query_text}" if query_text else ""),
            table_headers=["Date", "Bill No", "Patient", "Reg / UHID", "Visit", "Lines", "Qty", "Gross", "Discount", "Net", "By"],
            table_rows=[
                [row.get("sale_date") or "-", row.get("bill_no") or "-", row.get("patient_name") or "-", row.get("identity_label") or "-", row.get("visit_label") or "-", str(row.get("line_count") or 0), _format_amount(row.get("total_qty")), _format_amount(row.get("gross_amount")), _format_amount(row.get("discount_amount")), _format_amount(row.get("net_amount")), row.get("generated_by") or "-"]
                for row in rows
            ],
            printed_by=printed_by,
            printed_at=printed_at,
            summary_pairs=[("Bills", str(summary.get("bill_count", 0))), ("Qty", _format_amount(summary.get("total_qty", 0))), ("Gross", _format_amount(summary.get("gross_amount", 0))), ("Net", _format_amount(summary.get("net_amount", 0)))],
        )
        return send_file(pdf_buffer, mimetype="application/pdf", as_attachment=False, download_name="pharmacy-sale-register.pdf")

    @app.route("/api/pharmacy/sales/reports/issues/print")
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"}, required_section="pharmacy_sales")
    def api_pharmacy_sales_report_issues_print():
        unit, error = _sales_unit()
        if error:
            return error
        from_date = request.args.get("from_date") or request.args.get("from")
        to_date = request.args.get("to_date") or request.args.get("to")
        query_text = str(request.args.get("q") or "").strip()
        df = data_fetch.fetch_pharmacy_issue_report_rows(unit, from_date=from_date, to_date=to_date, query=query_text, limit=_safe_int(request.args.get("limit"), 500))
        if df is None:
            return jsonify({"status": "error", "message": "Failed to fetch existing issues."}), 500
        rows = _normalize_issue_report_rows(df)
        printed_by = str(session.get("username") or session.get("user") or "Unknown").strip() or "Unknown"
        printed_at = datetime.now(tz=LOCAL_TZ)
        pdf_buffer = _build_report_pdf(
            title_text="Existing Consumption Issues",
            subtitle_text=f"Range: {_format_date(from_date) or '-'} to {_format_date(to_date) or '-'}" + (f" | Filter: {query_text}" if query_text else ""),
            table_headers=["Issue Date", "Issue No", "Patient", "Reg / UHID", "Visit", "Net", "Generated By"],
            table_rows=[
                [row.get("issue_date") or "-", row.get("issue_no") or "-", row.get("patient_name") or "-", row.get("identity_label") or "-", row.get("visit_label") or "-", _format_amount(row.get("net_amount")), row.get("generated_by") or "-"]
                for row in rows
            ],
            printed_by=printed_by,
            printed_at=printed_at,
            summary_pairs=[("Rows", str(len(rows))), ("Gross", _format_amount(sum(_safe_float(r.get("gross_amount"), 0) for r in rows))), ("Discount", _format_amount(sum(_safe_float(r.get("discount_amount"), 0) for r in rows))), ("Net", _format_amount(sum(_safe_float(r.get("net_amount"), 0) for r in rows)))],
        )
        return send_file(pdf_buffer, mimetype="application/pdf", as_attachment=False, download_name="pharmacy-issues-list.pdf")

    @app.route("/api/pharmacy/sales/reports/issue-register/print")
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"}, required_section="pharmacy_sales")
    def api_pharmacy_sales_report_issue_register_print():
        unit, error = _sales_unit()
        if error:
            return error
        from_date = request.args.get("from_date") or request.args.get("from")
        to_date = request.args.get("to_date") or request.args.get("to")
        query_text = str(request.args.get("q") or "").strip()
        df = data_fetch.fetch_pharmacy_issue_report_rows(unit, from_date=from_date, to_date=to_date, query=query_text, limit=_safe_int(request.args.get("limit"), 2500))
        if df is None:
            return jsonify({"status": "error", "message": "Failed to fetch issue register."}), 500
        rows = _normalize_issue_report_rows(df)
        summary, _ = _summarize_report_rows(rows)
        printed_by = str(session.get("username") or session.get("user") or "Unknown").strip() or "Unknown"
        printed_at = datetime.now(tz=LOCAL_TZ)
        pdf_buffer = _build_report_pdf(
            title_text="Pharmacy Issue Register",
            subtitle_text=f"Range: {_format_date(from_date) or '-'} to {_format_date(to_date) or '-'}" + (f" | Filter: {query_text}" if query_text else ""),
            table_headers=["Date", "Issue No", "Patient", "Reg / UHID", "Visit", "Lines", "Qty", "Gross", "Discount", "Net", "By"],
            table_rows=[
                [row.get("sale_date") or "-", row.get("issue_no") or "-", row.get("patient_name") or "-", row.get("identity_label") or "-", row.get("visit_label") or "-", str(row.get("line_count") or 0), _format_amount(row.get("total_qty")), _format_amount(row.get("gross_amount")), _format_amount(row.get("discount_amount")), _format_amount(row.get("net_amount")), row.get("generated_by") or "-"]
                for row in rows
            ],
            printed_by=printed_by,
            printed_at=printed_at,
            summary_pairs=[("Issues", str(summary.get("bill_count", 0))), ("Qty", _format_amount(summary.get("total_qty", 0))), ("Gross", _format_amount(summary.get("gross_amount", 0))), ("Net", _format_amount(summary.get("net_amount", 0)))],
        )
        return send_file(pdf_buffer, mimetype="application/pdf", as_attachment=False, download_name="pharmacy-issue-register.pdf")

    @app.route("/api/pharmacy/sales", methods=["POST"])
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"}, required_section="pharmacy_sales")
    def api_pharmacy_sales_save():
        unit, error = _sales_unit()
        if error:
            return error
        payload = _sanitize_json_payload(request.get_json(silent=True) or {})
        result = data_fetch.save_pharmacy_sale(
            unit,
            visit_id=_safe_int(payload.get("visit_id"), 0),
            visit_bucket=str(payload.get("visit_bucket") or "OPD"),
            patient_mode=str(payload.get("patient_mode") or "visit"),
            walkin_patient_name=str(payload.get("walkin_patient_name") or "").strip(),
            walkin_doctor_name=str(payload.get("walkin_doctor_name") or "").strip(),
            sale_date=payload.get("sale_date"),
            discount_amount=_safe_float(payload.get("discount_amount"), 0),
            remarks=str(payload.get("remarks") or "").strip(),
            receipt_amount=_safe_float(payload.get("receipt_amount"), 0),
            receipt_payment_mode_id=_safe_int(payload.get("receipt_payment_mode_id"), 0),
            receipt_date=payload.get("receipt_date"),
            receipt_note=str(payload.get("receipt_note") or "").strip(),
            allow_mrp_override=_can_edit_mrp(),
            items=payload.get("items") or [],
            actor_user_id=_safe_int(session.get("accountid") or session.get("account_id"), 0),
            actor_username=str(session.get("username") or session.get("user") or "").strip(),
            actor_ip=request.remote_addr or "",
        )
        if result.get("status") != "success":
            _audit_log_event("pharmacy_sales", "sale_save", status="error", entity_type="transaction", unit=unit, summary="Pharmacy sale save failed", details={"message": result.get("message")})
            return jsonify({"status": "error", "message": result.get("message") or "Failed to save pharmacy sale."}), 400
        transaction_kind = str(result.get("transaction_kind") or "sale").strip().lower()
        if transaction_kind == "issue":
            issue_id = _safe_int(result.get("issue_id"), 0)
            result["print_url"] = url_for("api_pharmacy_issue_print", issue_id=issue_id)
            result["record_id"] = issue_id
            result["record_no"] = result.get("issue_no") or issue_id
            result["success_message"] = f"Issue saved successfully. Issue No. {result.get('issue_no') or issue_id}"
            _audit_log_event("pharmacy_sales", "issue_save", status="success", entity_type="issue", entity_id=str(issue_id), unit=unit, summary="Pharmacy patient issue saved", details={"issue_no": result.get("issue_no"), "order_id": result.get("order_id"), "item_count": len(payload.get("items") or [])})
        else:
            bill_id = _safe_int(result.get("bill_id"), 0)
            result["print_url"] = url_for("api_pharmacy_sales_print", bill_id=bill_id)
            result["record_id"] = bill_id
            result["record_no"] = result.get("bill_no") or bill_id
            received_amount = _safe_float(result.get("received_amount"), 0)
            if received_amount > 0:
                result["success_message"] = (
                    f"Sale and receipt saved successfully. Bill No. {result.get('bill_no') or bill_id}"
                    + (f", Receipt No. {result.get('receipt_no')}" if result.get("receipt_no") else "")
                )
            else:
                result["success_message"] = f"Sale saved successfully. Bill No. {result.get('bill_no') or bill_id}"
            _audit_log_event("pharmacy_sales", "sale_save", status="success", entity_type="bill", entity_id=str(bill_id), unit=unit, summary="Pharmacy sale saved", details={"bill_no": result.get("bill_no"), "drug_sale_id": result.get("drug_sale_id"), "receipt_no": result.get("receipt_no"), "received_amount": received_amount, "item_count": len(payload.get("items") or [])})
        return jsonify(result)

    @app.route("/api/pharmacy/sales/<int:bill_id>/receipts", methods=["GET"])
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"}, required_section="pharmacy_sales")
    def api_pharmacy_sales_receipt_context(bill_id: int):
        unit, error = _sales_unit()
        if error:
            return error
        payload = _normalize_receipt_context(data_fetch.fetch_pharmacy_sale_receipt_context(unit, bill_id))
        if not payload:
            return jsonify({"status": "error", "message": "Pharmacy bill not found for receipt collection."}), 404
        return jsonify({"status": "success", "unit": unit, "bill": payload})

    @app.route("/api/pharmacy/sales/<int:bill_id>/receipts", methods=["POST"])
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"}, required_section="pharmacy_sales")
    def api_pharmacy_sales_receive_payment(bill_id: int):
        unit, error = _sales_unit()
        if error:
            return error
        payload = _sanitize_json_payload(request.get_json(silent=True) or {})
        result = data_fetch.record_pharmacy_sale_receipt(
            unit,
            bill_id=bill_id,
            amount=_safe_float(payload.get("amount"), 0),
            payment_mode_id=_safe_int(payload.get("payment_mode_id"), 0),
            receipt_date=payload.get("receipt_date"),
            note=str(payload.get("note") or "").strip(),
            actor_user_id=_safe_int(session.get("accountid") or session.get("account_id"), 0),
            actor_username=str(session.get("username") or session.get("user") or "").strip(),
            actor_ip=request.remote_addr or "",
        )
        if result.get("status") != "success":
            _audit_log_event("pharmacy_sales", "receipt_save", status="error", entity_type="receipt", unit=unit, summary="Pharmacy receipt save failed", details={"bill_id": bill_id, "message": result.get("message")})
            return jsonify({"status": "error", "message": result.get("message") or "Failed to receive bill payment."}), 400

        refreshed = _normalize_receipt_context(data_fetch.fetch_pharmacy_sale_receipt_context(unit, bill_id))
        result["bill"] = refreshed
        _audit_log_event(
            "pharmacy_sales",
            "receipt_save",
            status="success",
            entity_type="receipt",
            entity_id=str(result.get("receipt_id") or ""),
            unit=unit,
            summary="Pharmacy bill receipt saved",
            details={"bill_id": bill_id, "bill_no": result.get("bill_no"), "receipt_no": result.get("receipt_no"), "amount": result.get("received_amount")},
        )
        return jsonify(result)

    @app.route("/api/pharmacy/sales/<int:bill_id>/print")
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"}, required_section="pharmacy_sales")
    def api_pharmacy_sales_print(bill_id: int):
        unit, error = _sales_unit()
        if error:
            return error
        payload = data_fetch.fetch_pharmacy_sale_print_payload(unit, bill_id)
        if not payload:
            return jsonify({"status": "error", "message": "Pharmacy bill not found."}), 404
        printed_by = str(session.get("username") or session.get("user") or "Unknown").strip() or "Unknown"
        printed_at = datetime.now(tz=LOCAL_TZ)
        pdf_buffer = _build_invoice_pdf(payload, printed_by, printed_at)
        safe_bill_no = str(payload.get("bill_no") or f"PH-{bill_id}").replace("/", "-").replace("\\", "-")
        _audit_log_event("pharmacy_sales", "sale_print", status="success", entity_type="bill", entity_id=str(bill_id), unit=unit, summary="Pharmacy bill PDF generated", details={"bill_no": payload.get("bill_no"), "printed_by": printed_by})
        return send_file(pdf_buffer, mimetype="application/pdf", as_attachment=False, download_name=f"{safe_bill_no}.pdf")

    @app.route("/api/pharmacy/sales/issues/<int:issue_id>/print")
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"}, required_section="pharmacy_sales")
    def api_pharmacy_issue_print(issue_id: int):
        unit, error = _sales_unit()
        if error:
            return error
        payload = data_fetch.fetch_pharmacy_issue_print_payload(unit, issue_id)
        if not payload:
            return jsonify({"status": "error", "message": "Pharmacy issue not found."}), 404
        printed_by = str(session.get("username") or session.get("user") or "Unknown").strip() or "Unknown"
        printed_at = datetime.now(tz=LOCAL_TZ)
        pdf_buffer = _build_issue_pdf(payload, printed_by, printed_at)
        safe_issue_no = str(payload.get("issue_no") or f"PISN{issue_id}").replace("/", "-").replace("\\", "-")
        _audit_log_event("pharmacy_sales", "issue_print", status="success", entity_type="issue", entity_id=str(issue_id), unit=unit, summary="Pharmacy issue PDF generated", details={"issue_no": payload.get("issue_no"), "printed_by": printed_by})
        return send_file(pdf_buffer, mimetype="application/pdf", as_attachment=False, download_name=f"{safe_issue_no}.pdf")
