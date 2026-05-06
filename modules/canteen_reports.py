import io
import re

import xlsxwriter


EXCEL_MIMETYPE = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
PDF_MIMETYPE = "application/pdf"

_WIDE_TEXT_KEYS = {
    "item_name",
    "ledger_name",
    "customer_label",
    "registration_no",
    "created_by",
}
_MEDIUM_TEXT_KEYS = {
    "bill_no",
    "bill_date",
    "last_receipt_date",
    "ledger_code",
    "type_name",
}
_MONEY_KEYS = {
    "rate",
    "amount",
    "net_amount",
    "received_amount",
    "due_amount",
    "opening_balance",
    "topup_amount",
    "consumption_amount",
    "closing_balance",
}


def _clean_text(value) -> str:
    if value is None:
        return ""
    text = str(value).replace("\r", " ").replace("\n", " ").strip()
    return re.sub(r"\s+", " ", text)


def _slugify(value: str, default: str = "Report") -> str:
    text = re.sub(r"[^A-Za-z0-9]+", "_", _clean_text(value))
    text = text.strip("_")
    return text or default


def _safe_float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _column_type(column: dict) -> str:
    explicit_format = str((column or {}).get("format") or "").strip().lower()
    if explicit_format in {"money", "number", "int", "count"}:
        return "int" if explicit_format in {"int", "count"} else explicit_format
    key = str((column or {}).get("key") or "").strip().lower()
    if key in _MONEY_KEYS:
        return "money"
    if key == "qty":
        return "number"
    return "text"


def _display_value(column: dict, value) -> str:
    if value is None:
        return "-"
    if _column_type(column) in {"money", "number"}:
        return f"{_safe_float(value, 0):,.2f}"
    if _column_type(column) == "int":
        try:
            return f"{int(round(_safe_float(value, 0))):,}"
        except Exception:
            return "0"
    text = _clean_text(value)
    return text or "-"


def _pdf_money(value) -> str:
    return f"Rs. {_safe_float(value, 0):,.2f}"


def _summary_rows(summary: dict | None) -> list[tuple[str, object, str]]:
    summary = summary or {}
    rows: list[tuple[str, object, str]] = [("Rows", int(summary.get("row_count") or 0), "int")]
    if "qty" in summary:
        rows.append(("Qty", _safe_float(summary.get("qty"), 0), "number"))
    if "amount" in summary:
        rows.append(("Amount", _safe_float(summary.get("amount"), 0), "money"))
    if "net_amount" in summary:
        rows.append(("Net", _safe_float(summary.get("net_amount"), 0), "money"))
    if "received_amount" in summary:
        rows.append(("Received", _safe_float(summary.get("received_amount"), 0), "money"))
    if "due_amount" in summary:
        rows.append(("Due", _safe_float(summary.get("due_amount"), 0), "money"))
    if "opening_balance" in summary:
        rows.append(("Opening Balance", _safe_float(summary.get("opening_balance"), 0), "money"))
    if "topup_amount" in summary:
        rows.append(("Total Top-up", _safe_float(summary.get("topup_amount"), 0), "money"))
    if "consumption_amount" in summary:
        rows.append(("Wallet Consumption", _safe_float(summary.get("consumption_amount"), 0), "money"))
    if "closing_balance" in summary:
        rows.append(("Closing Balance", _safe_float(summary.get("closing_balance"), 0), "money"))
    if "bill_count" in summary:
        rows.append(("Bill Count", int(summary.get("bill_count") or 0), "int"))
    if "transaction_count" in summary:
        rows.append(("Transaction Count", int(summary.get("transaction_count") or 0), "int"))
    return rows


def _filter_lines(filters: dict | None, report_label: str, branding: dict | None) -> list[str]:
    filters = filters or {}
    branding = branding or {}
    unit = _clean_text(filters.get("unit") or branding.get("unit_label"))
    lines = [
        f"Unit: {unit or 'N/A'}",
        f"Report: {_clean_text(report_label) or 'Report'}",
        f"Date Range: {_clean_text(filters.get('from_date')) or '-'} to {_clean_text(filters.get('to_date')) or '-'}",
        f"Customer Type: {_clean_text(filters.get('type_name')) or 'All Types'}",
    ]
    if _clean_text(filters.get("search")):
        lines.append(f"Search: {_clean_text(filters.get('search'))}")
    return lines


def _excel_column_width(column: dict, rows: list[dict]) -> int:
    key = str((column or {}).get("key") or "").strip().lower()
    label = str((column or {}).get("label") or key or "").strip()
    max_len = max(10, len(label) + 2)
    for row in (rows or [])[:120]:
        max_len = max(max_len, len(_display_value(column, (row or {}).get(key))) + 2)
    if key in _WIDE_TEXT_KEYS:
        return min(max(max_len, 20), 36)
    if key in _MEDIUM_TEXT_KEYS:
        return min(max(max_len, 14), 22)
    if _column_type(column) in {"money", "number"}:
        return min(max(max_len, 12), 18)
    return min(max_len, 24)


def _write_excel_table_sheet(
    workbook,
    worksheet,
    *,
    title: str,
    columns: list[dict],
    rows: list[dict],
    formats: dict,
    start_row: int = 0,
    header_row_offset: int = 2,
):
    if columns:
        worksheet.merge_range(start_row, 0, start_row, max(0, len(columns) - 1), title, formats["title"])
    else:
        worksheet.write(start_row, 0, title, formats["title"])
    header_row = start_row + header_row_offset
    for col_idx, column in enumerate(columns or []):
        worksheet.write(header_row, col_idx, _clean_text(column.get("label") or column.get("key")), formats["header"])
        worksheet.set_column(col_idx, col_idx, _excel_column_width(column, rows or []))

    row_ptr = header_row + 1
    for row in rows or []:
        for col_idx, column in enumerate(columns or []):
            key = str(column.get("key") or "").strip()
            value = (row or {}).get(key)
            col_type = _column_type(column)
            if col_type == "money":
                worksheet.write_number(row_ptr, col_idx, _safe_float(value, 0), formats["money"])
            elif col_type == "number":
                worksheet.write_number(row_ptr, col_idx, _safe_float(value, 0), formats["number"])
            elif col_type == "int":
                worksheet.write_number(row_ptr, col_idx, int(round(_safe_float(value, 0))), formats["int"])
            else:
                worksheet.write(row_ptr, col_idx, _clean_text(value), formats["text"])
        row_ptr += 1

    worksheet.freeze_panes(header_row + 1, 0)
    worksheet.set_default_row(20)


def _pdf_table_for_rows(*, columns: list[dict], rows: list[dict], available_width: float, colors, Paragraph, Table, TableStyle, header_style, cell_style, cell_right_style):
    header_row = [Paragraph(_clean_text(column.get("label") or column.get("key")), header_style) for column in (columns or [])]
    data_rows = [header_row]
    for row in rows or []:
        pdf_row = []
        for column in columns or []:
            style = cell_right_style if _column_type(column) in {"money", "number", "int"} else cell_style
            value = (row or {}).get(str(column.get("key") or "").strip())
            display = _display_value(column, value)
            if _column_type(column) == "money":
                display = _pdf_money(value)
            pdf_row.append(Paragraph(display, style))
        data_rows.append(pdf_row)

    weights = []
    for column in columns or []:
        key = str(column.get("key") or "").strip().lower()
        if key in _WIDE_TEXT_KEYS:
            weights.append(2.2)
        elif key in _MEDIUM_TEXT_KEYS:
            weights.append(1.4)
        elif _column_type(column) in {"money", "number", "int"}:
            weights.append(1.0)
        else:
            weights.append(1.2)
    total_weight = sum(weights) or 1
    col_widths = [available_width * (weight / total_weight) for weight in weights]

    table = Table(data_rows, colWidths=col_widths, repeatRows=1)
    table_style = [
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#0f5b78")),
        ("GRID", (0, 0), (-1, -1), 0.35, colors.HexColor("#d7e3ec")),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 5),
        ("RIGHTPADDING", (0, 0), (-1, -1), 5),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ]
    for row_idx in range(1, len(data_rows)):
        if row_idx % 2 == 0:
            table_style.append(("BACKGROUND", (0, row_idx), (-1, row_idx), colors.HexColor("#f8fbfd")))
    for idx, column in enumerate(columns or []):
        if _column_type(column) in {"money", "number", "int"}:
            table_style.append(("ALIGN", (idx, 1), (idx, -1), "RIGHT"))
    table.setStyle(TableStyle(table_style))
    return table


def build_canteen_excel(
    *,
    branding: dict,
    report_label: str,
    columns: list[dict],
    rows: list[dict],
    summary: dict,
    filters: dict,
    exported_by: str,
    exported_at: str,
    extra_tables: list[dict] | None = None,
):
    output = io.BytesIO()
    workbook = xlsxwriter.Workbook(output, {"in_memory": True})

    formats = {
        "title": workbook.add_format(
            {
                "bold": True,
                "font_size": 14,
                "align": "center",
                "valign": "vcenter",
                "bg_color": "#0f5b78",
                "font_color": "#ffffff",
            }
        ),
        "section": workbook.add_format(
            {
                "bold": True,
                "font_size": 10,
                "bg_color": "#e7f1f8",
                "font_color": "#0b4158",
                "border": 1,
            }
        ),
        "meta": workbook.add_format({"font_size": 9, "color": "#4b647d"}),
        "header": workbook.add_format(
            {
                "bold": True,
                "font_size": 9,
                "align": "center",
                "valign": "vcenter",
                "bg_color": "#0f5b78",
                "font_color": "#ffffff",
                "border": 1,
                "text_wrap": True,
            }
        ),
        "text": workbook.add_format({"font_size": 9, "border": 1, "valign": "top", "text_wrap": True}),
        "number": workbook.add_format({"font_size": 9, "border": 1, "num_format": "#,##0.00"}),
        "int": workbook.add_format({"font_size": 9, "border": 1, "num_format": "#,##0"}),
        "money": workbook.add_format({"font_size": 9, "border": 1, "num_format": '"\u20B9"#,##0.00'}),
    }

    title = _clean_text(branding.get("print_title") or branding.get("display_name") or "Canteen")
    unit_label = _clean_text(filters.get("unit") or branding.get("unit_label"))

    summary_ws = workbook.add_worksheet("Summary")
    summary_ws.merge_range(0, 0, 0, 4, f"{title} - {report_label}", formats["title"])
    summary_ws.write(2, 0, f"Exported By: {_clean_text(exported_by) or 'Unknown'}", formats["meta"])
    summary_ws.write(3, 0, f"Exported At: {_clean_text(exported_at)}", formats["meta"])

    filter_lines = _filter_lines(filters, report_label, branding)
    for idx, line in enumerate(filter_lines, start=5):
        summary_ws.write(idx, 0, line, formats["meta"])

    summary_ws.merge_range(11, 0, 11, 1, "Report Summary", formats["section"])
    summary_rows = _summary_rows(summary)
    for idx, (label, value, value_type) in enumerate(summary_rows, start=12):
        summary_ws.write(idx, 0, label, formats["text"])
        fmt = formats["money"] if value_type == "money" else formats["number"] if value_type == "number" else formats["int"]
        summary_ws.write(idx, 1, value, fmt)

    summary_ws.set_column(0, 0, 22)
    summary_ws.set_column(1, 1, 16)
    summary_ws.set_column(2, 4, 4)

    detail_title = _slugify(report_label, "Report")[:31] or "Report"
    detail_ws = workbook.add_worksheet(detail_title)
    _write_excel_table_sheet(workbook, detail_ws, title=f"{title} - {report_label}", columns=columns, rows=rows, formats=formats, header_row_offset=5)
    detail_ws.write(2, 0, f"Unit: {unit_label}", formats["meta"])
    detail_ws.write(3, 0, f"Rows Exported: {len(rows or [])}", formats["meta"])

    for table in extra_tables or []:
        table_title = _clean_text(table.get("title") or "Summary")
        sheet_name = _slugify(table.get("sheet_name") or table_title, "Summary")[:31] or "Summary"
        worksheet = workbook.add_worksheet(sheet_name)
        _write_excel_table_sheet(
            workbook,
            worksheet,
            title=f"{title} - {table_title}",
            columns=table.get("columns") or [],
            rows=table.get("rows") or [],
            formats=formats,
        )

    workbook.close()
    output.seek(0)
    filename = f"Canteen_{_slugify(report_label)}_{unit_label}_{_clean_text(filters.get('from_date'))}_to_{_clean_text(filters.get('to_date'))}.xlsx"
    return output.getvalue(), filename, EXCEL_MIMETYPE


def build_canteen_pdf(
    *,
    branding: dict,
    report_label: str,
    columns: list[dict],
    rows: list[dict],
    summary: dict,
    filters: dict,
    exported_by: str,
    exported_at: str,
    extra_tables: list[dict] | None = None,
):
    try:
        from reportlab.lib import colors
        from reportlab.lib.pagesizes import A4, landscape
        from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
        from reportlab.platypus import PageBreak, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle
    except Exception as exc:
        raise RuntimeError(f"ReportLab is required for PDF export: {exc}") from exc

    buffer = io.BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=landscape(A4),
        leftMargin=18,
        rightMargin=18,
        topMargin=18,
        bottomMargin=18,
    )

    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        "canteen_pdf_title",
        parent=styles["Heading2"],
        fontName="Helvetica-Bold",
        fontSize=14,
        leading=16,
        textColor=colors.HexColor("#0b4158"),
        alignment=1,
    )
    meta_style = ParagraphStyle(
        "canteen_pdf_meta",
        parent=styles["BodyText"],
        fontName="Helvetica",
        fontSize=8.5,
        leading=11,
        textColor=colors.HexColor("#4b647d"),
    )
    header_style = ParagraphStyle(
        "canteen_pdf_header",
        parent=styles["BodyText"],
        fontName="Helvetica-Bold",
        fontSize=7.8,
        leading=9,
        textColor=colors.white,
        alignment=1,
    )
    cell_style = ParagraphStyle(
        "canteen_pdf_cell",
        parent=styles["BodyText"],
        fontName="Helvetica",
        fontSize=7.4,
        leading=9,
        textColor=colors.HexColor("#11233a"),
        wordWrap="CJK",
    )
    cell_right_style = ParagraphStyle(
        "canteen_pdf_cell_right",
        parent=cell_style,
        alignment=2,
    )

    story = [
        Paragraph(_clean_text(branding.get("print_title") or branding.get("display_name") or "Canteen Report"), title_style),
        Paragraph(_clean_text(report_label), title_style),
        Spacer(1, 8),
        Paragraph(f"Exported By: {_clean_text(exported_by) or 'Unknown'}", meta_style),
        Paragraph(f"Exported At: {_clean_text(exported_at)}", meta_style),
    ]
    for line in _filter_lines(filters, report_label, branding):
        story.append(Paragraph(line, meta_style))
    story.append(Spacer(1, 10))

    summary_data = [["Metric", "Value"]]
    for label, value, value_type in _summary_rows(summary):
        if value_type == "money":
            display = _pdf_money(value)
        elif value_type == "number":
            display = f"{_safe_float(value, 0):,.2f}"
        else:
            display = str(int(value or 0))
        summary_data.append([label, display])

    summary_table = Table(summary_data, colWidths=[120, 100], repeatRows=1)
    summary_table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#0f5b78")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#d7e3ec")),
                ("BACKGROUND", (0, 1), (-1, -1), colors.HexColor("#f8fbfd")),
                ("ALIGN", (1, 1), (1, -1), "RIGHT"),
            ]
        )
    )
    story.append(summary_table)
    story.append(Spacer(1, 12))

    available_width = 790
    story.append(
        _pdf_table_for_rows(
            columns=columns,
            rows=rows,
            available_width=available_width,
            colors=colors,
            Paragraph=Paragraph,
            Table=Table,
            TableStyle=TableStyle,
            header_style=header_style,
            cell_style=cell_style,
            cell_right_style=cell_right_style,
        )
    )

    for table in extra_tables or []:
        story.append(PageBreak())
        table_title = _clean_text(table.get("title") or "Summary")
        story.append(Paragraph(table_title, title_style))
        story.append(Spacer(1, 8))
        table_summary = table.get("summary") or {}
        if table_summary:
            summary_data = [["Metric", "Value"]]
            for label, value, value_type in _summary_rows(table_summary):
                if value_type == "money":
                    display = _pdf_money(value)
                elif value_type == "number":
                    display = f"{_safe_float(value, 0):,.2f}"
                else:
                    display = str(int(value or 0))
                summary_data.append([label, display])
            extra_summary_table = Table(summary_data, colWidths=[120, 100], repeatRows=1)
            extra_summary_table.setStyle(
                TableStyle(
                    [
                        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#0f5b78")),
                        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                        ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#d7e3ec")),
                        ("BACKGROUND", (0, 1), (-1, -1), colors.HexColor("#f8fbfd")),
                        ("ALIGN", (1, 1), (1, -1), "RIGHT"),
                    ]
                )
            )
            story.append(extra_summary_table)
            story.append(Spacer(1, 10))
        story.append(
            _pdf_table_for_rows(
                columns=table.get("columns") or [],
                rows=table.get("rows") or [],
                available_width=available_width,
                colors=colors,
                Paragraph=Paragraph,
                Table=Table,
                TableStyle=TableStyle,
                header_style=header_style,
                cell_style=cell_style,
                cell_right_style=cell_right_style,
            )
        )

    doc.build(story)
    buffer.seek(0)
    filename = f"Canteen_{_slugify(report_label)}_{_clean_text(filters.get('unit'))}_{_clean_text(filters.get('from_date'))}_to_{_clean_text(filters.get('to_date'))}.pdf"
    return buffer.getvalue(), filename, PDF_MIMETYPE


def build_canteen_export(
    *,
    export_format: str,
    branding: dict,
    report_label: str,
    columns: list[dict],
    rows: list[dict],
    summary: dict,
    filters: dict,
    exported_by: str,
    exported_at: str,
    extra_tables: list[dict] | None = None,
):
    fmt = _clean_text(export_format).lower()
    if fmt == "pdf":
        return build_canteen_pdf(
            branding=branding,
            report_label=report_label,
            columns=columns,
            rows=rows,
            summary=summary,
            extra_tables=extra_tables or [],
            filters=filters,
            exported_by=exported_by,
            exported_at=exported_at,
        )
    return build_canteen_excel(
        branding=branding,
        report_label=report_label,
        columns=columns,
        rows=rows,
        summary=summary,
        extra_tables=extra_tables or [],
        filters=filters,
        exported_by=exported_by,
        exported_at=exported_at,
    )
