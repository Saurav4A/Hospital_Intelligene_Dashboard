import io
from datetime import datetime

import xlsxwriter


def _filter_lines(filters, scope_codes):
    filters = filters or {}
    lines = []
    scope_text = ", ".join(scope_codes or []) if scope_codes else "All permitted asset units"
    lines.append(f"Scope: {scope_text}")
    if filters.get("search"):
        lines.append(f"Search: {filters['search']}")
    if filters.get("location_code"):
        lines.append(f"Location: {filters['location_code']}")
    if filters.get("status_code"):
        lines.append(f"Status Code: {filters['status_code']}")
    if filters.get("assignment_type"):
        lines.append(f"Assignment Type: {filters['assignment_type']}")
    if filters.get("warranty_bucket"):
        lines.append(f"Warranty: {filters['warranty_bucket']}")
    if filters.get("coverage_alert"):
        lines.append(f"Coverage Priority: {filters['coverage_alert']}")
    return lines


def _write_table(worksheet, start_row, start_col, headers, rows, formats, column_widths=None):
    header_fmt = formats["header"]
    text_fmt = formats["text"]
    money_fmt = formats["money"]
    int_fmt = formats["int"]
    date_fmt = formats["date"]

    for col_idx, header in enumerate(headers):
        worksheet.write(start_row, start_col + col_idx, header["label"], header_fmt)
        if column_widths:
            worksheet.set_column(start_col + col_idx, start_col + col_idx, column_widths[col_idx])

    row_no = start_row + 1
    for row in rows:
        for col_idx, header in enumerate(headers):
            value = row.get(header["key"])
            fmt = text_fmt
            if header.get("type") == "money":
                fmt = money_fmt
            elif header.get("type") == "int":
                fmt = int_fmt
            elif header.get("type") == "date":
                fmt = date_fmt
            worksheet.write(row_no, start_col + col_idx, value, fmt)
        row_no += 1
    return row_no


def _write_section_title(worksheet, row, start_col, end_col, title, fmt):
    if end_col <= start_col:
        worksheet.write(row, start_col, title, fmt)
        return
    worksheet.merge_range(row, start_col, row, end_col, title, fmt)


def build_asset_excel(*, summary, register_rows, movement_rows, filters, exported_by, exported_at, scope_codes):
    output = io.BytesIO()
    workbook = xlsxwriter.Workbook(output, {"in_memory": True})

    formats = {
        "title": workbook.add_format(
            {
                "bold": True,
                "font_size": 14,
                "align": "center",
                "valign": "vcenter",
                "bg_color": "#243b7b",
                "font_color": "#ffffff",
            }
        ),
        "meta": workbook.add_format({"font_size": 9, "color": "#475569"}),
        "section": workbook.add_format(
            {
                "bold": True,
                "font_size": 11,
                "bg_color": "#e8eefc",
                "border": 1,
                "align": "left",
                "valign": "vcenter",
            }
        ),
        "header": workbook.add_format(
            {
                "bold": True,
                "font_size": 9,
                "align": "center",
                "valign": "vcenter",
                "bg_color": "#243b7b",
                "font_color": "#ffffff",
                "border": 1,
                "text_wrap": True,
            }
        ),
        "text": workbook.add_format({"font_size": 9, "border": 1, "text_wrap": True, "valign": "top"}),
        "int": workbook.add_format({"font_size": 9, "border": 1, "num_format": "#,##0", "valign": "top"}),
        "money": workbook.add_format({"font_size": 9, "border": 1, "num_format": "#,##0.00"}),
        "date": workbook.add_format({"font_size": 9, "border": 1, "valign": "top"}),
    }

    summary_ws = workbook.add_worksheet("Summary")
    summary_ws.merge_range(0, 0, 0, 8, "Asset Management Report", formats["title"])
    summary_ws.write(2, 0, f"Exported By: {exported_by}", formats["meta"])
    summary_ws.write(3, 0, f"Exported At: {exported_at}", formats["meta"])
    filter_lines = _filter_lines(filters, scope_codes)
    for idx, line in enumerate(filter_lines, start=4):
        summary_ws.write(idx, 0, line, formats["meta"])

    kpi_rows = [
        ["Total Assets", summary.get("total_assets", 0)],
        ["Total Asset Value", summary.get("total_asset_value", 0)],
        ["Active", summary.get("active_count", 0)],
        ["In Use", summary.get("in_use_count", 0)],
        ["In Store", summary.get("in_store_count", 0)],
        ["Under Repair", summary.get("repair_count", 0)],
        ["Disposed", summary.get("disposed_count", 0)],
        ["Lost", summary.get("lost_count", 0)],
        ["Expiring in 30 Days", summary.get("expiring_30_count", 0)],
        ["Expiring in 90 Days", summary.get("expiring_90_count", 0)],
        ["Expired", summary.get("expired_count", 0)],
    ]
    _write_section_title(summary_ws, 7, 0, 1, "Key Metrics", formats["section"])
    for row_idx, (label, value) in enumerate(kpi_rows, start=8):
        summary_ws.write(row_idx, 0, label, formats["text"])
        summary_ws.write(row_idx, 1, value, formats["money"] if label == "Total Asset Value" else formats["int"])

    status_headers = [{"key": "status_label", "label": "Status"}, {"key": "count", "label": "Count", "type": "int"}]
    _write_section_title(summary_ws, 7, 3, 4, "Status Summary", formats["section"])
    _write_table(summary_ws, 8, 3, status_headers, summary.get("status_counts") or [], formats, [24, 12])

    location_headers = [
        {"key": "location_code", "label": "Code"},
        {"key": "location_name", "label": "Location"},
        {"key": "count", "label": "Count", "type": "int"},
    ]
    _write_section_title(summary_ws, 20, 0, 2, "Location Summary", formats["section"])
    _write_table(summary_ws, 21, 0, location_headers, summary.get("location_counts") or [], formats, [12, 26, 12])

    machine_headers = [
        {"key": "machine_type_name", "label": "Machine Type"},
        {"key": "count", "label": "Count", "type": "int"},
    ]
    _write_section_title(summary_ws, 20, 4, 5, "Machine Type Summary", formats["section"])
    _write_table(summary_ws, 21, 4, machine_headers, summary.get("machine_type_counts") or [], formats, [28, 12])

    summary_ws.freeze_panes(8, 0)
    summary_ws.set_row(0, 22)
    summary_ws.set_column(0, 0, 24)
    summary_ws.set_column(1, 1, 14)
    summary_ws.set_column(2, 2, 4)
    summary_ws.set_column(3, 3, 24)
    summary_ws.set_column(4, 5, 14)
    summary_ws.set_column(6, 8, 4)

    register_ws = workbook.add_worksheet("Asset Register")
    register_ws.merge_range(0, 0, 0, 14, "Asset Register", formats["title"])
    register_ws.write(2, 0, f"Rows Exported: {len(register_rows or [])}", formats["meta"])
    register_headers = [
        {"key": "asset_code", "label": "Asset Code"},
        {"key": "equipment_name", "label": "Equipment"},
        {"key": "machine_type_name", "label": "Machine Type"},
        {"key": "model_name", "label": "Model"},
        {"key": "manufacturer_name", "label": "Manufacturer"},
        {"key": "supplier_name", "label": "Supplier"},
        {"key": "serial_number", "label": "Serial No."},
        {"key": "invoice_date_display", "label": "Invoice Date", "type": "date"},
        {"key": "asset_value", "label": "Asset Value", "type": "money"},
        {"key": "warranty_end_date_display", "label": "Warranty End", "type": "date"},
        {"key": "coverage_status_label", "label": "Coverage"},
        {"key": "coverage_expiry_date_display", "label": "Coverage Expiry", "type": "date"},
        {"key": "location_name", "label": "Location"},
        {"key": "current_holder_label", "label": "Current Holder"},
        {"key": "asset_status_label", "label": "Status"},
    ]
    _write_table(
        register_ws,
        4,
        0,
        register_headers,
        register_rows or [],
        formats,
        [20, 28, 18, 18, 28, 28, 18, 14, 14, 16, 20, 16, 24, 30, 14],
    )
    register_ws.set_default_row(20)
    register_ws.freeze_panes(5, 0)

    movement_ws = workbook.add_worksheet("Movement History")
    movement_ws.merge_range(0, 0, 0, 7, "Movement History", formats["title"])
    movement_ws.write(2, 0, f"Rows Exported: {len(movement_rows or [])}", formats["meta"])
    movement_headers = [
        {"key": "created_at_display", "label": "Activity Time"},
        {"key": "asset_code", "label": "Asset Code"},
        {"key": "equipment_name", "label": "Equipment"},
        {"key": "movement_type", "label": "Type"},
        {"key": "from_entity_label", "label": "From"},
        {"key": "to_entity_label", "label": "To"},
        {"key": "status_after_label", "label": "Status After"},
        {"key": "created_by_username", "label": "Updated By"},
        {"key": "remarks", "label": "Remarks"},
    ]
    _write_table(
        movement_ws,
        4,
        0,
        movement_headers,
        movement_rows or [],
        formats,
        [18, 18, 22, 14, 22, 22, 16, 18, 32],
    )
    movement_ws.set_default_row(20)
    movement_ws.freeze_panes(5, 0)

    workbook.close()
    output.seek(0)
    filename = f"Asset_Management_Report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    return output.getvalue(), filename, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"


def build_asset_pdf(*, summary, register_rows, movement_rows, filters, exported_by, exported_at, scope_codes):
    try:
        from reportlab.lib import colors
        from reportlab.lib.pagesizes import landscape, A4
        from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
        from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle
    except Exception as exc:
        raise RuntimeError(f"ReportLab is required for PDF export: {exc}") from exc

    buffer = io.BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=landscape(A4),
        leftMargin=18,
        rightMargin=18,
        topMargin=20,
        bottomMargin=18,
    )
    styles = getSampleStyleSheet()
    body_style = styles["BodyText"]
    body_style.fontName = "Helvetica"
    body_style.fontSize = 9
    body_style.leading = 11

    table_header_style = ParagraphStyle(
        "asset_table_header",
        parent=styles["BodyText"],
        fontName="Helvetica-Bold",
        fontSize=8,
        leading=10,
        textColor=colors.white,
    )
    table_cell_style = ParagraphStyle(
        "asset_table_cell",
        parent=styles["BodyText"],
        fontName="Helvetica",
        fontSize=7.4,
        leading=9,
        textColor=colors.HexColor("#10213d"),
        wordWrap="CJK",
    )
    table_cell_small_style = ParagraphStyle(
        "asset_table_cell_small",
        parent=table_cell_style,
        fontSize=7,
        leading=8.4,
    )

    def _pdf_cell(value, *, small=False):
        text = str(value or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        return Paragraph(text or "-", table_cell_small_style if small else table_cell_style)

    def _pdf_head(text):
        safe = str(text or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        return Paragraph(safe, table_header_style)

    story = [
        Paragraph("Asset Management Report", styles["Title"]),
        Spacer(1, 8),
        Paragraph(f"Exported By: {exported_by}", body_style),
        Paragraph(f"Exported At: {exported_at}", body_style),
    ]
    for line in _filter_lines(filters, scope_codes):
        story.append(Paragraph(line, body_style))
    story.append(Spacer(1, 10))

    kpi_table = Table(
        [
            [_pdf_head("Metric"), _pdf_head("Value")],
            [_pdf_cell("Total Assets"), _pdf_cell(summary.get("total_assets", 0))],
            [_pdf_cell("Total Asset Value"), _pdf_cell(summary.get("total_asset_value_display") or summary.get("total_asset_value", 0))],
            [_pdf_cell("Active"), _pdf_cell(summary.get("active_count", 0))],
            [_pdf_cell("In Use"), _pdf_cell(summary.get("in_use_count", 0))],
            [_pdf_cell("In Store"), _pdf_cell(summary.get("in_store_count", 0))],
            [_pdf_cell("Under Repair"), _pdf_cell(summary.get("repair_count", 0))],
            [_pdf_cell("Disposed"), _pdf_cell(summary.get("disposed_count", 0))],
            [_pdf_cell("Lost"), _pdf_cell(summary.get("lost_count", 0))],
            [_pdf_cell("Expiring in 30 Days"), _pdf_cell(summary.get("expiring_30_count", 0))],
            [_pdf_cell("Expiring in 90 Days"), _pdf_cell(summary.get("expiring_90_count", 0))],
            [_pdf_cell("Expired"), _pdf_cell(summary.get("expired_count", 0))],
        ],
        repeatRows=1,
        colWidths=[220, 90],
    )
    kpi_table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#243b7b")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#cbd5e1")),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("BACKGROUND", (0, 1), (-1, -1), colors.whitesmoke),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("LEFTPADDING", (0, 0), (-1, -1), 6),
                ("RIGHTPADDING", (0, 0), (-1, -1), 6),
                ("TOPPADDING", (0, 0), (-1, -1), 5),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
            ]
        )
    )
    story.append(kpi_table)
    story.append(Spacer(1, 14))

    story.append(Paragraph("Asset Register", styles["Heading2"]))
    register_table_data = [[
        _pdf_head("Asset Code"),
        _pdf_head("Equipment"),
        _pdf_head("Machine Type"),
        _pdf_head("Manufacturer"),
        _pdf_head("Supplier"),
        _pdf_head("Location"),
        _pdf_head("Value"),
        _pdf_head("Coverage"),
        _pdf_head("Current Holder"),
        _pdf_head("Status"),
    ]]
    for row in register_rows or []:
        register_table_data.append(
            [
                _pdf_cell(row.get("asset_code")),
                _pdf_cell(row.get("equipment_name")),
                _pdf_cell(row.get("machine_type_name"), small=True),
                _pdf_cell(row.get("manufacturer_name")),
                _pdf_cell(row.get("supplier_name")),
                _pdf_cell(row.get("location_name")),
                _pdf_cell(row.get("asset_value_display"), small=True),
                _pdf_cell(row.get("coverage_status_label"), small=True),
                _pdf_cell(row.get("current_holder_label")),
                _pdf_cell(row.get("asset_status_label"), small=True),
            ]
        )
    register_table = Table(register_table_data, repeatRows=1, colWidths=[68, 96, 62, 76, 76, 70, 58, 70, 92, 42])
    register_table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#243b7b")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#d9e2ef")),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("LEFTPADDING", (0, 0), (-1, -1), 5),
                ("RIGHTPADDING", (0, 0), (-1, -1), 5),
                ("TOPPADDING", (0, 0), (-1, -1), 4),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
            ]
        )
    )
    story.append(register_table)
    story.append(Spacer(1, 14))

    if movement_rows:
        story.append(Paragraph("Movement History", styles["Heading2"]))
        movement_table_data = [[
            _pdf_head("Time"),
            _pdf_head("Asset"),
            _pdf_head("Type"),
            _pdf_head("From"),
            _pdf_head("To"),
            _pdf_head("Status After"),
            _pdf_head("By"),
        ]]
        for row in movement_rows:
            movement_table_data.append(
                [
                    _pdf_cell(row.get("created_at_display"), small=True),
                    _pdf_cell(row.get("asset_code"), small=True),
                    _pdf_cell(row.get("movement_type"), small=True),
                    _pdf_cell(row.get("from_entity_label")),
                    _pdf_cell(row.get("to_entity_label")),
                    _pdf_cell(row.get("status_after_label"), small=True),
                    _pdf_cell(row.get("created_by_username"), small=True),
                ]
            )
        movement_table = Table(movement_table_data, repeatRows=1, colWidths=[76, 74, 54, 146, 146, 66, 70])
        movement_table.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#243b7b")),
                    ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                    ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#d9e2ef")),
                    ("VALIGN", (0, 0), (-1, -1), "TOP"),
                    ("LEFTPADDING", (0, 0), (-1, -1), 5),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 5),
                    ("TOPPADDING", (0, 0), (-1, -1), 4),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
                ]
            )
        )
        story.append(movement_table)

    doc.build(story)
    buffer.seek(0)
    filename = f"Asset_Management_Report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
    return buffer.getvalue(), filename, "application/pdf"


def build_asset_export(*, export_format, summary, register_rows, movement_rows, filters, exported_by, scope_codes):
    exported_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    fmt = str(export_format or "xlsx").strip().lower()
    if fmt == "pdf":
        return build_asset_pdf(
            summary=summary,
            register_rows=register_rows,
            movement_rows=movement_rows,
            filters=filters,
            exported_by=exported_by,
            exported_at=exported_at,
            scope_codes=scope_codes,
        )
    return build_asset_excel(
        summary=summary,
        register_rows=register_rows,
        movement_rows=movement_rows,
        filters=filters,
        exported_by=exported_by,
        exported_at=exported_at,
        scope_codes=scope_codes,
    )


def build_coverage_alert_pdf(*, unit_code, alert_type, rows, generated_at=None):
    try:
        from reportlab.lib import colors
        from reportlab.lib.pagesizes import landscape, A4
        from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
        from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle
    except Exception as exc:
        raise RuntimeError(f"ReportLab is required for coverage alert PDF: {exc}") from exc

    generated_at = generated_at or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=landscape(A4),
        leftMargin=18,
        rightMargin=18,
        topMargin=20,
        bottomMargin=18,
    )
    styles = getSampleStyleSheet()
    title_style = styles["Title"]
    body_style = styles["BodyText"]
    body_style.fontName = "Helvetica"
    body_style.fontSize = 8.5
    body_style.leading = 10.5
    head_style = ParagraphStyle(
        "coverage_head",
        parent=body_style,
        fontName="Helvetica-Bold",
        fontSize=7.4,
        leading=9,
        textColor=colors.white,
    )
    cell_style = ParagraphStyle(
        "coverage_cell",
        parent=body_style,
        fontSize=7,
        leading=8.4,
        wordWrap="CJK",
    )

    def esc(value):
        return str(value or "-").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

    def cell(value):
        return Paragraph(esc(value), cell_style)

    def head(value):
        return Paragraph(esc(value), head_style)

    story = [
        Paragraph(f"Asset Coverage Alert - {unit_code}", title_style),
        Paragraph(f"Alert Type: {alert_type} | Generated: {generated_at}", body_style),
        Spacer(1, 10),
    ]
    table_data = [[
        head("Unit"),
        head("Asset ID / Code"),
        head("Asset Name"),
        head("Department / Location"),
        head("Coverage Status"),
        head("Vendor"),
        head("Start Date"),
        head("Expiry Date"),
        head("Days Left / Overdue"),
        head("Responsible Remarks / Status"),
    ]]
    for row in rows or []:
        days_left = row.get("coverage_days_left")
        if days_left is None:
            days_text = "Pending update"
        else:
            try:
                days_int = int(days_left)
                days_text = f"{abs(days_int)} day(s) overdue" if days_int < 0 else f"{days_int} day(s) left"
            except Exception:
                days_text = "-"
        table_data.append(
            [
                cell(row.get("location_code") or unit_code),
                cell(f"{row.get('asset_id') or ''} / {row.get('asset_code') or ''}"),
                cell(row.get("equipment_name") or row.get("model_name")),
                cell(row.get("location_name")),
                cell(row.get("coverage_status_label")),
                cell(row.get("coverage_vendor")),
                cell(row.get("coverage_start_date_display")),
                cell(row.get("coverage_expiry_date_display")),
                cell(days_text),
                cell(row.get("coverage_remarks") or row.get("coverage_alert_label")),
            ]
        )

    table = Table(table_data, repeatRows=1, colWidths=[42, 82, 108, 106, 78, 84, 58, 58, 74, 144])
    table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#243b7b")),
                ("GRID", (0, 0), (-1, -1), 0.35, colors.HexColor("#d9e2ef")),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("LEFTPADDING", (0, 0), (-1, -1), 4),
                ("RIGHTPADDING", (0, 0), (-1, -1), 4),
                ("TOPPADDING", (0, 0), (-1, -1), 4),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
            ]
        )
    )
    story.append(table)
    doc.build(story)
    buffer.seek(0)
    filename = f"Asset_Coverage_Alert_{unit_code}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
    return buffer.getvalue(), filename


def build_breakdown_ticket_pdf(*, unit_code, title, rows, generated_at=None):
    try:
        from reportlab.lib import colors
        from reportlab.lib.pagesizes import landscape, A4
        from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
        from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle
    except Exception as exc:
        raise RuntimeError("ReportLab is required for breakdown PDFs.") from exc

    generated_at = generated_at or datetime.now()
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
    title_style = ParagraphStyle("BreakdownTitle", parent=styles["Heading1"], fontSize=15, textColor=colors.HexColor("#243b7b"), spaceAfter=8)
    meta_style = ParagraphStyle("BreakdownMeta", parent=styles["Normal"], fontSize=8, textColor=colors.HexColor("#475569"), spaceAfter=8)
    cell_style = ParagraphStyle("BreakdownCell", parent=styles["Normal"], fontSize=7.2, leading=8.4)
    head_style = ParagraphStyle("BreakdownHead", parent=styles["Normal"], fontSize=7.2, leading=8.4, textColor=colors.white)

    def para(value):
        text = str(value if value is not None else "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        return Paragraph(text or "-", cell_style)

    data = [[
        Paragraph("Ticket", head_style),
        Paragraph("Unit", head_style),
        Paragraph("Asset", head_style),
        Paragraph("Asset Name", head_style),
        Paragraph("Machine Type", head_style),
        Paragraph("Breakdown Date", head_style),
        Paragraph("Reason", head_style),
        Paragraph("Expected Visit", head_style),
        Paragraph("Status / Verdict", head_style),
    ]]
    for row in rows or []:
        verdict = row.get("repair_verdict_label") or row.get("status_label") or row.get("status")
        if row.get("non_repairable"):
            verdict = "Non-Repairable"
        data.append([
            para(row.get("ticket_no")),
            para(row.get("unit_code")),
            para(row.get("asset_code")),
            para(row.get("asset_name")),
            para(row.get("machine_type_name")),
            para(row.get("breakdown_datetime_display")),
            para(row.get("breakdown_reason")),
            para(row.get("expected_technician_visit_display")),
            para(verdict),
        ])
    if len(data) == 1:
        data.append([para("No affected breakdown tickets."), "", "", "", "", "", "", "", ""])

    table = Table(data, repeatRows=1, colWidths=[56, 38, 70, 94, 76, 74, 190, 74, 86])
    table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#243b7b")),
        ("GRID", (0, 0), (-1, -1), 0.35, colors.HexColor("#dbe5f2")),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f8fafc")]),
        ("SPAN", (0, 1), (-1, 1)) if len(data) == 2 and not rows else ("SPAN", (0, 0), (0, 0)),
    ]))

    story = [
        Paragraph(f"Asset Breakdown Tracking - {unit_code}", title_style),
        Paragraph(f"{title} | Generated: {generated_at.strftime('%Y-%m-%d %H:%M')}", meta_style),
        Spacer(1, 6),
        table,
    ]
    doc.build(story)
    filename = f"Asset_Breakdown_{unit_code}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
    return buffer.getvalue(), filename
