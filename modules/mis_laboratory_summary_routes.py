from datetime import datetime
import io
import os
from secrets import token_hex

import pandas as pd
from flask import jsonify, request, send_file, session

from modules import data_fetch


def _build_laboratory_summary_frames(details_df: pd.DataFrame):
    if details_df is None or details_df.empty:
        empty_summary = pd.DataFrame(columns=["Name", "Records", "Revenue"])
        return empty_summary, empty_summary, 0.0, 0, 0.0

    work = details_df.copy()
    work["Doctor"] = work["Doctor"].replace("", "Unknown").fillna("Unknown")
    work["PatientType"] = work["PatientType"].replace("", "Unknown").fillna("Unknown")

    total_revenue = float(work["Amount"].sum())
    total_records = int(len(work))
    total_quantity = float(work["Quantity"].sum())

    doctor_summary = (
        work.groupby("Doctor", dropna=False)
        .agg(Records=("Doctor", "size"), Revenue=("Amount", "sum"))
        .reset_index()
        .rename(columns={"Doctor": "Name"})
        .sort_values("Revenue", ascending=False)
    )

    patient_summary = (
        work.groupby("PatientType", dropna=False)
        .agg(Records=("PatientType", "size"), Revenue=("Amount", "sum"))
        .reset_index()
        .rename(columns={"PatientType": "Name"})
        .sort_values("Revenue", ascending=False)
    )

    return doctor_summary, patient_summary, total_revenue, total_records, total_quantity


def register_mis_laboratory_summary_routes(
    app,
    *,
    login_required,
    analytics_allowed_units_for_session,
    allowed_units_for_session,
    build_laboratory_details_df,
    lab_unit_header_text,
    excel_job_update,
    excel_job_get,
    export_cache_get_bytes,
    export_cache_put_bytes,
    export_executor,
    local_tz,
):
    """Register MIS laboratory summary routes."""
    _analytics_allowed_units_for_session = analytics_allowed_units_for_session
    _allowed_units_for_session = allowed_units_for_session
    _build_laboratory_details_df = build_laboratory_details_df
    _lab_unit_header_text = lab_unit_header_text
    _excel_job_update = excel_job_update
    _excel_job_get = excel_job_get
    _export_cache_get_bytes = export_cache_get_bytes
    _export_cache_put_bytes = export_cache_put_bytes
    EXPORT_EXECUTOR = export_executor
    LOCAL_TZ = local_tz

    @app.route('/api/mis/laboratory_summary')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_mis_laboratory_summary():
        unit = (request.args.get("unit") or "").strip().upper()
        from_date = request.args.get("from") or datetime.now(tz=LOCAL_TZ).strftime("%Y-%m-%d")
        to_date = request.args.get("to") or datetime.now(tz=LOCAL_TZ).strftime("%Y-%m-%d")

        allowed_units = _analytics_allowed_units_for_session()
        target_unit = unit or (allowed_units[0] if allowed_units else None)

        if not target_unit or (allowed_units and target_unit not in allowed_units):
            return jsonify({"status": "error", "message": "Unauthorized Unit Access"}), 403

        df_raw = data_fetch.fetch_laboratory_summary(target_unit, from_date, to_date)
        if df_raw is None:
            return jsonify({"status": "error", "message": "Database error"}), 500

        details_df = _build_laboratory_details_df(df_raw)
        doctor_summary, patient_summary, total_revenue, total_records, total_quantity = _build_laboratory_summary_frames(details_df)

        details_records = details_df.where(pd.notna(details_df), None).to_dict(orient="records")
        doctor_records = doctor_summary.to_dict(orient="records")
        patient_records = patient_summary.to_dict(orient="records")

        return jsonify({
            "status": "success",
            "unit": target_unit,
            "from_date": from_date,
            "to_date": to_date,
            "total_revenue": total_revenue,
            "total_records": total_records,
            "total_quantity": total_quantity,
            "doctor_summary": doctor_records,
            "patienttype_summary": patient_records,
            "details": details_records,
        })

    def _build_laboratory_summary_excel(unit: str, from_date: str, to_date: str, exported_by: str):
        df_raw = data_fetch.fetch_laboratory_summary(unit, from_date, to_date)
        if df_raw is None or df_raw.empty:
            return None, None, "No data available to export"

        details_df = _build_laboratory_details_df(df_raw)
        doctor_summary, patient_summary, total_revenue, total_records, total_quantity = _build_laboratory_summary_frames(details_df)

        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            workbook = writer.book

            title_fmt = workbook.add_format({
                "bold": True,
                "font_size": 14,
                "align": "center",
                "valign": "vcenter",
                "bg_color": "#1e3a8a",
                "font_color": "white",
            })
            subtitle_fmt = workbook.add_format({
                "bold": True,
                "font_size": 10,
                "align": "center",
                "valign": "vcenter",
                "bg_color": "#e2e8f0",
            })
            meta_fmt = workbook.add_format({
                "font_size": 9,
                "align": "center",
                "valign": "vcenter",
                "font_color": "#475569",
            })
            section_fmt = workbook.add_format({
                "bold": True,
                "font_size": 10,
                "align": "left",
                "valign": "vcenter",
                "bg_color": "#f1f5f9",
            })
            header_fmt = workbook.add_format({
                "bold": True,
                "font_size": 9,
                "align": "center",
                "valign": "vcenter",
                "bg_color": "#1e3a8a",
                "font_color": "white",
                "border": 1,
            })
            text_fmt = workbook.add_format({"font_size": 9, "border": 1, "align": "left"})
            int_fmt = workbook.add_format({"font_size": 9, "border": 1, "align": "right", "num_format": "#,##,##0"})
            pct_fmt = workbook.add_format({"font_size": 9, "border": 1, "align": "right", "num_format": "0.00%"})
            money_fmt = workbook.add_format({"font_size": 9, "border": 1, "align": "right", "num_format": "#,##,##0.00"})
            text_col_fmt = workbook.add_format({"font_size": 9, "align": "left"})
            int_col_fmt = workbook.add_format({"font_size": 9, "align": "right", "num_format": "#,##,##0"})
            money_col_fmt = workbook.add_format({"font_size": 9, "align": "right", "num_format": "#,##,##0.00"})
            border_only_fmt = workbook.add_format({"border": 1})

            exported_at = datetime.now(tz=LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S")
            unit_header = _lab_unit_header_text(unit)

            summary_ws = workbook.add_worksheet("Summary")
            writer.sheets["Summary"] = summary_ws

            summary_last_col = 3
            summary_ws.merge_range(0, 0, 0, summary_last_col, "Diagnostics & Laboratory MIS - Revenue Summary", title_fmt)
            summary_ws.merge_range(1, 0, 1, summary_last_col, f"Unit: {unit_header}", subtitle_fmt)
            summary_ws.merge_range(2, 0, 2, summary_last_col, f"Date Range: {from_date} to {to_date}", subtitle_fmt)
            summary_ws.merge_range(3, 0, 3, summary_last_col, f"Exported By: {exported_by} | Exported At: {exported_at}", meta_fmt)

            summary_ws.write(5, 0, "Total Revenue (INR)", section_fmt)
            summary_ws.write(5, 1, total_revenue, money_fmt)
            summary_ws.write(6, 0, "Total Records", section_fmt)
            summary_ws.write(6, 1, total_records, int_fmt)
            summary_ws.write(7, 0, "Total Quantity", section_fmt)
            summary_ws.write(7, 1, total_quantity, int_fmt)

            row_ptr = 9
            summary_ws.merge_range(row_ptr, 0, row_ptr, summary_last_col, "Doctor-wise Revenue Summary", section_fmt)
            row_ptr += 1
            summary_ws.write_row(row_ptr, 0, ["Doctor", "Records", "Revenue (INR)", "Share %"], header_fmt)
            row_ptr += 1
            for _, row in doctor_summary.iterrows():
                share = (float(row["Revenue"]) / total_revenue) if total_revenue else 0.0
                summary_ws.write(row_ptr, 0, row["Name"], text_fmt)
                summary_ws.write(row_ptr, 1, int(row["Records"]), int_fmt)
                summary_ws.write(row_ptr, 2, float(row["Revenue"]), money_fmt)
                summary_ws.write(row_ptr, 3, share, pct_fmt)
                row_ptr += 1

            row_ptr += 2
            summary_ws.merge_range(row_ptr, 0, row_ptr, summary_last_col, "Patient Type-wise Revenue Summary", section_fmt)
            row_ptr += 1
            summary_ws.write_row(row_ptr, 0, ["Patient Type", "Records", "Revenue (INR)", "Share %"], header_fmt)
            row_ptr += 1
            for _, row in patient_summary.iterrows():
                share = (float(row["Revenue"]) / total_revenue) if total_revenue else 0.0
                summary_ws.write(row_ptr, 0, row["Name"], text_fmt)
                summary_ws.write(row_ptr, 1, int(row["Records"]), int_fmt)
                summary_ws.write(row_ptr, 2, float(row["Revenue"]), money_fmt)
                summary_ws.write(row_ptr, 3, share, pct_fmt)
                row_ptr += 1

            summary_ws.merge_range(row_ptr + 1, 0, row_ptr + 1, summary_last_col, "Copyright: (c) ASARFI HOSPITAL", meta_fmt)
            summary_ws.set_column(0, 0, 34)
            summary_ws.set_column(1, 1, 14)
            summary_ws.set_column(2, 2, 18)
            summary_ws.set_column(3, 3, 12)
            summary_ws.freeze_panes(5, 0)

            details_df.to_excel(writer, sheet_name="Details", index=False, startrow=5)
            details_ws = writer.sheets["Details"]
            details_last_col = len(details_df.columns) - 1
            details_ws.merge_range(0, 0, 0, details_last_col, "Diagnostics & Laboratory MIS - Detail Report", title_fmt)
            details_ws.merge_range(1, 0, 1, details_last_col, f"Unit: {unit_header} | Date Range: {from_date} to {to_date}", subtitle_fmt)
            details_ws.merge_range(2, 0, 2, details_last_col, f"Exported By: {exported_by} | Exported At: {exported_at}", meta_fmt)

            for col_num, col_name in enumerate(details_df.columns):
                details_ws.write(5, col_num, col_name, header_fmt)
                if col_name == "Amount":
                    details_ws.set_column(col_num, col_num, 16, money_col_fmt)
                elif col_name == "Quantity":
                    details_ws.set_column(col_num, col_num, 10, int_col_fmt)
                elif col_name == "BillDate":
                    details_ws.set_column(col_num, col_num, 14, text_col_fmt)
                elif col_name in {"Patient", "Service_Name"}:
                    details_ws.set_column(col_num, col_num, 28, text_col_fmt)
                else:
                    details_ws.set_column(col_num, col_num, 20, text_col_fmt)

            data_start_row = 6
            data_end_row = 5 + len(details_df)
            if len(details_df) > 0:
                details_ws.conditional_format(
                    data_start_row, 0, data_end_row, details_last_col,
                    {"type": "formula", "criteria": "=TRUE", "format": border_only_fmt}
                )

            footer_row = 6 + len(details_df)
            details_ws.merge_range(footer_row, 0, footer_row, details_last_col, "Copyright: (c) ASARFI HOSPITAL", meta_fmt)
            details_ws.freeze_panes(6, 0)

        output.seek(0)
        filename = f"Laboratory_Summary_{unit}_{from_date}_to_{to_date}.xlsx"
        return output.getvalue(), filename, None

    def _run_laboratory_summary_excel_job(job_id: str, unit: str, from_date: str, to_date: str, exported_by: str):
        _excel_job_update(job_id, state="running")
        try:
            data, filename, err = _build_laboratory_summary_excel(unit, from_date, to_date, exported_by)
            if not data:
                _excel_job_update(job_id, state="error", error=err or "No data available to export")
                return
            _export_cache_put_bytes("lab_summary_xlsx_job", data, job_id)
            _excel_job_update(job_id, state="done", filename=filename)
        except Exception as exc:
            _excel_job_update(job_id, state="error", error=str(exc))

    @app.route('/api/mis/laboratory_summary/export_excel')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_mis_laboratory_summary_export_excel():
        unit = (request.args.get("unit") or "").strip().upper()
        from_date = (request.args.get("from") or "").strip()
        to_date = (request.args.get("to") or "").strip()
        allowed_units = _analytics_allowed_units_for_session()

        if not allowed_units:
            return "No unit access assigned", 403
        if not unit:
            if len(allowed_units) == 1:
                unit = allowed_units[0]
            else:
                return "Please select a unit", 400
        if unit not in allowed_units:
            return f"Unit {unit} not permitted", 403
        if not from_date or not to_date:
            return "Please select a valid date range", 400

        exported_by = session.get("username") or session.get("user") or "Unknown"
        data, filename, err = _build_laboratory_summary_excel(unit, from_date, to_date, exported_by)
        if not data:
            return err or "No data available to export", 404
        return send_file(
            io.BytesIO(data),
            as_attachment=True,
            download_name=filename,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    @app.route('/api/mis/laboratory_summary/export_excel_job', methods=["POST"])
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_mis_laboratory_summary_export_excel_job():
        payload = request.get_json(silent=True) or {}
        unit = (payload.get("unit") or request.args.get("unit") or "").strip().upper()
        from_date = (payload.get("from") or payload.get("from_date") or request.args.get("from") or "").strip()
        to_date = (payload.get("to") or payload.get("to_date") or request.args.get("to") or "").strip()
        allowed_units = _allowed_units_for_session()

        if not allowed_units:
            return jsonify({"status": "error", "message": "No unit access assigned"}), 403
        if not unit:
            if len(allowed_units) == 1:
                unit = allowed_units[0]
            else:
                return jsonify({"status": "error", "message": "Please select a unit"}), 400
        if unit not in allowed_units:
            return jsonify({"status": "error", "message": f"Unit {unit} not permitted"}), 403
        if not from_date or not to_date:
            return jsonify({"status": "error", "message": "Please select a valid date range"}), 400

        exported_by = session.get("username") or session.get("user") or "Unknown"
        job_id = token_hex(16)
        _excel_job_update(job_id, state="queued", filename=None)
        EXPORT_EXECUTOR.submit(_run_laboratory_summary_excel_job, job_id, unit, from_date, to_date, exported_by)
        return jsonify({"status": "queued", "job_id": job_id})

    @app.route('/api/mis/laboratory_summary/export_excel_job_status')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_mis_laboratory_summary_export_excel_job_status():
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

    @app.route('/api/mis/laboratory_summary/export_excel_job_result')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_mis_laboratory_summary_export_excel_job_result():
        job_id = (request.args.get("job_id") or "").strip()
        if not job_id:
            return "Missing job id", 400
        entry = _excel_job_get(job_id)
        if not entry:
            return "Job not found", 404
        if entry.get("state") != "done":
            return "Job not ready", 409
        data = _export_cache_get_bytes("lab_summary_xlsx_job", job_id)
        if not data:
            return "Export expired", 404
        filename = entry.get("filename") or "Laboratory_Summary.xlsx"
        return send_file(
            io.BytesIO(data),
            as_attachment=True,
            download_name=filename,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    def _build_laboratory_summary_pdf_buffer(unit: str, from_date: str, to_date: str, exported_by: str):
        from reportlab.lib.pagesizes import A4, landscape
        from reportlab.lib import colors
        from reportlab.lib.units import mm
        from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, PageBreak, Image
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        from reportlab.lib.enums import TA_CENTER, TA_RIGHT
        from xml.sax.saxutils import escape

        df_raw = data_fetch.fetch_laboratory_summary(unit, from_date, to_date)
        if df_raw is None or df_raw.empty:
            return None

        details_df = _build_laboratory_details_df(df_raw)
        doctor_summary, patient_summary, total_revenue, total_records, total_quantity = _build_laboratory_summary_frames(details_df)

        def _format_inr(number) -> str:
            try:
                n = float(number)
            except Exception:
                return "0.00"
            sign = "-" if n < 0 else ""
            n = abs(n)
            s = f"{n:.2f}"
            whole, frac = s.split(".")
            if len(whole) <= 3:
                grouped = whole
            else:
                last = whole[-3:]
                rest = whole[:-3]
                parts = []
                while rest:
                    parts.append(rest[-2:])
                    rest = rest[:-2]
                grouped = ",".join(reversed(parts)) + "," + last
            return f"{sign}{grouped}.{frac}"

        buffer = io.BytesIO()
        doc = SimpleDocTemplate(
            buffer,
            pagesize=landscape(A4),
            topMargin=14 * mm,
            bottomMargin=14 * mm,
            leftMargin=12 * mm,
            rightMargin=12 * mm,
        )

        styles = getSampleStyleSheet()
        style_title = ParagraphStyle("RptTitle", parent=styles["Heading2"], fontSize=12, alignment=TA_CENTER, textColor=colors.HexColor("#1e3a8a"))
        style_sub = ParagraphStyle("RptSub", parent=styles["Normal"], fontSize=9, alignment=TA_CENTER, textColor=colors.HexColor("#475569"))
        style_meta = ParagraphStyle("RptMeta", parent=styles["Normal"], fontSize=8, alignment=TA_CENTER, textColor=colors.HexColor("#475569"))
        style_th = ParagraphStyle("RptTh", parent=styles["Normal"], fontSize=8, textColor=colors.white, alignment=TA_CENTER)
        style_td = ParagraphStyle("RptTd", parent=styles["Normal"], fontSize=6.5, textColor=colors.black, leading=8)
        style_td_wrap = ParagraphStyle(
            "RptTdWrap",
            parent=styles["Normal"],
            fontSize=6.5,
            textColor=colors.black,
            leading=8,
            wordWrap="CJK",
        )
        style_td_right = ParagraphStyle("RptTdRight", parent=styles["Normal"], fontSize=6.5, textColor=colors.black, alignment=TA_RIGHT, leading=8)

        elements = []
        logo_path = os.path.join(app.root_path, "static", "logo", "asarfi.png")
        if os.path.exists(logo_path):
            logo = Image(logo_path, width=18 * mm, height=18 * mm, mask="auto")
            logo.hAlign = "CENTER"
            elements.append(logo)

        exported_at = datetime.now(tz=LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S")
        unit_header = _lab_unit_header_text(unit)
        elements.append(Spacer(1, 6))
        elements.append(Paragraph("Diagnostics & Laboratory MIS - Revenue Summary", style_title))
        elements.append(Paragraph(f"Unit: {unit_header}", style_sub))
        elements.append(Paragraph(f"Date Range: {from_date} to {to_date}", style_sub))
        elements.append(Paragraph(f"Exported By: {exported_by} | Exported At: {exported_at}", style_meta))
        elements.append(Spacer(1, 8))

        summary_rows = [
            [Paragraph("Metric", style_th), Paragraph("Value", style_th)],
            [Paragraph("Total Revenue (INR)", style_td), Paragraph(_format_inr(total_revenue), style_td_right)],
            [Paragraph("Total Records", style_td), Paragraph(f"{total_records:,}", style_td_right)],
            [Paragraph("Total Quantity", style_td), Paragraph(f"{total_quantity:,.0f}", style_td_right)],
        ]
        summary_table = Table(summary_rows, colWidths=[70 * mm, 40 * mm])
        summary_table.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1e3a8a")),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
            ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#1f2937")),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ]))
        elements.append(summary_table)
        elements.append(Spacer(1, 10))

        def _summary_table(title: str, frame: pd.DataFrame):
            rows = [[Paragraph(title, style_th), Paragraph("Records", style_th), Paragraph("Revenue (INR)", style_th)]]
            for _, row in frame.iterrows():
                rows.append([
                    Paragraph(escape(str(row["Name"] or "")), style_td),
                    Paragraph(str(int(row["Records"])), style_td_right),
                    Paragraph(_format_inr(row["Revenue"]), style_td_right),
                ])
            tbl = Table(rows, colWidths=[90 * mm, 25 * mm, 35 * mm], repeatRows=1)
            tbl.setStyle(TableStyle([
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1e3a8a")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#1f2937")),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ]))
            return tbl

        elements.append(_summary_table("Doctor-wise Revenue", doctor_summary))
        elements.append(Spacer(1, 8))
        elements.append(_summary_table("Patient Type-wise Revenue", patient_summary))
        elements.append(PageBreak())

        detail_headers = [
            Paragraph("Bill Date", style_th),
            Paragraph("Reg No", style_th),
            Paragraph("Patient", style_th),
            Paragraph("Patient Type", style_th),
            Paragraph("Visit Type", style_th),
            Paragraph("Doctor", style_th),
            Paragraph("Sub Dept", style_th),
            Paragraph("Service", style_th),
            Paragraph("Qty", style_th),
            Paragraph("Amount (INR)", style_th),
        ]
        detail_rows = [detail_headers]
        for _, row in details_df.iterrows():
            bill_date = str(row.get("BillDate") or "")
            reg_no = str(row.get("Registration_No") or "")
            patient = str(row.get("Patient") or "")
            patient_type = str(row.get("PatientType") or "")
            visit_type = str(row.get("TypeOfVisit") or "")
            doctor = str(row.get("Doctor") or "")
            sub_dept = str(row.get("SubDepartment_Name") or "")
            service = str(row.get("Service_Name") or "")
            qty_val = f"{float(row.get('Quantity') or 0):,.0f}"
            amt_val = _format_inr(row.get("Amount") or 0)
            detail_rows.append([
                Paragraph(escape(bill_date), style_td),
                Paragraph(escape(reg_no), style_td),
                Paragraph(escape(patient), style_td_wrap),
                Paragraph(escape(patient_type), style_td_wrap),
                Paragraph(escape(visit_type), style_td),
                Paragraph(escape(doctor), style_td_wrap),
                Paragraph(escape(sub_dept), style_td_wrap),
                Paragraph(escape(service), style_td_wrap),
                Paragraph(escape(qty_val), style_td_right),
                Paragraph(escape(amt_val), style_td_right),
            ])

        detail_tbl = Table(
            detail_rows,
            colWidths=[18 * mm, 20 * mm, 32 * mm, 24 * mm, 16 * mm, 30 * mm, 22 * mm, 40 * mm, 12 * mm, 20 * mm],
            repeatRows=1,
        )
        detail_tbl.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1e3a8a")),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, -1), 6.5),
            ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#1f2937")),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("ALIGN", (-2, 1), (-1, -1), "RIGHT"),
        ]))
        elements.append(detail_tbl)

        def _draw_page_number(canvas, doc_obj):
            canvas.saveState()
            width, _ = doc.pagesize
            canvas.setFont("Helvetica", 8)
            canvas.setFillColor(colors.HexColor("#64748b"))
            canvas.drawRightString(width - 12 * mm, 10 * mm, f"Page {doc_obj.page}")
            canvas.drawString(12 * mm, 10 * mm, "Copyright (c) ASARFI HOSPITAL")
            canvas.restoreState()

        doc.build(elements, onFirstPage=_draw_page_number, onLaterPages=_draw_page_number)
        buffer.seek(0)
        return buffer

    @app.route('/api/mis/laboratory_summary/export_pdf')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_mis_laboratory_summary_export_pdf():
        unit = (request.args.get("unit") or "").strip().upper()
        from_date = (request.args.get("from") or "").strip()
        to_date = (request.args.get("to") or "").strip()
        allowed_units = _allowed_units_for_session()

        if not allowed_units:
            return "No unit access assigned", 403
        if not unit:
            if len(allowed_units) == 1:
                unit = allowed_units[0]
            else:
                return "Please select a unit", 400
        if unit not in allowed_units:
            return f"Unit {unit} not permitted", 403
        if not from_date or not to_date:
            return "Please select a valid date range", 400

        exported_by = session.get("username") or session.get("user") or "Unknown"
        buffer = _build_laboratory_summary_pdf_buffer(unit, from_date, to_date, exported_by)
        if buffer is None:
            return "No data available to export", 404
        filename = f"Laboratory_Summary_{unit}_{from_date}_to_{to_date}.pdf"
        return send_file(buffer, mimetype="application/pdf", as_attachment=True, download_name=filename)
