from datetime import datetime, timedelta
import io
import re

from flask import jsonify, render_template, request, send_file, session


def register_mod_reports_dashboard_routes(
    app,
    *,
    login_required,
    analytics_allowed_units_for_session,
    allowed_units_for_session,
    mod_summary_default_limit,
    mod_summary_pick_lookback_days,
    fetch_mod_summary_daily_rows,
    aggregate_mod_summary_rows,
    fetch_mod_summary_department_insights,
    is_truthy,
    resolve_mod_export_date_range,
    fetch_ahl_sequence_export_extras,
    mod_seq_export_key,
    build_mod_sequence_audit_excel,
    fetch_night_snapshot_audit_rows,
    fetch_morning_snapshot_audit_rows,
    local_tz,
    morning_report_investigation_map,
    morning_report_key_procedures,
):
    """Register MOD dashboard and shared health/summary routes."""
    _analytics_allowed_units_for_session = analytics_allowed_units_for_session
    _allowed_units_for_session = allowed_units_for_session
    _mod_summary_default_limit = mod_summary_default_limit
    _mod_summary_pick_lookback_days = mod_summary_pick_lookback_days
    _fetch_mod_summary_daily_rows = fetch_mod_summary_daily_rows
    _aggregate_mod_summary_rows = aggregate_mod_summary_rows
    _fetch_mod_summary_department_insights = fetch_mod_summary_department_insights
    _is_truthy = is_truthy
    _resolve_mod_export_date_range = resolve_mod_export_date_range
    _fetch_ahl_sequence_export_extras = fetch_ahl_sequence_export_extras
    _mod_seq_export_key = mod_seq_export_key
    _build_mod_sequence_audit_excel = build_mod_sequence_audit_excel
    _fetch_night_snapshot_audit_rows = fetch_night_snapshot_audit_rows
    _fetch_morning_snapshot_audit_rows = fetch_morning_snapshot_audit_rows
    LOCAL_TZ = local_tz
    MORNING_REPORT_INVESTIGATION_MAP = morning_report_investigation_map
    MORNING_REPORT_KEY_PROCEDURES = morning_report_key_procedures

    @app.route('/mod_reports')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head", "Executive"})
    def mod_reports_dashboard():
        allowed_units = _analytics_allowed_units_for_session() or _allowed_units_for_session()
        if not allowed_units:
            selected_unit = "ALL"
        elif len(allowed_units) == 1:
            selected_unit = allowed_units[0]
        else:
            selected_unit = "ALL"
        return render_template(
            'mod_reports.html',
            allowed_units=allowed_units,
            selected_unit=selected_unit,
        )

    @app.route('/api/mod_reports/summary')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head", "Executive"})
    def api_mod_reports_summary():
        duration = (request.args.get("duration") or "daily").strip().lower()
        if duration not in {"daily", "monthly", "yearly"}:
            duration = "daily"
        requested_segment = (request.args.get("segment") or "total").strip().lower()
        if requested_segment not in {"total", "asarfi", "cardiac"}:
            requested_segment = "total"

        try:
            limit = int(request.args.get("limit") or _mod_summary_default_limit(duration))
        except Exception:
            limit = _mod_summary_default_limit(duration)
        limit = max(3, min(limit, 120))

        try:
            lookback_days = int(request.args.get("lookback_days") or _mod_summary_pick_lookback_days(duration))
        except Exception:
            lookback_days = _mod_summary_pick_lookback_days(duration)
        lookback_days = max(30, min(lookback_days, 3650))

        requested_unit = (request.args.get("unit") or "").strip().upper()
        allowed_units = _analytics_allowed_units_for_session() or _allowed_units_for_session()
        if not allowed_units:
            return jsonify({"status": "error", "message": "No unit access"}), 403

        if requested_unit and requested_unit != "ALL" and requested_unit not in allowed_units:
            return jsonify({"status": "error", "message": "Unit not allowed"}), 403
        if not requested_unit:
            requested_unit = allowed_units[0] if len(allowed_units) == 1 else "ALL"
        if requested_unit != "AHL":
            requested_segment = "total"

        units_to_use = allowed_units if requested_unit == "ALL" else [requested_unit]
        today = datetime.now(tz=LOCAL_TZ).date()
        include_today = _is_truthy(request.args.get("include_today"))
        closed_to = today if include_today else (today - timedelta(days=1))
        from_dt = today - timedelta(days=lookback_days)
        if closed_to < from_dt:
            from_dt = closed_to
        from_date = from_dt.strftime("%Y-%m-%d")
        to_date = closed_to.strftime("%Y-%m-%d")

        try:
            daily_rows = _fetch_mod_summary_daily_rows(units_to_use, from_date, to_date, segment=requested_segment)
            rows = _aggregate_mod_summary_rows(daily_rows, duration)
            dept_insights = _fetch_mod_summary_department_insights(units_to_use, from_date, to_date, duration)
        except Exception as e:
            return jsonify({"status": "error", "message": f"Failed to build MOD summary: {e}"}), 500

        rows = rows[:limit]

        def _avg(field: str) -> float:
            vals = [float(r[field]) for r in rows if r.get(field) is not None]
            if not vals:
                return 0.0
            return round(sum(vals) / len(vals), 2)

        def _trend(field: str) -> float | None:
            if len(rows) < 2:
                return None
            a = rows[0].get(field)
            b = rows[1].get(field)
            if a is None or b is None:
                return None
            return round(float(a) - float(b), 2)

        kpis = {
            "avg_occupancy_pct": _avg("avg_occupancy_pct"),
            "avg_opd": _avg("avg_opd"),
            "avg_ipd": _avg("avg_ipd"),
            "avg_night_admissions": _avg("avg_night_admissions"),
            "avg_night_discharges": _avg("avg_night_discharges"),
            "avg_investigations": _avg("avg_investigations"),
            "avg_procedures": _avg("avg_procedures"),
            "avg_coverage_pct": _avg("coverage_pct"),
            "total_reports_saved": int(sum(int(r.get("reports_saved") or 0) for r in rows)),
            "periods": len(rows),
        }
        trends = {
            "occupancy_pct": _trend("avg_occupancy_pct"),
            "opd": _trend("avg_opd"),
            "ipd": _trend("avg_ipd"),
            "night_admissions": _trend("avg_night_admissions"),
            "night_discharges": _trend("avg_night_discharges"),
            "coverage_pct": _trend("coverage_pct"),
        }

        for row in rows:
            row.pop("_updated_dt", None)

        segment_limited = bool(requested_unit == "AHL" and requested_segment in {"asarfi", "cardiac"})
        if segment_limited:
            dept_insights["note"] = "Department insights use combined AHL doctorwise snapshots; split-wise department data is not persisted."

        return jsonify(
            {
                "status": "success",
                "duration": duration,
                "unit": requested_unit,
                "segment": requested_segment,
                "units": units_to_use,
                "from": from_date,
                "to": to_date,
                "closed_only": not include_today,
                "rows": rows,
                "kpis": kpis,
                "trends": trends,
                "dept_insights": dept_insights,
                "available_units": allowed_units,
                "updated_at": datetime.now(tz=LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S"),
            }
        )

    @app.route('/api/mod_reports/sequence_audit/export_excel')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head", "Executive"})
    def api_mod_reports_sequence_audit_export_excel():
        duration_raw = request.args.get("range") or request.args.get("duration") or "custom"
        from_raw = request.args.get("from") or request.args.get("from_date")
        to_raw = request.args.get("to") or request.args.get("to_date")
        duration, from_dt, to_dt = _resolve_mod_export_date_range(duration_raw, from_raw, to_raw)

        if not from_dt or not to_dt:
            return jsonify({"status": "error", "message": "Valid from/to date is required (YYYY-MM-DD)."}), 400
        if to_dt < from_dt:
            return jsonify({"status": "error", "message": "To date cannot be earlier than from date."}), 400
        if (to_dt - from_dt).days > 3660:
            return jsonify({"status": "error", "message": "Date range too large. Keep it within 10 years."}), 400

        now_local = datetime.now(tz=LOCAL_TZ)
        today = now_local.date()
        if to_dt > today:
            return jsonify({"status": "error", "message": "Future dates are not allowed."}), 400

        allowed_units = _analytics_allowed_units_for_session() or _allowed_units_for_session()
        if not allowed_units:
            return jsonify({"status": "error", "message": "No unit access"}), 403

        requested_unit = (request.args.get("unit") or "").strip().upper()
        if requested_unit and requested_unit != "ALL" and requested_unit not in allowed_units:
            return jsonify({"status": "error", "message": "Unit not allowed"}), 403
        if not requested_unit:
            requested_unit = allowed_units[0] if len(allowed_units) == 1 else "ALL"
        units_to_use = allowed_units if requested_unit == "ALL" else [requested_unit]

        requested_segment = (request.args.get("segment") or "total").strip().lower()
        if requested_segment not in {"total", "asarfi", "cardiac"}:
            requested_segment = "total"
        if requested_unit != "AHL":
            requested_segment = "total"

        from_date = from_dt.strftime("%Y-%m-%d")
        to_date = to_dt.strftime("%Y-%m-%d")

        try:
            daily_rows = _fetch_mod_summary_daily_rows(units_to_use, from_date, to_date, segment=requested_segment)
        except Exception as e:
            return jsonify({"status": "error", "message": f"Failed to build MOD audit export data: {e}"}), 500

        rows_for_export = []
        for row in (daily_rows or []):
            period_key = str(row.get("period_key") or "").strip()
            try:
                period_dt = datetime.strptime(period_key, "%Y-%m-%d").date()
            except Exception:
                continue
            lock_ready_at = datetime.combine(period_dt, datetime.min.time(), tzinfo=LOCAL_TZ) + timedelta(hours=14)
            if now_local < lock_ready_at:
                continue
            expected_reports = max(1, int(row.get("expected_reports") or 0))
            reports_saved = int(row.get("reports_saved") or 0)
            out = dict(row)
            if reports_saved >= expected_reports:
                out["status_label"] = "LOCKED"
            elif reports_saved > 0:
                out["status_label"] = "PARTIAL"
            else:
                out["status_label"] = "MISSING"
            rows_for_export.append(out)

        locked_only = _is_truthy(request.args.get("locked_only", "1"))
        if locked_only:
            rows_for_export = [
                r for r in rows_for_export
                if int(r.get("reports_saved") or 0) >= int(r.get("expected_reports") or 0)
            ]

        rows_for_export.sort(key=lambda r: str(r.get("period_key") or ""), reverse=True)
        if not rows_for_export:
            return jsonify(
                {
                    "status": "error",
                    "message": "No locked MOD sequence rows found for this range/unit.",
                    "from": from_date,
                    "to": to_date,
                }
            ), 404

        include_ahl_details = requested_unit == "AHL"
        if include_ahl_details:
            extras_map = _fetch_ahl_sequence_export_extras(from_date, to_date)
            ahl_inv_labels = [str(label).strip() for label, _ in MORNING_REPORT_INVESTIGATION_MAP if str(label).strip()]
            ahl_proc_labels = [str(label).strip() for label in MORNING_REPORT_KEY_PROCEDURES if str(label).strip()]
            for row in rows_for_export:
                date_key = str(row.get("period_key") or row.get("period_label") or "").strip()
                ext = extras_map.get(date_key) or {}
                row["ahl_night_cash_occupancy"] = int(ext.get("night_cash_occupancy") or 0)
                row["ahl_night_cashless_occupancy"] = int(ext.get("night_cashless_occupancy") or 0)
                row["ahl_night_asarfi_admissions"] = int(ext.get("night_asarfi_admissions") or 0)
                row["ahl_night_asarfi_discharges"] = int(ext.get("night_asarfi_discharges") or 0)
                row["ahl_night_cardiac_admissions"] = int(ext.get("night_cardiac_admissions") or 0)
                row["ahl_night_cardiac_discharges"] = int(ext.get("night_cardiac_discharges") or 0)
                row["ahl_night_0000_0800_asarfi_admissions"] = int(ext.get("night_0000_0800_asarfi_admissions") or 0)
                row["ahl_night_0000_0800_cardiac_admissions"] = int(ext.get("night_0000_0800_cardiac_admissions") or 0)
                row["ahl_night_0000_0800_total_admissions"] = int(ext.get("night_0000_0800_total_admissions") or 0)
                row["ahl_night_ward_wise_patient_count"] = str(ext.get("ward_wise_patient_count") or "-")
                row["ahl_morning_key_procedures"] = int(ext.get("morning_key_procedures") or 0)
                for label in ahl_inv_labels:
                    key = _mod_seq_export_key(label)
                    row[f"ahl_inv_{key}_asarfi"] = int(ext.get(f"inv_{key}_asarfi") or 0)
                    row[f"ahl_inv_{key}_cardiac"] = int(ext.get(f"inv_{key}_cardiac") or 0)
                    row[f"ahl_inv_{key}_total"] = int(ext.get(f"inv_{key}_total") or 0)
                for label in ahl_proc_labels:
                    key = _mod_seq_export_key(label)
                    row[f"ahl_proc_{key}"] = int(ext.get(f"proc_{key}") or 0)

        exported_by = session.get("username") or session.get("user") or "Unknown"
        unit_label = requested_unit if requested_unit != "ALL" else "ALL Units"
        if requested_unit == "AHL" and requested_segment in {"asarfi", "cardiac"}:
            unit_label = f"AHL ({requested_segment.title()})"

        data = _build_mod_sequence_audit_excel(
            rows_for_export,
            from_date=from_date,
            to_date=to_date,
            unit_label=unit_label,
            duration=duration,
            segment=requested_segment,
            locked_only=locked_only,
            exported_by=exported_by,
            include_ahl_details=include_ahl_details,
        )

        unit_token = re.sub(r"[^A-Za-z0-9_]+", "_", unit_label.replace(" ", "_")).strip("_") or "ALL"
        filename = f"MOD_Sequence_Audit_{unit_token}_{from_date}_to_{to_date}.xlsx"
        return send_file(
            io.BytesIO(data),
            as_attachment=True,
            download_name=filename,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    @app.route('/mod_reports/night_snapshot_health')
    @login_required(allowed_roles={"IT", "Management"})
    def night_snapshot_health_page():
        return render_template('night_snapshot_health.html')

    @app.route('/api/mod_reports/night_snapshot_health')
    @login_required(allowed_roles={"IT", "Management"})
    def api_night_snapshot_health():
        limit_raw = request.args.get("limit") or "120"
        try:
            limit = int(limit_raw)
        except Exception:
            limit = 120
        rows = _fetch_night_snapshot_audit_rows(limit=limit)
        return jsonify({"status": "success", "rows": rows})

    @app.route('/mod_reports/morning_snapshot_health')
    @login_required(allowed_roles={"IT", "Management"})
    def morning_snapshot_health_page():
        return render_template('morning_snapshot_health.html')

    @app.route('/api/mod_reports/morning_snapshot_health')
    @login_required(allowed_roles={"IT", "Management"})
    def api_morning_snapshot_health():
        limit_raw = request.args.get("limit") or "120"
        try:
            limit = int(limit_raw)
        except Exception:
            limit = 120
        rows = _fetch_morning_snapshot_audit_rows(limit=limit)
        return jsonify({"status": "success", "rows": rows})
