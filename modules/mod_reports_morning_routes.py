from datetime import date, datetime
import io
import os
import time

from flask import jsonify, render_template, request, send_file, session


def register_mod_reports_morning_routes(
    app,
    *,
    login_required,
    analytics_allowed_units_for_session,
    allowed_units_for_session,
    resolve_morning_unit,
    get_login_db_connection,
    ensure_morning_report_tables,
    fetch_morning_report_snapshot,
    apply_morning_staff_non_med_prefill,
    ensure_morning_death_summary_row,
    build_morning_report_payload,
    build_morning_report_excel,
    build_morning_report_pdf,
    build_morning_report_jpg,
    save_morning_report_snapshot_to_aci,
    build_morning_report_diff,
    mod_report_edit_lock_info,
    attach_mod_report_lock,
    mod_report_cache_get,
    mod_report_cache_set,
    mod_report_cache_clear,
    rows_have_any_value,
    audit_log_event,
    local_tz,
):
    """Register MOD morning-report routes."""
    _analytics_allowed_units_for_session = analytics_allowed_units_for_session
    _allowed_units_for_session = allowed_units_for_session
    _resolve_morning_unit = resolve_morning_unit
    _get_login_db_connection = get_login_db_connection
    _ensure_morning_report_tables = ensure_morning_report_tables
    _fetch_morning_report_snapshot = fetch_morning_report_snapshot
    _apply_morning_staff_non_med_prefill = apply_morning_staff_non_med_prefill
    _ensure_morning_death_summary_row = ensure_morning_death_summary_row
    _build_morning_report_payload = build_morning_report_payload
    _build_morning_report_excel = build_morning_report_excel
    _build_morning_report_pdf = build_morning_report_pdf
    _build_morning_report_jpg = build_morning_report_jpg
    _save_morning_report_snapshot_to_aci = save_morning_report_snapshot_to_aci
    _build_morning_report_diff = build_morning_report_diff
    _mod_report_edit_lock_info = mod_report_edit_lock_info
    _attach_mod_report_lock = attach_mod_report_lock
    _mod_report_cache_get = mod_report_cache_get
    _mod_report_cache_set = mod_report_cache_set
    _mod_report_cache_clear = mod_report_cache_clear
    _rows_have_any_value = rows_have_any_value
    _audit_log_event = audit_log_event
    LOCAL_TZ = local_tz

    @app.route('/mod_reports/morning_report')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head", "Executive"})
    def morning_report_page():
        today = datetime.now(tz=LOCAL_TZ).strftime("%Y-%m-%d")
        mod_name = session.get("username") or session.get("user") or ""
        allowed_units = _analytics_allowed_units_for_session()
        if not allowed_units:
            allowed_units = _allowed_units_for_session()
        selected_unit = (request.args.get("unit") or "").strip().upper()
        if not selected_unit and allowed_units:
            selected_unit = allowed_units[0]
        return render_template(
            'morning_report.html',
            today=today,
            mod_name=mod_name,
            allowed_units=allowed_units,
            selected_unit=selected_unit,
        )

    @app.route('/api/mod_reports/morning_report/snapshot_dates')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head", "Executive"})
    def api_morning_report_snapshot_dates():
        unit = (request.args.get("unit") or "").strip().upper()
        allowed_units = _analytics_allowed_units_for_session() or _allowed_units_for_session()
        unit_norm = _resolve_morning_unit(allowed_units, unit)
        if not unit_norm:
            return jsonify({"status": "error", "message": "No unit access"}), 403

        try:
            with _get_login_db_connection() as conn:
                _ensure_morning_report_tables(conn)
                cur = conn.cursor()
                cur.execute(
                    """
                    SELECT TOP 120 SnapshotDate
                    FROM dbo.HID_Morning_Report_Header WITH (NOLOCK)
                    WHERE Unit = ?
                    ORDER BY SnapshotDate DESC
                    """,
                    (unit_norm,),
                )
                rows = cur.fetchall()
        except Exception as e:
            return jsonify({"status": "error", "message": f"Failed to load snapshot dates: {e}"}), 500

        dates = []
        for row in rows:
            snap_date = row[0] if row else None
            if snap_date is None:
                continue
            if isinstance(snap_date, (datetime, date)):
                dates.append(snap_date.strftime("%Y-%m-%d"))
            else:
                raw = str(snap_date).strip()
                if len(raw) >= 10 and raw[4] == "-" and raw[7] == "-":
                    raw = raw[:10]
                dates.append(raw)

        return jsonify({"status": "success", "dates": dates})

    @app.route('/api/mod_reports/morning_report')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head", "Executive"})
    def api_morning_report_data():
        now = datetime.now(tz=LOCAL_TZ)
        report_date_str = (request.args.get("date") or "").strip()
        mode = (request.args.get("mode") or "snapshot").strip().lower()
        unit_override = (request.args.get("unit") or "").strip()
        if not report_date_str:
            report_date = now.date()
            report_date_str = report_date.strftime("%Y-%m-%d")
        else:
            try:
                report_date = datetime.strptime(report_date_str, "%Y-%m-%d").date()
            except Exception:
                report_date = now.date()
                report_date_str = report_date.strftime("%Y-%m-%d")

        _t0 = time.perf_counter()

        def _log(stage: str):
            elapsed = (time.perf_counter() - _t0) * 1000
            print(f"[timing] morning_report {unit_override or '-'} {report_date_str} {mode} {stage}: {elapsed:.1f}ms")

        allowed_units = _analytics_allowed_units_for_session() or _allowed_units_for_session()
        _log("allowed_units")
        base_unit = _resolve_morning_unit(allowed_units, unit_override)
        if not base_unit:
            return jsonify({"status": "error", "message": "No unit access"}), 403

        role = str(session.get("role") or "").strip().lower()
        is_it = role == "it"

        cached_payload = _mod_report_cache_get("morning", base_unit, report_date_str, mode)
        _log("cache_check")
        if cached_payload:
            hydrated_payload = _apply_morning_staff_non_med_prefill(cached_payload, report_date_str, base_unit)
            if hydrated_payload:
                hydrated_payload = dict(hydrated_payload)
                hydrated_payload["summary_rows"] = _ensure_morning_death_summary_row(
                    base_unit,
                    report_date_str,
                    hydrated_payload.get("summary_rows") or [],
                )
            if hydrated_payload != cached_payload:
                _mod_report_cache_set("morning", base_unit, report_date_str, mode, hydrated_payload)
                cached_payload = hydrated_payload
                _log("cache_prefill_refresh")
            _log("cache_hit")
            return jsonify({"status": "success", "data": _attach_mod_report_lock(cached_payload, base_unit, report_date_str, is_it)})

        if mode == "snapshot":
            snapshot_payload = _fetch_morning_report_snapshot(report_date_str, base_unit)
            _log("fetch_snapshot")
            if snapshot_payload:
                needs_live = (
                    not _rows_have_any_value(snapshot_payload.get("doctorwise_rows"), ("department", "opd", "ipd"))
                    or not _rows_have_any_value(snapshot_payload.get("occupancy_rows"), ("ward", "patient_count", "occupied_count", "clinically_discharged_count"))
                    or not _rows_have_any_value(snapshot_payload.get("doctor_night_visits"), ("patient_name", "admission_time", "consultant", "visiting"))
                )
                if needs_live:
                    mod_name = session.get("username") or session.get("user") or ""
                    live_payload = _mod_report_cache_get("morning", base_unit, report_date_str, "live")
                    _log("live_cache_check")
                    if not live_payload:
                        live_payload = _build_morning_report_payload(
                            base_unit,
                            report_date,
                            source="live",
                            mod_name=mod_name,
                            report_time=now.strftime("%I:%M %p").lstrip("0") or now.strftime("%I:%M %p"),
                            snapshot_time=None,
                        )
                        _log("build_live_for_snapshot")
                        if live_payload:
                            _mod_report_cache_set("morning", base_unit, report_date_str, "live", live_payload)
                            _log("cache_set_live_for_snapshot")
                    if live_payload:
                        if not _rows_have_any_value(snapshot_payload.get("doctorwise_rows"), ("department", "opd", "ipd")):
                            snapshot_payload["doctorwise_rows"] = live_payload.get("doctorwise_rows") or []
                        if not _rows_have_any_value(snapshot_payload.get("occupancy_rows"), ("ward", "patient_count", "occupied_count", "clinically_discharged_count")):
                            snapshot_payload["occupancy_rows"] = live_payload.get("occupancy_rows") or []
                        if not _rows_have_any_value(snapshot_payload.get("doctor_night_visits"), ("patient_name", "admission_time", "consultant", "visiting")):
                            snapshot_payload["doctor_night_visits"] = live_payload.get("doctor_night_visits") or []
                    _log("live_merge_into_snapshot")
                snapshot_payload = _apply_morning_staff_non_med_prefill(snapshot_payload, report_date_str, base_unit)
                _mod_report_cache_set("morning", base_unit, report_date_str, mode, snapshot_payload)
                _log("cache_set_snapshot")
                return jsonify({"status": "success", "data": _attach_mod_report_lock(snapshot_payload, base_unit, report_date_str, is_it)})

        mod_name = session.get("username") or session.get("user") or ""
        live_payload = _build_morning_report_payload(
            base_unit,
            report_date,
            source="live",
            mod_name=mod_name,
            report_time=now.strftime("%I:%M %p").lstrip("0") or now.strftime("%I:%M %p"),
            snapshot_time=None,
        )
        _log("build_live")
        if not live_payload:
            return jsonify({"status": "error", "message": "Failed to load data"}), 500
        live_payload = _apply_morning_staff_non_med_prefill(live_payload, report_date_str, base_unit)
        _mod_report_cache_set("morning", base_unit, report_date_str, mode, live_payload)
        _log("cache_set_live")
        return jsonify({"status": "success", "data": _attach_mod_report_lock(live_payload, base_unit, report_date_str, is_it)})

    @app.route('/api/mod_reports/morning_report/save', methods=['POST'])
    @login_required(allowed_roles={"IT", "Management", "Departmental Head", "Executive"})
    def api_morning_report_save():
        payload = request.get_json(silent=True) or {}
        report_date = (payload.get("report_date") or "").strip()
        unit = (payload.get("unit") or "").strip().upper()
        if not report_date:
            report_date = datetime.now(tz=LOCAL_TZ).strftime("%Y-%m-%d")

        allowed_units = _analytics_allowed_units_for_session() or _allowed_units_for_session()
        if allowed_units and unit and unit not in allowed_units:
            _audit_log_event(
                "mod_reports",
                "morning_save",
                status="error",
                entity_type="morning_report",
                unit=unit,
                summary="Unit not allowed",
                details={"report_date": report_date},
            )
            return jsonify({"status": "error", "message": "Unit not allowed"}), 403
        if not unit:
            unit = _resolve_morning_unit(allowed_units, None)
        if not unit:
            _audit_log_event(
                "mod_reports",
                "morning_save",
                status="error",
                entity_type="morning_report",
                summary="Unit is required",
                details={"report_date": report_date},
            )
            return jsonify({"status": "error", "message": "Unit is required"}), 400
        entity_id = f"{unit}:{report_date}"

        role = str(session.get("role") or "").strip().lower()
        if role != "it":
            lock_info = _mod_report_edit_lock_info(unit, report_date)
            if lock_info.get("locked"):
                lock_at = lock_info.get("lock_at") or ""
                msg = f"Edits are locked for {report_date} (locked at {lock_at}). Contact IT."
                _audit_log_event(
                    "mod_reports",
                    "morning_save",
                    status="error",
                    entity_type="morning_report",
                    entity_id=entity_id,
                    unit=unit,
                    summary="Edits are locked",
                    details={"lock_at": lock_at},
                )
                return jsonify({"status": "error", "message": msg}), 403

        clean_payload = {
            "unit": unit,
            "report_date": report_date,
            "report_time": payload.get("report_time") or "",
            "mod_name": payload.get("mod_name") or (session.get("username") or session.get("user") or ""),
            "snapshot_time": datetime.now(tz=LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S"),
            "summary_rows": _ensure_morning_death_summary_row(unit, report_date, payload.get("summary_rows") or []),
            "investigations": payload.get("investigations") or [],
            "key_procedures": payload.get("key_procedures") or [],
            "admissions": payload.get("admissions") or [],
            "doctorwise_rows": payload.get("doctorwise_rows") or [],
            "occupancy_rows": payload.get("occupancy_rows") or [],
            "doctor_night_visits": payload.get("doctor_night_visits") or [],
            "staff_rows": payload.get("staff_rows") or [],
            "non_medical_rows": payload.get("non_medical_rows") or [],
            "referral_rows": payload.get("referral_rows") or [],
        }

        old_payload = None
        try:
            old_payload = _fetch_morning_report_snapshot(report_date, unit)
        except Exception:
            old_payload = None

        try:
            _save_morning_report_snapshot_to_aci(clean_payload)
            _mod_report_cache_clear("morning", unit, report_date)
            diff = _build_morning_report_diff(old_payload, clean_payload)
            _audit_log_event(
                "mod_reports",
                "morning_save",
                status="success",
                entity_type="morning_report",
                entity_id=entity_id,
                unit=unit,
                summary="Morning report saved",
                details={
                    "report_time": clean_payload.get("report_time"),
                    "mod_name": clean_payload.get("mod_name"),
                    "diff": diff,
                },
            )
            return jsonify({"status": "success"})
        except Exception as e:
            _audit_log_event(
                "mod_reports",
                "morning_save",
                status="error",
                entity_type="morning_report",
                entity_id=entity_id,
                unit=unit,
                summary="Failed to save morning report",
                details={"error": str(e), "report_time": clean_payload.get("report_time"), "mod_name": clean_payload.get("mod_name")},
            )
            return jsonify({"status": "error", "message": str(e)}), 500

    @app.route('/api/mod_reports/morning_report/export', methods=['GET', 'POST'])
    @login_required(allowed_roles={"IT", "Management", "Departmental Head", "Executive"})
    def api_morning_report_export():
        if request.method == 'POST':
            payload = request.get_json(silent=True) or {}
            exported_by = session.get("username") or session.get("user") or "Unknown"
            data = _build_morning_report_excel(payload, exported_by=exported_by)
            report_date = payload.get("report_date") or datetime.now(tz=LOCAL_TZ).strftime("%Y-%m-%d")
            unit = (payload.get("unit") or "UNIT").strip().upper()
            return send_file(
                io.BytesIO(data),
                as_attachment=True,
                download_name=f"Morning_Report_{unit}_{report_date}.xlsx",
                mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

        report_date = (request.args.get("date") or "").strip()
        mode = (request.args.get("mode") or "snapshot").strip().lower()
        unit = (request.args.get("unit") or "").strip().upper()
        if not report_date:
            report_date = datetime.now(tz=LOCAL_TZ).strftime("%Y-%m-%d")

        allowed_units = _analytics_allowed_units_for_session() or _allowed_units_for_session()
        if allowed_units and unit and unit not in allowed_units:
            return jsonify({"status": "error", "message": "Unit not allowed"}), 403
        if not unit:
            unit = _resolve_morning_unit(allowed_units, None)
        if not unit:
            return jsonify({"status": "error", "message": "Unit is required"}), 400

        payload = None
        if mode == "snapshot":
            export_dir = os.path.join("data", "exports", "morning_report")
            file_name = f"Morning_Report_{unit}_{report_date}.xlsx"
            file_path = os.path.join(export_dir, file_name)
            if os.path.exists(file_path):
                return send_file(
                    file_path,
                    as_attachment=True,
                    download_name=file_name,
                    mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
            payload = _fetch_morning_report_snapshot(report_date, unit)
        if not payload:
            payload = _build_morning_report_payload(unit, report_date, source="live", mod_name=(session.get("username") or session.get("user") or ""))
        if not payload:
            return jsonify({"status": "error", "message": "Failed to build report"}), 500

        exported_by = session.get("username") or session.get("user") or "Unknown"
        data = _build_morning_report_excel(payload, exported_by=exported_by)
        return send_file(
            io.BytesIO(data),
            as_attachment=True,
            download_name=f"Morning_Report_{unit}_{report_date}.xlsx",
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    @app.route('/api/mod_reports/morning_report/export_pdf', methods=['GET', 'POST'])
    @login_required(allowed_roles={"IT", "Management", "Departmental Head", "Executive"})
    def api_morning_report_export_pdf():
        if request.method == 'POST':
            payload = request.get_json(silent=True) or {}
            exported_by = session.get("username") or session.get("user") or "Unknown"
            data = _build_morning_report_pdf(payload, exported_by=exported_by)
            report_date = payload.get("report_date") or datetime.now(tz=LOCAL_TZ).strftime("%Y-%m-%d")
            unit = (payload.get("unit") or "UNIT").strip().upper()
            return send_file(
                io.BytesIO(data),
                as_attachment=True,
                download_name=f"Morning_Report_{unit}_{report_date}.pdf",
                mimetype="application/pdf",
            )

        report_date = (request.args.get("date") or "").strip()
        mode = (request.args.get("mode") or "snapshot").strip().lower()
        unit = (request.args.get("unit") or "").strip().upper()
        if not report_date:
            report_date = datetime.now(tz=LOCAL_TZ).strftime("%Y-%m-%d")

        allowed_units = _analytics_allowed_units_for_session() or _allowed_units_for_session()
        if allowed_units and unit and unit not in allowed_units:
            return jsonify({"status": "error", "message": "Unit not allowed"}), 403
        if not unit:
            unit = _resolve_morning_unit(allowed_units, None)
        if not unit:
            return jsonify({"status": "error", "message": "Unit is required"}), 400

        payload = None
        if mode == "snapshot":
            payload = _fetch_morning_report_snapshot(report_date, unit)
        if not payload:
            payload = _build_morning_report_payload(unit, report_date, source="live", mod_name=(session.get("username") or session.get("user") or ""))
        if not payload:
            return jsonify({"status": "error", "message": "Failed to build report"}), 500

        exported_by = session.get("username") or session.get("user") or "Unknown"
        data = _build_morning_report_pdf(payload, exported_by=exported_by)
        return send_file(
            io.BytesIO(data),
            as_attachment=True,
            download_name=f"Morning_Report_{unit}_{report_date}.pdf",
            mimetype="application/pdf",
        )

    @app.route('/api/mod_reports/morning_report/export_jpg', methods=['GET', 'POST'])
    @login_required(allowed_roles={"IT", "Management", "Departmental Head", "Executive"})
    def api_morning_report_export_jpg():
        if request.method == 'POST':
            payload = request.get_json(silent=True) or {}
            section = payload.get("section")
            try:
                section = int(section) if section is not None else None
            except Exception:
                section = None

            report_date = payload.get("report_date") or datetime.now(tz=LOCAL_TZ).strftime("%Y-%m-%d")
            unit = (payload.get("unit") or "UNIT").strip().upper()
            snapshot_payload = _fetch_morning_report_snapshot(report_date, unit)
            if snapshot_payload:
                if not _rows_have_any_value(payload.get("doctorwise_rows"), ("department", "opd", "ipd")):
                    payload["doctorwise_rows"] = snapshot_payload.get("doctorwise_rows") or []
                if not _rows_have_any_value(payload.get("occupancy_rows"), ("ward", "patient_count", "occupied_count", "clinically_discharged_count")):
                    payload["occupancy_rows"] = snapshot_payload.get("occupancy_rows") or []
                if not _rows_have_any_value(payload.get("doctor_night_visits"), ("patient_name", "admission_time", "consultant", "visiting")):
                    payload["doctor_night_visits"] = snapshot_payload.get("doctor_night_visits") or []
                if not _rows_have_any_value(payload.get("staff_rows"), ("department", "doctor", "nursing", "aaya", "ward_boy", "housekeeping")):
                    payload["staff_rows"] = snapshot_payload.get("staff_rows") or []
                if not _rows_have_any_value(payload.get("non_medical_rows"), ("left_label", "left_count", "right_label", "right_count")):
                    payload["non_medical_rows"] = snapshot_payload.get("non_medical_rows") or []
                if not _rows_have_any_value(payload.get("referral_rows"), ("patient_name", "visit_time", "consultant", "reason")):
                    payload["referral_rows"] = snapshot_payload.get("referral_rows") or []

            needs_live = (
                not _rows_have_any_value(payload.get("doctorwise_rows"), ("department", "opd", "ipd"))
                or not _rows_have_any_value(payload.get("occupancy_rows"), ("ward", "patient_count", "occupied_count", "clinically_discharged_count"))
                or not _rows_have_any_value(payload.get("doctor_night_visits"), ("patient_name", "admission_time", "consultant", "visiting"))
            )
            if needs_live:
                live_payload = _build_morning_report_payload(unit, report_date, source="live", mod_name=payload.get("mod_name"))
                if live_payload:
                    if not _rows_have_any_value(payload.get("doctorwise_rows"), ("department", "opd", "ipd")):
                        payload["doctorwise_rows"] = live_payload.get("doctorwise_rows") or []
                    if not _rows_have_any_value(payload.get("occupancy_rows"), ("ward", "patient_count", "occupied_count", "clinically_discharged_count")):
                        payload["occupancy_rows"] = live_payload.get("occupancy_rows") or []
                    if not _rows_have_any_value(payload.get("doctor_night_visits"), ("patient_name", "admission_time", "consultant", "visiting")):
                        payload["doctor_night_visits"] = live_payload.get("doctor_night_visits") or []

            exported_by = session.get("username") or session.get("user") or "Unknown"
            data = _build_morning_report_jpg(payload, exported_by=exported_by, section=section)
            suffix = f"_P{section}" if section in (1, 2, 3) else ""
            return send_file(
                io.BytesIO(data),
                as_attachment=True,
                download_name=f"Morning_Report_{unit}_{report_date}{suffix}.jpg",
                mimetype="image/jpeg",
            )

        report_date = (request.args.get("date") or "").strip()
        mode = (request.args.get("mode") or "snapshot").strip().lower()
        unit = (request.args.get("unit") or "").strip().upper()
        section = request.args.get("section")
        try:
            section = int(section) if section is not None else None
        except Exception:
            section = None
        if not report_date:
            report_date = datetime.now(tz=LOCAL_TZ).strftime("%Y-%m-%d")

        allowed_units = _analytics_allowed_units_for_session() or _allowed_units_for_session()
        if allowed_units and unit and unit not in allowed_units:
            return jsonify({"status": "error", "message": "Unit not allowed"}), 403
        if not unit:
            unit = _resolve_morning_unit(allowed_units, None)
        if not unit:
            return jsonify({"status": "error", "message": "Unit is required"}), 400

        payload = None
        if mode == "snapshot":
            payload = _fetch_morning_report_snapshot(report_date, unit)
        if not payload:
            payload = _build_morning_report_payload(unit, report_date, source="live", mod_name=(session.get("username") or session.get("user") or ""))
        if not payload:
            return jsonify({"status": "error", "message": "Failed to build report"}), 500

        exported_by = session.get("username") or session.get("user") or "Unknown"
        data = _build_morning_report_jpg(payload, exported_by=exported_by, section=section)
        suffix = f"_P{section}" if section in (1, 2, 3) else ""
        return send_file(
            io.BytesIO(data),
            as_attachment=True,
            download_name=f"Morning_Report_{unit}_{report_date}{suffix}.jpg",
            mimetype="image/jpeg",
        )
