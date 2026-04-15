from datetime import date, datetime, timedelta
import io
import os
import time

from flask import jsonify, render_template, request, send_file, session


def register_mod_reports_night_routes(
    app,
    *,
    login_required,
    analytics_allowed_units_for_session,
    allowed_units_for_session,
    get_login_db_connection,
    ensure_night_report_detail_table,
    ensure_night_report_ward_table,
    coerce_local_dt,
    fetch_previous_night_snapshot_date,
    night_payload_has_core_values,
    mod_report_cache_get,
    mod_report_cache_set,
    mod_report_cache_clear,
    attach_mod_report_lock,
    build_night_report_payload,
    build_night_report_excel,
    build_night_report_pdf,
    build_night_report_jpg,
    save_night_report_snapshot_atomic,
    build_night_report_diff,
    mod_report_edit_lock_info,
    is_truthy,
    audit_log_event,
    local_tz,
    night_report_equipment_cols,
):
    """Register MOD night-report routes."""
    _analytics_allowed_units_for_session = analytics_allowed_units_for_session
    _allowed_units_for_session = allowed_units_for_session
    _get_login_db_connection = get_login_db_connection
    _ensure_night_report_detail_table = ensure_night_report_detail_table
    _ensure_night_report_ward_table = ensure_night_report_ward_table
    _coerce_local_dt = coerce_local_dt
    _fetch_previous_night_snapshot_date = fetch_previous_night_snapshot_date
    _night_payload_has_core_values = night_payload_has_core_values
    _mod_report_cache_get = mod_report_cache_get
    _mod_report_cache_set = mod_report_cache_set
    _mod_report_cache_clear = mod_report_cache_clear
    _attach_mod_report_lock = attach_mod_report_lock
    _build_night_report_payload = build_night_report_payload
    _build_night_report_excel = build_night_report_excel
    _build_night_report_pdf = build_night_report_pdf
    _build_night_report_jpg = build_night_report_jpg
    _save_night_report_snapshot_atomic = save_night_report_snapshot_atomic
    _build_night_report_diff = build_night_report_diff
    _mod_report_edit_lock_info = mod_report_edit_lock_info
    _is_truthy = is_truthy
    _audit_log_event = audit_log_event
    LOCAL_TZ = local_tz
    NIGHT_REPORT_EQUIPMENT_COLS = night_report_equipment_cols

    @app.route('/mod_reports/night_report')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head", "Executive"})
    def night_report_page():
        allowed_units = _analytics_allowed_units_for_session()
        if not allowed_units:
            allowed_units = _allowed_units_for_session()
        selected_unit = (request.args.get("unit") or "").strip().upper()
        if not selected_unit and allowed_units:
            selected_unit = allowed_units[0]
        today = datetime.now(tz=LOCAL_TZ).strftime("%Y-%m-%d")
        return render_template(
            'night_report.html',
            allowed_units=allowed_units,
            selected_unit=selected_unit,
            today=today
        )

    @app.route('/api/mod_reports/night_report/snapshot_dates')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head", "Executive"})
    def api_night_report_snapshot_dates():
        unit = (request.args.get("unit") or "").strip().upper()
        allowed_units = _analytics_allowed_units_for_session() or _allowed_units_for_session()
        if allowed_units and unit and unit not in allowed_units:
            return jsonify({"status": "error", "message": "Unit not allowed"}), 403
        if not unit and allowed_units:
            unit = allowed_units[0]
        if not unit:
            return jsonify({"status": "error", "message": "Unit is required"}), 400

        try:
            with _get_login_db_connection() as conn:
                _ensure_night_report_detail_table(conn)
                _ensure_night_report_ward_table(conn)
                cur = conn.cursor()
                cur.execute(
                    """
                    SELECT TOP 120 d.SnapshotDate, d.MaxDetailTime, w.MaxWardTime
                    FROM (
                        SELECT SnapshotDate, MAX(SnapshotTime) AS MaxDetailTime
                        FROM dbo.HID_Night_Report_Detail WITH (NOLOCK)
                        WHERE Unit = ?
                        GROUP BY SnapshotDate
                    ) d
                    INNER JOIN (
                        SELECT SnapshotDate, MAX(SnapshotTime) AS MaxWardTime
                        FROM dbo.HID_Night_Report_Ward WITH (NOLOCK)
                        WHERE Unit = ?
                        GROUP BY SnapshotDate
                    ) w ON w.SnapshotDate = d.SnapshotDate
                    ORDER BY d.SnapshotDate DESC
                    """,
                    (unit, unit),
                )
                rows = cur.fetchall()
        except Exception as e:
            return jsonify({"status": "error", "message": f"Failed to load snapshot dates: {e}"}), 500

        dates = []
        now_local = datetime.now(tz=LOCAL_TZ)
        for row in rows:
            snap_date = row[0] if row else None
            detail_time = row[1] if row and len(row) > 1 else None
            ward_time = row[2] if row and len(row) > 2 else None
            if snap_date is None:
                continue
            latest_time = ward_time or detail_time
            if detail_time and ward_time:
                try:
                    latest_time = max(detail_time, ward_time)
                except Exception:
                    latest_time = ward_time or detail_time
            snap_dt = _coerce_local_dt(latest_time)
            if snap_dt and snap_dt > (now_local + timedelta(minutes=1)):
                continue
            if isinstance(snap_date, (datetime, date)):
                dates.append(snap_date.strftime("%Y-%m-%d"))
            else:
                raw = str(snap_date).strip()
                if len(raw) >= 10 and raw[4] == "-" and raw[7] == "-":
                    raw = raw[:10]
                dates.append(raw)

        return jsonify({"status": "success", "dates": dates})

    @app.route('/api/mod_reports/night_report')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head", "Executive"})
    def api_night_report_data():
        date_str = (request.args.get("date") or "").strip()
        mode = (request.args.get("mode") or "snapshot").strip().lower()
        unit = (request.args.get("unit") or "").strip().upper()
        if not date_str:
            date_str = datetime.now(tz=LOCAL_TZ).strftime("%Y-%m-%d")

        _t0 = time.perf_counter()

        def _log(stage: str):
            elapsed = (time.perf_counter() - _t0) * 1000
            print(f"[timing] night_report {unit or '-'} {date_str} {mode} {stage}: {elapsed:.1f}ms")

        allowed_units = _analytics_allowed_units_for_session() or _allowed_units_for_session()
        _log("allowed_units")
        if allowed_units and unit and unit not in allowed_units:
            return jsonify({"status": "error", "message": "Unit not allowed"}), 403
        if not unit and allowed_units:
            unit = allowed_units[0]
        if not unit:
            return jsonify({"status": "error", "message": "Unit is required"}), 400

        role = str(session.get("role") or "").strip().lower()
        is_it = role == "it"

        cached_payload = _mod_report_cache_get("night", unit, date_str, mode)
        _log("cache_check")
        if cached_payload:
            if mode == "snapshot":
                now_local = datetime.now(tz=LOCAL_TZ)
                snap_dt = _coerce_local_dt(cached_payload.get("snapshot_time"))
                is_future_snapshot = bool(snap_dt and snap_dt > (now_local + timedelta(minutes=1)))
                if (not _night_payload_has_core_values(cached_payload)) or is_future_snapshot:
                    _mod_report_cache_clear("night", unit, date_str)
                    cached_payload = None
                    _log("cache_drop_invalid_snapshot")
            if cached_payload:
                _log("cache_hit")
                return jsonify({"status": "success", "data": _attach_mod_report_lock(cached_payload, unit, date_str, is_it, report_kind="night")})

        if mode == "snapshot":
            now_local = datetime.now(tz=LOCAL_TZ)
            payload = _build_night_report_payload(unit, date_str, source="snapshot")
            _log("build_snapshot")
            snap_dt = _coerce_local_dt(payload.get("snapshot_time")) if payload else None
            is_future_snapshot = bool(snap_dt and snap_dt > (now_local + timedelta(minutes=1)))
            if (not payload) or (not _night_payload_has_core_values(payload)) or is_future_snapshot:
                prev_date = _fetch_previous_night_snapshot_date(date_str, unit)
                if prev_date:
                    prev_payload = _build_night_report_payload(unit, prev_date, source="snapshot")
                    if prev_payload and _night_payload_has_core_values(prev_payload):
                        prev_payload["requested_date"] = date_str
                        prev_payload["resolved_snapshot_date"] = prev_date
                        payload = prev_payload
                        _log("fallback_prev_snapshot")
            if not payload or not _night_payload_has_core_values(payload):
                return jsonify({"status": "error", "message": "No snapshot data for this date"}), 404
            if str(payload.get("date") or "") == date_str:
                _mod_report_cache_set("night", unit, date_str, mode, payload)
                _log("cache_set_snapshot")
            else:
                _log("skip_cache_snapshot_fallback")
            return jsonify({"status": "success", "data": _attach_mod_report_lock(payload, unit, date_str, is_it, report_kind="night")})

        payload = _build_night_report_payload(unit, date_str, source="live")
        _log("build_live")
        if not payload:
            return jsonify({"status": "error", "message": "Failed to load data"}), 500
        _mod_report_cache_set("night", unit, date_str, mode, payload)
        _log("cache_set_live")
        return jsonify({"status": "success", "data": _attach_mod_report_lock(payload, unit, date_str, is_it, report_kind="night")})

    @app.route('/api/mod_reports/night_report/export')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head", "Executive"})
    def api_night_report_export():
        date_str = (request.args.get("date") or "").strip()
        mode = (request.args.get("mode") or "snapshot").strip().lower()
        unit = (request.args.get("unit") or "").strip().upper()
        if not date_str:
            date_str = datetime.now(tz=LOCAL_TZ).strftime("%Y-%m-%d")

        allowed_units = _analytics_allowed_units_for_session() or _allowed_units_for_session()
        if allowed_units and unit and unit not in allowed_units:
            return jsonify({"status": "error", "message": "Unit not allowed"}), 403
        if not unit and allowed_units:
            unit = allowed_units[0]
        if not unit:
            return jsonify({"status": "error", "message": "Unit is required"}), 400

        payload = None
        if mode == "snapshot":
            export_dir = os.path.join("data", "exports", "night_report")
            file_name = f"Night_Report_{unit}_{date_str}.xlsx"
            file_path = os.path.join(export_dir, file_name)
            if os.path.exists(file_path):
                return send_file(
                    file_path,
                    as_attachment=True,
                    download_name=file_name,
                    mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
            payload = _build_night_report_payload(unit, date_str, source="snapshot")
            if not payload or not payload.get("ward_occupancy"):
                return jsonify({"status": "error", "message": "No snapshot data for this date"}), 404
        else:
            payload = _build_night_report_payload(unit, date_str, source="live")
            if not payload:
                return jsonify({"status": "error", "message": "Failed to load data"}), 500

        exported_by = session.get("username") or session.get("user") or "Unknown"
        data = _build_night_report_excel(payload, exported_by=exported_by)
        return send_file(
            io.BytesIO(data),
            as_attachment=True,
            download_name=f"Night_Report_{unit}_{date_str}.xlsx",
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    @app.route('/api/mod_reports/night_report/export_pdf', methods=['GET', 'POST'])
    @login_required(allowed_roles={"IT", "Management", "Departmental Head", "Executive"})
    def api_night_report_export_pdf():
        if request.method == 'POST':
            payload = request.get_json(silent=True) or {}
            unit = (payload.get("unit") or "").strip().upper()
            date_str = (payload.get("date") or "").strip() or datetime.now(tz=LOCAL_TZ).strftime("%Y-%m-%d")
            if not unit:
                return jsonify({"status": "error", "message": "Unit is required"}), 400
            force_fallback = _is_truthy(request.args.get("force_fallback") or payload.get("force_fallback"))
            if force_fallback:
                payload["force_fallback_pdf"] = True
            payload.setdefault("date", date_str)
            payload.setdefault("unit", unit)
            payload.setdefault("equipment_columns", NIGHT_REPORT_EQUIPMENT_COLS)
            exported_by = session.get("username") or session.get("user") or "Unknown"
            data = _build_night_report_pdf(payload, exported_by=exported_by)
            return send_file(
                io.BytesIO(data),
                as_attachment=True,
                download_name=f"Night_Report_{unit}_{date_str}.pdf",
                mimetype="application/pdf",
            )

        date_str = (request.args.get("date") or "").strip()
        mode = (request.args.get("mode") or "snapshot").strip().lower()
        unit = (request.args.get("unit") or "").strip().upper()
        if not date_str:
            date_str = datetime.now(tz=LOCAL_TZ).strftime("%Y-%m-%d")

        allowed_units = _analytics_allowed_units_for_session() or _allowed_units_for_session()
        if allowed_units and unit and unit not in allowed_units:
            return jsonify({"status": "error", "message": "Unit not allowed"}), 403
        if not unit and allowed_units:
            unit = allowed_units[0]
        if not unit:
            return jsonify({"status": "error", "message": "Unit is required"}), 400

        payload = None
        if mode == "snapshot":
            payload = _build_night_report_payload(unit, date_str, source="snapshot")
            if not payload or not payload.get("ward_occupancy"):
                return jsonify({"status": "error", "message": "No snapshot data for this date"}), 404
        else:
            payload = _build_night_report_payload(unit, date_str, source="live")
            if not payload:
                return jsonify({"status": "error", "message": "Failed to load data"}), 500
        force_fallback = _is_truthy(request.args.get("force_fallback"))
        if force_fallback:
            payload["force_fallback_pdf"] = True

        exported_by = session.get("username") or session.get("user") or "Unknown"
        data = _build_night_report_pdf(payload, exported_by=exported_by)
        return send_file(
            io.BytesIO(data),
            as_attachment=True,
            download_name=f"Night_Report_{unit}_{date_str}.pdf",
            mimetype="application/pdf",
        )

    @app.route('/api/mod_reports/night_report/export_jpg', methods=['GET', 'POST'])
    @login_required(allowed_roles={"IT", "Management", "Departmental Head", "Executive"})
    def api_night_report_export_jpg():
        if request.method == 'POST':
            payload = request.get_json(silent=True) or {}
            unit = (payload.get("unit") or "").strip().upper()
            date_str = (payload.get("date") or "").strip() or datetime.now(tz=LOCAL_TZ).strftime("%Y-%m-%d")
            if not unit:
                return jsonify({"status": "error", "message": "Unit is required"}), 400
            payload.setdefault("date", date_str)
            payload.setdefault("unit", unit)
            payload.setdefault("equipment_columns", NIGHT_REPORT_EQUIPMENT_COLS)
            exported_by = session.get("username") or session.get("user") or "Unknown"
            try:
                data = _build_night_report_jpg(payload, exported_by=exported_by)
            except Exception as exc:
                return jsonify({"status": "error", "message": str(exc)}), 500
            return send_file(
                io.BytesIO(data),
                as_attachment=True,
                download_name=f"Night_Report_{unit}_{date_str}.jpg",
                mimetype="image/jpeg",
            )

        date_str = (request.args.get("date") or "").strip()
        mode = (request.args.get("mode") or "snapshot").strip().lower()
        unit = (request.args.get("unit") or "").strip().upper()
        if not date_str:
            date_str = datetime.now(tz=LOCAL_TZ).strftime("%Y-%m-%d")

        allowed_units = _analytics_allowed_units_for_session() or _allowed_units_for_session()
        if allowed_units and unit and unit not in allowed_units:
            return jsonify({"status": "error", "message": "Unit not allowed"}), 403
        if not unit and allowed_units:
            unit = allowed_units[0]
        if not unit:
            return jsonify({"status": "error", "message": "Unit is required"}), 400

        payload = None
        if mode == "snapshot":
            payload = _build_night_report_payload(unit, date_str, source="snapshot")
            if not payload or not payload.get("ward_occupancy"):
                return jsonify({"status": "error", "message": "No snapshot data for this date"}), 404
        else:
            payload = _build_night_report_payload(unit, date_str, source="live")
            if not payload:
                return jsonify({"status": "error", "message": "Failed to load data"}), 500

        exported_by = session.get("username") or session.get("user") or "Unknown"
        try:
            data = _build_night_report_jpg(payload, exported_by=exported_by)
        except Exception as exc:
            return jsonify({"status": "error", "message": str(exc)}), 500
        return send_file(
            io.BytesIO(data),
            as_attachment=True,
            download_name=f"Night_Report_{unit}_{date_str}.jpg",
            mimetype="image/jpeg",
        )

    @app.route('/api/mod_reports/night_report/save', methods=['POST'])
    @login_required(allowed_roles={"IT", "Management", "Departmental Head", "Executive"})
    def api_night_report_save():
        payload = request.get_json(silent=True) or {}
        date_str = (payload.get("date") or "").strip()
        unit = (payload.get("unit") or "").strip().upper()
        if not date_str:
            date_str = datetime.now(tz=LOCAL_TZ).strftime("%Y-%m-%d")

        allowed_units = _analytics_allowed_units_for_session() or _allowed_units_for_session()
        if allowed_units and unit and unit not in allowed_units:
            return jsonify({"status": "error", "message": "Unit not allowed"}), 403
        if not unit and allowed_units:
            unit = allowed_units[0]
        if not unit:
            return jsonify({"status": "error", "message": "Unit is required"}), 400
        entity_id = f"{unit}:{date_str}"

        role = str(session.get("role") or "").strip().lower()
        if role != "it":
            lock_info = _mod_report_edit_lock_info(unit, date_str, report_kind="night")
            if lock_info.get("locked"):
                lock_at = lock_info.get("lock_at") or ""
                msg = f"Edits are locked for {date_str} (locked at {lock_at}). Contact IT."
                _audit_log_event(
                    "mod_reports",
                    "night_save",
                    status="error",
                    entity_type="night_report",
                    entity_id=entity_id,
                    unit=unit,
                    summary="Edits are locked",
                    details={"lock_at": lock_at},
                )
                return jsonify({"status": "error", "message": msg}), 403

        clean_payload = {
            "unit": unit,
            "date": date_str,
            "snapshot_time": datetime.now(tz=LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S"),
            "particulars": payload.get("particulars") or [],
            "occupancy_paytype": payload.get("occupancy_paytype") or [],
            "equipment_rows": payload.get("equipment_rows") or [],
            "doctor_entries": payload.get("doctor_entries") or [],
            "doctor_volume": payload.get("doctor_volume") or [],
            "ward_occupancy": payload.get("ward_occupancy") or [],
        }

        old_payload = None
        try:
            old_payload = _build_night_report_payload(unit, date_str, source="snapshot")
        except Exception:
            old_payload = None

        try:
            detail_rows, ward_rows = _save_night_report_snapshot_atomic(clean_payload)
        except Exception as e:
            _audit_log_event(
                "mod_reports",
                "night_save",
                status="error",
                entity_type="night_report",
                entity_id=entity_id,
                unit=unit,
                summary="Failed to save night report snapshot",
                details={"error": str(e)},
            )
            return jsonify({"status": "error", "message": f"Failed to save snapshot: {e}"}), 500

        _mod_report_cache_clear("night", unit, date_str)

        diff = _build_night_report_diff(old_payload, clean_payload)
        _audit_log_event(
            "mod_reports",
            "night_save",
            status="success",
            entity_type="night_report",
            entity_id=entity_id,
            unit=unit,
            summary="Night report saved",
            details={"diff": diff},
        )
        return jsonify({
            "status": "success",
            "message": "Night report saved.",
            "details_saved": detail_rows,
            "wards_saved": ward_rows,
        })
