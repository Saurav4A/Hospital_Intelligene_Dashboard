from flask import jsonify, render_template, request
import numpy as np
import pandas as pd

from modules import data_fetch


def register_patient_journey_routes(
    app,
    *,
    login_required,
    allowed_units_for_session,
    modification_units,
    sanitize_json_payload,
    coerce_int,
):
    """Register patient journey routes."""
    _allowed_units_for_session = allowed_units_for_session
    _modification_units = modification_units
    _sanitize_json_payload = sanitize_json_payload
    _coerce_int = coerce_int

    @app.route('/patient-care-journey')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def patient_care_journey_page_view():
        allowed_units = _modification_units(_allowed_units_for_session())
        return render_template('patient_care_journey.html', allowed_units=allowed_units)

    @app.route('/api/patient-care-journey/prewarm')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_patient_care_journey_prewarm():
        unit = (request.args.get("unit") or "").strip() or None
        allowed_units = _modification_units(_allowed_units_for_session())
        target_unit = (unit or (allowed_units[0] if allowed_units else None))
        if not target_unit:
            return jsonify({"status": "error", "message": "No unit available"}), 400
        target_unit = target_unit.strip().upper()
        if allowed_units and target_unit not in allowed_units:
            return jsonify({"status": "error", "message": "Unit not allowed"}), 403
        warmed = data_fetch.warm_patient_care_journey_reference_cache(target_unit)
        if not warmed:
            return jsonify({"status": "error", "message": "Failed to warm journey context"}), 500
        return jsonify({"status": "success", "unit": target_unit, "message": "Journey context warmed"})

    @app.route('/api/patient-care-journey/patients_search')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_patient_care_journey_patients_search():
        unit = (request.args.get("unit") or "").strip() or None
        query = (request.args.get("q") or "").strip()
        if not query:
            return jsonify({"status": "error", "message": "Search query required"}), 400
        if len(query) < 2 and not query.isdigit():
            return jsonify({"status": "error", "message": "Enter at least 2 characters"}), 400

        limit_raw = request.args.get("limit")
        try:
            limit = int(limit_raw) if limit_raw is not None else 200
        except Exception:
            limit = 200
        limit = max(1, min(limit, 1000))

        allowed_units = _modification_units(_allowed_units_for_session())
        target_unit = (unit or (allowed_units[0] if allowed_units else None))
        if not target_unit:
            return jsonify({"status": "error", "message": "No unit available"}), 400
        target_unit = target_unit.strip().upper()
        if allowed_units and target_unit not in allowed_units:
            return jsonify({"status": "error", "message": "Unit not allowed"}), 403

        df = data_fetch.fetch_patient_care_journey_patients(target_unit, query, limit=limit)
        if df is None:
            return jsonify({"status": "error", "message": "Database error"}), 500
        if df.empty:
            return jsonify({"status": "success", "data": [], "unit": target_unit, "count": 0}), 200

        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")
        df = df.replace({np.nan: None, np.inf: None, -np.inf: None})
        records = df.to_dict(orient="records")
        return jsonify({"status": "success", "data": records, "unit": target_unit, "count": len(records)})

    @app.route('/api/patient-care-journey/page')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_patient_care_journey_page():
        unit = (request.args.get("unit") or "").strip() or None
        patient_id_val = _coerce_int(request.args.get("patient_id", request.args.get("patientId")), allow_none=True)
        if not patient_id_val:
            return jsonify({"status": "error", "message": "Patient ID required"}), 400

        page = _coerce_int(request.args.get("page"), allow_none=True) or 1
        page_size = _coerce_int(request.args.get("page_size", request.args.get("pageSize")), allow_none=True) or 20
        if page_size not in {20, 50, 100}:
            page_size = 20

        allowed_units = _modification_units(_allowed_units_for_session())
        target_unit = (unit or (allowed_units[0] if allowed_units else None))
        if not target_unit:
            return jsonify({"status": "error", "message": "No unit available"}), 400
        target_unit = target_unit.strip().upper()
        if allowed_units and target_unit not in allowed_units:
            return jsonify({"status": "error", "message": "Unit not allowed"}), 403

        payload = data_fetch.fetch_patient_care_journey_page(target_unit, patient_id_val, page=page, page_size=page_size)
        if payload is None:
            return jsonify({"status": "error", "message": "Database error"}), 500

        response = {
            "status": "success",
            "unit": target_unit,
            "patient_id": patient_id_val,
            "patient_summary": payload.get("patient_summary") or {},
            "rows": payload.get("rows") or [],
            "page": payload.get("page") or page,
            "page_size": payload.get("page_size") or page_size,
            "total_rows": payload.get("total_rows") or 0,
            "total_pages": payload.get("total_pages") or 0,
        }
        return jsonify(_sanitize_json_payload(response))

    @app.route('/api/patient-care-journey/detail')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_patient_care_journey_detail():
        unit = (request.args.get("unit") or "").strip() or None
        patient_id_val = _coerce_int(request.args.get("patient_id", request.args.get("patientId")), allow_none=True)
        visit_id_val = _coerce_int(request.args.get("visit_id", request.args.get("visitId")), allow_none=True)
        source_visit_id_val = _coerce_int(request.args.get("source_visit_id", request.args.get("sourceVisitId")), allow_none=True)
        journey_row_type = (request.args.get("journey_row_type", request.args.get("journeyRowType")) or "").strip().lower()

        if not patient_id_val:
            return jsonify({"status": "error", "message": "Patient ID required"}), 400
        if not visit_id_val:
            return jsonify({"status": "error", "message": "Visit ID required"}), 400
        if journey_row_type not in {"visit", "virtual_visit", "duplicate", "visit_duplicate"}:
            return jsonify({"status": "error", "message": "Invalid journey row type"}), 400

        allowed_units = _modification_units(_allowed_units_for_session())
        target_unit = (unit or (allowed_units[0] if allowed_units else None))
        if not target_unit:
            return jsonify({"status": "error", "message": "No unit available"}), 400
        target_unit = target_unit.strip().upper()
        if allowed_units and target_unit not in allowed_units:
            return jsonify({"status": "error", "message": "Unit not allowed"}), 403

        detail = data_fetch.fetch_patient_care_journey_visit_detail(
            target_unit,
            patient_id_val,
            journey_row_type,
            visit_id_val,
            source_visit_id=source_visit_id_val,
        )
        if detail is None:
            return jsonify({"status": "error", "message": "Database error"}), 500

        response = {
            "status": "success",
            "unit": target_unit,
            "patient_id": patient_id_val,
            "journey_row_type": "virtual_visit" if journey_row_type in {"virtual_visit", "duplicate", "visit_duplicate"} else "visit",
            "visit_id": visit_id_val,
            "source_visit_id": source_visit_id_val,
            "detail": detail,
        }
        return jsonify(_sanitize_json_payload(response))
