from __future__ import annotations

import io
from typing import Any

from flask import Blueprint, current_app, jsonify, render_template, request, send_file, session

from . import services
from .reports_excel import build_excel_report
from .reports_pdf import build_pdf_report
from .utils import DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE, MODULE_KEY, parse_positive_int, safe_text, validate_int_list


def create_patient_diagnostic_blueprint(*, login_required, allowed_units_for_session):
    bp = Blueprint("patient_diagnostic_intelligence", __name__)

    def _allowed_units() -> list[str]:
        return [str(u).strip().upper() for u in (allowed_units_for_session() or []) if str(u).strip()]

    def _resolve_unit(payload: dict[str, Any] | None = None):
        payload = payload or {}
        requested = safe_text(payload.get("unit") or request.args.get("unit")).upper()
        allowed = _allowed_units()
        if not allowed:
            return None, allowed, (jsonify({"status": "error", "message": "No unit access assigned"}), 403)
        unit = requested or allowed[0]
        if unit not in allowed:
            return None, allowed, (jsonify({"status": "error", "message": "Unit not allowed"}), 403)
        return unit, allowed, None

    def _json_error(message: str, status: int = 500):
        return jsonify({"status": "error", "message": message}), status

    def _page_args():
        json_payload = request.get_json(silent=True) or {} if request.is_json else {}
        page = parse_positive_int(request.args.get("page") or json_payload.get("page") or 1, 1)
        page_size = parse_positive_int(
            request.args.get("page_size") or request.args.get("pageSize") or json_payload.get("page_size") or json_payload.get("pageSize"),
            DEFAULT_PAGE_SIZE,
            maximum=MAX_PAGE_SIZE,
        )
        return page, page_size

    @bp.route("/patient-diagnostic-intelligence")
    @login_required(required_section=MODULE_KEY)
    def page():
        allowed = _allowed_units()
        return render_template("patient_diagnostic_intelligence.html", allowed_units=allowed, default_unit=(allowed[0] if allowed else ""))

    @bp.route("/api/patient-diagnostic/test-master")
    @login_required(required_section=MODULE_KEY)
    def test_master():
        unit, _allowed, err = _resolve_unit()
        if err:
            return err
        try:
            return jsonify(services.get_test_master(unit, request.args.get("q") or "", request.args.get("limit") or 300))
        except Exception:
            current_app.logger.exception("Patient Diagnostic Intelligence test master failed")
            return _json_error("Unable to load pathology test master.")

    @bp.route("/api/patient-diagnostic/parameter-master")
    @login_required(required_section=MODULE_KEY)
    def parameter_master():
        unit, _allowed, err = _resolve_unit()
        if err:
            return err
        try:
            return jsonify(services.get_parameter_master(unit, validate_int_list(request.args.getlist("test_ids") or request.args.get("test_ids"))))
        except Exception:
            current_app.logger.exception("Patient Diagnostic Intelligence parameter master failed")
            return _json_error("Unable to load pathology parameters.")

    @bp.route("/api/patient-diagnostic/search-patients")
    @login_required(required_section=MODULE_KEY)
    def search_patients():
        unit, _allowed, err = _resolve_unit()
        if err:
            return err
        query = safe_text(request.args.get("q") or request.args.get("search"), 100)
        if len(query) < 2 and not query.isdigit():
            return _json_error("Enter at least 2 characters to search patients.", 400)
        try:
            return jsonify(services.search_patients(unit, query, request.args.get("limit") or 50))
        except Exception:
            current_app.logger.exception("Patient Diagnostic Intelligence patient search failed")
            return _json_error("Unable to search patients.")

    @bp.route("/api/patient-diagnostic/patient-tests", methods=["POST"])
    @login_required(required_section=MODULE_KEY)
    def patient_tests():
        payload = request.get_json(silent=True) or {}
        unit, _allowed, err = _resolve_unit(payload)
        if err:
            return err
        try:
            return jsonify(services.get_patient_test_suggestions(unit, payload))
        except Exception:
            current_app.logger.exception("Patient Diagnostic Intelligence patient test suggestions failed")
            return _json_error("Unable to load patient test suggestions.")

    @bp.route("/api/patient-diagnostic/patient-history", methods=["POST"])
    @login_required(required_section=MODULE_KEY)
    def patient_history():
        payload = request.get_json(silent=True) or {}
        unit, _allowed, err = _resolve_unit(payload)
        if err:
            return err
        page, page_size = _page_args()
        try:
            return jsonify(services.get_patient_history(unit, payload, page=page, page_size=page_size))
        except Exception:
            current_app.logger.exception("Patient Diagnostic Intelligence patient history failed")
            return _json_error("Unable to load patient diagnostic history.")

    @bp.route("/api/patient-diagnostic/abnormal-results", methods=["POST"])
    @login_required(required_section=MODULE_KEY)
    def abnormal_results():
        payload = request.get_json(silent=True) or {}
        unit, _allowed, err = _resolve_unit(payload)
        if err:
            return err
        page, page_size = _page_args()
        try:
            return jsonify(services.get_abnormal_results(unit, payload, page=page, page_size=page_size))
        except Exception:
            current_app.logger.exception("Patient Diagnostic Intelligence abnormal results failed")
            return _json_error("Unable to load abnormal result worklist.")

    @bp.route("/api/patient-diagnostic/test-comparison", methods=["POST"])
    @login_required(required_section=MODULE_KEY)
    def test_comparison():
        payload = request.get_json(silent=True) or {}
        unit, _allowed, err = _resolve_unit(payload)
        if err:
            return err
        try:
            return jsonify(services.get_patient_test_comparison(unit, payload))
        except Exception:
            current_app.logger.exception("Patient Diagnostic Intelligence parameter comparison failed")
            return _json_error("Unable to build patient parameter comparison.")

    @bp.route("/api/patient-diagnostic/followup-candidates", methods=["POST"])
    @login_required(required_section=MODULE_KEY)
    def followup_candidates():
        payload = request.get_json(silent=True) or {}
        unit, _allowed, err = _resolve_unit(payload)
        if err:
            return err
        page, page_size = _page_args()
        try:
            return jsonify(services.get_followup_candidates(unit, payload, page=page, page_size=page_size))
        except Exception:
            current_app.logger.exception("Patient Diagnostic Intelligence follow-up candidates failed")
            return _json_error("Unable to build follow-up candidate list.")

    @bp.route("/api/patient-diagnostic/reports/preview", methods=["POST"])
    @login_required(required_section=MODULE_KEY)
    def report_preview():
        payload = request.get_json(silent=True) or {}
        unit, _allowed, err = _resolve_unit(payload)
        if err:
            return err
        page, page_size = _page_args()
        try:
            return jsonify(services.build_report_preview(unit, payload, page=page, page_size=page_size))
        except Exception:
            current_app.logger.exception("Patient Diagnostic Intelligence report preview failed")
            return _json_error("Unable to generate report preview.")

    @bp.route("/api/patient-diagnostic/reports/export-excel", methods=["POST"])
    @login_required(required_section=MODULE_KEY)
    def export_excel():
        payload = request.get_json(silent=True) or {}
        unit, _allowed, err = _resolve_unit(payload)
        if err:
            return err
        try:
            dataset = services.build_export_dataset(unit, payload)
            if not (dataset.get("filters") or {}).get("test_ids"):
                return _json_error(dataset.get("message") or "Select at least one pathology test before exporting.", 400)
            data, filename = build_excel_report(dataset, session.get("username") or session.get("user") or "Unknown")
            return send_file(
                io.BytesIO(data),
                as_attachment=True,
                download_name=filename,
                mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        except Exception as exc:
            current_app.logger.exception("Patient Diagnostic Intelligence Excel export failed")
            return _json_error(str(exc) if "row limit" in str(exc).lower() else "Unable to generate Excel export.")

    @bp.route("/api/patient-diagnostic/reports/export-pdf", methods=["POST"])
    @login_required(required_section=MODULE_KEY)
    def export_pdf():
        payload = request.get_json(silent=True) or {}
        unit, _allowed, err = _resolve_unit(payload)
        if err:
            return err
        try:
            dataset = services.build_export_dataset(unit, payload)
            if not (dataset.get("filters") or {}).get("test_ids"):
                return _json_error(dataset.get("message") or "Select at least one pathology test before exporting.", 400)
            data, filename = build_pdf_report(dataset, session.get("username") or session.get("user") or "Unknown")
            return send_file(io.BytesIO(data), as_attachment=True, download_name=filename, mimetype="application/pdf")
        except ValueError as exc:
            return _json_error(str(exc), 400)
        except Exception:
            current_app.logger.exception("Patient Diagnostic Intelligence PDF export failed")
            return _json_error("Unable to generate PDF export.")

    @bp.route("/api/patient-diagnostic/reports/options")
    @login_required(required_section=MODULE_KEY)
    def report_options():
        return jsonify(services.report_options())

    @bp.route("/api/patient-diagnostic/settings")
    @login_required(required_section=MODULE_KEY)
    def settings():
        return jsonify(services.settings_payload())

    @bp.route("/api/patient-diagnostic/settings/save", methods=["POST"])
    @login_required(required_section=MODULE_KEY)
    def save_settings():
        return jsonify({"status": "success", "message": "Settings endpoint reserved for HID-side diagnostic rules."})

    return bp
