from __future__ import annotations

import base64
import io
import json
import time
from datetime import datetime, timezone
from typing import Callable
from urllib.parse import urlencode

from flask import Blueprint, g, jsonify, render_template, request, send_file, session

from .bridge import AbdmBridgeService
from .callbacks import create_abdm_callbacks_blueprint
from .client import AbdmClient, AbdmError
from .config import load_settings
from .abha_v3 import AbhaV3Client
from .m1 import get_m1_checklist
from .log_store import log_abdm_event, recent_abdm_logs, timed_ms
from .m2 import AbdmM2Service, AbdmM2Store, get_m2_checklist
from .scan_share import AbdmScanShareService, AbdmScanShareStore
from .token_store import (
    get_abha_session,
    get_latest_abha_session,
    get_latest_abha_session_by_token_type,
    get_recent_abha_sessions,
    save_abha_session,
    update_abha_session_profile,
)


def register_abdm_routes(
    app,
    *,
    login_required,
    audit_log_event: Callable | None = None,
    local_tz=None,
):
    bp = Blueprint("abdm", __name__)

    def _audit(action: str, *, status: str = "success", summary: str = "", details=None):
        log_abdm_event(
            category="workflow",
            action=action,
            status=status,
            direction="internal",
            entity_type="abdm_bridge",
            entity_id=(load_settings().client_id or ""),
            summary=summary,
            request_payload=details,
            request_id=_request_id_from_headers(),
        )
        if not audit_log_event:
            return
        try:
            audit_log_event(
                "abdm",
                action,
                status=status,
                entity_type="abdm_bridge",
                entity_id=(load_settings().client_id or ""),
                summary=summary,
                details=details,
            )
        except Exception:
            pass

    @bp.before_request
    def _abdm_before_request():
        g.abdm_started_at = time.time()

    @bp.after_request
    def _abdm_after_request(response):
        if request.endpoint != "abdm.abdm_logs":
            log_abdm_event(
                category="route",
                action=request.path,
                status="success" if response.status_code < 400 else "error",
                direction="inbound",
                method=request.method,
                url=request.path,
                http_status=response.status_code,
                request_id=_request_id_from_headers(),
                request_payload=request.get_json(silent=True) if request.is_json else None,
                summary=f"ABDM route returned HTTP {response.status_code}.",
                duration_ms=timed_ms(getattr(g, "abdm_started_at", time.time())),
            )
        return response

    def _error_response(exc: Exception, status_code: int = 500):
        payload = {"status": "error", "message": str(exc)}
        if isinstance(exc, AbdmError):
            if exc.status_code:
                status_code = exc.status_code
            payload["provider_response"] = exc.response
            payload["attempts"] = exc.attempts
            recovery_hint = _abdm_recovery_hint(exc.response)
            if recovery_hint:
                payload["recovery_hint"] = recovery_hint
        return jsonify(payload), status_code

    @bp.route("/abdm")
    @login_required(allowed_roles={"IT"}, required_section="abdm")
    def dashboard_page():
        settings = load_settings()
        return render_template(
            "abdm_m1.html",
            settings=settings.redacted_summary(),
            checklist=get_m1_checklist(),
            m2_checklist=get_m2_checklist(),
            user=session.get("username") or "",
        )

    @bp.route("/api/abdm/status")
    @login_required(allowed_roles={"IT"}, required_section="abdm")
    def status():
        settings = load_settings()
        bridge_service = AbdmBridgeService(settings=settings)
        return jsonify(
            {
                "status": "ok",
                "configured": settings.configured,
                "settings": settings.redacted_summary(),
                "m1_checklist": get_m1_checklist(),
                "bridge_update": {
                    "method": "PATCH",
                    "url": settings.bridge_v3_url,
                    "payload": {"bridgeId": settings.client_id, "url": settings.bridge_url},
                },
                "service_update": {
                    "method": settings.bridge_service_method,
                    "url": settings.bridge_services_url,
                    "payload": [bridge_service.default_service_payload()],
                    "source": "ABDM onboarding email",
                },
                "service_lookup": {
                    "method": "GET",
                    "url": settings.bridge_get_services_url,
                },
                "m1_abha_v3": {
                    "session_url": settings.abha_session_url,
                    "base_url": settings.abha_base_url,
                    "phr_base_url": settings.abha_phr_base_url,
                    "public_certificate_url": settings.abha_base_url.rstrip("/") + "/v3/profile/public/certificate",
                },
                "m1_facility_hrp_registration": {
                    "method": "POST",
                    "url": settings.facility_base_url.rstrip("/") + "/v1/bridges/MutipleHRPAddUpdateServices",
                    "payload": bridge_service.facility_hrp_payload(),
                    "source": "Milestone 1 Postman Collection-18-08-2025",
                },
                "m2_checklist": get_m2_checklist(),
                "m2": AbdmM2Service(settings=settings).endpoint_manifest(),
            }
        )

    @bp.route("/api/abdm/m2/status")
    @login_required(allowed_roles={"IT"}, required_section="abdm")
    def m2_status():
        store = AbdmM2Store()
        scan_store = AbdmScanShareStore()
        return jsonify(
            {
                "status": "ok",
                "checklist": get_m2_checklist(),
                "manifest": AbdmM2Service(store=store).endpoint_manifest(),
                "care_context_count": len(store.list_care_contexts()),
                "recent_event_count": len(store.recent_events(25)),
                "scan_share_recent_count": len(scan_store.list_profiles(25)),
            }
        )

    @bp.route("/api/abdm/m2/events")
    @login_required(allowed_roles={"IT"}, required_section="abdm")
    def m2_events():
        limit = request.args.get("limit", "50")
        try:
            limit_value = max(1, min(200, int(limit)))
        except Exception:
            limit_value = 50
        return jsonify({"status": "success", "events": AbdmM2Store().recent_events(limit_value)})

    @bp.route("/api/abdm/logs")
    @login_required(allowed_roles={"IT"}, required_section="abdm")
    def abdm_logs():
        limit = request.args.get("limit", "100")
        try:
            limit_value = max(1, min(500, int(limit)))
        except Exception:
            limit_value = 100
        return jsonify({"status": "success", "logs": recent_abdm_logs(limit_value)})

    @bp.route("/api/abdm/m2/care-contexts", methods=["GET"])
    @login_required(allowed_roles={"IT"}, required_section="abdm")
    def m2_care_contexts():
        return jsonify({"status": "success", "care_contexts": AbdmM2Store().list_care_contexts()})

    @bp.route("/api/abdm/m2/care-contexts", methods=["POST"])
    @login_required(allowed_roles={"IT"}, required_section="abdm")
    def m2_save_care_context():
        data = request.get_json(silent=True) or {}
        if not isinstance(data, dict):
            return jsonify({"status": "error", "message": "JSON object is required."}), 400
        try:
            record = AbdmM2Store().save_care_context(data)
            _audit("m2_care_context_save", summary="ABDM M2 care context registered locally.", details=record)
            return jsonify({"status": "success", "care_context": record})
        except Exception as exc:
            _audit("m2_care_context_save", status="error", summary=str(exc))
            return _error_response(exc, 400)

    @bp.route("/api/abdm/m1/scan-share/profiles", methods=["GET"])
    @login_required(allowed_roles={"IT"}, required_section="abdm")
    def m1_scan_share_profiles():
        limit = request.args.get("limit", "50")
        try:
            limit_value = max(1, min(200, int(limit)))
        except Exception:
            limit_value = 50
        return jsonify({"status": "success", "profiles": AbdmScanShareStore().list_profiles(limit_value)})

    @bp.route("/api/abdm/m1/scan-share/qr", methods=["POST"])
    @login_required(allowed_roles={"IT"}, required_section="abdm")
    def m1_scan_share_qr():
        data = request.get_json(silent=True) or {}
        if not isinstance(data, dict):
            data = {}
        settings = load_settings()
        facility_or_hip_id = str(
            data.get("facility_id")
            or data.get("facilityId")
            or data.get("hip_id")
            or data.get("hipId")
            or settings.service_id
            or settings.facility_id
            or ""
        ).strip()
        counter_code = str(data.get("counter_code") or data.get("counterCode") or data.get("counter_id") or data.get("counterId") or "").strip()
        purpose = str(data.get("purpose") or "registration").strip().lower()
        if not facility_or_hip_id:
            return jsonify({"status": "error", "message": "Facility/HIP ID is required."}), 400
        if not counter_code:
            return jsonify({"status": "error", "message": "Counter code is required."}), 400
        if not counter_code.isalnum():
            return jsonify({"status": "error", "message": "Counter code must be alphanumeric only, for example REG01 or 12345."}), 400
        if purpose not in {"registration", "opd", "billing", "pharmacy", "diagnostics"}:
            purpose = "registration"
        qr = _scan_share_qr_payload(settings, facility_or_hip_id=facility_or_hip_id, counter_code=counter_code, purpose=purpose)
        _audit("m1_scan_share_qr_metadata", summary="ABDM Scan & Share HSPR QR metadata prepared.", details=qr)
        return jsonify(
            {
                "status": "success",
                "mode": "hspr_official_qr_required",
                "message": "Use the official QR generated from HSPR/NHPR Manage QR. HID is ready to receive the callback.",
                "hspr_url": f"https://hspsbx.abdm.gov.in/nhpr/v4/software-linkage/{facility_or_hip_id}",
                "counter": qr,
            }
        )

    @bp.route("/api/abdm/m1/scan-share/link", methods=["POST"])
    @login_required(allowed_roles={"IT"}, required_section="abdm")
    def m1_scan_share_link():
        data = request.get_json(silent=True) or {}
        if not isinstance(data, dict):
            return jsonify({"status": "error", "message": "JSON object is required."}), 400
        try:
            result = AbdmScanShareStore().link_profile(data)
            _audit("m1_scan_share_link", summary="ABDM Scan & Share profile linked to patient registration.", details=result)
            return jsonify(result)
        except Exception as exc:
            _audit("m1_scan_share_link", status="error", summary=str(exc), details=data)
            return _error_response(exc, 400)

    @bp.route("/api/abdm/m2/discover/test", methods=["POST"])
    @login_required(allowed_roles={"IT"}, required_section="abdm")
    def m2_discover_test():
        data = request.get_json(silent=True) or {}
        if not isinstance(data, dict):
            data = {}
        patient = data.get("patient") if isinstance(data.get("patient"), dict) else {}
        if not patient:
            patient = {
                "id": str(data.get("abha_address") or "demo@sbx"),
                "verifiedIdentifiers": [
                    {"type": "MOBILE", "value": str(data.get("mobile") or "9999999999")},
                    {"type": "MR", "value": str(data.get("mr") or "DEMO-MR-001")},
                ],
            }
        payload = {
            "transactionId": str(data.get("transactionId") or "sandbox-" + (session.get("username") or "user")),
            "patient": patient,
        }
        result = AbdmM2Service(store=AbdmM2Store()).discover(payload)
        return jsonify({"status": "success", "result": result, "request": payload})

    @bp.route("/api/abdm/session/test", methods=["POST"])
    @login_required(allowed_roles={"IT"}, required_section="abdm")
    def session_test():
        try:
            token = AbdmClient().get_session_token(force_refresh=True)
            _audit("session_test", summary="ABDM gateway session token generated.")
            return jsonify(
                {
                    "status": "success",
                    "message": "ABDM gateway session token generated.",
                    "token_preview": f"{token[:8]}...{token[-6:]}" if len(token) > 16 else "***",
                }
            )
        except Exception as exc:
            _audit("session_test", status="error", summary=str(exc))
            return _error_response(exc)

    @bp.route("/api/abdm/m1/session/test", methods=["POST"])
    @login_required(allowed_roles={"IT"}, required_section="abdm")
    def m1_session_test():
        try:
            token = AbhaV3Client().get_session_token(force_refresh=True)
            _audit("m1_session_test", summary="ABDM ABHA V3 session token generated.")
            return jsonify(
                {
                    "status": "success",
                    "message": "ABDM ABHA V3 session token generated.",
                    "token_preview": f"{token[:8]}...{token[-6:]}" if len(token) > 16 else "***",
                }
            )
        except Exception as exc:
            _audit("m1_session_test", status="error", summary=str(exc))
            return _error_response(exc)

    @bp.route("/api/abdm/m1/public-certificate")
    @login_required(allowed_roles={"IT"}, required_section="abdm")
    def m1_public_certificate():
        try:
            result = AbhaV3Client().get_public_certificate()
            _audit("m1_public_certificate", summary="ABHA public certificate fetched.")
            data = result.get("data")
            preview = data
            if isinstance(data, str) and len(data) > 160:
                preview = f"{data[:80]}...{data[-40:]}"
            elif isinstance(data, dict):
                preview = _preview_large_strings(data)
            return jsonify({"status": "success", "result": {**result, "data": preview}})
        except Exception as exc:
            _audit("m1_public_certificate", status="error", summary=str(exc))
            return _error_response(exc)

    @bp.route("/api/abdm/m1/encrypt/test", methods=["POST"])
    @login_required(allowed_roles={"IT"}, required_section="abdm")
    def m1_encrypt_test():
        data = request.get_json(silent=True) or {}
        value = str(data.get("value") or "123456789012").strip()
        try:
            encrypted = AbhaV3Client().encrypt_value(value)
            _audit("m1_encrypt_test", summary="ABHA encryption test completed.")
            return jsonify(
                {
                    "status": "success",
                    "message": "ABHA encryption test completed.",
                    "encrypted_preview": f"{encrypted[:18]}...{encrypted[-10:]}" if len(encrypted) > 32 else encrypted,
                }
            )
        except Exception as exc:
            _audit("m1_encrypt_test", status="error", summary=str(exc))
            return _error_response(exc, 501)

    @bp.route("/api/abdm/m1/enrollment/aadhaar/request-otp", methods=["POST"])
    @login_required(allowed_roles={"IT"}, required_section="abdm")
    def m1_request_aadhaar_enrollment_otp():
        data = request.get_json(silent=True) or {}
        aadhaar_number = str(data.get("aadhaar") or data.get("aadhaar_number") or "").strip()
        if not aadhaar_number:
            return jsonify({"status": "error", "message": "aadhaar is required."}), 400
        try:
            result = AbhaV3Client().request_aadhaar_enrollment_otp(aadhaar_number, txn_id=str(data.get("txnId") or ""))
            _audit("m1_aadhaar_enrollment_otp", summary="ABHA Aadhaar enrollment OTP requested.")
            return jsonify({"status": "success", "result": _redact_sensitive_response(result)})
        except Exception as exc:
            _audit("m1_aadhaar_enrollment_otp", status="error", summary=str(exc))
            return _error_response(exc)

    @bp.route("/api/abdm/m1/enrollment/aadhaar/verify-otp", methods=["POST"])
    @login_required(allowed_roles={"IT"}, required_section="abdm")
    def m1_verify_aadhaar_enrollment_otp():
        data = request.get_json(silent=True) or {}
        txn_id = str(data.get("txnId") or data.get("txn_id") or "").strip()
        otp_value = str(data.get("otp") or data.get("otpValue") or "").strip()
        mobile_value = str(data.get("mobile") or "").strip()
        if not txn_id or not otp_value:
            return jsonify({"status": "error", "message": "txnId and otp are required."}), 400
        if not mobile_value:
            return jsonify({"status": "error", "message": "mobile is required for ABHA enrollment."}), 400
        try:
            result = AbhaV3Client().enrol_by_aadhaar_otp(txn_id, otp_value, mobile=mobile_value)
            _audit("m1_aadhaar_enrollment_verify", summary="ABHA Aadhaar enrollment OTP verified.")
            return jsonify({"status": "success", "result": _redact_sensitive_response(result)})
        except Exception as exc:
            _audit("m1_aadhaar_enrollment_verify", status="error", summary=str(exc))
            return _error_response(exc)

    @bp.route("/api/abdm/m1/abha-number/request-otp", methods=["POST"])
    @login_required(allowed_roles={"IT"}, required_section="abdm")
    def m1_request_abha_number_otp():
        data = request.get_json(silent=True) or {}
        abha_number = _digits_only(data.get("abha_number") or data.get("abhaNumber") or "")
        otp_system = str(data.get("otpSystem") or data.get("otp_system") or "aadhaar").strip().lower()
        auth_method = str(data.get("auth_method") or data.get("authMethod") or otp_system or "aadhaar").strip().lower()
        if not abha_number:
            return jsonify({"status": "error", "message": "abha_number is required."}), 400
        if len(abha_number) != 14:
            return jsonify({"status": "error", "message": "ABHA number must be 14 digits."}), 400
        try:
            result = AbhaV3Client().request_abha_number_verification_otp(abha_number, otp_system=otp_system, auth_method=auth_method)
            _audit("m1_abha_number_otp", summary="ABHA number verification OTP requested.")
            return jsonify({"status": "success", "result": _redact_sensitive_response(result)})
        except Exception as exc:
            _audit("m1_abha_number_otp", status="error", summary=str(exc))
            return _error_response(exc)

    @bp.route("/api/abdm/m1/abha-number/verify-otp", methods=["POST"])
    @login_required(allowed_roles={"IT"}, required_section="abdm")
    def m1_verify_abha_number_otp():
        data = request.get_json(silent=True) or {}
        txn_id = str(data.get("txnId") or data.get("txn_id") or "").strip()
        otp_value = str(data.get("otp") or data.get("otpValue") or "").strip()
        auth_method = str(data.get("auth_method") or data.get("authMethod") or "aadhaar").strip().lower()
        login_endpoint = str(data.get("login_endpoint") or data.get("loginEndpoint") or "").strip().lower()
        if not txn_id or not otp_value:
            return jsonify({"status": "error", "message": "txnId and otp are required."}), 400
        try:
            result = AbhaV3Client().verify_abha_number_otp(txn_id, otp_value, auth_method=auth_method, login_endpoint=login_endpoint)
            session_ref = _store_abha_login_result(result)
            _audit("m1_abha_number_verify", summary="ABHA number OTP verified.")
            return jsonify({"status": "success", "session_ref": session_ref, "result": _redact_sensitive_response(result)})
        except ValueError as exc:
            _audit("m1_abha_number_verify", status="error", summary=str(exc))
            return _error_response(exc, 502)
        except Exception as exc:
            _audit("m1_abha_number_verify", status="error", summary=str(exc))
            return _error_response(exc)

    @bp.route("/api/abdm/m1/existing-abha/request-otp", methods=["POST"])
    @login_required(allowed_roles={"IT"}, required_section="abdm")
    def m1_existing_abha_request_otp():
        data = request.get_json(silent=True) or {}
        abha_number = _digits_only(data.get("abha_number") or data.get("abhaNumber") or "")
        otp_system = str(data.get("otpSystem") or data.get("otp_system") or "abdm").strip().lower()
        auth_method = str(data.get("auth_method") or data.get("authMethod") or otp_system or "abdm").strip().lower()
        if not abha_number:
            return jsonify({"status": "error", "message": "abha_number is required."}), 400
        if len(abha_number) != 14:
            return jsonify({"status": "error", "message": "ABHA number must be 14 digits."}), 400
        try:
            result = AbhaV3Client().request_abha_number_verification_otp(
                abha_number,
                otp_system=otp_system,
                auth_method=auth_method,
                prefer_profile=True,
            )
            _audit("m1_existing_abha_otp", summary="Existing ABHA login OTP requested.")
            return jsonify({"status": "success", "result": _redact_sensitive_response(result)})
        except Exception as exc:
            _audit("m1_existing_abha_otp", status="error", summary=str(exc))
            return _error_response(exc)

    @bp.route("/api/abdm/m1/existing-abha/verify-otp", methods=["POST"])
    @login_required(allowed_roles={"IT"}, required_section="abdm")
    def m1_existing_abha_verify_otp():
        data = request.get_json(silent=True) or {}
        txn_id = str(data.get("txnId") or data.get("txn_id") or "").strip()
        otp_value = str(data.get("otp") or data.get("otpValue") or "").strip()
        auth_method = str(data.get("auth_method") or data.get("authMethod") or "abdm").strip().lower()
        login_endpoint = str(data.get("login_endpoint") or data.get("loginEndpoint") or "").strip().lower()
        if not txn_id or not otp_value:
            return jsonify({"status": "error", "message": "txnId and otp are required."}), 400
        try:
            result = AbhaV3Client().verify_abha_number_otp(txn_id, otp_value, auth_method=auth_method, login_endpoint=login_endpoint)
            session_ref = _store_abha_login_result(result)
            _audit("m1_existing_abha_verify", summary="Existing ABHA login OTP verified.")
            return jsonify({"status": "success", "session_ref": session_ref, "result": _redact_sensitive_response(result)})
        except ValueError as exc:
            _audit("m1_existing_abha_verify", status="error", summary=str(exc))
            return _error_response(exc, 502)
        except Exception as exc:
            _audit("m1_existing_abha_verify", status="error", summary=str(exc))
            return _error_response(exc)

    @bp.route("/api/abdm/m1/abha/profile", methods=["GET", "POST"])
    @login_required(allowed_roles={"IT"}, required_section="abdm")
    def m1_abha_profile():
        stored = None
        try:
            stored = _current_abha_session()
            result = AbhaV3Client().get_abha_profile(stored["token"])
            data = result.get("data") if isinstance(result, dict) else {}
            if isinstance(data, dict):
                update_abha_session_profile(stored["session_ref"], data)
            _audit("m1_abha_profile_fetch", summary="ABHA profile fetched.")
            return jsonify({"status": "success", "session_ref": stored["session_ref"], "result": _redact_sensitive_response(result)})
        except ValueError as exc:
            _audit("m1_abha_profile_fetch", status="error", summary=str(exc))
            return _error_response(exc, 401)
        except AbdmError as exc:
            if _is_invalid_x_token(exc.response) and stored and stored.get("profile"):
                _audit(
                    "m1_abha_profile_fetch",
                    status="warning",
                    summary="ABHA profile endpoint rejected transfer token; returned verified login snapshot.",
                )
                return jsonify(
                    {
                        "status": "success",
                        "session_ref": stored["session_ref"],
                        "source": "login_verify_snapshot",
                        "message": (
                            "ABDM returned a transfer token for ABHA-number login. "
                            "Profile endpoint rejected it, so HID is showing the verified login snapshot."
                        ),
                        "result": {
                            "status_code": exc.status_code or 400,
                            "data": _redact_sensitive_response(stored.get("profile") or {}),
                            "profile_fetch_error": _redact_sensitive_response(exc.response),
                        },
                    }
                )
            _audit("m1_abha_profile_fetch", status="error", summary=str(exc))
            return _error_response(exc)
        except Exception as exc:
            _audit("m1_abha_profile_fetch", status="error", summary=str(exc))
            return _error_response(exc)

    @bp.route("/api/abdm/m1/abha/card", methods=["GET", "POST"])
    @login_required(allowed_roles={"IT"}, required_section="abdm")
    def m1_abha_card():
        try:
            stored = _current_abha_session()
            card_session = _latest_matching_card_session(stored)
            if card_session:
                stored = card_session
                session["abdm_abha_session_ref"] = stored["session_ref"]
            elif _is_transfer_token(stored.get("token")):
                upgraded_from_transfer = _upgrade_from_latest_transfer_session(stored)
                if upgraded_from_transfer:
                    stored = upgraded_from_transfer
                    session["abdm_abha_session_ref"] = stored["session_ref"]
            if not _is_card_capable_session(stored):
                return jsonify(
                    {
                        "status": "error",
                        "message": "ABHA card download needs a profile-session X-token.",
                        "recovery_hint": "Use Existing ABHA Login and download from that verified profile-login session. Avoid the ABHA-address OTP session for card download.",
                    }
                ), 409
            try:
                content, content_type = AbhaV3Client().get_abha_card(stored["token"])
            except AbdmError as exc:
                if _is_x_token_expired(exc.response):
                    return jsonify(
                        {
                            "status": "error",
                            "message": "ABDM rejected the ABHA profile token for card download.",
                            "provider_response": _redact_sensitive_response(exc.response),
                            "recovery_hint": "Generate a fresh Existing ABHA OTP, verify it, then download the card with the newly stored profile session.",
                        }
                    ), 401
                raise
            _audit("m1_abha_card_download", summary="ABHA card downloaded.")
            extension = _download_extension(content_type)
            safe_name_part = _safe_download_name_part(stored.get("abha_number") or stored.get("session_ref") or "abha")
            download_name = f"abha-card-{safe_name_part}.{extension}"
            return send_file(
                io.BytesIO(content),
                mimetype=(content_type or "application/octet-stream").split(";", 1)[0],
                as_attachment=True,
                download_name=download_name,
            )
        except ValueError as exc:
            _audit("m1_abha_card_download", status="error", summary=str(exc))
            return _error_response(exc, 401)
        except Exception as exc:
            _audit("m1_abha_card_download", status="error", summary=str(exc))
            return _error_response(exc)

    @bp.route("/api/abdm/m1/abha/profile", methods=["PATCH"])
    @login_required(allowed_roles={"IT"}, required_section="abdm")
    def m1_abha_profile_update():
        data = request.get_json(silent=True) or {}
        if not isinstance(data, dict):
            return jsonify({"status": "error", "message": "JSON object is required."}), 400
        try:
            stored = _current_abha_session()
            profile_login_session = _latest_matching_card_session(stored)
            if profile_login_session:
                stored = profile_login_session
                session["abdm_abha_session_ref"] = stored["session_ref"]
            elif _is_transfer_token(stored.get("token")):
                upgraded_from_transfer = _upgrade_from_latest_transfer_session(stored)
                if upgraded_from_transfer:
                    stored = upgraded_from_transfer
                    session["abdm_abha_session_ref"] = stored["session_ref"]
            if not _is_card_capable_session(stored):
                return jsonify(
                    {
                        "status": "error",
                        "message": "ABHA profile update needs a profile-login X-token.",
                        "recovery_hint": "Use Existing ABHA Login first, then update the profile from that verified profile session.",
                    }
                ), 409
            payload = _abha_profile_update_payload(data, stored)
            if not payload:
                return jsonify({"status": "error", "message": "No supported profile fields were supplied."}), 400
            result = AbhaV3Client().update_abha_profile(stored["token"], payload)
            response_data = result.get("data") if isinstance(result, dict) else {}
            if isinstance(response_data, dict):
                update_abha_session_profile(stored["session_ref"], response_data)
            _audit("m1_abha_profile_update", summary="ABHA profile update requested.", details={k: bool(v) for k, v in payload.items()})
            return jsonify({"status": "success", "session_ref": stored["session_ref"], "result": _redact_sensitive_response(result)})
        except ValueError as exc:
            _audit("m1_abha_profile_update", status="error", summary=str(exc))
            return _error_response(exc, 401)
        except Exception as exc:
            _audit("m1_abha_profile_update", status="error", summary=str(exc))
            return _error_response(exc)

    @bp.route("/api/abdm/m1/abha/profile/mobile/request-otp", methods=["POST"])
    @login_required(allowed_roles={"IT"}, required_section="abdm")
    def m1_abha_profile_mobile_request_otp():
        data = request.get_json(silent=True) or {}
        mobile = _digits_only(data.get("mobile") or "")
        if len(mobile) != 10:
            return jsonify({"status": "error", "message": "Mobile number must be 10 digits."}), 400
        try:
            stored = _profile_login_session_for_action()
            result = AbhaV3Client().request_mobile_update_otp(stored["token"], mobile)
            _audit("m1_abha_profile_mobile_otp", summary="ABHA profile mobile update OTP requested.")
            return jsonify({"status": "success", "session_ref": stored["session_ref"], "result": _redact_sensitive_response(result)})
        except ValueError as exc:
            _audit("m1_abha_profile_mobile_otp", status="error", summary=str(exc))
            return _error_response(exc, 401)
        except Exception as exc:
            _audit("m1_abha_profile_mobile_otp", status="error", summary=str(exc))
            return _error_response(exc)

    @bp.route("/api/abdm/m1/abha/profile/mobile/verify-otp", methods=["POST"])
    @login_required(allowed_roles={"IT"}, required_section="abdm")
    def m1_abha_profile_mobile_verify_otp():
        data = request.get_json(silent=True) or {}
        txn_id = str(data.get("txnId") or data.get("txn_id") or "").strip()
        otp_value = str(data.get("otp") or data.get("otpValue") or "").strip()
        if not txn_id or not otp_value:
            return jsonify({"status": "error", "message": "txnId and otp are required."}), 400
        try:
            stored = _profile_login_session_for_action()
            result = AbhaV3Client().verify_mobile_update_otp(stored["token"], txn_id, otp_value)
            response_data = result.get("data") if isinstance(result, dict) else {}
            if isinstance(response_data, dict):
                update_abha_session_profile(stored["session_ref"], response_data)
            _audit("m1_abha_profile_mobile_verify", summary="ABHA profile mobile update OTP verified.")
            return jsonify({"status": "success", "session_ref": stored["session_ref"], "result": _redact_sensitive_response(result)})
        except ValueError as exc:
            _audit("m1_abha_profile_mobile_verify", status="error", summary=str(exc))
            return _error_response(exc, 401)
        except Exception as exc:
            _audit("m1_abha_profile_mobile_verify", status="error", summary=str(exc))
            return _error_response(exc)

    @bp.route("/api/abdm/m1/abha-address/request-otp", methods=["POST"])
    @login_required(allowed_roles={"IT"}, required_section="abdm")
    def m1_request_abha_address_otp():
        data = request.get_json(silent=True) or {}
        abha_address = str(data.get("abha_address") or data.get("abhaAddress") or "").strip()
        if not abha_address:
            return jsonify({"status": "error", "message": "abha_address is required."}), 400
        try:
            result = AbhaV3Client().request_abha_address_otp(abha_address)
            _audit("m1_abha_address_otp", summary="ABHA address OTP requested.")
            return jsonify({"status": "success", "result": _redact_sensitive_response(result)})
        except Exception as exc:
            _audit("m1_abha_address_otp", status="error", summary=str(exc))
            return _error_response(exc)

    @bp.route("/api/abdm/m1/abha-address/verify-otp", methods=["POST"])
    @login_required(allowed_roles={"IT"}, required_section="abdm")
    def m1_verify_abha_address_otp():
        data = request.get_json(silent=True) or {}
        txn_id = str(data.get("txnId") or data.get("txn_id") or "").strip()
        otp_value = str(data.get("otp") or data.get("otpValue") or "").strip()
        if not txn_id or not otp_value:
            return jsonify({"status": "error", "message": "txnId and otp are required."}), 400
        try:
            result = AbhaV3Client().verify_abha_address_otp(txn_id, otp_value)
            session_ref = ""
            try:
                session_ref = _store_abha_login_result(result)
            except ValueError:
                session_ref = ""
            _audit("m1_abha_address_verify", summary="ABHA address OTP verified.")
            return jsonify({"status": "success", "session_ref": session_ref, "result": _redact_sensitive_response(result)})
        except Exception as exc:
            _audit("m1_abha_address_verify", status="error", summary=str(exc))
            return _error_response(exc)

    @bp.route("/api/abdm/bridge/url", methods=["PATCH", "POST"])
    @login_required(allowed_roles={"IT"}, required_section="abdm")
    def update_bridge_url():
        data = request.get_json(silent=True) or {}
        bridge_url = (data.get("url") or "").strip()
        try:
            result = AbdmBridgeService().update_bridge_url(bridge_url or None)
            _audit("bridge_url_update", summary="ABDM bridge URL updated.", details={"url": bridge_url or load_settings().bridge_url})
            return jsonify({"status": "success", "result": result})
        except ValueError as exc:
            _audit("bridge_url_update", status="error", summary=str(exc))
            return _error_response(exc, 400)
        except Exception as exc:
            _audit("bridge_url_update", status="error", summary=str(exc))
            return _error_response(exc)

    @bp.route("/api/abdm/bridge/services", methods=["GET"])
    @login_required(allowed_roles={"IT"}, required_section="abdm")
    def get_services():
        try:
            result = AbdmBridgeService().get_services()
            _audit("bridge_services_get", summary="ABDM bridge services fetched.")
            return jsonify({"status": "success", "result": result})
        except Exception as exc:
            _audit("bridge_services_get", status="error", summary=str(exc))
            return _error_response(exc)

    @bp.route("/api/abdm/bridge/services", methods=["POST"])
    @login_required(allowed_roles={"IT"}, required_section="abdm")
    def add_update_services():
        data = request.get_json(silent=True) or {}
        overrides = data.get("service") if isinstance(data.get("service"), dict) else {}
        try:
            result = AbdmBridgeService().add_or_update_default_service(overrides)
            _audit("bridge_services_update", summary="ABDM bridge service added or updated.", details=overrides)
            return jsonify({"status": "success", "result": result})
        except Exception as exc:
            _audit("bridge_services_update", status="error", summary=str(exc))
            return _error_response(exc)

    @bp.route("/api/abdm/m1/facility/hrp", methods=["POST"])
    @login_required(allowed_roles={"IT"}, required_section="abdm")
    def register_facility_hrp():
        data = request.get_json(silent=True) or {}
        overrides = data.get("facility") if isinstance(data.get("facility"), dict) else {}
        try:
            result = AbdmBridgeService().register_facility_hrp(overrides)
            _audit("m1_facility_hrp_register", summary="ABDM M1 facility HRP registered.", details=overrides)
            return jsonify({"status": "success", "result": result})
        except ValueError as exc:
            _audit("m1_facility_hrp_register", status="error", summary=str(exc))
            return _error_response(exc, 400)
        except Exception as exc:
            _audit("m1_facility_hrp_register", status="error", summary=str(exc))
            return _error_response(exc)

    app.register_blueprint(bp)
    app.register_blueprint(create_abdm_callbacks_blueprint(audit_log_event=audit_log_event))


def _preview_large_strings(value):
    if isinstance(value, dict):
        return {k: _preview_large_strings(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_preview_large_strings(v) for v in value]
    if isinstance(value, str) and len(value) > 160:
        return f"{value[:80]}...{value[-40:]}"
    return value


def _abdm_recovery_hint(response):
    if not isinstance(response, dict):
        return ""
    error = response.get("error")
    if not isinstance(error, dict):
        return ""
    code = str(error.get("code") or "").strip().upper()
    message = str(error.get("message") or "")
    if code == "ABDM-1204" or "OTP match is exceeded" in message or "OTP is not generated" in message:
        return (
            "Generate a fresh Aadhaar OTP and use the new transaction ID. "
            "Do not retry the same OTP/txnId after this error."
        )
    return ""


def _request_id_from_headers() -> str:
    return (
        request.headers.get("REQUEST-ID")
        or request.headers.get("X-Request-ID")
        or request.headers.get("X-Correlation-ID")
        or ""
    )


def _digits_only(value) -> str:
    return "".join(ch for ch in str(value or "") if ch.isdigit())


def _store_abha_login_result(result: dict) -> str:
    data = result.get("data") if isinstance(result, dict) else {}
    if not isinstance(data, dict):
        raise ValueError("ABHA login response did not include profile data.")
    token = _extract_login_token(data)
    if not token:
        raise ValueError(f"ABHA login did not return an X-token. Response keys: {', '.join(sorted(map(str, data.keys())))}")
    profile = _extract_abha_login_profile(data)
    login_endpoint = str(result.get("abha_login_endpoint_used") or data.get("abha_login_endpoint_used") or "").strip()
    if login_endpoint:
        profile["_abha_login_endpoint_used"] = login_endpoint
    token_container = data.get("tokens") if isinstance(data.get("tokens"), dict) else {}
    session_ref = save_abha_session(
        username=session.get("username") or "",
        token=token,
        refresh_token=str(
            data.get("refreshToken")
            or data.get("refresh_token")
            or token_container.get("refreshToken")
            or token_container.get("refresh_token")
            or ""
        ),
        expires_in=data.get("expiresIn") or data.get("expires_in") or token_container.get("expiresIn") or token_container.get("expires_in"),
        profile=profile,
    )
    session["abdm_abha_session_ref"] = session_ref
    return session_ref


def _extract_login_token(data: dict) -> str:
    candidates = [
        data.get("token"),
        data.get("xToken"),
        data.get("x-token"),
        data.get("X-Token"),
        data.get("accessToken"),
        data.get("access_token"),
    ]
    token_container = data.get("tokens")
    if isinstance(token_container, dict):
        candidates.extend(
            [
                token_container.get("token"),
                token_container.get("xToken"),
                token_container.get("x-token"),
                token_container.get("X-Token"),
                token_container.get("accessToken"),
                token_container.get("access_token"),
            ]
        )
    for value in candidates:
        token = str(value or "").strip()
        if token:
            return token
    return ""


def _current_abha_session() -> dict:
    data = request.get_json(silent=True) if request.method in {"POST", "PATCH"} else {}
    if not isinstance(data, dict):
        data = {}
    session_ref = str(data.get("session_ref") or request.args.get("session_ref") or session.get("abdm_abha_session_ref") or "").strip()
    return get_abha_session(session_ref, username=session.get("username") or "")


def _profile_login_session_for_action() -> dict:
    stored = _current_abha_session()
    profile_login_session = _latest_matching_card_session(stored)
    if profile_login_session:
        stored = profile_login_session
        session["abdm_abha_session_ref"] = stored["session_ref"]
    elif _is_transfer_token(stored.get("token")):
        upgraded_from_transfer = _upgrade_from_latest_transfer_session(stored)
        if upgraded_from_transfer:
            stored = upgraded_from_transfer
            session["abdm_abha_session_ref"] = stored["session_ref"]
    if not _is_card_capable_session(stored):
        raise ValueError("ABHA profile action needs a verified profile-login X-token. Use Existing ABHA Login first.")
    return stored


def _profile_session_for(stored: dict) -> dict:
    try:
        alternate = get_latest_abha_session(
            username=session.get("username") or "",
            abha_number=stored.get("abha_number") or "",
            abha_address=stored.get("abha_address") or "",
            exclude_session_ref=stored.get("session_ref") or "",
        )
        if not _is_transfer_token(alternate.get("token")):
            return alternate
    except Exception:
        return {}
    return {}


def _latest_matching_profile_session(stored: dict) -> dict:
    try:
        latest = get_latest_abha_session(
            username=session.get("username") or "",
            abha_number=stored.get("abha_number") or "",
            abha_address=stored.get("abha_address") or "",
        )
        if latest.get("session_ref") != stored.get("session_ref") and not _is_transfer_token(latest.get("token")):
            return latest
    except Exception:
        return {}
    return {}


def _latest_matching_card_session(stored: dict) -> dict:
    if _is_card_capable_session(stored):
        return stored
    try:
        for candidate in get_recent_abha_sessions(
            username=session.get("username") or "",
            abha_number=stored.get("abha_number") or "",
            abha_address=stored.get("abha_address") or "",
            limit=20,
        ):
            if _is_card_capable_session(candidate):
                return candidate
    except Exception:
        return {}
    return {}


def _upgrade_profile_session_if_needed(stored: dict) -> dict:
    if _jwt_type(stored.get("token")) != "transaction":
        return {}
    claims = _jwt_claims(stored.get("token"))
    txn_id = str(claims.get("txnId") or claims.get("transactionId") or "").strip()
    abha_address = str(stored.get("abha_address") or claims.get("abhaAddress") or claims.get("phrAddress") or claims.get("sub") or "").strip()
    if not txn_id or not abha_address:
        return {}
    try:
        result = AbhaV3Client().verify_switch_profile(stored["token"], abha_address=abha_address, txn_id=txn_id)
        session_ref = _store_abha_login_result(result)
        return get_abha_session(session_ref, username=session.get("username") or "")
    except Exception as exc:
        log_abdm_event(
            category="workflow",
            action="m1_abha_switch_profile_verify",
            status="error",
            direction="internal",
            entity_type="abha_session",
            entity_id=stored.get("session_ref") or "",
            summary=str(exc),
            request_payload={"abha_address": abha_address, "txn_id": txn_id},
        )
        return {}


def _upgrade_from_latest_transfer_session(stored: dict) -> dict:
    try:
        transfer = get_latest_abha_session_by_token_type(
            token_type="transfer",
            username=session.get("username") or "",
            abha_number=stored.get("abha_number") or "",
            abha_address=stored.get("abha_address") or "",
        )
    except Exception:
        return {}
    try:
        claims = _jwt_claims(transfer.get("token"))
        txn_id = str(claims.get("txnId") or claims.get("transactionId") or "").strip()
        abha_number = str(transfer.get("abha_number") or claims.get("sub") or claims.get("healthIdNumber") or "").strip()
        result = AbhaV3Client().verify_abha_number_user(transfer["token"], abha_number=abha_number, txn_id=txn_id)
        session_ref = _store_abha_login_result(result)
        return get_abha_session(session_ref, username=session.get("username") or "")
    except Exception as exc:
        log_abdm_event(
            category="workflow",
            action="m1_abha_number_verify_user",
            status="error",
            direction="internal",
            entity_type="abha_session",
            entity_id=transfer.get("session_ref") or "",
            summary=str(exc),
            request_payload={"abha_number": transfer.get("abha_number") or "", "abha_address": transfer.get("abha_address") or ""},
        )
        return {}


def _extract_abha_login_profile(data: dict) -> dict:
    source = None
    for key in ("accounts", "users", "profiles"):
        value = data.get(key)
        if isinstance(value, list) and value and isinstance(value[0], dict):
            source = value[0]
            break
    if not source and isinstance(data.get("profile"), dict):
        source = data.get("profile")
    if isinstance(source, dict):
        profile = dict(source)
    else:
        profile = {}
    for key, value in data.items():
        if key in {"token", "xToken", "x-token", "X-Token", "accessToken", "access_token", "refreshToken", "refresh_token", "tokens", "accounts", "users", "profiles"}:
            continue
        if key not in profile:
            profile[key] = value
    return profile


def _abha_profile_update_payload(data: dict, stored: dict) -> dict:
    allowed = {"abhaNumber", "abha_number", "mobile", "email", "accountStatus", "account_status", "profilePhoto", "profile_photo"}
    payload = {}
    for key, value in data.items():
        if key not in allowed or value in (None, ""):
            continue
        if key == "abha_number":
            payload["abhaNumber"] = value
        elif key == "account_status":
            payload["accountStatus"] = value
        elif key == "profile_photo":
            payload["profilePhoto"] = value
        elif key == "mobile":
            mobile = _digits_only(value)
            if mobile and len(mobile) != 10:
                raise ValueError("Mobile number must be 10 digits.")
            if mobile:
                payload["mobile"] = mobile
        elif key == "email":
            email = str(value or "").strip()
            if email and ("@" not in email or "." not in email.rsplit("@", 1)[-1]):
                raise ValueError("Enter a valid email address.")
            if email:
                payload["email"] = email
        else:
            payload[key] = value
    return payload


def _scan_share_qr_payload(settings, *, facility_or_hip_id: str, counter_code: str, purpose: str) -> dict:
    bridge_url = (settings.bridge_url or "").rstrip("/")
    callback_url = bridge_url + "/api/abdm/callback/registration" if bridge_url else "/api/abdm/callback/registration"
    phr_host = "phr.abdm.gov.in" if str(settings.env or "").lower() in {"production", "prod", "live"} else "phrsbx.abdm.gov.in"
    qr_url = "https://" + phr_host + "/share-profile?" + urlencode({"hipid": facility_or_hip_id, "counterid": counter_code})
    payload = {
        "version": "ABDM-SCAN-SHARE-QR",
        "hipId": settings.service_id or facility_or_hip_id,
        "facilityId": settings.facility_id or facility_or_hip_id,
        "facilityName": settings.facility_name,
        "counterCode": counter_code,
        "purpose": purpose,
        "qrUrl": qr_url,
        "callbackUrl": callback_url,
        "bridgeUrl": bridge_url,
        "generatedAt": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
    }
    return {
        **payload,
        "display": f"{settings.facility_name or 'Facility'} | {counter_code} | {purpose.title()}",
        "qr_text": qr_url,
    }


def _download_extension(content_type: str) -> str:
    text = str(content_type or "").lower()
    if "pdf" in text:
        return "pdf"
    if "png" in text:
        return "png"
    if "jpeg" in text or "jpg" in text:
        return "jpg"
    return "bin"


def _safe_download_name_part(value: str) -> str:
    text = str(value or "").strip()
    safe = "".join(ch for ch in text if ch.isalnum() or ch in {"-", "_"}).strip("-_")
    return safe[:80] or "abha"


def _is_invalid_x_token(response) -> bool:
    text = json.dumps(response, ensure_ascii=True, default=str).lower()
    return "invalid x-token" in text or "invalid x token" in text or "abdm-1006" in text and "x-token" in text


def _is_x_token_expired(response) -> bool:
    text = json.dumps(response, ensure_ascii=True, default=str).lower()
    return "x-token expired" in text or ("abdm-1094" in text and "x-token" in text)


def _is_transfer_token(token: str) -> bool:
    return _jwt_type(token) == "transfer"


def _is_invalid_for_profile_token(token: str) -> bool:
    return _jwt_type(token) in {"transfer"}


def _is_invalid_for_card_token(token: str) -> bool:
    return _jwt_type(token) in {"transfer", "transaction"}


def _is_card_capable_session(stored: dict) -> bool:
    if not isinstance(stored, dict) or not stored.get("token"):
        return False
    claims = _jwt_claims(stored.get("token"))
    client_id = str(claims.get("clientId") or "").strip().lower()
    if client_id == "abha-profile-app-api":
        return True
    profile = stored.get("profile") if isinstance(stored.get("profile"), dict) else {}
    endpoint = str(profile.get("_abha_login_endpoint_used") or "").strip().lower()
    return endpoint == "profile"


def _jwt_type(token: str) -> str:
    claims = _jwt_claims(token)
    return str(claims.get("typ") or claims.get("type") or "").strip().lower()


def _jwt_claims(token: str) -> dict:
    text = str(token or "").strip()
    if text.lower().startswith("bearer "):
        text = text[7:].strip()
    parts = text.split(".")
    if len(parts) != 3:
        return {}
    try:
        payload = parts[1] + "=" * (-len(parts[1]) % 4)
        parsed = json.loads(base64.urlsafe_b64decode(payload.encode("ascii")).decode("utf-8"))
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def _redact_sensitive_response(value):
    sensitive_keys = {"token", "accessToken", "access_token", "refreshToken", "refresh_token", "xToken", "x-token"}
    if isinstance(value, dict):
        out = {}
        for key, item in value.items():
            if key in sensitive_keys and isinstance(item, str):
                out[key] = f"{item[:8]}...{item[-6:]}" if len(item) > 16 else "***"
            else:
                out[key] = _redact_sensitive_response(item)
        return out
    if isinstance(value, list):
        return [_redact_sensitive_response(item) for item in value]
    return value
