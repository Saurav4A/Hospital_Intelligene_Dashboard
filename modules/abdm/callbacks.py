from __future__ import annotations

import threading
import time
from datetime import datetime, timezone

from flask import Blueprint, g, jsonify, request

from .log_store import log_abdm_event, timed_ms
from .m2 import AbdmM2Service, AbdmM2Store
from .scan_share import AbdmScanShareService


def create_abdm_callbacks_blueprint(audit_log_event=None) -> Blueprint:
    bp = Blueprint("abdm_callbacks", __name__)
    store = AbdmM2Store()

    def _audit(action: str, *, status: str = "success", summary: str = "", details=None):
        log_abdm_event(
            category="callback",
            action=action,
            status=status,
            direction="inbound",
            method=request.method if request else "",
            url=request.path if request else "",
            entity_type="abdm_callback",
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
                entity_type="abdm_callback",
                summary=summary,
                details=details,
            )
        except Exception:
            pass

    @bp.before_request
    def _abdm_callback_before_request():
        g.abdm_callback_started_at = time.time()

    @bp.after_request
    def _abdm_callback_after_request(response):
        log_abdm_event(
            category="callback_route",
            action=request.path,
            status="success" if response.status_code < 400 else "error",
            direction="inbound",
            method=request.method,
            url=request.path,
            http_status=response.status_code,
            request_id=_request_id_from_headers(),
            request_payload=request.get_json(silent=True) if request.is_json else None,
            summary=f"ABDM callback returned HTTP {response.status_code}.",
            duration_ms=timed_ms(getattr(g, "abdm_callback_started_at", time.time())),
        )
        return response

    @bp.route("/api/abdm/callback/health", methods=["GET"])
    def health():
        return jsonify(
            {
                "status": "ok",
                "service": "abdm",
                "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
            }
        )

    @bp.route("/api/v3/hip/patient/care-context/discover", methods=["POST"])
    def m2_discover():
        payload = request.get_json(silent=True) or {}
        if not isinstance(payload, dict):
            payload = {}
        result = AbdmM2Service(store=store).discover(payload)
        _audit(
            "m2_discover",
            status="success" if result.get("patient") else "no_match",
            summary="ABDM M2 HIP discovery callback processed.",
            details={"transactionId": result.get("transactionId"), "matches": len(result.get("patient") or [])},
        )
        return jsonify(result), 200

    @bp.route("/api/v3/hip/profile/share", methods=["POST"])
    @bp.route("/api/v3/hip/patient/share", methods=["POST"])
    @bp.route("/api/v3/hip/patient/profile/share", methods=["POST"])
    @bp.route("/v3/patient-share/share", methods=["POST"])
    @bp.route("/hiecm/api/v3/patient-share/share", methods=["POST"])
    @bp.route("/v3/hip/patient/profile/share", methods=["POST"])
    @bp.route("/api/v3/profile/share", methods=["POST"])
    @bp.route("/v3/profile/share", methods=["POST"])
    @bp.route("/v1.0/patients/profile/share", methods=["POST"])
    @bp.route("/api/abdm/callback/profile/share", methods=["POST"])
    @bp.route("/api/abdm/callback/registration", methods=["POST"])
    def scan_share_profile():
        payload = request.get_json(silent=True) or {}
        if not isinstance(payload, dict):
            payload = {}
        headers = dict(request.headers)
        path = request.path
        request_id = _request_id_from_headers()
        threading.Thread(
            target=_process_scan_share_profile_async,
            args=(payload, headers, path, request_id),
            daemon=True,
        ).start()
        _audit(
            "m1_scan_share_profile_received",
            summary="ABDM Scan & Share profile callback accepted for background processing.",
            details={"path": path, "request_id": request_id},
        )
        return jsonify({"status": "accepted", "request_id": request_id}), 202

    @bp.route("/api/v3/hiu/patient/on-share", methods=["POST"])
    def scan_share_on_share_callback():
        payload = request.get_json(silent=True) or {}
        if not isinstance(payload, dict):
            payload = {}
        store.append_event("m1_scan_share_on_share_callback", payload, status="accepted")
        _audit(
            "m1_scan_share_on_share_callback",
            summary="ABDM Scan & Share on-share callback received.",
            details=payload,
        )
        return jsonify({"status": "accepted"}), 202

    @bp.route("/api/v3/hip/link/care-context/init", methods=["POST"])
    def m2_link_init():
        return _m2_gateway_ack("m2_link_init", lambda payload: AbdmM2Service(store=store).on_link_init(payload))

    @bp.route("/api/v3/hip/link/care-context/confirm", methods=["POST"])
    def m2_link_confirm():
        return _m2_gateway_ack("m2_link_confirm", lambda payload: AbdmM2Service(store=store).on_link_confirm(payload))

    @bp.route("/api/v3/hip/consent/request/notify", methods=["POST"])
    @bp.route("/v0.5/consents/hip/notify", methods=["POST"])
    def m2_consent_notify():
        return _m2_gateway_ack("m2_consent_notify", lambda payload: AbdmM2Service(store=store).on_consent_notify(payload))

    @bp.route("/api/v3/hip/health-information/request", methods=["POST"])
    @bp.route("/v0.5/health-information/hip/request", methods=["POST"])
    def m2_health_information_request():
        return _m2_gateway_ack(
            "m2_health_information_request",
            lambda payload: AbdmM2Service(store=store).on_health_information_request(payload),
        )

    @bp.route("/api/abdm/callback/<path:callback_path>", methods=["GET", "POST"])
    def placeholder(callback_path: str):
        return _placeholder_response(callback_path)

    @bp.route("/v0.5/<path:callback_path>", methods=["GET", "POST"])
    def v05_placeholder(callback_path: str):
        return _placeholder_response("v0.5/" + callback_path)

    @bp.route("/v1.0/<path:callback_path>", methods=["GET", "POST"])
    def v10_placeholder(callback_path: str):
        return _placeholder_response("v1.0/" + callback_path)

    @bp.route("/v3/<path:callback_path>", methods=["GET", "POST"])
    def v3_placeholder(callback_path: str):
        return _placeholder_response("v3/" + callback_path)

    def _placeholder_response(callback_path: str):
        _audit(
            "callback_placeholder",
            summary=f"ABDM callback placeholder hit: {callback_path}",
            details={
                "path": callback_path,
                "method": request.method,
                "request_id": request.headers.get("REQUEST-ID") or request.headers.get("X-Request-ID") or "",
            },
        )
        return jsonify(
            {
                "status": "accepted",
                "message": "ABDM callback endpoint is reachable. Workflow handling is pending M1 implementation.",
                "path": callback_path,
            }
        ), 202

    def _process_scan_share_profile_async(payload: dict, headers: dict, path: str, request_id: str) -> None:
        try:
            result = AbdmScanShareService().handle_profile_share(payload, headers=headers, path=path)
            log_abdm_event(
                category="callback",
                action="m1_scan_share_profile_processed",
                status="success",
                direction="internal",
                method="POST",
                url=path,
                request_id=request_id,
                entity_type="scan_share",
                entity_id=result.get("share_ref") or "",
                summary="ABDM Scan & Share profile callback processed in background.",
                request_payload={
                    "share_ref": result.get("share_ref"),
                    "scan_share_id": result.get("scan_share_id"),
                    "status": result.get("status"),
                    "on_share_status": (result.get("on_share") or {}).get("status"),
                },
            )
            if audit_log_event:
                try:
                    audit_log_event(
                        "abdm",
                        "m1_scan_share_profile_processed",
                        status="success",
                        entity_type="scan_share",
                        entity_id=result.get("share_ref") or "",
                        summary="ABDM Scan & Share profile callback processed in background.",
                        details={
                            "scan_share_id": result.get("scan_share_id"),
                            "status": result.get("status"),
                            "on_share_status": (result.get("on_share") or {}).get("status"),
                        },
                    )
                except Exception:
                    pass
        except Exception as exc:
            log_abdm_event(
                category="callback",
                action="m1_scan_share_profile_process_failed",
                status="error",
                direction="internal",
                method="POST",
                url=path,
                request_id=request_id,
                entity_type="scan_share",
                summary=str(exc),
                request_payload=payload,
                error_message=str(exc),
            )

    def _m2_gateway_ack(action: str, handler):
        payload = request.get_json(silent=True) or {}
        if not isinstance(payload, dict):
            payload = {}
        try:
            gateway_response = handler(payload)
            _audit(
                action,
                summary=f"ABDM {action} callback accepted and acknowledged.",
                details={"gateway_response": gateway_response},
            )
            return jsonify({"status": "accepted", "gateway_response": gateway_response}), 202
        except Exception as exc:
            store.append_event(action + "_ack_error", {"request": payload, "error": str(exc)}, status="error")
            _audit(action, status="error", summary=str(exc), details={"request": payload})
            return jsonify({"status": "accepted", "acknowledgement_error": str(exc)}), 202

    def _request_id_from_headers() -> str:
        return (
            request.headers.get("REQUEST-ID")
            or request.headers.get("X-Request-ID")
            or request.headers.get("X-Correlation-ID")
            or ""
        )

    return bp
