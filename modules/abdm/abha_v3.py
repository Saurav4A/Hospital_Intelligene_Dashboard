from __future__ import annotations

import base64
import json
import ssl
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from .client import AbdmError, _RETRYABLE_STATUS_CODES, _abdm_headers
from .config import AbdmSettings, load_settings
from .log_store import log_abdm_event, timed_ms


@dataclass
class AbhaV3Response:
    status_code: int
    data: Any
    request_id: str
    attempts: int = 1

    def as_dict(self) -> dict[str, Any]:
        return {
            "status_code": self.status_code,
            "request_id": self.request_id,
            "attempts": self.attempts,
            "data": self.data,
        }


class AbhaV3Client:
    """Client for ABDM M1 ABHA V3 APIs.

    Bridge registration remains in bridge.py. This client covers the M1 ABHA
    endpoints documented in the ABHA V3 API guide and M1 workbook.
    """

    def __init__(self, settings: AbdmSettings | None = None):
        self.settings = settings or load_settings()
        self._token = ""
        self._token_expires_at = 0.0
        self._ssl_context = self._build_ssl_context()

    def get_session_token(self, *, force_refresh: bool = False) -> str:
        now = time.time()
        if self._token and not force_refresh and now < self._token_expires_at - 60:
            return self._token
        if not self.settings.client_id or not self.settings.client_secret:
            raise AbdmError("ABDM_CLIENT_ID and ABDM_CLIENT_SECRET must be configured.")

        response = self._send_json(
            "POST",
            self.settings.abha_session_url,
            {
                "clientId": self.settings.client_id,
                "clientSecret": self.settings.client_secret,
                "grantType": "client_credentials",
            },
            auth=False,
            include_cm_id=True,
        )
        data = response.data if isinstance(response.data, dict) else {}
        token = str(data.get("accessToken") or data.get("access_token") or "").strip()
        if not token:
            raise AbdmError("ABDM ABHA V3 session did not return an access token.", response=response.data)

        try:
            ttl = max(60, int(data.get("expiresIn") or data.get("expires_in") or 600))
        except Exception:
            ttl = 600
        self._token = token
        self._token_expires_at = time.time() + ttl
        return token

    def get_public_certificate(self) -> dict[str, Any]:
        response = self.request("GET", self._abha_url("/v3/profile/public/certificate"), None)
        return response.as_dict()

    def request_aadhaar_enrollment_otp(self, aadhaar_number: str, *, txn_id: str = "") -> dict[str, Any]:
        encrypted_aadhaar = self.encrypt_value(_digits_only(aadhaar_number))
        payload = {
            "txnId": str(txn_id or ""),
            "scope": ["abha-enrol"],
            "loginHint": "aadhaar",
            "loginId": encrypted_aadhaar,
            "otpSystem": "aadhaar",
        }
        return self.request("POST", "/v3/enrollment/request/otp", payload).as_dict()

    def enrol_by_aadhaar_otp(self, txn_id: str, otp_value: str, *, mobile: str = "") -> dict[str, Any]:
        otp_payload: dict[str, Any] = {
            "timeStamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "txnId": str(txn_id or "").strip(),
            "otpValue": self.encrypt_value(str(otp_value or "").strip()),
        }
        mobile_value = _digits_only(mobile)
        if mobile_value:
            otp_payload["mobile"] = mobile_value
        payload = {
            "authData": {
                "authMethods": ["otp"],
                "otp": otp_payload,
            },
            "consent": {
                "code": "abha-enrollment",
                "version": "1.4",
            },
        }
        return self.request("POST", "/v3/enrollment/enrol/byAadhaar", payload).as_dict()

    def request_abha_number_verification_otp(
        self,
        abha_number: str,
        *,
        otp_system: str = "aadhaar",
        auth_method: str = "aadhaar",
        prefer_profile: bool = False,
    ) -> dict[str, Any]:
        clean_abha_number = _digits_only(abha_number)
        if len(clean_abha_number) != 14:
            raise ValueError("ABHA number must be 14 digits.")
        system = str(otp_system or "").strip().lower()
        method = str(auth_method or "").strip().lower()
        use_mobile_scope = system == "abdm" or method in {"abdm", "mobile", "mobile-verify", "abha-otp"}
        scope = ["abha-login", "mobile-verify"] if use_mobile_scope else ["abha-login", "aadhaar-verify"]
        otp_system_value = system or ("abdm" if use_mobile_scope else "aadhaar")
        candidates = [_format_abha_number(clean_abha_number), clean_abha_number]
        urls = self._login_endpoint_candidates(use_phr_app_first=use_mobile_scope and not prefer_profile, path="/login/request/otp")
        last_error: AbdmError | None = None
        for url_label, url in urls:
            for candidate in candidates:
                payload = {
                    "scope": scope,
                    "loginHint": "abha-number",
                    "loginId": self.encrypt_value(candidate),
                    "otpSystem": otp_system_value,
                }
                try:
                    result = self.request("POST", url, payload).as_dict()
                    result["abha_number_format_used"] = "hyphenated" if "-" in candidate else "digits"
                    result["abha_login_endpoint_used"] = url_label
                    return result
                except AbdmError as exc:
                    last_error = exc
                    if not _is_login_id_rejected(exc.response):
                        raise
        if last_error:
            raise last_error
        raise AbdmError("ABHA number OTP request failed.")

    def verify_abha_number_otp(
        self,
        txn_id: str,
        otp_value: str,
        *,
        auth_method: str = "aadhaar",
        login_endpoint: str = "",
    ) -> dict[str, Any]:
        method = str(auth_method or "").strip().lower()
        use_mobile_scope = method in {"abdm", "mobile", "mobile-verify", "abha-otp"}
        payload = {
            "scope": ["abha-login", "mobile-verify"] if use_mobile_scope else ["abha-login", "aadhaar-verify"],
            "authData": {
                "authMethods": ["otp"],
                "otp": {
                    "txnId": str(txn_id or "").strip(),
                    "otpValue": self.encrypt_value(str(otp_value or "").strip()),
                },
            },
        }
        preferred = str(login_endpoint or "").strip().lower()
        urls = self._login_endpoint_candidates(use_phr_app_first=use_mobile_scope or preferred == "phr_app", path="/login/verify")
        if preferred == "profile":
            urls = list(reversed(urls))
        last_error: AbdmError | None = None
        for url_label, url in urls:
            try:
                result = self.request("POST", url, payload).as_dict()
                result["abha_login_endpoint_used"] = url_label
                return result
            except AbdmError as exc:
                last_error = exc
                if not _is_txn_or_login_rejected(exc.response):
                    raise
        if last_error:
            raise last_error
        raise AbdmError("ABHA number OTP verification failed.")

    def get_abha_profile(self, x_token: str) -> dict[str, Any]:
        return self._request_json_fallbacks(
            "GET",
            [
                ("phr_app_profile", self._phr_app_url("/login/profile")),
                ("phr_web_profile", self._phr_url("/login/profile/abhaprofile")),
            ],
            None,
            extra_headers=_x_token_header(x_token),
        )

    def get_abha_card(self, x_token: str) -> tuple[bytes, str]:
        return self._request_raw_fallbacks(
            "GET",
            [
                ("phr_app_card", self._phr_app_url("/login/profile/phrCard")),
                ("profile_abha_card", self._abha_url("/v3/profile/account/abha-card")),
                ("phr_web_card_alt", self._phr_url("/login/profile/phrCard")),
                ("phr_web_card", self._phr_url("/login/profile/abha/phr-card")),
            ],
            None,
            extra_headers=_x_token_header(x_token),
        )

    def verify_switch_profile(self, x_token: str, *, abha_address: str, txn_id: str) -> dict[str, Any]:
        payload = {
            "abhaAddress": str(abha_address or "").strip(),
            "txnId": str(txn_id or "").strip(),
        }
        if not payload["abhaAddress"] or not payload["txnId"]:
            raise ValueError("ABHA address and transaction ID are required for switch-profile verification.")
        return self._request_json_fallbacks(
            "POST",
            [
                ("phr_app_verify_user", self._phr_app_url("/login/verify/user")),
                ("phr_web_verify_user", self._phr_url("/login/verify/user")),
                ("phr_app_switch_profile_verify", self._phr_app_url("/login/profile/verify/switch-profile/user")),
                ("phr_web_switch_profile_verify", self._phr_url("/login/profile/verify/switch-profile/user")),
            ],
            payload,
            extra_headers=_x_token_header(x_token),
        )

    def verify_abha_number_user(self, t_token: str, *, abha_number: str, txn_id: str) -> dict[str, Any]:
        clean_abha_number = _digits_only(abha_number)
        if len(clean_abha_number) != 14:
            raise ValueError("ABHA number must be 14 digits for verify user.")
        transaction_id = str(txn_id or "").strip()
        if not transaction_id:
            raise ValueError("Transaction ID is required for verify user.")
        last_error: AbdmError | None = None
        candidates = [_format_abha_number(clean_abha_number), clean_abha_number]
        for field_name in ("ABHANumber", "abhaNumber"):
            for candidate in candidates:
                payload = {
                    field_name: self.encrypt_value(candidate),
                    "txnId": transaction_id,
                }
                try:
                    response = self.request(
                        "POST",
                        "/v3/profile/login/verify/user",
                        payload,
                        extra_headers=_t_token_header(t_token),
                    )
                    result = response.as_dict()
                    result["abha_endpoint_used"] = "profile_verify_user"
                    result["abha_number_format_used"] = "hyphenated" if "-" in candidate else "digits"
                    result["abha_number_field_used"] = field_name
                    return result
                except AbdmError as exc:
                    last_error = exc
                    if not _is_abha_number_rejected(exc.response):
                        raise
        if last_error:
            raise last_error
        raise AbdmError("ABHA verify user failed.")

    def update_abha_profile(self, x_token: str, payload: dict[str, Any]) -> dict[str, Any]:
        try:
            return self._request_json_fallbacks(
                "PATCH",
                [("profile_account_update", self._abha_url("/v3/profile/account"))],
                payload,
                extra_headers=_x_token_header(x_token),
            )
        except AbdmError as exc:
            if not _is_not_found(exc.response, exc.status_code):
                raise
        return self._request_json_fallbacks(
            "POST",
            [("phr_app_update_profile", self._phr_app_url("/login/profile/updateProfile"))],
            payload,
            extra_headers=_x_token_header(x_token),
        )

    def request_mobile_update_otp(self, x_token: str, mobile: str) -> dict[str, Any]:
        mobile_value = _digits_only(mobile)
        if len(mobile_value) != 10:
            raise ValueError("Mobile number must be 10 digits.")
        payload = {
            "scope": ["abha-profile", "mobile-verify"],
            "loginHint": "mobile",
            "loginId": self.encrypt_value(mobile_value),
            "otpSystem": "abdm",
        }
        return self._request_json_fallbacks(
            "POST",
            [("profile_mobile_update_otp", self._abha_url("/v3/profile/account/request/otp"))],
            payload,
            extra_headers=_x_token_header(x_token),
        )

    def verify_mobile_update_otp(self, x_token: str, txn_id: str, otp_value: str) -> dict[str, Any]:
        transaction_id = str(txn_id or "").strip()
        otp = str(otp_value or "").strip()
        if not transaction_id or not otp:
            raise ValueError("Transaction ID and OTP are required for mobile update verification.")
        payload = {
            "scope": ["abha-profile", "mobile-verify"],
            "authData": {
                "authMethods": ["otp"],
                "otp": {
                    "timeStamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "txnId": transaction_id,
                    "otpValue": self.encrypt_value(otp),
                },
            },
        }
        return self._request_json_fallbacks(
            "POST",
            [("profile_mobile_update_verify", self._abha_url("/v3/profile/account/verify"))],
            payload,
            extra_headers=_x_token_header(x_token),
        )

    def request_abha_address_otp(self, abha_address: str, *, otp_system: str = "abdm") -> dict[str, Any]:
        address = str(abha_address or "").strip()
        if not address:
            raise ValueError("ABHA address is required.")
        payload = {
            "scope": ["abha-address-login", "mobile-verify"],
            "loginHint": "abha-address",
            "loginId": self.encrypt_value(address),
            "otpSystem": str(otp_system or "abdm").strip() or "abdm",
        }
        return self._request_json_fallbacks(
            "POST",
            [
                ("phr_app_abha_address_otp", self._phr_app_url("/login/abha/request/otp")),
                ("phr_web_abha_address_otp", self._phr_url("/login/abha/request/otp")),
            ],
            payload,
        )

    def verify_abha_address_otp(self, txn_id: str, otp_value: str) -> dict[str, Any]:
        payload = {
            "scope": ["abha-address-login", "mobile-verify"],
            "authData": {
                "authMethods": ["otp"],
                "otp": {
                    "txnId": str(txn_id or "").strip(),
                    "otpValue": self.encrypt_value(str(otp_value or "").strip()),
                },
            },
        }
        return self._request_json_fallbacks(
            "POST",
            [
                ("phr_app_abha_address_verify", self._phr_app_url("/login/abha/verify")),
                ("phr_web_abha_address_verify", self._phr_url("/login/abha/verify")),
            ],
            payload,
        )

    def encrypt_value(self, value: str, certificate: str | None = None) -> str:
        public_key = certificate or self._extract_certificate(self.get_public_certificate().get("data"))
        if not public_key:
            raise AbdmError("ABHA public certificate was not available for encryption.")
        try:
            from cryptography import x509
            from cryptography.hazmat.backends import default_backend
            from cryptography.hazmat.primitives import hashes, serialization
            from cryptography.hazmat.primitives.asymmetric import padding
        except Exception as exc:
            raise AbdmError(
                "ABHA encryption needs the cryptography package. Add cryptography to requirements.txt and install it on the server."
            ) from exc

        try:
            pem = self._normalise_public_key_pem(public_key)
            if "BEGIN CERTIFICATE" in pem:
                cert = x509.load_pem_x509_certificate(pem.encode("ascii"), default_backend())
                loaded_key = cert.public_key()
            else:
                loaded_key = serialization.load_pem_public_key(pem.encode("ascii"), backend=default_backend())
        except Exception as exc:
            raise AbdmError(f"Unable to load ABHA public key for encryption: {exc}") from exc

        encrypted = loaded_key.encrypt(
            str(value).encode("utf-8"),
            padding.OAEP(
                mgf=padding.MGF1(algorithm=hashes.SHA1()),
                algorithm=hashes.SHA1(),
                label=None,
            ),
        )
        return base64.b64encode(encrypted).decode("ascii")

    def request(
        self,
        method: str,
        path_or_url: str,
        payload: Any = None,
        *,
        token: str = "",
        include_cm_id: bool = True,
        extra_headers: dict[str, str] | None = None,
    ) -> AbhaV3Response:
        access_token = token or self.get_session_token()
        url = path_or_url if path_or_url.lower().startswith("http") else self._abha_url(path_or_url)
        return self._send_json(
            method,
            url,
            payload,
            token=access_token,
            auth=True,
            include_cm_id=include_cm_id,
            extra_headers=extra_headers,
        )

    def request_raw(
        self,
        method: str,
        path_or_url: str,
        payload: Any = None,
        *,
        token: str = "",
        include_cm_id: bool = True,
        extra_headers: dict[str, str] | None = None,
    ) -> tuple[bytes, str]:
        access_token = token or self.get_session_token()
        url = path_or_url if path_or_url.lower().startswith("http") else self._abha_url(path_or_url)
        return self._send_raw(
            method,
            url,
            payload,
            token=access_token,
            auth=True,
            include_cm_id=include_cm_id,
            extra_headers=extra_headers,
        )

    def _send_json(
        self,
        method: str,
        url: str,
        payload: Any = None,
        *,
        token: str = "",
        auth: bool = True,
        include_cm_id: bool = True,
        extra_headers: dict[str, str] | None = None,
    ) -> AbhaV3Response:
        body = None
        if payload is not None:
            body = json.dumps(payload, ensure_ascii=True).encode("utf-8")

        if auth and not token:
            raise AbdmError("Missing ABDM ABHA V3 token.")

        total_attempts = max(1, 1 + max(0, int(self.settings.retry_attempts or 0)))
        last_error: AbdmError | None = None
        started_at = time.time()
        for attempt_index in range(total_attempts):
            request_id = str(uuid.uuid4())
            headers = _abdm_headers(
                request_id=request_id,
                cm_id=self.settings.cm_id if include_cm_id else "",
                token=token if auth else "",
            )
            if extra_headers:
                headers.update(extra_headers)
            if auth and token and _has_x_token(headers) and "X-AUTH-TOKEN" not in headers:
                headers["X-AUTH-TOKEN"] = token
            req = Request(url, data=body, headers=headers, method=method.upper())
            attempts = attempt_index + 1
            try:
                with urlopen(req, timeout=self.settings.timeout_seconds, context=self._ssl_context) as response:
                    raw = response.read().decode("utf-8", errors="replace")
                    parsed = _json_or_text(raw)
                    status_code = int(getattr(response, "status", 0) or 0)
                    _log_abha_outbound(
                        "abha_v3_http",
                        status="success",
                        method=method,
                        url=url,
                        http_status=status_code,
                        request_id=request_id,
                        request_payload=payload,
                        response_payload=parsed,
                        duration_ms=timed_ms(started_at),
                    )
                    return AbhaV3Response(
                        status_code=status_code,
                        data=parsed,
                        request_id=request_id,
                        attempts=attempts,
                    )
            except HTTPError as exc:
                raw = exc.read().decode("utf-8", errors="replace")
                parsed = _json_or_text(raw)
                last_error = AbdmError(
                    f"ABDM ABHA V3 returned HTTP {exc.code}.",
                    status_code=int(exc.code or 0),
                    response=parsed,
                    attempts=attempts,
                )
                if int(exc.code or 0) in _RETRYABLE_STATUS_CODES and attempts < total_attempts:
                    self._sleep_before_retry(attempt_index)
                    continue
                _log_abha_outbound(
                    "abha_v3_http",
                    status="error",
                    method=method,
                    url=url,
                    http_status=int(exc.code or 0),
                    request_id=request_id,
                    request_payload=payload,
                    response_payload=parsed,
                    error_message=str(last_error),
                    duration_ms=timed_ms(started_at),
                )
                raise last_error
            except URLError as exc:
                last_error = AbdmError(f"ABDM ABHA V3 connection failed: {exc}", attempts=attempts)
                if attempts < total_attempts:
                    self._sleep_before_retry(attempt_index)
                    continue
                _log_abha_outbound(
                    "abha_v3_http",
                    status="error",
                    method=method,
                    url=url,
                    request_id=request_id,
                    request_payload=payload,
                    error_message=str(last_error),
                    duration_ms=timed_ms(started_at),
                )
                raise last_error from exc
            except AbdmError:
                raise
            except Exception as exc:
                _log_abha_outbound(
                    "abha_v3_http",
                    status="error",
                    method=method,
                    url=url,
                    request_id=request_id,
                    request_payload=payload,
                    error_message=str(exc),
                    duration_ms=timed_ms(started_at),
                )
                raise AbdmError(f"ABDM ABHA V3 request failed: {exc}", attempts=attempts) from exc
        if last_error:
            raise last_error
        raise AbdmError("ABDM ABHA V3 request failed without a response.", attempts=total_attempts)

    def _send_raw(
        self,
        method: str,
        url: str,
        payload: Any = None,
        *,
        token: str = "",
        auth: bool = True,
        include_cm_id: bool = True,
        extra_headers: dict[str, str] | None = None,
    ) -> tuple[bytes, str]:
        body = None
        if payload is not None:
            body = json.dumps(payload, ensure_ascii=True).encode("utf-8")

        if auth and not token:
            raise AbdmError("Missing ABDM ABHA V3 token.")

        total_attempts = max(1, 1 + max(0, int(self.settings.retry_attempts or 0)))
        last_error: AbdmError | None = None
        started_at = time.time()
        for attempt_index in range(total_attempts):
            request_id = str(uuid.uuid4())
            headers = _abdm_headers(
                request_id=request_id,
                cm_id=self.settings.cm_id if include_cm_id else "",
                token=token if auth else "",
            )
            if extra_headers:
                headers.update(extra_headers)
            if auth and token and _has_x_token(headers) and "X-AUTH-TOKEN" not in headers:
                headers["X-AUTH-TOKEN"] = token
            req = Request(url, data=body, headers=headers, method=method.upper())
            attempts = attempt_index + 1
            try:
                with urlopen(req, timeout=self.settings.timeout_seconds, context=self._ssl_context) as response:
                    content_type = str(response.headers.get("Content-Type") or "application/octet-stream")
                    raw_content = response.read()
                    status_code = int(getattr(response, "status", 0) or 0)
                    _log_abha_outbound(
                        "abha_v3_binary",
                        status="success",
                        method=method,
                        url=url,
                        http_status=status_code,
                        request_id=request_id,
                        request_payload=payload,
                        response_payload={"content_type": content_type, "bytes": len(raw_content)},
                        duration_ms=timed_ms(started_at),
                    )
                    return raw_content, content_type
            except HTTPError as exc:
                raw = exc.read().decode("utf-8", errors="replace")
                parsed = _json_or_text(raw)
                last_error = AbdmError(
                    f"ABDM ABHA V3 returned HTTP {exc.code}.",
                    status_code=int(exc.code or 0),
                    response=parsed,
                    attempts=attempts,
                )
                if int(exc.code or 0) in _RETRYABLE_STATUS_CODES and attempts < total_attempts:
                    self._sleep_before_retry(attempt_index)
                    continue
                _log_abha_outbound(
                    "abha_v3_binary",
                    status="error",
                    method=method,
                    url=url,
                    http_status=int(exc.code or 0),
                    request_id=request_id,
                    request_payload=payload,
                    response_payload=parsed,
                    error_message=str(last_error),
                    duration_ms=timed_ms(started_at),
                )
                raise last_error
            except URLError as exc:
                last_error = AbdmError(f"ABDM ABHA V3 connection failed: {exc}", attempts=attempts)
                if attempts < total_attempts:
                    self._sleep_before_retry(attempt_index)
                    continue
                _log_abha_outbound(
                    "abha_v3_binary",
                    status="error",
                    method=method,
                    url=url,
                    request_id=request_id,
                    request_payload=payload,
                    error_message=str(last_error),
                    duration_ms=timed_ms(started_at),
                )
                raise last_error from exc
            except AbdmError:
                raise
            except Exception as exc:
                _log_abha_outbound(
                    "abha_v3_binary",
                    status="error",
                    method=method,
                    url=url,
                    request_id=request_id,
                    request_payload=payload,
                    error_message=str(exc),
                    duration_ms=timed_ms(started_at),
                )
                raise AbdmError(f"ABDM ABHA V3 request failed: {exc}", attempts=attempts) from exc
        if last_error:
            raise last_error
        raise AbdmError("ABDM ABHA V3 request failed without a response.", attempts=total_attempts)

    def _abha_url(self, path: str) -> str:
        return self.settings.abha_base_url.rstrip("/") + "/" + str(path or "").strip("/")

    def _phr_url(self, path: str) -> str:
        return self.settings.abha_phr_base_url.rstrip("/") + "/" + str(path or "").strip("/")

    def _phr_app_url(self, path: str) -> str:
        base = self.settings.abha_phr_base_url.rstrip("/")
        if base.endswith("/phr/web"):
            base = base[: -len("/phr/web")] + "/phr/app"
        return base + "/" + str(path or "").strip("/")

    def _login_endpoint_candidates(self, *, use_phr_app_first: bool, path: str) -> list[tuple[str, str]]:
        profile_url = self._abha_url("/v3/profile" + "/" + str(path or "").strip("/"))
        phr_app_url = self._phr_app_url(path)
        values = [("phr_app", phr_app_url), ("profile", profile_url)] if use_phr_app_first else [("profile", profile_url), ("phr_app", phr_app_url)]
        seen = set()
        unique = []
        for label, url in values:
            if url in seen:
                continue
            seen.add(url)
            unique.append((label, url))
        return unique

    def _request_json_fallbacks(
        self,
        method: str,
        candidates: list[tuple[str, str]],
        payload: Any = None,
        *,
        extra_headers: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        last_error: AbdmError | None = None
        for label, url in candidates:
            try:
                result = self.request(method, url, payload, extra_headers=extra_headers).as_dict()
                result["abha_endpoint_used"] = label
                return result
            except AbdmError as exc:
                last_error = exc
                if not _should_try_next_endpoint(exc.response, exc.status_code):
                    raise
        if last_error:
            raise last_error
        raise AbdmError("ABHA request failed before reaching ABDM.")

    def _request_raw_fallbacks(
        self,
        method: str,
        candidates: list[tuple[str, str]],
        payload: Any = None,
        *,
        extra_headers: dict[str, str] | None = None,
    ) -> tuple[bytes, str]:
        last_error: AbdmError | None = None
        for _label, url in candidates:
            try:
                return self.request_raw(method, url, payload, extra_headers=extra_headers)
            except AbdmError as exc:
                last_error = exc
                if not _should_try_next_endpoint(exc.response, exc.status_code):
                    raise
        if last_error:
            raise last_error
        raise AbdmError("ABHA binary request failed before reaching ABDM.")

    def _build_ssl_context(self):
        if not self.settings.verify_ssl:
            return ssl._create_unverified_context()
        if self.settings.ca_bundle:
            return ssl.create_default_context(cafile=self.settings.ca_bundle)
        return ssl.create_default_context()

    def _sleep_before_retry(self, attempt_index: int) -> None:
        delay = max(0.0, float(self.settings.retry_backoff_seconds or 0.0)) * (2 ** max(0, attempt_index))
        if delay > 0:
            time.sleep(delay)

    @staticmethod
    def _extract_certificate(data: Any) -> str:
        if isinstance(data, str):
            return data.strip()
        if not isinstance(data, dict):
            return ""
        for key in ("publicKey", "public_key", "certificate", "cert", "data"):
            value = data.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
            if isinstance(value, dict):
                nested = AbhaV3Client._extract_certificate(value)
                if nested:
                    return nested
        return ""

    @staticmethod
    def _normalise_public_key_pem(value: str) -> str:
        text = str(value or "").strip()
        if "-----BEGIN" in text:
            return text
        body = "".join(text.split())
        lines = "\n".join(body[i : i + 64] for i in range(0, len(body), 64))
        return f"-----BEGIN PUBLIC KEY-----\n{lines}\n-----END PUBLIC KEY-----\n"


def _json_or_text(value: str) -> Any:
    if not value:
        return {}
    try:
        return json.loads(value)
    except Exception:
        return value


def _digits_only(value: str) -> str:
    return "".join(ch for ch in str(value or "") if ch.isdigit())


def _format_abha_number(value: str) -> str:
    digits = _digits_only(value)
    if len(digits) != 14:
        return str(value or "").strip()
    return f"{digits[:2]}-{digits[2:6]}-{digits[6:10]}-{digits[10:14]}"


def _is_login_id_rejected(response: Any) -> bool:
    text = json.dumps(response, ensure_ascii=True, default=str).lower()
    return "loginid" in text or "loginid is invalid" in text or "invalid abha number" in text


def _is_abha_number_rejected(response: Any) -> bool:
    text = json.dumps(response, ensure_ascii=True, default=str).lower()
    return "abhanumber" in text or "abha number" in text or "abha-number" in text or "invalid abha" in text


def _is_txn_or_login_rejected(response: Any) -> bool:
    text = json.dumps(response, ensure_ascii=True, default=str).lower()
    return "txnid" in text or "transaction" in text or _is_login_id_rejected(response)


def _is_not_found(response: Any, status_code: int | None = None) -> bool:
    if int(status_code or 0) == 404:
        return True
    text = json.dumps(response, ensure_ascii=True, default=str).lower()
    return '"404"' in text or "no matching resource" in text or "not found" in text


def _should_try_next_endpoint(response: Any, status_code: int | None = None) -> bool:
    text = json.dumps(response, ensure_ascii=True, default=str).lower()
    return (
        _is_not_found(response, status_code)
        or "invalid x-token" in text
        or "invalid x token" in text
        or "abdm-1006" in text
        or "access denied" in text
    )


def _x_token_header(x_token: str) -> dict[str, str]:
    token = str(x_token or "").strip()
    if token and not token.lower().startswith("bearer "):
        token = "Bearer " + token
    return {"X-token": token}


def _t_token_header(t_token: str) -> dict[str, str]:
    token = str(t_token or "").strip()
    if token and not token.lower().startswith("bearer "):
        token = "Bearer " + token
    return {"T-token": token}


def _has_x_token(headers: dict[str, str]) -> bool:
    return any(str(key).lower() == "x-token" and str(value or "").strip() for key, value in (headers or {}).items())


def _log_abha_outbound(
    action: str,
    *,
    status: str,
    method: str,
    url: str,
    http_status: int | None = None,
    request_id: str = "",
    request_payload: Any = None,
    response_payload: Any = None,
    error_message: str = "",
    duration_ms: int | None = None,
) -> None:
    log_abdm_event(
        category="abha_v3",
        action=action,
        status=status,
        direction="outbound",
        method=method,
        url=url,
        http_status=http_status,
        request_id=request_id,
        request_payload=request_payload,
        response_payload=response_payload,
        error_message=error_message,
        duration_ms=duration_ms,
        summary=f"ABDM ABHA V3 {method.upper()} completed with {status}.",
    )
