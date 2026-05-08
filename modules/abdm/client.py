from __future__ import annotations

import json
import ssl
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from .config import AbdmSettings, load_settings
from .log_store import log_abdm_event, timed_ms


_RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


class AbdmError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        response: Any = None,
        attempts: int = 1,
    ):
        super().__init__(message)
        self.status_code = status_code
        self.response = response
        self.attempts = attempts


@dataclass
class AbdmResponse:
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


class AbdmClient:
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
        )
        data = response.data if isinstance(response.data, dict) else {}
        token = str(
            data.get("accessToken")
            or data.get("access_token")
            or data.get("token")
            or ""
        ).strip()
        if not token:
            raise AbdmError("ABDM gateway did not return an access token.", response=response.data)

        expires_in = data.get("expiresIn") or data.get("expires_in") or 1200
        try:
            ttl = max(60, int(expires_in))
        except Exception:
            ttl = 1200
        self._token = token
        self._token_expires_at = time.time() + ttl
        return token

    def request(
        self,
        method: str,
        path_or_url: str,
        payload: Any = None,
        *,
        include_cm_id: bool = True,
        extra_headers: dict[str, str] | None = None,
    ) -> AbdmResponse:
        token = self.get_session_token()
        return self._send_json(
            method,
            path_or_url,
            payload,
            token=token,
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
    ) -> AbdmResponse:
        body = None
        if payload is not None:
            body = json.dumps(payload, ensure_ascii=True).encode("utf-8")

        if auth and not token:
            raise AbdmError("Missing ABDM gateway token.")

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
            req = Request(url, data=body, headers=headers, method=method.upper())
            attempts = attempt_index + 1
            try:
                with urlopen(req, timeout=self.settings.timeout_seconds, context=self._ssl_context) as response:
                    raw = response.read().decode("utf-8", errors="replace")
                    parsed = _json_or_text(raw)
                    status_code = int(getattr(response, "status", 0) or 0)
                    _log_outbound(
                        "gateway_http",
                        status="success",
                        method=method,
                        url=url,
                        http_status=status_code,
                        request_id=request_id,
                        request_payload=payload,
                        response_payload=parsed,
                        duration_ms=timed_ms(started_at),
                    )
                    return AbdmResponse(
                        status_code=status_code,
                        data=parsed,
                        request_id=request_id,
                        attempts=attempts,
                    )
            except HTTPError as exc:
                raw = exc.read().decode("utf-8", errors="replace")
                parsed = _json_or_text(raw)
                last_error = AbdmError(
                    f"ABDM gateway returned HTTP {exc.code}.",
                    status_code=int(exc.code or 0),
                    response=parsed,
                    attempts=attempts,
                )
                if int(exc.code or 0) in _RETRYABLE_STATUS_CODES and attempts < total_attempts:
                    self._sleep_before_retry(attempt_index)
                    continue
                _log_outbound(
                    "gateway_http",
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
                last_error = AbdmError(f"ABDM gateway connection failed: {exc}", attempts=attempts)
                if attempts < total_attempts:
                    self._sleep_before_retry(attempt_index)
                    continue
                _log_outbound(
                    "gateway_http",
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
                _log_outbound(
                    "gateway_http",
                    status="error",
                    method=method,
                    url=url,
                    request_id=request_id,
                    request_payload=payload,
                    error_message=str(exc),
                    duration_ms=timed_ms(started_at),
                )
                raise AbdmError(f"ABDM gateway request failed: {exc}", attempts=attempts) from exc
        if last_error:
            raise last_error
        raise AbdmError("ABDM gateway request failed without a response.", attempts=total_attempts)

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


def _abdm_headers(*, request_id: str, cm_id: str = "", token: str = "") -> dict[str, str]:
    headers = {
        "Accept": "*/*",
        "Content-Type": "application/json",
        "REQUEST-ID": request_id,
        "TIMESTAMP": datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
    }
    if cm_id:
        headers["X-CM-ID"] = cm_id
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def _json_or_text(value: str) -> Any:
    if not value:
        return {}
    try:
        return json.loads(value)
    except Exception:
        return value


def _log_outbound(
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
        category="gateway",
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
        summary=f"ABDM gateway {method.upper()} completed with {status}.",
    )
