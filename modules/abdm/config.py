from __future__ import annotations

import os
import json
from dataclasses import dataclass
from pathlib import Path

import config as app_config


_FILE_SETTINGS: dict[str, str] | None = None


def _load_file_settings() -> dict[str, str]:
    global _FILE_SETTINGS
    if _FILE_SETTINGS is not None:
        return _FILE_SETTINGS

    configured_path = os.getenv("ABDM_CONFIG_FILE", "").strip()
    candidates = []
    if configured_path:
        candidates.append(Path(configured_path))
    candidates.append(Path.cwd() / "instance" / "abdm_secrets.json")

    loaded: dict[str, str] = {}
    for path in candidates:
        try:
            if not path.exists():
                continue
            data = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                loaded = {str(k).strip(): str(v).strip() for k, v in data.items() if str(k).strip()}
                break
        except Exception:
            continue

    _FILE_SETTINGS = loaded
    return _FILE_SETTINGS


def _setting(name: str, default: str = "") -> str:
    value = os.getenv(name)
    if value is None:
        value = _load_file_settings().get(name)
    if value is None:
        value = getattr(app_config, name, default)
    return str(value or "").strip()


def _setting_int(name: str, default: int) -> int:
    raw = _setting(name, str(default))
    try:
        return int(raw)
    except Exception:
        return int(default)


def _setting_float(name: str, default: float) -> float:
    raw = _setting(name, str(default))
    try:
        return float(raw)
    except Exception:
        return float(default)


@dataclass(frozen=True)
class AbdmSettings:
    env: str
    gateway_base_url: str
    bridge_base_url: str
    service_base_url: str
    session_path: str
    bridge_path: str
    bridge_service_method: str
    abha_session_url: str
    abha_base_url: str
    abha_phr_base_url: str
    bridge_v3_url: str
    facility_base_url: str
    facility_id: str
    facility_name: str
    hip_name: str
    cm_id: str
    client_id: str
    client_secret: str
    bridge_url: str
    service_id: str
    service_name: str
    service_type: str
    service_alias: str
    service_endpoint_url: str
    service_endpoint_use: str
    timeout_seconds: int
    retry_attempts: int
    retry_backoff_seconds: float
    verify_ssl: bool
    ca_bundle: str

    @property
    def session_url(self) -> str:
        return self.gateway_base_url.rstrip("/") + "/" + self.session_path.strip("/")

    @property
    def bridge_url_api(self) -> str:
        return self.bridge_base_url.rstrip("/") + "/" + self.bridge_path.strip("/")

    @property
    def bridge_services_url(self) -> str:
        return self.service_base_url.rstrip("/") + "/" + self.bridge_path.strip("/") + "/addUpdateServices"

    @property
    def bridge_get_services_url(self) -> str:
        return self.service_base_url.rstrip("/") + "/" + self.bridge_path.strip("/") + "/getServices"

    @property
    def bridge_v3_services_url(self) -> str:
        base_url = self.gateway_base_url.rstrip("/")
        if base_url.endswith("/gateway"):
            base_url = base_url[: -len("/gateway")]
        return base_url + "/api/hiecm/gateway/v3/bridge-services"

    @property
    def configured(self) -> bool:
        return bool(self.client_id and self.client_secret)

    def redacted_summary(self) -> dict:
        return {
            "env": self.env,
            "gateway_base_url": self.gateway_base_url,
            "bridge_base_url": self.bridge_base_url,
            "service_base_url": self.service_base_url,
            "session_path": self.session_path,
            "bridge_path": self.bridge_path,
            "bridge_service_method": self.bridge_service_method,
            "abha_session_url": self.abha_session_url,
            "abha_base_url": self.abha_base_url,
            "abha_phr_base_url": self.abha_phr_base_url,
            "bridge_v3_url": self.bridge_v3_url,
            "facility_base_url": self.facility_base_url,
            "facility_id": self.facility_id,
            "facility_name": self.facility_name,
            "hip_name": self.hip_name,
            "cm_id": self.cm_id,
            "client_id": self.client_id,
            "client_secret_configured": bool(self.client_secret),
            "bridge_url": self.bridge_url,
            "service_id": self.service_id,
            "service_name": self.service_name,
            "service_type": self.service_type,
            "service_alias": self.service_alias,
            "service_endpoint_url": self.service_endpoint_url,
            "service_endpoint_use": self.service_endpoint_use,
            "timeout_seconds": self.timeout_seconds,
            "retry_attempts": self.retry_attempts,
            "retry_backoff_seconds": self.retry_backoff_seconds,
            "verify_ssl": self.verify_ssl,
            "ca_bundle_configured": bool(self.ca_bundle),
        }


def _setting_bool(name: str, default: bool) -> bool:
    raw = _setting(name, "true" if default else "false").lower()
    return raw in {"1", "true", "yes", "on"}


def load_settings() -> AbdmSettings:
    return AbdmSettings(
        env=_setting("ABDM_ENV", "sandbox"),
        gateway_base_url=_setting("ABDM_GATEWAY_BASE_URL", "https://dev.abdm.gov.in/gateway"),
        bridge_base_url=_setting("ABDM_BRIDGE_BASE_URL", "https://dev.abdm.gov.in/gateway"),
        service_base_url=_setting("ABDM_SERVICE_BASE_URL", "https://dev.abdm.gov.in/gateway"),
        session_path=_setting("ABDM_SESSION_PATH", "/v0.5/sessions"),
        bridge_path=_setting("ABDM_BRIDGE_PATH", "/v1/bridges"),
        bridge_service_method=_setting("ABDM_BRIDGE_SERVICE_METHOD", "POST").upper(),
        abha_session_url=_setting("ABDM_ABHA_SESSION_URL", "https://dev.abdm.gov.in/api/hiecm/gateway/v3/sessions"),
        abha_base_url=_setting("ABDM_ABHA_BASE_URL", "https://abhasbx.abdm.gov.in/abha/api"),
        abha_phr_base_url=_setting("ABDM_ABHA_PHR_BASE_URL", "https://abhasbx.abdm.gov.in/abha/api/v3/phr/web"),
        bridge_v3_url=_setting("ABDM_BRIDGE_V3_URL", "https://dev.abdm.gov.in/api/hiecm/gateway/v3/bridge/url"),
        facility_base_url=_setting("ABDM_FACILITY_BASE_URL", "https://facilitysbx.abdm.gov.in"),
        facility_id=_setting("ABDM_FACILITY_ID"),
        facility_name=_setting("ABDM_FACILITY_NAME", "ASARFI HOSPITAL"),
        hip_name=_setting("ABDM_HIP_NAME", "ASARFI-HOSPITAL-HIP"),
        cm_id=_setting("ABDM_CM_ID", "sbx"),
        client_id=_setting("ABDM_CLIENT_ID"),
        client_secret=_setting("ABDM_CLIENT_SECRET"),
        bridge_url=_setting("ABDM_BRIDGE_URL"),
        service_id=_setting("ABDM_SERVICE_ID", "ASARFI-HIP-SBX"),
        service_name=_setting("ABDM_SERVICE_NAME", "ASARFI-HOSPITAL-HIP"),
        service_type=_setting("ABDM_SERVICE_TYPE", "HIP"),
        service_alias=_setting("ABDM_SERVICE_ALIAS", "asarfi-hospital-hip"),
        service_endpoint_url=_setting("ABDM_SERVICE_ENDPOINT_URL"),
        service_endpoint_use=_setting("ABDM_SERVICE_ENDPOINT_USE", "registration"),
        timeout_seconds=_setting_int("ABDM_TIMEOUT_SECONDS", 30),
        retry_attempts=_setting_int("ABDM_RETRY_ATTEMPTS", 3),
        retry_backoff_seconds=_setting_float("ABDM_RETRY_BACKOFF_SECONDS", 2.0),
        verify_ssl=_setting_bool("ABDM_VERIFY_SSL", True),
        ca_bundle=_setting("ABDM_CA_BUNDLE"),
    )
