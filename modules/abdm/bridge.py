from __future__ import annotations

from dataclasses import replace
from typing import Any

from .abha_v3 import AbhaV3Client
from .client import AbdmClient, AbdmError
from .config import AbdmSettings, load_settings


class AbdmBridgeService:
    def __init__(self, client: AbdmClient | None = None, settings: AbdmSettings | None = None):
        self.settings = settings or load_settings()
        self.client = client or AbdmClient(self.settings)

    def update_bridge_url(self, url: str | None = None) -> dict[str, Any]:
        bridge_url = (url or self.settings.bridge_url or "").strip()
        if not bridge_url:
            raise ValueError("Bridge URL is required. Set ABDM_BRIDGE_URL or pass url.")
        if not bridge_url.lower().startswith("https://"):
            raise ValueError("ABDM bridge URL must be HTTPS with a valid SSL certificate.")
        payload = {
            "bridgeId": self.settings.client_id,
            "url": bridge_url,
        }
        try:
            response = AbhaV3Client(self.settings).request(
                "PATCH",
                self.settings.bridge_v3_url,
                payload,
                include_cm_id=True,
            )
            return response.as_dict()
        except AbdmError as exc:
            if _is_duplicate_bridge_patch(exc.response):
                return {
                    "status_code": exc.status_code or 400,
                    "request_id": "",
                    "attempts": exc.attempts,
                    "data": exc.response,
                    "accepted": True,
                    "message": "ABDM already has this bridge URL patch request pending.",
                    "payload": payload,
                }
            raise

    def add_or_update_default_service(self, overrides: dict[str, Any] | None = None) -> dict[str, Any]:
        existing = self.find_service_by_id()
        if _service_lookup_is_active(existing):
            return {
                "status_code": existing.get("status_code", 200),
                "request_id": existing.get("request_id", ""),
                "attempts": existing.get("attempts", 1),
                "data": existing.get("data"),
                "accepted": True,
                "message": "ABDM HIP service is already registered and active.",
            }
        payload = self.default_service_payload()
        if overrides:
            payload.update({k: v for k, v in overrides.items() if v not in (None, "")})
        response = self.client.request(
            self.settings.bridge_service_method or "POST",
            self.settings.bridge_services_url,
            [payload],
            include_cm_id=True,
        )
        return response.as_dict()

    def get_services(self) -> dict[str, Any]:
        response = AbhaV3Client(self.settings).request("GET", self.settings.bridge_v3_services_url, include_cm_id=True)
        return response.as_dict()

    def find_service_by_id(self, service_id: str | None = None) -> dict[str, Any]:
        sid = (service_id or self.settings.service_id or "").strip()
        if not sid:
            raise ValueError("ABDM service ID is required.")
        url = self.settings.bridge_v3_services_url.rstrip("/")[:-len("/bridge-services")] + "/bridge-service/serviceId/" + sid
        response = AbhaV3Client(self.settings).request("GET", url, include_cm_id=True)
        return response.as_dict()

    def register_facility_hrp(self, overrides: dict[str, Any] | None = None) -> dict[str, Any]:
        payload = self.facility_hrp_payload()
        if overrides:
            payload = self._merge_facility_payload(payload, overrides)
        if not payload.get("facilityId"):
            raise ValueError(
                "ABDM_FACILITY_ID is required to add/update HIP service through the current M1 facility API. "
                "The bridge URL is registered, but ABDM service list is empty until the facility ID is available."
            )
        url = self.settings.facility_base_url.rstrip("/") + "/v1/bridges/MutipleHRPAddUpdateServices"
        facility_settings = replace(
            self.settings,
            timeout_seconds=min(int(self.settings.timeout_seconds or 30), 8),
            retry_attempts=0,
        )
        response = AbhaV3Client(facility_settings).request(
            "POST",
            url,
            payload,
            include_cm_id=False,
            extra_headers={"accept": "application/json"},
        )
        return response.as_dict()

    def facility_hrp_payload(self) -> dict[str, Any]:
        return {
            "facilityId": self.settings.facility_id,
            "facilityName": self.settings.facility_name,
            "HRP": [
                {
                    "bridgeId": self.settings.client_id,
                    "hipName": self.settings.hip_name or self.settings.service_name,
                    "type": self.settings.service_type,
                    "active": True,
                }
            ],
        }

    def _merge_facility_payload(self, payload: dict[str, Any], overrides: dict[str, Any]) -> dict[str, Any]:
        merged = dict(payload)
        hrp = list(merged.get("HRP") or [])
        first_hrp = dict(hrp[0]) if hrp else {}
        for key, value in overrides.items():
            if value in (None, ""):
                continue
            if key in {"facilityId", "facilityName"}:
                merged[key] = value
            elif key == "name":
                first_hrp["hipName"] = value
            elif key == "id":
                first_hrp["bridgeId"] = value
            else:
                first_hrp[key] = value
        merged["HRP"] = [first_hrp]
        return merged

    def default_service_payload(self) -> dict[str, Any]:
        endpoint_url = (self.settings.service_endpoint_url or "").strip()
        if not endpoint_url and self.settings.bridge_url:
            endpoint_url = self.settings.bridge_url.rstrip("/") + "/api/abdm/callback/registration"
        if endpoint_url and not endpoint_url.lower().startswith("https://"):
            endpoint_url = ""
        payload = {
            "id": self.settings.service_id,
            "name": self.settings.service_name,
            "type": self.settings.service_type,
            "active": True,
        }
        alias = (self.settings.service_alias or "").strip()
        if alias:
            payload["alias"] = [alias]
        if endpoint_url:
            payload["endpoints"] = [
                {
                    "address": endpoint_url,
                    "connectionType": "https",
                    "use": self.settings.service_endpoint_use or "registration",
                }
            ]
        return payload


def _is_duplicate_bridge_patch(response: Any) -> bool:
    if not isinstance(response, dict):
        return False
    error = response.get("error")
    if not isinstance(error, dict):
        return False
    return str(error.get("code") or "").strip().upper() == "ABDM-1094"


def _service_lookup_is_active(result: dict[str, Any]) -> bool:
    data = result.get("data") if isinstance(result, dict) else None
    if not isinstance(data, dict):
        return False
    return bool(data.get("active") or data.get("isHip") or data.get("isHiu"))
