from __future__ import annotations

import json
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .client import AbdmClient
from .config import AbdmSettings, load_settings
from .log_store import log_abdm_event


M2_CHECKLIST = [
    {
        "key": "hip_discovery",
        "title": "HIP patient discovery",
        "status": "implemented",
        "detail": "Expose /api/v3/hip/patient/care-context/discover and match requests against local care-context records.",
    },
    {
        "key": "care_context_linking",
        "title": "Care-context linking",
        "status": "implemented",
        "detail": "Accept link init/confirm callbacks and send acknowledgement payloads back through the ABDM gateway.",
    },
    {
        "key": "consent_notify",
        "title": "Consent notification",
        "status": "implemented",
        "detail": "Accept HIP consent notifications and acknowledge the notification request.",
    },
    {
        "key": "health_information",
        "title": "Health-information request",
        "status": "started",
        "detail": "Accept health-information requests and acknowledge them. FHIR bundle generation and encrypted data push are the next slice.",
    },
    {
        "key": "audit_trace",
        "title": "M2 traceability",
        "status": "implemented",
        "detail": "Store recent M2 callback events and manually registered sandbox care contexts under the local workspace.",
    },
]


_LOCK = threading.Lock()


def get_m2_checklist() -> list[dict[str, str]]:
    return [dict(item) for item in M2_CHECKLIST]


class AbdmM2Store:
    def __init__(self, root: Path | None = None):
        self.root = root or Path.cwd()
        self.data_dir = self.root / "data"
        self.logs_dir = self.root / "Logs"
        self.contexts_path = self.data_dir / "abdm_m2_care_contexts.json"
        self.events_path = self.logs_dir / "abdm_m2_events.jsonl"

    def list_care_contexts(self) -> list[dict[str, Any]]:
        value = self._read_json(self.contexts_path, [])
        return value if isinstance(value, list) else []

    def save_care_context(self, payload: dict[str, Any]) -> dict[str, Any]:
        record = self.normalise_care_context(payload)
        with _LOCK:
            records = self.list_care_contexts()
            existing_index = _find_record_index(records, record)
            if existing_index >= 0:
                records[existing_index].update({k: v for k, v in record.items() if v not in ("", None, [])})
                record = records[existing_index]
            else:
                records.append(record)
            self._write_json(self.contexts_path, records)
        log_abdm_event(
            category="m2_store",
            action="care_context_saved",
            status="success",
            direction="internal",
            entity_type="care_context",
            entity_id=record.get("care_context_reference") or "",
            summary="ABDM M2 care context saved locally.",
            request_payload=record,
        )
        return record

    def find_matches(self, patient: dict[str, Any]) -> list[dict[str, Any]]:
        identifiers = _patient_identifiers(patient)
        if not identifiers:
            return []
        matches = []
        for record in self.list_care_contexts():
            if _record_matches(record, identifiers):
                matches.append(record)
        return matches

    def append_event(self, event_type: str, payload: dict[str, Any], *, status: str = "accepted") -> dict[str, Any]:
        event = {
            "id": str(uuid.uuid4()),
            "type": event_type,
            "status": status,
            "timestamp": _utc_now(),
            "payload": payload,
        }
        with _LOCK:
            self.events_path.parent.mkdir(parents=True, exist_ok=True)
            with self.events_path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(event, ensure_ascii=True, default=str) + "\n")
        log_abdm_event(
            category="m2_event",
            action=event_type,
            status=status,
            direction="callback",
            entity_type="m2_event",
            entity_id=event["id"],
            summary=f"ABDM M2 event stored: {event_type}.",
            request_payload=payload,
        )
        return event

    def recent_events(self, limit: int = 50) -> list[dict[str, Any]]:
        if not self.events_path.exists():
            return []
        try:
            lines = self.events_path.read_text(encoding="utf-8").splitlines()
        except Exception:
            return []
        events = []
        for line in lines[-max(1, int(limit or 50)) :]:
            try:
                parsed = json.loads(line)
                if isinstance(parsed, dict):
                    events.append(parsed)
            except Exception:
                continue
        return list(reversed(events))

    @staticmethod
    def normalise_care_context(payload: dict[str, Any]) -> dict[str, Any]:
        patient_reference = _clean(payload.get("patient_reference") or payload.get("patientReference"))
        care_context_reference = _clean(payload.get("care_context_reference") or payload.get("careContextReference"))
        mr_number = _clean(payload.get("mr") or payload.get("mr_number") or payload.get("uhid") or payload.get("patient_id"))
        abha_address = _clean(payload.get("abha_address") or payload.get("abhaAddress") or payload.get("health_id"))
        mobile = _digits_only(payload.get("mobile") or payload.get("phone"))
        if not patient_reference:
            patient_reference = f"PAT-{mr_number or abha_address or uuid.uuid4().hex[:10]}"
        if not care_context_reference:
            care_context_reference = f"CC-{mr_number or uuid.uuid4().hex[:10]}"
        display = _clean(payload.get("display") or payload.get("patient_display") or payload.get("name")) or patient_reference
        care_context_display = _clean(payload.get("care_context_display") or payload.get("careContextDisplay"))
        if not care_context_display:
            care_context_display = f"{_clean(payload.get('hi_type')) or 'Encounter'} - {care_context_reference}"
        return {
            "patient_reference": patient_reference,
            "patient_display": display,
            "care_context_reference": care_context_reference,
            "care_context_display": care_context_display,
            "abha_address": abha_address,
            "abha_number": _clean(payload.get("abha_number") or payload.get("abhaNumber")),
            "mr": mr_number,
            "mobile": mobile,
            "name": _clean(payload.get("name")),
            "gender": _clean(payload.get("gender")),
            "year_of_birth": _clean(payload.get("year_of_birth") or payload.get("yearOfBirth")),
            "hi_type": _clean(payload.get("hi_type") or payload.get("hiType")) or "OPConsultation",
            "active": bool(payload.get("active", True)),
            "created_at": _clean(payload.get("created_at") or payload.get("createdAt")) or _utc_now(),
        }

    def _read_json(self, path: Path, default: Any) -> Any:
        try:
            if not path.exists():
                return default
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return default

    def _write_json(self, path: Path, value: Any) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(value, indent=2, ensure_ascii=True, default=str), encoding="utf-8")


class AbdmM2Service:
    def __init__(
        self,
        client: AbdmClient | None = None,
        settings: AbdmSettings | None = None,
        store: AbdmM2Store | None = None,
    ):
        self.settings = settings or load_settings()
        self.client = client or AbdmClient(self.settings)
        self.store = store or AbdmM2Store()

    def endpoint_manifest(self) -> dict[str, Any]:
        bridge_url = self.settings.bridge_url.rstrip("/")
        return {
            "hip_id": self.settings.service_id,
            "bridge_url": self.settings.bridge_url,
            "callbacks": {
                "discover": bridge_url + "/api/v3/hip/patient/care-context/discover" if bridge_url else "/api/v3/hip/patient/care-context/discover",
                "link_init": bridge_url + "/api/v3/hip/link/care-context/init" if bridge_url else "/api/v3/hip/link/care-context/init",
                "link_confirm": bridge_url + "/api/v3/hip/link/care-context/confirm" if bridge_url else "/api/v3/hip/link/care-context/confirm",
                "consent_notify": bridge_url + "/api/v3/hip/consent/request/notify" if bridge_url else "/api/v3/hip/consent/request/notify",
                "health_information_request": bridge_url + "/api/v3/hip/health-information/request" if bridge_url else "/api/v3/hip/health-information/request",
            },
            "gateway_callbacks": {
                "link_on_init": self._gateway_url("/v0.5/links/link/on-init"),
                "link_on_confirm": self._gateway_url("/v0.5/links/link/on-confirm"),
                "consent_on_notify": self._gateway_url("/v0.5/consents/hip/on-notify"),
                "health_information_on_request": self._gateway_url("/v0.5/health-information/hip/on-request"),
            },
        }

    def discover(self, payload: dict[str, Any]) -> dict[str, Any]:
        patient = payload.get("patient") if isinstance(payload.get("patient"), dict) else {}
        matches = self.store.find_matches(patient)
        response = {
            "transactionId": _clean(payload.get("transactionId")) or str(uuid.uuid4()),
            "patient": [_patient_result(record) for record in matches],
            "matchedBy": sorted(_patient_identifiers(patient).keys()),
        }
        if not matches:
            response["error"] = {
                "code": "ABDM-M2-NO-MATCH",
                "message": "No matching care context is registered locally for this patient identifier.",
            }
        self.store.append_event("m2_discover", {"request": payload, "response": response}, status="success" if matches else "no_match")
        return response

    def on_link_init(self, payload: dict[str, Any]) -> dict[str, Any]:
        link_ref = _clean(payload.get("linkReferenceNumber") or _nested(payload, "link", "referenceNumber")) or str(uuid.uuid4())
        response = {
            "requestId": str(uuid.uuid4()),
            "timestamp": _utc_now(),
            "transactionId": _clean(payload.get("transactionId")),
            "link": {
                "referenceNumber": link_ref,
                "authenticationType": "DIRECT",
                "meta": {"communicationMedium": "MOBILE", "communicationHint": "******"},
            },
            "resp": {"requestId": _clean(payload.get("requestId"))},
        }
        self.store.append_event("m2_link_init", {"request": payload, "ack": response})
        return self._post_gateway("/v0.5/links/link/on-init", response)

    def on_link_confirm(self, payload: dict[str, Any]) -> dict[str, Any]:
        patient = payload.get("patient") if isinstance(payload.get("patient"), dict) else {}
        response = {
            "requestId": str(uuid.uuid4()),
            "timestamp": _utc_now(),
            "patient": {
                "referenceNumber": _clean(patient.get("referenceNumber")),
                "display": _clean(patient.get("display")),
                "careContexts": patient.get("careContexts") if isinstance(patient.get("careContexts"), list) else [],
            },
            "resp": {"requestId": _clean(payload.get("requestId"))},
        }
        self.store.append_event("m2_link_confirm", {"request": payload, "ack": response})
        return self._post_gateway("/v0.5/links/link/on-confirm", response)

    def on_consent_notify(self, payload: dict[str, Any]) -> dict[str, Any]:
        response = {
            "requestId": str(uuid.uuid4()),
            "timestamp": _utc_now(),
            "acknowledgement": {
                "status": "OK",
                "consentId": _clean(_nested(payload, "notification", "consentId") or payload.get("consentId")),
            },
            "resp": {"requestId": _clean(payload.get("requestId"))},
        }
        self.store.append_event("m2_consent_notify", {"request": payload, "ack": response})
        return self._post_gateway("/v0.5/consents/hip/on-notify", response)

    def on_health_information_request(self, payload: dict[str, Any]) -> dict[str, Any]:
        response = {
            "requestId": str(uuid.uuid4()),
            "timestamp": _utc_now(),
            "hiRequest": {
                "transactionId": _clean(payload.get("transactionId") or _nested(payload, "hiRequest", "transactionId")),
                "sessionStatus": "ACKNOWLEDGED",
            },
            "resp": {"requestId": _clean(payload.get("requestId"))},
        }
        self.store.append_event("m2_health_information_request", {"request": payload, "ack": response}, status="ack_only")
        return self._post_gateway("/v0.5/health-information/hip/on-request", response)

    def _post_gateway(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        response = self.client.request(
            "POST",
            self._gateway_url(path),
            payload,
            include_cm_id=True,
            extra_headers={"X-HIP-ID": self.settings.service_id} if self.settings.service_id else None,
        )
        return response.as_dict()

    def _gateway_url(self, path: str) -> str:
        return self.settings.gateway_base_url.rstrip("/") + "/" + str(path or "").strip("/")


def _patient_result(record: dict[str, Any]) -> dict[str, Any]:
    return {
        "referenceNumber": record.get("patient_reference"),
        "display": record.get("patient_display") or record.get("name") or record.get("patient_reference"),
        "careContexts": [
            {
                "referenceNumber": record.get("care_context_reference"),
                "display": record.get("care_context_display") or record.get("care_context_reference"),
            }
        ],
        "hiType": record.get("hi_type") or "OPConsultation",
    }


def _find_record_index(records: list[dict[str, Any]], record: dict[str, Any]) -> int:
    for index, existing in enumerate(records):
        if existing.get("care_context_reference") == record.get("care_context_reference"):
            return index
    return -1


def _record_matches(record: dict[str, Any], identifiers: dict[str, str]) -> bool:
    if not record.get("active", True):
        return False
    comparisons = {
        "abha_address": _clean(record.get("abha_address")).lower(),
        "abha_number": _clean(record.get("abha_number")).replace("-", ""),
        "mr": _clean(record.get("mr")).lower(),
        "mobile": _digits_only(record.get("mobile")),
    }
    for key, value in identifiers.items():
        if not value:
            continue
        comparable = value.lower() if key != "mobile" else _digits_only(value)
        if key == "abha_number":
            comparable = comparable.replace("-", "")
        if comparisons.get(key) and comparisons[key] == comparable:
            return True
    return False


def _patient_identifiers(patient: dict[str, Any]) -> dict[str, str]:
    identifiers: dict[str, str] = {}
    if not isinstance(patient, dict):
        return identifiers
    raw = patient.get("verifiedIdentifiers") or patient.get("unverifiedIdentifiers") or []
    if isinstance(raw, dict):
        raw = [raw]
    for item in raw if isinstance(raw, list) else []:
        if not isinstance(item, dict):
            continue
        key = _identifier_key(item.get("type") or item.get("system") or item.get("identifierType"))
        value = _clean(item.get("value") or item.get("identifier") or item.get("id"))
        if key and value:
            identifiers[key] = value
    for source, key in (
        ("id", "abha_address"),
        ("healthId", "abha_address"),
        ("abhaAddress", "abha_address"),
        ("abhaNumber", "abha_number"),
        ("mobile", "mobile"),
        ("phone", "mobile"),
        ("mr", "mr"),
        ("mrn", "mr"),
    ):
        value = _clean(patient.get(source))
        if value:
            identifiers[key] = value
    return identifiers


def _identifier_key(value: Any) -> str:
    text = _clean(value).lower().replace("_", "-").replace(" ", "-")
    if text in {"abha-address", "health-id", "healthid", "abha"}:
        return "abha_address"
    if text in {"abha-number", "abhanumber"}:
        return "abha_number"
    if text in {"mobile", "phone", "phone-number"}:
        return "mobile"
    if text in {"mr", "mrn", "uhid", "patient-id"}:
        return "mr"
    return ""


def _nested(payload: dict[str, Any], key: str, nested_key: str) -> Any:
    value = payload.get(key)
    if not isinstance(value, dict):
        return ""
    return value.get(nested_key)


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _digits_only(value: Any) -> str:
    return "".join(ch for ch in str(value or "") if ch.isdigit())


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
