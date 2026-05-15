from __future__ import annotations

import json
import time
import uuid
from datetime import datetime, timezone
from typing import Any

from .client import AbdmClient
from .config import AbdmSettings, load_settings
from .hospital_units import list_hospital_units
from .log_store import log_abdm_event
from .m2 import AbdmM2Store
from .token_store import _encrypt
from modules.db_connection import get_sql_connection


class AbdmScanShareStore:
    def save_callback(self, payload: dict[str, Any], *, headers: dict[str, str] | None = None, path: str = "") -> dict[str, Any]:
        _ensure_tables()
        normalized = normalize_scan_share_payload(payload)
        normalized.update(_resolve_hospital_unit(normalized))
        normalized["callback_path"] = path
        normalized["header_request_id"] = _header_request_id(headers or {})
        normalized["share_ref"] = str(uuid.uuid4())
        linking_token = extract_linking_token(payload)
        normalized["linking_token_stored"] = bool(linking_token)
        linking_token_cipher = _encrypt(linking_token) if linking_token else ""

        conn = _connect()
        try:
            cur = conn.cursor()
            match = _find_existing_linkage(cur, normalized)
            linkage = _linkage_from_normalized(normalized, match=match)
            linkage["linking_token_cipher"] = linking_token_cipher
            normalized.update(
                {
                    "status": linkage.get("status"),
                    "uhid": linkage.get("uhid"),
                    "mr": linkage.get("mr"),
                    "patient_reference": linkage.get("patient_reference"),
                    "care_context_reference": linkage.get("care_context_reference"),
                }
            )
            cur.execute(
                """
                INSERT INTO dbo.HID_ABDM_ScanShare_Profile (
                    ShareRef, HospitalCode, HospitalName, ReceivedAtUtc, CallbackPath, HeaderRequestId, RequestId, TransactionId,
                    FacilityId, HipId, CounterId, PatientReference, CareContextReference,
                    Uhid, MrNo, AbhaAddress, AbhaNumber, PatientName, Mobile, Gender,
                    YearOfBirth, Status, LinkingTokenCipher, RawJson, NormalizedJson
                )
                OUTPUT inserted.Id
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    normalized["share_ref"],
                    _clip(normalized.get("hospital_code"), 20),
                    _clip(normalized.get("hospital_name"), 120),
                    _utc_now_naive(),
                    _clip(path, 300),
                    _clip(normalized.get("header_request_id"), 100),
                    _clip(normalized.get("request_id"), 100),
                    _clip(normalized.get("transaction_id"), 100),
                    _clip(normalized.get("facility_id"), 100),
                    _clip(normalized.get("hip_id"), 100),
                    _clip(normalized.get("counter_id"), 100),
                    _clip(linkage.get("patient_reference"), 120),
                    _clip(linkage.get("care_context_reference"), 120),
                    _clip(linkage.get("uhid"), 80),
                    _clip(linkage.get("mr"), 80),
                    _clip(normalized.get("abha_address"), 120),
                    _clip(normalized.get("abha_number"), 40),
                    _clip(normalized.get("name"), 180),
                    _clip(normalized.get("mobile"), 20),
                    _clip(normalized.get("gender"), 10),
                    _safe_int_or_none(normalized.get("year_of_birth")),
                    _clip(linkage["status"], 30),
                    linking_token_cipher,
                    _json(_redact_linking_token(payload)),
                    _json(normalized),
                ),
            )
            row = cur.fetchone()
            scan_id = int(row[0]) if row and row[0] is not None else 0
            linkage["scan_share_id"] = scan_id
            linkage["share_ref"] = normalized["share_ref"]
            _upsert_linkage(cur, linkage)
            conn.commit()
        finally:
            conn.close()

        care_context = self._save_m2_care_context(normalized)
        result = {
            "scan_share_id": scan_id,
            "share_ref": normalized["share_ref"],
            "status": linkage["status"],
            "normalized": normalized,
            "linkage": linkage,
            "care_context": care_context,
        }
        log_abdm_event(
            category="scan_share",
            action="scan_share_profile_saved",
            status="success",
            direction="inbound",
            entity_type="scan_share",
            entity_id=normalized["share_ref"],
            summary="ABDM Scan & Share profile callback saved and linked locally.",
            request_payload=result,
        )
        return result

    def list_profiles(self, limit: int = 50, *, hospital_code: str = "") -> list[dict[str, Any]]:
        _ensure_tables()
        conn = _connect()
        try:
            cur = conn.cursor()
            where = ""
            params: list[Any] = [max(1, min(200, int(limit or 50)))]
            code = _clean(hospital_code).upper()
            if code:
                where = "WHERE ISNULL(HospitalCode, 'AHL') = ?"
                params.append(code)
            cur.execute(
                f"""
                SELECT TOP (?) Id, ShareRef, HospitalCode, HospitalName, ReceivedAtUtc, RequestId, TransactionId, FacilityId,
                       HipId, CounterId, PatientReference, CareContextReference, Uhid, MrNo,
                       AbhaAddress, AbhaNumber, PatientName, Mobile, Gender, YearOfBirth, Status,
                       CASE WHEN ISNULL(LinkingTokenCipher, '') = '' THEN CAST(0 AS bit) ELSE CAST(1 AS bit) END AS LinkingTokenStored
                FROM dbo.HID_ABDM_ScanShare_Profile
                {where}
                ORDER BY Id DESC
                """,
                tuple(params),
            )
            columns = [col[0] for col in cur.description]
            rows = []
            for row in cur.fetchall():
                item = dict(zip(columns, row))
                item["ReceivedAtUtc"] = item["ReceivedAtUtc"].isoformat() if item.get("ReceivedAtUtc") else ""
                rows.append(_lower_keys(item))
            return rows
        finally:
            conn.close()

    def link_profile(self, payload: dict[str, Any]) -> dict[str, Any]:
        _ensure_tables()
        scan_share_id = _safe_int_or_none(payload.get("scan_share_id") or payload.get("scanShareId"))
        share_ref = _clean(payload.get("share_ref") or payload.get("shareRef"))
        if not scan_share_id and not share_ref:
            raise ValueError("scan_share_id or share_ref is required.")

        conn = _connect()
        try:
            cur = conn.cursor()
            row = _fetch_scan_row(cur, scan_share_id=scan_share_id, share_ref=share_ref)
            if not row:
                raise ValueError("Scan & Share profile was not found.")
            normalized = _json_or_dict(row.get("NormalizedJson"))
            if not isinstance(normalized, dict):
                normalized = {}
            normalized.update(_resolve_hospital_unit(normalized))
            merged = dict(normalized)
            for key in ("uhid", "mr", "patient_reference", "care_context_reference", "care_context_display", "hi_type", "hospital_code", "hospital_name", "facility_id", "hip_id"):
                value = _clean(payload.get(key) or payload.get(_camel(key)))
                if value:
                    merged[key] = value
            merged.update(_resolve_hospital_unit(merged))
            linkage = _linkage_from_normalized(merged, force_linked=True)
            linkage["scan_share_id"] = int(row["Id"])
            linkage["share_ref"] = row["ShareRef"]
            linkage["linking_token_cipher"] = row.get("LinkingTokenCipher") or ""
            _upsert_linkage(cur, linkage)
            cur.execute(
                """
                UPDATE dbo.HID_ABDM_ScanShare_Profile
                   SET HospitalCode = ?, HospitalName = ?, FacilityId = ?, HipId = ?,
                       Uhid = ?, MrNo = ?, PatientReference = ?, CareContextReference = ?,
                       Status = ?, NormalizedJson = ?
                 WHERE Id = ?
                """,
                (
                    _clip(merged.get("hospital_code"), 20),
                    _clip(merged.get("hospital_name"), 120),
                    _clip(merged.get("facility_id"), 100),
                    _clip(merged.get("hip_id"), 100),
                    _clip(linkage.get("uhid"), 80),
                    _clip(linkage.get("mr"), 80),
                    _clip(linkage.get("patient_reference"), 120),
                    _clip(linkage.get("care_context_reference"), 120),
                    _clip(linkage.get("status"), 30),
                    _json(merged),
                    int(row["Id"]),
                ),
            )
            conn.commit()
        finally:
            conn.close()
        care_context = self._save_m2_care_context(merged)
        return {"status": "success", "linkage": linkage, "care_context": care_context}

    def _save_m2_care_context(self, normalized: dict[str, Any]) -> dict[str, Any]:
        care_context_payload = {
            "patient_reference": normalized.get("patient_reference"),
            "patient_display": normalized.get("name") or normalized.get("patient_reference"),
            "care_context_reference": normalized.get("care_context_reference"),
            "care_context_display": normalized.get("care_context_display"),
            "abha_address": normalized.get("abha_address"),
            "abha_number": normalized.get("abha_number"),
            "mr": normalized.get("mr") or normalized.get("uhid"),
            "mobile": normalized.get("mobile"),
            "name": normalized.get("name"),
            "gender": normalized.get("gender"),
            "year_of_birth": normalized.get("year_of_birth"),
            "hi_type": normalized.get("hi_type") or "OPConsultation",
        }
        return AbdmM2Store().save_care_context(care_context_payload)


class AbdmScanShareService:
    def __init__(
        self,
        *,
        store: AbdmScanShareStore | None = None,
        client: AbdmClient | None = None,
        settings: AbdmSettings | None = None,
    ):
        self.settings = settings or load_settings()
        self.store = store or AbdmScanShareStore()
        self.client = client or AbdmClient(self.settings)

    def handle_profile_share(self, payload: dict[str, Any], *, headers: dict[str, str] | None = None, path: str = "") -> dict[str, Any]:
        result = self.store.save_callback(payload, headers=headers, path=path)
        on_share_payload = self.build_on_share_payload(payload, result)
        try:
            gateway_response = self.client.request(
                "POST",
                self._hiecm_url("/api/hiecm/patient-share/v3/on-share"),
                on_share_payload,
                include_cm_id=True,
            ).as_dict()
            result["on_share"] = {"status": "success", "version": "v3", "gateway_response": gateway_response}
        except Exception as exc:
            result["on_share"] = {"status": "error", "message": str(exc)}
            log_abdm_event(
                category="scan_share",
                action="scan_share_on_share_failed",
                status="error",
                direction="outbound",
                entity_type="scan_share",
                entity_id=result.get("share_ref") or "",
                summary=str(exc),
                request_payload=on_share_payload,
                response_payload=getattr(exc, "response", None),
            )
        return result

    def build_on_share_payload(self, original_payload: dict[str, Any], result: dict[str, Any]) -> dict[str, Any]:
        normalized = result.get("normalized") if isinstance(result.get("normalized"), dict) else {}
        request_id = _clean(
            original_payload.get("requestId")
            or original_payload.get("request_id")
            or _nested_value(original_payload, "response", "requestId")
            or _nested_value(original_payload, "response", "request_id")
            or normalized.get("header_request_id")
        )
        token_number = str(result.get("scan_share_id") or "").zfill(4) or uuid.uuid4().hex[:6].upper()
        return {
            "acknowledgement": {
                "abhaAddress": normalized.get("abha_address"),
                "status": "SUCCESS",
                "profile": {
                    "context": normalized.get("counter_id") or normalized.get("patient_reference"),
                    "tokenNumber": token_number,
                    "expiry": "180",
                },
            },
            "response": {"requestId": request_id},
        }

    def _gateway_url(self, path: str) -> str:
        return self.settings.gateway_base_url.rstrip("/") + "/" + str(path or "").strip("/")

    def _hiecm_url(self, path: str) -> str:
        base = self.settings.gateway_base_url.rstrip("/")
        if base.endswith("/gateway"):
            base = base[: -len("/gateway")]
        if base.endswith("/hiecm/api"):
            base = base[: -len("/hiecm/api")]
        return base.rstrip("/") + "/" + str(path or "").strip("/")


def normalize_scan_share_payload(payload: dict[str, Any]) -> dict[str, Any]:
    payload = payload if isinstance(payload, dict) else {}
    meta = _first_dict(payload, "metaData", "metadata", "meta") or {}
    profile = _first_dict(payload, "profile", "patient", "data", "payload") or {}
    patient = _first_dict(profile, "patient", "patientDetails", "profile") or profile
    abha_number = _clean(_find_any(patient, "abhaNumber", "ABHANumber", "healthIdNumber"))
    abha_address = _clean(_find_any(patient, "abhaAddress", "preferredAbhaAddress", "phrAddress", "healthId"))
    mobile = _digits_only(_find_any(patient, "mobile", "mobileNumber", "phone", "phoneNumber"))
    name = _clean(_find_any(patient, "name", "fullName", "patientName", "display"))
    uhid = _clean(_find_any(payload, "uhid", "UHID", "registrationNo", "registration_no"))
    mr = _clean(_find_any(payload, "mr", "mrn", "mrNo", "MRNo", "patientId", "patientReference"))
    patient_reference = _clean(_find_any(payload, "patientReference", "patient_reference", "referenceNumber"))
    if not patient_reference:
        patient_reference = uhid or mr or f"SCAN-{_clean(payload.get('transactionId')) or uuid.uuid4().hex[:10]}"
    care_context_reference = _clean(_find_any(payload, "careContextReference", "care_context_reference"))
    if not care_context_reference:
        care_context_reference = f"REG-{patient_reference}"
    care_context_display = _clean(_find_any(payload, "careContextDisplay", "care_context_display"))
    if not care_context_display:
        care_context_display = f"Registration - {patient_reference}"
    return {
        "request_id": _clean(payload.get("requestId") or payload.get("request_id")),
        "transaction_id": _clean(payload.get("transactionId") or payload.get("transaction_id")),
        "facility_id": _clean(_find_any(payload, "facilityId", "hfrId", "facilityCode") or meta.get("hipId")),
        "hip_id": _clean(_find_any(payload, "hipId", "hip_id", "hipCode") or meta.get("hipId")),
        "counter_id": _clean(_find_any(payload, "counterId", "counter_id", "counterCode", "counterNo") or meta.get("context")),
        "patient_reference": patient_reference,
        "care_context_reference": care_context_reference,
        "care_context_display": care_context_display,
        "uhid": uhid,
        "mr": mr or uhid,
        "abha_address": abha_address,
        "abha_number": abha_number,
        "name": name,
        "mobile": mobile,
        "gender": _clean(_find_any(patient, "gender")),
        "year_of_birth": _clean(_find_any(patient, "yearOfBirth", "year_of_birth", "yob")),
        "hi_type": _clean(_find_any(payload, "hiType", "hi_type")) or "OPConsultation",
        "address": _clean(_find_any(patient, "address", "addressLine")),
        "district": _clean(_find_any(patient, "district", "districtName")),
        "state": _clean(_find_any(patient, "state", "stateName")),
        "pincode": _digits_only(_find_any(patient, "pincode", "pinCode", "pin")),
    }


def extract_linking_token(payload: dict[str, Any]) -> str:
    return _clean(_find_any(payload if isinstance(payload, dict) else {}, "linkingToken", "linking_token", "linkToken", "link_token"))


def _resolve_hospital_unit(normalized: dict[str, Any]) -> dict[str, str]:
    facility_id = _clean(normalized.get("facility_id"))
    hip_id = _clean(normalized.get("hip_id")) or facility_id
    hospital_code = _clean(normalized.get("hospital_code")).upper()
    units = list_hospital_units()
    matched = None
    if hospital_code:
        matched = next((unit for unit in units if _clean(unit.get("code")).upper() == hospital_code), None)
    if not matched and (facility_id or hip_id):
        incoming_ids = {facility_id.lower(), hip_id.lower()} - {""}
        matched = next(
            (
                unit
                for unit in units
                if (_clean(unit.get("facility_id")).lower() and _clean(unit.get("facility_id")).lower() in incoming_ids)
                or (_clean(unit.get("hip_id")).lower() and _clean(unit.get("hip_id")).lower() in incoming_ids)
            ),
            None,
        )
    if not matched:
        ahl = next((unit for unit in units if _clean(unit.get("code")).upper() == "AHL"), None)
        ahl_ids = {_clean((ahl or {}).get("facility_id")).lower(), _clean((ahl or {}).get("hip_id")).lower()}
        incoming_ids = {facility_id.lower(), hip_id.lower()} - {""}
        if incoming_ids and not incoming_ids.intersection(ahl_ids):
            matched = {
                "code": "UNMAPPED",
                "name": "Unmapped ABDM Facility",
                "facility_id": facility_id,
                "hip_id": hip_id,
            }
        else:
            matched = ahl
    if not matched:
        matched = {"code": "AHL", "name": "Asarfi Hospital Limited", "facility_id": facility_id, "hip_id": hip_id}
    resolved_facility = facility_id or _clean(matched.get("facility_id"))
    resolved_hip = hip_id or _clean(matched.get("hip_id")) or resolved_facility
    return {
        "hospital_code": _clean(matched.get("code")).upper() or "AHL",
        "hospital_name": _clean(matched.get("name")) or _clean(matched.get("code")).upper() or "AHL",
        "facility_id": resolved_facility,
        "hip_id": resolved_hip,
    }


def _ensure_tables() -> None:
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            IF NOT EXISTS (
                SELECT 1 FROM sys.objects
                WHERE object_id = OBJECT_ID(N'dbo.HID_ABDM_ScanShare_Profile')
                  AND type in (N'U')
            )
            BEGIN
                CREATE TABLE dbo.HID_ABDM_ScanShare_Profile (
                    Id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
                    ShareRef NVARCHAR(64) NOT NULL,
                    HospitalCode NVARCHAR(20) NULL,
                    HospitalName NVARCHAR(120) NULL,
                    ReceivedAtUtc DATETIME2 NOT NULL,
                    CallbackPath NVARCHAR(300) NULL,
                    HeaderRequestId NVARCHAR(100) NULL,
                    RequestId NVARCHAR(100) NULL,
                    TransactionId NVARCHAR(100) NULL,
                    FacilityId NVARCHAR(100) NULL,
                    HipId NVARCHAR(100) NULL,
                    CounterId NVARCHAR(100) NULL,
                    PatientReference NVARCHAR(120) NULL,
                    CareContextReference NVARCHAR(120) NULL,
                    Uhid NVARCHAR(80) NULL,
                    MrNo NVARCHAR(80) NULL,
                    AbhaAddress NVARCHAR(120) NULL,
                    AbhaNumber NVARCHAR(40) NULL,
                    PatientName NVARCHAR(180) NULL,
                    Mobile NVARCHAR(20) NULL,
                    Gender NVARCHAR(10) NULL,
                    YearOfBirth INT NULL,
                    Status NVARCHAR(30) NOT NULL,
                    LinkingTokenCipher NVARCHAR(MAX) NULL,
                    RawJson NVARCHAR(MAX) NULL,
                    NormalizedJson NVARCHAR(MAX) NULL
                )
                CREATE INDEX IX_HID_ABDM_ScanShare_Profile_Time
                    ON dbo.HID_ABDM_ScanShare_Profile (ReceivedAtUtc DESC)
                CREATE INDEX IX_HID_ABDM_ScanShare_Profile_ABHA
                    ON dbo.HID_ABDM_ScanShare_Profile (AbhaAddress, AbhaNumber)
                CREATE INDEX IX_HID_ABDM_ScanShare_Profile_UHID
                    ON dbo.HID_ABDM_ScanShare_Profile (Uhid, MrNo)
            END

            IF NOT EXISTS (
                SELECT 1 FROM sys.objects
                WHERE object_id = OBJECT_ID(N'dbo.HID_ABDM_Patient_Linkage')
                  AND type in (N'U')
            )
            BEGIN
                CREATE TABLE dbo.HID_ABDM_Patient_Linkage (
                    Id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
                    LinkRef NVARCHAR(64) NOT NULL,
                    ScanShareId BIGINT NULL,
                    ShareRef NVARCHAR(64) NULL,
                    Source NVARCHAR(40) NOT NULL,
                    HospitalCode NVARCHAR(20) NULL,
                    HospitalName NVARCHAR(120) NULL,
                    FacilityId NVARCHAR(100) NULL,
                    HipId NVARCHAR(100) NULL,
                    Uhid NVARCHAR(80) NULL,
                    MrNo NVARCHAR(80) NULL,
                    PatientReference NVARCHAR(120) NULL,
                    CareContextReference NVARCHAR(120) NULL,
                    AbhaAddress NVARCHAR(120) NULL,
                    AbhaNumber NVARCHAR(40) NULL,
                    PatientName NVARCHAR(180) NULL,
                    Mobile NVARCHAR(20) NULL,
                    Status NVARCHAR(30) NOT NULL,
                    LinkingTokenCipher NVARCHAR(MAX) NULL,
                    RawJson NVARCHAR(MAX) NULL,
                    CreatedAtUtc DATETIME2 NOT NULL,
                    UpdatedAtUtc DATETIME2 NOT NULL
                )
                CREATE INDEX IX_HID_ABDM_Patient_Linkage_ABHA
                    ON dbo.HID_ABDM_Patient_Linkage (AbhaAddress, AbhaNumber)
                CREATE INDEX IX_HID_ABDM_Patient_Linkage_UHID
                    ON dbo.HID_ABDM_Patient_Linkage (Uhid, MrNo)
            END

            IF COL_LENGTH('dbo.HID_ABDM_ScanShare_Profile', 'LinkingTokenCipher') IS NULL
                ALTER TABLE dbo.HID_ABDM_ScanShare_Profile ADD LinkingTokenCipher NVARCHAR(MAX) NULL

            IF COL_LENGTH('dbo.HID_ABDM_ScanShare_Profile', 'HospitalCode') IS NULL
                ALTER TABLE dbo.HID_ABDM_ScanShare_Profile ADD HospitalCode NVARCHAR(20) NULL

            IF COL_LENGTH('dbo.HID_ABDM_ScanShare_Profile', 'HospitalName') IS NULL
                ALTER TABLE dbo.HID_ABDM_ScanShare_Profile ADD HospitalName NVARCHAR(120) NULL

            IF COL_LENGTH('dbo.HID_ABDM_Patient_Linkage', 'LinkingTokenCipher') IS NULL
                ALTER TABLE dbo.HID_ABDM_Patient_Linkage ADD LinkingTokenCipher NVARCHAR(MAX) NULL

            IF COL_LENGTH('dbo.HID_ABDM_Patient_Linkage', 'HospitalCode') IS NULL
                ALTER TABLE dbo.HID_ABDM_Patient_Linkage ADD HospitalCode NVARCHAR(20) NULL

            IF COL_LENGTH('dbo.HID_ABDM_Patient_Linkage', 'HospitalName') IS NULL
                ALTER TABLE dbo.HID_ABDM_Patient_Linkage ADD HospitalName NVARCHAR(120) NULL

            IF COL_LENGTH('dbo.HID_ABDM_Patient_Linkage', 'FacilityId') IS NULL
                ALTER TABLE dbo.HID_ABDM_Patient_Linkage ADD FacilityId NVARCHAR(100) NULL

            IF COL_LENGTH('dbo.HID_ABDM_Patient_Linkage', 'HipId') IS NULL
                ALTER TABLE dbo.HID_ABDM_Patient_Linkage ADD HipId NVARCHAR(100) NULL

            EXEC('UPDATE dbo.HID_ABDM_ScanShare_Profile
                     SET HospitalCode = ''AHL'',
                         HospitalName = COALESCE(NULLIF(HospitalName, ''''), ''Asarfi Hospital Limited'')
                   WHERE (HospitalCode IS NULL OR HospitalCode = '''')
                     AND (FacilityId = ''IN2010000816'' OR HipId = ''IN2010000816'')')

            EXEC('UPDATE dbo.HID_ABDM_Patient_Linkage
                     SET HospitalCode = ''AHL'',
                         HospitalName = COALESCE(NULLIF(HospitalName, ''''), ''Asarfi Hospital Limited''),
                         FacilityId = COALESCE(NULLIF(FacilityId, ''''), ''IN2010000816''),
                         HipId = COALESCE(NULLIF(HipId, ''''), ''IN2010000816'')
                   WHERE HospitalCode IS NULL OR HospitalCode = ''''')
            """
        )
        conn.commit()
    finally:
        conn.close()


def _upsert_linkage(cur, linkage: dict[str, Any]) -> None:
    hospital_code = _clean(linkage.get("hospital_code")) or "AHL"
    cur.execute(
        """
        SELECT TOP 1 Id
        FROM dbo.HID_ABDM_Patient_Linkage
        WHERE
            ISNULL(HospitalCode, 'AHL') = ?
            AND (
                (AbhaAddress IS NOT NULL AND AbhaAddress <> '' AND LOWER(AbhaAddress) = LOWER(?))
                OR (AbhaNumber IS NOT NULL AND AbhaNumber <> '' AND REPLACE(AbhaNumber, '-', '') = ?)
                OR (Uhid IS NOT NULL AND Uhid <> '' AND Uhid = ?)
                OR (MrNo IS NOT NULL AND MrNo <> '' AND MrNo = ?)
            )
        ORDER BY UpdatedAtUtc DESC
        """,
        (
            hospital_code,
            linkage.get("abha_address") or "__no_abha_address__",
            _digits_only(linkage.get("abha_number")) or "__no_abha_number__",
            linkage.get("uhid") or "__no_uhid__",
            linkage.get("mr") or "__no_mr__",
        ),
    )
    row = cur.fetchone()
    if row:
        cur.execute(
            """
            UPDATE dbo.HID_ABDM_Patient_Linkage
               SET ScanShareId = ?, ShareRef = ?, HospitalCode = ?, HospitalName = ?,
                   FacilityId = ?, HipId = ?, Uhid = ?, MrNo = ?, PatientReference = ?,
                   CareContextReference = ?, AbhaAddress = ?, AbhaNumber = ?, PatientName = ?,
                   Mobile = ?, Status = ?, LinkingTokenCipher = COALESCE(NULLIF(?, ''), LinkingTokenCipher),
                   RawJson = ?, UpdatedAtUtc = ?
             WHERE Id = ?
            """,
            (
                linkage.get("scan_share_id"),
                _clip(linkage.get("share_ref"), 64),
                _clip(hospital_code, 20),
                _clip(linkage.get("hospital_name"), 120),
                _clip(linkage.get("facility_id"), 100),
                _clip(linkage.get("hip_id"), 100),
                _clip(linkage.get("uhid"), 80),
                _clip(linkage.get("mr"), 80),
                _clip(linkage.get("patient_reference"), 120),
                _clip(linkage.get("care_context_reference"), 120),
                _clip(linkage.get("abha_address"), 120),
                _clip(linkage.get("abha_number"), 40),
                _clip(linkage.get("name"), 180),
                _clip(linkage.get("mobile"), 20),
                _clip(linkage.get("status"), 30),
                linkage.get("linking_token_cipher") or "",
                _json(linkage),
                _utc_now_naive(),
                int(row[0]),
            ),
        )
        return
    cur.execute(
        """
        INSERT INTO dbo.HID_ABDM_Patient_Linkage (
            LinkRef, ScanShareId, ShareRef, Source, HospitalCode, HospitalName,
            FacilityId, HipId, Uhid, MrNo, PatientReference,
            CareContextReference, AbhaAddress, AbhaNumber, PatientName, Mobile,
            Status, LinkingTokenCipher, RawJson, CreatedAtUtc, UpdatedAtUtc
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            str(uuid.uuid4()),
            linkage.get("scan_share_id"),
            _clip(linkage.get("share_ref"), 64),
            "scan_share",
            _clip(hospital_code, 20),
            _clip(linkage.get("hospital_name"), 120),
            _clip(linkage.get("facility_id"), 100),
            _clip(linkage.get("hip_id"), 100),
            _clip(linkage.get("uhid"), 80),
            _clip(linkage.get("mr"), 80),
            _clip(linkage.get("patient_reference"), 120),
            _clip(linkage.get("care_context_reference"), 120),
            _clip(linkage.get("abha_address"), 120),
            _clip(linkage.get("abha_number"), 40),
            _clip(linkage.get("name"), 180),
            _clip(linkage.get("mobile"), 20),
            _clip(linkage.get("status"), 30),
            linkage.get("linking_token_cipher") or "",
            _json(linkage),
            _utc_now_naive(),
            _utc_now_naive(),
        ),
    )


def _fetch_scan_row(cur, *, scan_share_id: int | None = None, share_ref: str = "") -> dict[str, Any] | None:
    if scan_share_id:
        cur.execute("SELECT TOP 1 * FROM dbo.HID_ABDM_ScanShare_Profile WHERE Id = ?", (scan_share_id,))
    else:
        cur.execute("SELECT TOP 1 * FROM dbo.HID_ABDM_ScanShare_Profile WHERE ShareRef = ?", (share_ref,))
    columns = [col[0] for col in cur.description]
    row = cur.fetchone()
    return dict(zip(columns, row)) if row else None


def _find_existing_linkage(cur, normalized: dict[str, Any]) -> dict[str, Any] | None:
    hospital_code = _clean(normalized.get("hospital_code")) or "AHL"
    abha_address = _clean(normalized.get("abha_address"))
    abha_number = _digits_only(normalized.get("abha_number"))
    mobile = _digits_only(normalized.get("mobile"))
    clauses = ["ISNULL(HospitalCode, 'AHL') = ?"]
    params: list[Any] = [hospital_code]
    match_parts = []
    if abha_address:
        match_parts.append("(AbhaAddress IS NOT NULL AND AbhaAddress <> '' AND LOWER(AbhaAddress) = LOWER(?))")
        params.append(abha_address)
    if abha_number:
        match_parts.append("(AbhaNumber IS NOT NULL AND AbhaNumber <> '' AND REPLACE(AbhaNumber, '-', '') = ?)")
        params.append(abha_number)
    if mobile:
        match_parts.append("(Mobile IS NOT NULL AND Mobile <> '' AND Mobile = ?)")
        params.append(mobile)
    if not match_parts:
        return None
    clauses.append("(" + " OR ".join(match_parts) + ")")
    cur.execute(
        f"""
        SELECT TOP 1 *
        FROM dbo.HID_ABDM_Patient_Linkage
        WHERE {' AND '.join(clauses)}
        ORDER BY
            CASE WHEN (Uhid IS NOT NULL AND Uhid <> '') OR (MrNo IS NOT NULL AND MrNo <> '') THEN 0 ELSE 1 END,
            UpdatedAtUtc DESC
        """,
        tuple(params),
    )
    row = cur.fetchone()
    if not row:
        return None
    columns = [col[0] for col in cur.description]
    return dict(zip(columns, row))


def _linkage_from_normalized(normalized: dict[str, Any], *, match: dict[str, Any] | None = None, force_linked: bool = False) -> dict[str, Any]:
    matched_uhid = _clean((match or {}).get("Uhid"))
    matched_mr = _clean((match or {}).get("MrNo"))
    matched_patient_ref = _clean((match or {}).get("PatientReference"))
    matched_cc_ref = _clean((match or {}).get("CareContextReference"))
    has_local_id = bool(normalized.get("uhid") or normalized.get("mr"))
    if _clean(normalized.get("hospital_code")).upper() == "UNMAPPED":
        status = "unmapped_facility"
    elif force_linked or has_local_id:
        status = "linked"
    elif matched_uhid or matched_mr:
        status = "returning_matched"
    elif match:
        status = "duplicate_needs_review"
    else:
        status = "new_pending_registration"
    return {
        "status": status,
        "hospital_code": normalized.get("hospital_code") or "AHL",
        "hospital_name": normalized.get("hospital_name") or "",
        "facility_id": normalized.get("facility_id") or "",
        "hip_id": normalized.get("hip_id") or "",
        "uhid": normalized.get("uhid") or matched_uhid or "",
        "mr": normalized.get("mr") or normalized.get("uhid") or matched_mr or matched_uhid or "",
        "patient_reference": normalized.get("patient_reference") or matched_patient_ref or "",
        "care_context_reference": normalized.get("care_context_reference") or matched_cc_ref or "",
        "abha_address": normalized.get("abha_address") or "",
        "abha_number": normalized.get("abha_number") or "",
        "name": normalized.get("name") or "",
        "mobile": normalized.get("mobile") or "",
        "match_ref": (match or {}).get("LinkRef") or "",
    }


def _connect():
    conn = get_sql_connection("ACI")
    if not conn:
        raise RuntimeError("Unable to connect to ACI database for ABDM Scan & Share storage.")
    return conn


def _first_dict(source: dict[str, Any], *keys: str) -> dict[str, Any]:
    for key in keys:
        value = source.get(key)
        if isinstance(value, dict):
            return value
    return {}


def _find_any(source: Any, *keys: str) -> Any:
    wanted = {key.lower() for key in keys}
    if isinstance(source, dict):
        for key, value in source.items():
            if str(key).lower() in wanted:
                return value
        for value in source.values():
            found = _find_any(value, *keys)
            if found not in ("", None, [], {}):
                return found
    if isinstance(source, list):
        for item in source:
            found = _find_any(item, *keys)
            if found not in ("", None, [], {}):
                return found
    return ""


def _header_request_id(headers: dict[str, str]) -> str:
    for key in ("REQUEST-ID", "X-Request-ID", "X-Correlation-ID"):
        if headers.get(key):
            return str(headers.get(key) or "")
    return ""


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, default=str)


def _redact_linking_token(value: Any) -> Any:
    if isinstance(value, dict):
        output = {}
        for key, item in value.items():
            if str(key).lower().replace("_", "") in {"linkingtoken", "linktoken"}:
                output[key] = "***"
            else:
                output[key] = _redact_linking_token(item)
        return output
    if isinstance(value, list):
        return [_redact_linking_token(item) for item in value]
    return value


def _nested_value(source: dict[str, Any], key: str, nested_key: str) -> Any:
    value = source.get(key) if isinstance(source, dict) else None
    if not isinstance(value, dict):
        return ""
    return value.get(nested_key)


def _json_or_dict(value: Any) -> Any:
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    try:
        return json.loads(str(value))
    except Exception:
        return {}


def _lower_keys(item: dict[str, Any]) -> dict[str, Any]:
    return {str(key[:1]).lower() + str(key[1:]): value for key, value in item.items()}


def _clip(value: Any, limit: int) -> str:
    return str(value or "").strip()[:limit]


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _digits_only(value: Any) -> str:
    return "".join(ch for ch in str(value or "") if ch.isdigit())


def _safe_int_or_none(value: Any) -> int | None:
    try:
        text = str(value or "").strip()
        return int(text) if text else None
    except Exception:
        return None


def _camel(value: str) -> str:
    parts = str(value or "").split("_")
    return parts[0] + "".join(part[:1].upper() + part[1:] for part in parts[1:])


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _utc_now_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)
