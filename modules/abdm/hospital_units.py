from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import config as app_config

from modules.db_connection import get_sql_connection


def list_hospital_units() -> list[dict[str, Any]]:
    _ensure_table()
    _seed_defaults()
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT UnitCode, UnitName, FacilityId, HipId, CounterId, HrpType,
                   IsActive, LastRegisterStatus, LastRegisterMessage, LastRegisteredAtUtc,
                   CreatedAtUtc, UpdatedAtUtc
            FROM dbo.HID_ABDM_Hospital_Unit
            ORDER BY SortOrder, UnitCode
            """
        )
        rows = []
        for row in cur.fetchall():
            rows.append(
                {
                    "code": row[0],
                    "name": row[1],
                    "facility_id": row[2],
                    "hip_id": row[3],
                    "counter_id": row[4],
                    "hrp_type": row[5] or "HIP",
                    "active": bool(row[6]),
                    "last_register_status": row[7] or "",
                    "last_register_message": row[8] or "",
                    "last_registered_at": row[9].isoformat() if row[9] else "",
                    "created_at": row[10].isoformat() if row[10] else "",
                    "updated_at": row[11].isoformat() if row[11] else "",
                }
            )
        return rows
    finally:
        conn.close()


def upsert_hospital_unit(payload: dict[str, Any]) -> dict[str, Any]:
    _ensure_table()
    code = _clean(payload.get("code") or payload.get("hospital_code") or payload.get("unitCode")).upper()
    if not code:
        raise ValueError("Hospital unit code is required.")
    name = _clean(payload.get("name") or payload.get("unit_name") or payload.get("unitName") or code)
    facility_id = _clean(payload.get("facility_id") or payload.get("facilityId"))
    hip_id = _clean(payload.get("hip_id") or payload.get("hipId") or facility_id)
    counter_id = _clean(payload.get("counter_id") or payload.get("counterId") or "REG01")
    hrp_type = _clean(payload.get("hrp_type") or payload.get("hrpType") or payload.get("type") or "HIP").upper()
    active = _bool(payload.get("active", bool(facility_id or hip_id)))
    now = _utc_now()
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute("SELECT TOP 1 UnitCode FROM dbo.HID_ABDM_Hospital_Unit WHERE UnitCode = ?", (code,))
        exists = cur.fetchone()
        if exists:
            cur.execute(
                """
                UPDATE dbo.HID_ABDM_Hospital_Unit
                   SET UnitName = ?, FacilityId = ?, HipId = ?, CounterId = ?,
                       HrpType = ?, IsActive = ?, UpdatedAtUtc = ?
                 WHERE UnitCode = ?
                """,
                (name, facility_id, hip_id, counter_id, hrp_type, int(active), now, code),
            )
        else:
            cur.execute(
                """
                INSERT INTO dbo.HID_ABDM_Hospital_Unit (
                    UnitCode, UnitName, FacilityId, HipId, CounterId, HrpType,
                    IsActive, SortOrder, CreatedAtUtc, UpdatedAtUtc
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (code, name, facility_id, hip_id, counter_id, hrp_type, int(active), _sort_order(code), now, now),
            )
        conn.commit()
    finally:
        conn.close()
    return get_hospital_unit(code)


def get_hospital_unit(code: str) -> dict[str, Any]:
    target = _clean(code).upper()
    for unit in list_hospital_units():
        if str(unit.get("code") or "").upper() == target:
            return unit
    return {}


def mark_unit_registration(code: str, *, status: str, message: str = "") -> None:
    _ensure_table()
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE dbo.HID_ABDM_Hospital_Unit
               SET LastRegisterStatus = ?, LastRegisterMessage = ?, LastRegisteredAtUtc = ?, UpdatedAtUtc = ?
             WHERE UnitCode = ?
            """,
            (_clean(status)[:40], _clean(message)[:800], _utc_now(), _utc_now(), _clean(code).upper()),
        )
        conn.commit()
    finally:
        conn.close()


def _ensure_table() -> None:
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            IF NOT EXISTS (
                SELECT 1 FROM sys.objects
                WHERE object_id = OBJECT_ID(N'dbo.HID_ABDM_Hospital_Unit')
                  AND type in (N'U')
            )
            BEGIN
                CREATE TABLE dbo.HID_ABDM_Hospital_Unit (
                    UnitCode NVARCHAR(20) NOT NULL PRIMARY KEY,
                    UnitName NVARCHAR(160) NOT NULL,
                    FacilityId NVARCHAR(100) NULL,
                    HipId NVARCHAR(100) NULL,
                    CounterId NVARCHAR(100) NULL,
                    HrpType NVARCHAR(20) NOT NULL,
                    IsActive BIT NOT NULL,
                    SortOrder INT NOT NULL,
                    LastRegisterStatus NVARCHAR(40) NULL,
                    LastRegisterMessage NVARCHAR(800) NULL,
                    LastRegisteredAtUtc DATETIME2 NULL,
                    CreatedAtUtc DATETIME2 NOT NULL,
                    UpdatedAtUtc DATETIME2 NOT NULL
                )
            END
            """
        )
        conn.commit()
    finally:
        conn.close()


def _seed_defaults() -> None:
    defaults = getattr(app_config, "ABDM_HOSPITAL_UNITS", []) or []
    conn = _connect()
    try:
        cur = conn.cursor()
        for item in defaults:
            if not isinstance(item, dict):
                continue
            code = _clean(item.get("code")).upper()
            if not code:
                continue
            cur.execute("SELECT TOP 1 UnitCode FROM dbo.HID_ABDM_Hospital_Unit WHERE UnitCode = ?", (code,))
            if cur.fetchone():
                default_name = _clean(item.get("name")) or code
                default_facility_id = _clean(item.get("facility_id"))
                default_hip_id = _clean(item.get("hip_id") or item.get("facility_id"))
                default_counter_id = _clean(item.get("counter_id")) or "REG01"
                default_hrp_type = _clean(item.get("hrp_type") or item.get("type") or "HIP").upper()
                default_active = int(_bool(item.get("active", bool(default_facility_id))))
                cur.execute(
                    """
                    UPDATE dbo.HID_ABDM_Hospital_Unit
                       SET UnitName = CASE WHEN NULLIF(LTRIM(RTRIM(UnitName)), '') IS NULL THEN ? ELSE UnitName END,
                           FacilityId = CASE WHEN NULLIF(LTRIM(RTRIM(FacilityId)), '') IS NULL THEN ? ELSE FacilityId END,
                           HipId = CASE WHEN NULLIF(LTRIM(RTRIM(HipId)), '') IS NULL THEN ? ELSE HipId END,
                           CounterId = CASE WHEN NULLIF(LTRIM(RTRIM(CounterId)), '') IS NULL THEN ? ELSE CounterId END,
                           HrpType = CASE WHEN NULLIF(LTRIM(RTRIM(HrpType)), '') IS NULL THEN ? ELSE HrpType END,
                           IsActive = CASE
                               WHEN NULLIF(LTRIM(RTRIM(FacilityId)), '') IS NULL AND ? = 1 THEN 1
                               ELSE IsActive
                           END,
                           UpdatedAtUtc = ?
                     WHERE UnitCode = ?
                    """,
                    (
                        default_name,
                        default_facility_id,
                        default_hip_id,
                        default_counter_id,
                        default_hrp_type,
                        default_active,
                        _utc_now(),
                        code,
                    ),
                )
                continue
            cur.execute(
                """
                INSERT INTO dbo.HID_ABDM_Hospital_Unit (
                    UnitCode, UnitName, FacilityId, HipId, CounterId, HrpType,
                    IsActive, SortOrder, CreatedAtUtc, UpdatedAtUtc
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    code,
                    _clean(item.get("name")) or code,
                    _clean(item.get("facility_id")),
                    _clean(item.get("hip_id") or item.get("facility_id")),
                    _clean(item.get("counter_id")) or "REG01",
                    _clean(item.get("hrp_type") or item.get("type") or "HIP").upper(),
                    int(_bool(item.get("active", bool(_clean(item.get("facility_id")))))),
                    _sort_order(code),
                    _utc_now(),
                    _utc_now(),
                ),
            )
        conn.commit()
    finally:
        conn.close()


def _connect():
    conn = get_sql_connection("ACI")
    if not conn:
        raise RuntimeError("Unable to connect to ACI database for ABDM hospital units.")
    return conn


def _sort_order(code: str) -> int:
    order = {"AHL": 10, "ACI": 20, "BALLIA": 30}
    return order.get(_clean(code).upper(), 100)


def _bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    return text in {"1", "true", "yes", "on", "active"}


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)
