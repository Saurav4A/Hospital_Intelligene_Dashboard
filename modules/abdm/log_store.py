from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from typing import Any

from flask import has_request_context, request, session

from modules.db_connection import get_sql_connection


SENSITIVE_KEY_PARTS = {
    "aadhaar",
    "aadhar",
    "otp",
    "token",
    "secret",
    "password",
    "authorization",
    "x-token",
    "xtoken",
    "profilephoto",
    "photo",
    "mobile",
    "phone",
    "abhanumber",
    "healthidnumber",
    "healthid",
    "loginid",
}


def log_abdm_event(
    *,
    category: str,
    action: str,
    status: str = "success",
    direction: str = "",
    method: str = "",
    url: str = "",
    http_status: int | None = None,
    request_id: str = "",
    correlation_id: str = "",
    entity_type: str = "",
    entity_id: str = "",
    summary: str = "",
    request_payload: Any = None,
    response_payload: Any = None,
    error_message: str = "",
    duration_ms: int | None = None,
    username: str = "",
) -> None:
    try:
        _ensure_table()
        user, ip_addr, user_agent = _request_context_values(username)
        conn = _connect()
        try:
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO dbo.HID_ABDM_Process_Log (
                    EventTimeUtc, Category, Action, Status, Direction,
                    Method, Url, HttpStatus, RequestId, CorrelationId,
                    Username, EntityType, EntityId, Summary,
                    RequestJson, ResponseJson, ErrorMessage, DurationMs,
                    IpAddress, UserAgent
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    _utc_now().replace(tzinfo=None),
                    _clip(category, 60),
                    _clip(action, 100),
                    _clip(status, 30),
                    _clip(direction, 20),
                    _clip(method, 12),
                    _clip(url, 500),
                    http_status,
                    _clip(request_id, 80),
                    _clip(correlation_id, 100),
                    _clip(user, 128),
                    _clip(entity_type, 60),
                    _clip(entity_id, 120),
                    _clip(summary, 500),
                    _json(request_payload),
                    _json(response_payload),
                    _clip(error_message, 1000),
                    duration_ms,
                    _clip(ip_addr, 60),
                    _clip(user_agent, 255),
                ),
            )
            conn.commit()
        finally:
            conn.close()
    except Exception as exc:
        print(f"ABDM process log failed: {exc}")


def recent_abdm_logs(limit: int = 100) -> list[dict[str, Any]]:
    try:
        _ensure_table()
        conn = _connect()
        try:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT TOP (?) LogId, EventTimeUtc, Category, Action, Status,
                       Direction, Method, Url, HttpStatus, RequestId,
                       CorrelationId, Username, EntityType, EntityId,
                       Summary, ErrorMessage, DurationMs
                FROM dbo.HID_ABDM_Process_Log
                ORDER BY LogId DESC
                """,
                (max(1, min(500, int(limit or 100))),),
            )
            columns = [col[0] for col in cur.description]
            logs = []
            for row in cur.fetchall():
                item = dict(zip(columns, row))
                item["EventTimeUtc"] = item["EventTimeUtc"].isoformat() if item.get("EventTimeUtc") else ""
                logs.append(_lower_keys(item))
            return logs
        finally:
            conn.close()
    except Exception as exc:
        print(f"ABDM process log fetch failed: {exc}")
        return []


def timed_ms(start: float) -> int:
    return int(max(0, (time.time() - start) * 1000))


def _ensure_table() -> None:
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            IF NOT EXISTS (
                SELECT 1 FROM sys.objects
                WHERE object_id = OBJECT_ID(N'dbo.HID_ABDM_Process_Log')
                  AND type in (N'U')
            )
            BEGIN
                CREATE TABLE dbo.HID_ABDM_Process_Log (
                    LogId BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
                    EventTimeUtc DATETIME2 NOT NULL,
                    Category NVARCHAR(60) NOT NULL,
                    Action NVARCHAR(100) NOT NULL,
                    Status NVARCHAR(30) NOT NULL,
                    Direction NVARCHAR(20) NULL,
                    Method NVARCHAR(12) NULL,
                    Url NVARCHAR(500) NULL,
                    HttpStatus INT NULL,
                    RequestId NVARCHAR(80) NULL,
                    CorrelationId NVARCHAR(100) NULL,
                    Username NVARCHAR(128) NULL,
                    EntityType NVARCHAR(60) NULL,
                    EntityId NVARCHAR(120) NULL,
                    Summary NVARCHAR(500) NULL,
                    RequestJson NVARCHAR(MAX) NULL,
                    ResponseJson NVARCHAR(MAX) NULL,
                    ErrorMessage NVARCHAR(1000) NULL,
                    DurationMs INT NULL,
                    IpAddress NVARCHAR(60) NULL,
                    UserAgent NVARCHAR(255) NULL
                )
                CREATE INDEX IX_HID_ABDM_Process_Log_Time
                    ON dbo.HID_ABDM_Process_Log (EventTimeUtc DESC)
                CREATE INDEX IX_HID_ABDM_Process_Log_Action
                    ON dbo.HID_ABDM_Process_Log (Action, Status)
                CREATE INDEX IX_HID_ABDM_Process_Log_Request
                    ON dbo.HID_ABDM_Process_Log (RequestId)
            END
            """
        )
        conn.commit()
    finally:
        conn.close()


def _connect():
    conn = get_sql_connection("ACI")
    if not conn:
        raise RuntimeError("Unable to connect to ACI database for ABDM process logging.")
    return conn


def _request_context_values(username: str) -> tuple[str, str, str]:
    if not has_request_context():
        return username or "", "", ""
    user = username or session.get("username") or session.get("user") or ""
    return user, request.remote_addr or "", request.headers.get("User-Agent") or ""


def _json(value: Any) -> str | None:
    if value is None:
        return None
    try:
        return json.dumps(_sanitize(value), ensure_ascii=True, default=str)
    except Exception:
        return json.dumps(_clip(str(value), 2000), ensure_ascii=True)


def _sanitize(value: Any) -> Any:
    if isinstance(value, dict):
        output = {}
        for key, item in value.items():
            normalised = _normalise_key(key)
            if any(part in normalised for part in SENSITIVE_KEY_PARTS):
                output[key] = _mask_value(item, normalised)
            else:
                output[key] = _sanitize(item)
        return output
    if isinstance(value, list):
        return [_sanitize(item) for item in value[:50]]
    if isinstance(value, str) and len(value) > 600:
        return value[:240] + "..." + value[-120:]
    return value


def _mask_value(value: Any, key: str) -> str:
    text = str(value or "")
    if "aadhaar" in key or "aadhar" in key:
        digits = "".join(ch for ch in text if ch.isdigit())
        return "********" + digits[-4:] if len(digits) >= 4 else "***"
    if "otp" in key:
        return "***"
    if "mobile" in key:
        digits = "".join(ch for ch in text if ch.isdigit())
        return "******" + digits[-4:] if len(digits) >= 4 else "***"
    if "abhanumber" in key or "healthidnumber" in key or key == "healthid":
        digits = "".join(ch for ch in text if ch.isdigit())
        return "**********" + digits[-4:] if len(digits) >= 4 else "***"
    if len(text) <= 12:
        return "***"
    return text[:4] + "..." + text[-4:]


def _normalise_key(value: Any) -> str:
    return "".join(ch for ch in str(value or "").lower() if ch.isalnum() or ch == "-")


def _clip(value: Any, max_len: int) -> str:
    text = str(value or "")
    if len(text) <= max_len:
        return text
    return text[: max(0, max_len - 3)] + "..."


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _lower_keys(value: dict[str, Any]) -> dict[str, Any]:
    return {str(key[:1]).lower() + str(key[1:]): item for key, item in value.items()}
