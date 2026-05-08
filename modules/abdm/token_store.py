from __future__ import annotations

import base64
import hashlib
import json
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

import config
from modules.db_connection import get_sql_connection
from .log_store import log_abdm_event


def save_abha_session(
    *,
    username: str,
    token: str,
    refresh_token: str = "",
    expires_in: int | str | None = None,
    profile: dict[str, Any] | None = None,
) -> str:
    if not token:
        raise ValueError("ABHA X-token is required.")
    _ensure_table()
    session_ref = str(uuid.uuid4())
    now = _utc_now()
    expires_at = now + timedelta(seconds=_safe_int(expires_in, 1800))
    profile = profile or {}
    abha_number = _find_value(profile, {"abhanumber", "abha_number", "healthidnumber", "healthid"})
    abha_address = _find_value(profile, {"preferredabhaaddress", "abhaaddress", "phraddress"})
    profile_json = json.dumps(_preview_large_strings(profile), ensure_ascii=True, default=str)
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO dbo.HID_ABDM_ABHA_Sessions (
                SessionRef, Username, AbhaNumber, AbhaAddress,
                TokenCipher, RefreshTokenCipher, ExpiresAtUtc,
                ProfileJson, CreatedAtUtc, LastUsedAtUtc, IsActive
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
            """,
            (
                session_ref,
                username or "",
                abha_number,
                abha_address,
                _encrypt(token),
                _encrypt(refresh_token) if refresh_token else "",
                expires_at.replace(tzinfo=None),
                profile_json,
                now.replace(tzinfo=None),
                now.replace(tzinfo=None),
            ),
        )
        conn.commit()
    finally:
        conn.close()
    log_abdm_event(
        category="token_store",
        action="abha_session_saved",
        status="success",
        direction="internal",
        entity_type="abha_session",
        entity_id=session_ref,
        summary="Verified ABHA session token stored in ACI.",
        request_payload={"abha_number": abha_number, "abha_address": abha_address, "expires_at_utc": expires_at},
        username=username or "",
    )
    return session_ref


def get_abha_session(session_ref: str, *, username: str = "") -> dict[str, Any]:
    ref = str(session_ref or "").strip()
    if not ref:
        raise ValueError("ABHA session reference is required.")
    _ensure_table()
    conn = _connect()
    try:
        cur = conn.cursor()
        if username:
            cur.execute(
                """
                SELECT TOP 1 SessionRef, Username, AbhaNumber, AbhaAddress,
                       TokenCipher, RefreshTokenCipher, ExpiresAtUtc, ProfileJson
                FROM dbo.HID_ABDM_ABHA_Sessions
                WHERE SessionRef = ? AND Username = ? AND IsActive = 1
                ORDER BY CreatedAtUtc DESC
                """,
                (ref, username),
            )
        else:
            cur.execute(
                """
                SELECT TOP 1 SessionRef, Username, AbhaNumber, AbhaAddress,
                       TokenCipher, RefreshTokenCipher, ExpiresAtUtc, ProfileJson
                FROM dbo.HID_ABDM_ABHA_Sessions
                WHERE SessionRef = ? AND IsActive = 1
                ORDER BY CreatedAtUtc DESC
                """,
                (ref,),
            )
        row = cur.fetchone()
        if not row:
            raise ValueError("ABHA session was not found. Please login again.")
        expires_at = row[6]
        if expires_at and _utc_now().replace(tzinfo=None) > expires_at:
            raise ValueError("ABHA session has expired. Please login again.")
        cur.execute(
            "UPDATE dbo.HID_ABDM_ABHA_Sessions SET LastUsedAtUtc = ? WHERE SessionRef = ?",
            (_utc_now().replace(tzinfo=None), ref),
        )
        conn.commit()
        log_abdm_event(
            category="token_store",
            action="abha_session_loaded",
            status="success",
            direction="internal",
            entity_type="abha_session",
            entity_id=ref,
            summary="ABHA session token loaded from ACI for ABDM action.",
            username=username or "",
        )
        return {
            "session_ref": row[0],
            "username": row[1],
            "abha_number": row[2],
            "abha_address": row[3],
            "token": _decrypt(row[4]),
            "refresh_token": _decrypt(row[5]) if row[5] else "",
            "expires_at": expires_at.isoformat() if expires_at else "",
            "profile": _json_or_empty(row[7]),
        }
    finally:
        conn.close()


def get_latest_abha_session(
    *,
    username: str = "",
    abha_number: str = "",
    abha_address: str = "",
    exclude_session_ref: str = "",
) -> dict[str, Any]:
    _ensure_table()
    conn = _connect()
    try:
        cur = conn.cursor()
        clauses = ["IsActive = 1"]
        params: list[Any] = []
        if username:
            clauses.append("Username = ?")
            params.append(username)
        if abha_number:
            clauses.append("REPLACE(AbhaNumber, '-', '') = ?")
            params.append(_digits_only(abha_number))
        if abha_address:
            clauses.append("LOWER(ISNULL(AbhaAddress, '')) = LOWER(?)")
            params.append(str(abha_address or "").strip())
        if exclude_session_ref:
            clauses.append("SessionRef <> ?")
            params.append(exclude_session_ref)
        cur.execute(
            f"""
            SELECT TOP 1 SessionRef
            FROM dbo.HID_ABDM_ABHA_Sessions
            WHERE {' AND '.join(clauses)}
            ORDER BY CreatedAtUtc DESC
            """,
            tuple(params),
        )
        row = cur.fetchone()
        if not row:
            raise ValueError("No alternate ABHA session was found.")
        return get_abha_session(row[0], username=username)
    finally:
        conn.close()


def get_latest_abha_session_by_token_type(
    *,
    token_type: str,
    username: str = "",
    abha_number: str = "",
    abha_address: str = "",
) -> dict[str, Any]:
    target = str(token_type or "").strip().lower()
    if not target:
        raise ValueError("token_type is required.")
    _ensure_table()
    conn = _connect()
    try:
        cur = conn.cursor()
        clauses = ["IsActive = 1"]
        params: list[Any] = []
        if username:
            clauses.append("Username = ?")
            params.append(username)
        if abha_number:
            clauses.append("REPLACE(AbhaNumber, '-', '') = ?")
            params.append(_digits_only(abha_number))
        if abha_address:
            clauses.append("LOWER(ISNULL(AbhaAddress, '')) = LOWER(?)")
            params.append(str(abha_address or "").strip())
        cur.execute(
            f"""
            SELECT TOP 20 SessionRef, TokenCipher
            FROM dbo.HID_ABDM_ABHA_Sessions
            WHERE {' AND '.join(clauses)}
            ORDER BY CreatedAtUtc DESC
            """,
            tuple(params),
        )
        for row in cur.fetchall():
            token = _decrypt(row[1])
            if _jwt_type(token) == target:
                return get_abha_session(row[0], username=username)
        raise ValueError(f"No ABHA session with token type {token_type} was found.")
    finally:
        conn.close()


def get_recent_abha_sessions(
    *,
    username: str = "",
    abha_number: str = "",
    abha_address: str = "",
    limit: int = 20,
) -> list[dict[str, Any]]:
    _ensure_table()
    conn = _connect()
    try:
        cur = conn.cursor()
        clauses = ["IsActive = 1"]
        params: list[Any] = []
        if username:
            clauses.append("Username = ?")
            params.append(username)
        if abha_number:
            clauses.append("REPLACE(AbhaNumber, '-', '') = ?")
            params.append(_digits_only(abha_number))
        if abha_address:
            clauses.append("LOWER(ISNULL(AbhaAddress, '')) = LOWER(?)")
            params.append(str(abha_address or "").strip())
        params.insert(0, max(1, min(50, int(limit or 20))))
        cur.execute(
            f"""
            SELECT TOP (?) SessionRef
            FROM dbo.HID_ABDM_ABHA_Sessions
            WHERE {' AND '.join(clauses)}
            ORDER BY CreatedAtUtc DESC
            """,
            tuple(params),
        )
        sessions: list[dict[str, Any]] = []
        for row in cur.fetchall():
            try:
                sessions.append(get_abha_session(row[0], username=username))
            except Exception:
                continue
        return sessions
    finally:
        conn.close()


def update_abha_session_profile(session_ref: str, profile: dict[str, Any]) -> None:
    ref = str(session_ref or "").strip()
    if not ref:
        return
    _ensure_table()
    abha_number = _find_value(profile, {"abhanumber", "abha_number", "healthidnumber", "healthid"})
    abha_address = _find_value(profile, {"preferredabhaaddress", "abhaaddress", "phraddress"})
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE dbo.HID_ABDM_ABHA_Sessions
            SET AbhaNumber = COALESCE(NULLIF(?, ''), AbhaNumber),
                AbhaAddress = COALESCE(NULLIF(?, ''), AbhaAddress),
                ProfileJson = ?,
                LastUsedAtUtc = ?
            WHERE SessionRef = ?
            """,
            (
                abha_number,
                abha_address,
                json.dumps(_preview_large_strings(profile), ensure_ascii=True, default=str),
                _utc_now().replace(tzinfo=None),
                ref,
            ),
        )
        conn.commit()
    finally:
        conn.close()
    log_abdm_event(
        category="token_store",
        action="abha_session_profile_updated",
        status="success",
        direction="internal",
        entity_type="abha_session",
        entity_id=ref,
        summary="ABHA session profile snapshot updated in ACI.",
        request_payload={"abha_number": abha_number, "abha_address": abha_address},
    )


def _ensure_table() -> None:
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            IF NOT EXISTS (
                SELECT 1 FROM sys.objects
                WHERE object_id = OBJECT_ID(N'dbo.HID_ABDM_ABHA_Sessions')
                  AND type in (N'U')
            )
            BEGIN
                CREATE TABLE dbo.HID_ABDM_ABHA_Sessions (
                    SessionRef NVARCHAR(64) NOT NULL PRIMARY KEY,
                    Username NVARCHAR(128) NULL,
                    AbhaNumber NVARCHAR(64) NULL,
                    AbhaAddress NVARCHAR(255) NULL,
                    TokenCipher NVARCHAR(MAX) NOT NULL,
                    RefreshTokenCipher NVARCHAR(MAX) NULL,
                    ExpiresAtUtc DATETIME2 NULL,
                    ProfileJson NVARCHAR(MAX) NULL,
                    CreatedAtUtc DATETIME2 NOT NULL,
                    LastUsedAtUtc DATETIME2 NULL,
                    IsActive BIT NOT NULL DEFAULT 1
                )
                CREATE INDEX IX_HID_ABDM_ABHA_Sessions_Username
                    ON dbo.HID_ABDM_ABHA_Sessions (Username, CreatedAtUtc DESC)
            END
            """
        )
        conn.commit()
    finally:
        conn.close()


def _connect():
    conn = get_sql_connection("ACI")
    if not conn:
        raise RuntimeError("Unable to connect to ACI database for ABDM token storage.")
    return conn


def _encrypt(value: str) -> str:
    text = str(value or "")
    if not text:
        return ""
    try:
        from cryptography.fernet import Fernet

        return Fernet(_fernet_key()).encrypt(text.encode("utf-8")).decode("ascii")
    except Exception:
        return "plain:" + base64.b64encode(text.encode("utf-8")).decode("ascii")


def _decrypt(value: str) -> str:
    text = str(value or "")
    if not text:
        return ""
    if text.startswith("plain:"):
        return base64.b64decode(text[6:].encode("ascii")).decode("utf-8")
    try:
        from cryptography.fernet import Fernet

        return Fernet(_fernet_key()).decrypt(text.encode("ascii")).decode("utf-8")
    except Exception:
        return ""


def _fernet_key() -> bytes:
    secret = str(getattr(config, "SECRET_KEY", "") or "hid-abdm-local-secret").encode("utf-8")
    digest = hashlib.sha256(secret).digest()
    return base64.urlsafe_b64encode(digest)


def _safe_int(value: Any, default: int) -> int:
    try:
        return max(60, int(value))
    except Exception:
        return default


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _json_or_empty(value: Any) -> dict[str, Any]:
    try:
        parsed = json.loads(value or "{}")
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def _normalise_key(value: str) -> str:
    return "".join(ch for ch in str(value or "").lower() if ch.isalnum())


def _find_value(value: Any, keys: set[str]) -> str:
    if isinstance(value, dict):
        for key, item in value.items():
            if _normalise_key(key) in keys and item not in (None, "") and not isinstance(item, (dict, list)):
                return str(item)
        for item in value.values():
            found = _find_value(item, keys)
            if found:
                return found
    if isinstance(value, list):
        for item in value:
            found = _find_value(item, keys)
            if found:
                return found
    return ""


def _digits_only(value: Any) -> str:
    return "".join(ch for ch in str(value or "") if ch.isdigit())


def _jwt_type(token: str) -> str:
    text = str(token or "").strip()
    if text.lower().startswith("bearer "):
        text = text[7:].strip()
    parts = text.split(".")
    if len(parts) != 3:
        return ""
    try:
        payload = parts[1] + "=" * (-len(parts[1]) % 4)
        parsed = json.loads(base64.urlsafe_b64decode(payload.encode("ascii")).decode("utf-8"))
        return str(parsed.get("typ") or parsed.get("type") or "").strip().lower() if isinstance(parsed, dict) else ""
    except Exception:
        return ""


def _preview_large_strings(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _preview_large_strings(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_preview_large_strings(item) for item in value]
    if isinstance(value, str) and len(value) > 500:
        return value[:180] + "..." + value[-80:]
    return value
