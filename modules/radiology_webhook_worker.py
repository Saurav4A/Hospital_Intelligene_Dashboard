"""
Background worker that posts AHL RadiologyOrderList rows to the RPS webhook.

The worker is intended to run on the HID server as a separate background
process. It marks each source row with a delivery status so rows are posted
exactly once after a successful 2xx response and retried on failures.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import pyodbc
import requests

try:
    import config
    from modules.db_connection import get_sql_connection
except Exception:
    _root = Path(__file__).resolve().parents[1]
    if str(_root) not in sys.path:
        sys.path.insert(0, str(_root))
    import config
    from modules.db_connection import get_sql_connection


STATUS_PENDING = 0
STATUS_PROCESSING = 1
STATUS_SENT = 2
STATUS_FAILED = 3


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _setup_logging() -> logging.Logger:
    logs_dir = _repo_root() / "Logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("radiology_webhook_worker")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    logger.propagate = False

    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    file_handler = logging.FileHandler(logs_dir / "radiology_webhook_worker.log", encoding="utf-8")
    file_handler.setFormatter(formatter)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    return logger


LOGGER = _setup_logging()


def _close_quietly(*resources: Any) -> None:
    for resource in resources:
        try:
            if resource is not None:
                resource.close()
        except Exception:
            pass


def _worker_enabled() -> bool:
    return bool(getattr(config, "RADIOLOGY_WEBHOOK_ENABLED", True))


def _cfg_int(name: str, default: int, minimum: int) -> int:
    try:
        value = int(getattr(config, name, default))
    except Exception:
        value = int(default)
    return max(minimum, value)


def _get_connection():
    unit = str(getattr(config, "RADIOLOGY_WEBHOOK_UNIT", "AHL") or "AHL").strip().upper()
    conn = get_sql_connection(unit)
    if conn is None:
        raise RuntimeError(f"Unable to connect to {unit} SQL Server")
    try:
        conn.autocommit = True
    except Exception:
        pass
    return conn


def ensure_tracking_columns() -> None:
    conn = None
    cursor = None
    try:
        conn = _get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            IF COL_LENGTH('dbo.RadiologyOrderList', 'RadiologyWebhookStatus') IS NULL
            BEGIN
                ALTER TABLE dbo.RadiologyOrderList ADD RadiologyWebhookStatus TINYINT NULL;

                EXEC('UPDATE dbo.RadiologyOrderList
                      SET RadiologyWebhookStatus = 2
                      WHERE RadiologyWebhookStatus IS NULL');

                ALTER TABLE dbo.RadiologyOrderList
                ADD CONSTRAINT DF_RadiologyOrderList_RadiologyWebhookStatus DEFAULT (0)
                FOR RadiologyWebhookStatus;
            END;

            IF COL_LENGTH('dbo.RadiologyOrderList', 'RadiologyWebhookAttempts') IS NULL
                ALTER TABLE dbo.RadiologyOrderList
                ADD RadiologyWebhookAttempts INT NOT NULL
                    CONSTRAINT DF_RadiologyOrderList_RadiologyWebhookAttempts DEFAULT (0);

            IF COL_LENGTH('dbo.RadiologyOrderList', 'RadiologyWebhookLastAttemptOn') IS NULL
                ALTER TABLE dbo.RadiologyOrderList ADD RadiologyWebhookLastAttemptOn DATETIME NULL;

            IF COL_LENGTH('dbo.RadiologyOrderList', 'RadiologyWebhookSentOn') IS NULL
                ALTER TABLE dbo.RadiologyOrderList ADD RadiologyWebhookSentOn DATETIME NULL;

            IF COL_LENGTH('dbo.RadiologyOrderList', 'RadiologyWebhookResponseCode') IS NULL
                ALTER TABLE dbo.RadiologyOrderList ADD RadiologyWebhookResponseCode INT NULL;

            IF COL_LENGTH('dbo.RadiologyOrderList', 'RadiologyWebhookResponse') IS NULL
                ALTER TABLE dbo.RadiologyOrderList ADD RadiologyWebhookResponse NVARCHAR(1000) NULL;

            IF COL_LENGTH('dbo.RadiologyOrderList', 'RadiologyWebhookVisitId') IS NULL
                ALTER TABLE dbo.RadiologyOrderList ADD RadiologyWebhookVisitId INT NULL;

            IF NOT EXISTS (
                SELECT 1
                FROM sys.indexes
                WHERE object_id = OBJECT_ID('dbo.RadiologyOrderList')
                  AND name = 'IX_RadiologyOrderList_RadiologyWebhookQueue'
            )
            BEGIN
                CREATE INDEX IX_RadiologyOrderList_RadiologyWebhookQueue
                ON dbo.RadiologyOrderList
                    (RadiologyWebhookStatus, RadiologyWebhookAttempts, RadiologyWebhookLastAttemptOn, Id);
            END;
            """
        )
        try:
            conn.commit()
        except Exception:
            pass
        LOGGER.info("RadiologyOrderList webhook tracking columns are ready.")
    finally:
        _close_quietly(cursor, conn)


def release_stale_processing_rows() -> None:
    stale_minutes = _cfg_int("RADIOLOGY_WEBHOOK_STALE_PROCESSING_MINUTES", 10, 1)
    conn = None
    cursor = None
    try:
        conn = _get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            UPDATE dbo.RadiologyOrderList
            SET RadiologyWebhookStatus = ?,
                RadiologyWebhookResponse = 'Reset from stale processing state'
            WHERE RadiologyWebhookStatus = ?
              AND RadiologyWebhookLastAttemptOn < DATEADD(MINUTE, -?, GETDATE());
            """,
            STATUS_FAILED,
            STATUS_PROCESSING,
            stale_minutes,
        )
        try:
            conn.commit()
        except Exception:
            pass
        if cursor.rowcount:
            LOGGER.warning("Reset %s stale processing row(s).", cursor.rowcount)
    finally:
        _close_quietly(cursor, conn)


def claim_pending_rows(batch_size: int) -> list[dict[str, Any]]:
    max_attempts = _cfg_int("RADIOLOGY_WEBHOOK_MAX_ATTEMPTS", 20, 1)
    retry_seconds = _cfg_int("RADIOLOGY_WEBHOOK_RETRY_SECONDS", 60, 1)
    conn = None
    cursor = None
    try:
        conn = _get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            SET NOCOUNT ON;

            DECLARE @claimed TABLE (Id INT PRIMARY KEY);

            ;WITH next_rows AS (
                SELECT TOP (?)
                    Id
                FROM dbo.RadiologyOrderList WITH (READPAST, UPDLOCK, ROWLOCK)
                WHERE
                    (
                        RadiologyWebhookStatus = ?
                        OR (
                            RadiologyWebhookStatus = ?
                            AND RadiologyWebhookAttempts < ?
                            AND (
                                RadiologyWebhookLastAttemptOn IS NULL
                                OR RadiologyWebhookLastAttemptOn <= DATEADD(SECOND, -?, GETDATE())
                            )
                        )
                    )
                    AND NULLIF(LTRIM(RTRIM(ISNULL(UHIDNo, ''))), '') IS NOT NULL
                    AND NULLIF(LTRIM(RTRIM(ISNULL(patientName, ''))), '') IS NOT NULL
                ORDER BY Id
            )
            UPDATE rol
            SET RadiologyWebhookStatus = ?,
                RadiologyWebhookAttempts = ISNULL(RadiologyWebhookAttempts, 0) + 1,
                RadiologyWebhookLastAttemptOn = GETDATE(),
                RadiologyWebhookResponseCode = NULL,
                RadiologyWebhookResponse = NULL
            OUTPUT inserted.Id INTO @claimed(Id)
            FROM dbo.RadiologyOrderList rol
            INNER JOIN next_rows nr ON nr.Id = rol.Id;

            SELECT
                rol.Id,
                rol.UHIDNo,
                rol.patientName,
                rol.age,
                rol.gender,
                rol.mobileNo,
                rol.referringDoctor,
                rol.reportType,
                rol.insertedOn,
                rol.RadiologyWebhookAttempts
            FROM dbo.RadiologyOrderList rol
            INNER JOIN @claimed c ON c.Id = rol.Id
            ORDER BY rol.Id;
            """,
            int(batch_size),
            STATUS_PENDING,
            STATUS_FAILED,
            max_attempts,
            retry_seconds,
            STATUS_PROCESSING,
        )
        columns = [col[0] for col in cursor.description]
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
        try:
            conn.commit()
        except Exception:
            pass
        return rows
    finally:
        _close_quietly(cursor, conn)


def _to_int_or_none(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except Exception:
        return None


def _clean_text(value: Any) -> str:
    return str(value or "").strip()


def build_payload(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "patientId": _clean_text(row.get("UHIDNo")),
        "patientName": _clean_text(row.get("patientName")),
        "age": _to_int_or_none(row.get("age")),
        "gender": _clean_text(row.get("gender")),
        "mobileNo": _clean_text(row.get("mobileNo")),
        "referringDoctor": _clean_text(row.get("referringDoctor")),
        "reportType": _clean_text(row.get("reportType")),
    }


def _short_response(response: requests.Response) -> str:
    text = (response.text or "").strip()
    if len(text) > 900:
        text = text[:900] + "..."
    return text


def post_row(row: dict[str, Any]) -> tuple[bool, int | None, str, int | None]:
    url = str(getattr(config, "RADIOLOGY_WEBHOOK_URL", "") or "").strip()
    api_key = str(getattr(config, "RADIOLOGY_WEBHOOK_API_KEY", "") or "").strip()
    if not url:
        raise RuntimeError("RADIOLOGY_WEBHOOK_URL is not configured")

    payload = build_payload(row)
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["x-api-key"] = api_key

    response = requests.post(url, headers=headers, json=payload, timeout=30)
    body_text = _short_response(response)
    visit_id = None
    try:
        body_json = response.json()
        visit_id = _to_int_or_none(body_json.get("visitId"))
    except Exception:
        pass
    return 200 <= response.status_code < 300, response.status_code, body_text, visit_id


def mark_row(row_id: int, *, success: bool, response_code: int | None, response_text: str, visit_id: int | None) -> None:
    conn = None
    cursor = None
    try:
        conn = _get_connection()
        cursor = conn.cursor()
        if success:
            cursor.execute(
                """
                UPDATE dbo.RadiologyOrderList
                SET RadiologyWebhookStatus = ?,
                    RadiologyWebhookSentOn = GETDATE(),
                    RadiologyWebhookResponseCode = ?,
                    RadiologyWebhookResponse = ?,
                    RadiologyWebhookVisitId = ?
                WHERE Id = ?;
                """,
                STATUS_SENT,
                response_code,
                response_text,
                visit_id,
                row_id,
            )
        else:
            cursor.execute(
                """
                UPDATE dbo.RadiologyOrderList
                SET RadiologyWebhookStatus = ?,
                    RadiologyWebhookResponseCode = ?,
                    RadiologyWebhookResponse = ?
                WHERE Id = ?;
                """,
                STATUS_FAILED,
                response_code,
                response_text,
                row_id,
            )
        try:
            conn.commit()
        except Exception:
            pass
    finally:
        _close_quietly(cursor, conn)


def process_once() -> int:
    batch_size = _cfg_int("RADIOLOGY_WEBHOOK_BATCH_SIZE", 20, 1)
    rows = claim_pending_rows(batch_size)
    if not rows:
        return 0

    posted = 0
    for row in rows:
        row_id = int(row["Id"])
        try:
            ok, status_code, response_text, visit_id = post_row(row)
            mark_row(
                row_id,
                success=ok,
                response_code=status_code,
                response_text=response_text,
                visit_id=visit_id,
            )
            if ok:
                posted += 1
                LOGGER.info("Posted RadiologyOrderList Id=%s UHID=%s", row_id, row.get("UHIDNo"))
            else:
                LOGGER.warning(
                    "Webhook rejected RadiologyOrderList Id=%s status=%s response=%s",
                    row_id,
                    status_code,
                    response_text,
                )
        except Exception as exc:
            mark_row(
                row_id,
                success=False,
                response_code=None,
                response_text=f"{type(exc).__name__}: {exc}",
                visit_id=None,
            )
            LOGGER.exception("Failed posting RadiologyOrderList Id=%s", row_id)
    return posted


def main_loop(poll_interval_seconds: int | None = None) -> None:
    poll_interval_seconds = max(
        30,
        int(poll_interval_seconds or getattr(config, "RADIOLOGY_WEBHOOK_POLL_SECONDS", 120) or 120),
    )
    LOGGER.info(
        "Radiology webhook worker starting at %s; checking every %s seconds",
        datetime.now().isoformat(timespec="seconds"),
        poll_interval_seconds,
    )
    ensure_tracking_columns()

    while True:
        if not _worker_enabled():
            LOGGER.info("RADIOLOGY_WEBHOOK_ENABLED is false; exiting.")
            return

        try:
            release_stale_processing_rows()
            posted = process_once()
            if posted:
                LOGGER.info("Radiology webhook worker posted %s row(s) this cycle.", posted)
            time.sleep(poll_interval_seconds)

        except pyodbc.OperationalError as exc:
            LOGGER.warning("Radiology webhook worker skipped this cycle: database connection failed: %s", exc)
            time.sleep(poll_interval_seconds)
        except RuntimeError as exc:
            LOGGER.warning("Radiology webhook worker skipped this cycle: %s", exc)
            time.sleep(poll_interval_seconds)
        except Exception:
            LOGGER.exception("Radiology webhook worker error")
            time.sleep(poll_interval_seconds)


if __name__ == "__main__":
    main_loop()
