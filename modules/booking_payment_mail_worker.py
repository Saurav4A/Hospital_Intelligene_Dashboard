"""
Background worker to send Summer Training payment receipt emails
for new rows inserted into dbo.BookingPayment.
"""

import time
from datetime import datetime
from decimal import Decimal, InvalidOperation
from html import escape

import pyodbc
import requests

import config

try:
    from modules.ms_graph_it import get_graph_headers, GRAPH_API_ENDPOINT
except Exception:
    import sys
    from pathlib import Path

    _root = Path(__file__).resolve().parents[1]
    if str(_root) not in sys.path:
        sys.path.insert(0, str(_root))
    from modules.ms_graph_it import get_graph_headers, GRAPH_API_ENDPOINT


APP_ID = "6ce0f640-b848-4b77-828c-3d4676fa1562"
SCOPES = ["Mail.Send"]

WORKER_LOCK_NAME = "booking_payment_receipt_mail_worker"

BCC_RECIPIENTS = ["cancerresearch@asarfihospital.com"]
PROGRAM_NAME = "Summer Training on Molecular Techniques"
COORDINATOR_NAME = "Dr. Smita"
COORDINATOR_PHONE = "+91 73763 37605"
COORDINATOR_EMAIL = "cancerresearch@asarfihospital.com"
SIGNATURE_NAME = "We Fight Cancer Research Lab"
SIGNATURE_PHONE = "8227996523"
SIGNATURE_EMAIL = "suraj.mishra@asarfihospital.com"


def _worker_enabled() -> bool:
    return bool(getattr(config, "ENABLE_BOOKING_PAYMENT_MAIL_WORKER", False))


def _booking_db_config() -> dict:
    cfg = getattr(config, "BOOKING_PAYMENT_EMAIL_DB", {}) or {}
    return {
        "DRIVER": cfg.get("DRIVER", "{ODBC Driver 18 for SQL Server}"),
        "SERVER": cfg.get("SERVER", "192.168.1.102,1433"),
        "DATABASE": cfg.get("DATABASE", "Prodoc22"),
        "UID": cfg.get("UID", "sa"),
        "PWD": cfg.get("PWD", "Prodoc_20"),
        "Encrypt": cfg.get("Encrypt", "yes"),
        "TrustServerCertificate": cfg.get("TrustServerCertificate", "yes"),
        "Connection Timeout": str(cfg.get("Connection Timeout", 5)),
        "ConnectRetryCount": str(cfg.get("ConnectRetryCount", 3)),
        "ConnectRetryInterval": str(cfg.get("ConnectRetryInterval", 5)),
    }


def get_db_connection(*, autocommit: bool = False):
    cfg = _booking_db_config()
    conn_str = (
        f"DRIVER={cfg['DRIVER']};"
        f"SERVER={cfg['SERVER']};"
        f"DATABASE={cfg['DATABASE']};"
        f"UID={cfg['UID']};"
        f"PWD={cfg['PWD']};"
        f"Encrypt={cfg['Encrypt']};"
        f"TrustServerCertificate={cfg['TrustServerCertificate']};"
        f"Connection Timeout={cfg['Connection Timeout']};"
        f"ConnectRetryCount={cfg['ConnectRetryCount']};"
        f"ConnectRetryInterval={cfg['ConnectRetryInterval']};"
    )
    return pyodbc.connect(
        conn_str,
        timeout=int(cfg["Connection Timeout"]),
        autocommit=autocommit,
    )


def _close_quietly(*resources):
    for resource in resources:
        try:
            if resource is not None:
                resource.close()
        except Exception:
            pass


def acquire_worker_lock(cursor) -> bool:
    cursor.execute(
        """
        DECLARE @lock_result INT;
        EXEC @lock_result = sp_getapplock
            @Resource = ?,
            @LockMode = 'Exclusive',
            @LockOwner = 'Session',
            @LockTimeout = 0;
        SELECT @lock_result;
        """,
        WORKER_LOCK_NAME,
    )
    row = cursor.fetchone()
    return bool(row) and int(row[0]) >= 0


def open_worker_lock():
    conn = None
    cursor = None
    try:
        conn = get_db_connection(autocommit=True)
        cursor = conn.cursor()
        if not acquire_worker_lock(cursor):
            _close_quietly(cursor, conn)
            return None, None
        return conn, cursor
    except Exception:
        _close_quietly(cursor, conn)
        raise


def fetch_pending_rows(batch_size: int = 20):
    conn = None
    cursor = None
    try:
        conn = get_db_connection(autocommit=True)
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT TOP (?)
                Id,
                FullName,
                Mobile,
                Email,
                Amount,
                PaymentId,
                CreatedDate,
                PaymentStatus,
                emailSendStatus
            FROM dbo.BookingPayment WITH (READPAST)
            WHERE ISNULL(emailSendStatus, 0) = 0
              AND NULLIF(LTRIM(RTRIM(ISNULL(Email, ''))), '') IS NOT NULL
              AND NULLIF(LTRIM(RTRIM(ISNULL(PaymentId, ''))), '') IS NOT NULL
            ORDER BY Id;
            """,
            batch_size,
        )
        columns = [c[0] for c in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]
    finally:
        _close_quietly(cursor, conn)


def _format_amount(value) -> str:
    if value in (None, ""):
        return "-"
    try:
        return f"INR {Decimal(str(value)).quantize(Decimal('0.01'))}"
    except (InvalidOperation, ValueError, TypeError):
        return escape(str(value))


def _format_datetime(value) -> str:
    if isinstance(value, datetime):
        return value.strftime("%d %b %Y, %I:%M %p")
    if value in (None, ""):
        return "-"
    return escape(str(value))


def _safe_text(value, fallback: str = "-") -> str:
    text = str(value or "").strip()
    return escape(text) if text else fallback


def build_email_content(row):
    full_name = _safe_text(row.get("FullName"), "Participant")
    email_id = _safe_text(row.get("Email"))
    mobile = _safe_text(row.get("Mobile"))
    payment_id = _safe_text(row.get("PaymentId"))
    payment_date = _format_datetime(row.get("CreatedDate"))
    amount_paid = _format_amount(row.get("Amount"))

    subject = f"Payment Receipt - {PROGRAM_NAME}"
    body = f"""
    <html>
    <head>
      <style>
        body {{
          font-family: Arial, sans-serif;
          color: #243447;
          background: #f5f7fb;
          line-height: 1.6;
        }}
        .card {{
          max-width: 760px;
          margin: 20px auto;
          background: #ffffff;
          border: 1px solid #dbe3ef;
          border-radius: 12px;
          padding: 28px 32px;
        }}
        .title {{
          font-size: 22px;
          font-weight: 700;
          color: #0f2d52;
          margin-bottom: 10px;
        }}
        p {{
          margin: 0 0 14px;
        }}
        table {{
          width: 100%;
          border-collapse: collapse;
          margin: 12px 0 22px;
        }}
        th {{
          text-align: left;
          padding: 10px 12px;
          background: #eef4fb;
          color: #113a67;
          font-size: 14px;
        }}
        td {{
          padding: 9px 12px;
          border-bottom: 1px solid #edf2f7;
          font-size: 14px;
        }}
        .label {{
          width: 34%;
          font-weight: 600;
          color: #425466;
        }}
        .note {{
          padding: 14px 16px;
          background: #f8fbff;
          border-left: 4px solid #1d6fdc;
          border-radius: 8px;
        }}
        .footer {{
          margin-top: 22px;
        }}
      </style>
    </head>
    <body>
      <div class="card">
        <div class="title">Payment Receipt</div>
        <p>Dear {full_name},</p>
        <p>
          Thank you for registering for the <strong>{PROGRAM_NAME}</strong>.
          We are pleased to confirm that your payment has been received successfully.
        </p>
        <p>Please find your registration and payment details below for your records:</p>

        <table>
          <tr><th colspan="2">Participant Details</th></tr>
          <tr><td class="label">Name</td><td>{full_name}</td></tr>
          <tr><td class="label">Email ID</td><td>{email_id}</td></tr>
          <tr><td class="label">Phone Number</td><td>{mobile}</td></tr>
        </table>

        <table>
          <tr><th colspan="2">Payment Details</th></tr>
          <tr><td class="label">Transaction ID</td><td>{payment_id}</td></tr>
          <tr><td class="label">Payment Date</td><td>{payment_date}</td></tr>
          <tr><td class="label">Amount Paid</td><td>{amount_paid}</td></tr>
          <tr><td class="label">Payment Mode</td><td>Razorpay</td></tr>
        </table>

        <p class="note">This email serves as your official payment receipt.</p>

        <table>
          <tr><th colspan="2">Training Details</th></tr>
          <tr><td class="label">Program</td><td>{PROGRAM_NAME}</td></tr>
          <tr><td class="label">Coordinator</td><td>{COORDINATOR_NAME}</td></tr>
          <tr><td class="label">Phone Number</td><td>{COORDINATOR_PHONE}</td></tr>
          <tr><td class="label">Email</td><td>{COORDINATOR_EMAIL}</td></tr>
        </table>

        <p>
          If you have any questions or need any assistance, please feel free to contact us at
          <strong>{COORDINATOR_PHONE}</strong> or <strong>{COORDINATOR_EMAIL}</strong>.
        </p>
        <p>We look forward to your participation and wish you a rewarding learning experience.</p>

        <div class="footer">
          <p>Warm regards,</p>
          <p>
            <strong>{SIGNATURE_NAME}</strong><br>
            {SIGNATURE_PHONE}<br>
            {SIGNATURE_EMAIL}
          </p>
        </div>
      </div>
    </body>
    </html>
    """
    return subject, body


def send_mail(subject, body_html, headers, to_recipients, bcc_recipients=None):
    endpoint = GRAPH_API_ENDPOINT + "/me/sendMail"
    message = {
        "subject": subject,
        "body": {
            "contentType": "HTML",
            "content": body_html,
        },
        "toRecipients": [
            {"emailAddress": {"address": addr}}
            for addr in (to_recipients or [])
        ],
    }

    if bcc_recipients:
        message["bccRecipients"] = [
            {"emailAddress": {"address": addr}}
            for addr in bcc_recipients
        ]

    resp = requests.post(
        endpoint,
        headers=headers,
        json={"message": message, "saveToSentItems": True},
        timeout=30,
    )
    if resp.status_code not in (200, 202):
        raise Exception(f"Graph sendMail failed ({resp.status_code}): {resp.text}")


def mark_as_sent(row_id) -> bool:
    conn = None
    cursor = None
    try:
        conn = get_db_connection(autocommit=False)
        cursor = conn.cursor()
        cursor.execute(
            """
            UPDATE dbo.BookingPayment
            SET emailSendStatus = 1
            WHERE Id = ?
              AND ISNULL(emailSendStatus, 0) = 0;
            """,
            row_id,
        )
        updated = int(cursor.rowcount or 0) > 0
        conn.commit()
        return updated
    except Exception:
        if conn is not None:
            try:
                conn.rollback()
            except Exception:
                pass
        raise
    finally:
        _close_quietly(cursor, conn)


def process_pending_rows(headers):
    pending_rows = fetch_pending_rows(batch_size=20)
    if not pending_rows:
        return 0

    sent_count = 0
    for row in pending_rows:
        row_id = row.get("Id")
        recipient = str(row.get("Email") or "").strip()
        if not recipient:
            print(f"BookingPayment worker: row {row_id} has no email; skipping.")
            continue

        try:
            subject, body = build_email_content(row)
            send_mail(
                subject=subject,
                body_html=body,
                headers=headers,
                to_recipients=[recipient],
                bcc_recipients=BCC_RECIPIENTS,
            )
            if mark_as_sent(row_id):
                sent_count += 1
                print(f"BookingPayment worker: sent receipt for Id={row_id} to {recipient}")
            else:
                print(
                    f"BookingPayment worker: email sent for Id={row_id}, "
                    "but the row was not updated; it may retry."
                )
        except Exception as row_error:
            print(f"BookingPayment worker: failed for Id={row_id}: {row_error}")
    return sent_count


def main_loop(poll_interval_seconds: int = 5):
    poll_interval_seconds = max(2, int(poll_interval_seconds or 5))
    print("BookingPayment receipt email worker started (noreply mailbox). Watching for new rows...")

    while True:
        if not _worker_enabled():
            print("BookingPayment worker disabled in config; exiting main loop.")
            return

        lock_conn = None
        lock_cursor = None
        try:
            lock_conn, lock_cursor = open_worker_lock()
            if lock_conn is None:
                time.sleep(poll_interval_seconds)
                continue

            headers = get_graph_headers(APP_ID, SCOPES)
            sent_count = process_pending_rows(headers)

            if sent_count == 0:
                time.sleep(poll_interval_seconds)

        except pyodbc.OperationalError as e:
            print("BookingPayment worker database error:", repr(e))
            time.sleep(10)
        except Exception as e:
            print("BookingPayment worker error:", repr(e))
            time.sleep(10)
        finally:
            _close_quietly(lock_cursor, lock_conn)


if __name__ == "__main__":
    main_loop()
