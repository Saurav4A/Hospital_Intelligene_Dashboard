# otp_mail_worker.py
"""
Background worker to send Discount / Cancellation OTP emails
using the noreply@asarfihospital.com mailbox (via Graph).
"""

import time
import pyodbc
import requests
from datetime import datetime
import config
import re

# Flexible import: works whether you run "python -m modules.otp_mail_worker"
# or "python modules/otp_mail_worker.py"
try:
    from modules.ms_graph_it import get_graph_headers, GRAPH_API_ENDPOINT
except Exception:
    import sys
    from pathlib import Path
    _root = Path(__file__).resolve().parents[1]
    if str(_root) not in sys.path:
        sys.path.insert(0, str(_root))
    from modules.ms_graph_it import get_graph_headers, GRAPH_API_ENDPOINT


# ======== GRAPH / APP CONFIG FOR noreply MAILBOX ========
# 👉 Put your Azure AD "Application (client) ID" here
APP_ID = "6ce0f640-b848-4b77-828c-3d4676fa1562"   # TODO: change this
SCOPES = ["Mail.Send"]


# ======== SQL SERVER CONFIG =========
# Adjust if needed; this is your HMIS DB where DiscountApprovalRequest lives.
SQL_SERVER = "192.168.1.4"
SQL_DB     = "Prodoc2021"
SQL_USER   = "sa"
SQL_PWD    = "Prodoc09"
# ====================================


# ======== MAIL RECIPIENTS =========
# Who should receive these OTP mails (approvers)?
APPROVER_EMAILS = [
    # "us@asarfihospital.com",
    # "suraj.hazari@asarfihospital.com",
    # "accounts@asarfihospital.com",
    # "agmo@asarfihospital.com",
    "ahl.it@asarfihospital.com"
    # add more if required
]

# Optional BCC (can be noreply itself for logging)
IT_BCC = ["noreply@asarfihospital.com"]

PO_REQUEST_KEYS = {"POAPPROVAL", "PO"}


# ========= DB HELPERS =========

def get_db_connection():
    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={SQL_SERVER};"
        f"DATABASE={SQL_DB};"
        f"UID={SQL_USER};"
        f"PWD={SQL_PWD};"
        "Encrypt=yes;"
        "TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str)


def get_login_db_connection():
    cfg = getattr(config, "LOGIN_DB", None)
    if not cfg:
        return None
    driver = cfg.get("DRIVER", "{ODBC Driver 18 for SQL Server}")
    server = cfg.get("SERVER", "192.168.20.100")
    database = cfg.get("DATABASE", cfg.get("DB") or "ACI")
    uid = cfg.get("UID") or cfg.get("USER")
    pwd = cfg.get("PWD")
    if not all([driver, server, database, uid, pwd]):
        return None
    conn_str = (
        f"DRIVER={driver};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={uid};PWD={pwd};"
        "TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str, timeout=5)


def fetch_purchase_incharge_emails(unit: str | None = None):
    conn = get_login_db_connection()
    if not conn:
        return []
    try:
        cursor = conn.cursor()
        unit_val = (unit or "").strip().upper()
        cursor.execute("""
            IF OBJECT_ID('dbo.HID_Purchase_Incharge', 'U') IS NULL
            BEGIN
                SELECT CAST(NULL AS NVARCHAR(200)) AS Email WHERE 1=0;
            END
            ELSE IF COL_LENGTH('dbo.HID_Purchase_Incharge', 'Unit') IS NULL
            BEGIN
                SELECT Email
                FROM dbo.HID_Purchase_Incharge
                WHERE ISNULL(IsActive, 1) = 1;
            END
            ELSE
            BEGIN
                SELECT Email
                FROM dbo.HID_Purchase_Incharge
                WHERE ISNULL(IsActive, 1) = 1
                  AND (
                        ? = ''
                        OR Unit IS NULL
                        OR LTRIM(RTRIM(Unit)) = ''
                        OR UPPER(Unit) = 'ALL'
                        OR UPPER(Unit) = ?
                  );
            END
        """, (unit_val, unit_val))
        rows = cursor.fetchall()
        return [row.Email for row in rows if row and row[0]]
    except Exception:
        return []
    finally:
        try:
            conn.close()
        except Exception:
            pass


def fetch_bill_edit_director_emails(unit: str | None = None):
    conn = get_login_db_connection()
    if not conn:
        return []
    try:
        cursor = conn.cursor()
        unit_val = (unit or "").strip().upper()
        cursor.execute("""
            IF OBJECT_ID('dbo.HID_Bill_Edit_Directors', 'U') IS NULL
            BEGIN
                SELECT CAST(NULL AS NVARCHAR(200)) AS Email WHERE 1=0;
            END
            ELSE IF COL_LENGTH('dbo.HID_Bill_Edit_Directors', 'Unit') IS NULL
            BEGIN
                SELECT Email
                FROM dbo.HID_Bill_Edit_Directors
                WHERE ISNULL(IsActive, 1) = 1;
            END
            ELSE
            BEGIN
                SELECT Email
                FROM dbo.HID_Bill_Edit_Directors
                WHERE ISNULL(IsActive, 1) = 1
                  AND (
                        ? = ''
                        OR Unit IS NULL
                        OR LTRIM(RTRIM(Unit)) = ''
                        OR UPPER(Unit) = 'ALL'
                        OR UPPER(Unit) = ?
                  );
            END
        """, (unit_val, unit_val))
        rows = cursor.fetchall()
        return [row.Email for row in rows if row and row[0]]
    except Exception:
        return []
    finally:
        try:
            conn.close()
        except Exception:
            pass


def is_po_request_type(req_type_raw: str) -> bool:
    if not req_type_raw:
        return False
    cleaned = re.sub(r"[^A-Z0-9]+", "", str(req_type_raw).strip().upper())
    return cleaned in PO_REQUEST_KEYS


def is_bill_edit_request_type(req_type_raw: str) -> bool:
    if not req_type_raw:
        return False
    cleaned = re.sub(r"[^A-Z0-9]+", "", str(req_type_raw).strip().upper())
    return cleaned.startswith("BILLEDIT")


def fetch_pending_rows(cursor, batch_size=50):
    """
    Pending OTP requests from DiscountApprovalRequest.
    Assumes you have a table like:

        DiscountApprovalRequest(
            Id INT,
            BillNo VARCHAR(..),
            ReceiptNo VARCHAR(..),
            UHIDNo VARCHAR(..),
            PatientName VARCHAR(..),
            RequestType VARCHAR(20),
            RequestedAmount DECIMAL(18,2),
            Reason NVARCHAR(500),
            RequestedByUserName VARCHAR(100),
            RequestedAt DATETIME,
            OtpPlain VARCHAR(10),
            EmailSent BIT,
            Status VARCHAR(20)
        )
    """
    cursor.execute(
        """
        SELECT TOP (?)
            Id,
            BillNo,
            ReceiptNo,
            UHIDNo,
            PatientName,
            RequestType,
            RequestedAmount,
            Reason,
            RequestedByUserName,
            RequestedAt,
            Unit,
            OtpPlain   -- plain OTP to be mailed
        FROM dbo.DiscountApprovalRequest
        WHERE EmailSent = 0
          AND Status = 'PENDING'
        ORDER BY Id;
        """,
        batch_size,
    )

    columns = [c[0] for c in cursor.description]
    rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
    # PO OTPs are handled directly in the app to avoid duplicate emails.
    return [r for r in rows if not is_po_request_type(r.get("RequestType"))]


# ========= MAIL CONTENT =========

def build_email_content(row):
    """
    Build the HTML body for OTP approval mail.
    """

    bill_no = row.get("BillNo") or ""
    receipt_no = row.get("ReceiptNo") or ""
    uhid = row.get("UHIDNo") or ""
    patient = row.get("PatientName") or ""
    req_type_raw = (row.get("RequestType") or "").strip()
    req_type_disp = re.sub(r"[_\-]+", " ", req_type_raw).strip()
    req_type_disp = " ".join(w.capitalize() for w in re.split(r"\s+", req_type_disp) if w)
    req_type = req_type_disp or req_type_raw.title()
    req_amt = row.get("RequestedAmount") or ""
    reason = row.get("Reason") or ""
    reason_disp = re.sub(r"[_\-]+", " ", str(reason or "")).strip()
    user_reason = reason
    change_detail = ""
    if is_bill_edit_request_type(req_type_raw):
        marker = "|||CHANGES:"
        if marker in str(reason):
            before, after = str(reason).split(marker, 1)
            user_reason = before.strip()
            change_detail = after.strip()
    req_by = row.get("RequestedByUserName") or ""
    req_at = row.get("RequestedAt")
    otp_code = row.get("OtpPlain") or ""
    unit_val = row.get("Unit") or ""

    if isinstance(req_at, datetime):
        req_at_str = req_at.strftime("%d-%b-%Y %H:%M")
    else:
        req_at_str = str(req_at or "")

    if is_po_request_type(req_type_raw):
        subject = f"OTP Approval - Purchase Order {bill_no}"
        body = f"""
    <html>
    <head>
      <style>
        body {{
          font-family: Arial, sans-serif;
          font-size: 14px;
          color: #333;
        }}
        .box {{
          border: 1px solid #e2e8f0;
          border-radius: 10px;
          padding: 16px 18px;
          max-width: 650px;
          background: #ffffff;
        }}
        .otp {{
          font-size: 26px;
          font-weight: bold;
          color: #1e3a8a;
          letter-spacing: 6px;
          margin: 8px 0 4px;
        }}
        table {{
          width: 100%;
          border-collapse: collapse;
          margin-top: 10px;
        }}
        td {{
          padding: 4px 6px;
          font-size: 13px;
          vertical-align: top;
        }}
        .label {{
          width: 35%;
          font-weight: bold;
          color: #555;
          white-space: nowrap;
        }}
        .footer {{
          margin-top: 20px;
          font-size: 12px;
          color: #777;
        }}
      </style>
    </head>
    <body>
      <div class="box">
        <p>Dear Sir/Madam,</p>

        <p>
          A purchase order approval request has been raised.
        </p>

        <p><b>OTP for approval:</b></p>
        <div class="otp">{otp_code}</div>
        <p style="font-size:12px; color:#666;">
          Please share this OTP with the requesting user after verification.
        </p>

        <h3>PO Snapshot</h3>
        <table>
          <tr><td class="label">Unit</td><td>: {unit_val}</td></tr>
          <tr><td class="label">PO No</td><td>: {bill_no}</td></tr>
          <tr><td class="label">PO Id</td><td>: {receipt_no}</td></tr>
          <tr><td class="label">Supplier</td><td>: {patient}</td></tr>
          <tr><td class="label">Requested Amount</td><td>: {req_amt}</td></tr>
          <tr><td class="label">Requested By</td><td>: {req_by}</td></tr>
          <tr><td class="label">Requested At</td><td>: {req_at_str}</td></tr>
          <tr><td class="label">Subject/Notes</td><td>: {reason}</td></tr>
        </table>

        <p class="footer">
          This is an automated email from the Purchase Order Approval Module.
        </p>
      </div>
    </body>
    </html>
    """
    elif is_bill_edit_request_type(req_type_raw):
        req_type_detail = f"Bill Edit - {change_detail}" if change_detail else req_type_disp or req_type_raw
        patient_subject = (patient or "").strip()
        if not patient_subject:
            patient_subject = f"Bill {bill_no}" if bill_no else "Bill Edit"
        subject = f"Bill Edit for {patient_subject}"
        body = f"""
    <html>
    <head>
      <style>
        body {{
          font-family: Arial, sans-serif;
          font-size: 14px;
          color: #333;
        }}
        .box {{
          border: 1px solid #e2e8f0;
          border-radius: 10px;
          padding: 16px 18px;
          max-width: 650px;
          background: #ffffff;
        }}
        .otp {{
          font-size: 26px;
          font-weight: bold;
          color: #1e3a8a;
          letter-spacing: 6px;
          margin: 8px 0 4px;
        }}
        table {{
          width: 100%;
          border-collapse: collapse;
          margin-top: 10px;
        }}
        td {{
          padding: 4px 6px;
          font-size: 13px;
          vertical-align: top;
        }}
        .label {{
          width: 35%;
          font-weight: bold;
          color: #555;
          white-space: nowrap;
        }}
        .footer {{
          margin-top: 20px;
          font-size: 12px;
          color: #777;
        }}
      </style>
    </head>
    <body>
      <div class="box">
        <p>Dear Sir/Madam,</p>

        <p>
          A bill edit approval request has been raised.
        </p>

        <p><b>OTP for approval:</b></p>
        <div class="otp">{otp_code}</div>
        <p style="font-size:12px; color:#666;">
          Please share this OTP with the requesting user after verification.
        </p>

        <h3>Bill Snapshot</h3>
        <table>
          <tr><td class="label">UHID</td><td>: {uhid}</td></tr>
          <tr><td class="label">Patient Name</td><td>: {patient}</td></tr>
          <tr><td class="label">Bill No</td><td>: {bill_no}</td></tr>
          <tr><td class="label">Request Type</td><td>: {req_type_detail}</td></tr>
          <tr><td class="label">Unit</td><td>: {unit_val}</td></tr>
          <tr><td class="label">Requested By</td><td>: {req_by}</td></tr>
          <tr><td class="label">Requested At</td><td>: {req_at_str}</td></tr>
          <tr><td class="label">Reason</td><td>: {user_reason}</td></tr>
        </table>

        <p class="footer">
          This is an automated email from the Bill Edit Approval Module.<br>
          Kindly verify the details before sharing the OTP with the requester.
        </p>
      </div>
    </body>
    </html>
    """
    else:
        subject = f"OTP Approval - {req_type} for Bill {bill_no}"
        body = f"""
    <html>
    <head>
      <style>
        body {{
          font-family: Arial, sans-serif;
          font-size: 14px;
          color: #333;
        }}
        .box {{
          border: 1px solid #e2e8f0;
          border-radius: 10px;
          padding: 16px 18px;
          max-width: 650px;
          background: #ffffff;
        }}
        .otp {{
          font-size: 26px;
          font-weight: bold;
          color: #1e3a8a;
          letter-spacing: 6px;
          margin: 8px 0 4px;
        }}
        table {{
          width: 100%;
          border-collapse: collapse;
          margin-top: 10px;
        }}
        td {{
          padding: 4px 6px;
          font-size: 13px;
          vertical-align: top;
        }}
        .label {{
          width: 35%;
          font-weight: bold;
          color: #555;
          white-space: nowrap;
        }}
        .footer {{
          margin-top: 20px;
          font-size: 12px;
          color: #777;
        }}
      </style>
    </head>
    <body>
      <div class="box">
        <p>Dear Sir/Madam,</p>

        <p>
          An approval request has been raised for
          <b>{req_type}</b> on the following bill.
        </p>

        <p><b>OTP for approval:</b></p>
        <div class="otp">{otp_code}</div>
        <p style="font-size:12px; color:#666;">
          This OTP is valid for a limited time only. Please share it only with the billing user requesting approval.
        </p>

        <h3>Bill Snapshot</h3>
        <table>
          <tr><td class="label">UHID</td><td>: {uhid}</td></tr>
          <tr><td class="label">Patient Name</td><td>: {patient}</td></tr>
          <tr><td class="label">Bill No</td><td>: {bill_no}</td></tr>
          <tr><td class="label">Receipt No</td><td>: {receipt_no}</td></tr>
          <tr><td class="label">Request Type</td><td>: {req_type}</td></tr>
          <tr><td class="label">Requested Amount</td><td>: {req_amt}</td></tr>
          <tr><td class="label">Requested By</td><td>: {req_by}</td></tr>
          <tr><td class="label">Requested At</td><td>: {req_at_str}</td></tr>
          <tr><td class="label">Reason</td><td>: {reason_disp}</td></tr>
        </table>

        <p class="footer">
          This is an automated email from Asarfi Hospital HMIS Discount/Cancellation Approval Module.<br>
          Kindly verify the details before sharing the OTP with the requester.
        </p>
      </div>
    </body>
    </html>
    """

    return subject, body


def resolve_recipients(row):
    req_type = str(row.get("RequestType") or "").strip()
    if is_po_request_type(req_type):
        emails = fetch_purchase_incharge_emails(row.get("Unit"))
        if emails:
            return emails
    if is_bill_edit_request_type(req_type):
        emails = fetch_bill_edit_director_emails(row.get("Unit"))
        if emails:
            return emails
    return APPROVER_EMAILS


# ========= MAIL SENDER =========

def send_mail(subject, body_html, headers, to_recipients, bcc_recipients=None):
    """
    Send mail via Graph /me/sendMail using the noreply mailbox.
    """
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

    request_body = {
        "message": message,
        "saveToSentItems": True,
    }

    resp = requests.post(endpoint, headers=headers, json=request_body)

    if resp.status_code not in (200, 202):
        raise Exception(
            f"Graph sendMail failed ({resp.status_code}): {resp.text}"
        )


def mark_as_sent(cursor, row_id):
    cursor.execute(
        """
        UPDATE dbo.DiscountApprovalRequest
        SET EmailSent = 1,
            EmailSentAt = GETDATE()
        WHERE Id = ?;
        """,
        row_id,
    )


# ========= MAIN LOOP =========

def main_loop(poll_interval_seconds: int = 5):
    print("Discount/Cancellation OTP email worker started (noreply mailbox). Watching for new rows...")

    while True:
        conn = None
        cursor = None
        try:
            conn = get_db_connection()
            conn.autocommit = False
            cursor = conn.cursor()

            pending_rows = fetch_pending_rows(cursor, batch_size=20)

            if not pending_rows:
                cursor.close()
                conn.close()
                time.sleep(poll_interval_seconds)
                continue

            headers = get_graph_headers(APP_ID, SCOPES)

            for row in pending_rows:
                subject, body = build_email_content(row)
                recipients = resolve_recipients(row)
                if not recipients:
                    print(f"No recipients configured; skipping row {row['Id']}.")
                    continue

                print(f"Sending OTP mail for approval request Id={row['Id']}")
                send_mail(
                    subject=subject,
                    body_html=body,
                    headers=headers,
                    to_recipients=recipients,
                    bcc_recipients=IT_BCC,
                )

                mark_as_sent(cursor, row["Id"])

            conn.commit()
            cursor.close()
            conn.close()

        except pyodbc.OperationalError as e:
            print("Database connection error in loop:", repr(e))
            try:
                if cursor is not None:
                    cursor.close()
                if conn is not None:
                    conn.close()
            except Exception:
                pass
            time.sleep(10)

        except Exception as e:
            print("Error in loop:", repr(e))
            try:
                if conn is not None:
                    conn.rollback()
            except Exception:
                pass
            try:
                if cursor is not None:
                    cursor.close()
                if conn is not None:
                    conn.close()
            except Exception:
                pass
            time.sleep(10)


if __name__ == "__main__":
    main_loop()
