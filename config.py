# ============================================================
# CONFIG.PY — Hospital Intelligence Dashboard (Deploy Edition)
# ============================================================

# ---------------------------
# Source DBs for analytics
# ---------------------------
import os

DB_CONFIGS = {
    "AHL": {
        "DRIVER": "{ODBC Driver 18 for SQL Server}",
        "SERVER": "192.168.1.4,1433",      # ✅ actual IP of AHL SQL Server
        "DB": "Prodoc2021",
        "USER": "sa",
        "PWD": "Prodoc09",
        "ENCRYPT": "yes",
        "TRUST_CERT": "yes",
        "TIMEOUT": 5,
    },
    "ACI": {
        "DRIVER": "{ODBC Driver 18 for SQL Server}",
        "SERVER": "192.168.20.100,1433",
        "DB": "ACI",
        "USER": "sa",
        "PWD": "Prodoc_23",
        "ENCRYPT": "yes",
        "TRUST_CERT": "yes",
        "TIMEOUT": 5,
    },
    "BALLIA": {
        "DRIVER": "{ODBC Driver 18 for SQL Server}",
        "SERVER": "192.168.1.102,1433",
        "DB": "Prodoc2022",
        "USER": "sa",
        "PWD": "Prodoc_20",
        "ENCRYPT": "yes",
        "TRUST_CERT": "yes",
        "TIMEOUT": 5,
    },
    "SHARPSIGHT": {
        "DRIVER": "{ODBC Driver 18 for SQL Server}",
        "SERVER": "192.168.20.100,1433",
        "DB": "Nayanshree",
        "USER": "sa",
        "PWD": "Prodoc_23",
        "ENCRYPT": "yes",
        "TRUST_CERT": "yes",
        "TIMEOUT": 5,
    },
    "AHLSTORE": {
        "DRIVER": "{ODBC Driver 18 for SQL Server}",
        "SERVER": "192.168.1.102,1433",
        "DB": "AHLStore",
        "USER": "sa",
        "PWD": "Prodoc_20",
        "ENCRYPT": "yes",
        "TRUST_CERT": "yes",
        "TIMEOUT": 5,
    },
    "CANCERUNITSTORE": {
        "DRIVER": "{ODBC Driver 18 for SQL Server}",
        "SERVER": "192.168.1.102,1433",
        "DB": "CancerUnitStore",
        "USER": "sa",
        "PWD": "Prodoc_20",
        "ENCRYPT": "yes",
        "TRUST_CERT": "yes",
        "TIMEOUT": 5,
    },
    "BALLIASTORE": {
        "DRIVER": "{ODBC Driver 18 for SQL Server}",
        "SERVER": "192.168.1.102,1433",
        "DB": "BalliaStore",
        "USER": "sa",
        "PWD": "Prodoc_20",
        "ENCRYPT": "yes",
        "TRUST_CERT": "yes",
        "TIMEOUT": 5,
    },
}

# ---------------------------
# Dedicated legacy canteen DBs
# ---------------------------
CANTEEN_DB_CONFIGS = {
    "AHL": {
        "DRIVER": "{ODBC Driver 18 for SQL Server}",
        "SERVER": "192.168.1.4,1433",
        "DB": "EmpAtten20",
        "USER": "sa",
        "PWD": "Prodoc09",
        "ENCRYPT": "yes",
        "TRUST_CERT": "yes",
        "TIMEOUT": 5,
    },
    "ACI": {
        "DRIVER": "{ODBC Driver 18 for SQL Server}",
        "SERVER": "192.168.20.100,1433",
        "DB": "CanteenACI",
        "USER": "sa",
        "PWD": "Prodoc_23",
        "ENCRYPT": "yes",
        "TRUST_CERT": "yes",
        "TIMEOUT": 5,
    },
}

# Units that should be excluded from analytics/occupancy/revenue background jobs.
ANALYTICS_EXCLUDE_UNITS = ["AHLSTORE", "CANCERUNITSTORE", "BALLIASTORE"]

# ---------------------------
# Login database (HMIS)
# ---------------------------
LOGIN_DB = {
    "DRIVER": "{ODBC Driver 18 for SQL Server}",
    "SERVER": "192.168.20.100,1433",   # Login DB host (ACI)
    "DATABASE": "ACI",
    "UID": "sa",
    "PWD": "Prodoc_23",
    "Encrypt": "yes",
    "TrustServerCertificate": "yes",
    "Connection Timeout": "5",
}

# ---------------------------
# Optional local cache
# ---------------------------
LOCAL_DB = "data/local/metrics.sqlite"
REDIS_URL = "redis://localhost:6379/0"  # shared sessions/caches; override via environment if needed
# SQL stored-procedure fast path for corporate reconciliation.
# For A/B comparison, call API with engine=sp or engine=py to override per request.
USE_CORP_RECON_SP = True

# ---------------------------
# App secret
# ---------------------------
SECRET_KEY = "asarfi_rid_secret_2025"

# ---------------------------
# OTP mail worker (Graph sender)
# ---------------------------
# Set True to start the background OTP mail worker from app.py
ENABLE_OTP_MAIL_WORKER = True
# Optional: tweak polling interval (seconds) for the worker loop
OTP_WORKER_POLL_SECONDS = 5

# ---------------------------
# Booking payment receipt mail worker (Graph sender)
# ---------------------------
ENABLE_BOOKING_PAYMENT_MAIL_WORKER = False
BOOKING_PAYMENT_MAIL_POLL_SECONDS = 5
BOOKING_PAYMENT_EMAIL_DB = {
    "DRIVER": "{ODBC Driver 18 for SQL Server}",
    "SERVER": "192.168.1.102,1433",
    "DATABASE": "Prodoc22",
    "UID": "sa",
    "PWD": "Prodoc_20",
    "Encrypt": "yes",
    "TrustServerCertificate": "yes",
    "Connection Timeout": 5,
    "ConnectRetryCount": 3,
    "ConnectRetryInterval": 5,
}

# ---------------------------
# SMS gateway (PRP Bulk SMS)
# ---------------------------
# Canteen billing queues SMS after the bill is saved. Any provider failure is
# audit-logged but does not block counter billing.
ENABLE_CANTEEN_BILL_SMS = os.getenv("ENABLE_CANTEEN_BILL_SMS", "true").strip().lower() in {"1", "true", "yes", "on"}
PRP_SMS_API_KEY = os.getenv("PRP_SMS_API_KEY", "OpXAiuQyWB8KbWS").strip()
PRP_SMS_USERNAME = os.getenv("PRP_SMS_USERNAME", "20160357").strip()
PRP_SMS_SENDER = os.getenv("PRP_SMS_SENDER", "ARFHSP").strip()
PRP_SMS_TEMPLATE_NAME_URL = os.getenv(
    "PRP_SMS_TEMPLATE_NAME_URL",
    "https://api.bulksmsadmin.com/BulkSMSapi/keyApiSendSMS/SendSmsTemplateName",
).strip()
PRP_SMS_TIMEOUT_SECONDS = float(os.getenv("PRP_SMS_TIMEOUT_SECONDS", "8") or "8")
PRP_SMS_USER_AGENT = os.getenv(
    "PRP_SMS_USER_AGENT",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36 HID/1.0",
).strip()
CANTEEN_BILL_SMS_TEMPLATE_NAME = os.getenv("CANTEEN_BILL_SMS_TEMPLATE_NAME", "CanteenBillCustomerUpdate").strip()

# ---------------------------
# Asset coverage lifecycle reminders (Graph sender)
# ---------------------------
ENABLE_ASSET_COVERAGE_REMINDER_WORKER = True
ASSET_COVERAGE_REMINDER_POLL_SECONDS = 300
# Optional direct recipients. Additional active recipients can be maintained in
# dbo.HID_Asset_Coverage_Recipients.
ASSET_COVERAGE_EMAIL_GROUPS = {
    "AHL": [],
    "ACI": [],
}

# ---------------------------
# Public Asset Breakdown QR employee lookup webhook
# ---------------------------
# Optional backend-only integration for Emp ID lookup from the HR server.
# HID signs each request as HMAC-SHA256 over "<employeeCode>:<timestamp>"
# using EMP_LOOKUP_WEBHOOK_SECRET_KEY and sends:
#   {"code": "...", "timestamp": 1714560000, "signature": "..."}
# Never expose this key to browser/client-side code.
EMP_LOOKUP_WEBHOOK_URL = os.getenv(
    "EMP_LOOKUP_WEBHOOK_URL",
    "https://hr.asarfi.in/api/external-access/get-active-employee-details",
).strip()
EMP_LOOKUP_WEBHOOK_SECRET_KEY = os.getenv("EMP_LOOKUP_WEBHOOK_SECRET_KEY", "AsarfiCall@!2345").strip()
EMP_LOOKUP_WEBHOOK_TIMEOUT_SECS = int(os.getenv("EMP_LOOKUP_WEBHOOK_TIMEOUT_SECS", "8") or "8")

# ---------------------------
# Logging
# ---------------------------
LOG_FILE = "logs/app.log"

# Historical lock is currently disabled. Keep the implementation in code
# so it can be re-enabled later without rebuilding the workflow.
HISTORICAL_LOCK_ENABLED = False

# Historical access: during the first fiscal month, allow the prior
# 3 full calendar months for non-corporate sections. The cutoff
# automatically reverts to the fiscal boundary after that month ends.
HISTORICAL_BOUNDARY_GRACE_MONTHS = 3

# ============================================================
# Reports & Email
# ============================================================

REPORT_KEY = "c1c9b13d1a654e0e9e21a2c1c2b5d5a3b8c9d7ef1a2b3c4d5e6f708192a4bcde"
REPORT_RECIPIENTS = ["ahl.it@asarfihospital.com"]
REPORT_SENDER     = "ahl.it@asarfihospital.com"

SMTP_SERVER = "smtp.office365.com"
SMTP_PORT   = 587
SMTP_USER   = "ahl.it@asarfihospital.com"
SMTP_PASS   = "Frostmourne1135"  # use an app password if required

# LAN base URL for screenshots / callbacks
BASE_URL = "http://127.0.0.1:5358"
