# ============================================================
# CONFIG.PY — Hospital Intelligence Dashboard (Deploy Edition)
# ============================================================

# ---------------------------
# Source DBs for analytics
# ---------------------------
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
# Logging
# ---------------------------
LOG_FILE = "logs/app.log"

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
