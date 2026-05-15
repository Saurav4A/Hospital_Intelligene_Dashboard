from flask import (
    Blueprint, render_template, request, flash, redirect, url_for, session,
    send_file, jsonify, send_from_directory, abort
)
from functools import wraps
from docxtpl import DocxTemplate
import pyodbc
import os
from datetime import datetime, date
import time
from . import config
import importlib
import hmac
import hashlib
import requests
from werkzeug.utils import safe_join
import logging

# ---------- NEW (attendance editor): imports ----------
import pandas as pd
from io import StringIO
import json
import re  # <-- added

hid_config = importlib.import_module("config")

# ---------- logging ----------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

bp = Blueprint("hr_docgen", __name__, url_prefix="/hr/docgen")
_hid_login_required = None
_allowed_units_for_session = None
_HR_DOCGEN_ALLOWED_ROLES = {"IT", "Management", "Departmental Head", "Executive"}


def create_hr_docgen_blueprint(login_required, allowed_units_for_session=None):
    global _hid_login_required, _allowed_units_for_session
    _hid_login_required = login_required
    _allowed_units_for_session = allowed_units_for_session
    return bp

# Directory to serve template previews from
TEMPLATE_PREVIEW_DIR = getattr(
    config, "TEMPLATE_DIR",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "word_templates")
)
_TEMPLATE_PLACEHOLDER_CACHE = {}

def _list_docx_templates():
    """Return sorted .docx templates from TEMPLATE_PREVIEW_DIR."""
    try:
        if not os.path.isdir(TEMPLATE_PREVIEW_DIR):
            return []
        files = []
        for name in os.listdir(TEMPLATE_PREVIEW_DIR):
            full = os.path.join(TEMPLATE_PREVIEW_DIR, name)
            if os.path.isfile(full) and name.lower().endswith(".docx"):
                files.append(name)
        return sorted(files, key=lambda x: x.lower())
    except Exception:
        logging.exception("Could not list templates from %s", TEMPLATE_PREVIEW_DIR)
        return []


def _template_search_dirs():
    """Template locations in priority order for imported and legacy DocGen files."""
    dirs = [TEMPLATE_PREVIEW_DIR, getattr(config, "TEMPLATE_DIR", "")]
    legacy_dir = os.path.join(os.path.splitdrive(config.BASE_DIR)[0] + os.sep, "DocGen", "word_templates")
    dirs.append(legacy_dir)
    out = []
    for path in dirs:
        if path and path not in out and os.path.isdir(path):
            out.append(path)
    return out


def _template_lookup_key(name):
    return re.sub(r"[^a-z0-9]+", "", str(name or "").lower())


def _resolve_template_path(template_name):
    """
    Resolve legacy DB template names without mutating old records.
    Example: "Experience Certificate.docx" -> "Experience_Certificate.docx".
    """
    safe_name = os.path.basename(str(template_name or "").strip())
    if not safe_name:
        raise FileNotFoundError("No template selected for this record.")

    names_to_try = [safe_name]
    if not safe_name.lower().endswith(".docx"):
        names_to_try.append(f"{safe_name}.docx")

    for folder in _template_search_dirs():
        for name in names_to_try:
            candidate = safe_join(folder, name)
            if candidate and os.path.isfile(candidate):
                return candidate

    requested_key = _template_lookup_key(os.path.splitext(safe_name)[0])
    for folder in _template_search_dirs():
        for name in os.listdir(folder):
            full = os.path.join(folder, name)
            if not os.path.isfile(full) or not name.lower().endswith(".docx"):
                continue
            if _template_lookup_key(os.path.splitext(name)[0]) == requested_key:
                return full

    searched = ", ".join(_template_search_dirs()) or "no template folders"
    raise FileNotFoundError(f"Template '{safe_name}' was not found. Searched: {searched}")

def _get_template_placeholders(template_name):
    """
    Return undeclared template variables for one .docx template with a simple mtime cache.
    """
    safe_name = os.path.basename(template_name or "")
    if not safe_name.lower().endswith(".docx"):
        raise FileNotFoundError("Template must be a .docx file")

    full_path = safe_join(TEMPLATE_PREVIEW_DIR, safe_name)
    if not full_path or not os.path.isfile(full_path):
        raise FileNotFoundError("Template not found")

    src_mtime = os.path.getmtime(full_path)
    cached = _TEMPLATE_PLACEHOLDER_CACHE.get(safe_name)
    if cached and cached.get("mtime") == src_mtime:
        return list(cached.get("placeholders", []))

    tpl = DocxTemplate(full_path)
    placeholders = sorted(tpl.get_undeclared_template_variables())
    _TEMPLATE_PLACEHOLDER_CACHE[safe_name] = {"mtime": src_mtime, "placeholders": placeholders}
    return placeholders

def _canonical_placeholder_key(value):
    base = re.sub(r"_(dmy|lower|upper|title)$", "", str(value or ""), flags=re.IGNORECASE)
    return re.sub(r"[^a-z0-9]+", "", base.lower())

def _template_needs_gender(template_name):
    gender_keys = {"gender", "aliashisher", "aliashimher", "aliasheshe"}
    try:
        placeholders = _get_template_placeholders(template_name)
    except Exception:
        return False
    return any(_canonical_placeholder_key(ph) in gender_keys for ph in placeholders)

# ===== Auto-logout on inactivity (idle timeout) =====
# Fixed timeout policy: 30 minutes idle timeout with a warning at 28 minutes.
IDLE_TIMEOUT_SECONDS = 30 * 60
IDLE_WARNING_SECONDS = 2 * 60

def _enforce_idle_timeout():
    # HID owns session lifetime for integrated DocGen routes.
    return None
    # Allow login and static assets through without checks
    if request.endpoint in ("login", "static"):
        return

    now = time.time()
    user = session.get("user")
    last = session.get("last_activity")
    is_heartbeat = request.endpoint == "session_heartbeat"

    if user:
        # If we've been idle longer than the timeout, clear session and force re-login
        if last and (now - float(last) > IDLE_TIMEOUT_SECONDS):
            session.clear()
            if is_heartbeat:
                return jsonify({"ok": False, "expired": True}), 401
            flash("Session expired due to inactivity. Please log in again.", "warning")
            return redirect(url_for("login"))
        # Update activity timestamp on every request
        session["last_activity"] = now
    elif is_heartbeat:
        return jsonify({"ok": False, "expired": True}), 401
    else:
        # No logged-in user; make sure we don't carry a stale timestamp
        session.pop("last_activity", None)


def _inject_session_guard(response):
    # HID injects and handles its own session keepalive behavior.
    return response
    endpoint = request.endpoint or ""
    if "user" not in session:
        return response
    if endpoint in ("login", "static", "session_heartbeat"):
        return response
    if response.direct_passthrough:
        return response

    content_type = (response.headers.get("Content-Type") or "").lower()
    if "text/html" not in content_type:
        return response

    html = response.get_data(as_text=True)
    if "session_guard.js" in html:
        return response

    script_src = url_for("static", filename="hr_docgen/js/session_guard.js")
    script_tag = (
        f'<script src="{script_src}" defer '
        f'data-idle-timeout-seconds="{IDLE_TIMEOUT_SECONDS}" '
        f'data-warning-seconds="{IDLE_WARNING_SECONDS}"></script>'
    )
    patched_html, replaced = re.subn(
        r"</body\s*>",
        f"{script_tag}</body>",
        html,
        flags=re.IGNORECASE,
        count=1,
    )
    if replaced:
        response.set_data(patched_html)
        response.headers.pop("Content-Length", None)
    return response



# ---------------- Indian currency formatter ----------------
def format_inr(num):
    try:
        n = float(str(num).replace(",", "").strip())
    except Exception:
        return str(num)
    n = round(n)
    s = str(int(n))
    if len(s) <= 3:
        grp = s
    else:
        last3 = s[-3:]
        rest = s[:-3]
        parts = []
        while len(rest) > 2:
            parts.append(rest[-2:])
            rest = rest[:-2]
        if rest:
            parts.append(rest)
        grp = ",".join(reversed(parts)) + "," + last3
    return f"Rs {grp}"

def _to_number_or_none(val):
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None
    s = s.replace(",", "")
    try:
        return float(s)
    except Exception:
        return None

def _pct(val):
    n = _to_number_or_none(val)
    return float(n) if n is not None else 0.0

# ---- NEW: Date helpers ----
def _dmy(value):
    """Return DD-MM-YYYY from many possible inputs (str/datetime/None)."""
    if not value:
        return ""
    if isinstance(value, datetime):
        return value.strftime("%d-%m-%Y")
    s = str(value).strip()
    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%Y/%m/%d", "%d/%m/%Y", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(s, fmt).strftime("%d-%m-%Y")
        except ValueError:
            continue
    return s

def _attach_dmy(ctx: dict):
    """Attach *_dmy keys for all date fields used in templates."""
    date_fields = [
        "DateIssued", "Start_Date", "From_Date", "To_Date",
        "Joining_Date", "Relieving_Date"
    ]
    for k in date_fields:
        try:
            ctx[f"{k}_dmy"] = _dmy(ctx.get(k))
        except Exception:
            ctx[f"{k}_dmy"] = ""

# ---- NEW: Alias/pronoun case variants for templates ----
def _attach_alias_variants(ctx: dict):
    """
    For each alias-like field, attach lower/upper/title variants so templates
    can choose the correct casing without changing stored values.
    """
    def _title(s: str) -> str:
        return s[:1].upper() + s[1:].lower() if s else s

    fields = ["Alias_HisHer", "Alias_himHer", "Alias_HeShe", "article"]
    for k in fields:
        v = ctx.get(k)
        if v is None:
            v = ""
        v = str(v)
        ctx[f"{k}_lower"] = v.lower()
        ctx[f"{k}_upper"] = v.upper()
        ctx[f"{k}_title"] = _title(v)

# ---- XML-safe context (for DOCX) ----  <-- added block
_XML_ENTITY_PATTERN = re.compile(r'&(?!amp;|lt;|gt;|quot;|apos;)')

def _xml_escape_string(s: str) -> str:
    s = _XML_ENTITY_PATTERN.sub('&amp;', s)
    s = s.replace('<', '&lt;').replace('>', '&gt;')
    s = s.replace('"', '&quot;').replace("'", '&apos;')
    return s

def _xml_escape_ctx(obj):
    if isinstance(obj, dict):
        return {k: _xml_escape_ctx(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_xml_escape_ctx(v) for v in obj]
    if isinstance(obj, tuple):
        return tuple(_xml_escape_ctx(v) for v in obj)
    if isinstance(obj, str):
        return _xml_escape_string(obj)
    return obj

# Trim helper to drop leading/trailing whitespace from strings inside mappings/lists
def _trim_strings(obj):
    if isinstance(obj, dict):
        return {k: _trim_strings(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_trim_strings(v) for v in obj]
    if isinstance(obj, tuple):
        return tuple(_trim_strings(v) for v in obj)
    if isinstance(obj, str):
        s = obj.replace("\xa0", " ")
        s = s.strip()
        s = re.sub(r"\s+", " ", s)               # collapse internal whitespace/tabs
        s = re.sub(r"\s+([,.;:])", r"\1", s)     # remove space before common punctuation
        return s
    return obj

# --------- filename sanitizer (Windows-safe) ---------
_INVALID_FILENAME_CHARS = re.compile(r'[<>:"/\\|?*\t\r\n]+')

def _safe_filename_part(val, fallback="file"):
    """
    Sanitize one part of a filename to avoid Windows invalid chars and control whitespace.
    """
    s = str(val or "").strip()
    s = _INVALID_FILENAME_CHARS.sub("_", s)
    s = s.replace(" ", "_")
    s = re.sub(r"_+", "_", s).strip("_")
    return s or fallback
# --------------------------------------

def _pick_sql_driver():
    """
    Pick an installed SQL Server ODBC driver.
    Prefers config.SQL_DRIVER, then 18/17/13, then any SQL Server driver.
    """
    preferred = []
    cfg_driver = getattr(config, "SQL_DRIVER", None)
    if cfg_driver:
        preferred.append(cfg_driver)
    preferred.extend([
        "ODBC Driver 18 for SQL Server",
        "ODBC Driver 17 for SQL Server",
        "ODBC Driver 13 for SQL Server",
        "SQL Server",
    ])

    installed = pyodbc.drivers()
    installed_lower = {d.lower(): d for d in installed}

    for name in preferred:
        key = str(name).lower()
        if key in installed_lower:
            return installed_lower[key]

    if installed:
        # Fall back to the last installed driver if none matched the preferred list
        return installed[-1]

    raise RuntimeError(
        "No ODBC SQL Server driver found. Please install Microsoft ODBC Driver 18 or 17."
    )

SQL_DRIVER = _pick_sql_driver()
logging.info("Using SQL driver: %s", SQL_DRIVER)

# =====================================================
# SQL CONNECTION
# =====================================================
if config.SQL_TRUSTED:
    conn_str = (
        f"DRIVER={{{SQL_DRIVER}}};"
        f"SERVER={config.SQL_SERVER};"
        f"DATABASE={config.SQL_DATABASE};"
        "Trusted_Connection=yes;"
        "TrustServerCertificate=yes;"
        "Connection Timeout=5;"
    )
else:
    conn_str = (
        f"DRIVER={{{SQL_DRIVER}}};"
        f"SERVER={config.SQL_SERVER};"
        f"DATABASE={config.SQL_DATABASE};"
        f"UID={config.SQL_USERNAME};PWD={config.SQL_PASSWORD};"
        "TrustServerCertificate=yes;"
        "Connection Timeout=5;"
    )

LAST_PRIMARY_ERR = None
LAST_EMP_ERR = None
conn_emp = None


def _connect_primary():
    return pyodbc.connect(conn_str)


try:
    conn = _connect_primary()
    logging.info("HR DocGen connected to DocGenDB successfully.")
except Exception as exc:
    conn = None
    LAST_PRIMARY_ERR = exc
    logging.error("HR DocGen primary DB connection failed during startup: %r", exc)

def _current_db_name(cxn):
    cur = cxn.cursor()
    cur.execute("SELECT DB_NAME()")
    return cur.fetchone()[0]

# ---------- NEW: tiny diag helpers ----------
def _server_db_info(cxn):
    """Return (server_name, db_name) or ('?', '?') if not available."""
    try:
        cur = cxn.cursor()
        cur.execute("SELECT @@SERVERNAME, DB_NAME()")
        row = cur.fetchone()
        return (row[0] or "?", row[1] or "?")
    except Exception:
        try:
            cur = cxn.cursor()
            cur.execute("SELECT DB_NAME()")
            row = cur.fetchone()
            return ("?", row[0] or "?")
        except Exception:
            return ("?", "?")

def _is_transient_comm_error(err: Exception) -> bool:
    s = str(err)
    return ("08S01" in s) or ("10054" in s) or ("Communication link failure" in s)

def ping_conn(cxn, label):
    """Quick health check; returns (ok_bool, message)."""
    try:
        cur = cxn.cursor()
        cur.execute("SELECT 1")
        cur.fetchone()
        server, db = _server_db_info(cxn)
        return True, f"{label}: OK (server={server}, db={db})"
    except Exception as e:
        server, db = _server_db_info(cxn)
        return False, f"{label}: FAIL (server={server}, db={db}) -> {e!r}"

def safe_execute(cxn, sql, params=(), label="PRIMARY"):
    """
    Wrap .execute() to log which connection failed and auto-reconnect
    once on transient 08S01/10054 errors.
    """
    global conn, conn_emp, LAST_EMP_ERR, LAST_PRIMARY_ERR
    attempts = 2
    for i in range(attempts):
        try:
            if cxn is None:
                if label == "PRIMARY":
                    conn = _connect_primary()
                    LAST_PRIMARY_ERR = None
                    cxn = conn
                else:
                    raise RuntimeError(f"{label} connection is not configured")
            cur = cxn.cursor()
            cur.execute(sql, params)
            return cur
        except Exception as e:
            if (not _is_transient_comm_error(e)) or (i == attempts - 1):
                server, db = _server_db_info(cxn)
                logging.error("DB error on %s (server=%s, db=%s): %r", label, server, db, e)
                raise
            logging.warning("%s connection dropped (%s). Reconnecting and retrying...", label, e)
            try:
                try:
                    cxn.close()
                except Exception:
                    pass
                if (label == "PRIMARY") or (cxn is conn):
                    conn = _connect_primary()
                    LAST_PRIMARY_ERR = None
                    cxn = conn
                else:
                    conn = _connect_primary()
                    LAST_PRIMARY_ERR = None
                    cxn = conn
                time.sleep(0.2)
            except Exception as re:
                logging.error("Reconnect failed for %s: %r", label, re)
                raise

# --- helper: locate EmployeeMaster view with correct database/schema/name ---
def _employee_master_fqn(cxn):
    """
    Returns a fully-qualified name to query the Employee master object,
    using config.EMPLOYEE_SCHEMA and config.EMPLOYEE_VIEW.
    """
    db_name = _current_db_name(cxn)
    schema  = getattr(config, "EMPLOYEE_SCHEMA", "dbo")
    view    = getattr(config, "EMPLOYEE_VIEW", "vEmployeeMaster")
    return f"[{db_name}].[{schema}].[{view}]"

def _parse_header_date(col_name: str):
    """
    Given a column name (e.g., '01-Apr-2024'), return a date object or None.
    Attempts multiple date formats.
    """
    s = str(col_name).strip()
    for fmt in ("%d-%b-%Y", "%d-%B-%Y", "%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            pass
    return None

def _dominant_month_from_headers(keys):
    """
    From CSV/edit grid headers, compute:
      - period_start (min date header)
      - period_end   (max date header)
      - dominant month label (month-year with most headers)
    Returns (period_start, period_end, "Month YYYY") or (None, None, None) if no date headers.
    """
    dates = []
    for k in keys:
        d = _parse_header_date(k)
        if d is not None:
            dates.append(d)
    if not dates:
        return None, None, None

    period_start = min(dates)
    period_end = max(dates)

    by_month = {}
    for d in dates:
        ym = (d.year, d.month)
        by_month[ym] = by_month.get(ym, 0) + 1

    dominant_ym = sorted(by_month.items(), key=lambda kv: (kv[1], kv[0][0], kv[0][1]))[-1][0]
    dominant_dt = date(dominant_ym[0], dominant_ym[1], 1)
    dominant_label = dominant_dt.strftime("%B %Y")
    return period_start, period_end, dominant_label

def _ensure_attendance_tables(cxn):
    """
    Create persistence tables if not present:
      - dbo.Attendance_Batches
      - dbo.Attendance_Rows
      - dbo.AlwaysFullAttendance (NEW)
    """
    global conn, LAST_PRIMARY_ERR
    if cxn is None:
        conn = _connect_primary()
        LAST_PRIMARY_ERR = None
        cxn = conn
    safe_execute(cxn, """
        IF NOT EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'dbo.Attendance_Batches') AND type = 'U')
        BEGIN
            CREATE TABLE dbo.Attendance_Batches(
                BatchID            INT IDENTITY(1,1) PRIMARY KEY,
                UploadedOn         DATETIME NOT NULL CONSTRAINT DF_AttBatch_UploadedOn DEFAULT(GETDATE()),
                UploadedBy         NVARCHAR(100) NULL,
                OriginalFilename   NVARCHAR(255) NULL,
                PeriodStart        DATE NULL,
                PeriodEnd          DATE NULL,
                DominantMonthLabel NVARCHAR(40) NULL
            );
        END
    """, label="PRIMARY")

    safe_execute(cxn, """
        IF NOT EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'dbo.Attendance_Rows') AND type = 'U')
        BEGIN
            CREATE TABLE dbo.Attendance_Rows(
                RowID   INT IDENTITY(1,1) PRIMARY KEY,
                BatchID INT NOT NULL FOREIGN KEY REFERENCES dbo.Attendance_Batches(BatchID),
                RowJSON NVARCHAR(MAX) NOT NULL
            );
        END
    """, label="PRIMARY")
    
    # NEW TABLE: Always Full Attendance list
    # Check if table exists with old column name and migrate if needed
    try:
        safe_execute(cxn, """
            IF EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'dbo.AlwaysFullAttendance') AND type = 'U')
            BEGIN
                -- Check if old column name exists
                IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'dbo.AlwaysFullAttendance') AND name = 'EmployeeName')
                BEGIN
                    -- Rename column from EmployeeName to EmployeeCode
                    EXEC sp_rename 'dbo.AlwaysFullAttendance.EmployeeName', 'EmployeeCode', 'COLUMN';
                END
            END
            ELSE
            BEGIN
                -- Create new table with correct column name
                CREATE TABLE dbo.AlwaysFullAttendance(
                    ID          INT IDENTITY(1,1) PRIMARY KEY,
                    EmployeeCode NVARCHAR(255) NOT NULL UNIQUE,
                    AddedOn     DATETIME NOT NULL CONSTRAINT DF_AlwaysFullAtt_AddedOn DEFAULT(GETDATE()),
                    AddedBy     NVARCHAR(100) NULL
                );
            END
        """, label="PRIMARY")
        cxn.commit()
    except Exception as e:
        logging.warning("Could not create/migrate AlwaysFullAttendance table: %r", e)
    
    cxn.commit()

# Make sure the tables exist
try:
    _ensure_attendance_tables(conn)
    logging.info("HR DocGen attendance tables ready.")
except Exception as e:
    logging.warning("Could not ensure HR DocGen attendance tables: %r", e)

# ------- NEW: UI filtering helper to hide uploaded calc columns -------
def _canon(name: str) -> str:
    """canonicalize a header: lowercase, strip spaces/underscores/hyphens and '+' """
    if not name:
        return ""
    s = str(name).lower()
    for ch in (" ", "_", "-", "+"):
        s = s.replace(ch, "")
    return s

_CALC_CANON = {
    "present",
    "totalleave",
    "totalholiday",
    "totalweeklyoff",
    "payabledays",
    "co",
}

_CALC_SYNONYMS = {
    "Present",
    "TotalLeave", "Total Leave", "Total Leaves",
    "TotalHoliday", "Total Holidays",
    "TotalWeeklyOff", "Total Weekly Off",
    "PayableDays", "Payable Days",
    "CO+", "CO Plus", "COPlus",
}

def _drop_calc_cols_from_df(df: pd.DataFrame) -> pd.DataFrame:
    """Remove columns from an uploaded CSV that correspond to the calculated set."""
    drop_cols = []
    for c in df.columns:
        c_str = str(c)
        if c_str in _CALC_SYNONYMS:
            drop_cols.append(c)
            continue
        if _canon(c_str) in _CALC_CANON:
            drop_cols.append(c)
    if drop_cols:
        return df.drop(columns=drop_cols, errors="ignore")
    return df

# =====================================================
# NEW: Always Full Attendance Helper Functions
# =====================================================
def _get_always_full_employees():
    """Get list of employees who should always have 100% attendance"""
    try:
        # Ensure connection is alive
        if conn is None:
            logging.error("Database connection not available")
            return []
        
        cursor = safe_execute(
            conn,
            "SELECT EmployeeCode FROM dbo.AlwaysFullAttendance ORDER BY EmployeeCode",
            label="PRIMARY"
        )
        rows = cursor.fetchall()
        return [row[0] for row in rows]
    except Exception as e:
        logging.error("Error fetching always full attendance list: %r", e)
        return []

def _add_always_full_employee(employee_code, added_by):
    """Add an employee to the always full attendance list"""
    try:
        # Ensure connection is alive
        if conn is None:
            return False, "Database connection not available"
        
        safe_execute(
            conn,
            "INSERT INTO dbo.AlwaysFullAttendance (EmployeeCode, AddedBy) VALUES (?, ?)",
            (employee_code.strip(), added_by),
            label="PRIMARY"
        )
        conn.commit()
        return True, "Employee added successfully"
    except Exception as e:
        if "UNIQUE" in str(e) or "duplicate" in str(e).lower():
            return False, "Employee code already exists in the list"
        logging.error("Error adding employee to always full list: %r", e)
        return False, str(e)

def _remove_always_full_employee(employee_code):
    """Remove an employee from the always full attendance list"""
    try:
        # Ensure connection is alive
        if conn is None:
            return False, "Database connection not available"
        
        safe_execute(
            conn,
            "DELETE FROM dbo.AlwaysFullAttendance WHERE EmployeeCode = ?",
            (employee_code,),
            label="PRIMARY"
        )
        conn.commit()
        return True, "Employee removed successfully"
    except Exception as e:
        logging.error("Error removing employee from always full list: %r", e)
        return False, str(e)

def _process_always_full_attendance(df: pd.DataFrame) -> pd.DataFrame:
    """
    Process dataframe to:
    1. Ensure employees in 'Always Full Attendance' list have 100% attendance (fill 'P').
    2. Add missing employees from the list.
    3. STRICTLY FORCE CO+ to 0 for these employees (Sanitization).
    """
    always_full_list = _get_always_full_employees()
    
    if not always_full_list:
        return df
    
    # Identify the employee code column
    code_col = None
    for col in df.columns:
        col_lower = str(col).lower().replace(' ', '').replace('_', '')
        if any(x in col_lower for x in ['employeecode', 'empcode', 'code']):
            code_col = col
            break
    
    if code_col is None:
        logging.warning("Could not identify employee code column in attendance data")
        return df
    
    # Identify Date columns
    date_cols = [col for col in df.columns if _parse_header_date(col) is not None]
    
    # Identify CO+ column (to force it to 0)
    co_col = None
    for col in df.columns:
        if _canon(str(col)) in ["co+", "coplus", "co_plus"]:
            co_col = col
            break

    # --- PROCESS ROWS ---
    for idx, row in df.iterrows():
        emp_code = str(row[code_col]).strip()
        
        if emp_code in always_full_list:
            # 1. Fill 'P' in date columns if they exist
            if date_cols:
                for date_col in date_cols:
                    df.at[idx, date_col] = 'P'
            
            # 2. FORCE CO+ TO 0
            if co_col:
                df.at[idx, co_col] = 0

            logging.info("Enforced Full Salary Rules for %s (P=All, CO+=0)", emp_code)

    # --- ADD MISSING EMPLOYEES ---
    existing_employees = set(df[code_col].astype(str).str.strip())
    missing_employees = [emp for emp in always_full_list if emp not in existing_employees]
    
    if missing_employees:
        new_rows = []
        for emp_code in missing_employees:
            new_row = {code_col: emp_code}
            
            # Set 'P' for dates
            for date_col in date_cols:
                new_row[date_col] = 'P'
            
            # Set 0 for CO+ if column exists
            if co_col:
                new_row[co_col] = 0

            # Fill other columns with empty strings to match DataFrame structure
            for col in df.columns:
                if col not in new_row:
                    new_row[col] = ''
            
            new_rows.append(new_row)
            logging.info("Added %s with 100%% attendance", emp_code)

        if new_rows:
            df = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)
    
    return df

# =====================================================
# LOGIN DECORATOR
# =====================================================
def login_required(roles=None):
    def wrapper(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if _hid_login_required is None:
                abort(500)
            protected = _hid_login_required(allowed_roles=_HR_DOCGEN_ALLOWED_ROLES)(f)
            return protected(*args, **kwargs)
        return decorated_function
    return wrapper

# =====================================================
# LOGIN & DASHBOARD
# =====================================================
@bp.route("/")
@login_required(roles=["admin", "hr-admin", "data-entry"])
def home():
    """Main HR certificate entry form"""
    return render_template("hr_docgen/data_entry.html", template_files=_list_docx_templates())

@bp.route("/index")
def index():
    return redirect(url_for("hr_docgen.dashboard"))

@bp.route("/login", methods=["GET", "POST"])
def login():
    return redirect(url_for("login"))

@bp.route("/session/heartbeat", methods=["POST"])
def session_heartbeat():
    if "username" not in session:
        return jsonify({"ok": False, "expired": True}), 401
    return jsonify({
        "ok": True,
        "timeout_seconds": IDLE_TIMEOUT_SECONDS,
        "warning_seconds": IDLE_WARNING_SECONDS,
    })

@bp.route("/logout")
def logout():
    return redirect(url_for("logout"))

@bp.route("/dashboard")
@login_required()
def dashboard():
    stats = {
        "total_records": 0,
        "active_records": 0,
        "inactive_records": 0,
        "template_count": 0,
        "today_entries": 0,
        "attendance_batches": 0,
    }
    health = {
        "primary_ok": False,
        "primary_msg": "Unknown",
        "employee_ok": True,
        "employee_msg": "Using HR Systems POST/fetch source",
    }

    def _scalar_int(sql, params=(), default=0):
        try:
            cur = safe_execute(conn, sql, params, label="PRIMARY")
            row = cur.fetchone()
            if not row:
                return default
            val = row[0]
            return int(val) if val is not None else default
        except Exception:
            logging.exception("Dashboard metric query failed")
            return default

    stats["total_records"] = _scalar_int("SELECT COUNT(1) FROM dbo.HR_Certificates_Master")
    stats["active_records"] = _scalar_int("SELECT COUNT(1) FROM dbo.HR_Certificates_Master WHERE IsActive=1")
    stats["inactive_records"] = _scalar_int("SELECT COUNT(1) FROM dbo.HR_Certificates_Master WHERE IsActive=0")
    stats["template_count"] = _scalar_int(
        "SELECT COUNT(DISTINCT TemplateName) FROM dbo.HR_Certificates_Master WHERE TemplateName IS NOT NULL AND LTRIM(RTRIM(TemplateName)) <> ''"
    )
    stats["today_entries"] = _scalar_int(
        "SELECT COUNT(1) FROM dbo.HR_Certificates_Master WHERE CAST(InsertedOn AS DATE)=CAST(GETDATE() AS DATE)"
    )
    stats["attendance_batches"] = _scalar_int(
        """
        SELECT CASE WHEN OBJECT_ID('dbo.Attendance_Batches','U') IS NULL
                    THEN 0
                    ELSE (SELECT COUNT(1) FROM dbo.Attendance_Batches)
               END
        """
    )

    primary_ok, primary_msg = ping_conn(conn, "PRIMARY")
    health["primary_ok"] = primary_ok
    health["primary_msg"] = primary_msg

    return render_template(
        "hr_docgen/dashboard.html",
        username=session.get("username") or session.get("user"),
        role=session.get("role"),
        stats=stats,
        health=health,
    )

@bp.route("/word_templates/<path:filename>")
@login_required(roles=["admin", "hr-admin", "data-entry"])
def serve_word_template(filename):
    """
    Serves files from the word_templates directory.
    """
    safe_name = os.path.basename(filename)
    full_path = safe_join(TEMPLATE_PREVIEW_DIR, safe_name)

    if not full_path or not os.path.isfile(full_path):
        abort(404)

    return send_from_directory(
        TEMPLATE_PREVIEW_DIR,
        safe_name,
        mimetype="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        as_attachment=False,
        download_name=safe_name,
        max_age=0
    )

@bp.route("/template_preview/<path:filename>")
@login_required(roles=["admin", "hr-admin", "data-entry"])
def serve_template_preview_pdf(filename):
    """Render a .docx template to PDF and serve it inline for preview."""
    safe_name = os.path.basename(filename)
    if not safe_name.lower().endswith(".docx"):
        abort(404)
    full_path = safe_join(TEMPLATE_PREVIEW_DIR, safe_name)

    if not full_path or not os.path.isfile(full_path):
        abort(404)

    preview_dir = os.path.join(config.OUTPUT_DIR, "template_previews")
    os.makedirs(preview_dir, exist_ok=True)
    pdf_name = os.path.splitext(safe_name)[0] + ".pdf"
    pdf_path = os.path.join(preview_dir, pdf_name)

    try:
        src_mtime = os.path.getmtime(full_path)
        if not os.path.exists(pdf_path) or os.path.getmtime(pdf_path) < src_mtime:
            from docx2pdf import convert
            convert(full_path, preview_dir)
        if not os.path.exists(pdf_path) or os.path.getsize(pdf_path) == 0:
            raise RuntimeError("PDF preview was not generated")
        return send_file(pdf_path, mimetype="application/pdf", as_attachment=False, max_age=0)
    except Exception:
        logging.exception("Error generating template preview PDF for %s", safe_name)
        abort(500)

@bp.route("/api/template_placeholders/<path:filename>")
@login_required(roles=["admin", "hr-admin", "data-entry"])
def api_template_placeholders(filename):
    """Return undeclared placeholders for one selected .docx template."""
    safe_name = os.path.basename(filename)
    try:
        placeholders = _get_template_placeholders(safe_name)
        return jsonify({
            "status": "success",
            "template": safe_name,
            "placeholders": placeholders,
        })
    except FileNotFoundError:
        return jsonify({"status": "error", "message": "Template not found"}), 404
    except Exception as e:
        logging.exception("Failed to read placeholders for template %s", safe_name)
        return jsonify({"status": "error", "message": str(e)}), 500

@bp.route("/edit/<int:record_id>")
@login_required(roles=["admin", "hr-admin"])
def edit_record(record_id):
    """Open the data_entry form with an existing record pre-filled."""
    try:
        cursor = safe_execute(
            conn,
            "SELECT * FROM dbo.HR_Certificates_Master WHERE ID=?",
            (record_id,),
            label="PRIMARY"
        )
        row = cursor.fetchone()
        if not row:
            flash("Record not found!", "danger")
            return redirect(url_for("hr_docgen.show_records"))
        columns = [col[0] for col in cursor.description]
        record = _trim_strings(dict(zip(columns, row)))

        record["PF"] = record.get("ExtraField1") or ""
        record["ESIC"] = record.get("ExtraField2") or ""

        return render_template(
            "hr_docgen/data_entry.html",
            record=record,
            edit_mode=True,
            template_files=_list_docx_templates()
        )
    except Exception as e:
        print("Error loading record for edit:", e)
        flash(f"Error loading record for edit: {e}", "danger")
        return redirect(url_for("hr_docgen.show_records"))

# ===================== HR Systems employee source adapter =====================
def _clean_text(value, limit=300):
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    return text[:limit]


def _pick_value(source: dict, keys, limit=300):
    if not isinstance(source, dict):
        return ""
    for key in keys:
        if key in source and source.get(key) not in (None, ""):
            return _clean_text(source.get(key), limit)
    lower_map = {str(k).lower(): v for k, v in source.items()}
    for key in keys:
        val = lower_map.get(str(key).lower())
        if val not in (None, ""):
            return _clean_text(val, limit)
    return ""


def _normalise_gender(value):
    text = _clean_text(value, 20).lower()
    if text in {"m", "male"}:
        return "Male"
    if text in {"f", "female"}:
        return "Female"
    return ""


def _extract_employee_payload(payload):
    if not isinstance(payload, dict):
        return None
    for key in ("employee", "data", "result", "details"):
        value = payload.get(key)
        if isinstance(value, dict):
            return value
        if isinstance(value, list) and value and isinstance(value[0], dict):
            return value[0]
    return payload


def _fetch_employee_from_hr_system(emp_id: str):
    emp_code = _clean_text(emp_id, 80)
    if not emp_code:
        return None
    url = str(getattr(hid_config, "EMP_LOOKUP_WEBHOOK_URL", "") or "").strip()
    secret = str(getattr(hid_config, "EMP_LOOKUP_WEBHOOK_SECRET_KEY", "") or "").strip()
    timeout = int(getattr(hid_config, "EMP_LOOKUP_WEBHOOK_TIMEOUT_SECS", 8) or 8)
    if not url or not secret:
        logging.warning("HR Systems employee lookup is not configured.")
        return None

    timestamp = int(time.time())
    signature = hmac.new(
        secret.encode("utf-8"),
        f"{emp_code}:{timestamp}".encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    response = requests.post(
        url,
        headers={"Content-Type": "application/json"},
        json={"code": emp_code, "timestamp": timestamp, "signature": signature},
        timeout=max(1, min(timeout, 30)),
    )
    payload = response.json() if response.content else {}
    if response.status_code == 404:
        return None
    response.raise_for_status()
    if isinstance(payload, dict) and payload.get("success") is False:
        return None
    employee = _extract_employee_payload(payload)
    if not isinstance(employee, dict):
        return None

    emp_id_value = _pick_value(employee, ["emp_id", "employee_id", "EmpID", "EmployeeCode", "code", "employeeCode"], 50)
    name_value = _pick_value(employee, ["emp_name", "employee_name", "EmployeeName", "name", "employeeName", "fullName"], 200)
    if not emp_id_value or not name_value:
        return None

    return {
        "Emp_ID": emp_id_value,
        "Emp_Name": name_value,
        "Gender": _normalise_gender(_pick_value(employee, ["gender", "Gender", "sex", "Sex"], 20)),
        "Department": _pick_value(employee, ["department", "Department", "departmentName", "dept"], 200),
        "Designation": _pick_value(employee, ["designation", "Designation", "designationName", "SubDepartmentName", "MedicalDepartment"], 200),
        "Unit": _pick_value(employee, ["unit", "Unit", "unitName", "unit_code", "Location", "Deputation"], 80),
        "source": "hr_systems_post_fetch",
    }


@bp.route("/api/employees", methods=["GET"])
@login_required(roles=["admin", "hr-admin", "data-entry"])
def api_employees():
    """
    Returns employees with:
      Emp_ID (EmployeeCode), Emp_Name (EmployeeName), Gender (Sex), Department
    """
    try:
        q = _clean_text(request.args.get("q", ""), 80)
        data = []
        if q:
            employee = _fetch_employee_from_hr_system(q)
            if employee:
                data.append(employee)
        return jsonify({"status": "success", "data": data})
    except Exception as e:
        logging.warning("Error fetching HR Systems employee: %r", e)
        return jsonify({"status": "error", "message": str(e)}), 500

@bp.route("/api/employee/<emp_id>", methods=["GET"])
@login_required(roles=["admin", "hr-admin", "data-entry"])
def api_employee_by_id(emp_id):
    """
    Returns one employee by Emp_ID (EmployeeCode):
      Emp_ID, Emp_Name, Gender, Department
    """
    try:
        employee = _fetch_employee_from_hr_system(emp_id)
        if not employee:
            return jsonify({"status": "not_found", "data": None}), 404
        return jsonify({"status": "success", "data": employee})
    except Exception as e:
        logging.warning("Error fetching HR Systems employee by id: %r", e)
        return jsonify({"status": "error", "message": str(e)}), 500
# ===========================================================================================

@bp.route("/save_generate", methods=["POST"])
@login_required(roles=["admin", "hr-admin", "data-entry"])
def save_generate():
    """Insert OR Update entry in HR_Certificates_Master."""
    try:
        form = request.form
        rec_id = form.get("ID")

        CertificateType = form.get("CertificateType")
        Ref_No = form.get("Ref_No")
        DateIssued = form.get("DateIssued")
        Emp_Name = form.get("Emp_Name")
        Emp_2ndName = form.get("Emp_2ndName")
        Emp_ID = form.get("Emp_ID")
        Department = form.get("Department")
        Designation = form.get("Designation")
        Institute = form.get("Institute")
        Institute_Location = form.get("Institute_Location")
        Internship_Location = form.get("Internship_Location")
        Duration = form.get("Duration")
        Start_Date = form.get("Start_Date")
        Reporting_to = form.get("Reporting_to")
        Timing = form.get("Timing")
        Reason_1 = form.get("Reason_1")
        ETA = form.get("ETA")
        Summary = form.get("Summary")
        Para_1 = form.get("Para_1")
        Para_2 = form.get("Para_2")
        Para_3 = form.get("Para_3")
        Alias_HisHer = form.get("Alias_HisHer")
        Alias_himHer = form.get("Alias_himHer")
        Alias_HeShe = form.get("Alias_HeShe")
        To_Date = form.get("To_Date")
        From_Date = form.get("From_Date")
        Joining_Date = form.get("Joining_Date")
        Relieving_Date = form.get("Relieving_Date")
        Subject = form.get("Subject")
        Reason = form.get("Reason")
        Penalty = form.get("Penalty")
        ExtraField1 = form.get("ExtraField1")
        ExtraField2 = form.get("ExtraField2")
        TemplateName = form.get("TemplateName")
        ExportPDF = 1 if "ExportPDF" in form else 0

        Address = form.get("Address")
        State = form.get("State")
        Pincode = form.get("Pincode")
        Organizationwithaddress = form.get("Organizationwithaddress")
        Salary = form.get("Salary")
        SalaryinWords = form.get("SalaryinWords")
        HR = form.get("HR")
        Place = form.get("Place")
        Leaves_allotted = form.get("Leaves_allotted")
        article = form.get("article")
        Gender = form.get("Gender")

        Basic_Percentage = form.get("Basic_Percentage") or 0
        HRA_Percentage = form.get("HRA_Percentage") or 0
        Conveyance_Percentage = form.get("Conveyance_Percentage") or 0
        Special_Percentage = form.get("Special_Percentage") or 0

        required_fields = {
            "Certificate Type": CertificateType,
            "Reference No.": Ref_No,
            "Template Name": TemplateName,
        }
        if _template_needs_gender(TemplateName):
            required_fields["Gender"] = Gender
        missing = [label for label, val in required_fields.items() if not (val and str(val).strip())]
        if missing:
            flash(f"Missing required fields: {', '.join(missing)}", "danger")
            return redirect(url_for("hr_docgen.home"))

        PF_override = form.get("PF")
        ESIC_override = form.get("ESIC")
        if PF_override and PF_override.strip():
            ExtraField1 = PF_override.strip()
        if ESIC_override and ESIC_override.strip():
            ExtraField2 = ESIC_override.strip()

        if rec_id:
            # ===================== FIXED: proper "col = ?" pairs =====================
            cursor = safe_execute(conn, """
                UPDATE dbo.HR_Certificates_Master SET
                    CertificateType = ?, Ref_No = ?, DateIssued = ?, Emp_Name = ?, Emp_2ndName = ?, Emp_ID = ?, Department = ?,
                    Designation = ?, Institute = ?, Institute_Location = ?, Internship_Location = ?, Duration = ?,
                    Start_Date = ?, Reporting_to = ?, Timing = ?, Reason_1 = ?, ETA = ?, Summary = ?, Para_1 = ?, Para_2 = ?, Para_3 = ?,
                    Alias_HisHer = ?, Alias_himHer = ?, Alias_HeShe = ?, To_Date = ?, From_Date = ?, Joining_Date = ?,
                    Relieving_Date = ?, Subject = ?, Reason = ?, Penalty = ?, ExtraField1 = ?, ExtraField2 = ?,
                    TemplateName = ?, ExportPDF = ?, Address = ?, State = ?, Pincode = ?, Organizationwithaddress = ?, Salary = ?,
                    SalaryinWords = ?, HR = ?, Place = ?, Leaves_allotted = ?, article = ?, Basic_Percentage = ?, HRA_Percentage = ?,
                    Conveyance_Percentage = ?, Special_Percentage = ?, Gender = ?
                WHERE ID = ?
            """, (
                CertificateType, Ref_No, DateIssued, Emp_Name, Emp_2ndName, Emp_ID, Department,
                Designation, Institute, Institute_Location, Internship_Location, Duration,
                Start_Date, Reporting_to, Timing, Reason_1, ETA, Summary, Para_1, Para_2, Para_3,
                Alias_HisHer, Alias_himHer, Alias_HeShe, To_Date, From_Date, Joining_Date,
                Relieving_Date, Subject, Reason, Penalty, ExtraField1, ExtraField2,
                TemplateName, ExportPDF, Address, State, Pincode, Organizationwithaddress, Salary,
                SalaryinWords, HR, Place, Leaves_allotted, article, Basic_Percentage, HRA_Percentage,
                Conveyance_Percentage, Special_Percentage, Gender, rec_id
            ), label="PRIMARY")
            # ========================================================================
        else:
            cursor = safe_execute(conn, """
                INSERT INTO dbo.HR_Certificates_Master (
                    CertificateType, Ref_No, DateIssued, Emp_Name, Emp_2ndName, Emp_ID, Department,
                    Designation, Institute, Institute_Location, Internship_Location, Duration,
                    Start_Date, Reporting_to, Timing, Reason_1, ETA, Summary, Para_1, Para_2, Para_3,
                    Alias_HisHer, Alias_himHer, Alias_HeShe, To_Date, From_Date, Joining_Date,
                    Relieving_Date, Subject, Reason, Penalty, ExtraField1, ExtraField2,
                    TemplateName, ExportPDF, InsertedOn, IsActive,
                    Address, State, Pincode, Organizationwithaddress, Salary,
                    SalaryinWords, HR, Place, Leaves_allotted, article,
                    Basic_Percentage, HRA_Percentage, Conveyance_Percentage, Special_Percentage, Gender
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, GETDATE(), 1,
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                CertificateType, Ref_No, DateIssued, Emp_Name, Emp_2ndName, Emp_ID, Department,
                Designation, Institute, Institute_Location, Internship_Location, Duration,
                Start_Date, Reporting_to, Timing, Reason_1, ETA, Summary, Para_1, Para_2, Para_3,
                Alias_HisHer, Alias_himHer, Alias_HeShe, To_Date, From_Date, Joining_Date,
                Relieving_Date, Subject, Reason, Penalty, ExtraField1, ExtraField2,
                TemplateName, ExportPDF,
                Address, State, Pincode, Organizationwithaddress, Salary,
                SalaryinWords, HR, Place, Leaves_allotted, article,
                Basic_Percentage, HRA_Percentage, Conveyance_Percentage, Special_Percentage, Gender
            ), label="PRIMARY")

        conn.commit()
        flash("Record saved successfully!", "success")
        return redirect(url_for("hr_docgen.show_records"))

    except Exception as e:
        print("Error saving record:", e)
        flash(f"Error saving record: {e}", "danger")
        return redirect(url_for("hr_docgen.home"))

# =====================================================
# TEMPLATE PREVIEWS
# =====================================================
@bp.route("/templates/<filename>")
@login_required()
def serve_template_preview(filename):
    """Serve .docx files for preview."""
    try:
        return send_from_directory(TEMPLATE_PREVIEW_DIR, filename)
    except Exception as e:
        abort(404)

# =====================================================
# FORM & RECORD MANAGEMENT
# =====================================================

@bp.route("/records")
@login_required(roles=["admin", "hr-admin", "data-entry"])
def show_records():
    try:
        cursor = safe_execute(
            conn,
            "SELECT * FROM dbo.HR_Certificates_Master ORDER BY InsertedOn DESC",
            label="PRIMARY"
        )
        rows = cursor.fetchall()
        if not rows:
            return render_template("hr_docgen/person_list.html", records=[])
        columns = [col[0] for col in cursor.description]
        records = [dict(zip(columns, row)) for row in rows]
        return render_template("hr_docgen/person_list.html", records=records)
    except Exception as e:
        print("Error loading records:", e)
        flash(f"Error loading records: {e}", "danger")
        return redirect(url_for("hr_docgen.dashboard"))

@bp.route("/toggle_status/<int:record_id>", methods=["POST"], endpoint="toggle_status")
@login_required(roles=["admin", "hr-admin"])
def toggle_status(record_id):
    """Activate or deactivate a record"""
    try:
        cursor = safe_execute(
            conn,
            "SELECT IsActive FROM dbo.HR_Certificates_Master WHERE ID=?",
            (record_id,),
            label="PRIMARY"
        )
        row = cursor.fetchone()
        if not row:
            flash("Record not found!", "danger")
            return redirect(url_for("hr_docgen.show_records"))

        new_status = 0 if row[0] == 1 else 1
        safe_execute(
            conn,
            "UPDATE dbo.HR_Certificates_Master SET IsActive=? WHERE ID=?",
            (new_status, record_id),
            label="PRIMARY"
        )
        conn.commit()

        msg = "Record deactivated successfully!" if new_status == 0 else "Record reactivated successfully!"
        flash(msg, "info")
        return redirect(url_for("hr_docgen.show_records"))
    except Exception as e:
        print("Error toggling status:", e)
        flash(f"Error toggling status: {e}", "danger")
        return redirect(url_for("hr_docgen.show_records"))

@bp.route("/generate_pdf/<int:record_id>", methods=["POST"])
@login_required(roles=["admin", "hr-admin", "data-entry"])
def generate_pdf(record_id):
    try:
        cursor = safe_execute(
            conn,
            "SELECT * FROM dbo.HR_Certificates_Master WHERE ID=?",
            (record_id,),
            label="PRIMARY"
        )
        columns = [col[0] for col in cursor.description]
        row = cursor.fetchone()
        if not row:
            flash("Record not found!", "danger")
            return redirect(url_for("hr_docgen.show_records"))
        record = _trim_strings(dict(zip(columns, row)))

        if record.get("IsActive") == 0:
            flash("Cannot generate PDF for deactivated record!", "warning")
            return redirect(url_for("hr_docgen.show_records"))

        total_salary = _to_number_or_none(record.get("Salary")) or 0.0
        monthly_total = round(total_salary / 12)

        basic_pct = _pct(record.get("Basic_Percentage"))
        hra_pct = _pct(record.get("HRA_Percentage"))
        conv_pct = _pct(record.get("Conveyance_Percentage"))
        spec_pct = _pct(record.get("Special_Percentage"))

        if basic_pct == 0 and hra_pct == 0 and conv_pct == 0 and spec_pct == 0:
            basic_pct, hra_pct, conv_pct, spec_pct = 55.0, 13.0, 10.0, 5.0

        basic = round(total_salary * basic_pct / 100.0)
        hra = round(total_salary * hra_pct / 100.0)
        conveyance = round(total_salary * conv_pct / 100.0)
        special = round(total_salary * spec_pct / 100.0)

        pf_override = _to_number_or_none(record.get("ExtraField1"))
        esic_override = _to_number_or_none(record.get("ExtraField2"))

        pf_auto = round(total_salary * 0.12)
        esic_auto = round(total_salary * 0.0075) if (monthly_total <= 21000) else 0

        pf_yearly = int(pf_override) if pf_override is not None else pf_auto
        esic_yearly = int(esic_override) if esic_override is not None else esic_auto

        pf_monthly = round(pf_yearly / 12.0)
        esic_monthly = round(esic_yearly / 12.0)

        total_deductions_yearly = pf_yearly + esic_yearly
        total_deductions_monthly = pf_monthly + esic_monthly

        inhand_yearly = round(total_salary - total_deductions_yearly)
        inhand_monthly = round(monthly_total - total_deductions_monthly)

        record.update({
            "BasicYearly": format_inr(basic),
            "HRAYearly": format_inr(hra),
            "ConveyanceYearly": format_inr(conveyance),
            "SpecialYearly": format_inr(special),
            "GrossYearly": format_inr(total_salary),
            "BasicMonthly": format_inr(round(basic / 12.0)),
            "HRAMonthly": format_inr(round(hra / 12.0)),
            "ConveyanceMonthly": format_inr(round(conveyance / 12.0)),
            "SpecialMonthly": format_inr(round(special / 12.0)),
            "GrossMonthly": format_inr(monthly_total),
            "PFYearly": format_inr(pf_yearly),
            "PFMonthly": format_inr(pf_monthly),
            "ESICYearly": format_inr(esic_yearly),
            "ESICMonthly": format_inr(esic_monthly),
            "DeductionsYearly": format_inr(total_deductions_yearly),
            "DeductionsMonthly": format_inr(total_deductions_monthly),
            "SubTotalYearly": format_inr(total_deductions_yearly),
            "SubTotalMonthly": format_inr(total_deductions_monthly),
            "InHandYearly": format_inr(inhand_yearly),
            "InHandMonthly": format_inr(inhand_monthly),
        })

        _attach_dmy(record)
        _attach_alias_variants(record)

        os.makedirs(config.OUTPUT_DIR, exist_ok=True)
        emp_part = _safe_filename_part(record.get("Emp_Name"), "Employee")
        cert_part = _safe_filename_part(record.get("CertificateType"), "Document")
        safe_name = f"{emp_part}_{cert_part}"
        unique_name = f"{safe_name}_{record_id}_{int(time.time() * 1000)}"
        output_docx = os.path.join(config.OUTPUT_DIR, f"{unique_name}.docx")

        template_path = _resolve_template_path(record.get("TemplateName"))
        doc = DocxTemplate(template_path)

        # Ensure XML-safe text for DOCX (handles & and friends)  <-- added
        record_escaped = _xml_escape_ctx(record)

        doc.render(record_escaped)
        doc.save(output_docx)

        from docx2pdf import convert
        convert(output_docx)
        pdf_path = output_docx.replace(".docx", ".pdf")

        return send_file(pdf_path, as_attachment=True, download_name=f"{safe_name}.pdf")
    except Exception as e:
        logging.exception("Error generating PDF for HR DocGen record %s", record_id)
        flash(f"Error generating PDF: {e}", "danger")
        return redirect(url_for("hr_docgen.show_records"))

@bp.route("/generate_docx/<int:record_id>", methods=["POST"])
@login_required(roles=["admin", "hr-admin", "data-entry"])
def generate_docx(record_id):
    try:
        cursor = safe_execute(
            conn,
            "SELECT * FROM dbo.HR_Certificates_Master WHERE ID=?",
            (record_id,),
            label="PRIMARY"
        )
        columns = [col[0] for col in cursor.description]
        row = cursor.fetchone()
        if not row:
            flash("Record not found!", "danger")
            return redirect(url_for("hr_docgen.show_records"))
        record = _trim_strings(dict(zip(columns, row)))

        if record.get("IsActive") == 0:
            flash("Cannot export Word for deactivated record!", "warning")
            return redirect(url_for("hr_docgen.show_records"))

        total_salary = _to_number_or_none(record.get("Salary")) or 0.0
        monthly_total = round(total_salary / 12)

        basic_pct = _pct(record.get("Basic_Percentage"))
        hra_pct   = _pct(record.get("HRA_Percentage"))
        conv_pct  = _pct(record.get("Conveyance_Percentage"))
        spec_pct  = _pct(record.get("Special_Percentage"))

        if basic_pct == 0 and hra_pct == 0 and conv_pct == 0 and spec_pct == 0:
            basic_pct, hra_pct, conv_pct, spec_pct = 55.0, 13.0, 10.0, 5.0

        basic      = round(total_salary * basic_pct / 100.0)
        hra        = round(total_salary * hra_pct / 100.0)
        conveyance = round(total_salary * conv_pct / 100.0)
        special    = round(total_salary * spec_pct / 100.0)

        pf_override   = _to_number_or_none(record.get("ExtraField1"))
        esic_override = _to_number_or_none(record.get("ExtraField2"))

        pf_auto   = round(total_salary * 0.12)
        esic_auto = round(total_salary * 0.0075) if (monthly_total <= 21000) else 0

        pf_yearly   = int(pf_override) if pf_override is not None else pf_auto
        esic_yearly = int(esic_override) if esic_override is not None else esic_auto

        pf_monthly   = round(pf_yearly / 12.0)
        esic_monthly = round(esic_yearly / 12.0)

        total_deductions_yearly  = pf_yearly + esic_yearly
        total_deductions_monthly = pf_monthly + esic_monthly

        inhand_yearly  = round(total_salary - total_deductions_yearly)
        inhand_monthly = round(monthly_total - total_deductions_monthly)

        record.update({
            "BasicYearly":        format_inr(basic),
            "HRAYearly":          format_inr(hra),
            "ConveyanceYearly":   format_inr(conveyance),
            "SpecialYearly":      format_inr(special),
            "GrossYearly":        format_inr(total_salary),
            "BasicMonthly":       format_inr(round(basic / 12.0)),
            "HRAMonthly":         format_inr(round(hra / 12.0)),
            "ConveyanceMonthly":  format_inr(round(conveyance / 12.0)),
            "SpecialMonthly":     format_inr(round(special / 12.0)),
            "GrossMonthly":       format_inr(monthly_total),
            "PFYearly":           format_inr(pf_yearly),
            "PFMonthly":          format_inr(pf_monthly),
            "ESICYearly":         format_inr(esic_yearly),
            "ESICMonthly":        format_inr(esic_monthly),
            "DeductionsYearly":   format_inr(total_deductions_yearly),
            "DeductionsMonthly":  format_inr(total_deductions_monthly),
            "SubTotalYearly":     format_inr(total_deductions_yearly),
            "SubTotalMonthly":    format_inr(total_deductions_monthly),
            "InHandYearly":       format_inr(inhand_yearly),
            "InHandMonthly":      format_inr(inhand_monthly),
        })

        _attach_dmy(record)
        _attach_alias_variants(record)

        os.makedirs(config.OUTPUT_DIR, exist_ok=True)
        emp_part = _safe_filename_part(record.get("Emp_Name"), "Employee")
        cert_part = _safe_filename_part(record.get("CertificateType"), "Document")
        safe_name = f"{emp_part}_{cert_part}"
        unique_name = f"{safe_name}_{record_id}_{int(time.time() * 1000)}"
        output_docx = os.path.join(config.OUTPUT_DIR, f"{unique_name}.docx")

        template_path = _resolve_template_path(record.get("TemplateName"))
        doc = DocxTemplate(template_path)

        # Ensure XML-safe text for DOCX (handles & and friends)  <-- added
        record_escaped = _xml_escape_ctx(record)

        doc.render(record_escaped)
        doc.save(output_docx)

        return send_file(output_docx, as_attachment=True, download_name=f"{safe_name}.docx")

    except Exception as e:
        logging.exception("Error generating DOCX for HR DocGen record %s", record_id)
        flash(f"Error generating Word file: {e}", "danger")
        return redirect(url_for("hr_docgen.show_records"))

@bp.route("/diag/dbcheck")
@login_required()
def diag_dbcheck():
    lines = []
    ok1, msg1 = ping_conn(conn, "PRIMARY")
    lines.append(msg1)
    lines.append("EMPLOYEE: using HR Systems POST/fetch source")
    all_ok = bool(ok1)
    return "<br>".join(lines), (200 if all_ok else 500)

# =================================================================
# ATTENDANCE EDITOR
# =================================================================

ATTENDANCE_CACHE = {}

def _attn_key():
    return session.get("username") or session.get("user") or "anon"

def _load_df_from_csv(file_storage):
    for enc in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            return pd.read_csv(file_storage, encoding=enc)
        except Exception:
            try:
                file_storage.seek(0)
            except Exception:
                pass
    try:
        file_storage.seek(0)
    except Exception:
        pass
    return pd.read_csv(file_storage)

@bp.route("/attendance", methods=["GET", "POST"])
@login_required(roles=["admin", "hr-admin", "data-entry"])
def attendance_upload():
    if request.method == "POST":
        f = request.files.get("file")
        if not f or f.filename == "":
            return render_template("hr_docgen/attendance_upload.html", error="Please choose a CSV file.")
        try:
            df = _load_df_from_csv(f)
            
            # NEW: Process always full attendance employees
            df = _process_always_full_attendance(df)
            
            ATTENDANCE_CACHE[_attn_key()] = df
            session["attn_filename"] = f.filename
            return redirect(url_for("hr_docgen.attendance_edit"))
        except Exception as e:
            return render_template("hr_docgen/attendance_upload.html", error=f"Upload failed: {e}")
    return render_template("hr_docgen/attendance_upload.html")

@bp.route("/attendance/edit")
@login_required(roles=["admin", "hr-admin", "data-entry"])
def attendance_edit():
    df = ATTENDANCE_CACHE.get(_attn_key())
    if df is None:
        return redirect(url_for("hr_docgen.attendance_upload"))

    df_view = _drop_calc_cols_from_df(df)

    columns = list(df_view.columns)
    records = df_view.fillna("").astype(str).to_dict(orient="records")
    full_salary_list = _get_always_full_employees()
    return render_template("hr_docgen/attendance_edit.html", columns=columns, records=records, full_salary_list=full_salary_list)

@bp.route("/attendance/save", methods=["POST"])
@login_required(roles=["admin", "hr-admin", "data-entry"])
def attendance_save():
    data = request.get_json(silent=True) or {}
    rows = data.get("rows", [])
    if not rows:
        return jsonify({"ok": False, "msg": "No data received."})
    try:
        df = pd.DataFrame(rows)
        
        # NEW: Process always full attendance before saving
        df = _process_always_full_attendance(df)
        
        ATTENDANCE_CACHE[_attn_key()] = df

        _ensure_attendance_tables(conn)

        headers = list(df.columns)
        period_start, period_end, dominant_label = _dominant_month_from_headers(headers)

        uploaded_by = session.get("username") or session.get("user")
        original_filename = session.get("attn_filename")

        # --- FIXED SEQUENCE & TRANSACTION ---
        prev_autocommit = conn.autocommit
        conn.autocommit = False
        try:
            cur = conn.cursor()

            # 1) Insert batch and capture BatchID in the same statement (reliable across drivers)
            cur.execute(
                """
                INSERT INTO dbo.Attendance_Batches
                    (UploadedBy, OriginalFilename, PeriodStart, PeriodEnd, DominantMonthLabel)
                OUTPUT INSERTED.BatchID
                VALUES (?, ?, ?, ?, ?)
                """,
                (uploaded_by, original_filename, period_start, period_end, dominant_label)
            )
            batch_id = cur.fetchone()[0]

            # 2) Bulk insert rows for this batch
            params = [(batch_id, json.dumps(r, ensure_ascii=False)) for r in rows]
            if params:
                cur.fast_executemany = True
                cur.executemany(
                    "INSERT INTO dbo.Attendance_Rows (BatchID, RowJSON) VALUES (?, ?)",
                    params
                )

            # 3) Commit the whole unit of work
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.autocommit = prev_autocommit

        return jsonify({
            "ok": True,
            "batch_id": batch_id,
            "dominant_month": dominant_label,
            "period_start": period_start.isoformat() if period_start else None,
            "period_end": period_end.isoformat() if period_end else None
        })
    except Exception as e:
        logging.error("Attendance save error: %r", e)
        return jsonify({"ok": False, "msg": str(e)}), 400

@bp.route("/attendance/download")
@login_required(roles=["admin", "hr-admin", "data-entry"])
def attendance_download():
    df = ATTENDANCE_CACHE.get(_attn_key())
    if df is None:
        return redirect(url_for("hr_docgen.attendance_upload"))
    buf = StringIO()
    df.to_csv(buf, index=False)
    buf.seek(0)
    return send_file(
        buf,
        mimetype="text/csv",
        as_attachment=True,
        download_name=f"attendance_edited_{datetime.now():%Y%m%d_%H%M%S}.csv"
    )

@bp.route("/attendance/history")
@login_required(roles=["admin", "hr-admin", "data-entry"])
def attendance_history():
    """Show all saved attendance batches"""
    try:
        cursor = safe_execute(
            conn,
            """
            SELECT 
                BatchID,
                UploadedOn,
                UploadedBy,
                OriginalFilename,
                PeriodStart,
                PeriodEnd,
                DominantMonthLabel,
                (SELECT COUNT(*) FROM dbo.Attendance_Rows WHERE BatchID = b.BatchID) AS TotalRows
            FROM dbo.Attendance_Batches b
            ORDER BY UploadedOn DESC
            """,
            label="PRIMARY"
        )
        rows = cursor.fetchall()
        columns = [col[0] for col in cursor.description]
        batches = [dict(zip(columns, row)) for row in rows]
        
        return render_template("hr_docgen/attendance_history.html", batches=batches)
    except Exception as e:
        logging.error("Error loading attendance history: %r", e)
        flash(f"Error loading history: {e}", "danger")
        return redirect(url_for("hr_docgen.dashboard"))

@bp.route("/attendance/load/<int:batch_id>")
@login_required(roles=["admin", "hr-admin", "data-entry"])
def attendance_load(batch_id):
    """Load a saved batch back into the editor"""
    try:
        cursor = safe_execute(
            conn,
            "SELECT DominantMonthLabel, OriginalFilename FROM dbo.Attendance_Batches WHERE BatchID=?",
            (batch_id,),
            label="PRIMARY"
        )
        batch_info = cursor.fetchone()
        if not batch_info:
            flash("Batch not found!", "danger")
            return redirect(url_for("hr_docgen.attendance_history"))
        
        cursor = safe_execute(
            conn,
            "SELECT RowJSON FROM dbo.Attendance_Rows WHERE BatchID=? ORDER BY RowID",
            (batch_id,),
            label="PRIMARY"
        )
        rows = cursor.fetchall()
        
        data = [json.loads(row[0]) for row in rows]
        df = pd.DataFrame(data)
        
        # Reapply "Always Full Attendance" rules in case the list changed after this batch was saved
        df = _process_always_full_attendance(df)
        
        ATTENDANCE_CACHE[_attn_key()] = df
        session["attn_filename"] = batch_info[1] or f"batch_{batch_id}.csv"
        
        batch_label = batch_info[0] or batch_info[1] or f"batch #{batch_id}"
        flash(f"Loaded {len(data)} rows from {batch_label}", "success")
        return redirect(url_for("hr_docgen.attendance_edit"))
        
    except Exception as e:
        logging.error("Error loading batch: %r", e)
        flash(f"Error loading batch: {e}", "danger")
        return redirect(url_for("hr_docgen.attendance_history"))

@bp.route("/attendance/download/<int:batch_id>")
@login_required(roles=["admin", "hr-admin", "data-entry"])
def attendance_download_batch(batch_id):
    """Download a saved batch as CSV"""
    try:
        cursor = safe_execute(
            conn,
            "SELECT DominantMonthLabel, OriginalFilename FROM dbo.Attendance_Batches WHERE BatchID=?",
            (batch_id,),
            label="PRIMARY"
        )
        batch_info = cursor.fetchone()
        if not batch_info:
            flash("Batch not found!", "danger")
            return redirect(url_for("hr_docgen.attendance_history"))
        
        cursor = safe_execute(
            conn,
            "SELECT RowJSON FROM dbo.Attendance_Rows WHERE BatchID=? ORDER BY RowID",
            (batch_id,),
            label="PRIMARY"
        )
        rows = cursor.fetchall()
        
        data = [json.loads(row[0]) for row in rows]
        df = pd.DataFrame(data)
        
        buf = StringIO()
        df.to_csv(buf, index=False)
        buf.seek(0)
        
        filename = f"attendance_{batch_info[0].replace(' ', '_')}_{batch_id}.csv"
        
        return send_file(
            buf,
            mimetype="text/csv",
            as_attachment=True,
            download_name=filename
        )
        
    except Exception as e:
        logging.error("Error downloading batch: %r", e)
        flash(f"Error downloading batch: {e}", "danger")
        return redirect(url_for("hr_docgen.attendance_history"))

@bp.route("/attendance/delete/<int:batch_id>", methods=["POST"])
@login_required(roles=["admin", "hr-admin"])
def attendance_delete(batch_id):
    """Delete a saved batch"""
    try:
        safe_execute(
            conn,
            "DELETE FROM dbo.Attendance_Rows WHERE BatchID=?",
            (batch_id,),
            label="PRIMARY"
        )
        
        safe_execute(
            conn,
            "DELETE FROM dbo.Attendance_Batches WHERE BatchID=?",
            (batch_id,),
            label="PRIMARY"
        )
        
        conn.commit()
        flash(f"Batch #{batch_id} deleted successfully", "success")
        
    except Exception as e:
        logging.error("Error deleting batch: %r", e)
        flash(f"Error deleting batch: {e}", "danger")
        
    return redirect(url_for("hr_docgen.attendance_history"))

# =====================================================
# NEW: Always Full Attendance Management Routes
# =====================================================
@bp.route("/attendance/always-full")
@login_required(roles=["admin", "hr-admin"])
def always_full_attendance_list():
    """Display the list of employees with always full attendance"""
    try:
        # Ensure connection is alive
        if conn is None:
            flash("Database connection not available", "danger")
            return redirect(url_for("hr_docgen.dashboard"))
        
        cursor = safe_execute(
            conn,
            """
            SELECT ID, EmployeeCode, AddedOn, AddedBy 
            FROM dbo.AlwaysFullAttendance 
            ORDER BY EmployeeCode
            """,
            label="PRIMARY"
        )
        rows = cursor.fetchall()
        columns = [col[0] for col in cursor.description]
        employees = [dict(zip(columns, row)) for row in rows]
        
        return render_template("hr_docgen/always_full_attendance.html", employees=employees)
    except Exception as e:
        logging.error("Error loading always full attendance list: %r", e)
        flash(f"Error loading list: {e}", "danger")
        return redirect(url_for("hr_docgen.dashboard"))

@bp.route("/attendance/always-full/add", methods=["POST"])
@login_required(roles=["admin", "hr-admin"])
def always_full_attendance_add():
    """Add an employee to the always full attendance list"""
    employee_code = request.form.get("employee_code", "").strip()
    
    if not employee_code:
        flash("Employee code cannot be empty", "danger")
        return redirect(url_for("hr_docgen.always_full_attendance_list"))
    
    added_by = session.get("username") or session.get("user")
    success, message = _add_always_full_employee(employee_code, added_by)
    
    if success:
        flash(f"{employee_code} added to always full attendance list", "success")
    else:
        flash(str(message), "danger")
    
    return redirect(url_for("hr_docgen.always_full_attendance_list"))

@bp.route("/attendance/always-full/remove/<int:emp_id>", methods=["POST"])
@login_required(roles=["admin", "hr-admin"])
def always_full_attendance_remove(emp_id):
    """Remove an employee from the always full attendance list"""
    try:
        # Ensure connection is alive
        if conn is None:
            flash("Database connection not available", "danger")
            return redirect(url_for("hr_docgen.always_full_attendance_list"))
        
        # Get employee code first
        cursor = safe_execute(
            conn,
            "SELECT EmployeeCode FROM dbo.AlwaysFullAttendance WHERE ID=?",
            (emp_id,),
            label="PRIMARY"
        )
        row = cursor.fetchone()
        
        if not row:
            flash("Employee not found in the list", "danger")
            return redirect(url_for("hr_docgen.always_full_attendance_list"))
        
        employee_code = row[0]
        
        # Remove the employee
        success, message = _remove_always_full_employee(employee_code)
        
        if success:
            flash(f"{employee_code} removed from always full attendance list", "success")
        else:
            flash(str(message), "danger")
            
    except Exception as e:
        logging.error("Error removing employee: %r", e)
        flash(f"Error removing employee: {e}", "danger")
    
    return redirect(url_for("hr_docgen.always_full_attendance_list"))

# =====================================================
# DEBUG: Database Connection Status Route
# =====================================================
@bp.route("/debug/db-status")
@login_required(roles=["admin"])
def debug_db_status():
    """Debug route to check database connections"""
    status = {
        "primary_db": {},
        "employee_source": {
            "connected": True,
            "message": "Using HR Systems POST/fetch source",
            "url_configured": bool(getattr(hid_config, "EMP_LOOKUP_WEBHOOK_URL", "")),
        },
    }
    
    # Check primary connection
    if conn:
        ok, msg = ping_conn(conn, "PRIMARY")
        status["primary_db"] = {
            "connected": ok,
            "message": msg,
            "connection_string": conn_str.replace(config.SQL_PASSWORD, "***") if not config.SQL_TRUSTED else conn_str
        }
    else:
        status["primary_db"]["connected"] = False
        status["primary_db"]["message"] = "Connection object is None"
    
    return jsonify(status)

