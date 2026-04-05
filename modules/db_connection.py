import os
import pyodbc
import sqlite3
import config

# Optional SQLAlchemy-based pooling (preferred for production); falls back to direct pyodbc.
try:
    from sqlalchemy import create_engine
    from sqlalchemy.engine import URL
except Exception:
    create_engine = None
    URL = None

# Build a small, conservative pool per unit.
ENGINE_CACHE = {}


def _env_int(name: str, default: int, *, minimum: int | None = None) -> int:
    raw = os.getenv(name)
    try:
        value = int(str(raw).strip()) if raw is not None else int(default)
    except Exception:
        value = int(default)
    if minimum is not None:
        value = max(int(minimum), value)
    return value


DB_POOL_SIZE = _env_int("DB_POOL_SIZE", 5, minimum=1)
DB_POOL_MAX_OVERFLOW = _env_int("DB_POOL_MAX_OVERFLOW", 2, minimum=0)
DB_POOL_TIMEOUT = _env_int("DB_POOL_TIMEOUT", 30, minimum=1)
DB_POOL_RECYCLE = _env_int("DB_POOL_RECYCLE", 300, minimum=30)


def _brace_driver(name: str) -> str:
    n = (name or "ODBC Driver 17 for SQL Server").strip()
    while n.startswith("{") and n.endswith("}") and len(n) > 2:
        n = n[1:-1].strip()
    return "{" + n + "}"


def _clean_driver(name: str) -> str:
    """Return driver name without surrounding braces (for SQLAlchemy URL)."""
    n = (name or "ODBC Driver 17 for SQL Server").strip()
    while n.startswith("{") and n.endswith("}") and len(n) > 2:
        n = n[1:-1].strip()
    return n


def _reset_session_state(conn) -> bool:
    """Normalize session settings before handing out a connection."""
    try:
        cur = conn.cursor()
        cur.execute(
            """
            IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
            SET IMPLICIT_TRANSACTIONS OFF;
            SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
            """
        )
        cur.close()
        return True
    except Exception:
        return False


def _direct_connect(db: dict):
    conn_str = (
        f"DRIVER={_brace_driver(db.get('DRIVER') or 'SQL Server')};"
        f"SERVER={db['SERVER']};"
        f"DATABASE={db.get('DB') or db.get('DATABASE')};"
        f"UID={db.get('USER') or db.get('UID')};"
        f"PWD={db['PWD']};"
        "TransactionIsolation=ReadCommitted;"
        "TrustServerCertificate=yes;"
    )
    timeout = int(db.get("TIMEOUT", 5))
    conn = pyodbc.connect(conn_str, timeout=timeout, autocommit=True)
    if not _reset_session_state(conn):
        try:
            conn.close()
        except Exception:
            pass
        raise RuntimeError("Failed to reset SQL session state after direct connect")
    return conn


def _make_engine(cfg: dict):
    if not create_engine or not URL:
        return None
    try:
        url = URL.create(
            "mssql+pyodbc",
            username=cfg.get("USER") or cfg.get("UID"),
            password=cfg.get("PWD"),
            host=cfg.get("SERVER"),
            database=cfg.get("DB") or cfg.get("DATABASE"),
            query={
                # SQLAlchemy expects driver name without braces
                "driver": _clean_driver(cfg.get("DRIVER") or "ODBC Driver 17 for SQL Server"),
                "TrustServerCertificate": "yes",
            },
        )
        timeout = int(cfg.get("TIMEOUT", 5))
        return create_engine(
            url,
            pool_size=DB_POOL_SIZE,
            max_overflow=DB_POOL_MAX_OVERFLOW,
            pool_timeout=DB_POOL_TIMEOUT,
            pool_recycle=DB_POOL_RECYCLE,
            pool_pre_ping=True,
            connect_args={"timeout": timeout},
        )
    except Exception:
        return None


for _unit, _cfg in (getattr(config, "DB_CONFIGS", {}) or {}).items():
    if isinstance(_cfg, dict):
        _engine = _make_engine(_cfg)
        if _engine:
            ENGINE_CACHE[_unit] = _engine


def get_sql_connection(unit):
    try:
        db = config.DB_CONFIGS[unit]
        eng = ENGINE_CACHE.get(unit)
        if eng:
            # Return a pooled DBAPI connection (pyodbc) for compatibility with existing code.
            conn = eng.raw_connection()
            try:
                conn.autocommit = True
            except Exception:
                pass
            if _reset_session_state(conn):
                return conn
            try:
                conn.close()
            except Exception:
                pass
            try:
                eng.dispose()
            except Exception:
                pass
            # Fallback to a fresh direct connection if pooled session reset fails.
            return _direct_connect(db)

        return _direct_connect(db)
    except Exception as e:
        print(f"�?O Connection failed for {unit}: {e}")
        return None


def get_local_connection():
    return sqlite3.connect(config.LOCAL_DB)
