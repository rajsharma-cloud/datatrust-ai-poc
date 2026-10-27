"""
Portal store: who can log in, which sources are connected, access requests and the portal's own audit log.
Everything lives in schema `portal` of the POC database (config.py / .env PG_*). Created on first start.

Passwords of connected sources are NEVER stored here: a connection keeps the NAME of a .env variable
(e.g. CONN_SALES_PASSWORD) and the password is read from .env when the portal connects.
"""
import base64
import hashlib
import hmac
import json
import os
import secrets
import sys
import time
from pathlib import Path

import psycopg
from psycopg.rows import dict_row

ROOT = next(f for f in Path(__file__).resolve().parents if (f / "config.py").exists())
sys.path.insert(0, str(ROOT))
import config  # noqa: E402

SESSION_HOURS = 8


def db():
    """Connection to the portal's own database (the POC database)."""
    return psycopg.connect(host=config.PG_HOST, port=config.PG_PORT, dbname=config.PG_DATABASE,
                           user=config.PG_USER, password=config.PG_PASSWORD, row_factory=dict_row, connect_timeout=5)


def init() -> None:
    """Create the portal tables, the first admin and the default connection (safe to run every start)."""
    with db() as conn:
        conn.execute("""
            CREATE SCHEMA IF NOT EXISTS portal;
            CREATE TABLE IF NOT EXISTS portal.settings (key TEXT PRIMARY KEY, value TEXT);
            CREATE TABLE IF NOT EXISTS portal.users (
                username      TEXT PRIMARY KEY,
                password_hash TEXT NOT NULL,
                role          TEXT NOT NULL CHECK (role IN ('admin', 'viewer')),
                active        BOOLEAN DEFAULT TRUE,
                created_by    TEXT,
                created_at    TIMESTAMP DEFAULT now());
            CREATE TABLE IF NOT EXISTS portal.connections (
                connection_id SERIAL PRIMARY KEY,
                name          TEXT UNIQUE NOT NULL,
                source_type   TEXT NOT NULL DEFAULT 'postgresql',
                host          TEXT NOT NULL,
                port          INT  NOT NULL,
                database      TEXT NOT NULL,
                username      TEXT NOT NULL,
                password_env  TEXT NOT NULL,          -- name of the .env variable holding the password
                created_by    TEXT,
                created_at    TIMESTAMP DEFAULT now());
            ALTER TABLE portal.connections ADD COLUMN IF NOT EXISTS settings JSONB DEFAULT '{}'::jsonb;
            CREATE TABLE IF NOT EXISTS portal.access_requests (
                request_id    SERIAL PRIMARY KEY,
                connection_id INT REFERENCES portal.connections(connection_id),
                database      TEXT,
                request_type  TEXT,                   -- CREATE_USER / GRANT / REVOKE / GRANT_ROLE
                grantee       TEXT,
                details       JSONB,                  -- privileges, object type, object name ...
                sql_preview   TEXT,                   -- the statements that will run (passwords masked)
                reason        TEXT,
                status        TEXT DEFAULT 'PENDING', -- PENDING / APPLIED / REJECTED / FAILED
                requested_by  TEXT,
                requested_at  TIMESTAMP DEFAULT now(),
                decided_by    TEXT,
                decided_at    TIMESTAMP,
                error         TEXT);
            CREATE TABLE IF NOT EXISTS portal.audit_log (
                audit_id   BIGSERIAL PRIMARY KEY,
                at         TIMESTAMP DEFAULT now(),
                username   TEXT,
                action     TEXT,
                target     TEXT,
                detail     TEXT);
        """)
        if not conn.execute("SELECT 1 FROM portal.users LIMIT 1").fetchone():
            user = os.getenv("PORTAL_ADMIN_USER", "admin")
            password = os.getenv("PORTAL_ADMIN_PASSWORD") or secrets.token_urlsafe(9)
            conn.execute("INSERT INTO portal.users (username, password_hash, role, created_by) VALUES (%s, %s, 'admin', 'setup')",
                         (user, hash_password(password)))
            if not os.getenv("PORTAL_ADMIN_PASSWORD"):
                print("\n" + "=" * 70 + f"\n  Data Estate portal: first admin created\n  user: {user}   password: {password}\n"
                      "  (shown once; change it on the Access page, or set PORTAL_ADMIN_PASSWORD in .env)\n" + "=" * 70 + "\n")
        if not conn.execute("SELECT 1 FROM portal.connections LIMIT 1").fetchone():
            conn.execute("""INSERT INTO portal.connections (name, host, port, database, username, password_env, created_by)
                            VALUES ('DataTrust POC (local)', %s, %s, %s, %s, 'PG_PASSWORD', 'setup')""",
                         (config.PG_HOST, config.PG_PORT, config.PG_DATABASE, config.PG_USER))
        conn.commit()


# ============================================================================ passwords and sessions
def hash_password(password: str) -> str:
    salt = secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, 200_000)
    return "pbkdf2$" + base64.b64encode(salt).decode() + "$" + base64.b64encode(digest).decode()


def check_password(password: str, stored: str) -> bool:
    try:
        _, salt, digest = stored.split("$")
        test = hashlib.pbkdf2_hmac("sha256", password.encode(), base64.b64decode(salt), 200_000)
        return hmac.compare_digest(test, base64.b64decode(digest))
    except Exception:
        return False


def _secret() -> bytes:
    """Signing key for session cookies: .env PORTAL_SECRET, else one generated once and kept in portal.settings."""
    if os.getenv("PORTAL_SECRET"):
        return os.getenv("PORTAL_SECRET").encode()
    with db() as conn:
        row = conn.execute("SELECT value FROM portal.settings WHERE key = 'session_secret'").fetchone()
        if row:
            return row["value"].encode()
        value = secrets.token_hex(32)
        conn.execute("INSERT INTO portal.settings (key, value) VALUES ('session_secret', %s) ON CONFLICT DO NOTHING", (value,))
        conn.commit()
        return conn.execute("SELECT value FROM portal.settings WHERE key = 'session_secret'").fetchone()["value"].encode()


def make_session(username: str, role: str) -> str:
    payload = base64.urlsafe_b64encode(json.dumps({"u": username, "r": role,
                                                   "exp": int(time.time()) + SESSION_HOURS * 3600}).encode()).decode()
    sig = hmac.new(_secret(), payload.encode(), hashlib.sha256).hexdigest()
    return f"{payload}.{sig}"


def read_session(token: str):
    """-> {'username', 'role'} or None. The user must still exist and be active."""
    try:
        payload, sig = token.split(".")
        if not hmac.compare_digest(sig, hmac.new(_secret(), payload.encode(), hashlib.sha256).hexdigest()):
            return None
        data = json.loads(base64.urlsafe_b64decode(payload))
        if data["exp"] < time.time():
            return None
        with db() as conn:
            row = conn.execute("SELECT username, role FROM portal.users WHERE username = %s AND active",
                               (data["u"],)).fetchone()
        return row
    except Exception:
        return None


def login(username: str, password: str):
    with db() as conn:
        row = conn.execute("SELECT username, role, password_hash FROM portal.users WHERE username = %s AND active",
                           (username,)).fetchone()
    if not row or not check_password(password, row["password_hash"]):
        audit(username, "LOGIN_FAILED", "portal", "wrong user or password")
        return None
    audit(username, "LOGIN", "portal", row["role"])
    return {"username": row["username"], "role": row["role"]}


# ============================================================================ portal users
def list_users():
    with db() as conn:
        return conn.execute("SELECT username, role, active, created_by, created_at FROM portal.users ORDER BY username").fetchall()


def save_user(username: str, role: str, password: str, by: str, active: bool = True):
    if role not in ("admin", "viewer"):
        raise ValueError("role must be admin or viewer")
    if not username.strip() or len(password) < 8:
        raise ValueError("username required and password of at least 8 characters")
    with db() as conn:
        conn.execute("""INSERT INTO portal.users (username, password_hash, role, active, created_by) VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (username) DO UPDATE SET password_hash = EXCLUDED.password_hash,
                               role = EXCLUDED.role, active = EXCLUDED.active""",
                     (username.strip(), hash_password(password), role, active, by))
        conn.commit()
    audit(by, "PORTAL_USER_SAVED", username, role)


# ============================================================================ connections
def list_connections():
    with db() as conn:
        return conn.execute("""SELECT connection_id, name, source_type, host, port, database, username, password_env,
                                      settings, created_by, created_at FROM portal.connections ORDER BY connection_id""").fetchall()


def get_connection(connection_id: int) -> dict:
    with db() as conn:
        row = conn.execute("SELECT * FROM portal.connections WHERE connection_id = %s", (connection_id,)).fetchone()
    if not row:
        raise ValueError(f"connection {connection_id} not found")
    return row


def add_connection(name, host, port, database, username, password_env, by, source_type="postgresql", settings=None):
    from .source_types import DEFAULT_PORT, SOURCE_TYPES
    if source_type not in SOURCE_TYPES:
        raise ValueError(f"unknown source type {source_type}")
    if not name.strip() or not host.strip() or not username.strip():
        raise ValueError("name, host/account and login user are required")
    if not password_env or not password_env.replace("_", "").isalnum():
        raise ValueError("password_env must be the NAME of a variable in .env, e.g. CONN_SALES_PASSWORD")
    with db() as conn:
        row = conn.execute("""INSERT INTO portal.connections (name, source_type, host, port, database, username, password_env,
                                     settings, created_by)
                              VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING connection_id""",
                           (name.strip(), source_type, host.strip(), int(port or DEFAULT_PORT[source_type]), (database or "").strip(),
                            username.strip(), password_env, json.dumps(settings or {}), by)).fetchone()
        conn.commit()
    audit(by, "CONNECTION_ADDED", name, f"{source_type} {username}@{host}:{port}/{database}")
    return row["connection_id"]


def delete_connection(connection_id: int, by: str):
    with db() as conn:
        conn.execute("DELETE FROM portal.access_requests WHERE connection_id = %s AND status = 'PENDING'", (connection_id,))
        conn.execute("UPDATE portal.access_requests SET connection_id = NULL WHERE connection_id = %s", (connection_id,))
        conn.execute("DELETE FROM portal.connections WHERE connection_id = %s", (connection_id,))
        conn.commit()
    audit(by, "CONNECTION_REMOVED", str(connection_id), "")


def resolve_password(password_env: str) -> str:
    """Read the password from .env every time (a newly added variable works without a restart)."""
    from dotenv import dotenv_values
    value = dotenv_values(ROOT / ".env").get(password_env) or os.getenv(password_env)
    if value is None and password_env == "PG_PASSWORD":
        value = config.PG_PASSWORD                     # the POC's own database (default from config.py)
    if value is None:
        raise ValueError(f"{password_env} is not set (add it to .env, or as an environment variable / secret in the cloud)")
    return value


# ============================================================================ audit
def audit(username, action, target, detail=""):
    try:
        with db() as conn:
            conn.execute("INSERT INTO portal.audit_log (username, action, target, detail) VALUES (%s, %s, %s, %s)",
                         (username, action, target, str(detail)[:2000]))
            conn.commit()
    except Exception as e:
        print(f"!! portal audit failed: {e}")


def audit_log(limit: int = 200):
    with db() as conn:
        return conn.execute("SELECT at, username, action, target, detail FROM portal.audit_log ORDER BY audit_id DESC LIMIT %s",
                            (limit,)).fetchall()
