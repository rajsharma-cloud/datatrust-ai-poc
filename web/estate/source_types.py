"""
Source types the portal knows: the fields the "Add a source" wizard asks for, and a connection test per type.

Browsing (catalog, changes, access) needs a full connector (service.CONNECTORS). Today: PostgreSQL.
Snowflake, SQL Server and SAP HANA can already be added and tested; their connectors come next.
The Python driver for each type is imported only when that type is used, so nothing extra is needed until then.
"""
from . import store

SOURCE_TYPES = {
    "postgresql": {
        "label": "PostgreSQL", "browse": True, "driver": "psycopg (already installed)",
        "fields": [
            {"key": "host", "label": "Host", "placeholder": "e.g. 10.20.30.45 or localhost", "required": True},
            {"key": "port", "label": "Port", "default": "5432", "required": True},
            {"key": "database", "label": "Database", "placeholder": "e.g. sales_dw", "required": True},
            {"key": "username", "label": "Login user", "required": True},
        ],
    },
    "snowflake": {
        "label": "Snowflake", "browse": False, "driver": "pip install snowflake-connector-python",
        "fields": [
            {"key": "host", "label": "Account identifier", "placeholder": "e.g. myorg-myaccount (from your Snowflake URL)", "required": True},
            {"key": "username", "label": "Login user", "required": True},
            {"key": "warehouse", "label": "Warehouse", "placeholder": "e.g. COMPUTE_WH", "required": True},
            {"key": "role", "label": "Role", "placeholder": "e.g. ACCOUNTADMIN or a read role", "required": False},
            {"key": "database", "label": "Default database", "placeholder": "optional", "required": False},
        ],
    },
    "sqlserver": {
        "label": "SQL Server", "browse": False, "driver": "pip install pyodbc + Microsoft ODBC Driver 18 for SQL Server",
        "fields": [
            {"key": "host", "label": "Server", "placeholder": "e.g. sqlprod01 or myserver.database.windows.net", "required": True},
            {"key": "port", "label": "Port", "default": "1433", "required": True},
            {"key": "database", "label": "Database", "placeholder": "e.g. Finance", "required": True},
            {"key": "username", "label": "SQL login user", "required": True},
            {"key": "odbc_driver", "label": "ODBC driver", "default": "ODBC Driver 18 for SQL Server", "required": True},
            {"key": "trust_cert", "label": "Trust server certificate (yes/no)", "default": "yes", "required": False},
        ],
    },
    "saphana": {
        "label": "SAP HANA", "browse": False, "driver": "pip install hdbcli",
        "fields": [
            {"key": "host", "label": "Host", "placeholder": "e.g. hana01.company.local", "required": True},
            {"key": "port", "label": "SQL port", "default": "30015", "placeholder": "3<instance>15, or 443 for HANA Cloud", "required": True},
            {"key": "database", "label": "Tenant database", "placeholder": "optional, e.g. HXE", "required": False},
            {"key": "username", "label": "Login user", "required": True},
            {"key": "encrypt", "label": "Encrypted connection (yes/no)", "default": "yes", "required": False},
        ],
    },
}
DEFAULT_PORT = {"postgresql": 5432, "snowflake": 443, "sqlserver": 1433, "saphana": 30015}


def test_connection(cfg: dict) -> dict:
    """Connect once and read the version. cfg = connection row (+ settings). Raises with a readable message."""
    t = cfg["source_type"]
    password = store.resolve_password(cfg["password_env"])
    s = cfg.get("settings") or {}
    if t == "postgresql":
        from .postgres_connector import PostgresSource
        return PostgresSource(cfg).test()
    if t == "snowflake":
        try:
            import snowflake.connector
        except ImportError:
            raise ValueError("Snowflake driver not installed: pip install snowflake-connector-python")
        con = snowflake.connector.connect(account=cfg["host"], user=cfg["username"], password=password,
                                          warehouse=s.get("warehouse") or None, role=s.get("role") or None,
                                          database=cfg.get("database") or None, login_timeout=15)
        try:
            row = con.cursor().execute("SELECT CURRENT_VERSION(), CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE()").fetchone()
        finally:
            con.close()
        return {"version": f"Snowflake {row[0]}", "login_user": row[1], "database": cfg.get("database") or "",
                "role": row[2], "warehouse": row[3]}
    if t == "sqlserver":
        try:
            import pyodbc
        except ImportError:
            raise ValueError("SQL Server driver not installed: pip install pyodbc (and Microsoft ODBC Driver 18 for SQL Server)")
        trust = "yes" if str(s.get("trust_cert", "yes")).lower().startswith("y") else "no"
        con = pyodbc.connect(f"DRIVER={{{s.get('odbc_driver') or 'ODBC Driver 18 for SQL Server'}}};"
                             f"SERVER={cfg['host']},{cfg['port']};DATABASE={cfg['database']};UID={cfg['username']};"
                             f"PWD={{{password}}};Encrypt=yes;TrustServerCertificate={trust}", timeout=15)
        try:
            row = con.cursor().execute("SELECT @@VERSION, SUSER_SNAME(), DB_NAME()").fetchone()
        finally:
            con.close()
        return {"version": row[0].split("\n")[0], "login_user": row[1], "database": row[2]}
    if t == "saphana":
        try:
            from hdbcli import dbapi
        except ImportError:
            raise ValueError("SAP HANA driver not installed: pip install hdbcli")
        kw = {"address": cfg["host"], "port": int(cfg["port"]), "user": cfg["username"], "password": password,
              "encrypt": str(s.get("encrypt", "yes")).lower().startswith("y")}
        if cfg.get("database"):
            kw["databaseName"] = cfg["database"]
        con = dbapi.connect(**kw)
        try:
            cur = con.cursor()
            cur.execute("SELECT VERSION, CURRENT_USER, DATABASE_NAME FROM M_DATABASE")
            row = cur.fetchone()
        finally:
            con.close()
        return {"version": f"SAP HANA {row[0]}", "login_user": row[1], "database": row[2]}
    raise ValueError(f"unknown source type {t}")
