"""
PostgreSQL connector for the Data Estate portal.

Reads (catalog, grants, roles, changes) run in READ ONLY sessions with a 10 s timeout.
Writes happen only in two places, both started by an admin from the portal:
  * enable_change_tracking()  installs schema dt_audit + two event triggers in ONE database
  * apply_statements()        runs the statements of an APPROVED access request in one transaction
All SQL that contains user input is built with psycopg.sql (identifiers quoted), never by string pasting.
"""
import secrets
import string
from datetime import date, datetime
from decimal import Decimal

import psycopg
from psycopg import sql
from psycopg.rows import dict_row

from . import store

SYSTEM_SCHEMAS = ("pg_catalog", "information_schema", "pg_toast")

PRIVILEGES = {                      # what can be granted on each object type
    "DATABASE": ["CONNECT", "CREATE", "TEMPORARY", "ALL"],
    "SCHEMA": ["USAGE", "CREATE", "ALL"],
    "ALL_TABLES_IN_SCHEMA": ["SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "ALL"],
    "TABLE": ["SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER", "ALL"],
    "VIEW": ["SELECT", "ALL"],
    "FUNCTION": ["EXECUTE", "ALL"],
    "PROCEDURE": ["EXECUTE", "ALL"],
}


def plain(v):
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, (date, datetime)):
        return v.isoformat(sep=" ") if isinstance(v, datetime) else v.isoformat()
    if isinstance(v, memoryview):
        return None
    return v


def clean(rows):
    return [{k: plain(v) for k, v in r.items()} for r in rows]


class PostgresSource:
    def __init__(self, conn_cfg: dict):
        self.cfg = conn_cfg

    # ------------------------------------------------------------------ connections
    def connect(self, database: str = None, read_only: bool = True):
        c = psycopg.connect(host=self.cfg["host"], port=self.cfg["port"], dbname=database or self.cfg["database"],
                            user=self.cfg["username"], password=store.resolve_password(self.cfg["password_env"]),
                            row_factory=dict_row, connect_timeout=5, autocommit=read_only,
                            application_name="datatrust-estate-portal")
        if read_only:
            c.execute("SET default_transaction_read_only = on")
            c.execute("SET statement_timeout = '10s'")
        return c

    def q(self, query, params=None, database=None):
        with self.connect(database) as c:
            return clean(c.execute(query, params).fetchall())

    def test(self) -> dict:
        r = self.q("SELECT version() AS version, current_user AS login_user, current_database() AS database")[0]
        r["version"] = r["version"].split(",")[0]
        return r

    # ------------------------------------------------------------------ catalog
    def databases(self):
        return self.q("""SELECT d.datname AS name, pg_get_userbyid(d.datdba) AS owner,
                                pg_database_size(d.datname) AS size_bytes,
                                has_database_privilege(d.datname, 'CONNECT') AS can_connect
                         FROM pg_database d WHERE NOT d.datistemplate ORDER BY 1""")

    def schemas(self, database):
        return self.q("""SELECT n.nspname AS name, pg_get_userbyid(n.nspowner) AS owner,
                                (SELECT COUNT(*) FROM pg_class c WHERE c.relnamespace = n.oid AND c.relkind IN ('r','p')) AS tables,
                                (SELECT COUNT(*) FROM pg_class c WHERE c.relnamespace = n.oid AND c.relkind IN ('v','m')) AS views,
                                (SELECT COUNT(*) FROM pg_proc p WHERE p.pronamespace = n.oid AND p.prokind IN ('f','p')) AS routines
                         FROM pg_namespace n
                         WHERE n.nspname NOT LIKE 'pg\\_%%' AND n.nspname <> 'information_schema'
                         ORDER BY 1""", database=database)

    def objects(self, database, schema):
        return self.q("""
            SELECT c.relname AS name,
                   CASE c.relkind WHEN 'r' THEN 'TABLE' WHEN 'p' THEN 'TABLE' WHEN 'v' THEN 'VIEW'
                        WHEN 'm' THEN 'MATERIALIZED VIEW' WHEN 'S' THEN 'SEQUENCE' WHEN 'f' THEN 'FOREIGN TABLE' END AS type,
                   pg_get_userbyid(c.relowner) AS owner,
                   CASE WHEN c.relkind IN ('r','p','m') THEN GREATEST(c.reltuples, 0)::bigint END AS est_rows,
                   CASE WHEN c.relkind IN ('r','p','m') THEN pg_total_relation_size(c.oid) END AS size_bytes,
                   (SELECT COUNT(*) FROM pg_index i WHERE i.indrelid = c.oid) AS indexes,
                   obj_description(c.oid, 'pg_class') AS comment
            FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %(s)s AND c.relkind IN ('r','p','v','m','S','f')
            UNION ALL
            SELECT p.proname || '(' || pg_get_function_identity_arguments(p.oid) || ')',
                   CASE p.prokind WHEN 'p' THEN 'PROCEDURE' ELSE 'FUNCTION' END,
                   pg_get_userbyid(p.proowner), NULL, NULL, NULL, obj_description(p.oid, 'pg_proc')
            FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
            WHERE n.nspname = %(s)s AND p.prokind IN ('f','p')
              AND NOT EXISTS (SELECT 1 FROM pg_depend d WHERE d.objid = p.oid AND d.deptype = 'e')
            ORDER BY 2, 1""", {"s": schema}, database)

    def object_detail(self, database, schema, name, obj_type):
        """Columns, indexes, constraints, definition, grants, lineage and recent changes of one object."""
        full = f"{schema}.{name}"
        with self.connect(database) as c:
            if obj_type in ("FUNCTION", "PROCEDURE"):
                ident = sql.SQL("{}.").format(sql.Identifier(schema)).as_string(c) + _quote_routine(c, name)
                oid = c.execute("SELECT to_regprocedure(%s)::oid AS oid", (ident,)).fetchone()["oid"]
                if not oid:
                    raise ValueError(f"{full} not found")
                info = c.execute("""SELECT pg_get_userbyid(proowner) AS owner, pg_get_functiondef(oid) AS definition,
                                           pg_get_function_result(oid) AS returns, l.lanname AS language
                                    FROM pg_proc p JOIN pg_language l ON l.oid = p.prolang WHERE p.oid = %s""", (oid,)).fetchone()
                grants = c.execute("""SELECT CASE WHEN a.grantee = 0 THEN 'PUBLIC' ELSE pg_get_userbyid(a.grantee) END AS grantee,
                                             a.privilege_type, pg_get_userbyid(a.grantor) AS grantor
                                      FROM pg_proc p, aclexplode(COALESCE(p.proacl, acldefault('f', p.proowner))) a
                                      WHERE p.oid = %s ORDER BY 1, 2""", (oid,)).fetchall()
                result = {"type": obj_type, "name": full, **info, "columns": [], "indexes": [], "constraints": [],
                          "grants": grants, "used_by": [], "uses": []}
            else:
                regclass = sql.SQL("{}.{}").format(sql.Identifier(schema), sql.Identifier(name)).as_string(c)
                oid = c.execute("SELECT to_regclass(%s)::oid AS oid", (regclass,)).fetchone()["oid"]
                if not oid:
                    raise ValueError(f"{full} not found")
                info = c.execute("""SELECT pg_get_userbyid(relowner) AS owner, relkind,
                                           obj_description(oid, 'pg_class') AS comment,
                                           CASE WHEN relkind IN ('r','p','m') THEN pg_total_relation_size(oid) END AS size_bytes,
                                           CASE WHEN relkind IN ('v','m') THEN pg_get_viewdef(oid, true) END AS definition
                                    FROM pg_class WHERE oid = %s""", (oid,)).fetchone()
                columns = c.execute("""SELECT a.attname AS column, format_type(a.atttypid, a.atttypmod) AS data_type,
                                              NOT a.attnotnull AS nullable, pg_get_expr(d.adbin, d.adrelid) AS default_value,
                                              col_description(a.attrelid, a.attnum) AS comment
                                       FROM pg_attribute a LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
                                       WHERE a.attrelid = %s AND a.attnum > 0 AND NOT a.attisdropped
                                       ORDER BY a.attnum""", (oid,)).fetchall()
                indexes = c.execute("""SELECT i.relname AS index, pg_get_indexdef(x.indexrelid) AS definition,
                                              x.indisunique AS is_unique, x.indisprimary AS is_primary
                                       FROM pg_index x JOIN pg_class i ON i.oid = x.indexrelid
                                       WHERE x.indrelid = %s ORDER BY 1""", (oid,)).fetchall()
                constraints = c.execute("""SELECT conname AS constraint,
                                                  CASE contype WHEN 'p' THEN 'PRIMARY KEY' WHEN 'f' THEN 'FOREIGN KEY'
                                                       WHEN 'u' THEN 'UNIQUE' WHEN 'c' THEN 'CHECK' ELSE contype::text END AS type,
                                                  pg_get_constraintdef(oid) AS definition
                                           FROM pg_constraint WHERE conrelid = %s ORDER BY 1""", (oid,)).fetchall()
                grants = c.execute("""SELECT CASE WHEN a.grantee = 0 THEN 'PUBLIC' ELSE pg_get_userbyid(a.grantee) END AS grantee,
                                             a.privilege_type, pg_get_userbyid(a.grantor) AS grantor
                                      FROM pg_class r, aclexplode(COALESCE(r.relacl, acldefault('r', r.relowner))) a
                                      WHERE r.oid = %s ORDER BY 1, 2""", (oid,)).fetchall()
                used_by = c.execute("""SELECT DISTINCT view_schema || '.' || view_name AS object FROM information_schema.view_table_usage
                                       WHERE table_schema = %s AND table_name = %s ORDER BY 1""", (schema, name)).fetchall()
                uses = c.execute("""SELECT DISTINCT table_schema || '.' || table_name AS object FROM information_schema.view_table_usage
                                    WHERE view_schema = %s AND view_name = %s ORDER BY 1""", (schema, name)).fetchall()
                row_count = None
                if info["relkind"] in ("r", "p", "m"):
                    try:
                        row_count = c.execute(sql.SQL("SELECT COUNT(*) AS n FROM {}.{}").format(
                            sql.Identifier(schema), sql.Identifier(name))).fetchone()["n"]
                    except Exception:
                        row_count = None              # no SELECT right, or too big for the 10 s timeout
                result = {"type": obj_type, "name": full, **info, "row_count": row_count, "columns": columns,
                          "indexes": indexes, "constraints": constraints, "grants": grants,
                          "used_by": [u["object"] for u in used_by], "uses": [u["object"] for u in uses]}
            result.pop("relkind", None)
        result = {k: (clean(v) if isinstance(v, list) and v and isinstance(v[0], dict) else plain(v)) for k, v in result.items()}
        result["changes"] = self.changes(database, object_name=full, limit=20) if self.tracking_enabled(database) else []
        return result

    def search(self, database, text):
        return self.q("""SELECT n.nspname AS schema, c.relname AS name,
                                CASE c.relkind WHEN 'v' THEN 'VIEW' WHEN 'm' THEN 'MATERIALIZED VIEW' ELSE 'TABLE' END AS type
                         FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
                         WHERE c.relkind IN ('r','p','v','m') AND n.nspname NOT IN ('pg_catalog','information_schema','pg_toast')
                           AND c.relname ILIKE %(t)s
                         UNION ALL
                         SELECT table_schema, table_name || '  (column ' || column_name || ')', 'COLUMN'
                         FROM information_schema.columns
                         WHERE column_name ILIKE %(t)s AND table_schema NOT IN ('pg_catalog','information_schema')
                         ORDER BY 1, 2 LIMIT 100""", {"t": f"%{text}%"}, database)

    # ------------------------------------------------------------------ who changed what (event triggers)
    def tracking_enabled(self, database) -> bool:
        return bool(self.q("SELECT 1 FROM pg_event_trigger WHERE evtname = 'dt_audit_ddl'", database=database))

    def enable_change_tracking(self, database):
        """Installs dt_audit.ddl_log + 2 event triggers in this database (needs a superuser login).
        From then on every CREATE / ALTER / DROP / GRANT / REVOKE on objects is logged with who and when."""
        with self.connect(database, read_only=False) as c:
            c.execute("""
                CREATE SCHEMA IF NOT EXISTS dt_audit;
                CREATE TABLE IF NOT EXISTS dt_audit.ddl_log (
                    log_id          BIGSERIAL PRIMARY KEY,
                    event_time      TIMESTAMPTZ DEFAULT now(),
                    username        TEXT DEFAULT session_user,
                    client_addr     TEXT DEFAULT inet_client_addr()::text,
                    application     TEXT DEFAULT current_setting('application_name', true),
                    command_tag     TEXT,
                    object_type     TEXT,
                    schema_name     TEXT,
                    object_identity TEXT,
                    query           TEXT);
                CREATE OR REPLACE FUNCTION dt_audit.log_ddl() RETURNS event_trigger LANGUAGE plpgsql SECURITY DEFINER AS $f$
                DECLARE r record;
                BEGIN
                  FOR r IN SELECT * FROM pg_event_trigger_ddl_commands() LOOP
                    IF r.schema_name IS DISTINCT FROM 'dt_audit' THEN
                      INSERT INTO dt_audit.ddl_log (command_tag, object_type, schema_name, object_identity, query)
                      VALUES (r.command_tag, r.object_type, r.schema_name, r.object_identity, left(current_query(), 4000));
                    END IF;
                  END LOOP;
                  IF tg_tag IN ('GRANT', 'REVOKE') AND NOT FOUND THEN
                    INSERT INTO dt_audit.ddl_log (command_tag, query) VALUES (tg_tag, left(current_query(), 4000));
                  END IF;
                END $f$;
                CREATE OR REPLACE FUNCTION dt_audit.log_drop() RETURNS event_trigger LANGUAGE plpgsql SECURITY DEFINER AS $f$
                DECLARE r record;
                BEGIN
                  FOR r IN SELECT * FROM pg_event_trigger_dropped_objects() WHERE original LOOP
                    INSERT INTO dt_audit.ddl_log (command_tag, object_type, schema_name, object_identity, query)
                    VALUES (tg_tag, r.object_type, r.schema_name, r.object_identity, left(current_query(), 4000));
                  END LOOP;
                END $f$;
                DROP EVENT TRIGGER IF EXISTS dt_audit_ddl;
                DROP EVENT TRIGGER IF EXISTS dt_audit_drop;
                CREATE EVENT TRIGGER dt_audit_ddl  ON ddl_command_end EXECUTE FUNCTION dt_audit.log_ddl();
                CREATE EVENT TRIGGER dt_audit_drop ON sql_drop        EXECUTE FUNCTION dt_audit.log_drop();
            """)
            c.commit()

    def changes(self, database, object_name=None, username=None, limit=200):
        if not self.tracking_enabled(database):
            return []
        where, params = [], {"n": limit}
        if object_name:
            where.append("(object_identity ILIKE %(o)s OR replace(query, '\"', '') ILIKE %(o)s)")
            params["o"] = f"%{object_name}%"
        if username:
            where.append("username = %(u)s")
            params["u"] = username
        return self.q(f"""SELECT event_time, username, command_tag, object_type, schema_name,
                                 COALESCE(object_identity, left(regexp_replace(query, '\\s+', ' ', 'g'), 160)) AS object_identity,
                                 client_addr, application, query
                          FROM dt_audit.ddl_log {'WHERE ' + ' AND '.join(where) if where else ''}
                          ORDER BY log_id DESC LIMIT %(n)s""", params, database)

    # ------------------------------------------------------------------ what failed
    def failures(self, database, limit=100):
        """Failures this database records: DataTrust pipeline errors (audit.error_log) when present,
        plus database-level health counters (rollbacks, deadlocks, conflicts)."""
        out = {"pipeline": [], "health": None}
        if self.q("SELECT to_regclass('audit.error_log') IS NOT NULL AS ok", database=database)[0]["ok"]:
            out["pipeline"] = self.q("""SELECT logged_at, batch_id, layer, pipeline_id, status, error_type, sql_state,
                                               left(error_message, 500) AS error_message
                                        FROM audit.error_log WHERE status IN ('FAILED', 'CANCELLED')
                                        ORDER BY error_id DESC LIMIT %s""", (limit,), database)
            out["warnings"] = self.q("""SELECT logged_at, batch_id, layer, pipeline_id, error_type, left(error_message, 300) AS message
                                        FROM audit.error_log WHERE status = 'WARNING' ORDER BY error_id DESC LIMIT %s""",
                                     (limit,), database)
        out["health"] = self.q("""SELECT xact_commit AS commits, xact_rollback AS rollbacks, deadlocks, conflicts,
                                         temp_files, stats_reset FROM pg_stat_database WHERE datname = current_database()""",
                               database=database)[0]
        return out

    # ------------------------------------------------------------------ users, roles, access
    def roles(self):
        return self.q("""SELECT r.rolname AS name, r.rolcanlogin AS can_login, r.rolsuper AS superuser,
                                r.rolcreaterole AS can_create_roles, r.rolcreatedb AS can_create_db, r.rolvaliduntil AS valid_until,
                                ARRAY(SELECT b.rolname FROM pg_auth_members m JOIN pg_roles b ON b.oid = m.roleid
                                      WHERE m.member = r.oid ORDER BY 1) AS member_of
                         FROM pg_roles r WHERE r.rolname NOT LIKE 'pg\\_%%' ORDER BY r.rolcanlogin DESC, 1""")

    def role_access(self, database, role):
        """Everything a role can do in one database (direct grants + PUBLIC), objects first."""
        objects = self.q("""
            SELECT n.nspname AS schema, c.relname AS object,
                   CASE c.relkind WHEN 'v' THEN 'VIEW' WHEN 'm' THEN 'MATERIALIZED VIEW' WHEN 'S' THEN 'SEQUENCE' ELSE 'TABLE' END AS type,
                   string_agg(DISTINCT a.privilege_type, ', ' ORDER BY a.privilege_type) AS privileges,
                   bool_or(a.grantee = 0) AS via_public
            FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace,
                 aclexplode(COALESCE(c.relacl, acldefault('r', c.relowner))) a
            WHERE c.relkind IN ('r','p','v','m','S') AND n.nspname NOT IN ('pg_catalog','information_schema','pg_toast')
              AND (a.grantee = 0 OR a.grantee = (SELECT oid FROM pg_roles WHERE rolname = %(r)s))
            GROUP BY 1, 2, 3 ORDER BY 1, 2""", {"r": role}, database)
        schemas = self.q("""
            SELECT n.nspname AS schema, string_agg(DISTINCT a.privilege_type, ', ') AS privileges
            FROM pg_namespace n, aclexplode(COALESCE(n.nspacl, acldefault('n', n.nspowner))) a
            WHERE n.nspname NOT LIKE 'pg\\_%%' AND n.nspname <> 'information_schema'
              AND (a.grantee = 0 OR a.grantee = (SELECT oid FROM pg_roles WHERE rolname = %(r)s))
            GROUP BY 1 ORDER BY 1""", {"r": role}, database)
        owned = self.q("""SELECT n.nspname AS schema, c.relname AS object FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
                          WHERE c.relowner = (SELECT oid FROM pg_roles WHERE rolname = %(r)s) AND c.relkind IN ('r','p','v','m')
                            AND n.nspname NOT IN ('pg_catalog','information_schema') ORDER BY 1, 2 LIMIT 500""",
                       {"r": role}, database)
        database_rights = self.q("""SELECT has_database_privilege(%(r)s, current_database(), 'CONNECT') AS connect,
                                           has_database_privilege(%(r)s, current_database(), 'CREATE') AS create_schemas""",
                                 {"r": role}, database)[0]
        return {"database": database_rights, "schemas": schemas, "objects": objects, "owns": owned}

    # ------------------------------------------------------------------ building access changes (no execution)
    def build_statements(self, request_type, database, details, grantee, password=None):
        """-> (statements, preview). Validates every name against the catalog first."""
        roles = {r["name"] for r in self.roles()}
        ident = sql.Identifier
        stmts = []
        with self.connect(database) as c:
            def exists_rel(s, n):
                return c.execute("SELECT to_regclass(%s) IS NOT NULL AS ok",
                                 (sql.SQL("{}.{}").format(ident(s), ident(n)).as_string(c),)).fetchone()["ok"]

            if request_type == "CREATE_USER":
                if grantee in roles:
                    raise ValueError(f"role {grantee} already exists")
                if not grantee or len(grantee) > 63 or not all(ch.isalnum() or ch in "_.-" for ch in grantee):
                    raise ValueError("user name: letters, digits, _ . - only (max 63)")
                stmts.append(sql.SQL("CREATE ROLE {} LOGIN PASSWORD {}").format(
                    ident(grantee), sql.Literal(password) if password else sql.SQL("'********'")))
                stmts.append(sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(ident(database), ident(grantee)))
                for role in details.get("member_of", []):
                    if role not in roles:
                        raise ValueError(f"role {role} does not exist")
                    stmts.append(sql.SQL("GRANT {} TO {}").format(ident(role), ident(grantee)))
            elif request_type == "GRANT_ROLE":
                for name in (grantee, details.get("role")):
                    if name not in roles:
                        raise ValueError(f"role {name} does not exist")
                stmts.append(sql.SQL("GRANT {} TO {}").format(ident(details["role"]), ident(grantee)))
            elif request_type in ("GRANT", "REVOKE"):
                if grantee not in roles and grantee != "PUBLIC":
                    raise ValueError(f"user or role {grantee} does not exist")
                otype = details["object_type"]
                privs = details.get("privileges") or []
                if otype not in PRIVILEGES or not privs or any(p not in PRIVILEGES[otype] for p in privs):
                    raise ValueError(f"privileges {privs} are not valid for {otype}")
                priv_sql = sql.SQL("ALL PRIVILEGES") if "ALL" in privs else sql.SQL(", ").join(sql.SQL(p) for p in privs)
                who = sql.SQL("PUBLIC") if grantee == "PUBLIC" else ident(grantee)
                verb, prep = (sql.SQL("GRANT"), sql.SQL("TO")) if request_type == "GRANT" else (sql.SQL("REVOKE"), sql.SQL("FROM"))
                schema, obj = details.get("schema"), details.get("object")
                if otype == "DATABASE":
                    target = sql.SQL("DATABASE {}").format(ident(database))
                elif otype in ("SCHEMA", "ALL_TABLES_IN_SCHEMA"):
                    if not c.execute("SELECT 1 FROM pg_namespace WHERE nspname = %s", (schema,)).fetchone():
                        raise ValueError(f"schema {schema} does not exist")
                    target = sql.SQL("SCHEMA {}" if otype == "SCHEMA" else "ALL TABLES IN SCHEMA {}").format(ident(schema))
                elif otype in ("TABLE", "VIEW"):
                    if not exists_rel(schema, obj):
                        raise ValueError(f"{schema}.{obj} does not exist")
                    target = sql.SQL("TABLE {}.{}").format(ident(schema), ident(obj))
                else:                                                   # FUNCTION / PROCEDURE with its arguments
                    routine = sql.SQL("{}.").format(ident(schema)).as_string(c) + _quote_routine(c, obj)
                    if not c.execute("SELECT to_regprocedure(%s) IS NOT NULL AS ok", (routine,)).fetchone()["ok"]:
                        raise ValueError(f"{schema}.{obj} does not exist")
                    target = sql.SQL(otype + " " + routine)
                if request_type == "GRANT" and details.get("include_access_path", True) and otype not in ("DATABASE",):
                    # a grant on an object is useless without CONNECT on the database and USAGE on its schema
                    stmts.append(sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(ident(database), who))
                    if otype != "SCHEMA":
                        stmts.append(sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(ident(schema), who))
                stmts.append(sql.SQL("{} {} ON {} {} {}").format(verb, priv_sql, target, prep, who))
            else:
                raise ValueError(f"unknown request type {request_type}")
            preview = ";\n".join(s.as_string(c) for s in stmts) + ";"
        return stmts, preview

    def apply(self, request_type, database, details, grantee):
        """Run an approved request in ONE transaction. Returns the generated password for CREATE_USER (shown once)."""
        password = None
        if request_type == "CREATE_USER":
            alphabet = string.ascii_letters + string.digits
            password = "".join(secrets.choice(alphabet) for _ in range(16))
        stmts, _ = self.build_statements(request_type, database, details, grantee, password)
        with self.connect(database, read_only=False) as c:
            for s in stmts:
                c.execute(s)
            c.commit()
        return password


def _quote_routine(conn, routine: str) -> str:
    """'my_func(integer, text)' -> '"my_func"(integer, text)'; argument types are checked by to_regprocedure."""
    name, _, args = routine.partition("(")
    args = args.rstrip(")")
    if any(ch in args for ch in ";'\"\\"):
        raise ValueError("invalid routine signature")
    return sql.Identifier(name.strip()).as_string(conn) + "(" + args + ")"
