"""
Data Estate service: connectors, the access-request workflow (request -> approve -> apply) and the estate assistant.
"""
import json
import re

from . import store
from .postgres_connector import PRIVILEGES, PostgresSource
from .source_types import SOURCE_TYPES, test_connection

CONNECTORS = {"postgresql": PostgresSource}          # add SnowflakeSource / SqlServerSource here later


def source(connection_id: int):
    cfg = store.get_connection(connection_id)
    cls = CONNECTORS.get(cfg["source_type"])
    if cls is None:
        raise ValueError(f"no connector for {cfg['source_type']}")
    return cls(cfg)


def overview(connection_id: int) -> dict:
    """One call for the Connections card: reachable?, version, databases, change tracking per database."""
    cfg = store.get_connection(connection_id)
    label = SOURCE_TYPES[cfg["source_type"]]["label"]
    if cfg["source_type"] not in CONNECTORS:                  # can be tested, not browsed yet
        try:
            info = test_connection(cfg)
        except Exception as e:
            return {"ok": False, "limited": True, "label": label, "error": f"{type(e).__name__}: {e}", "databases": []}
        return {"ok": True, "limited": True, "label": label, **info, "databases": []}
    src = source(connection_id)
    try:
        info = src.test()
    except Exception as e:
        return {"ok": False, "label": label, "error": f"{type(e).__name__}: {e}", "databases": []}
    dbs = src.databases()
    for d in dbs:
        try:
            d["tracking"] = src.tracking_enabled(d["name"]) if d["can_connect"] else None
        except Exception:
            d["tracking"] = None
    return {"ok": True, "label": label, **info, "databases": dbs}


def test_new(source_type, host, port, database, username, password_env, settings) -> dict:
    """Test a source before saving it."""
    from .source_types import DEFAULT_PORT
    cfg = {"source_type": source_type, "host": host, "port": int(port or DEFAULT_PORT.get(source_type, 0)),
           "database": database, "username": username, "password_env": password_env, "settings": settings or {}}
    try:
        return {"ok": True, **test_connection(cfg)}
    except Exception as e:
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}


# ============================================================================ access requests
def create_request(connection_id, database, request_type, grantee, details, reason, user):
    """Validate against the live catalog, build the SQL preview, store as PENDING. Nothing runs yet."""
    src = source(connection_id)
    if user["role"] != "admin" and not (request_type in ("GRANT",) and grantee == user["username"]):
        raise PermissionError("viewers can only request access for themselves")
    _, preview = src.build_statements(request_type, database, details, grantee)
    with store.db() as conn:
        row = conn.execute("""INSERT INTO portal.access_requests (connection_id, database, request_type, grantee, details,
                                     sql_preview, reason, requested_by) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                              RETURNING request_id""",
                           (connection_id, database, request_type, grantee, json.dumps(details), preview, reason,
                            user["username"])).fetchone()
        conn.commit()
    store.audit(user["username"], "ACCESS_REQUESTED", f"{request_type} {grantee}", preview)
    return {"request_id": row["request_id"], "sql_preview": preview}


def list_requests(status=None, limit=200):
    with store.db() as conn:
        rows = conn.execute("""SELECT r.request_id, r.connection_id, c.name AS connection, r.database, r.request_type,
                                      r.grantee, r.details, r.sql_preview, r.reason, r.status, r.requested_by,
                                      r.requested_at, r.decided_by, r.decided_at, r.error
                               FROM portal.access_requests r LEFT JOIN portal.connections c USING (connection_id)
                               WHERE %(s)s::text IS NULL OR r.status = %(s)s
                               ORDER BY r.request_id DESC LIMIT %(n)s""", {"s": status, "n": limit}).fetchall()
    return rows


def decide(request_id: int, approve: bool, user: dict):
    """Admin decision. Approve = run the statements now on the source, in one transaction."""
    if user["role"] != "admin":
        raise PermissionError("only admins can approve or reject")
    with store.db() as conn:
        r = conn.execute("SELECT * FROM portal.access_requests WHERE request_id = %s FOR UPDATE", (request_id,)).fetchone()
        if not r:
            raise ValueError("request not found")
        if r["status"] != "PENDING":
            raise ValueError(f"request is already {r['status']}")
        result = {"request_id": request_id}
        if not approve:
            status, error = "REJECTED", None
        else:
            try:
                password = source(r["connection_id"]).apply(r["request_type"], r["database"], r["details"], r["grantee"])
                status, error = "APPLIED", None
                if password:
                    result["password"] = password          # shown once to the admin, never stored
            except Exception as e:
                status, error = "FAILED", f"{type(e).__name__}: {e}"
        conn.execute("""UPDATE portal.access_requests SET status = %s, decided_by = %s, decided_at = now(), error = %s
                        WHERE request_id = %s""", (status, user["username"], error, request_id))
        conn.commit()
    store.audit(user["username"], f"ACCESS_{status}", f"request {request_id} {r['request_type']} {r['grantee']}",
                error or r["sql_preview"])
    result.update(status=status, error=error)
    return result


# ============================================================================ estate assistant (rule based)
def _reply(answer, table=None, sources=None):
    return {"answer": answer, "table": table, "sources": sources or []}


def _table(rows, cols=None):
    if not rows:
        return None
    cols = cols or list(rows[0].keys())
    return {"columns": cols, "rows": [[r.get(c) for c in cols] for r in rows]}


def _find_object(src, database, text):
    """'silver.payments' or just 'payments' -> (schema, name, type) using the catalog."""
    m = re.search(r"\b([a-z_][a-z0-9_]*)\.([a-z_][a-z0-9_]*)\b", text)
    hits = src.search(database, m.group(2) if m else text)
    hits = [h for h in hits if h["type"] != "COLUMN"]
    if m:
        hits = [h for h in hits if h["schema"] == m.group(1) and h["name"] == m.group(2)] or hits
    return hits[0] if hits else None


def ask(connection_id: int, database: str, question: str) -> dict:
    src = source(connection_id)
    q = question.lower().strip()
    database = database or store.get_connection(connection_id)["database"]
    obj_words = re.findall(r"\b[a-z_][a-z0-9_]*\.[a-z_][a-z0-9_]*\b", q)

    if re.search(r"\b(hi|hello|help)\b", q) and len(q) < 15:
        return _reply("Ask me about this source, for example: *list databases*, *tables in schema gold*, "
                      "*describe silver.payments*, *who changed silver.customers*, *recent changes*, *what failed*, "
                      "*who has access to gold.daily_revenue*, *what can datatrust_team access*, *list users*, "
                      "*pending access requests*.")
    if re.search(r"pending|request", q):
        rows = list_requests("PENDING")
        return _reply(f"**{len(rows)} pending access request(s).**" if rows else "No pending access requests.",
                      _table(rows, ["request_id", "request_type", "grantee", "database", "requested_by", "requested_at", "reason"]),
                      ["portal.access_requests"])
    if re.search(r"\bfail|error|broke|issue", q):
        f = src.failures(database)
        text = f"**{len(f['pipeline'])} failure(s)** recorded in {database}." if f["pipeline"] else f"No failures recorded in {database}."
        h = f["health"]
        text += f" Database counters since {h['stats_reset'] or 'start'}: {h['rollbacks']:,} rollbacks, {h['deadlocks']} deadlocks."
        return _reply(text, _table(f["pipeline"]), ["audit.error_log", "pg_stat_database"])
    if re.search(r"who (changed|modified|altered|created|dropped|updated)|change|modif|altered|history|ddl", q):
        if not src.tracking_enabled(database):
            return _reply(f"Change tracking is not enabled in **{database}** yet. An admin can enable it on the "
                          "Connections page; from then on every CREATE / ALTER / DROP / GRANT is logged with who and when.")
        user = re.search(r"\bby ([a-z_][a-z0-9_.-]*)", q)
        rows = src.changes(database, object_name=obj_words[0] if obj_words else None, username=user.group(1) if user else None, limit=50)
        what = obj_words[0] if obj_words else "all objects"
        return _reply(f"**{len(rows)} change(s)** to {what} in {database}" + (f" by {user.group(1)}" if user else "") + ".",
                      _table(rows, ["event_time", "username", "command_tag", "object_identity", "client_addr"]),
                      ["dt_audit.ddl_log"])
    if re.search(r"who (has|can)|access to|permission on|grants? on", q) and obj_words:
        hit = _find_object(src, database, obj_words[0])
        if not hit:
            return _reply(f"{obj_words[0]} was not found in {database}.")
        d = src.object_detail(database, hit["schema"], hit["name"], hit["type"])
        return _reply(f"**Access to {d['name']}** ({d['type'].lower()}, owner {d['owner']}):",
                      _table(d["grants"]), ["pg_class.relacl"])
    m = re.search(r"what (can|does) ([a-z_][a-z0-9_.-]*) (access|see|have)|access (of|for) (user )?([a-z_][a-z0-9_.-]*)", q)
    if m:
        role = m.group(2) or m.group(6)
        if role not in {r["name"] for r in src.roles()}:
            return _reply(f"There is no user or role called {role}.")
        a = src.role_access(database, role)
        text = (f"**{role} in {database}:** connect {'yes' if a['database']['connect'] else 'no'}, "
                f"{len(a['schemas'])} schema(s), {len(a['objects'])} table(s)/view(s) with direct or PUBLIC grants, owns {len(a['owns'])}.")
        return _reply(text, _table(a["objects"]), ["pg_class.relacl", "pg_namespace.nspacl"])
    if re.search(r"\b(users?|roles?|members?|logins?)\b", q):
        rows = src.roles()
        return _reply(f"**{sum(1 for r in rows if r['can_login'])} login user(s)** and "
                      f"{sum(1 for r in rows if not r['can_login'])} group role(s).", _table(rows), ["pg_roles"])
    if re.search(r"databases?", q) and not obj_words:
        rows = src.databases()
        return _reply(f"**{len(rows)} database(s)** on this server.", _table(rows), ["pg_database"])
    m = re.search(r"(tables?|views?|objects?|procedures?|functions?) in (schema )?([a-z_][a-z0-9_]*)", q)
    if m:
        rows = src.objects(database, m.group(3))
        kind = m.group(1).rstrip("s").upper()
        if kind in ("TABLE", "VIEW", "PROCEDURE", "FUNCTION"):
            rows = [r for r in rows if kind in r["type"]]
        return _reply(f"**{len(rows)} {m.group(1)}** in schema {m.group(3)} ({database}).",
                      _table(rows, ["name", "type", "owner", "est_rows", "indexes"]), ["pg_class", "pg_proc"])
    if re.search(r"schemas?", q):
        rows = src.schemas(database)
        return _reply(f"**{len(rows)} schema(s)** in {database}.", _table(rows), ["pg_namespace"])
    if obj_words or re.search(r"describe|columns|structure|definition|index", q):
        hit = _find_object(src, database, obj_words[0] if obj_words else q.split()[-1])
        if not hit:
            return _reply("I could not find that object. Use schema.name, e.g. *describe silver.payments*.")
        d = src.object_detail(database, hit["schema"], hit["name"], hit["type"])
        text = (f"**{d['name']}** ({d['type'].lower()}, owner {d['owner']}): {len(d['columns'])} columns, "
                f"{len(d['indexes'])} index(es)" + (f", {d['row_count']:,} rows" if d.get("row_count") is not None else "") +
                (f". Used by: {', '.join(d['used_by'])}" if d["used_by"] else "") +
                (f". Reads from: {', '.join(d['uses'])}" if d["uses"] else "") + ".")
        return _reply(text, _table(d["columns"]), ["pg_attribute", "pg_index"])
    hits = src.search(database, q.split()[-1]) if q else []
    if hits:
        return _reply(f"Objects matching **{q.split()[-1]}**:", _table(hits), ["pg_class", "information_schema.columns"])
    return _reply("I did not understand that. Try: *tables in schema gold*, *describe silver.payments*, "
                  "*who changed silver.customers*, *what failed*, *who has access to gold.daily_revenue*, *list users*.")


__all__ = ["source", "overview", "create_request", "list_requests", "decide", "ask", "PRIVILEGES"]




