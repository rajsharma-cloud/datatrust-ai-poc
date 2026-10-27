"""
/api/estate/* — the Data Estate module's API. Every call needs a portal login (cookie dt_session);
changes (connections, change tracking, approvals, portal users) need the admin role.
"""
import os
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from pydantic import BaseModel

from . import service, store

router = APIRouter(prefix="/api/estate")
COOKIE = "dt_session"


def current_user(request: Request) -> dict:
    user = store.read_session(request.cookies.get(COOKIE, ""))
    if not user:
        raise HTTPException(401, "please log in")
    return user


def admin_user(user: dict = Depends(current_user)) -> dict:
    if user["role"] != "admin":
        raise HTTPException(403, "admin only")
    return user


def guard(fn, *args, **kwargs):
    """Turn validation problems into a readable 400 instead of a 500."""
    try:
        return fn(*args, **kwargs)
    except PermissionError as e:
        raise HTTPException(403, str(e))
    except ValueError as e:
        raise HTTPException(400, str(e))


# ---------------------------------------------------------------- login
class Login(BaseModel):
    username: str
    password: str


@router.post("/login")
def login(body: Login, response: Response):
    user = store.login(body.username, body.password)
    if not user:
        raise HTTPException(401, "wrong user name or password")
    # PORTAL_COOKIE_SECURE=true in the cloud (HTTPS): the login cookie is then never sent over plain HTTP
    response.set_cookie(COOKIE, store.make_session(user["username"], user["role"]), httponly=True, samesite="lax",
                        secure=os.getenv("PORTAL_COOKIE_SECURE", "false").lower() == "true",
                        max_age=store.SESSION_HOURS * 3600)
    return user


@router.post("/logout")
def logout(response: Response):
    response.delete_cookie(COOKIE)
    return {"ok": True}


@router.get("/session")
def session(request: Request):
    """Who is logged in (null when nobody): used by the page on load, never answers 401."""
    return {"user": store.read_session(request.cookies.get(COOKIE, ""))}


@router.get("/me")
def me(user: dict = Depends(current_user)):
    return user


# ---------------------------------------------------------------- portal users (admin)
class PortalUser(BaseModel):
    username: str
    password: str
    role: str = "viewer"
    active: bool = True


@router.get("/portal-users")
def portal_users(user: dict = Depends(admin_user)):
    return store.list_users()


@router.post("/portal-users")
def save_portal_user(body: PortalUser, user: dict = Depends(admin_user)):
    guard(store.save_user, body.username, body.role, body.password, user["username"], body.active)
    return {"ok": True}


# ---------------------------------------------------------------- connections
class NewConnection(BaseModel):
    name: str = ""
    source_type: str = "postgresql"
    host: str
    port: Optional[int] = None
    database: str = ""
    username: str
    password_env: str
    settings: dict = {}


@router.get("/source-types")
def source_types(user: dict = Depends(current_user)):
    from .source_types import SOURCE_TYPES
    return SOURCE_TYPES


@router.post("/connections/test")
def test_connection(body: NewConnection, user: dict = Depends(admin_user)):
    return service.test_new(body.source_type, body.host, body.port, body.database, body.username,
                            body.password_env, body.settings)


@router.get("/connections")
def connections(user: dict = Depends(current_user)):
    rows = store.list_connections()
    if user["role"] != "admin":
        for r in rows:
            r.pop("password_env", None)
    return rows


@router.post("/connections")
def add_connection(body: NewConnection, user: dict = Depends(admin_user)):
    cid = guard(store.add_connection, body.name, body.host, body.port, body.database, body.username,
                body.password_env, user["username"], body.source_type, body.settings)
    return {"connection_id": cid, "overview": service.overview(cid)}


@router.delete("/connections/{cid}")
def delete_connection(cid: int, user: dict = Depends(admin_user)):
    store.delete_connection(cid, user["username"])
    return {"ok": True}


@router.get("/connections/{cid}/overview")
def overview(cid: int, user: dict = Depends(current_user)):
    return guard(service.overview, cid)


@router.post("/connections/{cid}/tracking/{database}")
def enable_tracking(cid: int, database: str, user: dict = Depends(admin_user)):
    guard(service.source(cid).enable_change_tracking, database)
    store.audit(user["username"], "TRACKING_ENABLED", f"{cid}/{database}", "dt_audit event triggers installed")
    return {"ok": True}


# ---------------------------------------------------------------- catalog
@router.get("/connections/{cid}/db/{database}/schemas")
def schemas(cid: int, database: str, user: dict = Depends(current_user)):
    return guard(service.source(cid).schemas, database)


@router.get("/connections/{cid}/db/{database}/schemas/{schema}/objects")
def objects(cid: int, database: str, schema: str, user: dict = Depends(current_user)):
    return guard(service.source(cid).objects, database, schema)


@router.get("/connections/{cid}/db/{database}/object")
def object_detail(cid: int, database: str, schema: str, name: str, type: str, user: dict = Depends(current_user)):
    return guard(service.source(cid).object_detail, database, schema, name, type)


@router.get("/connections/{cid}/db/{database}/search")
def search(cid: int, database: str, text: str, user: dict = Depends(current_user)):
    return guard(service.source(cid).search, database, text)


# ---------------------------------------------------------------- activity
@router.get("/connections/{cid}/db/{database}/changes")
def changes(cid: int, database: str, object_name: Optional[str] = None, username: Optional[str] = None,
            user: dict = Depends(current_user)):
    src = service.source(cid)
    return {"tracking": src.tracking_enabled(database),
            "changes": guard(src.changes, database, object_name or None, username or None)}


@router.get("/connections/{cid}/db/{database}/failures")
def failures(cid: int, database: str, user: dict = Depends(current_user)):
    return guard(service.source(cid).failures, database)


# ---------------------------------------------------------------- access
@router.get("/connections/{cid}/roles")
def roles(cid: int, user: dict = Depends(current_user)):
    return guard(service.source(cid).roles)


@router.get("/connections/{cid}/db/{database}/roles/{role}/access")
def role_access(cid: int, database: str, role: str, user: dict = Depends(current_user)):
    return guard(service.source(cid).role_access, database, role)


@router.get("/privileges")
def privileges(user: dict = Depends(current_user)):
    return service.PRIVILEGES


class AccessRequest(BaseModel):
    connection_id: int
    database: str
    request_type: str              # CREATE_USER / GRANT / REVOKE / GRANT_ROLE
    grantee: str
    details: dict = {}
    reason: str = ""


@router.post("/access-requests")
def create_access_request(body: AccessRequest, user: dict = Depends(current_user)):
    return guard(service.create_request, body.connection_id, body.database, body.request_type,
                 body.grantee.strip(), body.details, body.reason, user)


@router.get("/access-requests")
def access_requests(status: Optional[str] = None, user: dict = Depends(current_user)):
    return service.list_requests(status)


@router.post("/access-requests/{rid}/{decision}")
def decide(rid: int, decision: str, user: dict = Depends(admin_user)):
    if decision not in ("approve", "reject"):
        raise HTTPException(400, "decision must be approve or reject")
    return guard(service.decide, rid, decision == "approve", user)


@router.get("/audit")
def portal_audit(user: dict = Depends(admin_user)):
    return store.audit_log()


# ---------------------------------------------------------------- estate assistant
class Question(BaseModel):
    connection_id: int
    database: Optional[str] = None
    question: str


@router.post("/ask")
def ask(body: Question, user: dict = Depends(current_user)):
    return guard(service.ask, body.connection_id, body.database, body.question)
