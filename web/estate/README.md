# Data Estate module

Connect databases and warehouses to the DataTrust portal and see, in one place:
what is in them (catalog), who changed what, what failed, who has access, and grant access
through request -> approve -> apply. An estate assistant answers questions about any connected source.

Version 1 supports PostgreSQL sources. Snowflake / SQL Server connectors plug into `service.CONNECTORS`.

## Files
| File | Purpose |
|---|---|
| store.py | Portal tables in schema `portal` (users, connections, access requests, audit log), login, sessions |
| postgres_connector.py | Catalog, change tracking (event triggers), failures, roles, safe GRANT/REVOKE/CREATE USER |
| service.py | Request workflow and the estate assistant |
| routes.py | API under /api/estate (login required; changes need admin) |
| ../static/estate.js | The five pages in the sidebar group "Data estate" |

## First start
`python web\server.py` creates schema `portal` and the first admin. If `PORTAL_ADMIN_PASSWORD` is not set in .env,
a password is generated and printed ONCE in the terminal. The POC database is added as the first source.

## Security
* Passwords of sources are never stored: a source keeps the NAME of a .env variable.
* Browsing runs in read-only sessions (10 s timeout).
* Access changes run only after an admin approves; the exact SQL is shown before; identifiers are quoted;
  every name is checked against the live catalog; each step goes to portal.audit_log.
* New database users get a random 16-character password, shown once to the approving admin.
* Change tracking (Connections page) installs schema dt_audit + 2 event triggers in the chosen database;
  it needs a superuser login on that source.
