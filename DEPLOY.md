# Deploying the DataTrust AI web app (for the DevOps team)

The web app (dashboard, AI agent, Data Estate portal) is one container. It needs one PostgreSQL database.
The data pipeline (Spark) and Kafka streaming are separate and are NOT part of this container.

```
 Users ──HTTPS──► Load balancer / ingress (TLS) ──► datatrust-web container :8000 ──► PostgreSQL 17 (managed)
                                                     health check: GET /health        bronze/silver/gold/audit/portal
```

## 1. Database
* Managed PostgreSQL 17 (e.g. Azure Database for PostgreSQL Flexible Server).
* Load the data once: restore `datatrust_backup.backup` with pg_restore (or run the pipeline against it).
* The app creates schema `portal` itself on first start.
* Network: allow the container's outbound IP / VNet only; require SSL.

## 2. Container
```bash
docker build -t datatrust-web .
docker run -p 8000:8000 --env-file prod.env datatrust-web        # local smoke test
```
Image: python:3.12-slim, only `config.py` + `web/`, non-root user, built-in HEALTHCHECK.
No secrets or data are baked in (`.dockerignore` excludes .env, data, backups).

Try everything on one machine (app + a PostgreSQL container): `docker compose -f docker-compose.web.yml up -d --build`

## 3. Settings (environment variables; secrets from Key Vault / secret store)
| Variable | Value in the cloud | Secret? |
|---|---|---|
| PG_HOST, PG_PORT, PG_DATABASE, PG_USER | managed PostgreSQL | |
| PG_PASSWORD | database password | yes |
| WEB_HOST | 0.0.0.0 (already set in the image) | |
| PORT or WEB_PORT | port the platform expects (default 8000) | |
| PORTAL_ADMIN_USER / PORTAL_ADMIN_PASSWORD | first portal admin (used only when no portal user exists) | password: yes |
| PORTAL_SECRET | long random string; signs login cookies (keep the same across restarts and instances) | yes |
| PORTAL_COOKIE_SECURE | true (site served over HTTPS) | |
| CONN_<NAME>_PASSWORD | one per extra source added in the Data Estate portal | yes |
| LLM_PROVIDER (+ ANTHROPIC_API_KEY or OLLAMA_URL) | none / anthropic / ollama | key: yes |
| DATATRUST_AS_OF | data (default) or today | |

## 4. Platform checklist
* HTTPS only (TLS at the load balancer); redirect HTTP.
* Health probe: `GET /health` → 200 `{"status":"ok"}`, 503 if the database is unreachable.
* Scaling: stateless; sessions are signed cookies, so several instances work if they share PORTAL_SECRET.
* Logs: stdout (set WEB_LOG_LEVEL=info for request logs).
* Access: put the site behind company SSO / IP allow-list for the demo if possible; the portal login is a POC login.
* The Data Estate portal can change permissions on connected databases (after admin approval):
  give its source logins only the rights you want it to manage.
