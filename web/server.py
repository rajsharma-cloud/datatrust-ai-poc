"""
DataTrust AI web app.

    python web/server.py            then open http://localhost:8000

Reads PostgreSQL live (gold views + audit tables). The page checks /api/version every 15 seconds:
when a pipeline run writes new data, every panel refreshes by itself.
"""
import os
import sys
from pathlib import Path

import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import agent          # noqa: E402
import data_service as ds  # noqa: E402

app = FastAPI(title="DataTrust AI")
app.mount("/static", StaticFiles(directory=HERE / "static"), name="static")

# Data Estate module (connections, catalog, changes, access): separate package, own API under /api/estate
from estate import routes as estate_routes, store as estate_store  # noqa: E402
app.include_router(estate_routes.router)
try:
    estate_store.init()
except Exception as e:
    print(f"!! Data Estate module not ready: {e}")


@app.exception_handler(Exception)
async def on_error(request, exc):
    """Any database / query problem is shown on the page instead of a blank screen."""
    return JSONResponse(status_code=500, content={"error": f"{type(exc).__name__}: {exc}"})


try:                                                    # table for approvals made on the page
    ds.ensure_web_tables()
except Exception as e:
    print(f"!! could not prepare audit.remediation_log: {e}")


@app.get("/")
def index():
    return FileResponse(HERE / "static" / "index.html")


@app.get("/health")
def health():
    """For the cloud load balancer / container health check: 200 when the app and its database answer."""
    try:
        ds.one("SELECT 1 AS ok")
        return {"status": "ok"}
    except Exception as e:
        return JSONResponse(status_code=503, content={"status": "database unavailable", "error": type(e).__name__})


@app.get("/api/version")
def version():
    return ds.data_version()


@app.get("/api/overview")
def overview():
    return {"overview": ds.overview(), "trust": ds.trust_score(), "pipeline": ds.pipeline_status(),
            "as_of": ds.as_of_date().isoformat(), "llm": os.getenv("LLM_PROVIDER", "none")}


@app.get("/api/incidents")
def incidents():
    return {"open": ds.incidents(), "resolved": ds.resolved_incidents()}


@app.get("/api/investigate/{incident_id}")
def investigate(incident_id: str):
    return ds.investigate(incident_id)


@app.get("/api/lineage")
def lineage():
    return ds.lineage()


@app.get("/api/pipeline")
def pipeline():
    return ds.pipeline_status()


class Approval(BaseModel):
    incident_id: str
    title: str = ""
    actions: list = []
    approved_by: str = "web user"
    note: str = ""


@app.post("/api/approve")
def approve(a: Approval):
    if not a.actions:
        raise HTTPException(400, "select at least one action")
    ds.approve(a.incident_id, a.title, a.actions, a.approved_by, a.note)
    return {"ok": True}


class Question(BaseModel):
    question: str
    history: list = []


@app.post("/api/ask")
def ask(q: Question):
    return agent.ask(q.question, q.history)


if __name__ == "__main__":
    # laptop: 127.0.0.1 (only this computer). Container / cloud: WEB_HOST=0.0.0.0, and the platform's PORT if it sets one
    port = int(os.getenv("PORT") or os.getenv("WEB_PORT", "8000"))
    host = os.getenv("WEB_HOST", "127.0.0.1")
    print(f"DataTrust AI  ->  http://localhost:{port}   (listening on {host}; Ctrl+C to stop)")
    uvicorn.run(app, host=host, port=port, log_level=os.getenv("WEB_LOG_LEVEL", "warning"), proxy_headers=True,
                forwarded_allow_ips="*")
