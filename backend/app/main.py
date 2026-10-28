import time
import uuid

import structlog
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from sqlalchemy import text

from app.api import admin, auth, imports, orgs, trade
from app.config import get_settings
from app.db import SessionLocal
from app.logging import configure_logging, log
from app.services import ratelimit

settings = get_settings()
configure_logging(settings.log_level)

app = FastAPI(
    title="TradeRadar API",
    version="0.1.0",
    docs_url="/api/docs" if settings.app_env != "production" else None,
    openapi_url="/api/openapi.json" if settings.app_env != "production" else None,
)


@app.middleware("http")
async def request_context(request: Request, call_next):
    request_id = request.headers.get("X-Request-ID") or uuid.uuid4().hex
    structlog.contextvars.clear_contextvars()
    structlog.contextvars.bind_contextvars(request_id=request_id)
    start = time.perf_counter()
    try:
        response: Response = await call_next(request)
    except Exception:
        log.exception("request.unhandled", path=request.url.path, method=request.method)
        response = JSONResponse({"detail": "Internal server error"}, status_code=500)
    elapsed_ms = round((time.perf_counter() - start) * 1000, 1)
    response.headers["X-Request-ID"] = request_id
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    response.headers["Cache-Control"] = response.headers.get("Cache-Control", "no-store")
    if not request.url.path.startswith(("/healthz", "/readyz")):
        log.info(
            "request",
            method=request.method,
            path=request.url.path,
            status=response.status_code,
            ms=elapsed_ms,
        )
    return response


@app.get("/healthz", include_in_schema=False)
def healthz() -> dict:
    return {"ok": True}


@app.get("/readyz", include_in_schema=False)
def readyz() -> JSONResponse:
    checks = {"database": False, "redis": ratelimit.ping()}
    try:
        with SessionLocal() as db:
            db.execute(text("SELECT 1"))
        checks["database"] = True
    except Exception as e:  # pragma: no cover - exercised only when the DB is down
        log.warning("readyz.db_failed", error=str(e))
    ok = all(checks.values())
    return JSONResponse({"ok": ok, **checks}, status_code=200 if ok else 503)


for r in (auth.router, orgs.router, trade.router, imports.router, admin.router):
    app.include_router(r)
