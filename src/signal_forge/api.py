import os
import sqlite3
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated
from uuid import UUID

import httpx
from fastapi import FastAPI, Header, HTTPException, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from .delivery import dispatch
from .models import (
    BatchRequest,
    BatchView,
    DeliveryView,
    DispatchRequest,
    DispatchResult,
    Health,
    PipelineReport,
    ReplayRequest,
    ReplayResult,
    RuleSetRequest,
    RuleSetView,
)
from .storage import BatchNotFound, BatchNotReady, IdempotencyConflict, RuleSetNotFound, Store

IdempotencyKey = Annotated[
    str,
    Header(alias="Idempotency-Key", min_length=1, max_length=128, pattern=r"^[A-Za-z0-9._:-]+$"),
]


def create_app(database_path: str | Path | None = None) -> FastAPI:
    store = Store(
        database_path
        if database_path is not None
        else os.environ.get("SIGNAL_FORGE_DB", "var/signal-forge.sqlite3")
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        store.initialize()
        yield

    app = FastAPI(
        title="Signal Forge",
        version="0.1.0",
        description="A local, synthetic sensor-event pipeline. Submit, run, and inspect a batch.",
        lifespan=lifespan,
    )

    @app.exception_handler(RequestValidationError)
    async def invalid_request(request: Request, exc: RequestValidationError) -> JSONResponse:
        # Raw inputs can contain infinity or exception objects that JSON cannot encode.
        errors = [
            {field: error[field] for field in ("loc", "msg", "type")} for error in exc.errors()
        ]
        return JSONResponse(status_code=422, content={"detail": errors})

    @app.exception_handler(BatchNotFound)
    async def not_found(request: Request, exc: BatchNotFound) -> JSONResponse:
        return JSONResponse(status_code=404, content={"detail": "Batch not found"})

    @app.exception_handler(RuleSetNotFound)
    async def rules_not_found(request: Request, exc: RuleSetNotFound) -> JSONResponse:
        return JSONResponse(status_code=404, content={"detail": "Rule set not found"})

    @app.exception_handler(BatchNotReady)
    async def not_ready(request: Request, exc: BatchNotReady) -> JSONResponse:
        return JSONResponse(
            status_code=409, content={"detail": "Run the batch before reading its report"}
        )

    @app.exception_handler(IdempotencyConflict)
    async def key_conflict(request: Request, exc: IdempotencyConflict) -> JSONResponse:
        return JSONResponse(
            status_code=409,
            content={"detail": "Idempotency-Key already belongs to a different payload"},
        )

    @app.exception_handler(sqlite3.OperationalError)
    async def database_error(request: Request, exc: sqlite3.OperationalError) -> JSONResponse:
        if getattr(exc, "sqlite_errorcode", None) in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED):
            return JSONResponse(
                status_code=503,
                content={
                    "detail": "Database busy. Retry the request with the same Idempotency-Key"
                },
                headers={"Retry-After": "1"},
            )
        raise exc

    @app.get("/health", tags=["system"])
    def health() -> Health:
        store.health()
        return Health(status="ok")

    @app.get("/v1/rules", tags=["rules"])
    def list_rules() -> list[RuleSetView]:
        return store.list_rules()

    @app.get("/v1/rules/active", tags=["rules"])
    def active_rules() -> RuleSetView:
        return store.get_rules()

    @app.post("/v1/rules", status_code=201, tags=["rules"])
    def create_rules(body: RuleSetRequest, response: Response) -> RuleSetView:
        view = store.create_rules(body)
        response.headers["Location"] = f"/v1/rules/{view.version}"
        return view

    @app.get("/v1/rules/{version}", tags=["rules"])
    def get_rules(version: int) -> RuleSetView:
        return store.get_rules(version)

    @app.post("/v1/rules/{version}/activate", tags=["rules"])
    def activate_rules(version: int) -> RuleSetView:
        return store.activate_rules(version)

    @app.post("/v1/rules/{version}/replay", tags=["rules"])
    def replay_rules(version: int, body: ReplayRequest) -> ReplayResult:
        return store.replay(version, body.limit)

    @app.post("/v1/batches", status_code=202, tags=["batches"])
    def submit_batch(
        batch: BatchRequest, response: Response, idempotency_key: IdempotencyKey
    ) -> BatchView:
        """Store a pending batch. An identical retry returns 200 and the existing batch."""
        view, created = store.submit(batch, idempotency_key)
        response.status_code = 202 if created else 200
        response.headers["Location"] = f"/v1/batches/{view.id}"
        return view

    @app.get("/v1/batches/{batch_id}", tags=["batches"])
    def get_batch(batch_id: UUID) -> BatchView:
        return store.get(str(batch_id))

    @app.post("/v1/batches/{batch_id}/run", tags=["batches"])
    def run_batch(batch_id: UUID) -> PipelineReport:
        """Run synchronously. A completed batch returns its stored report without rerunning."""
        return store.run(str(batch_id))

    @app.get("/v1/batches/{batch_id}/report", tags=["batches"])
    def get_report(batch_id: UUID) -> PipelineReport:
        return store.report(str(batch_id))

    @app.get("/v1/batches/{batch_id}/delivery", tags=["deliveries"])
    def get_delivery(batch_id: UUID) -> DeliveryView:
        delivery = store.delivery(str(batch_id))
        if delivery is None:
            raise HTTPException(404, "Delivery not found")
        return delivery

    @app.post("/v1/deliveries/dispatch", tags=["deliveries"])
    def dispatch_deliveries(body: DispatchRequest) -> DispatchResult:
        url = os.environ.get("SIGNAL_FORGE_WEBHOOK_URL", "")
        try:
            parsed = httpx.URL(url)
            valid = parsed.scheme == "https" and bool(parsed.host)
        except Exception:
            valid = False
        if not valid:
            raise HTTPException(503, "Webhook requires a configured HTTPS URL")
        return dispatch(store, url, body.limit)

    return app
