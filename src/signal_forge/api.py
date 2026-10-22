import os
import sqlite3
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated
from uuid import UUID

from fastapi import FastAPI, Header, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from .models import BatchRequest, BatchView, Health, PipelineReport
from .storage import BatchNotFound, BatchNotReady, IdempotencyConflict, Store

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

    return app
