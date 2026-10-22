import hashlib
import json
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from .models import BatchRequest, BatchView, PipelineReport
from .pipeline import run_pipeline


class BatchNotFound(Exception):
    pass


class BatchNotReady(Exception):
    pass


class IdempotencyConflict(Exception):
    pass


class Store:
    def __init__(self, path: str | Path):
        if str(path) == ":memory:":
            raise ValueError("Use a file-backed SQLite database, not :memory:.")
        self.path = Path(path)

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self.path, timeout=5)
        connection.row_factory = sqlite3.Row
        try:
            with connection:
                yield connection
        finally:
            connection.close()

    def initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as connection:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS batches (
                    id TEXT PRIMARY KEY,
                    idempotency_key TEXT NOT NULL UNIQUE,
                    fingerprint TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    reading_count INTEGER NOT NULL,
                    status TEXT NOT NULL CHECK (status IN ('pending', 'completed')),
                    created_at TEXT NOT NULL,
                    completed_at TEXT,
                    report TEXT,
                    CHECK (
                        (status = 'pending' AND completed_at IS NULL AND report IS NULL)
                        OR
                        (status = 'completed' AND completed_at IS NOT NULL AND report IS NOT NULL)
                    )
                )
                """
            )

    def health(self) -> None:
        with self.connect() as connection:
            connection.execute("SELECT id FROM batches LIMIT 1").fetchone()

    @staticmethod
    def _view(row: sqlite3.Row) -> BatchView:
        return BatchView.model_validate({name: row[name] for name in BatchView.model_fields})

    @staticmethod
    def _find(connection: sqlite3.Connection, batch_id: str) -> sqlite3.Row:
        row = connection.execute("SELECT * FROM batches WHERE id = ?", (batch_id,)).fetchone()
        if row is None:
            raise BatchNotFound(batch_id)
        return row

    def submit(self, batch: BatchRequest, key: str) -> tuple[BatchView, bool]:
        payload = json.dumps(batch.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
        fingerprint = hashlib.sha256(payload.encode()).hexdigest()
        with self.connect() as connection:
            # Serialize the key check and insert across threads and app processes.
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                "SELECT * FROM batches WHERE idempotency_key = ?", (key,)
            ).fetchone()
            if existing is not None:
                if existing["fingerprint"] != fingerprint:
                    raise IdempotencyConflict(key)
                return self._view(existing), False
            batch_id = str(uuid4())
            connection.execute(
                """
                INSERT INTO batches (
                    id, idempotency_key, fingerprint, payload, reading_count, status, created_at
                ) VALUES (?, ?, ?, ?, ?, 'pending', ?)
                """,
                (
                    batch_id,
                    key,
                    fingerprint,
                    payload,
                    len(batch.readings),
                    datetime.now(UTC).isoformat(),
                ),
            )
            return self._view(self._find(connection, batch_id)), True

    def get(self, batch_id: str) -> BatchView:
        with self.connect() as connection:
            return self._view(self._find(connection, batch_id))

    def report(self, batch_id: str) -> PipelineReport:
        with self.connect() as connection:
            row = self._find(connection, batch_id)
            if row["status"] != "completed":
                raise BatchNotReady(batch_id)
            return PipelineReport.model_validate_json(row["report"])

    def run(self, batch_id: str) -> PipelineReport:
        with self.connect() as connection:
            # Keep processing and the final write atomic. A failure leaves the batch pending.
            connection.execute("BEGIN IMMEDIATE")
            row = self._find(connection, batch_id)
            if row["status"] == "completed":
                return PipelineReport.model_validate_json(row["report"])
            report = run_pipeline(BatchRequest.model_validate_json(row["payload"]))
            connection.execute(
                """
                UPDATE batches SET status = 'completed', completed_at = ?, report = ? WHERE id = ?
                """,
                (datetime.now(UTC).isoformat(), report.model_dump_json(), batch_id),
            )
            return report
