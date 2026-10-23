import hashlib
import json
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

from .models import (
    BatchRequest,
    BatchView,
    DeliveryPayload,
    DeliveryView,
    PipelineReport,
    RuleSetRequest,
    RuleSetView,
)
from .pipeline import run_pipeline
from .rules import DEFAULT_RULES


class BatchNotFound(Exception):
    pass


class BatchNotReady(Exception):
    pass


class IdempotencyConflict(Exception):
    pass


class RuleSetNotFound(Exception):
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

            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS deliveries (
                    id TEXT PRIMARY KEY,
                    batch_id TEXT NOT NULL UNIQUE REFERENCES batches(id),
                    payload TEXT NOT NULL,
                    status TEXT NOT NULL CHECK (status IN
                        ('pending', 'in_flight', 'succeeded', 'dead')),
                    attempt_count INTEGER NOT NULL DEFAULT 0,
                    next_attempt_at TEXT,
                    last_error TEXT,
                    claim_id TEXT,
                    claim_expires_at TEXT
                )
                """
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS deliveries_due ON deliveries(status, next_attempt_at)"
            )
            connection.execute(
                """CREATE TABLE IF NOT EXISTS rule_sets (
                    version INTEGER PRIMARY KEY, rules TEXT NOT NULL, note TEXT,
                    created_at TEXT NOT NULL, active INTEGER NOT NULL CHECK (active IN (0, 1))
                )"""
            )
            connection.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS one_active_ruleset ON rule_sets(active) "
                "WHERE active = 1"
            )
            connection.execute(
                "INSERT OR IGNORE INTO rule_sets VALUES (1, ?, NULL, ?, 1)",
                (json.dumps(DEFAULT_RULES), datetime.now(UTC).isoformat()),
            )

    @staticmethod
    def _ruleset(row: sqlite3.Row) -> RuleSetView:
        return RuleSetView(
            version=row["version"],
            rules=json.loads(row["rules"]),
            note=row["note"],
            created_at=row["created_at"],
            active=bool(row["active"]),
        )

    @staticmethod
    def _find_ruleset(connection: sqlite3.Connection, version: int) -> sqlite3.Row:
        row = connection.execute("SELECT * FROM rule_sets WHERE version = ?", (version,)).fetchone()
        if row is None:
            raise RuleSetNotFound(version)
        return row

    def list_rules(self) -> list[RuleSetView]:
        with self.connect() as connection:
            return [
                self._ruleset(row)
                for row in connection.execute("SELECT * FROM rule_sets ORDER BY version")
            ]

    def get_rules(self, version: int | None = None) -> RuleSetView:
        with self.connect() as connection:
            row = (
                connection.execute("SELECT * FROM rule_sets WHERE active = 1").fetchone()
                if version is None
                else self._find_ruleset(connection, version)
            )
            return self._ruleset(row)

    def create_rules(self, body: RuleSetRequest) -> RuleSetView:
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            version = connection.execute("SELECT MAX(version) + 1 FROM rule_sets").fetchone()[0]
            connection.execute(
                "INSERT INTO rule_sets VALUES (?, ?, ?, ?, 0)",
                (
                    version,
                    json.dumps([r.model_dump() for r in body.rules]),
                    body.note,
                    datetime.now(UTC).isoformat(),
                ),
            )
            return self._ruleset(self._find_ruleset(connection, version))

    def activate_rules(self, version: int) -> RuleSetView:
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            self._find_ruleset(connection, version)
            connection.execute("UPDATE rule_sets SET active = 0 WHERE active = 1")
            connection.execute("UPDATE rule_sets SET active = 1 WHERE version = ?", (version,))
            return self._ruleset(self._find_ruleset(connection, version))

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
            active = self._ruleset(
                connection.execute("SELECT * FROM rule_sets WHERE active = 1").fetchone()
            )
            report = run_pipeline(
                BatchRequest.model_validate_json(row["payload"]), active.rules, active.version
            )
            connection.execute(
                """
                UPDATE batches SET status = 'completed', completed_at = ?, report = ? WHERE id = ?
                """,
                (datetime.now(UTC).isoformat(), report.model_dump_json(), batch_id),
            )
            if report.severity_counts["warning"] or report.severity_counts["critical"]:
                delivery_id = str(uuid4())
                payload = DeliveryPayload(
                    delivery_id=delivery_id,
                    batch_id=batch_id,
                    pipeline_version=report.pipeline_version,
                    severity_counts=report.severity_counts,
                )
                connection.execute(
                    """INSERT INTO deliveries (id, batch_id, payload, status, next_attempt_at)
                    VALUES (?, ?, ?, 'pending', ?)""",
                    (
                        delivery_id,
                        batch_id,
                        payload.model_dump_json(),
                        datetime.now(UTC).isoformat(),
                    ),
                )
            return report

    def delivery(self, batch_id: str) -> DeliveryView | None:
        with self.connect() as connection:
            row = connection.execute(
                "SELECT * FROM deliveries WHERE batch_id = ?", (batch_id,)
            ).fetchone()
            if row is None:
                return None
            return DeliveryView(
                **{name: row[name] for name in DeliveryView.model_fields if name != "payload"},
                payload=DeliveryPayload.model_validate_json(row["payload"]),
            )

    def claim(self, now: datetime, excluded: list[str]) -> dict | None:
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            # A reserved attempt counts even if its worker disappears before recording a result.
            connection.execute(
                """UPDATE deliveries SET status = 'dead', next_attempt_at = NULL,
                last_error = 'Claim expired after final attempt', claim_id = NULL,
                claim_expires_at = NULL WHERE status = 'in_flight'
                AND claim_expires_at <= ? AND attempt_count >= 5""",
                (now.isoformat(),),
            )
            placeholders = ",".join("?" for _ in excluded) or "''"
            row = connection.execute(
                f"""SELECT * FROM deliveries WHERE id NOT IN ({placeholders}) AND
                ((status = 'pending' AND next_attempt_at <= ?)
                OR (status = 'in_flight' AND claim_expires_at <= ?))
                ORDER BY next_attempt_at, id LIMIT 1""",
                (*excluded, now.isoformat(), now.isoformat()),
            ).fetchone()
            if row is None:
                return None
            claim_id = str(uuid4())
            connection.execute(
                """UPDATE deliveries SET status = 'in_flight', claim_id = ?,
                claim_expires_at = ?, attempt_count = attempt_count + 1 WHERE id = ?""",
                (claim_id, (now + timedelta(seconds=60)).isoformat(), row["id"]),
            )
            return dict(row) | {"claim_id": claim_id, "attempt_count": row["attempt_count"] + 1}

    def finish(self, claim: dict, now: datetime, code: int | None) -> str:
        success = code is not None and 200 <= code < 300
        retryable = code is None or code == 429 or 500 <= code < 600
        outcome = (
            "succeeded"
            if success
            else ("retried" if retryable and claim["attempt_count"] < 5 else "dead")
        )
        next_at = (
            (now + timedelta(seconds=2 ** (claim["attempt_count"] - 1))).isoformat()
            if outcome == "retried"
            else None
        )
        error = None if success else ("Network failure" if code is None else f"HTTP {code}")
        with self.connect() as connection:
            updated = connection.execute(
                """UPDATE deliveries SET status = ?, next_attempt_at = ?, last_error = ?,
                claim_id = NULL, claim_expires_at = NULL
                WHERE id = ? AND status = 'in_flight' AND claim_id = ?""",
                (
                    "pending" if outcome == "retried" else outcome,
                    next_at,
                    error,
                    claim["id"],
                    claim["claim_id"],
                ),
            )
            return outcome if updated.rowcount else "stale"
