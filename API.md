# API reference

Signal Forge is a local FastAPI service for synthetic sensor readings.
A client submits a batch. A separate client request runs the pipeline synchronously.
There is no background worker or automatic processing.

## Routes

| Method | Route                           | Result                                                                                  |
| ------ | ------------------------------- | --------------------------------------------------------------------------------------- |
| `GET`  | `/health`                       | `200` after a database read succeeds.                                                   |
| `POST` | `/v1/batches`                   | `202` for a new pending batch. `200` for an identical retry.                            |
| `GET`  | `/v1/batches/{batch_id}`        | `200` with the batch ID, status, timestamps, and reading count.                         |
| `POST` | `/v1/batches/{batch_id}/run`    | `200` with the processed report. Completed batches return the saved report.             |
| `GET`  | `/v1/batches/{batch_id}/report` | `200` with the saved report. `409` if the batch is pending.                             |
| `GET` | `/v1/batches/{batch_id}/delivery` | `200` with the saved delivery; `404` if none exists. |
| `POST` | `/v1/deliveries/dispatch` | `200` with outcome counts for bounded manual dispatch. |
| `GET`  | `/docs`                         | Interactive OpenAPI documentation. The UI uses CDN assets and can need internet access. |
| `GET`  | `/openapi.json`                 | Machine-readable API schema.                                                            |

Unknown batch IDs return `404`. Invalid UUIDs and malformed requests return `422`.
A busy or locked database returns `503` with `Retry-After: 1`.
Unexpected processing failures return `500` and leave the batch pending.

## Submission contract

`POST /v1/batches` requires an `Idempotency-Key` header.
The key contains 1–128 ASCII letters, digits, periods, underscores, colons, or hyphens.
The key identifies one request in this database. It has no expiry.

The body contains a `readings` list with 1–500 items. Each item has these fields:

| Field         | Type     | Rules                                                                                                        |
| ------------- | -------- | ------------------------------------------------------------------------------------------------------------ |
| `event_id`    | string   | 1–64 ASCII letters, digits, periods, underscores, colons, or hyphens.                                        |
| `sensor_id`   | string   | Same format as `event_id`.                                                                                   |
| `observed_at` | datetime | A timestamp with a timezone that stays within years 1–9999 after UTC conversion. Stored and returned in UTC. |
| `kind`        | string   | `temperature` or `humidity`.                                                                                 |
| `value`       | number   | Finite. Pydantic also accepts numeric strings. Booleans are rejected.                                        |
| `unit`        | string   | 1–16 characters. Unsupported units are quarantined during processing.                                        |

Unknown fields are rejected. Structurally invalid readings reject the entire request before storage.
Validation errors include `loc`, `msg`, and `type`, but do not echo the raw input.
Old or future timestamps are accepted. There is no sensor registry or freshness check.

The idempotency comparison uses a SHA-256 hash of the validated, canonical JSON payload.
JSON object key order, equivalent timezone offsets, and integer versus float notation do not change the hash.
Positive and negative zero use the same canonical value.
Reading order, unit spelling, and all other field values do affect the hash.
For example, `176 F` and `80 C` are different submissions even though they produce the same normalized temperature.

An identical retry returns the batch's current state, including `completed` if it has already run.
A changed payload with the same key returns `409`. The original batch stays unchanged.
A new key creates an independent batch, even when its readings match an earlier batch.

## Pipeline version 1

The processing order is duplicate detection, unit normalization, range validation, severity scoring, and report assembly.

- Duplicate detection uses `event_id` within one batch, not across batches or sensors.
- The first occurrence reserves its ID, even if its value or unit is invalid.
- Later occurrences are quarantined.
- Units have leading and trailing whitespace removed and are converted to uppercase.
- Temperature accepts `C` and `F`. Fahrenheit converts through `(F - 32) * 5 / 9`.
- All normalized temperatures are rounded to six decimal places before range validation and scoring.
- Temperature values from `-100` through `150` C are valid.
- Humidity accepts `%` and values from `0` through `100`, inclusive. Humidity values are not rounded.
- Unsupported units and out-of-range values are quarantined rather than failed batches.

### Severity rules

| Reading     | Normal               | Warning                       | Critical        |
| ----------- | -------------------- | ----------------------------- | --------------- |
| Temperature | Below `60 C`         | At least `60 C`, below `80 C` | At least `80 C` |
| Humidity    | `20–80 %`, inclusive | Below `20 %` or above `80 %`  | Never           |

These are fixed demo rules, not validated industrial safety thresholds.

### Reports

`pipeline_version` is `"1"`. Reports include `total`, `accepted`, `quarantined`, and all three `severity_counts` keys.
Only accepted readings contribute to severity counts.
`total = accepted + quarantined` and `accepted = sum(severity_counts.values())`.

`readings` contains the accepted readings in input order, with normalized units, a severity, and a reason.
`quarantine` contains each rejected reading's zero-based input `index`, `event_id`, and reason.
An all-quarantined batch still completes successfully.

## Persistence and failure behavior

`SIGNAL_FORGE_DB` selects the SQLite file. The default is `var/signal-forge.sqlite3`, relative to the process working directory.
The application creates parent directories and the schema at startup. In-memory SQLite is not supported.

Each operation opens and closes its own database connection.
SQLite uses write-ahead logging and a five-second lock timeout.
Submission uses `BEGIN IMMEDIATE` to make the key check and insert one transaction.
Concurrent submissions with the same key cannot create multiple batches.

Processing also uses one `BEGIN IMMEDIATE` transaction.
The transaction includes pipeline execution, the report write, and any alert outbox insert.
A unique batch ID in the outbox prevents duplicate deliveries.
A completed batch returns its saved report instead of rerunning the pipeline.
This prevents duplicate processing by concurrent requests against the same file.
An exception or process exit before commit leaves the batch pending.
Reports, deliveries, keys, and timestamps survive an application restart.

A saved report keeps the pipeline version used for that run.
A future code change does not rescore completed batches automatically.

## Durable alert delivery

A completed batch creates one delivery if its accepted readings include a warning or critical
severity. Normal-only and all-quarantined batches create none. An exception during processing
or either persistence write rolls back both the report and delivery. Running a completed
batch returns its saved report without creating another delivery. Existing completed batches
from before this feature are not backfilled.

`GET /v1/batches/{batch_id}/delivery` returns this shape:

```json
{
  "id": "delivery-uuid",
  "batch_id": "batch-uuid",
  "payload": {
    "delivery_id": "delivery-uuid",
    "batch_id": "batch-uuid",
    "pipeline_version": "1",
    "severity_counts": {"normal": 1, "warning": 1, "critical": 1}
  },
  "status": "pending",
  "attempt_count": 0,
  "next_attempt_at": "2026-10-23T12:00:00Z",
  "last_error": null
}
```

The UUIDs above are placeholders. Unknown batches, pending batches, and completed batches
without an alert return `404` on this route. Invalid UUIDs return `422`.
The payload is saved once and sent unchanged across retries with its delivery ID in the
outbound `Idempotency-Key` header. It contains summary counts only, with all three severity keys.

`POST /v1/deliveries/dispatch` accepts a JSON object `{"limit":10}`; `{}` uses the default 10.
`limit` must be an integer from 1 through 100. Invalid limits, unknown fields, and a missing
body return `422`. Callers cannot supply a webhook URL. The response contains integer counts:

```json
{"attempted": 2, "succeeded": 1, "retried": 1, "dead": 0, "stale": 0}
```

`attempted` counts sends started by this call. The other counts sum to `attempted`.
`stale` means a newer claim or expiry recovery superseded this worker's result, which
was discarded. An empty queue or only future retries returns all zeroes.
Each call sends at most `limit` requests and considers each delivery at most once.
There is no background loop or automatic dispatch on batch completion.

`SIGNAL_FORGE_WEBHOOK_URL` must contain an HTTPS URL with a host. Missing or invalid
configuration returns `503` with a generic message before claims or attempts are consumed.
The configured URL is read for each dispatch call and is never saved in the queue or returned.
Requests use a ten-second timeout for each HTTP operation, disable redirects and environment
proxy settings, and ignore receiver bodies. Response bodies and exception text are never
persisted. Errors contain only `HTTP <status>` or `Network failure` (or the final claim-expiry
message below), preventing credential-bearing URLs from appearing in API errors.

| Status | Meaning |
| --- | --- |
| `pending` | Waiting for its initial send or next retry time. |
| `in_flight` | A worker holds a claim for a reserved attempt. |
| `succeeded` | Any `2xx` response was received. Retained for inspection. |
| `dead` | Terminal response, exhausted retries, or expired final attempt. Retained for inspection. |

Network exceptions, `429`, and `500–599` responses retry after 1, 2, 4, and 8 seconds,
measured from completion of the failed attempt. After five failed attempts the delivery is
`dead`. Other non-2xx statuses, including redirects, become `dead` immediately.
Terminal records have `next_attempt_at: null`; success also clears `last_error`.
There is no automatic retry or redrive of terminal records.

Claims reserve an attempt and expire after 60 seconds. A short SQLite write transaction
selects a due record and assigns a unique claim identity. HTTP runs after that transaction
commits. Another dispatcher cannot claim an active record. After expiry, a dispatcher can
recover it, including after process restart. Each reservation consumes an attempt even if
its worker stops before sending or writing a result. An expired fifth reservation is marked
`dead` with `Claim expired after final attempt` on a later dispatch; this maintenance does
not count as an HTTP attempt in that call's response. Result writes check claim identity,
so an old worker cannot overwrite a newer claim or its result.

Delivery is **at least once**, not exactly once. A worker can stop after the receiver accepts
the request but before SQLite records success. A request that outlives its lease can overlap
with a recovery attempt. The receiver must deduplicate stable delivery IDs. Dispatch requests
run synchronously and may take time proportional to the limit and network operation timeouts.

## Source map

- `src/signal_forge/models.py`: request and response contracts.
- `src/signal_forge/pipeline.py`: deterministic normalization, quarantine, and scoring.
- `src/signal_forge/storage.py`: SQLite schema, idempotency, and atomic processing.
- `src/signal_forge/delivery.py`: bounded dispatch and HTTPS sender.
- `src/signal_forge/api.py`: application factory, HTTP routes, and error mapping.
- `tests/test_pipeline.py`: rule and boundary tests.
- `tests/test_api.py`: API, persistence, rollback, and concurrency tests.
- `tests/test_delivery.py`: outbox, retry, transport, and claim acceptance tests.
- `examples/batch.json`: synthetic demonstration input.

## Limits

This is an agent exercise, not a production monitoring service.
There is no authentication, tenant isolation, body-byte limit, retention policy, rate limit, or database migration system.
The database has one writer at a time. Pipeline execution holds the write lock and blocks unrelated writes.
Request bodies and responses stay in memory. The API accepts at most 500 readings per batch.
There is no batch list route, automatic worker, or external event source.

The documented launch command binds only to `127.0.0.1`.
Local processing and tests need no network after dependency installation.
Manual webhook dispatch requires access to the configured HTTPS receiver.
The interactive documentation can load JavaScript and CSS from a CDN.
