# Add durable alert delivery

Extend the current pipeline with a persistent webhook queue.
A webhook is an HTTP request that this service sends to another service when a batch produces an alert.
Keep the current routes and processing rules compatible.

## Establish the baseline

1. Read `README.md` and `API.md`.
2. Run the tests, Ruff lint, and Ruff format check.
3. Inspect `Store.run()` before changing its transaction boundary.

Work only in `signal-forge/`. Use `uv` and the locked dependencies.
Do not add Redis, a broker, or a separate database.
Do not send test requests to real external services.

## Implement the queue

1. Add an SQLite outbox table.
   An outbox is a queue stored in the same database transaction as the report that creates it.
2. Create one delivery per completed batch with at least one warning or critical reading.
3. Commit the delivery and report together.
   A processing failure must persist neither result.
4. Prevent duplicate deliveries when a completed batch runs again or two requests run it concurrently.
5. Store a stable delivery ID, batch ID, JSON payload, status, attempt count, next attempt time, and last error.
6. Keep successful deliveries for inspection.

Use one summary payload per batch, not one request per reading.
Include the stable delivery ID, batch ID, pipeline version, and severity counts.

## Add inspection and dispatch

Add these routes:

- `GET /v1/batches/{batch_id}/delivery`: return the delivery, or `404` when none exists.
- `POST /v1/deliveries/dispatch`: attempt a bounded number of due deliveries and return outcome counts.

Dispatch runs only when a client calls the route. Do not start a background loop.
Set a maximum of 100 attempts per dispatch request.
Reject invalid limits with `422`.
Return `503` without consuming attempts when the webhook URL is not configured.

Read the webhook URL from `SIGNAL_FORGE_WEBHOOK_URL`.
Do not let callers choose a URL in a request body.
Require HTTPS, set a finite request timeout, and do not follow redirects.
Send the stable delivery ID as the outbound `Idempotency-Key`.
The receiver uses this key to ignore repeated delivery attempts.

Treat any `2xx` response as success.
Retry network failures, `429`, and `5xx` responses.
Treat other status codes, including redirects, as terminal failures.
Use delays of 1, 2, 4, and 8 seconds after retryable failures.
After five failed attempts, mark the delivery as dead.
Keep status names and response shapes explicit in the API documentation.

Do not hold an SQLite write transaction open during an HTTP request.
Use a claim with an expiry time to stop concurrent dispatch calls from sending the same active delivery.
An expired claim must be recoverable after a worker process stops.
Protect updates with the claim's identity so an old worker cannot overwrite a newer worker's result.

Delivery is at least once: a process can stop after the receiver accepts a request but before the local success write.
Do not claim exactly-once HTTP delivery.

## Verify acceptance cases

Use an injected clock or equivalent test control. Do not make retry tests sleep.
Use an HTTP mock transport or an injected sender. Tests must run without network access.

Add checks for all of these cases:

- A normal-only batch creates no delivery.
- A warning or critical batch creates exactly one delivery.
- An all-quarantined batch creates no delivery.
- Repeated and concurrent runs create no duplicate delivery.
- A pipeline failure leaves no report or delivery.
- Pending deliveries survive an application restart.
- A `2xx` response marks a delivery as successful.
- Retryable failures follow the specified schedule and stop after attempt five.
- A terminal response does not retry.
- A future retry is not dispatched early.
- Concurrent dispatch calls respect the claim.
- Expired claims recover, and stale workers cannot overwrite a newer claim.
- The payload and idempotency key stay stable across retries.
- Missing configuration consumes no attempts.
- The API never exposes webhook credentials in responses or stored error messages.

Run all existing checks after the change. Update `API.md` and the README where behavior changed.
Report changed files, checks that passed, and any unverified behavior.

## Optional extensions

Do not implement these unless requested:

- Cursor pagination for batch history.
- Configurable scoring rules with versioned snapshots.
- Request authentication and tenant-scoped idempotency keys.
- Metrics for queue depth and delivery latency.
