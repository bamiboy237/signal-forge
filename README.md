# Signal Forge

Build a local sensor-event workflow: submit a batch, run its processing pipeline, and inspect the alert report.
The sample uses synthetic kiln, greenhouse, and lab readings. Processing runs locally.
Optional manual dispatch sends alert summaries to a configured HTTPS webhook.

## Run the API

Use Python 3.14 and [uv](https://docs.astral.sh/uv/).
Run each command from `signal-forge/`, not the parent workspace.

1. Install the locked dependencies:

   ```sh
   uv sync --locked
   ```

2. Start the API on the local machine:

   ```sh
   uv run --locked uvicorn signal_forge.api:create_app --factory --app-dir src --host 127.0.0.1 --port 8000
   ```

3. Open <http://127.0.0.1:8000/docs> for the interactive API.

4. In another terminal, check the API and its database:

   ```sh
   curl --fail-with-body http://127.0.0.1:8000/health
   ```

   Expect `{"status":"ok"}`.

## Process the sample batch

1. Submit the supplied JSON file:

   ```sh
   curl --fail-with-body -i http://127.0.0.1:8000/v1/batches \
     -H 'Content-Type: application/json' \
     -H 'Idempotency-Key: sample-001' \
     --data-binary @examples/batch.json
   ```

   Expect `202 Accepted`, a batch with status `pending`, and a `Location` header.
   Submission stores the batch. It does not run the pipeline.

2. In `/docs`, expand `POST /v1/batches/{batch_id}/run`.
   Select **Try it out**.
   Enter the `id` from the submission response.
   Select **Execute**.

   Expect five input readings, three accepted readings, and two quarantined readings.
   The severity counts are one `normal`, one `warning`, and one `critical`.
   A quarantined reading stays in the report with a reason, but it does not produce a score.

3. Use `GET /v1/batches/{batch_id}/report` to read the saved result.
   Running the completed batch again returns that same result.

4. Repeat the original submission with the same key and data.
   Expect `200 OK` and the same batch ID.
   Change a reading but keep the same key to receive `409 Conflict`.

5. Stop and restart the API.
   Retrieve the batch again to confirm that SQLite kept its state.

## Run checks

```sh
uv run --locked pytest
uv run --locked ruff check src tests
uv run --locked ruff format --check src tests
```

Tests use temporary databases and synthetic data. They do not contact external services.

## Deliver alerts

A completed batch with accepted warning or critical readings creates one durable summary
in the same SQLite transaction as its report. Normal-only and all-quarantined batches
create no delivery. Inspect it at `GET /v1/batches/{batch_id}/delivery`.

Set `SIGNAL_FORGE_WEBHOOK_URL` to your receiver's HTTPS URL in the API process environment.
Then explicitly dispatch up to ten due deliveries:

```sh
curl --fail-with-body http://127.0.0.1:8000/v1/deliveries/dispatch \
  -H 'Content-Type: application/json' --data '{"limit":10}'
```

There is no background worker. Call dispatch again when retries become due.
The limit is 1–100. Missing or invalid HTTPS configuration returns `503` without attempts.
Network failures, `429`, and `5xx` retry after 1, 2, 4, and 8 seconds; the fifth failed
attempt is dead. Other non-2xx responses, including redirects, are immediately dead.
Successful and dead records remain available for inspection across restarts.

Delivery is at least once. Receivers should deduplicate the stable `Idempotency-Key`.
Expiring claims recover interrupted workers; an interrupted request may have reached
the receiver even when the local result was never recorded. See the API reference
for claim expiry and attempt accounting.

For the current contract, storage rules, and known limits, read [API.md](API.md).
