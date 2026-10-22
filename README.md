# Signal Forge

Build a local sensor-event workflow: submit a batch, run its processing pipeline, and inspect the alert report.
The sample uses synthetic kiln, greenhouse, and lab readings. It makes no external API calls and needs no secrets.

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

## Check the baseline

```sh
uv run --locked pytest
uv run --locked ruff check src tests
uv run --locked ruff format --check src tests
```

Tests use temporary databases and synthetic data. They do not contact external services.

## Test a coding agent

Give the next agent this prompt:

> Work only in `signal-forge/`. Read `README.md`, `API.md`, and `AGENT_TASK.md`. Run the baseline checks, then implement the durable alert-delivery task in `AGENT_TASK.md`. Preserve the existing API behavior. Add tests for failures, restarts, and concurrent dispatch. Report the changed files and the checks that passed.

The baseline is complete. `AGENT_TASK.md` describes a new feature, not an intentionally hidden defect.

For the current contract, storage rules, and known limits, read [API.md](API.md).
