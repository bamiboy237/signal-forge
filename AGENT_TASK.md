# Add a versioned alert rules engine

The pipeline scores every reading with thresholds that are written into `pipeline.py`.
Replace them with rule sets that users write, version, activate, and test against past batches.
Keep every current route, response shape, and processing result compatible.

`tests/test_rules.py` is the acceptance test for this task.
Make it pass without editing it.
The existing tests must also keep passing.

## Establish the baseline

1. Read `README.md`, `API.md`, `src/signal_forge/pipeline.py`, and `src/signal_forge/storage.py`.
2. Run the tests, Ruff lint, and the Ruff format check:

   ```sh
   uv run --locked pytest
   uv run --locked ruff check .
   uv run --locked ruff format --check .
   ```

3. Confirm that `tests/test_rules.py` fails before you change any code.

Work only in this repository. Use `uv` and the locked dependencies.
Do not add a dependency. Do not add a parser generator.
Do not use `eval`, `exec`, `compile`, or the `ast` module to evaluate rules.
Commit after each section below, so that the history shows your progress.

## Define a rule

A rule has four fields. Reject any other field.

| Field | Type | Meaning |
| --- | --- | --- |
| `id` | Identifier, the same pattern as `sensor_id` | Unique within its rule set. |
| `when` | String, 1 to 500 characters | An expression in the rule language below. |
| `severity` | `normal`, `warning`, or `critical` | The severity of a matching reading. |
| `reason` | String, 1 to 200 characters | The reason text for a matching reading. |

A rule set has 1 to 50 rules and an optional `note` of up to 200 characters.

## Implement the rule language

Write a tokenizer, a parser, a type checker, and an evaluator.

### Grammar

```text
expression := or
or         := and ("or" and)*
and        := not ("and" not)*
not        := "not" not | comparison
comparison := term (operator term)? | term "matches" string
operator   := "==" | "!=" | "<" | "<=" | ">" | ">="
term       := number | string | field | function | "(" expression ")"
field      := "kind" | "sensor_id" | "unit" | "value"
function   := "avg" "(" "value" "," integer ")" | "delta" "(" "value" ")"
number     := "-"? digits ("." digits)?
string     := '"' characters '"'
```

- `not` binds tighter than `and`, and `and` binds tighter than `or`.
- Keywords, fields, and function names are lowercase.
- Whitespace separates tokens and has no other meaning.
- A string uses double quotes. Inside a string, `\"` is a quote and `\\` is a backslash.

### Types

- `kind`, `sensor_id`, and `unit` are strings.
- `value`, `avg(...)`, and `delta(...)` are numbers.
- `==` and `!=` compare two values of the same type.
- `<`, `<=`, `>`, and `>=` compare two numbers.
- `matches` takes a string on the left and a string literal on the right.
- An expression must be a boolean. A lone term such as `value` is not valid.

### Meaning

- `value` is the normalized value from the current pipeline: Celsius for temperature and percent for humidity.
- `matches` is a glob match on the whole string. `*` matches any run of characters, including none. `?` matches exactly one character. Every other character matches itself.
- `avg(value, n)` is the mean value of the last `n` accepted readings for the same `sensor_id` and `kind`, ending with the current reading. `n` is an integer from 1 to 20. When fewer than `n` readings exist, the result is null.
- `delta(value)` is the current value minus the value of the previous accepted reading for the same `sensor_id` and `kind`. For the first reading, the result is null.
- Order the readings for `avg` and `delta` by `observed_at`, then by position in the batch. Only accepted readings count. Quarantined readings never count.
- Any comparison with a null operand is false. `not` applies to the result as usual, so `not delta(value) > 1` is true for the first reading.

### Evaluation

1. Evaluate the rules of a rule set in order.
2. The first rule whose `when` is true sets the reading's severity and reason.
3. When no rule matches, the severity is `normal` and the reason is `No rule matched.`
4. A reason can contain the placeholders `{sensor_id}`, `{unit}`, and `{value}`. Replace each one. Render `{value}` with `format(value, "g")`, so `80.0` becomes `80`.

### Validation

Validate a rule set when it is created. Reject an invalid rule set with `422` and the existing error shape.

- Put the error on the field that caused it. For example, an invalid `when` in the second rule has `loc` equal to `["body", "rules", 1, "when"]`, and an unknown placeholder has `loc` ending in `"reason"`.
- Duplicate rule IDs have `loc` equal to `["body", "rules"]`.
- A syntax error message includes `at position N`. `N` is the 1-based character position of the token where the error starts.
- Report unknown fields, unknown functions, type errors, and an `avg` window outside 1 to 20 as errors.

## Store and version rule sets

1. Add an SQLite table for rule sets. Number the versions 1, 2, 3, and so on.
2. A rule set never changes after it is created.
3. Exactly one version is active.
4. On a new database, create version 1 as active. Version 1 reproduces the current thresholds exactly, with these rules in this order:

   | `id` | `when` | `severity` | `reason` |
   | --- | --- | --- | --- |
   | `temp-critical` | `kind == "temperature" and value >= 80` | critical | `Temperature is at or above 80 C.` |
   | `temp-warning` | `kind == "temperature" and value >= 60` | warning | `Temperature is at or above 60 C and below 80 C.` |
   | `temp-normal` | `kind == "temperature"` | normal | `Temperature is below 60 C.` |
   | `humidity-low` | `kind == "humidity" and value < 20` | warning | `Humidity is below 20 %.` |
   | `humidity-high` | `kind == "humidity" and value > 80` | warning | `Humidity is above 80 %.` |
   | `humidity-normal` | `kind == "humidity"` | normal | `Humidity is between 20 % and 80 %, inclusive.` |

5. Keep the version history and the active version across app restarts.

## Run batches with the active version

1. Running a pending batch uses the version that is active at that moment.
2. Add `ruleset_version` to the pipeline report. Keep `pipeline_version` at `"1"`.
3. Running a completed batch again returns its stored report, even after another version becomes active.
4. Keep the current unit conversion, range checks, duplicate handling, and quarantine rules unchanged.
5. Keep `run_pipeline(batch)` working with one argument. It scores with the version 1 rules, so `tests/test_pipeline.py` passes unchanged.
6. Keep the webhook outbox behavior unchanged.

## Add the routes

| Route | Result |
| --- | --- |
| `GET /v1/rules` | All versions, oldest first. |
| `GET /v1/rules/active` | The active version. |
| `POST /v1/rules` | `201` and a new, inactive version, with a `Location` header of `/v1/rules/{version}`. |
| `GET /v1/rules/{version}` | One version, or `404` with `{"detail": "Rule set not found"}`. |
| `POST /v1/rules/{version}/activate` | `200` and the version, now active. `404` when it does not exist. |
| `POST /v1/rules/{version}/replay` | A dry run of the version against completed batches. See below. |

Each version has this shape:

```json
{
  "version": 2,
  "rules": [{"id": "hot", "when": "value > 90", "severity": "critical", "reason": "Too hot."}],
  "note": "Tighter kiln limits",
  "created_at": "2026-10-23T12:00:00Z",
  "active": false
}
```

### Replay

The body is `{"limit": N}`. `limit` is a strict integer from 1 to 100, and its default is 20.
Replay re-scores the stored input of the most recently completed batches, newest first, with the given version.
It changes nothing: stored reports, deliveries, and the active version stay as they are.

```json
{
  "version": 2,
  "batches": [
    {
      "batch_id": "...",
      "ruleset_version": 1,
      "before": {"normal": 3, "warning": 1, "critical": 0},
      "after": {"normal": 2, "warning": 1, "critical": 1},
      "changes": [{"event_id": "evt-4", "before": "normal", "after": "critical"}]
    }
  ]
}
```

- `ruleset_version` is the version that produced the stored report.
- `changes` lists only readings whose severity differs, in batch order.
- Return `404` for an unknown version and `422` for an invalid limit.

## Document the work

1. Document the rule language, the routes, and the replay result in `API.md`.
2. Add a short walkthrough to `README.md`: create a version, replay it, then activate it.
3. Use examples that run against the local API.

## Finish

1. Run the full test suite, Ruff lint, and the Ruff format check. All must pass.
2. Make sure `tests/test_rules.py` is unchanged.
3. Commit the final work.
