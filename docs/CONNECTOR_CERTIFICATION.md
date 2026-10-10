# SDK connector certification

The synthetic certification suite runs every registered SDK connector through the same lifecycle:

1. Validate a JSON-serializable connection specification and its required properties.
2. Check a successful connection against a fixture.
3. Reject planted bad credentials without exposing the secret in the returned error or logs.
4. Discover the stream, primary key, cursor, and supported sync modes.
5. Read the exact fixture population with no duplicate primary keys.
6. Read changed rows incrementally and verify that the cursor does not regress.
7. Inject a page failure, write and checkpoint completed pages, then resume from saved state.
8. Retry a 429 response using `Retry-After` and an injected fake sleep.
9. Report schema additions and type changes, and reject removal of primary-key or cursor fields.

Each lifecycle result is recorded as `pass`, `fail`, or `skip`. A skip must be declared in the connector descriptor with a non-empty reason. Undeclared and stale skips fail certification.

## Adding a connector

Register the connector in `connectors/sdk`, attach one immutable `ConnectorDescriptor`, and add a synthetic `CertificationCase` in `tests/connector_certification/cases.py`. The case supplies a fixture-backed connector factory, fixture routes, stream identity, primary key, cursor, and fixture mutations. The shared harness then applies the same lifecycle checks to the new descriptor; a missing case or descriptor drift fails the registry tests.

Only add a descriptor skip when the connector genuinely cannot perform that lifecycle step. Keep the reason specific and actionable. Do not mark a step unsupported merely to avoid implementing or testing a capability.

## Evidence labels and limitations

`synthetic-fixture` means the connector passed against controlled test fixtures; it is not a live-service claim. `live` is reserved for evidence collected against an actual vendor service and should be accompanied by its own reproducible evidence record.

Synthetic certification does **not** prove vendor API compatibility, quota behavior, or performance at real data volumes. It also does not establish exactly-once delivery: page checkpoint recovery is at-least-once, so consumers must tolerate replay of boundary records.

## Writing a report

Tests never write proof artifacts. To deliberately generate the JSON report at `apps/api/data/proofs/connector_certification.json`, run from `apps/api`:

```bash
python scripts/connector_certification_report.py --write-report
```

Without `--write-report`, the script prints the report to stdout and does not create or modify the artifact.
