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

## M4 source-only SDK connectors

The GitHub, Jira Cloud, and Intercom manifests are registered SDK sources and have been certified as SDK source on synthetic fixtures; not yet wired into the transfer engine. Each remains planned and unexposed in the transfer catalog. M4 did not change catalog, capability, form-schema, or transfer-engine wiring.

- **GitHub** — docs: [GitHub REST API](https://docs.github.com/en/rest). `repositories` is a full-refresh stream; `issues` uses `since` on `updated_at`. The issues endpoint also returns pull requests, which are retained, including the `pull_request` object.
- **Jira Cloud** — docs: [Jira Cloud REST API v3 issue search](https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-search/) and [project search](https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-projects/). Issue search uses a 60-second-or-greater lookback because JQL timestamps have minute granularity; incremental delivery is at-least-once.
- **Intercom** — docs: [Intercom contacts](https://developer.intercom.com/docs/references/rest-api/api.intercom.io/contacts) and [conversation search](https://developers.intercom.com/docs/references/rest-api/api.intercom.io/conversations/searchconversations). Conversation incremental reads use epoch-second cursors with a one-second overlap and are at-least-once.

The connector lifecycle matrix covers the four previously registered SDK connectors and these three M4 connectors:

| Connector | Spec | Check | Bad credentials | Discover | Full read | Incremental | Resume after failure | Rate limit | Schema drift |
|---|---|---|---|---|---|---|---|---|---|
| `declarative_http` | pass | pass | pass | pass | pass | pass | skip — legacy spec has no pagination keys; single-page source; recovery is a full re-read of that page; use a DeclarativeSource manifest for paginated APIs | pass | pass |
| `declarative_source` | pass | pass | pass | pass | pass | pass | pass | pass | pass |
| `hubspot_cdk` | pass | pass | pass | pass | pass | pass | pass | pass | pass |
| `singer_tap` | pass | pass | pass | pass | pass | pass | pass | skip — the tap owns HTTP; the bridge has no request layer | pass |
| `github` | pass | pass | pass | pass | pass | pass | pass | pass | pass |
| `jira` | pass | pass | pass | pass | pass | pass | pass | pass | pass |
| `intercom` | pass | pass | pass | pass | pass | pass | pass | pass | pass |

The two skips above are existing, descriptor-authorized limitations; M4 adds no skips. The optional GitHub live smoke is separate from this synthetic matrix and runs only when `GITHUB_TOKEN` is set and the two-second `api.github.com` probe succeeds. It reads one page through the GitHub manifest and is not live evidence unless actually run against the vendor.

For M5 planning, `sdk_read_as_matrix()` currently stops after the first `RecordBatch`; M4 intentionally leaves that transfer-engine helper unchanged.

## Writing a report

Tests never write proof artifacts. To deliberately generate the JSON report at `apps/api/data/proofs/connector_certification.json`, run from `apps/api`:

```bash
python scripts/connector_certification_report.py --write-report
```

Without `--write-report`, the script prints the report to stdout and does not create or modify the artifact.
