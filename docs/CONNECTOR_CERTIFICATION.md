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

GitHub, Jira Cloud, and Intercom are registered SDK sources, routed through the transfer engine, and certified against synthetic fixtures. They are source-only: catalog enrichment reports `source_ready=true`, `dest_ready=false`, `transfer_ready=false`, `effective_status=beta`, and `certification_tier=source_only`. Transfer capabilities are full-refresh-only (`incremental=false`); descriptors retain manifest-declared sync modes. Synthetic routing and catalog evidence is in `apps/api/tests/test_gconn_sdk_transfer_routing.py`.

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

The two skips above are existing, descriptor-authorized limitations. The optional GitHub live smoke is separate from this synthetic matrix and runs only when `GITHUB_TOKEN` is set and the two-second `api.github.com` probe succeeds. It reads one page through the GitHub manifest and is not live evidence unless actually run against the vendor.

The SDK engine E2E coverage is synthetic. GitHub and Jira prove page-fault recovery by a full reread with primary-key upsert; Intercom's fresh-destination three-page read is covered separately. Live vendor compatibility, quotas, real-volume behavior, and destination-role readiness are not established.

## Known engine gaps (G-CONN M5)

(a) **Existing-destination retry can remap epoch integers to timestamps.** On an Intercom retry, the bridge and stream retain `created_at` as declared `INTEGER` / `BIGINT` with the epoch value `1767225500`, but existing-destination mapping selects `TIMESTAMP` and `datetime`; the transformed UTC `Z` value is rejected by SQLite's NTZ validator. The existing-destination mapping call is in `apps/api/src/transfer/engine.py:2049-2074` and `_auto_map` dispatch in `apps/api/src/transfer/engine.py:4285-4291`; transform inference is in `apps/api/services/mapping_pipeline.py:1218-1225`, application in `apps/api/connectors/writer_common.py:1819`, epoch conversion in `apps/api/services/transform_engine.py:838-839`, and NTZ refusal in `apps/api/connectors/writer_common.py:3415-3419`. Trace: `/home/ubuntu/gconn/logs/m5_intercom_type_trace_second.txt`. This is not SDK-specific: another source with epoch `*_at` integer values rerun against an existing `TEXT`/`BIGINT` destination may hit the same remap class. The defect is not fixed or verified.

(b) **SDK `resume=True` fails strict Gate-8 reconciliation.** A resumed SDK read uses the saved page-two state, but `write_pass_fingerprints` covers only that resumed session while Gate-8 compares the whole destination population. SDK drivers are not in the independent reread set. The resumed-digest behavior is documented at `apps/api/src/transfer/stream.py:2072-2075` and `3819-3823`; the fallback digest is at `apps/api/src/transfer/stream.py:4033-4038`; SDK exclusions are in `apps/api/services/source_reread.py:49-63`. Trace: `/home/ubuntu/gconn/logs/m5_sdk_transfer_e2e_fifth.txt`. This gap is not fixed or verified.

The supported recovery for a page failure is a full reread with primary-key upsert, rather than checkpoint resume; GitHub/Jira synthetic engine tests exercise that path. Intercom's retry may still be blocked by gap (a).

## Writing a report

Tests never write proof artifacts. To deliberately generate the JSON report at `apps/api/data/proofs/connector_certification.json`, run from `apps/api`:

```bash
python scripts/connector_certification_report.py --write-report
```

Without `--write-report`, the script prints the report to stdout and does not create or modify the artifact.
