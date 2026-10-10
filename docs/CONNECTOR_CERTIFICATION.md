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

The SDK engine E2E coverage is synthetic. GitHub and Jira prove page-fault recovery by a full reread with primary-key upsert; Intercom's fresh-destination three-page read and fault-then-full-reread retry are covered separately. SDK `resume=True` is refused before source HTTP or destination writes. Live vendor compatibility, quotas, real-volume behavior, and destination-role readiness are not established.

## Known engine gaps (G-CONN M5)

(a) **Existing-destination retry could remap epoch integers to timestamps — fixed for the verified SQLite path.** SQLite introspection now preserves PRAGMA `declared_type` / `native_type` and sample-refinement provenance; the existing `physical_carriers=True` consumer restores the live destination carrier instead of using a sample-refined type. An existing `TEXT` column therefore maps an integer-epoch `created_at` as `TEXT` / `none`, while first-run mapping remains `INTEGER` / `none`. Explicit operator transforms remain honored. Tests: `test_existing_sqlite_text_epoch_uses_physical_type_for_mapping`, `test_first_sqlite_run_keeps_integer_epoch_mapping`, and `test_sqlite_operator_mapping_override_wins` in `apps/api/tests/test_sqlite_physical_carrier_epoch_mapping.py`; `test_intercom_fault_then_full_reread_retry_uses_pk_upsert` in `apps/api/tests/test_gconn_sdk_transfer_e2e.py`. The original runtime trace is `/home/ubuntu/gconn/logs/m5_intercom_type_trace_second.txt`; focused failing-first evidence is `/home/ubuntu/gconn/logs/gap1_sqlite_mapping_before_fix.txt` and `/home/ubuntu/gconn/logs/gap1_operator_override_before_fix.txt`. This change preserves SQLite's declared carrier without changing its non-physical inferred type. Other dialect paths have not been changed.

(b) **SDK `resume=True` is refused and remains unsupported.** The engine fails the job during validation, before destination writes/DDL or source HTTP, with: `Resume is not supported for <connector> yet; rerun the job — rows are upserted by primary key.` The refusal is covered by `test_github_resume_is_refused_before_http_or_destination_mutation`; the existing SQL resume path remains allowed in `test_engine_stream_sqlite_to_sqlite_resume_from_checkpoint`. The underlying strict Gate-8 limitation remains: a resumed SDK read uses saved page state, but `write_pass_fingerprints` covers only that resumed session while Gate-8 compares the whole destination population, and SDK drivers are not in the independent reread set. The resumed-digest behavior is documented at `apps/api/src/transfer/stream.py:2072-2075` and `3819-3823`; the fallback digest is at `apps/api/src/transfer/stream.py:4033-4038`; SDK exclusions are in `apps/api/services/source_reread.py:49-63`. Diagnostic trace: `/home/ubuntu/gconn/logs/m5_sdk_transfer_e2e_fifth.txt`. No checksum/Gate-8 or `source_reread` behavior was changed.

The supported recovery for a page failure is a full reread with primary-key upsert, rather than checkpoint resume; GitHub/Jira synthetic engine tests exercise that path, and the Intercom retry test now covers it for the verified SQLite destination case.

## Writing a report

Tests never write proof artifacts. To deliberately generate the JSON report at `apps/api/data/proofs/connector_certification.json`, run from `apps/api`:

```bash
python scripts/connector_certification_report.py --write-report
```

Without `--write-report`, the script prints the report to stdout and does not create or modify the artifact.
