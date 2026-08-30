# Quality and Accuracy — measured, not claimed

Every number here is a command anyone can re-run in this repository. Where a
service was unreachable the tests skip and are reported as skips; a skip is not
a pass. Nothing in this document says the product is globally correct.

Branch: `feature/Venkat-Analysis`. Backend runs use
`.venv/bin/python -m pytest -q -p no:randomly` from `apps/api`.

## 1. Backend suite

| Run | Result |
| --- | --- |
| Before this wave | 15369 passed, 1504 skipped, **37 failed** |
| After the Gate-8 source-count wave | 15425 passed, 1504 skipped, **2 failed** |
| After the module-budget extractions | see `FULL_SUITE` block below |
| Whole suite, team/accounts wave (`pytest tests -q`, 31m) | 16450 passed, 1050 skipped, **8 failed** |

The eight failures in the last run were re-run on the parent commit in a clean
worktree and seven fail there identically, so they are not from that wave:

* `pgvector` destination and `edge_types_csv_to_postgresql` — this host's
  PostgreSQL 16 has no `vector` extension installed
  (`Could not open extension control file .../vector.control`).
* `property8_unicode_form` / `property8_json_polarity` — the assertions encode a
  MariaDB build without `utf8mb4_0900_ai_ci`; this host's MariaDB has it. The
  test states a host fact, not a product fact.
* Three `typed_fidelity_transfer_matrix_e2e` cases — `ts_utc TIMESTAMPTZ →
  DATETIME(6)` is refused as a fidelity collapse on the PostgreSQL→MySQL and
  →Redis typed routes. **This one is a real product defect**, still open: an
  instant landing in an instant carrier should not need a Risk Contract.
* `test_pilot_llm_wave41::test_hybrid_footnote_on_auth_failure` passes on its own
  and in its own file; it only fails inside the whole-suite order, so provider
  state leaks between tests. Open as a test-isolation defect.

The two remaining failures at the second checkpoint were:

* `tests/test_module_size_budgets_f8.py::test_module_size_budgets_script_ok` —
  eight modules had grown past their frozen line budgets. Fixed by extraction
  (§4), not by raising a budget.
* `tests/test_sftp_email_connectors.py::TestEmailConnector::test_write_email_csv_matches_shared_serialize`
  — the product was right and the test's *expectation* was built from a float
  row (`1000.0`), so it asserted the value the delimited-scale fix removed. The
  fixture now serializes the same textual cells the connector maps.

## 2. What the failures were, by cause

Of the 37 failures at the start of this wave:

* **Gate-8 source-count propagation.** Native (Snowflake, BigQuery, MySQL) and
  object-store (S3, GCS, ADLS) writers computed the reader's population but did
  not report it, so reconciliation refused correct loads with
  `source_row_count_unmeasured`. Writers now stamp the reader's count and the
  object-store materializer reports the *spool* population, so quarantined rows
  stay in source accounting instead of being hidden behind a writer ack.
* **Delimited decimal scale.** `10.50` landed as `10.5` in CSV exports: a
  textual source decimal went through JSON-style numeric conversion. Delimited
  output now preserves the source text; JSON output stays typed.
* **Optional Snowflake SQLAlchemy dialect.** An advisory pre-drop spelling probe
  routed through a dialect this environment does not ship and blocked native
  Snowflake routes. The probe is advisory and logs at debug; the write and the
  destination re-read remain fail-closed.
* **Quarantine reason named the wrong carrier.** Snowflake and Oracle spell the
  decimal carrier `NUMBER`, so a reason that said `DECIMAL(38,10)` named a type
  the operator cannot find in their own catalog.
* **Test-harness defect (not product).** Object-store fixtures reset the Moto
  session backend, so later tests saw `NoSuchBucket` depending on order.

## 3. Accuracy

| Measurement | Result |
| --- | --- |
| Mapping golden set | 108 / 108 correct |
| Frontend tests | 510 passed |
| Frontend `tsc --noEmit` | clean |
| CI mypy scope (`decision_kernel`, `type_system`, `type_ddl_specialty`) | no issues, 17 files |
| CI Ruff scope | all checks passed |

"100%" is only ever said about a named fixture. The golden set is 108 named
column pairs; it is not a claim about every schema in the world.

## 4. Module size freeze

`scripts/check_module_size_budgets.py` now reports `ok: true`. Eight modules
were over (~1700 lines); each was extracted into a module named for what it
owns, with the trunk re-exporting every name:

| Trunk | Extracted module |
| --- | --- |
| `connectors/writer_common.py` | `connectors/write_quarantine_exotic.py` |
| `connectors/generic_sql.py` | `connectors/merge_dialects.py` (existing home) |
| `src/transfer/engine.py` | `src/transfer/job_failure.py` |
| `src/transfer/stream.py` | `src/transfer/stream_scd2.py` |
| `src/transfer/adapters.py` | `src/transfer/adapters_introspect.py` |
| `services/preflight_service.py` | `services/preflight_destination.py` |
| `services/semantic_mapper.py` | `services/semantic_abbreviations.py` |
| `services/type_system.py` | `services/type_polarity_invent.py` |

No budget was raised.

## 5. What is still not proven

These stay explicitly unproven, and no UI or document may report them as ready:

* Platform-wide exactly-once. `PLATFORM_EXACTLY_ONCE_CLAIMED = False`.
  Route-scoped exactly-once is live-proven only for PostgreSQL, MySQL, Oracle
  and SQL Server.
* Azure SQL, DuckDB, `generic_sql` and Snowflake exactly-once.
* Long-running logical-replication / binlog loops, slot and binlog restart
  behaviour, the complete snapshot-to-stream handoff, and full incremental
  snapshot-window closure.
* Any route whose service was unreachable in this environment. Those tests skip
  and say why.

## 6. Standing rules these numbers are measured against

* Catalog counts do not prove a migration; only an independent destination
  re-read does.
* A writer acknowledgement is not destination proof.
* Capped key samples are diagnostic, never a scope.
* Unknown metadata stays unknown; unsupported semantics stay unsupported.
* A deterministic gate refusal must not consume a retry budget.
* Full-refresh overwrite must clear the destination or fail closed.
