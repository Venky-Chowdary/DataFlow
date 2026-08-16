# DataFlow / Datawrap — PRD & Working Memory

## Problem statement (verbatim intent)
Deep principal-architect audit of the DataFlow universal data-movement platform
on a new branch (`feature/Venkat-Analysis`). Enterprise-grade, production-ready,
brutal real implementation (no POC/mock), tighten algorithms, follow the
`.cursor/rules`, test completely, prove with artifacts, push to a new branch.

## Product
Universal one-click data transfer: File↔DB, DB↔DB, DB↔File, API→DB, with AI
semantic mapping, fail-fast preflight gates (G1–G15), quarantine/replay,
checksum reconcile, signed contracts, Debezium-class CDC (at-least-once upsert).
Monorepo: `apps/api` (FastAPI), `apps/web` (React 19), `apps/cli`,
`packages/preflight`, `packages/ml`.

## Architecture / scale
- ~1,548 Python files, ~446K LoC, 15,864 collected tests.
- Map SSOT: `services.semantic_mapper.map_columns`. Dest-exists:
  `services.shape_contract.classify_dest_exists_shape`. Preflight engine:
  `packages/preflight` + `services/preflight_rules.py` (G1–G15).

## Standards (from .cursor/rules)
Zero silent data loss; one bug ⇒ fix the algorithm for the whole matrix; claims
need artifacts (pass/fail/skip, run_id, reconcile checksum); catalog count ≠
live; CDC default is at-least-once upsert.

## Done this session (2026-06)
- Stood up real PG(logical)/Mongo(rs0)/Redis/DuckDB for live proof.
- Honest baseline: 14,553 pass / 123 fail / 1,306 skip (parallel); 76 fail serial
  (xdist isolation inflates the count).
- FIXED: missing `pydantic-settings` dep (boot); MongoClient cache poisoning
  (+3 regression tests); `tsv` capability profile; `fetch_scan_page`
  AttributeError fallback (+7 CDC tests); stale `total_gates` golden 11→13
  (+8 tests); DuckDB dialect env; completed MongoClient patch-target migration.
- Full audit: `docs/AUDIT_FEATURE_VENKAT_ANALYSIS.md`.

## Prioritized backlog (real gaps, not env/driver)
- P0 Locale/currency auto-normalization fidelity (locale-aware, golden matrix,
  preflight risk surface) — no silent US/EU separator corruption.
- P0 Hostile-identifier quoting in Oracle/MSSQL snapshot readers
  (double-quote escaping + ROW_NUMBER keyset windowing).
- P1 PRODUCTION_SKU ↔ LIVE_MATRIX honesty reconciliation (drop or prove
  sqlserver routes).
- P1 Parallel-safe test isolation (autouse fixtures / singletons) so `pytest -n`
  is trustworthy in client CI.
- P2 Direct-write create-new consistency (`pending_dest_schema` materialization
  outside `execute_tracked`).
- P2 Refresh drifted golden/precondition tests (bigquery_array_json symbol,
  data_integrity_p0 label, module_size budget).
- P2 Relax `packages/preflight` `requires-python` 3.12 pin vs 3.11 runtime.

## Env / services
See `memory/test_credentials.md`.

## Code-review follow-up (2026-06)
- FIXED + testing_agent-verified: stripped UTF-8 BOM from 6 test files
  (32 pass, 88 regression pass, 0 issues).
- Validated the rest of the 20/100 scanner report as FALSE POSITIVES:
  cdc_lease_store 'eval' = Redis EVAL(Lua); procedure_source 'exec' = SQL
  EXEC/cursor.execute; engine_checksum md5 = Postgres SQL md5() reconciliation
  (non-crypto); 2 'circular imports' already broken by lazy/function-local
  imports; 'insecure random' = deterministic synthetic-data/jitter. No
  production Python eval/exec or security-MD5 exists.
- Out of scope / unsafe on a client-bound branch: blindly refactoring 4527
  "high-complexity" functions, splitting 100+-import files, and "fixing" 378
  guarded try/except-ImportError "undefined vars" — would degrade a working
  99%-passing suite. Tracked as future refactor, not hot-patched.
