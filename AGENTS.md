# AGENTS.md — DataFlow / Datawrap engineering notes

## Running API tests
- Run from `apps/api`: `python -m pytest tests/<file>.py -q -p no:cacheprovider`
- No `pytest-xdist`; large slices take minutes — run in the background.
- In-process engine e2e tests (no services): set `DATAFLOW_JOB_STORE=memory`,
  `DATAFLOW_DISABLE_OBJECT_STORE=1`, patch `src.transfer.engine.get_mongodb_service`
  with `tests.test_property2_golden_path_never_blocked._FakeMongo`, and drive
  `UniversalTransferEngine().execute_tracked(request, job_id)` on SQLite files.
  Pattern: `tests/test_acc02_composite_watermark_second_run.py`.
- `_FakeMongo` lacks `update_job_fields`; subclass it when a path needs it.

## Local environment facts (this workstation)
- MongoDB on `localhost:27017` (`df-mongo`) runs as a single-node replica
  set `rs0` — required: snapshot reads/change streams decline on a
  standalone mongod. Recreated 2026-10-10 to match
  `.github/scripts/start_ci_databases.sh`; `mongo-rs1` also runs on 27018.
- PostgreSQL (`df-pg`), MySQL (`dataflow-ci-mysql`), Azurite, fake-gcs and
  Redis (`df-redis`) are live in docker; live pair tests run.
- SQL Server 1433, Oracle 1521 are not running locally — those legs skip.
- `pymssql` is installed in `.venv`.

## Known pre-existing failures (fail identically on the base branch)
- None currently: the elasticsearch decimal, oracle ''-bind and pymssql
  drivername tests all pass as of `e9713a323`. Re-verify against a
  `git stash` baseline before calling a new failure a regression.

## Verification habit
- For a defect fix, prove the new test fails on the pre-fix code (temporarily
  `git show HEAD:<file> > <file>`, run, restore) before claiming the fix.
