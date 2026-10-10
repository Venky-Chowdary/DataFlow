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
- MongoDB 8.x on `localhost:27017` is live (live Mongo tests run).
- A PostgreSQL on `:5432` is running but is NOT the project container: the
  `dataflow/dataflow` credentials are rejected, so live-PG tests fail with
  `password authentication failed`. These are environmental, not regressions —
  compare against a `git stash` baseline before calling a failure new.
- MySQL 3306, SQL Server 1433, Oracle 1521 are not running locally.
- `pymssql` is not installed (one generic_sql driver-fallback test fails on that).

## Known pre-existing failures (fail identically on the base branch)
- `tests/test_elasticsearch_decimal_mapping.py::test_writer_binds_decimal_as_fixed_point_text_not_float`
- `tests/test_oracle_empty_string_bind.py::test_quarantine_cell_wire_preserves_sql_null_not_empty`
- `tests/test_generic_sql_drivername.py::test_mssql_drivername_falls_back_to_pymssql_without_odbc_driver` (needs pymssql)

## Verification habit
- For a defect fix, prove the new test fails on the pre-fix code (temporarily
  `git show HEAD:<file> > <file>`, run, restore) before claiming the fix.
