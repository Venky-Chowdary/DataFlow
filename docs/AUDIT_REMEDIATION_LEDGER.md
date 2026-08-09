# Audit Remediation Ledger

Branch: `fix/audit-p0-remediation` (cut from `devin/deep-audit-1784855991` @ `b1193a1`).

Status values: `NOT_STARTED` | `IN_PROGRESS` | `DONE_VERIFIED` | `BLOCKED` | `REGRESSED` | `DISPUTED`

| item | status | files changed | tests added | verify output | notes |
|------|--------|---------------|-------------|---------------|-------|
| 1 | DONE_VERIFIED | `apps/api/services/decision_kernel/type_invent.py`, `apps/api/services/decision_kernel/invent.py`, `apps/api/connectors/generic_sql.py` | `apps/api/tests/test_item1_pg_writer_bare_integer_is_bigint.py`, `apps/api/tests/test_item1_sa_bare_integer_never_int32.py` | See ITEM 1 verify log (2026-08-09): stash 4 failed → 4 passed; VERIFY set **486 passed, 1 skipped** | Fixed CREATE string path: bare logical `integer` rematerializes via `ddl_type`→BIGINT (`pg_type`); stamp_additive no longer casefolds logical `integer` onto physical `INTEGER`. Live PG writer: `udt=int8`, bit-exact int64. |
| 2 | NOT_STARTED | | | | |
| 3 | NOT_STARTED | | | | |
| 4 | NOT_STARTED | | | | |
| 5 | NOT_STARTED | | | | |
| 6 | NOT_STARTED | | | | |
| 7 | NOT_STARTED | | | | |
| 8 | NOT_STARTED | | | | |
| 9 | NOT_STARTED | | | | |
| 10 | NOT_STARTED | | | | |
| 11 | NOT_STARTED | | | | |
| 12 | NOT_STARTED | | | | |
| 13 | NOT_STARTED | | | | |
| 14 | NOT_STARTED | | | | |
| 15 | NOT_STARTED | | | | |
| 16 | NOT_STARTED | | | | |
| 17 | NOT_STARTED | | | | |
| 18 | NOT_STARTED | | | | |
| 19 | NOT_STARTED | | | | |
| 20 | NOT_STARTED | | | | |
| 21 | NOT_STARTED | | | | |
| 22 | NOT_STARTED | | | | |
| 23 | NOT_STARTED | | | | |
| 24 | NOT_STARTED | | | | |
| 25 | NOT_STARTED | | | | **NEW:** Excel→PG empty→decimal/datetime FAIL_JOB quarantine (job `6a77e32d…`); minio ImportError suppressed. |

## Flow track

```
devin/deep-audit-1784855991 (b1193a1)
        └── fix/audit-p0-remediation  ← ITEM work lands here only
```

## ITEM 1 verify log (2026-08-09) — CREATE string path + stamp identity

### Root cause (adversarial)
`postgresql_writer.pg_type` → `materialize_dest_ddl` treated bare logical `integer`
as physical pass-through; PostgreSQL keyword `integer` = INT32. Separately,
`stamp_additive_mapping_types` casefolded `integer` ≡ `INTEGER` and re-invented INT32.

### Stash proof (`test_item1_pg_writer_bare_integer_is_bigint.py`)
```
===== WITHOUT FIX =====
FFFF
FAILED test_materialize_and_pg_type_bare_integer_are_bigint
  AssertionError: assert 'integer' == 'BIGINT'
FAILED test_materialize_bare_integer_never_narrower_across_sql_dests
FAILED test_stamp_additive_does_not_collapse_logical_integer_to_int32
FAILED test_live_pg_writer_create_bare_integer_is_int8_and_holds_int64
  PostgreSQL rejected 3 row(s) (int64 into int4)
4 failed

===== WITH FIX =====
....  4 passed
```

### VERIFY command
```
pytest tests/test_item1_pg_writer_bare_integer_is_bigint.py \
  tests/test_item1_sa_bare_integer_never_int32.py \
  tests/test_canonical_width_never_narrower.py \
  tests/test_universal_type_harness.py \
  tests/test_bigint_create_new_roundtrip_width.py -q
→ 486 passed, 1 skipped
```

### Collateral
```
pytest tests/test_stamp_additive_mapping_types.py -q → 7 passed
```
