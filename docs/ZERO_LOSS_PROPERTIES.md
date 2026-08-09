# Zero-Loss Properties — Migration Assurance Ledger

Each property is either **PROVEN** (executable proof against real services or an
exhaustive engine matrix attached below), **PARTIAL**, **UNPROVEN**, or
**NOT_GUARANTEED**. There is no third option between proven and documented-absent.

| # | Property | Status | Proof command | Engines covered | Engines NOT covered |
|---|----------|--------|---------------|-----------------|---------------------|
| 1 | Type identity is referentially transparent | **PROVEN** | `cd apps/api && python -m pytest tests/test_property1_type_identity_case_transparent.py -q` (424 passed) + live PG introspect when reachable | All `DDL_TYPES` destinations (case×logical matrix); live PostgreSQL introspect `integer`→`INT4` | Docker MySQL/ClickHouse/Iceberg not run on this host (no Docker); matrix covers their invent DDL |
| 2 | The legitimate path is never blocked | **PARTIAL** | `cd apps/api && python -m pytest tests/test_property2_golden_path_never_blocked.py -q` | SQLite↔SQLite (always); live PG→PG, CSV→PG, PG→SQLite, PG→Parquet, Mongo→PG when services up; CI job `no-config-transfer` | MySQL (no Docker on proof host); kill-9 resume-after-kill matrix not in this suite (see existing checkpoint resume tests) |
| 3 | Source reads are snapshot-consistent | UNPROVEN | — | — | — |
| 4 | Writes are exactly-once observable | UNPROVEN | — | — | — |
| 5 | Five-layer verification, not sampling | UNPROVEN | — | — | — |
| 6 | Schema fidelity is more than column types | UNPROVEN | — | — | — |
| 7 | Referential integrity across multi-table migration | UNPROVEN | — | — | — |
| 8 | Semantic value fidelity | UNPROVEN | — | — | — |
| 9 | Every row is accounted for | UNPROVEN | — | — | — |
| 10 | Determinism | UNPROVEN | — | — | — |
| 11 | The migration certificate | UNPROVEN | — | — | — |
| 12 | Adversarial and chaos testing | UNPROVEN | — | — | — |

---

## Property 1 — PROVEN (2026-08-09)

### Defect
`ddl_type` disambiguated native vs logical integer/float by **surface case**:
`integer`→64-bit, `INTEGER`/`Integer`/`INT`→32-bit; `float`→64-bit, `FLOAT`→32-bit
on ClickHouse/Iceberg/MySQL. Five spellings normalized to the same logical type
but invented different widths.

### Fix
1. **`LogicalType` / `NativeType`** in `services/decision_kernel/logical_type.py` —
   width-bearing logical carriers; `ddl_type` accepts them.
2. **Ambiguous tokens** (`INTEGER`/`INT`/`FLOAT` any case, plus bare logical
   `integer`/`float`) → width unknown → invent via `DDL_TYPES` (64-bit / IEEE-64).
3. **Unambiguous carriers** only select 32-bit: `INT4`/`INT32`/`SERIAL`,
   `REAL`/`FLOAT4`/`FLOAT32`/`FLOAT(p≤24)`.
4. **Introspect** emits `INT4` (PG int4) and MySQL `INT4` / `FLOAT32`.
5. **Case-variant × destination matrix** + monotonicity vs `DDL_TYPES`.

### Proof output (excerpt)

```
CASE MATRIX (ambiguous spellings → identical invent)
  postgresql   integer/INTEGER/Integer/INT/int → BIGINT
  clickhouse   integer/INTEGER/Integer/INT/int → Int64
  iceberg      integer/INTEGER/Integer/INT/int → long
  clickhouse   float/FLOAT/Float             → Float64
  iceberg      float/FLOAT/Float             → double
  mysql        float/FLOAT/Float             → DOUBLE

unambiguous:
  INT4  → postgresql INTEGER / clickhouse Int32 / iceberg int
  FLOAT32 → iceberg float / clickhouse Float32

introspect:
  PG integer/int4 → INT4
  MySQL int/float → INT4 / FLOAT32

pytest: tests/test_property1_type_identity_case_transparent.py — 424 passed
monotonicity: 0 failures across all DDL_TYPES × {integer,float}
```

### Live PostgreSQL (this host)

```
LIVE_PG_FORMAT_TYPE [('i', 'integer'), ('b', 'bigint'), ('r', 'real'), ('d', 'double precision')]
i integer -> INT4
b bigint -> BIGINT
r real -> REAL
d double precision -> DOUBLE PRECISION
LIVE_PG_OK
```

### NOT claimed
End-to-end transfer matrices on MySQL/ClickHouse/Iceberg Docker were **not** run
on this Windows host (Docker unavailable). Invent DDL for those engines is proven
by the case matrix against `ddl_type` / `DDL_TYPES` authority.

---

## Property 2 — PARTIAL (2026-08-09)

### Defect
Plain create-new / overwrite transfers with auto-derived identity mappings
(no Map `target_type`) were blocked by `g6_additive_stamp`:
`Additive column(s) … lack Map target_type under partial Studio`.
`stamp_additive_mapping_types` listed every blank mapping as “unstamped” even
when invent authority was create-table, and `_auto_map` overwrite identity maps
did not request CREATE invent.

### Fix
1. Overwrite auto-maps stamp `create_new` + `source_type` so Kernel invent runs.
2. `stamp_additive_mapping_types(..., dest_table_exists=False)` invents CREATE
   TABLE stamps; `unstamped` is invent-required-but-failed only.
3. Golden-path suite + gate ALLOW/BLOCK pair + CI job `no-config-transfer`.

### Proof output (this host)

```
pytest tests/test_property2_golden_path_never_blocked.py -q
… passed (SQLite↔SQLite × maps × skip_preflight; PG→PG; CSV→PG; PG→SQLite;
          PG→Parquet; Mongo→PG when up; MySQL skipped — no Docker)
g6 BLOCK (invent refused) + g6 ALLOW (create-table invent) green
```

### NOT claimed / remaining for PROVEN
* MySQL 8 Docker route
* kill-9 mid-chunk resume on every golden route (checkpoint resume tests exist
  separately; not yet in this always-green suite)
* Reconciliation checksum assert on every golden route (row-count + success only)
