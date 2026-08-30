# Desktop lab — 80 catalog slots as source and dest

Operator option on **Proofs → Integrity ledger → Run desktop lab**.

`POST /api/v1/workspace/proofs/desktop-lab` binds every id in
`services.desktop_lab.DESKTOP_LAB_CONNECTORS` (≥80) and runs:

1. **Map SSOT** — `semantic_mapper.map_columns` must land `id`/`amount`/`code`
2. **Cell transform SSOT** — `apply_transform` integer/decimal/none on the fixture
3. **ShapeEngine** — trim + upper on `code` (` usd` → `USD`) before Map/write
4. **Validate** — Execute with preflight (`skip_preflight=False`, strict)
5. **Dest role** — shaped 2-row fixture → that connector
6. **Source role** — that object → SQLite
7. **Payload reconcile** — SQLite must contain `(1, 1000.00, USD)` and `(2, 2000.50, EUR)`
8. **No silent loss** — `rejected_rows` and `coerced_null_rows` must be 0

`100%` on this fixture means every listed slot passed Map + Validate + dest +
source + payload with zero rejected/coerced rows. Source-only (PDF/DOCX/HTML/REST)
and dest-only (pgvector) tiles are excluded — they cannot pass both ways.

## Unique-engine cartesian

`POST /api/v1/workspace/proofs/desktop-lab-cross` (Proofs → **Run unique-engine matrix**)
runs every *live unique engine* as source × every live unique engine as dest:

Default unique engines (25 pairs): PostgreSQL, MySQL, MongoDB, SQLite, MinIO S3.

`DATAFLOW_CROSS_EXTENDED=1` adds SQL Server, Oracle, fake-gcs, Azurite, DynamoDB
Local, fakesnow, BigQuery emulator, Redis, Iceberg REST. Those dests have hung
create-new probes on this host — they are opt-in, never fake green.

That is **not** 80×80 catalog aliases (Neon/RDS share the Postgres wire). A
backend that is down is `skipped`. Salesforce / HubSpot / Stripe stay omitted
until a live backend exists. Emulators are not a customer-tenant SKU.

```bash
DATAFLOW_CROSS_MATRIX=1 PYTHONPATH=. python -m pytest \
  tests/test_desktop_lab_cross_matrix.py -q
```

## Type × sync × schema (named fixture — not every type)

`tests/desktop_lab_dimensions.py` is the live type × two-run sync × schema-shape
matrix on PostgreSQL and MySQL only. It is **not** every SQL type, every
canonical sync mode, or every dest-exists shape.

Measured on this host (`desktop_lab_dimensions.json`): **24 passed / 0 failed /
0 skipped** of 24 cells.

- **Types:** the 7-column FIDELITY fixture (`id`, `amt_dec`, `amt_float`,
  `note_null`, `note_empty`, `ts_utc`, `flag`). JSON / array / UUID / binary
  are not in this fixture.
- **Sync:** overwrite, append, incremental_append, upsert (two-run row counts).
  CDC / SCD2 / mirror / reverse_etl / incremental_deduped are not claimed here.
- **Schema:** create-new typed, dest-exists compatible, dest-exists DECIMAL→INT
  (fail-closed), and dest-exists extra source G13 (fail-closed) on four SQL
  routes. Dest-exists overwrite keeps live dest types/nullability.

```bash
PYTHONPATH=. python -m pytest tests/test_desktop_lab_dimensions.py -q
```

## Previously untested dimensions (live desktop)

`tests/desktop_lab_untested.py` covers what the 7-column overwrite fixture
skipped, against services that are actually up.

Measured on this host: **29 passed / 0 failed / 1 skipped** of 30 cells
(`test_desktop_lab_untested_important_dimensions`, 48.47s,
`desktop_lab_untested.json`).

Passed: dest-exists JSONB/UUID/BYTEA/INTERVAL; **INT[] create-new and
dest-exists stay `bigint[]` (never invent JSONB)**; XML + native POINT;
**PostGIS GEOGRAPHY** dest-exists (`geography(Point,4326)`); nested explode
CSV→PG; incremental_deduped and mirror on PG and MySQL dest; SCD2 PG→SQLite;
reverse-ETL PG→MySQL; **local SaaS HTTP stub** Salesforce / HubSpot / Stripe
(not a customer org; Stripe Planned refuse measured); MySQL ROW binlog →
SQLite CDC plus **redelivery replay** (`exactly_once_claimed=false`);
PG logical → PG CDC; G14 dest-only NOT NULL; PG→SQLite/Mongo/S3/SQL Server
dest-exists; **Oracle dest-exists** (fresh process); **GCS / ADLS create-new**
on fake-gcs / Azurite (emulator, not customer-tenant);
`production_sku_validate_honesty` (78 routes, 0 planned).

Skipped (not invented green): **BQ create-new** — goccy emulator
`sql: connection is already closed` (5s health probe). Create-new was
measured on this host when the emulator was healthy; that is not a
customer-tenant PRODUCTION_SKU. Catalog tiles are not transfer-live.
CDC default remains **at-least-once upsert**.

```bash
PYTHONPATH=. python -m pytest tests/test_desktop_lab_untested.py -q
```

## Honesty

- **80 is catalog slots**, not unique engines, not catalog tile count, not 650+ live.
- Hosted twins (Neon / RDS / CNPG / OpenShift PostgreSQL) share the parent driver.
  They prove alias wiring on a real write/read.
- Unique duplex engines are counted separately (`unique_engines_duplex_passed`).
- A backend that is down is `skipped` with a reason — never a fake green.
- CDC default remains **at-least-once upsert**.

## Reproduce

```bash
cd apps/api
PYTHONPATH=. python -m pytest tests/test_desktop_lab_duplex_matrix.py -q
```
