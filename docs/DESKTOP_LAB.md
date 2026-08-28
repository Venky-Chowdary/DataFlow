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
