# Desktop lab — 80 catalog slots as source and dest

Operator option on **Proofs → Integrity ledger → Run desktop lab**.

`POST /api/v1/workspace/proofs/desktop-lab` binds every id in
`services.desktop_lab.DESKTOP_LAB_CONNECTORS` (≥80) and runs:

1. **Map SSOT** — `semantic_mapper.map_columns` must land `id`/`amount`
2. **Validate** — Execute with preflight (`skip_preflight=False`, strict)
3. **Dest role** — 2-row CSV fixture → that connector
4. **Source role** — that object → SQLite
5. **Payload reconcile** — SQLite must contain exactly `(1, 1000.00)` and `(2, 2000.50)`
6. **No silent loss** — `rejected_rows` and `coerced_null_rows` must be 0

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
