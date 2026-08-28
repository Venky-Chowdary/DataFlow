# Desktop lab — 45 catalog slots as source and dest

Operator option on **Proofs → Integrity ledger → Run desktop lab**.

`POST /api/v1/workspace/proofs/desktop-lab` binds every id in
`services.desktop_lab.DESKTOP_LAB_CONNECTORS` (≥45) and runs:

1. **Dest role** — CSV fixture → that connector
2. **Source role** — that object → SQLite

## Honesty

- **45 is catalog slots**, not unique engines, not catalog tile count, not 650+ live.
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
