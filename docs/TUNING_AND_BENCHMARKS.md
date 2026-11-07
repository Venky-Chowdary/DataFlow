# Tuning & Benchmarks (Phase F6)

## Defaults (post-F6)

| Knob | Previous | Current default | Notes |
|------|----------|-----------------|-------|
| `PARALLEL_WORKERS` | `min(2, cpu)` | `min(4, cpu)` | Per-transfer chunk parallelism |
| `TRANSFER_WORKERS` | `4` | `8` | Concurrent jobs in one process |
| `BULK_EXPORT` | off | off | Set `1` for PG COPY (F3) |
| `RECONCILE_SOURCE_REREAD` | off | off | Inline write-pass checksums (F1) |
| `CDC_PG_TRANSPORT` | `peek` | `peek` | Set `streaming` for F4 |

## Measured microbench (local SQLite, developer laptop)

Run:

```bash
cd apps/api
python scripts/throughput_microbench.py
```

Artifact: `data/proofs/throughput_microbench.json`

Measured on this repo (2026-08-08, Windows 11, Intel 16-logical-CPU laptop, Python 3.12.10) — from `data/proofs/throughput_microbench.json`:

| Route | Rows | PARALLEL_WORKERS | rows/s | Notes |
|-------|-----:|-----------------:|-------:|-------|
| sqlite→sqlite | 5_000 | 1 | **3659.88** | keyset + inline checksum |
| sqlite→sqlite | 5_000 | 4 | **3478.56** | Default; SQLite writer lock caps gain |

Hardware must be named in the JSON (`platform`, `cpu_count`). Re-run before quoting in sales.

## Operator guidance

1. Raise `PARALLEL_WORKERS` until CPU or destination write saturates; past that, returns diminish and checkpoint churn rises.
2. On multi-replica, use `SCHEDULER_MODE=claim` (auto in prod) — do not scale by only increasing `TRANSFER_WORKERS` on one box.
3. Warehouses: prefer keyset (`pagination_mode=keyset`) or `BULK_EXPORT=1` (PG); OFFSET is the cost cliff.
4. CDC: keep `peek` until streaming lag curves are proven on your slot; then `CDC_PG_TRANSPORT=streaming`.

## CI / competitive floor (`test_benchmark_harness`)

`test_dataflow_exceeds_competitive_baseline` runs a **100k file→sqlite** transfer.
Default floor is **800 rows/s** (`DATAFLOW_BENCH_MIN_RPS`), below the measured
microbench (~3.6k on 5k rows) because the full path includes reconcile and is
slower on the same laptop (~1–2k). Do **not** reinstate an unmeasured 5k gate.

## Honesty

Catalog tile counts and “TB/hour” claims are forbidden without this artifact class. CI publishes transfer_ready / warehouse / CDC matrices separately.
