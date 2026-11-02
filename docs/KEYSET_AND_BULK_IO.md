# Keyset Pagination & Bulk Export (Phase F2 / F3)

## F2 — Keyset (seek) pagination

**SSOT:** `services/keyset_pagination.py` (shared with CDC via `cdc_snapshot_window`).

| Capability | Status |
|------------|--------|
| Single-column PK | All keyset-capable engines |
| Composite PK (N-col OR/AND) | Transfer + generic_sql; SQL Server / Oracle portable |
| SQL Server / Oracle transfer | Routed through `read_table_cursor_batch` |
| OFFSET fallback | When no PK — surfaces `pagination_mode=offset` + warning |

Bookmark encoding uses ``KEYSET_SEP`` (`U+001F`); legacy ``cursor\|pk`` still decodes for 2-col watermarks.

Operator proof: `dest_summary.pagination_mode` ∈ {`keyset`, `offset`, `bulk_copy`} and `pagination_key_columns` when keyset.

## F3 — Bulk export

| Engine | Path | Status | Capability label |
|--------|------|--------|------------------|
| PostgreSQL | `COPY (SELECT…) TO STDOUT` CSV | **Implemented** (`connectors/bulk_export.py`) | `bulk_export_status=implemented_pg_copy` |
| Snowflake | Stage `COPY INTO` + GET | **Planned** — `NotImplementedError` if forced | `bulk_export_status=planned` |
| BigQuery | Storage Read API | **Planned** — `NotImplementedError` if forced | `bulk_export_status=planned` |

**Gate:** `DATAFLOW_BULK_EXPORT=0` (default). Set `1` / `force` for PostgreSQL COPY on full-refresh, unfiltered transfers. Do not claim Snowflake/BQ bulk source unload until those paths leave Planned.

COPY currently materializes the CSV buffer then pages — safe for tens-of-GB migration jobs on well-sized API nodes; true streaming COPY writer is a follow-up before enabling `auto` in production fleets.

## F4 — PostgreSQL CDC transport

| Mode | Env | Product status |
|------|-----|----------------|
| `auto` (default) | unset or `DATAFLOW_CDC_PG_TRANSPORT=auto` | **Production default** — `START_REPLICATION` streaming; falls back to `peek` (logged, `cdc_transport_fallback_reason` in CDC metadata) when a replication connection cannot be opened |
| `streaming` | `DATAFLOW_CDC_PG_TRANSPORT=streaming` | Same as `auto` (fallback still applies on open failure) |
| `peek` | `DATAFLOW_CDC_PG_TRANSPORT=peek` | Opt-out — `pg_logical_slot_peek_*` + slot advance on ack; re-decodes WAL from `confirmed_flush_lsn` every poll |

Streaming contract (`connectors/postgresql_cdc_transport.py`): `poll` returns complete transactions and keeps them until `ack` (peek semantics); `ack` drops through the acked COMMIT only (interleaved transactions are never dropped by LSN); confirmed-flush feedback is sent only for acked LSNs, or the server's sent position on an idle slot with an empty buffer; transient connection failures (SQLSTATE 08*, 57P*, 55006) reconnect from the ack LSN with capped backoff (`DATAFLOW_CDC_PG_STREAM_RECONNECT_MAX`, default 5), anything else fails the job. Transport stats (`stream_*`) are in `cdc_metadata()`.

Evidence: `tests/test_cdc_postgres_streaming_transport_live.py` (ack feedback, walsender kill → reconnect without loss or resend, idle WAL release, runner forced restarts duplicate-free) plus the full live PG CDC suite run on streaming. Local capture latency (PG 16, commit → batch): 200 paced inserts p50 0.063s peek / 0.032s streaming; 3,000-row burst p50 1.30s / p95 2.28s peek vs 0.031s / 0.057s streaming. Measured on one PG version on one host — not a fleet SLO.

Capability: `cdc_transport_default=auto`, `cdc_streaming_status=default_with_peek_fallback`, `supports_streaming=true`.

At-least-once is preserved — confirmed LSN is never advanced on receive alone.

Module: `connectors/postgresql_cdc_transport.py`.
