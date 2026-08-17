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
| `peek` (default) | `DATAFLOW_CDC_PG_TRANSPORT=peek` | **Production default** — `pg_logical_slot_peek_*` + slot advance on ack |
| `streaming` | `DATAFLOW_CDC_PG_TRANSPORT=streaming` | **Planned / opt-in** — `START_REPLICATION`; feedback only after ack; falls back to peek if connection fails |

Capability honesty: `cdc_transport_default=peek`, `cdc_streaming_status=planned_opt_in`, `supports_streaming=false` until lag curves are proven and streaming becomes default.

At-least-once is preserved — confirmed LSN is never advanced on receive alone.

Module: `connectors/postgresql_cdc_transport.py`.
