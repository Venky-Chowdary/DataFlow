# Redis TTL / EXPIRE semantics (honest)

Redis keys may carry **TTL** (`EXPIRE` / `PEXPIRE`). Datawrap Redis read/write
paths move **values** (and typed document fields) — they do **not** productize
TTL preservation as a first-class migration guarantee.

## Operator expectations

| Behavior | Reality |
|----------|---------|
| Read Redis → SQL/warehouse | Document/hash fields transfer; TTL is **not** copied onto destination rows |
| Write SQL → Redis | Keys are written without inheriting a source RDBMS “TTL”; set EXPIRE in a post-load job if needed |
| Overwrite / upsert | At-least-once; concurrent writers need key conventions (`prefix` + identity) |
| Lease TTL (CDC) | Unrelated — CDC Redis leases are lock TTLs, not KV data TTLs |

Validate surfaces a **soft policy warning** when Redis is source or destination
so operators do not assume EXPIRE survives the transfer.
