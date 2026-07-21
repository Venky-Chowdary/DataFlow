# CDC lease Redis HA runbook

DataFlow CDC leases prevent two workers from consuming the same logical slot /
binlog `server_id` / capture instance. Delivery remains **at-least-once upsert**.

## Backends

| `DATAFLOW_CDC_LEASE_BACKEND` | Behavior |
|---|---|
| `auto` (default) | Redis when `DATAFLOW_CDC_LEASE_REDIS_URL` or `DATAFLOW_REDIS_URL` is set; else file |
| `redis` | Multi-node authority; **fail-closed** if Redis is unreachable |
| `file` | Single-host JSON + flock |
| `memory` | Process-local (tests only) |

Fencing: each steal increments `generation`. Renew/release require matching holder **and** generation.

## Production checklist

1. Point **all** API replicas at the same Redis URL (`DATAFLOW_CDC_LEASE_REDIS_URL`).
2. Prefer Redis Sentinel / managed HA — DataFlow does not invent split-brain fallback to file when `backend=redis`.
3. TTL default `DATAFLOW_CDC_LEASE_TTL_SEC=120`. Keep heartbeat renew well under TTL.
4. On conflict: Jobs / Theater → **Force-release lease** (fencing-aware) or cancel the holder job first.
5. Ops APIs: `GET /api/v1/ops/cdc-leases?cursor_key=…`, `POST /api/v1/ops/cdc-leases/force-release`.

## Freshness SLOs

- `GET /api/v1/ops/freshness?warn_seconds=60` — per-pipeline lag + heartbeat age.
- Default critical ≈ `5 × warn`. Heartbeat stale default 300s.
- Overview surfaces alerts with **Open pipeline / Open job** CTAs.

## Failure modes (honest)

| Symptom | Likely cause | Action |
|---|---|---|
| `cdc_lease_conflict` | Second consumer on same resource | Stop holder or force-release, then resume |
| `cdc_lease_store_unavailable` | Redis down with `backend=redis` | Restore Redis; do not flip to file mid-incident |
| Freshness critical + healthy lease | Source lag or slow apply | Check CDC lag metrics / warehouse capacity |
| Heartbeat stale | Worker crashed without release | Wait TTL or force-release |

## Do not claim

- Exactly-once delivery
- Automatic Redis Sentinel discovery without operator config
- That force-release stops the prior process (it fails on next renew only)
