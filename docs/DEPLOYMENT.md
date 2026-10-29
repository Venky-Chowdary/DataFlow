# Datawrap — Production Deployment Guide

Deploy the full stack (API + Web + MongoDB) with Docker, or split UI/API for Varity, Railway, or Render.

## Quick deploy (Docker Compose)

```bash
# 1. Secrets
cp .env.production.example .env
chmod +x scripts/generate-production-secrets.sh
./scripts/generate-production-secrets.sh
# Paste output into .env

# 2. Build & run
docker compose -f docker-compose.prod.yml up -d --build

# 3. Verify
curl http://localhost/health-api
curl http://localhost:8000/health
```

Open **http://localhost** — sign in with the admin user you configured in `DATAFLOW_AUTH_USERS`.

---

## Architecture

| Service | Role | Port |
|---------|------|------|
| **web** | React UI + nginx (`/api` → API) | 80 |
| **api** | FastAPI, connectors, transfers | 8000 |
| **mongodb** | Job history, transfer state | 27017 |

Persistent volumes: `dataflow_data` (connectors, schedules), `dataflow_uploads`, `dataflow_vectors`.

---

## Required environment variables

| Variable | Description |
|----------|-------------|
| `DATAFLOW_ENV` | `production` |
| `DATAFLOW_REQUIRE_AUTH` | `1` |
| `DATAFLOW_AUTH_SECRET` | Random 32+ byte hex (`openssl rand -hex 32`) |
| `DATAFLOW_AUTH_USERS` | JSON array with SHA-256 password hashes |
| `MONGODB_URI` | Production MongoDB connection string |
| `CORS_ORIGINS` | Public frontend URL(s), comma-separated |
| `DATAFLOW_SECRETS_KEY` | Fernet key for encrypted connector passwords |

Optional:

| Variable | Default (prod) | Purpose |
|----------|----------------|---------|
| `DATAFLOW_TRAINING` | `off` | Disable background ML training |
| `DATAFLOW_AUTO_INSTALL_DRIVERS` | `0` | Drivers baked in Docker image |
| `DATAFLOW_ENABLE_DOCS` | `0` | Hide `/docs` |
| `DATAFLOW_EXPOSE_ERROR_DETAIL` | `0` | Debug-only: add redacted, truncated exception detail to safe API errors. Never enable in production. |
| `DATAFLOW_SEED_DEMO` | `0` | No demo connectors |
| `DATAFLOW_RBAC_UNRULED_ROUTES` | `deny` | `deny` rejects authenticated requests without a reviewed public exception or explicit permission rule; `allow_and_log` temporarily restores method-based fallback permissions and warns/audits. Invalid values fail closed. |
| `DATAWRAP_SINGER_TAPS_DIR` | unset | Directory of operator-installed `tap-*` executables. **Unset disables the `singer_tap` connector.** Only `tap-*` programs inside it run (no interpreters, no paths elsewhere); only the platform administrator may create, test or run a Singer connector. Every `tap-*` entry in the directory, symlinks included, is trusted. |
| `DATAFLOW_SQLITE_ROOT` | `<data dir>/sqlite` in production; unset (any path) in development | Directory (or `os.pathsep`-separated list) local SQLite connector files must live under; a relative name is anchored at the first. The service data directory itself is always refused. A SQLite connector pointing elsewhere (e.g. `/tmp/x.db`) is refused in production. |
| `DATAFLOW_DUCKDB_ROOT` | `DATAFLOW_SQLITE_ROOT`, else `<data dir>/duckdb` in production | Same confinement for local DuckDB files (MotherDuck `md:` and `:memory:` are unaffected). |
| `DATAFLOW_ICEBERG_ROOT` | `<data dir>/iceberg` in production; unset in development | Directory local Iceberg warehouses and SQLite catalogs must live under. Object-store warehouses (`s3://`, `gs://` ...) are unaffected. |
| `DATAFLOW_SOURCE_PATH_ROOTS` | unset | Extra `os.pathsep`-separated directories a file transfer may read `source_path` from, besides the upload/staging directories. |
| `DATAFLOW_MAX_UPLOAD_BYTES` | `268435456` | Largest upload body read into memory by the upload routes; over it the API answers 413 (matches nginx `client_max_body_size 256m`). |
| `DATAFLOW_MAX_DECOMPRESSED_BYTES` | `1073741824` | Largest size a gzip payload may inflate to where a reader needs the whole image (JSON, Excel, XML, Parquet/ORC footers); streaming CSV/TSV readers are not capped. |

### Multi-replica / enterprise coordination

| Variable | Purpose |
|----------|---------|
| `DATAFLOW_JOB_STORE` | Use `mongodb` (shared) for multi-replica. `memory` is **single-instance / tests only**. |
| `DATAFLOW_MULTI_REPLICA` | Set `1` to fail-closed when Mongo leases/locks are unavailable (no in-memory fallback). |
| `DATAFLOW_CDC_LEASE_REDIS_URL` (or `DATAFLOW_REDIS_URL`) | Required for CDC when `DATAFLOW_MULTI_REPLICA=1`: the file and memory lease backends fail closed. `docker-compose.prod.yml` ships a `redis` service and sets `DATAWRAP_CDC_LEASE_REDIS_URL`; set a URL-safe `REDIS_PASSWORD` in `.env`. Health reports `cdc_lease_backend` / `cdc_lease_distributed`. |
| `DATAFLOW_WORKER_LEASE_TTL` | Lease TTL seconds (default `60`). Heartbeat loss **cancels** the job. |
| `DATAFLOW_REQUIRE_WORKSPACE` | `1` denies empty `X-Workspace-Id` (hard tenant isolation). On in production by default. |
| `DATAFLOW_WORKER_FLEET` | `1` enables Mongo job queue for dedicated worker processes (`services.worker_fleet`). |

**Single replica (dev):** `DATAFLOW_JOB_STORE=memory` is fine; leases stay process-local.

**Multi replica (Railway / prod):** require shared MongoDB, set `DATAFLOW_MULTI_REPLICA=1` (or rely on Railway production detection). Sync cursors and schedules use Mongo when connected. Do not run multiple API replicas against a memory job store.

---

## Create an admin user

```bash
# SHA-256 hash of your password
echo -n 'YourSecurePassword' | shasum -a 256
```

```json
DATAFLOW_AUTH_USERS=[{"email":"admin@company.com","password_hash":"<hash>","name":"Admin","role":"owner"}]
```

---

## Varity / Railway / Render

### Railway (step-by-step)

Full guide: **[docs/RAILWAY.md](docs/RAILWAY.md)**

1. Create Railway project from GitHub
2. Add **MongoDB** plugin
3. Deploy **API** service — config `deploy/railway/api.toml`, Dockerfile.api
4. Deploy **Web** service — config `deploy/railway/web.toml`, set `VITE_API_BASE`
5. Mount **Volume** at `/data` on API service

### Recommended split (other hosts)

1. **Web** — static host or Varity static (build `apps/web` with `VITE_API_BASE=https://api.yourdomain.com/api/v1`)
2. **API** — dynamic/container host (Python, ≥2 GB RAM)
3. **MongoDB Atlas** — free M0 or paid cluster

### Varity dynamic deploy

```bash
pip install varitykit
export DATAFLOW_ENV=production
# Set all vars from .env.production.example in Varity dashboard
varitykit app deploy --hosting dynamic
```

Point Varity-managed MongoDB URI to `MONGODB_URI`.

### Manual API only

```bash
docker build -f Dockerfile.api -t dataflow-api .
docker run -p 8000:8000 --env-file .env \
  -v dataflow_data:/data/data \
  -v dataflow_uploads:/data/uploads \
  dataflow-api
```

---

## Connectors in production

- **File upload → database**: Works when users provide destination credentials reachable from the API server.
- **Database → database**: Requires saved connectors (encrypted at rest) and network access to both endpoints.
- **Snowflake / BigQuery / S3**: Require customer cloud credentials — not bundled with the platform.
- **Drivers**: Installed in the Docker image via `requirements.txt` — no user `pip install`.

---

## Health checks

| Endpoint | Use |
|----------|-----|
| `GET /health` | Load balancer — checks MongoDB, storage, drivers |
| `GET /health-api` (via nginx) | Web container probe |

---

## Pre-release checklist

- [ ] `.env` filled — no `CHANGE_ME` values
- [ ] `DATAFLOW_ALLOW_DEV_USER=0`
- [ ] `DATAFLOW_ENABLE_DOCS=0`
- [ ] MongoDB reachable from API
- [ ] CORS matches frontend URL
- [ ] `npm run test` passes
- [ ] `npm run build` passes
- [ ] Smoke test: login → upload CSV → PostgreSQL transfer

---

## Local development (unchanged)

```bash
docker compose up -d          # Postgres, Redis, MinIO, MongoDB
npm run dev:api
npm run dev
```

Dev auth: `test@gmail.com` / `password123` (when `DATAFLOW_REQUIRE_AUTH=0`).

---

## Authorization behaviour flags (off by default = pre-hardening behaviour)

These flags gate access-control changes that were not part of the original
defect brief. Each defaults to the behaviour production had before the change,
and logs an audit entry when the new rule *would* have applied. Read with the
`DATAWRAP_` prefix first, then `DATAFLOW_`.

### `DATAFLOW_TENANT_IP_ALLOWLIST_ENFORCE` (default `0`)

Per-tenant `ip_allowlist` (tenant custom domains, `TenantMiddleware`).

- Before: the bare `"/"` entry in the public-path list matched every path as a
  prefix, so the allowlist was **never enforced** on any route.
- Now: public paths are matched on segment boundaries (`/` only exactly), so
  the allowlist can actually block. That is only active with
  `DATAFLOW_TENANT_IP_ALLOWLIST_ENFORCE=1`.
- With the flag off (default) a request from an IP outside the tenant's
  allowlist is **allowed** and an audit event
  `tenant.ip_allowlist.would_block` (outcome `skipped`, level `warn`) is
  written with `tenant_id`, `workspace_id`, `path` and `client_ip`.

Owner how-to: leave off, then search the audit log for
`tenant.ip_allowlist.would_block` for a few days. Every entry is a client that
would be locked out; add their IPs to the tenant allowlist (or fix the proxy
/ `X-Forwarded-For` trust settings so the real client IP is seen), then set
the flag to `1`. Unauthenticated routes (login, signup, webhooks, health, SSO,
contact-sales) stay outside the allowlist regardless of the flag.
