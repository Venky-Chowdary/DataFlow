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

## Connector egress policy (private networks, loopback, metadata)

Connector hosts are operator-supplied, so the API refuses dial targets it should
not reach. There are two tiers.

| Target class | Where it applies | Behaviour |
|--------------|------------------|-----------|
| Cloud metadata (`169.254.169.254`, `100.100.100.200`, `fd00:ec2::254`, `metadata.google.internal`), link-local (`169.254/16`, `fe80::/10`), multicast | Every site | Always refused. No flag or allowlist can permit it. |
| Private (`10/8`, `172.16/12`, `192.168/16`, IPv6 ULA), loopback, CGNAT (`100.64/10`) | Sites production already enforced: connector Test, the SQL / MySQL / PostgreSQL / AWS drivers, PostgreSQL CDC, notification webhooks, the Copilot connector tools | Refused unless allowlisted (unchanged). |
| Private, loopback, CGNAT | Newer sites: connector save/update, transfer reads and writes, schema introspection, destination precount and probes, shared HTTP / SMTP / Mongo / Redis clients, HTTP redirects and pagination | **Audit only by default**: the call proceeds and an `egress.would_block` audit event is written. |

`egress.would_block` carries `point` (which site), `connector` (name, when known),
`host`, `reason`, `host_class`, and the workspace. It is written once per
connector, host and site per hour. Find them in the audit log (Settings, Audit,
or `GET /api/v1/audit/events?level=warn`, action `egress.would_block`).

### Flag

| Variable | Default | Meaning |
|----------|---------|---------|
| `DATAFLOW_EGRESS_ENFORCE_NEW` (or `DATAWRAP_EGRESS_ENFORCE_NEW`) | off | `1`/`true`/`yes`/`on`: the newer sites above refuse private, loopback and CGNAT hosts exactly like the production sites. |

Saving or renaming an existing connector whose host, endpoint URL and connection
string are unchanged is never refused because the host is private, even with the
flag on. Only a new or changed target is checked.

### Before turning the flag on

Count what it would break. The script is read-only (it never writes to a store
and never prints connection strings or secrets):

```bash
cd apps/api
python scripts/audit_egress_impact.py                          # store as the app is configured
python scripts/audit_egress_impact.py --file connectors.json   # a connector store file
MONGO_PROD=mongodb://... python scripts/audit_egress_impact.py --mongo-url-env MONGO_PROD
python scripts/audit_egress_impact.py --workspace ws-acme --json
```

It prints every saved connector whose `host`, `endpoint_url`, `connection_string`
(or `base_url` / `url` / registry URL) the enforcing rule would refuse, with the
workspace, the host class (`loopback`, `private`, `cgnat`, `link-local`,
`metadata`) and the reason, then a count (`N of M saved connectors would be
refused`). It honours workspace allowlists and `DATAFLOW_EGRESS_ALLOWLIST`, so
add the ranges first and re-run until the count is what you expect. Rows with
`link-local` / `metadata` are refused today regardless of the flag.

### Allowing a private range for a workspace

A workspace admin adds CIDR ranges (not host names) to the tenant record's
`egress_allowlist`. Metadata, link-local, multicast and `0.0.0.0/0` cannot be
listed.

```bash
# find the tenant id for the workspace
curl -s -H "Authorization: Bearer $TOKEN" -H "X-Workspace-Id: ws-acme" \
  "$API/api/v1/workspace/tenant"

# allow the on-prem database VLAN and one VPC peer
curl -s -X PATCH -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"egress_allowlist": ["10.20.0.0/16", "172.31.5.0/24"]}' \
  "$API/api/v1/workspace/tenant/$TENANT_ID"
```

The PATCH replaces the list, so send every range the workspace needs. A connector
whose host is `10.20.4.7` then passes at every site (and `audit_egress_impact.py`
stops listing it). For a single-process lab or a proof run, set
`DATAFLOW_EGRESS_ALLOWLIST=127.0.0.0/8,10.20.0.0/16` in the API environment
instead; production deploys normally leave it unset and use the workspace list.

---

## Singer taps: what users see and how to enable them

`singer_tap` runs a program on the API host, so it stays **disabled until
`DATAWRAP_SINGER_TAPS_DIR` is set** and is usable only by the platform
administrator. Neither rule changed; what changed is that a user who hits one
now gets a specific in-app error instead of a failed run with a generic message.

| Code | HTTP | Meaning | Who fixes it |
|------|------|---------|--------------|
| `singer_taps_dir_unset` | 409 | `DATAWRAP_SINGER_TAPS_DIR` (or `DATAFLOW_SINGER_TAPS_DIR`) is unset, or is not a directory. | The server administrator: set it to the directory of `tap-*` executables and restart the API. |
| `singer_admin_required` | 403 | The caller is not the platform administrator. | Ask the platform administrator; a workspace admin is not enough. |

Both come back as `{"error": {"code": ..., "message": ...}}` and are raised when a
Singer connector is tested (`/connectors/test`, `/connectors/saved/{id}/test`),
created or edited, run as a transfer (Studio, Copilot confirm), pressed Run now
on a schedule, or put on a schedule (create, enable, or change its connectors).
Pausing or renaming an existing schedule is not blocked, and an unattended
scheduled run is not pre-empted: it starts, and the run fails with the same
message in its history.

Before enabling taps, list what exists (read-only; prints no `tap_config` or
secrets):

```bash
cd apps/api
python scripts/audit_singer_usage.py                          # store as the app is configured
python scripts/audit_singer_usage.py --file connectors.json   # schedules come from DATAFLOW_DATA_DIR
MONGO_PROD=mongodb://... python scripts/audit_singer_usage.py --mongo-url-env MONGO_PROD --json
```

It lists every saved `singer_tap` connector with its workspace, recorded tap
executable and whether this server can run it today (`ok`,
`singer_taps_dir_unset`, `tap not found in taps dir`), plus each schedule that
reads from or writes to it (enabled or paused).

---

## Upload, decompression and export limits

| Variable | Default | What it bounds |
|----------|---------|----------------|
| `DATAFLOW_MAX_UPLOAD_BYTES` (or `DATAWRAP_MAX_UPLOAD_BYTES`) | `268435456` (256 MiB) | The body an upload route (connector upload, Transfer Studio file run, analyze-file) reads into memory. |
| `DATAFLOW_MAX_DECOMPRESSED_BYTES` (or `DATAWRAP_MAX_DECOMPRESSED_BYTES`) | `1073741824` (1 GiB) | What a gzip file may expand to when a reader needs the whole image (parse, JSON, Excel, XML, Parquet/ORC). Streaming readers are not capped. |
| `DATAFLOW_EXPORTS_DIR` (or `DATAWRAP_EXPORTS_DIR`) | `apps/api/exports` | The one folder client-named exports (`/query/export`, file destinations) may write into. Created if missing; `..`, symlinks and absolute paths outside it are still refused, and only export extensions are allowed. |

All three are read on every request, so a change needs a restart only if your
process manager caches the environment. A non-numeric or non-positive byte value
falls back to the default.

When a limit is hit the API answers HTTP 413 and the message tells the user what
happened and who can change it:

```json
{
  "error": "payload_too_large",
  "detail": "Uploaded file is 312.4 MB; the limit is 256 MB. A server administrator can raise it with DATAFLOW_MAX_UPLOAD_BYTES (or DATAWRAP_MAX_UPLOAD_BYTES), currently 268435456 bytes.",
  "limit_bytes": 268435456,
  "actual_bytes": 327572275,
  "setting": "DATAFLOW_MAX_UPLOAD_BYTES"
}
```

`actual_bytes` appears only when the size is known (a declared upload size); a
body that was cut off at the limit, or a gzip stream that expanded past it, says
"larger than the ... limit" and `setting` is `DATAFLOW_MAX_DECOMPRESSED_BYTES`
for the latter. "MB" here is MiB (1024 x 1024 bytes).

To accept uploads above 256 MiB, raise `DATAFLOW_MAX_UPLOAD_BYTES` **and** the
`client_max_body_size` in `deploy/nginx.conf` (and `deploy/nginx.conf.template`),
otherwise nginx refuses the body first with its own 413.

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
