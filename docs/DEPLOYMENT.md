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
| `DATAFLOW_SQLITE_ROOT` | unset (any path); with `DATAFLOW_CONFINE_LOCAL_FILES=1`, `<data dir>/sqlite` in production | Directory (or `os.pathsep`-separated list) local SQLite connector files must live under; a relative name is anchored at the first. The service data directory itself is always refused. Honoured whenever set. |
| `DATAFLOW_DUCKDB_ROOT` | unset (any path); with `DATAFLOW_CONFINE_LOCAL_FILES=1`, `DATAFLOW_SQLITE_ROOT`, else `<data dir>/duckdb` in production | Same confinement for local DuckDB files (MotherDuck `md:` and `:memory:` are unaffected). |
| `DATAFLOW_ICEBERG_ROOT` | unset; with `DATAFLOW_CONFINE_LOCAL_FILES=1`, `<data dir>/iceberg` in production | Directory local Iceberg warehouses and SQLite catalogs must live under. Object-store warehouses (`s3://`, `gs://` ...) are unaffected. Honoured whenever set. |
| `DATAFLOW_CONFINE_LOCAL_FILES` | off | `1`: production confines local SQLite, DuckDB and Iceberg files to the persistent-volume folders above when no explicit root is set. See "Local file confinement" below. |
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

### Client IP and https behind proxies (`DATAWRAP_TRUSTED_PROXY_COUNT`)

The API derives the client address with Werkzeug ProxyFix semantics: every
proxy appends the address of the peer it received the request from to
`X-Forwarded-For`, so with N trusted proxies the client is `hops[-N]`
(`services/client_ip.py`, applied for the whole app by `ProxyFixMiddleware`,
which also takes the scheme from `X-Forwarded-Proto`). uvicorn runs with
`--no-proxy-headers` so it cannot substitute its own, left-most choice.

| Deployment | Chain | `DATAWRAP_TRUSTED_PROXY_COUNT` |
|---|---|---|
| Railway, web nginx proxying to the API's public domain (default `deploy/nginx.conf.template`) | browser -> Railway edge -> nginx -> Railway edge -> API | `3` |
| Railway, nginx proxying over private networking (`http://<api>.railway.internal:<port>`) | browser -> edge -> nginx -> API | `2` |
| Docker Compose (`deploy/nginx.conf`, nginx is the edge) | browser -> nginx -> API | `1` |
| API reached directly, no proxy | browser -> API | `0` (default) |

- **Unset (default) behaves as `0`**: forwarded headers are ignored and the
  socket peer is used, which cannot be spoofed. Behind a proxy that peer is
  the proxy, so every user looks the same; while the count is unset, IP
  allowlists are therefore not enforced on private/proxy addresses (see the
  allowlist section) and the Enterprise page says so. Setting the count
  explicitly — including `0` for an API reached directly — turns enforcement
  on for every address.
- Too high a count lets a client choose its address by prepending entries;
  too low a count makes every user look like a proxy. A chain shorter than
  the count falls back to the socket peer.
- On Railway with the API's public domain enabled, a client can call that
  domain directly and skip nginx (one fewer hop, so it controls `hops[-3]`).
  Prefer private networking (count `2`) and remove the API's public domain,
  or restrict it, when IP allowlists matter.
- Check after deploying: sign in, open Settings -> Enterprise and save; the
  response's `enforcement.caller_ip` (and any allowlist refusal) must show
  your public address, not a `10.x`/`100.64.x` one.

### Public origin for links (`DATAWRAP_PUBLIC_API_ORIGIN`)

MCP manifest/status URLs and the SAML SP entity id / ACS URL are built from
the public origin, never from the request's Host (behind nginx that is the
Railway backend over http). Order: a forwarded host on the allow-list (the
configured origins, `DATAWRAP_PUBLIC_ORIGIN_ALLOWED_HOSTS`, saved tenant custom
domains), then `DATAWRAP_PUBLIC_API_ORIGIN`, then `DATAWRAP_WEB_URL` /
`DATAWRAP_WEB_DOMAIN`. https is forced in production. On Railway set
`DATAWRAP_WEB_DOMAIN` (or `DATAWRAP_PUBLIC_API_ORIGIN=https://www.datawrap.io`)
on the **API** service.

### Session timeout (backfill note)

A workspace's saved `session_timeout_hours` is enforced as an idle timeout on
sign-in sessions (`enforce_session_timeout`, default on; saving the hours turns
it on unless the same save sends `enforce_session_timeout: false`). Sessions
are stored in MongoDB (`auth_sessions`) with the local file as a mirror and
fallback, so a redeploy no longer drops them or their idle clock.

Tenants saved before this change keep the `false` they were stored with; the
Enterprise page, the posture and the report show "recorded, not enforced"
for them. To enforce, the workspace admin saves the Enterprise form (the
checkbox defaults on), or an operator runs once on the API host:

```bash
python -c "from services.tenant_store import list_tenants, update_tenant; \
[update_tenant(t.id, session_timeout_hours=t.session_timeout_hours) for t in list_tenants() if not t.enforce_session_timeout]"
```

(from `apps/api`; it re-saves each tenant's hours, which turns enforcement on,
and leaves every other field unchanged). It also enforces a tenant whose admin
meant "policy only": before round 11 that was the default and cannot be told
apart, so tell workspace admins before running it. Existing sign-in sessions keep
working; they start being idle-checked on their next request.

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

## Local file confinement (SQLite, DuckDB, Iceberg)

Status: the production-mode root is **off by default and waits for owner
approval (D180 is not approved)**. Nothing below turns it on.

A workspace member controls a local connector's `database` / warehouse path, so
the API restricts where those files may live. The rules split in two:

| Rule | Applies | Flag |
|------|---------|------|
| SQLite/DuckDB path may not point into the service data directory (`DATAFLOW_DATA_DIR`: job store, ledgers, secrets), except its `sqlite/` and `duckdb/` sub-folders. `..` and symlinks are resolved before the check, on every route (test, run, precount, undo, reconcile, SQLAlchemy URLs). | Always | none |
| Iceberg schema, table and relative catalog names may not contain `..`, a path separator or be absolute. | Always | none |
| An explicit `DATAFLOW_SQLITE_ROOT`, `DATAFLOW_DUCKDB_ROOT` or `DATAFLOW_ICEBERG_ROOT` confines that kind of file to the listed folders. | Whenever set | none (opt-in by setting it) |
| **Production default roots**: SQLite to `<data dir>/sqlite`, DuckDB to `<data dir>/duckdb`, Iceberg to `<data dir>/iceberg`; relative names anchored there; `DATAFLOW_SQLITE_ROOT` also applying to DuckDB; an absolute Iceberg catalog file held to the Iceberg root. | Production only | `DATAFLOW_CONFINE_LOCAL_FILES=1` (default off) |

With the flag off, a production process behaves as before for paths outside
those folders: they open as given (a relative name lands in the working
directory, `/tmp/x.db` in the container temp). Each one is recorded once an hour
as an audit event `local_file.would_confine` (`kind`, resolved `path`,
`would_be_root`, `flag`) so you can see what the flag would refuse before
turning it on. Development processes are never audited or confined by default.

### Migrating existing connectors without losing files

The failure the default root prevents is a database file on the container's
ephemeral disk, lost at the next restart. The flag does not move files; it
refuses paths outside the volume folders. To turn it on safely:

1. Run the read-only audit on the connector store (and again on the server whose
   working directory the API uses, because a relative path resolves against it):

   ```bash
   cd apps/api
   python scripts/audit_sqlite_paths.py --cwd /app/apps/api            # as configured
   python scripts/audit_sqlite_paths.py --file connectors.json --data-dir /data --json
   python scripts/audit_sqlite_paths.py --copy-plan > migrate.sh        # prints cp commands
   ```

   Each saved `sqlite` / `duckdb` connector is listed with its resolved path,
   whether the file exists, whether it is inside the proposed root (the explicit
   root if set, else `<data dir>/sqlite|duckdb`), and a verdict: `inside`,
   `outside root`, `service data directory (always refused)`, or `in-memory`.
2. Copy, never move. For every connector `outside root`, run the generated
   `mkdir -p` / `cp -p -n` line (the audit prints it; it never runs it). The
   original stays where it is, so rolling back is just unsetting the flag.
3. Edit each connector to the new path (UI, or `PUT /api/v1/connectors/saved/<id>`
   with `database` set to the file name under the root, e.g. `orders.db`), then
   Test it. Two files with the same name from different folders are flagged in
   the plan; give one a distinct name.
4. Re-run the audit until every SQLite/DuckDB connector says `inside`, check that
   no `local_file.would_confine` events arrive for a day, then set
   `DATAFLOW_CONFINE_LOCAL_FILES=1`.

Iceberg warehouses are not covered by the connector audit script: list
`local_file.would_confine` events with `kind: iceberg`, copy the warehouse
folder under `<data dir>/iceberg`, and update the connector.

---

### PostgreSQL / Redshift TLS requirement (`ssl_mode`)

A saved PostgreSQL or Redshift connector records what the user chose for TLS in `ssl_mode`:

| `ssl_mode` | Meaning | Behaviour |
|---|---|---|
| `require` | The user turned SSL/TLS on (Studio checkbox, or `ssl: true` in the API). | Strict. `sslmode=require` on every read, sample, probe and write; a server without TLS is refused with "TLS is required ... refusing to fall back to plaintext". Never retried in plaintext. |
| `prefer` / `""` (unset) | The user never asked for TLS. | Negotiates TLS (`sslmode=prefer`) and may use plaintext. |

Saving a connector sets `require` when the payload has `ssl: true`, and clears it when `ssl` is false. A payload that does not mention `ssl` leaves the stored choice alone.

**Existing connectors.** Documents saved before `ssl_mode` existed hold only the boolean `ssl`, and the old store defaulted it to true when a caller omitted it, so a stored `ssl=true` cannot be told apart from an injected default. These documents are read as unset: their behaviour is unchanged (negotiate, plaintext allowed). To make an existing connector strictly TLS, open it in Studio and save it with SSL/TLS ticked (or `PUT` it with `ssl: true`). Check production connectors that must be encrypted and re-save them once.

## Text key collation on tables DataFlow creates (MySQL / MariaDB, D205)

MySQL, MariaDB and SQL Server compare `VARCHAR` keys under PAD SPACE
(`''` = `' '`, `'a'` = `'a '`), and a `_ci` / `CI_AS` collation also equates
`'a'` and `'A'`. Two such source keys are one key to the destination's unique
index.

| Rule | Applies | Flag |
|------|---------|------|
| Validate walks the source key population under the destination key column's comparison rules and refuses a keyed load (upsert / merge / overwrite with a declared key) whose distinct source keys the destination would merge. The message names the colliding keys and the remedy: a binary NO PAD collation (MySQL `utf8mb4_0900_bin`, MariaDB `utf8mb4_nopad_bin`), a `_BIN2` collation on SQL Server for case, or a key transform. SQL Server has no NO PAD collation, so keys that differ only by trailing spaces are always refused there. | Always | none |
| Gate-8's keyed read-back selects the written keys exactly (binary / `BIN2`), so run 2 updating `'A'` beside `'a'` is not mistaken for a mismatch. | Always | none |
| A case-sensitive source text column on a table DataFlow **creates** on MySQL / MariaDB gets the NO PAD binary collation `utf8mb4_0900_bin` (MariaDB 11.4+ accepts it; older MariaDB: `utf8mb4_nopad_bin`) instead of `utf8mb4_bin` (PAD SPACE), so `'a'` and `'a '` load as two keys. Tables created earlier keep `utf8mb4_bin`; run 2 into them is unchanged. | New tables only | `DATAFLOW_KEY_COLLATION_NO_PAD=1` (default off) |

With the flag off, each create that would have used the NO PAD collation is
recorded once an hour as an audit event `create_ddl.would_use_no_pad_collation`
(`dialect`, `today`, `with_flag`).

## pgvector destination layout (D209)

A pgvector destination is PostgreSQL with the `vector` extension. It holds one
of two shapes, and the plan (`destination_contract`), the preflight warning
`pgvector_destination_layout` and the job all use the same one:

| Layout | Physical table | Gate-8 |
|--------|----------------|--------|
| `table` (default for a new table) | The typed table the Map shows: one column per mapped field with its type, SQL NULL as NULL, plus `embedding vector(n)` after them (and `embedding_text TEXT` when a `text_template` builds the text). Written and verified as PostgreSQL. | Full PostgreSQL read-back of every source column |
| `document` (opt-in) | `id TEXT` (the source key), `content TEXT` (embedded text), `embedding vector(n)`, `metadata JSONB` (every mapped value, SQL NULL as JSON null), `source_id`, `chunk_index`, `created_at`. | Every source value re-read and found in `metadata` |

Both layouts embed the same text, from one declared rule for every row:
`text_template`, else `content_column` (NULL / '' gives no vector), else
precomputed vectors in `embedding_column`, else the mapped row as canonical
JSON. `n` comes from `embedding_dimensions` or the embedding model; the plan
and the preflight warning name it.

How the layout is chosen, first match wins:

1. `vector_layout` (`table` / `document`) on the run (`plan_transfer` /
   `start_transfer` argument) or in the saved connector's `extra`.
2. An existing table keeps its shape: a table with `id, content, embedding,
   metadata, source_id, chunk_index` stays a document table; any other existing
   table is typed (its embedding column is filled only if it already has one).
   Tables DataFlow created before this change keep working on run 2.
3. A new table: `table` (owner decision, round 8). Round 6's
   `DATAFLOW_PGVECTOR_TYPED_TABLE_DEFAULT` flag is removed; to keep building
   document tables for new destinations, set `vector_layout: document` in the
   connector's `extra`.

Document tables created before this change: run 2 re-embeds documents whose
text changes under the single rule (the old per-row pick of the longest text
column is gone) and adds NULL fields to `metadata` as JSON null.

## Health checks

| Endpoint | Use |
|----------|-----|
| `GET /health` | Load balancer — checks MongoDB, storage, drivers |
| `GET /health-api` (via nginx) | Web container probe |
| `GET /health/ready` | Readiness: returns 503 `degraded` while the connector pool is saturated or its oldest call is too old |

`/health` and `/health/ready` both carry a `connector_pool` object
(`in_flight`, `queued`, `abandoned`, `max_workers`, `oldest_in_flight_seconds`,
`per_workspace`, `abandoned_total`, `rejected_total`). `/health` stays a cheap
liveness check and never fails on it.

### Connector I/O limits (D181)

Pilot/MCP tools that dial out to a customer system (`run_query`,
`test_connector`, `aggregate_data`, `plan_transfer`, `start_transfer`,
`sample_connector_object`, schema introspection, ...) run on their own bounded
worker pool, not the shared asyncio pool. A call that outlives its deadline
returns a clear error naming the host and step; the stuck worker is counted as
`abandoned` until it really ends. A workspace that already has its share of
calls in flight gets an immediate "connector calls in flight" error instead of
queueing. All settings are optional (`DATAWRAP_*` is read before `DATAFLOW_*`).

| Variable | Default | Meaning |
|----------|---------|---------|
| `DATAFLOW_CONNECTOR_POOL_WORKERS` | `16` | Workers in the connector pool |
| `DATAFLOW_CONNECTOR_WORKSPACE_MAX` | `6` | Connector calls in flight per workspace (running, queued and abandoned) |
| `DATAFLOW_CONNECTOR_READY_MAX_AGE_SECONDS` | `300` | `/health/ready` degrades when the oldest in-flight call has run longer |
| `DATAFLOW_QUERY_TOOL_DEADLINE_SECONDS` | `120` | Deadline for `run_query`, `test_connector`, `aggregate_data` |
| `DATAFLOW_TOOL_DEADLINE_SECONDS` | `45` | Deadline for plan/start/sample/introspect tools (existing, D169) |
| `DATAFLOW_DESTINATION_CONNECT_DEADLINE_SECONDS` | `60` | A run fails, naming the destination, when its first contact (reading the destination schema) has not answered in this long; the job pulses while it waits (D19) |
| `DATAFLOW_QUERY_STATEMENT_TIMEOUT_SECONDS` | `120` | Server-side statement cap for interactive SQL (PostgreSQL `statement_timeout`, MySQL `MAX_EXECUTION_TIME`, SQL Server `LOCK_TIMEOUT`); `0` disables |

Connect timeouts are unchanged (15 s default, `connect_timeout` on the connector).
If `/health/ready` reports a saturated pool, look at `per_workspace` for the
tenant holding the workers and at the sources those calls point to.

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

### `DATAWRAP_TENANT_IP_ALLOWLIST_ENFORCE` (kill switch, default on)

Workspace `ip_allowlist` (Settings -> Enterprise). Since QA round 11 (S7) the
list is **enforced as soon as a workspace saves one**: every authenticated
request to that workspace, on the shared app host and on a custom domain, for
sign-in sessions and API keys alike (`WorkspaceIpAllowlistMiddleware`, after
auth, before RBAC). Before, it was evaluated only when the Host header was the
tenant custom domain, which nginx never forwards, so it never applied.

- Outside the list: `403` with `reason: ip_not_in_workspace_allowlist`, and an
  audit event `workspace.ip_allowlist.blocked` (workspace, client IP, path,
  session or API key).
- Which workspace: the `X-Workspace-Id` header; without it every workspace the
  caller can read (an API key: its own; a member: all memberships). A platform
  admin with no header acts deployment-wide and is not evaluated.
- Exempt: the unauthenticated paths (login, signup, SSO, webhooks, health,
  MCP discovery, contact-sales) — the same list as before.
- **Break-glass**: a platform admin outside the list can send
  `X-Datawrap-Break-Glass: <reason, 8+ characters>`; the request is admitted
  and audited as `workspace.ip_allowlist.break_glass` every time. Workspace
  admins cannot use it.
- **Lock-out guard**: saving a list that does not contain the saving admin's
  own address is refused with `400` ("... does not include your current IP
  address (x.x.x.x) ...") and audited as `workspace.ip_allowlist.lockout_refused`.
- **Kill switch**: `DATAWRAP_TENANT_IP_ALLOWLIST_ENFORCE=0` (or `false`/`off`)
  turns every refusal into an admitted request plus a
  `tenant.ip_allowlist.would_block` audit event, for the whole deployment. The
  posture, the compliance report and the save response then say "saved, not
  enforced — ask your platform administrator to enable it".

- **Deploy safety (trusted address only)**: a list is enforced only on a
  client address the server can trust, i.e. `DATAWRAP_TRUSTED_PROXY_COUNT` is
  set (even to `0`) or the resolved address is public. With the count unset
  and a private, loopback, link-local or CGNAT (`100.64/10`) address — what
  every request looks like behind Railway before the count is set — nothing
  is blocked: the request is admitted and audited as
  `tenant.ip_allowlist.would_block` with reason `client_ip_untrusted`, the
  lock-out guard accepts the save, and the save response, posture and report
  show the list as **not enforced** ("the platform's proxy hop count is not
  configured; ask your platform administrator to set it"). The API logs one
  warning at startup when lists exist and the count is unset.

The allowlist is only as good as the client address the API sees: set the
trusted proxy hop count below **before** customers save lists. The save
response's `enforcement.caller_ip` shows the address the server sees.

### `DATAFLOW_WORKSPACE_ADMIN_IAM` (default `1`)

Whether a *workspace* admin (workspace membership role `admin`, own platform
role not `admin`) holds `iam.manage` and `scim.provision`.

| Flag | `/api/v1/iam/*` | `/api/v1/scim/v2/*` | `/api/v1/workspace/api-keys` |
|---|---|---|---|
| `1` (default, pre-hardening behaviour) | workspace admins allowed | workspace admins allowed | workspace admins allowed, own workspace only (see below) |
| `0` (strict) | platform admin only | platform admin only | platform admin only |

"Platform admin" = the principal's own role is `admin` (including an API key
minted by one). With the flag on, every IAM/SCIM request admitted only because
of the flag writes an audit event `authz.workspace_admin_iam.would_deny`
(outcome `skipped`, level `warn`; `required_permission`, `effective_role`,
`path`, `method`).

**Always on, regardless of the flag:** `/workspace/api-keys` only lists and
revokes keys stamped with the caller's own workspace (`workspace_id`, taken
from the key creator's request scope). Keys minted before this field existed,
and keys minted via `/iam/*`, carry no workspace and are visible/revocable
only by a platform admin; a foreign or unscoped key id answers 404 to a
workspace admin. Before this change the key store had no workspace at all and
`/workspace/api-keys` returned every tenant's keys to any caller that passed
the route gate; the permission gate in strict mode was the only protection.

**Residual risk while the flag is `1`:** the `/iam/*` routes (service
accounts list/rotate/revoke, SCIM token mint, group mappings) are
deployment-wide and are *not* workspace-scoped, so a workspace admin can see
and revoke other tenants' service accounts and mint a role-`admin` key
(which is a platform admin). The same is true of SCIM provisioning. Set
`DATAFLOW_WORKSPACE_ADMIN_IAM=0` on any deployment hosting mutually
untrusting tenants.

Owner how-to: leave at `1`, review `authz.workspace_admin_iam.would_deny`
events for who relies on it, move those people to a platform admin role or an
admin-minted key, then set `0`.

### `DATAFLOW_INCREMENTAL_DRIFT_FAILS` (default `0`)

D188. An incremental run (upsert / deduped / append by cursor, composite
watermarks included, and the quiet poll that reads nothing new) is verified on
the rows its cursor window selected. A source row *outside* the window that
disagrees with the destination (edited without moving the cursor, back-filled
with an old cursor value, hard-deleted, or a destination row changed by hand)
is reported on the run as `reconciliation.out_of_window_drift` (count, kinds,
sample keys) and in the run message ("Out-of-window drift: ..."), and the run
is **not** failed for it. A loss inside the window still fails the run.

- `0` (default): drift is reported, the run passes, and an audit event
  `gate8.would_fail_out_of_window_drift` (level `warn`; `rows`,
  `sample_keys`) records that the strict setting would have failed it.
- `1`: out-of-window drift fails the run (fail closed; rows are never rolled
  back or overwritten).

Before D188 a write-pass route (SQL Server -> MySQL, PG -> MySQL COPY, SQL
Server -> PG) failed every later run on such drift and a quiet keyed poll
failed closed on it, while other routes passed without mentioning it.

Owner how-to: leave at `0`; set `1` to treat any divergence as fatal.

---

## Transform-library RBAC (what changed, per role)

The 13 `/api/v1/transform-library/*` routes now carry explicit permissions:
the 6 `GET` routes need `job.read`; the 7 writes (`POST`, `PATCH`, `DELETE`,
including `compile`, `preview`, `upgrade-preview` and template `clone`) need
`connector.write`. This is not behind a flag: it is a rule-table entry that
makes the routes behave like every other reviewed route. Computed from
`services/rbac.py` (`_ROLE_PERMISSIONS`, `_PATH_RULES`, `_fallback_permission`)
and pinned by `tests/test_transform_library_rbac_matrix.py`.

Role tables: `viewer` and `operator` hold `job.read` but not
`connector.write`; `editor` and `admin` hold both.

| Role | GET (6 routes) before -> after | Writes (7 routes) before -> after |
|---|---|---|
| viewer | see below -> allowed | see below -> **denied** |
| operator | see below -> allowed | see below -> **denied** |
| editor | see below -> allowed | see below -> allowed |
| admin | see below -> allowed | see below -> allowed |

"Before" depends on `DATAFLOW_RBAC_UNRULED_ROUTES` (the routes matched no rule):

- **`deny` (the default, also the default before this change):** every
  authenticated request to all 13 routes was rejected with 403 `no_rule`, for
  every role including admin. The change *grants* access: viewer/operator/
  editor/admin read, editor/admin write. No role loses access that worked.
- **`allow_and_log`:** the legacy fallback applied (`GET` -> `job.read`,
  writes -> `connector.write`), which is the same permission the explicit rules
  now name. Nothing changes for any role: viewer/operator could already not
  write, and everyone could already read.

Neither mode ever let a viewer or operator write recipes through these routes
in the code as it stands, so no currently-working role loses access. (The
commit message of `fix(rbac): the transform-library routes carry explicit
permissions` describes "unruled = everyone allowed"; that does not match the
fallback table above.) Deployments that actually saw viewers or operators
saving recipes were running with RBAC off (`DATAFLOW_REQUIRE_AUTH=0`), where
this middleware is skipped and nothing changes.

---

## `scripts/backfill_legacy_workspaces.py` (legacy workspace backfill, D154)

Run from `apps/api`. **Dry run is the default**; nothing is written unless
`--apply` is passed (`--dry-run` and `--apply` are mutually exclusive).
It is never run at startup. Run it before turning self-serve signup on
(workspace isolation), so existing owners keep their history.

```bash
python scripts/backfill_legacy_workspaces.py                       # dry run, prints a JSON report
python scripts/backfill_legacy_workspaces.py --apply
python scripts/backfill_legacy_workspaces.py --apply --owner-email ops@example.com
```

**Exactly what `--apply` changes.** It sets the single field `workspace_id` (for
contracts: `metadata.workspace_id`) on rows whose workspace is empty/missing,
and nothing else. It never deletes, never touches a row that already has a
workspace, and never touches audit events (their HMAC chain covers every field;
unscoped audit rows stay platform-admin only). Per store:

| Report key | Rows stamped | Owner decided by |
|---|---|---|
| `connectors` (file/connector store) | unscoped connectors | owner field on the row, else the one account whose legacy jobs/schedules use it, else platform owner |
| `job_store_connectors` | unscoped connector docs in the job store | same |
| `jobs` | unscoped jobs (`updated_at` not bumped) | `created_by` / `owner_email` / `owner` / `triggered_by` (also inside `transfer_request`) naming a real account, else platform owner |
| `schedules` | unscoped schedules | owner field, else the one account whose jobs ran it, else platform owner |
| `contracts`, `transform_recipes` | unscoped | the one workspace of the schedules using them, else platform owner's workspace |
| `transform_projects` | unscoped | the workspace of the destination connector, else platform owner's |
| `quarantine_dlq`, `usage_events` | unscoped events | the owner of the job the event belongs to (an event it cannot place is `skipped`) |
| `preflight_runs`, `transfer_plans`, `shape_drafts` | unscoped | always the platform owner's workspace (no ownership evidence exists) |

An owner's workspace is their personal workspace, else their earliest
membership. An owner on record without any workspace falls back to the
platform owner. The platform owner is `--owner-email`, else the bootstrap admin
(`DATAWRAP_ADMIN_EMAIL`/`DATAFLOW_ADMIN_EMAIL`), else the earliest stored admin.
The only thing ever *created* is one workspace for the platform owner, and only
if some row needs it and they have none.

**Idempotent:** only unscoped rows are selected, so a second `--apply` reports
`assigned: 0` everywhere and creates nothing. A row whose write fails is counted
under `skipped`, logged, and retried by the next run. The DLQ JSONL fallback and
the JSONL stores are rewritten atomically; run `--apply` in a maintenance window
because concurrent appends are not locked against that rewrite.

**Dry run** prints the same counts as `--apply` would produce (`applied: false`)
and writes nothing, including no workspace creation (a would-be workspace shows
as `(new workspace for <email>)`). Verified on throwaway stores by
`tests/test_backfill_legacy_workspaces_cli.py` (file-tree hash unchanged after a
dry run and after a second apply). Take a backup of the Mongo database and the
data directory before the first `--apply`.

## Brand: names the product leaves in customer systems (`DATAWRAP_BRAND_OBJECT_NAMES`)

The product is Datawrap. Text people read (API and MCP messages, the web app,
download file names, e-mail sender defaults, the certificate PDF, the MCP
server name `datawrap`) says Datawrap unconditionally; the product name comes
from `services/brand.py` (`PRODUCT_NAME`, `PRODUCT_SLUG`) and, in the web app,
`apps/web/src/lib/brand.ts`.

Objects the product writes into a customer's systems are different: existing
customers already hold them under the old `dataflow` / `_df_` names and the
next run has to keep working against them. `services/brand.ARTIFACTS` lists
every such name. The rules:

| Flag | Default | Effect |
|---|---|---|
| `DATAWRAP_BRAND_OBJECT_NAMES` (`DATAFLOW_BRAND_OBJECT_NAMES` also read) | `0` | `1`: a **new** artifact of a switchable kind gets the new name. |

* An artifact that already exists is used under the name it has, old name
  first. Nothing is ever renamed. Both names are recognised as the product's
  own (for example a table commented `dataflow:managed` **or**
  `datawrap:managed` may get the upsert index without the operator-table DDL
  gate; any other comment still needs approval).
* Turning the flag off again keeps using artifacts created under the new name
  (the CDC watermark table is resolved per destination, so state never forks).
* Every artifact created under a new name writes an audit event
  `brand.object_name.created` (`details.kind`, `name`, `legacy_name`).

Switched by the flag in this build:

| Kind | Old name | New name |
|---|---|---|
| PostgreSQL `application_name` | `dataflow` | `datawrap` |
| Table ownership comment (PostgreSQL, MySQL; row writers and the MySQL COPY routes) | `dataflow:managed` | `datawrap:managed` |
| Exactly-once CDC watermark table (SQLAlchemy destinations: PostgreSQL, MySQL, SQL Server, Snowflake, …) | `_df_cdc_eos_watermarks` (Oracle `df_cdc_eos_watermarks`) | `_dw_cdc_eos_watermarks` (Oracle `dw_cdc_eos_watermarks`) |
| Prefix of a newly minted workspace API key (both prefixes always authenticate) | `dfk_` | `dwk_` |

Recognised and listed, but still created under the old name even with the flag
on (their run-to-run identity is not yet resolved per destination; switching
them is a separate change): the CDC `_df_lsn` column and other `_df_*`
bookkeeping columns, `_dataflow_write_ledger`, `<table>_df_quarantine`,
`dataflow_file_manifest`, `_df_vector_collections`, the MySQL row-backup
comment `dataflow:overwrite-row-backup`,
`dataflow_signal` (source database), `<table>__df_bak*` backups, staging
prefixes (`_df_stg_`, `_dataflow_stg_`, `_df_mirrorkeys_`, `_df_merge_stage_`,
`_df_upsert_stage_`, `_df_copy_ups_`, `_DF_UPSERT_`, `df_mrg_`), `df_enum_*`
types, `df_*` replication slots / publications, Kafka group ids
`dataflow-kafka-*`, object-store staging keys, the Redshift stage prefix,
Iceberg `dataflow.*` snapshot properties, default vector collections, the
SQLite-native watermark table, temporary `.dataflow-*` files on SFTP/local
destinations, SaaS idempotency keys `dataflow-*`, and the defaults used when a
connector names none: BigQuery dataset `dataflow`, Snowflake schema
`DATAFLOW`, Kafka topic `dataflow.events`, object-store key
`exports/dataflow_export.json`, Redis prefix `dataflow`.

### Before turning the flag on

1. List what a destination already holds (read-only, renames nothing):

   ```bash
   cd apps/api
   python scripts/brand_artifacts_dry_run.py --type postgresql \
     --config '{"host":"…","port":5432,"database":"…","username":"…","password":"…","schema":"public"}'
   ```

   Each entry has `kind`, `object`, `named` (`legacy`/`new`), `new_name` and
   `action: keep`.
2. Set `DATAWRAP_BRAND_OBJECT_NAMES=1` on every API, worker and scheduler
   process (a mixed fleet would name new artifacts inconsistently; existing
   ones are unaffected either way).
3. Do not rename old artifacts by hand while pipelines run against them. A
   renamed watermark table makes the next CDC run start a new one and
   re-snapshot; a renamed `_df_lsn` column breaks the monotonic-apply guard.

### Staged rollout runbook (owner decision: new objects only, flag first)

The flag stays **off** by default in every environment. Turn it on in staging
first, prove that old and new names both work, then turn it on in production.
`tests/test_r8_brand_flag_rollout_e2e.py` runs this sequence end to end
(two API keys, two runs, flag off → on → off).

1. **Staging: enable.** Run the dry run above against each staging
   destination and keep its output. Set `DATAWRAP_BRAND_OBJECT_NAMES=1` on every
   staging API, worker and scheduler process and restart them together.
2. **Staging: check that old names still work.**
   * An existing `dfk_` key still authenticates (any authenticated call, e.g.
     `GET /api/v1/workspace/pilot-engine` with `Authorization: Bearer dfk_…`).
   * A scheduled or manual run into a table created before the switch
     (comment `dataflow:managed`) succeeds, and the table keeps its comment and
     name. A CDC destination keeps using `_df_cdc_eos_watermarks` (no
     re-snapshot).
3. **Staging: check that new names work.**
   * A key minted now starts with `dwk_` and authenticates.
   * A run that creates a new table marks it `datawrap:managed`; run 2 into it
     upserts. A new CDC destination creates `_dw_cdc_eos_watermarks`.
   * Each new-name creation appears once in the audit log as
     `brand.object_name.created` (`details.kind`, `name`, `legacy_name`).
   * Re-run the dry run: every pre-existing artifact is still listed with
     `action: keep`.
4. **Staging: check rollback.** Unset the flag, restart, and repeat steps 2
   and 3 for the objects created in step 3: `dwk_` keys still authenticate and
   `datawrap:managed` tables and `_dw_` watermark tables keep being used. New
   objects go back to the old names. Re-enable the flag.
5. **Production: enable.** Repeat step 1 in production (dry run first, then the
   flag on every process at once), then repeat the spot checks of steps 2 and 3
   on one low-risk pipeline and one API key before announcing the change.
6. **If anything fails:** unset the flag and restart. Nothing was renamed, so
   every pipeline and key keeps working; objects created under the new names
   while the flag was on keep working as they are.
