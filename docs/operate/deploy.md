# Deploy Datawrap

This page explains what runs, what it needs, and how to choose between one
process and several. For the full list of settings see
[configuration.md](configuration.md).

## What runs

Datawrap ships as two container images.

| Image | Built from | What it is |
|---|---|---|
| API image | `Dockerfile.api` | The Python service. The same image runs as the API, a worker or the scheduler, depending on the start command. |
| Web image | `Dockerfile.web` (or `Dockerfile.railway.web`) | The browser app, served as static files by nginx, which also proxies `/api/v1` to the API. |

The API image has three start commands.

| Role | Start command | What it does | Listens on HTTP |
|---|---|---|---|
| API | `scripts/start-api.sh` | Serves `/api/v1`. Accepts transfers and either runs them itself or puts them on the queue. | Yes, port `$PORT` (default 8000), one uvicorn worker |
| Worker | `scripts/start-worker.sh` | Claims jobs from the queue under a lease and runs them. | No |
| Scheduler | `scripts/start-scheduler.sh` | Fires due pipelines by putting them on the queue. Never runs a transfer. | No |

A worker can be limited to one kind of work with `DATAWRAP_WORKER_MODE`:
`batch`, `cdc`, or both when unset. The production compose file and the Helm
chart run one batch worker deployment and one CDC worker deployment from the
same command.

The worker and scheduler commands set their own role flags at start, so a
mis-set role variable in the environment cannot turn a worker into an API.

## Backing services

| Service | Needed for | Notes |
|---|---|---|
| MongoDB | Jobs, the job queue, leases, schedules, connectors, audit events and account data | Required in production: the API refuses to serve when the connection string is missing or points at localhost. The database name is `datatransfer`. |
| Redis | CDC source leases when more than one process runs CDC | Set `DATAWRAP_CDC_LEASE_REDIS_URL`. Without it CDC leases are kept in a local file. |
| A persistent volume | State files, uploads, the local vector store, local exports | Mount it and point `DATAWRAP_DATA_DIR` and `DATAWRAP_UPLOAD_DIR` at it. In production the API logs a warning at start when the data directory is on the container's own filesystem. |
| Object storage (S3 or compatible) | Staging uploaded files and exports so every process can read them | Optional on one node. See the `S3_*`, `EXPORT_*` and `BLOB_ROOT` rows in the configuration reference. |

PostgreSQL appears in `docker-compose.yml` and in the Helm values, but as a
database to transfer to and from during development. The service keeps its own
state in MongoDB and files, not in PostgreSQL. See
[backup-and-upgrade.md](backup-and-upgrade.md) for exactly what lives where.

## Option 1: one node

This is the simplest supported layout: one API process that also runs
transfers and fires pipelines, plus the web container.

- Leave `DATAWRAP_PROCESS_ROLE`, `DATAWRAP_MULTI_REPLICA` and
  `DATAWRAP_WORKER_FLEET` unset.
- The API runs transfers on its own thread pool and runs the pipeline cadence
  loop itself.
- Run exactly one API process. A second one would start its own thread pool
  and its own cadence loop.

For local development, `docker-compose.yml` starts the databases only
(PostgreSQL, MySQL, MongoDB as a single-node replica set, Redis, MinIO and
emulators). You start the API and web app yourself.

## Option 2: several processes

Use this when you want more than one API replica, or transfers that survive an
API restart. `docker-compose.prod.yml` is the reference: it runs `api`,
`scheduler`, `worker`, `cdc-worker`, `web`, MongoDB and Redis.

Set these on every process:

| Setting | API | Scheduler | Worker |
|---|---|---|---|
| `DATAWRAP_MULTI_REPLICA` | `1` | `1` | `1` |
| `DATAWRAP_PROCESS_ROLE` | `api` | `scheduler` | `worker` |
| `DATAWRAP_WORKER_FLEET` | `1` | `1` | `1` |
| `DATAWRAP_API_CLAIM_LOOP` | `0` | `0` | `0` |
| `DATAWRAP_SCHEDULE_LOOP` | `0` | `1` | `0` |
| `DATAWRAP_ACK_BACKEND` | `mongo` | `mongo` | `mongo` |
| `MONGODB_URI` | same value everywhere | | |
| `DATAWRAP_AUTH_SECRET`, `DATAWRAP_SECRETS_KEY`, `DATAWRAP_AUDIT_HMAC_KEY` | same value everywhere | | |

With `DATAWRAP_MULTI_REPLICA=1` each process checks its own settings at start
and stops when the split is unsafe. A worker or scheduler exits with code 2. The API
stays up for health checks and refuses every other route. It refuses:

- local scheduler mode, which would run a thread pool on every API replica;
- an API with the cadence loop or the claim loop left on;
- a worker with the cadence loop on, or a scheduler with it off;
- a file-based approval ledger, which other replicas cannot see.

A second scheduler replica may run as a standby; one Mongo lock keeps a single
beat.

### What is not shared between replicas today

The topology check covers the scheduler, the claim loop and the approval
ledger. It does not cover every store. The following are kept in files under
the data directory or in process memory with no shared backend, so each
replica has its own copy unless all replicas mount the same volume:

- workspace settings in `tenants.json` (custom domain, data region, session
  timeout, IP allowlist, require-SSO);
- customer-managed key records, resource ACLs, notification channels, transfer
  plans, preflight run history, transform drafts, the MCP call log, audit tip
  anchors;
- login rate-limit counters and lockouts, and in-progress Validate runs.

The production compose file avoids the problem by mounting the same four
volumes into every container. If your replicas do not share a volume, treat
these features as single-replica.

## The Helm chart

A chart lives in `deploy/enterprise/helm/dataflow`, with Terraform for AWS in
`deploy/enterprise/terraform/aws`. **A separate change is fixing the chart; do
not rely on it as shipped at this commit.** What the templates expect:

- Five deployments: API (3 replicas), web (2), worker (2, mode `batch`),
  CDC worker (1, mode `cdc`) and scheduler (1), with the role settings from the
  table above.
- Common settings on every pod: `DATAFLOW_MULTI_REPLICA=1`,
  `DATAFLOW_JOB_STORE=mongodb`, `DATAFLOW_CDC_LEASE_BACKEND=redis`, a Redis
  URL, a MongoDB URL and an S3 bucket name.
- Passwords are referenced as `$(REDIS_PASSWORD)`, `$(POSTGRES_PASSWORD)` and
  `$(MONGODB_PASSWORD)` and are only supplied when `externalSecrets.enabled`
  is true, which is not the default.
- API probes: liveness `/health`, readiness `/health/ready`.
- A read-only root filesystem and a non-root user, with no volume declared in
  the templates. The service writes state files, uploads and exports to disk,
  so it needs a writable data volume.

The chart was read, not installed. Nothing here is a claim that it deploys.

## Health and metrics

| Path | Meaning |
|---|---|
| `GET /health` and `GET /api/v1/health` | Liveness. Cheap; does not touch MongoDB. Reports `status: misconfigured` when production settings are incomplete, while every other route stays closed. |
| `GET /health/ready` | Readiness. Returns 503 until warm-up completes and dependency checks pass. |
| `GET /metrics` | Prometheus text for job, row and CDC counters. Requires the `workspace.manage` permission. |

The metrics are counters held inside each process. A process that runs no
transfers reports zeros, and workers expose no HTTP port, so `/metrics` on the
API does not describe a worker fleet.

## Client IP behind proxies

The API is started with `--no-proxy-headers` and works out the client address
itself. Set `DATAWRAP_TRUSTED_PROXY_COUNT` to the number of proxies you
control in front of the API. With a count of N, the client address is the
entry N from the right in `X-Forwarded-For`, and the scheme comes from
`X-Forwarded-Proto`. With the default of 0 nothing is rewritten and every
caller appears to come from the proxy.

This matters for the workspace IP allowlist, rate limits and the address
recorded in audit events. Set the count explicitly, even to 0: the allowlist
only enforces on an address it can trust.

## Web container

- `Dockerfile.web` builds the app with `VITE_API_BASE=/api/v1` and serves it
  with `deploy/nginx.conf`, which proxies `/api/v1` to a host named `api` on
  port 8000.
- `Dockerfile.railway.web` uses `deploy/nginx.conf.template` and needs
  `DATAWRAP_API_BASE` at run time; it refuses to start without it.

## Not covered

- Sizing, throughput and capacity guidance. No measured figures are in this
  repository at this commit.
- TLS termination, DNS and ingress for your platform.
- High availability of MongoDB and Redis themselves.
- Graceful shutdown: workers handle a keyboard interrupt only. There is no
  SIGTERM drain, so a stopped worker's job waits for its lease to expire and is
  then resumed or settled by the orphan sweep (see [runbooks.md](runbooks.md)).
- The Terraform module. It was not applied or reviewed for this page.

## Verified against

- `Dockerfile.api`, `Dockerfile.web`, `Dockerfile.railway.web`
- `scripts/start-api.sh`, `scripts/start-worker.sh`, `scripts/start-scheduler.sh`
- `apps/api/src/worker_main.py`, `apps/api/src/scheduler_main.py`
- `apps/api/services/process_role.py`, `apps/api/services/scheduler_mode.py`,
  `apps/api/services/worker_leases.py`
- `apps/api/services/platform_config.py` (production checks, data directory,
  storage warning)
- `apps/api/services/mongodb_service.py` (database name)
- `apps/api/src/main.py` (health, readiness, metrics routes)
- `apps/api/src/middleware/proxy_fix_middleware.py`, `apps/api/services/client_ip.py`
- `docker-compose.yml`, `docker-compose.prod.yml`
- `deploy/enterprise/helm/dataflow/values.yaml` and `templates/`
- `deploy/nginx.conf`, `deploy/nginx.conf.template`, `deploy/railway/*.toml`
- `apps/api/tests/test_process_topology.py`
