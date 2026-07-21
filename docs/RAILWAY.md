# Deploy DataFlow on Railway

Recommended production shape: **Web + API + Worker + MongoDB** — not a microservice per feature.

## Architecture

```
┌──────────────┐   HTTPS    ┌──────────────┐
│ Web          │ ─────────► │ API          │  control plane
│ (React/nginx)│            │ FastAPI      │
└──────────────┘            └──────┬───────┘
                                   │ enqueue job_id (DATAFLOW_WORKER_FLEET=1)
                                   ▼
                            ┌──────────────┐
                            │ MongoDB      │  jobs · leases · queue · repair · CDC signals
                            └──────┬───────┘
                                   │ claim
                                   ▼
                            ┌──────────────┐
                            │ Worker       │  data plane (same Docker image, start-worker.sh)
                            └──────────────┘
```

| Service | Role | Scale independently? |
|---------|------|----------------------|
| **Web** | UI | Yes |
| **API** | Auth, Studio, enqueue | Yes |
| **Worker** | Transfers / CDC | Yes — add replicas for throughput |
| **MongoDB** | Shared control plane | Managed plugin / Atlas |

You do **not** need one Railway service per connector. That keeps the platform maintainable.

---

## Step 1 — Create Railway project

1. Go to [railway.app](https://railway.app) → **New Project** → **Deploy from GitHub repo**
2. Select this repository

---

## Step 2 — Add MongoDB

1. In the project → **+ New** → **Database** → **MongoDB**
2. Railway creates `MONGO_URL` automatically

---

## Step 3 — Deploy API service

1. **+ New** → **GitHub Repo** (same repo) — name it `dataflow-api`
2. **Settings** → **Config file path**: `deploy/railway/api.toml`
3. **Settings** → **Networking** → **Generate Domain** (e.g. `dataflow-api-production.up.railway.app`)

### API variables (Settings → Variables)

Run locally first:

```bash
./scripts/generate-production-secrets.sh
```

Then set in Railway:

| Variable | Value |
|----------|--------|
| `DATAFLOW_AUTH_SECRET` | from script |
| `DATAFLOW_SECRETS_KEY` | from script |
| `DATAFLOW_AUTH_USERS` | JSON admin user from script |
| `MONGODB_URI` | `${{MongoDB.MONGO_URL}}` (reference syntax) |
| `DATAFLOW_WEB_DOMAIN` | your web service domain (step 4) |
| `DATAFLOW_WORKER_FLEET` | `1` only after Worker is deployed (default **OFF** — unset does not auto-enable) |
| `DATAFLOW_S3_BUCKET` + keys | Optional object store so Workers can read uploads |
| `DATAFLOW_S3_ENDPOINT` | Optional (R2/MinIO/S3-compatible) |

**Link MongoDB:** In API service → Variables → **Add Reference** → `MongoDB` → `MONGO_URL` → map to `MONGODB_URI`

### Persistent volume (recommended)

1. API service → **Volumes** → **Add Volume**
2. Mount path: `/data`
3. Variables already point to `/data/data`, `/data/uploads`, etc.

Without a volume, **file uploads** need an object store (`DATAFLOW_S3_BUCKET` + keys)
so Workers can materialize files. Jobs, repair, CDC signals, DLQ, and upload registry
live in **Mongo**.

---

## Step 3b — Deploy Worker service (data plane)

Same Docker image as the API; different start command. This is the HA / scale leap.

1. **+ New** → **GitHub Repo** (same repo) — name it `dataflow-worker`
2. **Settings** → **Config file path**: `deploy/railway/worker.toml`
3. Link the **same** MongoDB + secrets as the API
4. Variables:

| Variable | Value |
|----------|--------|
| `DATAFLOW_WORKER_FLEET` | `1` |
| `MONGODB_URI` | same Mongo reference as API |
| `DATAFLOW_AUTH_SECRET` | same as API |
| `DATAFLOW_SECRETS_KEY` | same as API (connector decrypt) |

5. Scale: Worker → **Replicas** (2–N) for parallel transfers.

With fleet mode on, the API enqueues; workers claim under leases. HTTP and sync CPU no longer share one process.

---

## Step 4 — Deploy Web service

1. **+ New** → **GitHub Repo** (same repo) — name it `dataflow-web`
2. **Settings** → **Config file path**: `deploy/railway/web.toml`
3. **Settings** → **Networking** → **Generate Domain**

### Web build variable

| Variable | Value |
|----------|--------|
| `VITE_API_BASE` | `https://YOUR-API-DOMAIN.up.railway.app/api/v1` |

Use the exact API domain from Step 3.

### Update API CORS

Back on **API service**, set:

```
DATAFLOW_WEB_DOMAIN=your-web-domain.up.railway.app
```

Or:

```
CORS_ORIGINS=https://your-web-domain.up.railway.app
```

Redeploy API after changing CORS.

---

## Step 5 — Verify

```bash
curl https://YOUR-API-DOMAIN.up.railway.app/health
curl https://YOUR-API-DOMAIN.up.railway.app/api/v1/transfer/readiness
curl https://YOUR-API-DOMAIN.up.railway.app/api/v1/transfer/platform
```

Expected: `"status": "healthy"` or `"degraded"` (degraded = check mongodb section).  
`/transfer/readiness` should report `"ready": true` when all 18 drivers are wired.  
`/transfer/platform` shows honest `transfer_ready` count (native drivers only, not catalog aliases).

Open `https://YOUR-WEB-DOMAIN.up.railway.app` → sign in with your admin user.

---

## CLI deploy (optional)

```bash
npm i -g @railway/cli
railway login
railway link

# API
railway up --service dataflow-api -c deploy/railway/api.toml

# Worker
railway up --service dataflow-worker -c deploy/railway/worker.toml

# Web
railway variables set VITE_API_BASE=https://xxx.up.railway.app/api/v1 --service dataflow-web
railway up --service dataflow-web -c deploy/railway/web.toml
```

---

## Troubleshooting

| Issue | Fix |
|-------|-----|
| API crash on boot | Check **Deploy Logs** — usually missing `DATAFLOW_AUTH_SECRET` or `MONGODB_URI` |
| CORS error in browser | Set `DATAFLOW_WEB_DOMAIN` on API to match web URL exactly |
| Data Pilot “Could not reach” | Web nginx proxies `/api/` → API. Set `DATAFLOW_API_BASE=https://YOUR-API.up.railway.app/api/v1` on **web** (runtime) and redeploy web. Confirm you are signed in. Chat uses a 120s timeout. |
| Login fails | Verify `DATAFLOW_AUTH_USERS` JSON is valid one-line string |
| Jobs empty / transfer fails | Confirm MongoDB reference is linked to `MONGODB_URI` |
| Jobs stay pending forever | Deploy Worker + set `DATAFLOW_WORKER_FLEET=1` on **both** API and Worker |
| Connectors lost after deploy | Add **Volume** mounted at `/data` |
| Build timeout | API image is large (~2GB RAM recommended) — upgrade Railway plan if needed |
| 502 on API | `/health` is fast liveness; RAG warm-up runs in background. If deploy fails for 5m, check Deploy Logs for `[FATAL] Production config` or crash — path is still `/health`. |

---

## Resource recommendations

| Service | Railway plan hint |
|---------|-------------------|
| **API** | ≥ 1–2 GB RAM (lighter once fleet offloads syncs) |
| **Worker** | ≥ 2 GB RAM — scale **replicas** here for throughput |
| **Web** | 512 MB is enough |
| **MongoDB** | Railway Mongo plugin or MongoDB Atlas M0+ |

---

## Environment reference

See [.env.railway.example](../.env.railway.example) for all variables.

Railway auto-sets: `PORT`, `RAILWAY_ENVIRONMENT`, `RAILWAY_PUBLIC_DOMAIN`, `RAILWAY_SERVICE_ID`.

The API reads `MONGO_URL` automatically when `MONGODB_URI` is not set.
