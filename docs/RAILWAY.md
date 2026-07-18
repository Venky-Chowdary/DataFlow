# Deploy DataFlow on Railway

Two Railway services + MongoDB plugin. Estimated cost: ~$5–20/mo (API + Web + MongoDB on Railway).

## Architecture

```
┌─────────────────────┐     HTTPS      ┌─────────────────────┐
│  Web service        │ ──────────────►│  API service        │
│  Dockerfile.railway │   API calls    │  Dockerfile.api     │
│  .web               │                │  PORT from Railway  │
└─────────────────────┘                └──────────┬──────────┘
                                                  │
                                                  ▼
                                         ┌─────────────────┐
                                         │ MongoDB plugin  │
                                         │ (MONGO_URL)     │
                                         └─────────────────┘
```

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

**Link MongoDB:** In API service → Variables → **Add Reference** → `MongoDB` → `MONGO_URL` → map to `MONGODB_URI`

### Persistent volume (recommended)

1. API service → **Volumes** → **Add Volume**
2. Mount path: `/data`
3. Variables already point to `/data/data`, `/data/uploads`, etc.

Without a volume, connectors, uploads, audit logs, and workspace settings are **lost on redeploy**.

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
| Connectors lost after deploy | Add **Volume** mounted at `/data` |
| Build timeout | API image is large (~2GB RAM recommended) — upgrade Railway plan if needed |
| 502 on API | Boot takes 60–120s (ML models) — healthcheck timeout is 300s |

---

## Resource recommendations

| Service | Railway plan hint |
|---------|-------------------|
| **API** | ≥ 2 GB RAM (sentence-transformers + drivers) |
| **Web** | 512 MB is enough |
| **MongoDB** | Railway Mongo plugin or MongoDB Atlas M0 |

---

## Environment reference

See [.env.railway.example](../.env.railway.example) for all variables.

Railway auto-sets: `PORT`, `RAILWAY_ENVIRONMENT`, `RAILWAY_PUBLIC_DOMAIN`, `RAILWAY_SERVICE_ID`.

The API reads `MONGO_URL` automatically when `MONGODB_URI` is not set.
