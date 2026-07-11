# DataFlow — Production Deployment Guide

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
| `DATAFLOW_SEED_DEMO` | `0` | No demo connectors |

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
