# DataFlow — Universal One-Click Data Transfer Platform

**Any data · any source · any destination · one click**

Enterprise platform for all data operations: upload, migration, dump, transfer, and format conversion — with AI semantic mapping and fail-fast preflight gates.

## Supported operations

- **File → Database** — CSV, Excel, JSON, Parquet, PDF, Word, fixed-width, any format
- **Database → Database** — configure connection strings, migrate with schema + data
- **Database → File** — dump to CSV, Excel, JSON, Word, SQL
- **File → File** — format conversion (e.g. CSV → Word)
- **API → Database** — OpenAPI / REST sources

See [docs/PRODUCT_SCOPE.md](docs/PRODUCT_SCOPE.md) for full scope.

## Architecture

- **apps/web** — React 19 UI (3-screen one-click flow)
- **apps/api** — FastAPI orchestrator
- **packages/preflight** — 8-gate validation engine
- **packages/ml** — Synthetic schema factory + training pipeline
- **design/tokens** — Precision Data design system tokens

## Quick start

```bash
# Infrastructure
docker compose up -d

# API (terminal 1)
cd apps/api && pip install -e "../../packages/preflight[dev]" && pip install -r requirements.txt
npm run dev:api

# Web (terminal 2)
npm install
npm run dev
```

Open **http://localhost:5173** — dashboard with operations overview, connector catalog, and job history.  
Click **New transfer** for the 3-step wizard with real file upload and semantic mapping.

### Production deploy

See **[docs/DEPLOYMENT.md](docs/DEPLOYMENT.md)** for Docker, Varity, and environment setup.

```bash
cp .env.production.example .env
./scripts/generate-production-secrets.sh   # paste into .env
npm run docker:prod
```

### Railway deploy

See **[docs/RAILWAY.md](docs/RAILWAY.md)** — two services (API + Web) + MongoDB plugin.

```bash
npm run railway:secrets   # generate auth secrets
# Then follow docs/RAILWAY.md to set Railway variables
```

### Live CSV → PostgreSQL transfer

```bash
docker compose up -d   # Postgres on localhost:5432 (dataflow/dataflow)
```

In the wizard: upload CSV → destination PostgreSQL (`localhost`, `5432`, `dataflow`, user/pass `dataflow`) → test connection → transfer.  
Data lands in `public.df_<filename>_<id>` with Gate 8 reconciliation report.

## Preflight gates

All transfers pass 8 gates before any production data is written. See `packages/preflight/README.md`.
