# Datawrap Railway env rename checklist

The app now prefers **`DATAWRAP_*`** and still accepts **`DATAFLOW_*`** (dual-read).  
Nothing breaks if you keep the old names. Rename when ready — **do not delete old vars until the new ones are set and the deploy is healthy**.

## Cutover steps (per Railway service: api / worker / web)

1. Add the new `DATAWRAP_*` variable with the **same value** as the current `DATAFLOW_*`.
2. Redeploy (or wait for the next deploy).
3. Smoke-test login, transfer, Pilot, and health.
4. Remove the old `DATAFLOW_*` variable.

Web nginx also accepts `DATAWRAP_API_BASE` first, then `DATAFLOW_API_BASE`, then `VITE_API_BASE`.

## High-priority vars (rename these first)

| Old (keep until cutover) | New (add first) |
|--------------------------|-----------------|
| `DATAFLOW_ENV` | `DATAWRAP_ENV` |
| `DATAFLOW_REQUIRE_AUTH` | `DATAWRAP_REQUIRE_AUTH` |
| `DATAFLOW_AUTH_SECRET` | `DATAWRAP_AUTH_SECRET` |
| `DATAFLOW_AUTH_USERS` | `DATAWRAP_AUTH_USERS` |
| `DATAFLOW_ADMIN_EMAIL` | `DATAWRAP_ADMIN_EMAIL` |
| `DATAFLOW_ADMIN_PASSWORD` | `DATAWRAP_ADMIN_PASSWORD` |
| `DATAFLOW_SECRETS_KEY` | `DATAWRAP_SECRETS_KEY` |
| `DATAFLOW_WEB_DOMAIN` | `DATAWRAP_WEB_DOMAIN` |
| `DATAFLOW_WEB_URL` | `DATAWRAP_WEB_URL` |
| `DATAFLOW_PUBLIC_URL` | `DATAWRAP_PUBLIC_URL` |
| `DATAFLOW_API_BASE` | `DATAWRAP_API_BASE` |
| `DATAFLOW_API_URL` | `DATAWRAP_API_URL` |
| `DATAFLOW_API_TOKEN` | `DATAWRAP_API_TOKEN` |
| `DATAFLOW_DATA_DIR` | `DATAWRAP_DATA_DIR` |
| `DATAFLOW_UPLOAD_DIR` | `DATAWRAP_UPLOAD_DIR` |
| `DATAFLOW_VECTOR_STORE_DIR` | `DATAWRAP_VECTOR_STORE_DIR` |
| `DATAFLOW_VOLUME_PATH` | `DATAWRAP_VOLUME_PATH` |
| `DATAFLOW_WORKER_FLEET` | `DATAWRAP_WORKER_FLEET` |
| `DATAFLOW_WORKER_MODE` | `DATAWRAP_WORKER_MODE` |
| `DATAFLOW_MULTI_REPLICA` | `DATAWRAP_MULTI_REPLICA` |
| `DATAFLOW_JOB_STORE` | `DATAWRAP_JOB_STORE` |
| `DATAFLOW_REDIS_URL` | `DATAWRAP_REDIS_URL` |
| `DATAFLOW_CDC_LEASE_REDIS_URL` | `DATAWRAP_CDC_LEASE_REDIS_URL` |
| `DATAFLOW_CDC_LEASE_BACKEND` | `DATAWRAP_CDC_LEASE_BACKEND` |
| `DATAFLOW_S3_*` | `DATAWRAP_S3_*` |
| `DATAFLOW_SSO_*` / `DATAFLOW_SAML_*` | `DATAWRAP_SSO_*` / `DATAWRAP_SAML_*` |
| `DATAFLOW_TRAINING` | `DATAWRAP_TRAINING` |
| `DATAFLOW_SEED_DEMO` | `DATAWRAP_SEED_DEMO` |
| `DATAFLOW_ENABLE_DOCS` | `DATAWRAP_ENABLE_DOCS` |
| `DATAFLOW_REQUIRE_WORKSPACE` | `DATAWRAP_REQUIRE_WORKSPACE` |
| `DATAFLOW_EMAIL_*` / `DATAFLOW_SMTP_*` | `DATAWRAP_EMAIL_*` / `DATAWRAP_SMTP_*` |

Rule: every `DATAFLOW_FOO` becomes `DATAWRAP_FOO` (same suffix).

## Leave alone for now (migration risk)

These are **not** renamed in this cutover — changing them mid-flight breaks live systems:

- CDC signal table / collection name `dataflow_signal`
- Redis CDC lease key prefix `df:cdc:*`
- Prometheus metric names `dataflow_*`
- GitOps `apiVersion: dataflow.space/v1`
- npm packages `@dataflow/*`, CLI module `dataflow_cli`
- CSS prefixes `--df-*` / `.df2-*` (internal only)

## Domains / emails (optional DNS)

Docs and UI now say `datawrap.app` / `sales@datawrap.app`. Point DNS when you own the domain; Railway service hostnames can stay as-is.

## Automatic pickup

After this code ships, **existing `DATAFLOW_*` Railway vars keep working**.  
When you add `DATAWRAP_*` duplicates, the new names win automatically — no further code change required.
