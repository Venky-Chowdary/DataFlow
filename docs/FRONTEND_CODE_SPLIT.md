# Frontend code-split (Phase F9)

## What changed

* Vite no longer forces `inlineDynamicImports` — route and vendor chunks are separate hashed files.
* **Overview is eager.** It is the signed-in home screen and must not depend on a
  separate `DashboardPage-*.js` fetch. A cached `index.html` after deploy used to
  404 that chunk and show “Overview failed to load”.
* Other app screens load via `lazyNamed` in `apps/web/src/lib/lazyPage.ts`.
  A stale hashed chunk reloads the shell **once** (session guard). The error
  boundary never prints the Vite asset URL.
* Transfer Studio helpers/constants live in `apps/web/src/pages/transfer/*`.

## Deploy note

Hashed `/assets/*` filenames are immutable. `index.html` (and the SPA fallback)
must be `Cache-Control: no-cache` so a new deploy cannot leave operators on
deleted chunk names. nginx configs in `deploy/` set that split.

If a leftover tab still holds a pre-fix shell, the first stale-chunk error
reloads automatically. A second failure (true network outage) shows a Reload
empty state — not a red stack dump.

## Chunk budgets

```bash
cd apps/web
npm run build
node scripts/check_chunk_budgets.mjs
```

Budgets live in `apps/web/chunk_budgets.json`. Lower them as TransferPage continues to split.
