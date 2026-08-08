# Frontend code-split (Phase F9)

## What changed

* Vite no longer forces `inlineDynamicImports` — route and vendor chunks are separate hashed files.
* App screens (Transfer, Jobs, …) load via `React.lazy` + `Suspense`.
* Transfer Studio helpers/constants moved to `apps/web/src/pages/transfer/*` so `TransferPage.tsx` can keep shrinking.

## Deploy note

Hashed chunk names invalidate caches. After a production deploy, operators should hard-refresh once if an old shell HTML still requests deleted chunk names. Prefer CDN/`Cache-Control` on `index.html` that is short-lived (HTML) vs immutable (hashed assets).

## Chunk budgets

```bash
cd apps/web
npm run build
node scripts/check_chunk_budgets.mjs
```

Budgets live in `apps/web/chunk_budgets.json`. Lower them as TransferPage continues to split.
