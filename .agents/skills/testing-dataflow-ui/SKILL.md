---
name: testing-dataflow-ui
description: How to bring up the DataFlow stack locally, log into the workspace UI, hit exact browser viewports, and measure real rendered CSS geometry (list-row density, padding, typography tokens) for browser-based verification.
---

# Testing the DataFlow web UI end-to-end

## Bring up the stack

- Mongo: `docker start df-mongo` (Jobs / Schedules / Contracts do not persist without it).
- API: run from the repo virtualenv, **not** system python (system python has no FastAPI):
  ```bash
  cd /path/to/DataFlow/apps/api && ../../.venv/bin/python -m uvicorn src.main:app --host 127.0.0.1 --port 8001
  ```
- Web: `npm install && npm run dev` in `apps/web` → `http://127.0.0.1:5173`. Vite proxies `/api` → `:8001`.

## Logging in

- Sign in through the visible UI at the marketing header **Log in** button. The workspace lives behind
  auth; visiting `#/connectors` while signed out silently shows the *public connector catalog*, not the
  workspace Connectors page — an easy way to test the wrong surface.
- Local credentials come from the API process environment: `DATAFLOW_ADMIN_EMAIL` /
  `DATAFLOW_ADMIN_PASSWORD` (read with `tr '\0' '\n' < /proc/<uvicorn_pid>/environ`). Never print them.
- The password may contain shell-special characters. Read it with `head -c <len>` from a file rather
  than interpolating into a shell string, and verify `input.value.length` in the page before submitting —
  a wrong length means `xdotool type` mangled it.

## Route aliases

`#/pipelines` == "Schedules" · `#/docs` == "Help" · `#/benchmarks` == "Proofs" · `#/customers` == "Evidence".

## Exact viewport control

The `computer` tool's click mapping breaks after an X resolution change, so:

1. Pick an X mode larger than the biggest target: `xrandr --output VNC-0 --mode 2000x1300`
   (use an existing mode; custom `--newmode` may be rejected).
2. Size Chrome to `W+32 x H+129`: `xdotool windowsize <window_id> 1312 929` → CSS viewport 1280x800.
3. **Always confirm** with `innerWidth/innerHeight` in the console before measuring.
4. Convert CSS coords → screen coords for `xdotool mousemove ... click 1` by adding the window chrome
   offset (about `+16, +97` at these sizes). Re-derive it after any resize by attaching a temporary
   `mousemove` listener and comparing reported `clientX/clientY` to the coordinate you moved to.

## Measuring rows

Row selectors: `.df2-connector-row`, `.df2-contract-row`, `.df2-pipeline-row`, `.df2-job-row`.

Keep-alive leaves inactive pages **mounted at height 0** — every measurement must filter
`getBoundingClientRect().height > 0`, otherwise you will average in invisible rows.

Useful snippet:

```js
const rows=[...document.querySelectorAll(SEL)].filter(r=>r.getBoundingClientRect().height>0);
const c=getComputedStyle(rows[0]);
({n:rows.length,
  heights:[...new Set(rows.map(r=>+r.getBoundingClientRect().height.toFixed(2)))],
  minH:c.minHeight, padT:c.paddingTop, padL:c.paddingLeft, gap:c.columnGap})
```

Also record `matchMedia('(max-width:1366px)').matches` etc. to prove exactly one density media query
matches — overlapping density queries have been the root cause of cramped rows before.

## Token-ownership checks

A row can sit on the right height while its *typography* is still hard-coded. Compare the resolved
custom property to the element's rendered value:

```js
const root=getComputedStyle(document.querySelector('.df2-app'));
({token:root.getPropertyValue('--df-list-row-title').trim(),
  actual:getComputedStyle(document.querySelector('.df2-job-row-name')).fontSize})
```

Page-scoped rules near the end of `apps/web/src/styles/enterprise-ui.css`
(e.g. `.df2-page-jobs .df2-job-row-name`) may still override shared tokens — check there first when a
token appears "not applied".

## Accessible-name checks

Icon-only buttons often *have* label text in the DOM but hide it with CSS at narrow widths. Assert the
computed style of the label span, not just `textContent`:

```js
[...document.querySelectorAll('button')].filter(b=>{
  const lbl=b.querySelector('.df2-btn-label');
  const hidden=lbl && getComputedStyle(lbl).display==='none';
  return (hidden || !b.textContent.trim()) && !b.getAttribute('aria-label') && !b.getAttribute('title');
})
```

## Transfer Studio: preflight / Validate / acknowledgment testing

Reaching Validate with a *chosen* blocker takes fixture control. What works:

- **Reachable destination.** Saved "Prod Postgres *" connectors point at `127.0.0.1:5432`, which is
  usually dead. Start `df-pg` (host port **5433**) and create a fresh connection through
  Connectors ▸ New connection; Snowflake is not reachable in this env.
- **Trip the PII gate on purpose.** `services/compliance_guard.py` marks `ssn` / `dob` / `account`
  columns as high-risk, and any high-risk field forces `requires_review = True` regardless of the
  0.45 risk-score floor. A 5-row file with `username,email,phone,ssn,dob,amount` is enough.
- **Isolate PII as the *only* blocker.** An existing destination table whose column types differ even
  slightly (e.g. dest `numeric(12,2)` vs source `DECIMAL(10,4)`) raises a `schema_drift`
  `narrow_type` blocker that hides the compliance-only path. Match the dest type exactly
  (`ALTER TABLE t ALTER COLUMN amount TYPE numeric(10,4)`) to get the clean
  "Approve PII to unlock Execute" headline; mismatch it deliberately to test the mixed-blocker path.
- **Two different preflight transports exist.** `TransferPage.executePreflight` calls
  `preflightTransferPlan(planId)` → `POST /api/v1/transfer/plans/{id}/preflight` and `return`s early
  whenever a plan is persisted; `POST /api/v1/preflight/run` is only reached when no plan exists.
  Both now carry the same acknowledgment body (`compliance_acknowledged`, `acknowledgment_actor`,
  `acknowledgment_reason`), and the plan service persists it on `plan.policies` stamped with the
  mapping revision. Always confirm *which* endpoint fired before judging an ack result.
- **A lossy type change on an existing destination column is a dead end at Map** (e.g. dest `int4`
  for a `DECIMAL(10,4)` source): bulk "Approve eligible" excludes existing-DDL conflicts and
  signing the risk contract still leaves "Continue to Validate" disabled. Build mixed-blocker
  fixtures from **narrow text widths** instead (`phone varchar(6)`, `ssn varchar(4)`, matching
  numeric type) — those pass Map and block Validate on `g6_target_ddl`.
- **The footer Re-run control disappears once Validate is green.** To re-test acknowledgment
  persistence, re-enter through Back ▸ Continue to Validate.

### Capturing preflight request/response bodies

Install the hook **before** clicking anything on Validate, and always record the body length —
a length of 9 means the body was the string `"undefined"`, i.e. no payload was sent at all:

```js
(()=>{const of=window.fetch;window.__pfLog=[];window.fetch=async function(...a){
  const url=typeof a[0]==='string'?a[0]:(a[0]&&a[0].url)||'';const init=a[1]||{};
  const r=await of.apply(this,a);
  if(/preflight/i.test(url))window.__pfLog.push({url,status:r.status,
    req:typeof init.body==='string'?init.body:String(init.body),
    resp:await r.clone().text().catch(()=>'<unreadable>')});
  return r;};window.__errs=[];
  addEventListener('error',e=>window.__errs.push(e.message));
  addEventListener('unhandledrejection',e=>window.__errs.push(String(e.reason)));})()
```

Note this only sees `init.body`; if a call ever uses `new Request(url,{body})` or FormData you must
extend it. Execute/run endpoints use **FormData**, not JSON. Filter on
`/transfer/plans/<id>/preflight` specifically — the AI-assist call embeds a whole preflight payload
in its own body and will otherwise be mistaken for "the last preflight call".

### Reading the verdict

Assert on the response, not the headline: `passed`, `proof_bundle.transfer_decision.decision`
(`approve` / `review` / `block`), `.compliance_only`, `proof_bundle.compliance.acknowledged`, and
`blockers[].details.compliance_ack_required`. Execute unlocks only when `passed === true` **and**
`decision === "approve"` **and** the validated contract key still matches **and** `run_id` does not
start with `pf_local_` — name the specific false condition rather than saying "still blocked".

Two regressions worth re-checking on every Validate change: no surface may print an internal
blocker id (`proof_0` / `proof 0`) — including the AI-assist narrative and the explain issue cards —
and the "Approve PII for this transfer" button must render only when the API sets
`compliance_ack_required: true`, never on a message regex match.

The assist / explain surfaces render only after clicking **"Explain & fix with AI"**, and that panel
sits far down an inner scroll container — scroll it into view first. An "no internal ids" assertion
made with the panel closed passes vacuously. Assert with
`document.body.innerText.match(/proof[ _]?0/gi)` **plus** per-selector checks on
`div.df2-vd-assist-narrative > p`, `li.sev-block > strong`, and
`li.sev-block span.df2-vd-explain-gate code`.

Restarting the API **wipes in-process transfer plans**, so every post-restart Validate test has to be
rebuilt from Source.

Confirm no rows landed with a destination count, e.g.
`docker exec df-pg psql -U postgres -d dataflow -tAc "SELECT count(*) FROM <table>;"`.

## Devin Secrets Needed

None — local credentials come from the running API process environment.
