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

### Destination-unreadable / "create-new vs exists" Map testing

- **Source types without Snowflake credentials.** A local **fakesnow** connector (account `fakesnow`,
  catalog under `apps/api/data/fakesnow_data`) reproduces genuine `VARCHAR(16777216)` source types.
  Only source *types* are covered — Snowflake auth/network stays untested.
- **Induce "destination unreadable" AFTER the destination is chosen**: pick MySQL (`df-mysql`, host
  port **3307**, db `dataflow`, root/dataflow), then `docker stop df-mysql`, then trigger the Map
  render (Continue to Map, or retype the table name on the Destination step).
- **Assert on the `<select>` value, not the chip.** Read
  `[...document.querySelectorAll('select[aria-label^="Destination type"]')].map(s => s.value)`;
  a pending row must be `""` with the option text `— destination type not loaded —`. The chip
  (`Dest Type Not Loaded`) and the select have historically disagreed. Also check the *option list*:
  a stale `"<source type> — current"` option may still be offered while pending.
- **Check the Why column separately.** Pending rows have shown fallback provenance text such as
  `Inferred from live connector schema` or `No type-compatible destination column` even when nothing
  was read. Scan `main.innerText` for `/Inferred from live connector schema/i` as its own assertion.
- **`Reload destination schema` may never become clickable.** While the destination is down the button
  can stay disabled as `Reading destination…` for minutes, and the Map state often **self-heals** to
  create-new once the container is back — so a click-driven reload assertion can be impossible to
  prove. Capture the button's `disabled` + label over time and report the recovery trigger honestly
  instead of assuming the click did the work.
- **Compact blocker bar checks:** `.df2-map-blocker-bar` height should be ~46px with
  `.df2-map-blocker-list li` length 0 collapsed; the toggle reads `Why (n)` where n = distinct
  *causes* (not columns) and flips to `Hide detail` with exactly n `<li>`.
- **Fixtures used for the three semantics:** absent table name (e.g. `Newdata_e2e`) ⇒ create-new;
  `employee_wide_exist` (10 cols, `varchar(64/128/8/2)`, `date`) and `tx_tbl_0d54d1`
  (`id int`, `region varchar(4)`, `city varchar(6)`) ⇒ existing-table real DDL comparisons; a narrow
  existing column yields a *measured* lossy row requiring an execution policy + `Sign Risk Contract`.
- **HIRE_DATE create-new target is not stable** across runs (`DATE` in one run, `DATETIME(6)` in
  another, the latter demanding a lossy policy). Do not hard-code the expected temporal type; assert
  only that the type is MySQL-valid and contains no `16777216`.
- **Test declared temporal mapping with a declared-DATE fixture, not an all-VARCHAR one.** Seed a
  fakesnow table such as `PUBLIC.EMPLOYEE_DATED` (`EMPLOYEE_ID VARCHAR`, `FIRST_NAME VARCHAR`,
  `HIRE_DATE DATE`, `DEPARTMENT VARCHAR`). With `source_kind="database"` the declared type is
  authoritative, so a declared `DATE` should project MySQL `DATE` deterministically. `EMPLOYEE_WIDE`
  declares every column `VARCHAR(16777216)`, so its date-like column may still project `DATETIME(6)`
  and demand a Risk Contract — that is a *different* case, do not conflate the two.
- **Confirm `source_kind` actually reaches the engine** by hooking fetch before entering Map:
  `window.__maps` style capture of `/transfer/map` request bodies; assert `source_kind` and
  `destination_table_exists` per call, and diff `target_type` across repeated Maps for determinism.
- **A declared OLTP probe timeout (`lib/destProbeTimeout.ts`, 45s OLTP / 180s warehouse) may not be
  observable in the UI.** With `df-mysql` stopped, the control has been seen stuck disabled as
  `Reading destination…` for 2.5+ minutes with zero `existence unknown` / `did not answer within`
  text. Poll the control's label+`disabled` on a timer and report the timeout as unproven rather than
  waiting indefinitely.
- **Always count the probe requests, not just the button state.** A stuck `Reading destination…` is
  usually a *probe storm*: hook fetch and count `POST /api/v1/transfer/introspect` while the host is
  down (`window.__intro=[]` + push `Date.now()`), then corroborate with
  `grep -c "transfer/introspect" <api log>`. A healthy backoff is a handful of calls per minute; a
  loop shows a call every ~3s (observed 140 browser-side calls / 222s, 36 in the last 60s even after
  a 15s automatic-probe cooldown shipped). Note that a *stopped* container refuses connections
  instantly, so each probe fails fast and the 45s timeout branch may never be the one that runs —
  to exercise the real timeout path, black-hole the port instead (e.g. DROP/REJECT-with-drop on the
  host port, or point the connector at an unroutable host) so the TCP connect hangs.
- **Toggling destination reachability instantly (best tool for probe/retry/recovery tests).** `docker
  stop`/`start` is too slow and too coarse: startup takes ~10s during which automatic probes keep
  firing, so a self-heal usually beats your click and recovery can never be attributed to the operator.
  Instead keep the container running and flip a loopback firewall rule (sudo works on this box):
  `sudo iptables -I OUTPUT 1 -p tcp -d 127.0.0.1 --dport 3307 -j REJECT --reject-with tcp-reset`
  → instant refusal; delete the same rule with `-D` → instantly reachable again. To exercise the
  *hanging* branch (45s OLTP timeout) instead of refusal, either `docker pause <container>` (port
  still accepts, no MySQL greeting) or bind a python socket listener that accepts and never writes.
  Recipe for click-attributed recovery: get Map into the pending state while refused, note the last
  browser-side introspect timestamp, delete the rule right after an automatic probe fails, then click
  `Reload destination schema` well inside the 15s cooldown window; a click-fired probe shows up within
  ~1-2s of the click, so it is unambiguously distinguishable from the next automatic probe.
  Always clean the rule up (`sudo iptables -S OUTPUT`) when finished.
- **Cooldown/backoff regressions can hide behind a shipped module.** Verify the served bundle contains
  the new code (`curl -s http://127.0.0.1:5173/src/lib/destProbeTimeout.ts | head -30`) before
  concluding the fix isn't loaded; a present-but-ineffective fix is a different (and more useful)
  finding than a stale bundle.
- **Pending state may offer no approve control at all** (no `Approve eligible` button rendered). That is
  a stronger fail-closed outcome than a disabled button — assert "no control can make a pending row
  ready" by enumerating `button` text for `/approve/i` plus the footer's `n need review` count.

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

## Auditing workspace UI geometry (control heights, overlap, toasts, a11y)

Helper scripts used for this (repo root, run from the repo so `playwright` resolves):
`vp.mjs <w> <h>` (exact CSS viewport via CDP), `probe.mjs` (installs `window.__M` geometry probe),
`route.mjs "<Nav label>" <out.png>` (sidebar click + screenshot + measure), `why.mjs <sel> <prop>`
(CDP matched-CSS-rules, shows which rule actually wins), `crop.mjs`, `vis.mjs` (list visible buttons).

Pitfalls that silently produce false results:

- **The Devin browser ignores zoom keys** and is pinned by launch flags. Set exact widths with
  `Emulation.setDeviceMetricsOverride` over CDP (Chrome exposes it on `127.0.0.1:29229`). The override
  survives client disconnect and applies to the same tab you click in.
- **Lazy routes paint an `aria-busy="true"` fallback first.** Measuring or screenshotting on a fixed
  timeout captures "Loading ..." instead of the page. Always
  `waitForFunction(() => !document.querySelector('[aria-busy="true"]'))` before asserting.
- **`window.__M` (or any injected probe) is destroyed by a page reload / `page.goto`.** Re-run
  `probe.mjs` after any reload, otherwise every route reports `__M not defined`.
- **Keep-alive mounts duplicate page panels.** Measure only *visible* elements and scope click
  selectors to the visible page root (e.g. `.df2-connectors-page:visible`), not `.df2-screen-panel`.
- **aria-label beats visible text for `getByRole`.** The connector row button reads "Test" but its
  accessible name is `Test <connector> connection`; a bare `:has-text("Test")` also matches the
  sidebar user row (`Test test@gmail.com`) and will navigate you to Settings. Jobs detail tabs carry
  count badges ("Log\n5"), so anchor only the start of the label.
- **Token ownership differs per context.** `.df2-toolbar` declares its own `--df-toolbar-h` and forces
  toolbar buttons to it with `!important`, so toolbar buttons legitimately differ from
  `--df-btn-height-sm`. Read the token from the nearest toolbar, not from `.df2-app`.
- **A later rule with equal specificity silently kills a media-query step.** Use `why.mjs` before
  calling a responsive step "not applied" - and note an un-`!important` rule loses to an
  `!important` one regardless of order (this is how the Pilot edge-tab content clearance can be
  defeated by a generic `padding-right: 16px !important` on `.df2-content-inner`).
- **Settings persistence needs a generous settle.** Reading `#timezone` ~2.5s after reload can catch
  the pre-fetch default and look like a lost save. Wait longer, and cross-check
  `GET /api/v1/workspace/settings`. Note `fetchWorkspaceSettings()` returns hardcoded defaults
  (`Datawrap` / `UTC` / `90`) when the GET fails, so a backend failure is indistinguishable from
  real data in the UI.
- **The Docs screenshot reel autoplays (~3s).** Tab+Enter assertions race the timer; activate a dot
  deterministically (focus index N, press Enter, compare `aria-selected` to N) instead. All six frame
  `<img>` elements are always laid out, so "which frame is visible" cannot be read from img rects -
  use the caption text or a cropped screenshot.
- **Assert the computed style on the element the rule actually targets.** `justify-content: safe center`
  lives on `.df2-pilot-main-inner`, while its scroll host `.df2-pilot-main-scroll` computes `normal` -
  reading the host produced a false "fix not applied" verdict. Resolve the selector in the stylesheet
  first, then measure that node.
- Reset Pilot to its empty state with the **New chat** button before asserting empty-state layout.

## Auditing workspace typography

Read `getComputedStyle` family/size/weight/line-height on every visible text node across the 13
authenticated routes at 1920/1440/1280/1024, group by *role* (button, field, tab, chip, label, th, td,
heading, mono) and count variants per role. A role with many variants is the finding; a raw list of
sizes is not.

- **`document.fonts.check()` lies about weights.** It answers "yes" for `650` because the browser
  matches the nearest shipped face, so a synthesised weight looks loaded. Compare requested
  `font-weight` values in CSS against the `@fontsource/...` imports in `main.tsx` instead.
- **Native controls do not inherit `font-family`.** A button with no family rule renders in the UA
  font (Arial here) beside IBM Plex Sans body text, which reads as "inconsistent fonts" while every
  stylesheet looks correct.
- The workspace base `font-size` on `.df2-app` legitimately steps down for density
  (13.5 → 13 → 12.5px), so an element with *no* size rule measures differently per width by design.
  Only elements whose size comes from a role token should be expected to hold constant.
- `.df2-page-title` currently renders nowhere in the workspace shell — measure before assuming a
  class is live, or you will "fix" dead CSS. Every route's `<h1>` is also `.df2-sr-only` (clipped to
  1×1px), so page-title font rungs have **no visible effect anywhere** in the workspace today; do not
  claim to have visually verified a page-title size.

### Prove the font that was actually rasterized, not the declared one

A declared `font-family` only records what CSS *asked for*: if the face never loaded, the element still
paints in a fallback and `getComputedStyle` looks perfect. Regex-over-CSS unit tests (e.g.
`typeLadder.test.ts`) never render and cannot see this at all.

- Use CDP `CSS.getPlatformFontsForNode` → `familyName` + `isCustomFont` + `glyphCount`. `isCustomFont:
  false` means a *system* font painted (e.g. `Consolas`, `Arial`), which is the real defect signature.
- **It returns an empty array on container nodes.** It only reports text in the node's own inline
  boxes, so calling it on `.df2-app` yields `[]` and a false "no violations" pass. Call it per
  text-carrying element. To keep it cheap, group visible elements by *distinct computed
  `font-family` string* and probe one representative per group — fallback resolution depends only on
  the declared list, so the group shares a verdict.
- **Always run a negative control.** Inject `button,input,select,textarea{font-family:Arial!important}`,
  confirm the probe reports Arial, then purge and re-measure. A probe that cannot detect an injected
  Arial proves nothing about "no Arial".
- A raw literal such as `font-family: ui-monospace, SFMono-Regular, Menlo, monospace` (no
  `var(--df-font-mono, …)`) paints **Consolas** on Windows; children inherit it, so one missed sheet
  silently drags whole subtrees (including `.df2-btn`s) off the workspace font.

### `page.goto()` with a hash-only change does NOT reload — it corrupts injected-style cleanup

`page.goto('http://127.0.0.1:5173/#/other')` from `#/current` is a *same-document* navigation. The
document, and therefore anything added by `page.addStyleTag`, survives. This produced a spectacular
false positive: an Arial override injected as a negative control persisted through every later
"hard reload" and made all 13 routes report Arial everywhere.

- For a genuine new document use `page.reload({waitUntil:'load'})` (set `location.hash` first if you
  need a different route), and **assert the cleanup worked** before trusting any measurement:
  `[...document.querySelectorAll('style')].filter(s => /Arial\s*!important/.test(s.textContent))`
  must be empty.
- Anything described as needing a "fresh first load" (the Pilot ≤1279px empty state) must use a real
  reload; a hash nav silently re-measures the old document and can report a different clearance.

### Page-level `!important` literals still beat the role ladder

The type ladder in `app-styles.css` is later in source order but **not** `!important`, so any page
sheet with `font-size: …!important` wins regardless of order — e.g.
`.df2-job-name-rename-btn{font-size:11px!important}` keeps that button off the 12px `--df-fs-btn-sm`
rung while its `.df2-btn-sm` peers sit at 12px. Grep the page sheets for `font-size:.*!important`
before believing a role is uniform, and note `typeLadder.test.ts`'s guard only matches selectors
containing `.df2-btn|tab|input|select` as a whole token, so bespoke names like
`.df2-job-name-rename-btn` slip past it.

## Auditing the BYO AI-provider slice (Settings → AI Models)

### Restart the API after switching branches — it does not auto-reload

The uvicorn command in this skill has no `--reload`, so a process started before a checkout keeps
serving the old app. New routes then return **404** and the UI degrades *silently but plausibly*:
`Settings → AI Models` shows a permanently **disabled** engine selector stuck on
"Loading which engine Pilot will use…". That looks exactly like a frontend bug. Before filing anything,
check `GET /api/v1/workspace/pilot-engine` — a 404 means stale process, not a defect. Kill it
(`Get-CimInstance Win32_Process -Filter "Name='python.exe'"`, match `*uvicorn*8001*`) and restart.

### Wait on real readiness signals, not a sleep

The AI Models panel paints *before* its two fetches resolve: the engine hint starts at
"Loading which engine Pilot will use…", the selector starts `disabled`, and the provider cards
**do not exist at all** until `/copilot/models` lands. A fixed `waitForTimeout` reads that transient
state and produces a false "selector disabled / no provider cards" failure even while the network log
shows `200`s. Gate measurement on: selector present *and not disabled*, hint not matching
`/^Loading which engine/`, and `document.querySelectorAll("article").length >= 3`.

### Reaching a "no cloud key" state without destroying the operator's real key

The box may hold a real encrypted provider key, so the honest baseline is
`preference=auto, engine=hybrid, source=configured_provider` — not a clean no-key box. Two safe levers:

- **Engine pin** — set the selector to `Local engine only`. Touches only the saved preference.
- **Withhold the key** — set `ai_providers.<provider>.enabled=false` in
  `apps/api/data/integrations.json`. `resolve_provider_api_key()` checks `ai_provider_enabled()`
  *first*, so the key stops resolving while the encrypted blob stays byte-identical. This is the only
  way to exercise the automatic no-key path, because **the Configure modal has no Enabled toggle**.

Always `Copy-Item` the store to `integrations.json.audit-backup` first and restore with a sha256
comparison afterwards. A blank API-key field in the modal is safe: `_encrypt_field(value, existing)`
returns the existing blob when the value is empty or the mask, so "leave blank to keep" is honest.
Never paste a real key; saving one is live-validated and a bad one is rejected with HTTP 400.

### The provider auth-failure cache is in-memory, and a 401 makes the engine local

A recorded 401 lives in a process-wide set, not on disk, so **restarting the API resets provider
state** — the cheapest way back to a clean baseline. While it is set, the provider stays `configured`
(the key really is saved) but drops out of `usable_cloud_providers()`, so the engine resolves to
`local` with "The saved key for openai was rejected…" as its reason. Disabling a provider outranks a
cached 401 in `blocked_reason`. Budget for all four reasons: `configure`, `invalid_key`, `offline`,
`disabled`.

### `Test key` does live network I/O, bounded to one attempt and 12s

The route is synchronous and calls the provider SDK for real, with `max_retries=0` and a 12s deadline
(`VERIFY_TIMEOUT_SECONDS`), so a hung provider returns "did not answer within 12s — the key was not
verified" rather than stranding the button. Allow ~20s on `waitForResponse`; anything longer than that
is a real defect, not a slow provider.

Also note the button only renders when `tier === "cloud" && provider.configured`, so a provider with
**no** key exposes no `Test key` button at all — the backend's "No API key is saved for this provider."
branch is unreachable from the UI and must be reported as such rather than faked.

### Playwright locator trap: filtering a card by its button label

`page.locator("article").filter({ hasText: "Test key" })` stops matching the instant the label becomes
"Testing…", so `innerText()` throws and an in-flight assertion silently reports "never seen". Filter
cards on stable text (e.g. the model name `gpt-4o-mini`) and address the button by role.

### Grounded Pilot question for engine testing

Ask `How many rows are in ttd_orders_ok on Local PG 5433?`. On this box
`public.ttd_orders_ok` has **5** rows while `ttd_orders` and `ttd_pii` have **0**, so a hallucinated or
cached answer is distinguishable from a real one. A correct local run returns
`method: pilot_local_engine` with `tools_used: [{name: "aggregate_data", summary: "count = 5"}]`, and
the Pilot header shows an **Offline** pill when no cloud provider is available.

### Pilot / RAG answer path: what to capture and the traps

Capture every turn with a fetch hook on `POST /api/v1/copilot/chat` and assert the response fields
against the rendered bubble — `method`, `grounded`, `confidence`, `sources`, `tools_used`,
`suggested_actions` — plus `location.hash` **before and after** each turn. A correct API response with
an empty bubble is the signature of client-side auto-navigation swallowing the answer
(`applyPilotSafeActions`), so never judge Pilot by the DOM alone.

Traps found repeatedly on this app:
- Auto-navigation should only fire when `tools_used` contains `{name:"navigate", success:true}`.
  Verify with an explanatory question (`What is append mode?`, hash must not change) *and* an explicit
  one (`open jobs` / `take me to connectors`, hash must change) in the same session.
- The Pilot page keeps **stale chat history from before an API restart**, and `New chat` can retain the
  previously attached **Job context chip**. Reload the page (F5) and then click `New chat` before
  asserting fresh-turn behaviour, and re-install the fetch hook after every reload.
- Conversations persist in `localStorage`: the rail in `df2.pilot.rail.v1`, the Pilot page in
  `df2.pilot.sessions.v1` / `df2.pilot.activeId.v1`. Removing the key and reloading is the only
  reliable way to get a genuinely fresh rail chat.
- Job counts: `GET /api/v1/connectors/jobs` returns at most **50 recent rows** plus whole-history
  `total` / `status_counts`. The Jobs chips and nav badge must read those counts (via
  `lib/jobHistory.ts`) and agree with Pilot; the row list is a window and says so
  ("Showing X of Y failed job(s) — this list holds the 50 most recent of N jobs"). That note is a
  `<p class="df2-jobs-v3-window-note">` at the bottom of the scrollable job-list `aside`, so scroll the
  *list* (not the page) to photograph it, and narrow rows with the list's own
  `input[type=search]` (placeholder "Search route, type, status, job id…") — the header search does not
  affect it. Cross-check ground truth with `mongo.count_jobs()` in the
  API venv before deciding which surface is wrong. Selecting `Failed (N)` can list fewer rows than N —
  that is the stated window, not a count bug.
- Unsupported-question refusal: `how do I cook rice` must return the `will not answer it from
  guesswork` refusal with `sources: []`, `grounded: false`, `confidence 0.2` and no live-data tool.
  It previously varied between that refusal and confident product prose at `confidence 0.7`
  (`search_knowledge: 3 knowledge hits`), so assert on the *answer text* and `tools_used`, not just
  `grounded`, and test it both on a fresh chat and after a product question in the same conversation.
  Reproduce out of band with `curl -X POST /api/v1/copilot/chat -d '{"message":"how do I cook rice"}'`
  to prove whether a regression is backend or UI.
- Citations truncate `#/help/<slug>#<section>` to the article route, so section anchoring cannot be
  proven; article-level click-through can.

### Distinguish state styling from drift

Tabs legitimately render weight 600 when `.active`/`aria-selected` and 500 when inactive at the same
12.5px. Bucket by role **and** state before calling a two-tuple role inconsistent. Likewise a UA rule
(`strong,b{font-weight:bolder}`) can push inherited 600 to a computed **900** that no shipped face
covers — that is a real synthesis risk but it comes from the UA sheet, not from a page literal.

## Auditing Schedules: approval inbox / delegated authority (Autopilot)

### The shipped UI cannot complete a decision when auth is disabled
Decision routes resolve the actor via `schedules_router._decider()`. With `REQUIRE_AUTH=0`
(the default off-production, and the default on this box) it demands an `X-Actor` header of
>=2 chars and ignores any session. The web client never sends it - `apiFetch` only sets
`Authorization`, and `grep -r "X-Actor" apps/web/src` returns 0 hits. So every approve/reject
click returns:

    400 {"detail":"X-Actor must name the person making this decision"}

Test this in two modes and report them separately:
- **Mode A (as shipped, auth off)**: prove the 400 on a real click. This is what a user hits.
- **Mode B (auth on)**: the only config where items can be verified end-to-end.

To drive the UI past the missing header without patching product code, inject it over CDP.
Do NOT use `page.route()` - rewriting the request breaks the inbox refetch and the row
disappears, which looks like a product bug:

```js
const cdp = await ctx.newCDPSession(page);
await cdp.send("Network.enable");
await cdp.send("Network.setExtraHTTPHeaders", { headers: { "X-Actor": "Autopilot Auditor" } });
```

### Auth-on startup needs an explicit ENV or the app exits
`platform_config.is_production()` treats `REQUIRE_AUTH=1` with an unset `ENV` as production,
then fails closed on missing Fernet secrets and localhost Mongo. Always set
`DATAFLOW_ENV=development` alongside `DATAFLOW_REQUIRE_AUTH=1`. Run the auth-on instance on a
**separate port** (e.g. 8002) and use throwaway local credentials only.

### Two API instances share one store - kill the extra one before teardown
An auth-on instance on 8002 runs its own scheduler against the same Mongo. It will rewrite
schedules you are trying to delete and can re-run beats under you. Stop it first.

### Deleting the LAST schedule silently fails (Mongo backend)
`schedule_store._save_mongo` ends with `if seen: coll.delete_many({"_id": {"$nin": list(seen)}})`.
Removing the final schedule leaves `seen` empty, so the guard is False and the doc is never
removed - while `delete_schedule()` returns True and the API answers `200 {"success":true}`.
Consequences for teardown and for tests:
- Never trust a 200 from `DELETE /api/v1/schedules/{id}`; assert `list_schedules()` afterwards.
- Deleting one of two works, so a "delete works" test with >1 schedule hides this entirely.
- To clean up the last one, clear the collection directly:
  `db["pipeline_schedules"].delete_many({})` (also check the legacy `schedule_store` blob).
- With PR #52 this also strands the schedule's open approval in the inbox permanently.

### Park a real finding with real DDL, not a fabricated approval
`_guard_source_schema_drift` compares the schedule's remembered source fingerprint against the
live table, so the product raises its own `ApprovalRequired` after a genuine `ALTER TABLE`:
- **approvable** (soft/additive drift -> scopes `net_additive_drift`, `replay_schema_drift_ack`):
  `ALTER TABLE ttd_orders_ok DROP COLUMN customer;`
- **non-approvable** (`_NEVER_DELEGABLE` -> empty scopes, `approvable:false`):
  `ALTER TABLE ttd_types_ok ALTER COLUMN price TYPE integer USING price::integer;` (narrow_type)

`approvable == bool(requested_scopes)`. Never-delegable wording includes lossy, narrowing,
precision loss, truncat, unsupported conversion, not null, primary key, cursor.

**Dropping a column destroys its values.** Copy the table (or run a baseline transfer to a
destination table) BEFORE mutating, so the original rows are recoverable at teardown. Restore
with `ALTER TABLE ... ADD COLUMN` + `UPDATE ... FROM <destination>` and re-assert the exact
column order, types and row values.

### Use bare table names in schedules
`source_table: "public.ttd_orders_ok"` gets double-prefixed by the runner:
`relation "public.public.ttd_orders_ok" does not exist`. Use `ttd_orders_ok`.

### Assert decisions on three surfaces
A row vanishing from the inbox proves nothing (React could have dropped it). For every decision
assert the DOM, the captured HTTP request/response, AND the schedule re-read from the store
(`approval_request.status`, `resolved_by`, `resolved_reason`, `last_status`, `next_run_at`,
`enabled`, `standing_authorization.max_uses`/`expires_at`). Approve-once mints `max_uses=1`
expiring in ~1 day; standing grants use `max_uses=0` and the expiry you typed. Revoke keeps the
record with `revoked_at`/`revoked_by` rather than deleting it.

### Inbox layout: what is and is not a defect at narrow widths
- The `source -> destination` route intentionally truncates
  (`overflow:hidden; text-overflow:ellipsis; white-space:nowrap` + full route in `title`).
  A generic clipping heuristic flags this as a false positive - check the matched styles.
- Real defect at 390px: `@media (max-width:1023px)` forces
  `min-height: var(--df-btn-height,36px)` on `.df2-input`, which includes the multi-line reason
  `textarea`, so a realistic two-line reason hides ~21px of content (`scrollHeight` 55 vs
  `clientHeight` 34). Measure `scrollHeight` vs `clientHeight` on the textarea after typing.

### Occurrences and scopes render conditionally
`seen N x` only renders when `occurrences > 1`, and requested scopes only render inside the
expanded form. A collapsed-row probe correctly returns null for both - expand before asserting.

## Schedules page: selectors and env traps that cost real time

### Mongo database name is `datatransfer`, not `dataflow`
Schedules persist in `datatransfer.pipeline_schedules` (Postgres data lives in the `dataflow`
Postgres DB - the names do not match). A query against `dataflow.pipeline_schedules` returns `[]`
and looks like "nothing persisted". Verify with:
`docker exec df-mongo mongosh datatransfer --quiet --eval "db.pipeline_schedules.countDocuments({})"`
The legacy blob is `datatransfer.schedule_store` (`_id: "primary"`); it may not exist at all, in
which case you cannot observe the `superseded_by` marker being written - prove the
no-resurrection property by outcome (hard reload shows nothing) instead.

### Create-form selectors (these break the obvious locators)
- The form's text inputs have **no `type` attribute** - only `class="df2-input"`. So
  `input[type="text"]` matches nothing. Match on placeholder:
  `Nightly orders sync` (name), `orders` (source table), `orders_warehouse` (dest table),
  `10 10 * * *` (cron, only after clicking the Cron tab).
- A **hidden `#studio-contract` `<select>` precedes the form's own selects**, so `select` nth-index
  is off by one and `selectOption` times out on an invisible element. Use `select:visible`:
  index 0 = source connector, 1 = destination connector, 2 = schema policy, 3 = contract.
- The **destination connector defaults to the SQLite connector** - set it explicitly to the
  Postgres one or the run targets the wrong database.
- A previous script can leave the create form open, so `New schedule` no longer exists. Make scripts
  state-aware: if `Create recurring sync` is in the body text, click `Cancel` first.
- `button[aria-label^="Open "]` matches a hidden `aria-label="Open navigation"` mobile-nav button.
  Match `[aria-label*="detail" i]` for the schedule detail drawer opener.

### Sub-hourly cadence only exists on the Cron tab
Presets are `Hourly` / `Daily` / `Weekly` only. For a 2-minute cadence click `Cron` and enter
`*/2 * * * *`. The saved document then holds **both** `cron: "*/2 * * * *"` and a stale
`interval: "daily"`; the cron wins (`next_run_at` advances every 2 minutes), so do not read
`interval` as the effective cadence.

### Deleting a schedule: drawer only, and no confirmation
Schedule list cards expose **no** delete control - open the detail drawer
(`Open <name> details`) to find `Delete`. There is no confirm dialog: one click destroys it.
Always assert deletion on three surfaces (DOM, `GET /api/v1/schedules`, Mongo `countDocuments`),
and specifically test deleting the **last** schedule - a >1-schedule test hides the historical
"API says 200, schedule survives" bug.

### Do not use `full_refresh_append` for repeat-beat tests
Re-running an append schedule against a non-empty destination fails every beat with
`Checksum mismatch (balanced): ... Destination has N extra row(s)` and **appends another copy each
beat** (dest 5 -> 10 -> 15 -> ...). This masks whatever refusal you were trying to observe. Use
full overwrite, or truncate the destination between beats, when the test needs repeated runs.

### Parking a finding deterministically
Driving a real source `ALTER TABLE` and waiting for a beat is slow and may be pre-empted by other
failures. The reliable path is the runner's own refusal code path:
`services.schedule_runner._open_finding(schedule_id, ApprovalRequired(...), attempt=1)`, pushing
`next_run_at` ~6h out first so the live scheduler does not clobber the parked state. Use
`scopes=("net_additive_drift", "replay_schema_drift_ack")` for an **approvable** finding and a
`narrow_type` evidence kind with `scopes=()` for a **non-approvable** one. Label this in the report
as a service-level park driven through product code, not a scheduler-observed drift.

### Reason textarea at 390px may still clip
The `min-height` control rule is gone (computed `min-height: 0px`), but the textarea is `rows=2`,
which at 390px still hides part of a realistic two-line reason (`scrollHeight` 73 vs
`clientHeight` 54). Measure at 390 **and** at desktop; desktop is clean (54 vs 54).

## Permission / RBAC testing (PR #59 era and later)

### Enable RBAC or every permission test is vacuous
`rbac.py` passes everything through unless `DATAFLOW_REQUIRE_AUTH=1`. Start the API with
`DATAFLOW_REQUIRE_AUTH=1` **and** `DATAFLOW_ENV=development`, and re-supply
`DATAFLOW_ADMIN_EMAIL` / `DATAFLOW_ADMIN_PASSWORD` / `DATAFLOW_ADMIN_NAME` on every restart or you
get `{"auth_required":false,"has_users":false}` and an unusable login form.

### Setting the active workspace requires a reload
`localStorage["df2.workspace"]` is read when `PermissionsContext` resolves identity. Setting it and
then `page.goto("#/other")` does **not** re-resolve authority (a hash change is not a document
load), so the account keeps its previous - often viewer - authority and every write control stays
disabled. Always `page.reload()` after writing the key, then wait for `/auth/me` to answer.

### `can()` is permissive until `/auth/me` answers
`PermissionsContext` returns `true` for every permission while identity is unknown, so controls are
briefly enabled for everyone. Any "the control is disabled/enabled" assertion is meaningless unless
you first wait for the `/auth/me` response.

### A header-less request is not the same as a viewer
An account with memberships in several workspaces and **no** `X-Workspace-Id` is answered
ambiguously and fails closed, which shows up as `GET /schedules/ 403` right after login. The same
request returns `200` once the header is present. Do not report that first 403 as a permission
defect - re-test with the workspace chosen before drawing any conclusion.

### Read-permission and route-gate can disagree; watch for fabricated empty states
`/auth/me` may grant `schedule.read` while `GET /api/v1/schedules/` still answers `403` for that
role. The page then renders the "No schedules yet ... Create schedule" empty state even though
schedules exist in that workspace. Whenever a list looks empty, confirm the underlying GET returned
`2xx` - an empty list drawn from a failed read is a reportable defect, not a passing test.

## Engine / schedule fixtures

### Choose the sync mode deliberately, and probe the real button labels
The mode buttons are `Full append`, `Full overwrite`, `CDC`, `Incremental append`,
`Incremental deduped`, `Mirror`, `SCD Type 2` - there is no button reading "Full refresh", so a
`/Full refresh/i` locator silently falls through to whatever matched first (often `CDC`). Dump the
visible button labels before selecting. For a "does a repeat beat duplicate rows?" proof use
**Full overwrite**: after N beats the destination must equal the source exactly.

### CDC needs a cursor and a primary key
A CDC schedule saved without them fails its first beat with `Sync mode contract incomplete`
(`rejected_rows`, `records_transferred: 0`), then parks. That is a fixture mistake, not a product
bug - do not report it as one.

### `tfid_src` carries a schema-drift blocker; `ttd_orders_ok` does not
Any schedule reading `tfid_src` parks on `SOURCE_SCHEMA_DRIFT` with the self-contradictory finding
`ts_plain: TIMESTAMP_NTZ -> TIMESTAMP_NTZ (narrow_type)` (identical old/new type reported as
breaking) and `approvable: false`, so it can never fire twice. Use `ttd_orders_ok` (5 rows, sum
`1367.875000`, no temporal columns) for cadence and duplication tests, and keep it pristine.

### A parked schedule freezes its own cadence
`last_status: "needs_approval"` suppresses further beats, so "it only ran once" is expected for a
parked schedule. Check `run_count` in Mongo before assuming the scheduler is broken.

## Windows / PowerShell API access

`Invoke-WebRequest` / `Invoke-RestMethod` fail here - `-SkipHttpErrorCheck` does not exist on
PS 5.1 and an error response makes it try to prompt (`NonInteractive mode`). Use `curl.exe`:
`curl.exe -s -o NUL -w "%{http_code}" -X DELETE -H "Authorization: Bearer $t" -H "X-Workspace-Id: $w" <url>`.
Response shapes differ per route: `GET /api/v1/schedules/` returns a **bare array**, while
`GET /api/v1/connectors/saved` returns `{"connectors":[...]}` - indexing the wrong one yields `0`
and looks like successful teardown when nothing was deleted.

### Connector rows are only partly workspace-scoped
Connectors created through the current UI carry the real `workspace_id`; pre-existing rows carry
`workspace_id: ""`. Any workspace-isolation assertion must account for those unscoped legacy rows.

## Settings tab selectors, Team endpoints, and "fake data" false alarms (PR #59 re-verify era)

Four traps that each invalidated a run:

- **Settings tab buttons are NOT exact text labels.** Each renders label+subtitle concatenated, e.g. the
  Team button''s `textContent` is `TeamMembers & roles`, Notifications is `NotificationsAlerts &
  integrations`, Enterprise is `EnterpriseTenant, BYOK, residency`. A locator on `/^Team$/` silently
  matches nothing, the click is a no-op, and the script keeps scraping the **General** panel while
  reporting it as Team/Notifications/Enterprise. Use a **prefix** regex (`/^Team/`) AND assert a
  panel-specific string ("Members & roles", "Notification channels", "Enterprise tenant") before
  recording any result for that tab.
- **Team membership endpoints live under a `/team` prefix** (`team_router.py`):
  `GET|POST /api/v1/team/workspaces/{workspace_id}/members`,
  `PATCH|DELETE /api/v1/team/workspaces/{workspace_id}/members/{email}` (email is URL-encoded).
  The intuitive `/api/v1/workspaces/{id}/members` is a **404** - do not read that as "no members".
- **`Create workspace` renders only for `platformAdmin`** in TeamSettings. Its absence for a viewer is by
  design (gated by non-rendering), not a missing-control defect. For an admin it starts `disabled` purely
  because of the `!newWorkspaceName.trim()` guard - type a name and re-read `disabled` before calling it
  over-gated. This distinction is easy to misreport in both directions.
- **TenantSettings placeholders look exactly like real data.** `Wells Fargo`,
  `dataflow.wellsfargo.com`, `security@example.com` and `10.0.0.0/8 / 192.168.1.50` are `placeholder=`
  attributes, and `us-east-1` / `8` are client-side `useState` defaults for an unfilled create-tenant
  form. A screenshot of Enterprise after a 403 therefore *looks* like invented server data but is not.
  Always compare the DOM `value` against the `placeholder` attribute before reporting fabricated data.

**Admin membership regression (the risk when write controls get gated):** driving Team through the real UI
should yield `POST .../members 200`, `PATCH .../members/{email} 200` on the row `<select>`, and
`DELETE .../members/{email} 200`, with the row gone after a reload. Uncheck the "create a login" checkbox
when adding a temp member so teardown does not leave a sign-in-capable account behind.

## Schedule detail drawer: control selectors and refusal wiring

The drawer''s action bar is `.df2-drawer-actions`; enumerate its `button`s and read `disabled` +
`title` rather than guessing by pixel colour. Labels are **state-dependent**: `Run now` becomes
`Running...`/`Breaker open`, and `Pause` becomes `Activate` once the schedule is paused (a `Last job`
button also appears after a run). A `/^Pause$/` locator therefore fails on a paused schedule.

`PipelineDetailDrawer` takes `runRefusal` (job.run) and `manageRefusal` (schedule.manage); for a viewer
Run now / Pause / Edit / Delete are disabled with those sentences as titles. **`Export YAML` stays
enabled for a viewer by design** - it is a read (`GET /schedules/{id}/export?format=yaml` 200), so do not
report it as an un-gated write.

**Known residual gap:** the drawer body''s `Run parallel-run check` is NOT covered by those refusals - as a
viewer it renders enabled with a generic title, fires `POST /api/v1/fidelity/check` (403,
`connector.write`), and shows **no** user-visible feedback. When checking for silent failures, sample the
DOM repeatedly (300ms-6s) after the click: the page-level `PermissionNotice` banner also matches
`[class*="toast"]`/`[role="alert"]`, so a single late scan can be mistaken for a refusal toast that never
appeared. Compare against a pre-click snapshot.

Restart Vite when component **props/signatures** change and prove the new module is served with
`curl.exe http://127.0.0.1:5173/src/components/<File>.tsx | Select-String <newPropName>` before trusting a
disabled/title assertion.

**Update (commit `9578d9a6`):** that gap is closed - `Run parallel-run check` is now disabled for a viewer
with the shared `job.run` refusal, its handler returns early, and the backend rule
`("POST", "/api/v1/fidelity/", JOB_RUN)` replaced the fall-through to the `connector.write` mutation
default (which also refused an *operator*, since the operator role holds `JOB_RUN` but not
`CONNECTOR_WRITE`). Any change to that route table needs a hard uvicorn restart.

## Cheap fixtures for the drawer''s conditional controls (breaker / standing authority)

`Reset breaker` and `Revoke authority` only render under state that no normal test flow produces. Both can
be injected in seconds instead of driving the contract/approval flows:

- **Standing authority** (`Revoke authority`): set `standing_authorization` on the schedule document in
  Mongo `datatransfer.pipeline_schedules`. The drawer requires an object with an `id` key; use the
  `StandingAuthorization` shape (`id, actor, reason, granted_at, expires_at, scopes, max_uses, uses`).
- **Open breaker** (`Reset breaker`): the drawer needs `sched.contract_id` **and** a breaker whose state is
  `open`/`half_open` (`breakerBlocksRuns`). Insert a doc into `datatransfer.contract_breakers` with
  `{contract_id, state:"open", failure_count, success_count, failure_threshold, recovery_timeout_seconds,
  half_open_max, canary_pct}` and point the schedule''s `contract_id` at it.
- **Trap:** `POST /contracts/{id}/breaker/reset` 404s ("Contract not found") unless a *real contract
  document* exists - `GET /contracts/{id}/breaker` happily invents a default breaker, so a breaker-only
  fixture looks fine until you press Reset. Create the contract through `POST /api/v1/contracts`
  (body needs only `name`, `source`, `destination`) and use the returned `id` for both the breaker doc and
  the schedule''s `contract_id`, or you will misreport a fixture artifact as a product defect.
- With an open breaker, admin `Run now` is legitimately **disabled** with the title
  "Reset the contract breaker before running" - that is not over-gating.

## Reading the Dual Run campaign out-of-band

`POST /api/v1/fidelity/check` persists the cycle on the schedule as `fidelity_campaign` (Mongo
`pipeline_schedules`). `GET /api/v1/schedules/` does **not** expose that field, so the drawer''s PARALLEL RUN
panel reads "Not started" even when a campaign already exists; it only populates from the check response.
Verify a recorded cycle by matching the response `run_id` against a new `fidelity_campaign.history` entry -
do not use `cycles_observed`, which does not advance for a UI-initiated check (the drawer sends no
mappings/`column_types`, so the cycle lands as `assurance_level: "no_columns"`, `passed: false`). An
overwrite `Run now` records its own `full_checksum` cycle, so capture the campaign as a baseline first.

## Tenant administration (Settings -> Enterprise)

`GET /api/v1/workspace/tenant` resolves the tenant from the **workspace header** (`df2.workspace` in
localStorage, sent as `X-Workspace-Id`). Until a workspace is selected on the **Team** tab, every
tenant call 404s and the Enterprise tab looks broken - set the workspace first, or you will report a
fixture gap as a product defect.

Coverage limits worth knowing before planning a round: `deleteTenant` and `rotateByokKey` do **not**
exist anywhere in `apps/web/src`. The page only offers Create/Update tenant and `Add BYOK key`, so
tenant delete and BYOK rotate have to be exercised over HTTP with the authenticated session.

Authority is resolved from the workspace named in the request **body** (platform role + membership
row, via `resolve_effective_role`), not from the header - a workspace admin cannot create a tenant in
another workspace by sending its own `X-Workspace-Id`. A viewer/editor must see the read-only notice
*and* get `403 Workspace admin required to create a tenant` for its own token.

## Audit level vocabulary trap

Audit readers (`GET /api/v1/audit/events?level=...` and the Settings audit table) match the stored
string **exactly** against `info|success|warn|error`. A synonym such as `warning` or `critical` used
to render as `info` and vanish from the Warnings filter even though Mongo held the intended value;
`append_audit_event` now canonicalizes it. When verifying that an event was recorded at a severity,
cross-check **all three** layers - Mongo `datatransfer.audit_events`, the audit API response, and the
DOM Level cell - because agreement between the first two proves nothing about what an operator sees.

## Shape step (Transfer Studio, pre-write recipe) - runtime testing notes

Wizard order is Source -> Destination -> **Shape** -> Map -> Validate -> Run -> Proof.
API: `GET /api/v1/shape/catalog`, `POST /api/v1/shape/{validate,expression,profile,preview}`.

Traps that cost real time here:

- **uvicorn runs without `--reload`.** A Shape branch checked out while the old API process is
  still up gives `/shape/catalog` -> 404 *with a valid admin token*, and the whole feature looks
  missing. Confirm the feature is live first: `GET /openapi.json` and grep for `/shape` paths.
  Recover the running process env (`DATAFLOW_ADMIN_*`, `DATAFLOW_AUTH_USERS`, `DATAFLOW_ENV`,
  `DATAFLOW_REQUIRE_AUTH`, `PYTHONPATH`) before restarting, or RBAC and logins silently change.
- **Uploading the fixture:** the dropzone is not the input. `select_file` only works on an element
  the browser tool itself annotated - a hand-set `devinid` attribute is rejected. Make the hidden
  `input[type=file]` visible via the console (position/opacity/z-index), re-`view` so the tool
  assigns it a real `devinid`, then `select_file` on that id.
- **Recipe identity:** the header badge renders the hash uppercased by CSS, so a
  `/recipe [0-9a-f]{16}/` regex on `innerText` will not match. Match case-insensitively.
- **The hash is not one thing.** `/shape/validate` can return a different hash than the UI/engine
  identity for the same recipe. Always compare the *engine's* hash (execute payload / proof) with
  the badge, and state which surface produced which value.
- **Stale-approval guard is not reachable through ordinary UI navigation:** the page sends
  `shape_recipe` and `approved_shape_recipe_hash` from the same live shape identity, so a
  post-Validate edit updates both and self-agrees. Exercise the guard over authenticated HTTP with
  an edited recipe plus the older hash, and verify the destination table was never created.
- **Empty-recipe regression is its own scenario.** "Continue without shaping" still submits an
  approved hash for the empty recipe; if the engine also receives no `shape_recipe`, Execute can be
  refused with "approved with a shaping recipe ... but no recipe was supplied". Always run the
  no-recipe path end to end and reread Postgres - do not assume "no recipe" is a no-op.
- **Viewer role:** `/shape/profile` requires `job.plan`, so a viewer gets HTTP 403 and the profile
  panel renders with no facts and no suggestions while the lock notice is still shown. Decide
  whether that is intended before calling the read-only state a pass.
- **Editor role:** provision by resetting the login in Settings -> Team (one-time password is shown
  once in a `<code>` element). Stash it in `localStorage` and fill the login form from there so the
  value never lands in a screenshot or a log; delete any screenshot that captured it. First sign-in
  forces a password change - the change screen can appear a beat *after* the app shell renders.
- **Schedules cannot carry a recipe.** `ScheduleCreate` has no shape field and the New schedule form
  never mentions shaping, so "does the recipe replay on each beat" is unanswerable through schedules
  in this build - report it as untested with that evidence rather than simulating it.
- Destination claims: always reread Postgres directly (`127.0.0.1:5433`, db `dataflow`, connector
  `Local PG 5433`). Exact-row equality plus `COUNT`/`SUM`; a shaped write must show trimmed/uppercased
  text, rounded numerics and the filtered ids absent.

## Transform (pre-load) step — the renamed Shape step (PR #71 era and later)

The wizard step 3 is now **Transform (pre-load)** (`TransferTransformStep.tsx`,
`TransformGuidePanel/TransformColumnChart/TransformStepBuilder`, `lib/transformProfile.ts`,
`styles/transform-prep.css`). Backend wire names are still `shape*` (`/api/v1/shape/{catalog,profile,
preview}`, `approvedShapeRecipeHash`), so finding `shape` in a **payload** is not a naming defect —
only operator-visible text counts. Check these surfaces individually, because a partial rename is the
likeliest failure: Destination CTA ("Continue to Transform"), stepper step 3 ("Transform", narrow
"Xform"), inspector title, page heading/eyebrow, and the Proof/result dashboard.

### Shape-family wording that may still leak into operator-visible text
`conservationLedger.ts` emits a ledger cell labelled **"Shaped out"** and the Proof screen renders it,
`summarizeEffect` composes `... N shaped out ...` on the step itself, and the refusal alert can say
"shaping step N". `api.ts` still surfaces "Shape catalog unavailable" / "Shape preview failed" on the
error path. These are outside the renamed files, so a rename PR can pass its own diff and still show
Shape wording — grep operator-facing strings first, then confirm **on screen**.

### Viewer role: the profile fetch is skipped client-side, not merely 403'd
The step's effect begins `if (!plan.allowed || !sampleRows.length) return;`, so a viewer never issues
`/shape/profile` at all. Consequence: "What the sample holds" renders as an **empty card**, "Columns
with findings" reads **0** even for a demonstrably messy fixture, and the operation vocabulary is
absent — while the lock alert still claims "The operations below are the real vocabulary this engine
accepts. You can read them". Treat that contradiction as a finding rather than a pass; the correct
assertions are that the lock states the `job.plan` reason, that `Add a step` is `disabled`, and that
clicking it adds no step (steps count unchanged, builder never opens).

### A viewer can only reach step 3 by typing the destination table by hand
On Destination the viewer's schema read fails with a toast ("Could not read destination schema … needs
`connector.write`") and the table dropdown stays empty, so `Continue to Transform` is disabled. Type
the table name into the free-text `Pick table or type new name` field; if Continue flips back to
disabled after a re-analysis, click **Analyze Route**, wait, then click Continue. Do not conclude the
Transform step is unreachable for viewers.

### Charts and guide assertions worth pre-computing
Build the fixture so finding counts are known in advance — a messy text column should yield **one bar
per finding, not a stacked share bar** (`Blank`, `Leading/trailing space`, `Repeated inner space`,
`Control character`, `Un-normalised Unicode`, and one bar per distinct placeholder token `NULL`/`N/A`/
`n/a`), each with a `count/rows` label, a proportional non-zero fill width, and a `title` tooltip. A
wrong *number* then localises a profiler bug that a missing-element check would miss.
`changedCellIndex` deliberately excludes `kind === "removed"`, so removed rows are not highlighted —
not a bug.

### Transform is not applied before the Validate control-character gate
A recipe containing `strip_characters` can still make Validate block on the **raw** source value
(e.g. `name` row 6, U+0001), i.e. preflight scans pre-transform data. The UI offers a "Strip controls
& re-run" workaround that adds remediation mappings. Verify whether the approved recipe reaches
preflight before treating a shaped transfer as clean, and expect this may still be open.

### Responsive evidence: screenshot through Playwright, not the generic tool
The generic screenshot tool may render at a fixed viewport and silently ignore CDP
`Emulation.setDeviceMetricsOverride`, producing "proof" at the wrong width. Drive the width over CDP,
assert geometry numerically (`getBoundingClientRect().left` equal for all grid children = one column;
`document.documentElement.scrollWidth <= window.innerWidth` = no page overflow;
`.df2-xform-scroll` `scrollWidth > clientWidth` = table scrolls inside its card), and capture the
image via Playwright directly. Cards are two-column at 1440 and collapse to one below 1180.

### Transform builder: required free-text options are easy to miss with a selector
Several ops take a **required free-text** option that is NOT a `<select>`: `strip_characters` needs
`Characters *` (type `non_printable`), `filter_rows` needs `Condition *`, `round_number` needs
`Decimal places`. These inputs often carry **no `type` attribute**, so `main input[type=text]` misses
them — locate them by the surrounding `label`/`div` text instead. Always assert `Steps applied`
incremented and a `recipe <hash>` badge rendered before believing a step was applied.

Historically a blank required option left `Add step` **enabled** and silently added nothing. As of the
PR #71 fix round the builder shows a visible `role="alert"` inline error (e.g. *"Characters is required
for strip_characters."*), marks the field `is-invalid`, and disables `Add step` **before** the click.
If you are testing this, set the **operation `<select>` first** — otherwise `Add step` is disabled for
the wrong reason and the test passes vacuously.

Operation error policy lives in a `<select>` whose surrounding text reads *"If a value cannot be
computed"*, with values `refuse` / `divert` / `null` (**not** `fail` — that enum is invalid and yields a
recipe-validation error that is easily mistaken for a row refusal). `refuse` is the default.

### A retyped carrier can be refused at Map as "lossy" (carrier widening) — and how to verify it isn't
`/shape/preview` may re-read a rounded column as `INTEGER`, yet Map can render the source type as the
wider engine carrier (`BIGINT`) and judge `BIGINT → INT4` a fidelity loss, with a self-contradictory
banner: *"arr_time loses fidelity (BIGINT → INT4) — INTEGER → INT4 round-trips without loss."* and
`Continue to Validate` disabled. Fixed in the PR #71 era via `_reported_source_carrier` /
`declared_source_types` in `mapping_pipeline.py`, but it is the most regression-prone surface here.

Do not read a transformed carrier on the wire as proof Map accepted it — assert the **rendered row**
plus the enabled/disabled state of Continue. Three traps make this assertion lie:

- **The Map safe band is COLLAPSED by default**, so the row looks absent. Expand by clicking only a
  header matching `/^\d+ (safe mappings|ready)/i` that also contains the word `Expand`. A generic
  "click anything that expands" loop also hits **`Approve safe (n)`**, which collapses the table into
  an approved group rendering *"No matching columns"* — a screenshot of that proves nothing.
- **Never do a page-wide type-name search.** The destination `<select>` renders the whole engine type
  vocabulary (`TEXT SMALLINT INTEGER BIGINT NUMERIC …`), so "does the page say `BIGINT`" always
  false-positives. Read the row's own **Type cell** (index 2 of the row's cells).
- **Word-boundary regexes fail on concatenated `innerText`.** The row flattens to `…p3,s0 · intINTEGER`,
  so `/\bINTEGER\b/` is `false` even though the Type cell is exactly `INTEGER`. Extract per-cell text.

A correct pass renders Type **`INTEGER`** → Destination **`INT4 — current`** + `exists`, Transform
`None`, ~99% confidence, inside the *safe* band, Continue enabled with no Risk Contract.

### Prove the transformed path against a matched UNSHAPED control
A build that greens everything is indistinguishable from a real fix unless you also run the same decimal
source into the same integer destination with **zero** steps. That unshaped run must still be refused
(row badged **Lossy**, `DECIMAL(11,8) → INT4`, `Sign Risk Contract`, Continue disabled, wizard never
reaches Validate) and the destination `count(*)` must stay **0**. Build fixtures so the arithmetic
discriminates: values `22.433332, 21.833334, 9.500001, 8.499999, 4.100000` give round ⇒ SUM **66**,
truncate ⇒ **64**, ceil ⇒ **69**, so a reread cannot pass with the wrong operation.

### Map dead ends are sometimes escapable via the row-level "Review" button
When bulk `Approve eligible (n)` excludes an existing-DDL conflict and `Sign Risk Contract` never
appears (or stays disabled), selecting the row's `Execution policy for <col>` select and then clicking
the **row-level `Review` button inside the table** can approve it and enable `Continue to Validate`.
Prefer this over declaring the wizard unreachable, but report that the workaround was required.

### Validate can green the transform panel while other panels still judge RAW values
`plans/{id}/preflight` receives `shape_recipe` and returns `transform_image` + the "Transform recipe
… applied before the gates" warning. Historically the sibling call `/api/v1/preflight/preview-cells`
was sent **without** the recipe, so the quarantine preview still cited raw values (e.g. "Invalid
integer: '22.43'") directly under a panel claiming the transformed rows were judged; and a
`Schema drift` / `hard_breaking:narrow_type` root cause fired on every transformed narrowing (with
`source_changed:false`) because the drift gate compared the **declared** source carrier to the target.
Both are fixed in the PR #71 era (`preview-cells` accepts `shape_recipe`; `detect_schema_drift()` takes
`declared_source_*` for fingerprint questions while `source_*` carries the transformed image).

**Assert the verdict and every panel**, not just the transform panel: no `Invalid integer` / "fractional
value is not an integer" anywhere, `blockers: []`, no `Schema drift`, no `hard_breaking:narrow_type`.
If drift ever returns, note that the UI's own **`Re-map drifted columns`** remediation does not clear it
(rows come back `ready (approved)` and Validate still returns `schema_drift`) — a hard dead end.

### API fail-closed contract: 400 vs 200-with-refusal differ per endpoint (not a bug)
- `/api/v1/preflight/preview-cells` — body is `{headers, sample_rows: [[...]], mappings, column_types,
  shape_recipe}`. Row refusal, missing column and bogus op all return **400**; empty recipe returns
  **200 with no `transform_image`**; a valid recipe returns **200 with** it.
- `/api/v1/shape/preview` — body is `{sample_rows: [{col: val}, …], source_columns, column_types,
  recipe}` (**objects**, not header+array rows; the wrong shape yields a pydantic **422** that is easy
  to misreport as a defect). Unrunnable/bogus recipes are **400**, but a **row refusal returns 200 with
  a structured `refusal` object** — by design, since this is the preview that must *render* the refusal
  and disable Continue. Do not report that 200 as a fail-closed violation.
- A bogus op must say *"is not a **transform** operation"* (no "shaping").

### Do not flag "Dest-exists shape" as a shaping-wording leak
When sweeping rendered text for `/shap\w*/i`, the Validate gate list legitimately contains the
pre-existing gate **"Dest-exists shape"** (*"Dest-exists shape is bound — insert more into the existing
table"*) — that is the destination table's *shape*, unrelated to transform "shaping". Also run the sweep
on the **actual Run/Proof screen**: if Execute silently did not start you are still on Validate, whose
gate list contains those matches, and you will report a phantom regression.

### Execute often needs an exact-text, document-wide click
A `main button:has-text("Execute")` click can silently no-op — the flow stays on Validate, the
completion banner never appears, and the destination stays empty. Click the button whose
`innerText.trim().toLowerCase() === "execute"` that is enabled and has an `offsetParent`, document-wide,
after `scrollIntoView`, then poll `document.body.innerText` for `/Transfer complete|Data transferred/`.
Enumerate candidates first (`[...document.querySelectorAll("button,[role=button]")]` filtered by
`/execute/i`) — that immediately shows whether the control is disabled, offscreen, or absent.
The run screen's own counters say `DEST UNMEASURED` / `Writer ack is not destination proof`, so an
independent SQL reread is mandatory, not optional.

### Screenshotting one specific sentence: pick the DEEPEST matching element
`[...document.querySelectorAll("*")].find(e => re.test(e.textContent))` returns a huge ancestor (its
`textContent` starts with the whole sidebar), so `scrollIntoView` targets the wrong place and the
screenshot is not evidence. Take the **last** element of the filtered list, or select by the real class
(e.g. `.df2-xform-ledger`, which also exposes `is-unbalanced` state in `className`).

### MySQL via `docker exec` exits 1 even on success
`mysql -u… -p…` prints *"Using a password on the command line interface can be insecure"* to stderr,
which PowerShell turns into `NativeCommandError` and exit code 1 while stdout is perfectly valid. An
**empty** result is then ambiguous (did the query run?) — include a control column such as
`SELECT 'CONTROL_QUERY_RAN' AS proof;` in the same `-e` batch so an empty table is provably empty.

### ffmpeg recording must be stopped gracefully or the mp4 is unplayable
`gdigrab` capture works, but `Stop-Process -Force` kills ffmpeg before it writes the `moov` atom and
`ffprobe` then reports *"moov atom not found / Invalid data"* — the file is a worthless ~MB blob.
Send `q` to ffmpeg's stdin (or start it so stdin is writable) to finalize, and **ffprobe the file
before claiming a recording exists**. There is no annotation tool in this environment.

### Fixture caution
Do not put literal `NaN`/`Infinity` in a source column: `/api/v1/shape/{profile,preview}` returns
HTTP 500 (`shape_suggest.py:181`, `as_tuple().exponent` is `'n'`/`'F'` for non-finite Decimals),
Continue is disabled, and the whole step becomes unreachable — masking everything under test.
