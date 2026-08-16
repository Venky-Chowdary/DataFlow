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

## Devin Secrets Needed

None — local credentials come from the running API process environment.
