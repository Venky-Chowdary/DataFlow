import type { ReactNode } from "react";
import { Ban, Check, CircleSlash, Fingerprint, KeyRound, OctagonX, Pause, Play, RefreshCw, Sparkles } from "lucide-react";
import { Bento, BentoTile } from "./kit/Bento";
import { RECORDED_CHECK_GROUPS, RECORDED_MAPPING, RECORDED_ROUTE, StatusGlyph } from "./ProductPanels";
import { COVERAGE_RUNGS, MIGRATION_RUN, PILOT_CANNOT, QUERY_SEGMENTS, RUN, RUN_HELD_KEYS, fmtN, identity } from "./StoryPanels";
import { TRANSFER_EXAMPLES } from "./StoryScenes";
import { EXAMPLE_JOBS } from "./app-frame/AppFrame";
import { ConnectorIcon } from "../../app/brand-icons";
import { PROVEN_CDC_SOURCES } from "../../lib/provenEvidence";

/*
 * Bentos for the product and solution pages (pass 7). Every tile's UI is a
 * fragment of a screen the app or the pages already draw, with the same
 * example data: Transfer Studio uses its own store_orders example; Job
 * space uses the example workspace and the home page's run; Migrations uses
 * the example cutover load; the other pages use the example workspace.
 * Labels, fields and defaults come from the app's own screens or the docs
 * named beside each tile.
 */

/** The one mapped column a person still has to look at in the Map example. */
const REVIEW_COL = "ordered_at";
const MAP_EXCERPT = ["order_id", "unit_price", "ordered_at", "attributes"];

/* ── Transfer Studio: the four steps, each as its screen ───────────── */

export function TransferBento({ points }: { points: { step: string; title: string; body: string }[] }) {
  const by = (step: string) => points.find((p) => p.step === step) ?? { step, title: step, body: "" };
  const ex = TRANSFER_EXAMPLES.transfer;
  const run = ex.run;
  const checks = RECORDED_CHECK_GROUPS.flatMap((g) => g.checks);
  const passed = checks.filter((c) => c.state === "pass").length;
  const fit = RECORDED_CHECK_GROUPS.find((g) => g.name === "Will every value fit") ?? RECORDED_CHECK_GROUPS[0];
  const plan = by("Plan");
  const validate = by("Validate");
  const execute = by("Execute");
  const verify = by("Verify");
  return (
    <Bento className="mk-kit-bento--steps" label="The four steps of a transfer">
      <BentoTile span={7} step={{ n: 1, label: plan.step }} title={plan.title} body={plan.body} bleed>
        <div className="mk-app-surface mk-tui">
          <div className="mk-tui-head">
            <strong>Column matches</strong>
            <span className="mk-app-pill mk-app-pill--completed">
              {RECORDED_MAPPING.length} of {RECORDED_MAPPING.length} mapped
            </span>
            <span className="mk-app-pill mk-app-pill--v-not-run">1 needs review</span>
          </div>
          <table className="mk-tui-table">
            <caption className="mk-sr-only">Column mapping excerpt (example, from a recorded run)</caption>
            <thead>
              <tr>
                <th scope="col">Source column</th>
                <th scope="col">{RECORDED_ROUTE.sourceEngine}</th>
                <th scope="col">{RECORDED_ROUTE.destEngine}</th>
                <th scope="col">Status</th>
              </tr>
            </thead>
            <tbody>
              {RECORDED_MAPPING.filter((m) => MAP_EXCERPT.includes(m.col)).map((m) => {
                const review = m.col === REVIEW_COL;
                return (
                  <tr key={m.col} className={review ? "is-review" : undefined}>
                    <th scope="row">
                      <code>{m.col}</code>
                    </th>
                    <td>
                      <code>{m.from}</code>
                    </td>
                    <td>
                      <code>{m.to}</code>
                      {m.transform ? <span className="mk-tui-muted"> · {m.transform}</span> : null}
                    </td>
                    <td>
                      {review ? (
                        <span className="mk-app-pill mk-app-pill--v-not-run">Needs review</span>
                      ) : (
                        <span className="mk-app-pill mk-app-pill--completed">Safe</span>
                      )}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </BentoTile>

      <BentoTile span={5} step={{ n: 2, label: validate.step }} title={validate.title} body={validate.body} bleed>
        <div className="mk-app-surface mk-tui">
          <div className="mk-tui-head">
            <span className="mk-app-pill mk-app-pill--completed">Approve</span>
            <span className="mk-tui-muted">
              {passed} passed · 0 blocked · {checks.length - passed} not applicable
            </span>
          </div>
          <ul className="mk-tui-gates" aria-label={`${fit.name} (example)`}>
            {fit.checks.slice(0, 4).map((c) => (
              <li key={c.name}>
                <Check size={14} aria-hidden />
                <span>{c.name}</span>
                <small>Passed</small>
              </li>
            ))}
          </ul>
          <p className="mk-tui-foot">
            <Fingerprint size={14} aria-hidden />
            Plan hash recorded on approval
          </p>
        </div>
      </BentoTile>

      <BentoTile span={5} step={{ n: 3, label: execute.step }} title={execute.title} body={execute.body} bleed>
        <div className="mk-app-surface mk-tui">
          <div className="mk-tui-head">
            <strong>Quarantine</strong>
            <span className="mk-app-pill mk-app-pill--v-explained">{fmtN(run.held)} held</span>
            <span className="mk-tui-grow" />
            <span className="mk-tui-muted">{ex.duration}</span>
          </div>
          <table className="mk-tui-table mk-tui-table--held">
            <caption className="mk-sr-only">Rows held on the example run</caption>
            <thead>
              <tr>
                <th scope="col">Column</th>
                <th scope="col">Value</th>
                <th scope="col">Reason</th>
                <th scope="col" className="is-num">
                  Rows
                </th>
              </tr>
            </thead>
            <tbody>
              <tr className="is-held">
                <th scope="row">
                  <code>status</code>
                </th>
                <td>
                  <code>NULL</code>
                </td>
                <td>NOT NULL in the destination</td>
                <td className="is-num">{fmtN(run.held)}</td>
              </tr>
            </tbody>
          </table>
          <p className="mk-tui-foot">
            <StatusGlyph kind="skipped" />
            {run.skipped} skipped by <code>region = &apos;TEST&apos;</code>
          </p>
        </div>
      </BentoTile>

      <BentoTile span={7} step={{ n: 4, label: verify.step }} title={verify.title} body={verify.body} bleed>
        <div className="mk-app-surface mk-tui">
          <div className="mk-tui-head">
            <strong>Read back</strong>
            <span className="mk-app-pill mk-app-pill--t-proven">Proven equal</span>
          </div>
          <dl className="mk-tui-eq" aria-label={`${fmtN(run.read)} read equals ${fmtN(run.landed)} landed plus ${run.held} held plus ${run.skipped} skipped`}>
            <div>
              <dt>Read</dt>
              <dd>{fmtN(run.read)}</dd>
            </div>
            <div className="is-landed" data-op="=">
              <dt>Landed</dt>
              <dd>{fmtN(run.landed)}</dd>
            </div>
            <div className="is-held" data-op="+">
              <dt>Held</dt>
              <dd>{fmtN(run.held)}</dd>
            </div>
            <div data-op="+">
              <dt>Skipped</dt>
              <dd>{fmtN(run.skipped)}</dd>
            </div>
          </dl>
          <p className="mk-tui-foot">Landed rows counted by {RECORDED_ROUTE.destEngine} and compared by full checksum. None differs.</p>
        </div>
      </BentoTile>
    </Bento>
  );
}

/* ── Job space: what a finished run carries ────────────────────────── */

const VERDICT_JOBS = ["6af912d7", "5d20b7c4", "4b88c1f0", "7c41e9a2"];
const TONE_PILL: Record<string, string> = {
  proven: "mk-app-pill--t-proven",
  explained: "mk-app-pill--t-explained",
  open: "mk-app-pill--t-open",
  none: "mk-app-pill--t-none",
};

export function JobsBento() {
  const explained = EXAMPLE_JOBS.find((j) => j.id === "5d20b7c4") ?? EXAMPLE_JOBS[2];
  const verdictJobs = VERDICT_JOBS.map((id) => EXAMPLE_JOBS.find((j) => j.id === id)).filter((j): j is (typeof EXAMPLE_JOBS)[number] => Boolean(j));
  return (
    <Bento label="What a finished run carries">
      <BentoTile
        span={7}
        title="Each table names the comparison that ran"
        body="Only a comparison of every row is called proven."
        bleed
      >
        <div className="mk-app-surface mk-tui">
          <table className="mk-tui-table mk-tui-table--ladder">
            <caption className="mk-sr-only">Which comparison ran, from weakest to strongest</caption>
            <thead>
              <tr>
                <th scope="col">Comparison</th>
                <th scope="col">What is compared</th>
                <th scope="col">What it shows</th>
              </tr>
            </thead>
            <tbody>
              {COVERAGE_RUNGS.map((r) => (
                <tr key={r.id} className={r.proof ? "is-proof" : undefined}>
                  <th scope="row">
                    <code>{r.id}</code>
                  </th>
                  <td>{r.what}</td>
                  <td>
                    {r.proof ? (
                      <span className="mk-tui-proof">
                        <Check size={14} aria-hidden />
                        {r.strength}
                      </span>
                    ) : (
                      r.strength
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </BentoTile>

      <BentoTile span={5} title="Held, not dropped" body="A refused row keeps its column, value and reason, and replays into the same table." bleed>
        <div className="mk-app-surface mk-tui">
          <div className="mk-tui-head">
            <span className="mk-app-pill mk-app-pill--v-explained">{RUN.held} held</span>
            <code>status</code>
            <span className="mk-tui-muted">is NOT NULL in the destination</span>
          </div>
          <ul className="mk-tui-rows">
            {RUN_HELD_KEYS.slice(0, 3).map((k) => (
              <li key={k}>
                <code>order_id {k}</code>
                <code>status = NULL</code>
                <span>Held</span>
              </li>
            ))}
          </ul>
          <p className="mk-tui-foot">
            Run state <code>completed_with_quarantine</code>, not <code>completed</code>
          </p>
        </div>
      </BentoTile>

      <BentoTile span={5} title="A verdict on every run" body="The Trust Report names the check behind each verdict." bleed>
        <div className="mk-app-surface mk-tui">
          <ul className="mk-tui-verdicts" aria-label="Verdicts in the example workspace">
            {verdictJobs.map((j) => (
              <li key={j.id}>
                <code>{j.name}</code>
                <span className="mk-tui-muted">{j.trust.check}</span>
                <span className={`mk-app-pill ${TONE_PILL[j.trust.tone]}`}>{j.trust.verdict}</span>
              </li>
            ))}
          </ul>
        </div>
      </BentoTile>

      <BentoTile span={7} title="Differences come with their cause" body="An explained difference names the column and the reason, so a reviewer can sign it." bleed>
        <div className="mk-app-surface mk-tui">
          <div className="mk-tui-head">
            <strong>
              {explained.name} → {explained.dest}
            </strong>
            <span className={`mk-app-pill ${TONE_PILL[explained.trust.tone]}`}>{explained.trust.verdict}</span>
          </div>
          <dl className="mk-tui-counts">
            <div>
              <dt>Rows read at the source</dt>
              <dd>{fmtN(explained.trust.source)}</dd>
            </div>
            <div>
              <dt>Rows in the destination</dt>
              <dd>{fmtN(explained.trust.destination)}</dd>
            </div>
            <div>
              <dt>Rows that differ</dt>
              <dd>{fmtN(explained.trust.differ)}</dd>
            </div>
          </dl>
          <p className="mk-tui-note">{explained.trust.note}</p>
        </div>
      </BentoTile>
    </Bento>
  );
}

/* ── Shared fragments ──────────────────────────────────────────────── */

const jobById = (id: string) => EXAMPLE_JOBS.find((j) => j.id === id) ?? EXAMPLE_JOBS[0];

/** Label and value rows, as the app's drawers and forms show them. */
function Kv({ rows, label, className = "" }: { rows: [ReactNode, ReactNode][]; label?: string; className?: string }) {
  return (
    <dl className={`mk-tui-kv ${className}`.trim()} aria-label={label}>
      {rows.map(([k, v], i) => (
        <div key={i}>
          <dt>{k}</dt>
          <dd>{v}</dd>
        </div>
      ))}
    </dl>
  );
}

/** A switch drawn as the app draws it; state is in the text too. */
function SwitchRow({ title, note, on }: { title: string; note: string; on: boolean }) {
  return (
    <div className="mk-tui-switch">
      <span>
        <strong>{title}</strong>
        <small>{note}</small>
      </span>
      <span className={`mk-tui-switch-track${on ? " is-on" : ""}`} aria-hidden>
        <span />
      </span>
      <span className="mk-sr-only">{on ? "On" : "Off"}</span>
    </div>
  );
}

/** Things no tool exists for: a circle-slash per line. */
function CannotRows({ items, label }: { items: readonly string[]; label: string }) {
  return (
    <ul className="mk-tui-cannot" aria-label={label}>
      {items.map((c) => (
        <li key={c}>
          <CircleSlash size={14} aria-hidden />
          {c}
        </li>
      ))}
    </ul>
  );
}

/* ── Pilot: what you can ask about ─────────────────────────────────── */

/** Plan tools in the shared registry (docs/use/mcp.md, Tools). */
const PLAN_TOOLS: { tool: string; does: string }[] = [
  { tool: "plan_transfer", does: "Plans a transfer between two saved connectors" },
  { tool: "plan_transfer_route", does: "Proposes a route, sync mode and the checks it needs" },
  { tool: "remediate_validation", does: "Proposes a fix for a Validate finding" },
];

/** Pilot's engine settings (docs/use/pilot.md, Use of AI models). */
const ENGINES: { id: string; does: string }[] = [
  { id: "local", does: "A built-in planner runs the tools and writes the answer. No model provider is called." },
  { id: "hybrid", does: "Tools, gates and facts run in your deployment. The provider only words the answer." },
  { id: "cloud", does: "A provider model drives the tool loop and writes the answer." },
];

export function PilotBento() {
  const failed = jobById("3e71aa58");
  return (
    <Bento className="mk-kit-bento--fill" label="What you can ask Pilot about">
      <BentoTile span={7} title="Why a run failed, from the run itself" body="Pilot reads the job and its Trust Report, and answers from what they say." bleed>
        <div className="mk-app-surface mk-tui">
          <p className="mk-tui-you">Why did {failed.name} fail?</p>
          <div className="mk-tui-answer">
            <p>{failed.trust.note}</p>
            <p className="mk-tui-head">
              <span className="mk-app-chip">
                <Sparkles size={12} aria-hidden />
                get_job
              </span>
              <span className="mk-app-pill mk-app-pill--completed">Grounded</span>
              <span className="mk-tui-muted">
                Job <code>{failed.id}</code>
              </span>
            </p>
          </div>
        </div>
      </BentoTile>

      <BentoTile span={5} title="Plans that change nothing" body="Ask for a route, a sync mode or a fix. A plan is a proposal; nothing runs." bleed>
        <div className="mk-app-surface mk-tui">
          <ul className="mk-tui-tools" aria-label="Plan tools (from the docs)">
            {PLAN_TOOLS.map((t) => (
              <li key={t.tool}>
                <code>{t.tool}</code>
                <span className="mk-app-pill mk-app-pill--t-none">Plan</span>
                <span className="mk-tui-muted">{t.does}</span>
              </li>
            ))}
          </ul>
        </div>
      </BentoTile>

      <BentoTile span={5} title="Keep it local if you need to" body="Pin the engine to local and no model provider is called." bleed>
        <div className="mk-app-surface mk-tui">
          <ul className="mk-tui-options" aria-label="Pilot engine settings (from the docs)">
            {ENGINES.map((e, i) => (
              <li key={e.id} className={i === 0 ? "is-active" : undefined}>
                <span className="mk-tui-radio" aria-hidden />
                <code>
                  {e.id}
                  {i === 0 ? <span className="mk-sr-only"> (selected)</span> : null}
                </code>
                <span className="mk-tui-options-does">{e.does}</span>
              </li>
            ))}
          </ul>
        </div>
      </BentoTile>

      <BentoTile span={7} title="No tool exists for these" body="Pilot can word a result. It cannot change one." bleed>
        <div className="mk-app-surface mk-tui">
          <CannotRows items={PILOT_CANNOT} label="What Pilot has no tool for" />
        </div>
      </BentoTile>
    </Bento>
  );
}

/* ── MCP server: what an agent can reach ───────────────────────────── */

/** The tool counts in docs/use/mcp.md (generated from the registry). */
const MCP_TOOL_COUNT = { total: 71, read: 45, plan: 3, mutate: 23 };

const MCP_TOOLS: { tool: string; perm: string; effect: "read" | "plan" | "mutate" }[] = [
  { tool: "list_connectors", perm: "connector.read", effect: "read" },
  { tool: "explain_parity", perm: "job.read", effect: "read" },
  { tool: "run_query", perm: "query.use", effect: "read" },
  { tool: "plan_transfer", perm: "job.plan", effect: "plan" },
  { tool: "start_transfer", perm: "job.run", effect: "mutate" },
  { tool: "replay_quarantine", perm: "job.manage", effect: "mutate" },
  { tool: "create_schedule", perm: "schedule.manage", effect: "mutate" },
];

const EFFECT: Record<"read" | "plan" | "mutate", { name: string; pill: string }> = {
  read: { name: "Read", pill: "mk-app-pill--completed" },
  plan: { name: "Plan", pill: "mk-app-pill--t-none" },
  mutate: { name: "Staged", pill: "mk-app-pill--v-not-run" },
};

/** What a key's role reaches over MCP (helpDocs help-mcp, connect Cursor). */
const KEY_ROLES: [string, string][] = [
  ["Viewer", "Read-only tools"],
  ["Operator", "Also runs and cancels jobs"],
  ["Editor", "Everything except deleting connections"],
  ["Admin", "Every tool"],
];

/** What MCP cannot do (docs/use/mcp.md). */
const MCP_CANNOT = ["Sign a Trust Report, or approve a fix to one", "Change workspace settings, members, roles or API keys", "Run SQL that changes data"] as const;

/** The confirmation model (docs/use/mcp.md). */
const MCP_RULES: [ReactNode, ReactNode][] = [
  [<code key="s">start_transfer</code>, <>Stages the action and returns an <code>ack_id</code> with a preview</>],
  [<code key="c">confirm_action</code>, "Runs it, after the permission is checked again"],
  ["Expires", "15 minutes after staging"],
  ["Secrets", <>Stay on the server; the client gets the <code>ack_id</code> and the preview</>],
];

export function McpBento() {
  return (
    <Bento className="mk-kit-bento--fill" label="What an agent can reach over MCP">
      <BentoTile
        span={7}
        title="Each tool needs the permission its API route needs"
        body={`${MCP_TOOL_COUNT.total} tools in the docs: ${MCP_TOOL_COUNT.read} read, ${MCP_TOOL_COUNT.plan} plan, and ${MCP_TOOL_COUNT.mutate} that change something.`}
        bleed
      >
        <div className="mk-app-surface mk-tui">
          <table className="mk-tui-table mk-tui-table--stack">
            <caption className="mk-sr-only">MCP tools, the permission each needs, and its kind (an excerpt of the docs)</caption>
            <thead>
              <tr>
                <th scope="col">Tool</th>
                <th scope="col">Permission</th>
                <th scope="col">Kind</th>
              </tr>
            </thead>
            <tbody>
              {MCP_TOOLS.map((t) => (
                <tr key={t.tool}>
                  <th scope="row">
                    <code>{t.tool}</code>
                  </th>
                  <td>
                    <code>{t.perm}</code>
                  </td>
                  <td>
                    <span className={`mk-app-pill ${EFFECT[t.effect].pill}`}>{EFFECT[t.effect].name}</span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </BentoTile>

      <BentoTile span={5} title="The key's role decides the tools" body="A key works like a person with the same role, never with more." bleed>
        <div className="mk-app-surface mk-tui">
          <p className="mk-tui-keyline">
            <KeyRound size={14} aria-hidden />
            <code>Authorization: Bearer &lt;workspace-api-key&gt;</code>
          </p>
          <Kv rows={KEY_ROLES} label="What each role's key can reach" />
        </div>
      </BentoTile>

      <BentoTile span={5} title="No tool exists for these" body="An agent can read and explain a Trust Report. It cannot sign one." bleed>
        <div className="mk-app-surface mk-tui">
          <CannotRows items={MCP_CANNOT} label="What MCP cannot do" />
        </div>
      </BentoTile>

      <BentoTile span={7} title="A change waits for confirm_action" body="A tool that would change something stages it. Nothing has happened yet." bleed>
        <div className="mk-app-surface mk-tui">
          <div className="mk-tui-head">
            <strong>Staged action</strong>
            <span className="mk-app-pill mk-app-pill--v-not-run">Staged, not run</span>
          </div>
          <Kv rows={MCP_RULES} label="The confirmation model" />
        </div>
      </BentoTile>
    </Bento>
  );
}

/* ── Schedules: what a schedule runs under ─────────────────────────── */

/** The example schedule (the Orders feed every page shows). */
const FEED = {
  name: "Orders feed, every 2 min",
  route: "Orders Postgres (demo) → Warehouse Snowflake (demo)",
  cadence: "Every 2 minutes",
  mode: "Incremental deduped",
  cursor: "ordered_at",
  key: "order_id",
};

/** One tick opened: the same facts as the Schedules page's tick panel (ProductPanels TickDetailPanel). */
const TICK = {
  at: "12:07",
  started: "12:07, the run after the reviewed column",
  approved: "12:05",
  checks: "Same Validate checks as the first run, all passed",
  accounting: "Every row read landed; none held, none skipped",
};

/** The schedule form's defaults (components/schedules/ScheduleForm.tsx). */
const RETRY_DEFAULTS = { retries: 2, backoff: 30 };

export function SchedulesBento() {
  const delays = Array.from({ length: RETRY_DEFAULTS.retries }, (_, i) => `${RETRY_DEFAULTS.backoff * (i + 1)}s`).join(", ");
  return (
    <Bento className="mk-kit-bento--fill" label="What a schedule runs under">
      <BentoTile span={7} title="One approved transfer, on a cadence" body="The schedule keeps the route, the map and the sync mode you approved, and runs them as a full job each time." bleed>
        <div className="mk-app-surface mk-tui">
          <div className="mk-tui-head">
            <strong>{FEED.name}</strong>
            <span className="mk-tui-muted">Overview</span>
          </div>
          <Kv
            label="Schedule overview (example)"
            rows={[
              ["Route", FEED.route],
              ["Cadence", FEED.cadence],
              ["Sync mode", FEED.mode],
              [
                "Cursor and key",
                <>
                  <code>{FEED.cursor}</code>, deduplicated on <code>{FEED.key}</code>
                </>,
              ],
            ]}
          />
          <div className="mk-tui-actions" aria-hidden>
            <span className="mk-app-tbtn">
              <Play size={13} />
              Run now
            </span>
            <span className="mk-app-tbtn">
              <RefreshCw size={13} />
              Re-sync (full)
            </span>
            <span className="mk-app-tbtn">
              <Pause size={13} />
              Pause
            </span>
            <span className="mk-app-tbtn">Export YAML</span>
          </div>
        </div>
      </BentoTile>

      <BentoTile span={5} title="Retries and alerts you set" body="A run that errors is retried with a backoff you set; you choose who hears about it." bleed>
        <div className="mk-app-surface mk-tui">
          <div className="mk-tui-head">
            <strong>Retry &amp; notifications</strong>
          </div>
          <div className="mk-tui-fields">
            <p>
              <small>Max retries</small>
              <span className="mk-tui-input">{RETRY_DEFAULTS.retries}</span>
            </p>
            <p>
              <small>Retry backoff (seconds)</small>
              <span className="mk-tui-input">{RETRY_DEFAULTS.backoff}</span>
            </p>
          </div>
          <p className="mk-tui-muted">
            Up to {RETRY_DEFAULTS.retries} retries, linear backoff before attempts: {delays}.
          </p>
          <SwitchRow title="Notify on failure" note="Alert configured channels when a run fails." on />
          <SwitchRow title="Notify on success" note="Also alert when a run completes cleanly." on={false} />
        </div>
      </BentoTile>

      <BentoTile span={12} title="Every tick opens as its own job" body="Its map version, the checks it cleared, and its statement, with the job's Trust Report one click away." bleed>
        <div className="mk-app-surface mk-tui">
          <div className="mk-tui-head">
            <strong>Tick at {TICK.at}</strong>
            <span className="mk-app-pill mk-app-pill--completed">Balanced</span>
            <span className="mk-tui-muted">{FEED.name}</span>
          </div>
          <Kv
            className="mk-tui-kv--2col"
            label={`Tick at ${TICK.at} (example)`}
            rows={[
              ["Started", TICK.started],
              [
                "Map",
                <>
                  The version approved at {TICK.approved}, with <code>coupon_code</code> added
                </>,
              ],
              ["Checks", TICK.checks],
              ["Accounting", TICK.accounting],
            ]}
          />
        </div>
      </BentoTile>
    </Bento>
  );
}

/* ── Query: what it will and will not do ───────────────────────────── */

const READ_HEADS = ["SELECT", "WITH", "EXPLAIN", "SHOW", "DESCRIBE", "ANALYZE", "PRAGMA", "VALUES"];
const REFUSED = ["DML", "DDL", "CALL", "EXEC"];
const READ_ONLY_TX = [
  { id: "postgresql", name: "PostgreSQL" },
  { id: "mysql", name: "MySQL" },
  { id: "sqlite", name: "SQLite" },
  { id: "oracle", name: "Oracle" },
];
const QUERY_SQL = "SELECT segment, COUNT(*) AS customers\nFROM customers\nGROUP BY segment\nORDER BY segment;";

export function QueryBento() {
  return (
    <Bento className="mk-kit-bento--fill" label="What Query will and will not do">
      <BentoTile span={7} title="Reads, and nothing else" body="Read and metadata statements run. Writes are refused in the editor, before anything is sent." bleed>
        <div className="mk-app-surface mk-tui">
          <div className="mk-tui-chiprow">
            <span className="mk-tui-muted">Runs</span>
            <ul aria-label="Statements that run">
              {READ_HEADS.map((h) => (
                <li key={h}>
                  <code>{h}</code>
                </li>
              ))}
            </ul>
          </div>
          <div className="mk-tui-chiprow is-refused">
            <span className="mk-tui-muted">Refused</span>
            <ul aria-label="Statements refused before they are sent">
              {REFUSED.map((h) => (
                <li key={h}>
                  <Ban size={12} aria-hidden />
                  <code>{h}</code>
                </li>
              ))}
            </ul>
          </div>
          <p className="mk-tui-foot">
            <span>Also in a read-only transaction on</span>
            {READ_ONLY_TX.map((c) => (
              <span key={c.id} className="mk-app-chip">
                <ConnectorIcon id={c.id} size={14} />
                {c.name}
              </span>
            ))}
          </p>
        </div>
      </BentoTile>

      <BentoTile span={5} title="Runs as you, on saved connections" body="Each tab keeps its own connection and runs under your workspace role." bleed>
        <div className="mk-app-surface mk-tui">
          <Kv
            label="Query tab (example)"
            rows={[
              [
                "Connection",
                <span key="c" className="mk-app-chip">
                  <ConnectorIcon id="mysql" size={14} />
                  Customers MySQL (demo)
                </span>,
              ],
              ["Permission", <code key="p">query.use</code>],
              ["Credentials", "In the vault"],
            ]}
          />
        </div>
      </BentoTile>

      <BentoTile span={5} title="Small by default" body="A result stops at the row limit until you raise it, and copies out in three formats." bleed>
        <div className="mk-app-surface mk-tui">
          <div className="mk-tui-fields">
            <p>
              <small>Row limit</small>
              <span className="mk-tui-input">{fmtN(1000)}</span>
            </p>
            <p>
              <small>Rows</small>
              <span className="mk-tui-input is-plain">{QUERY_SEGMENTS.length}</span>
            </p>
          </div>
          <div className="mk-tui-actions" aria-hidden>
            <span className="mk-tui-muted">Copy as</span>
            <span className="mk-app-tbtn">CSV</span>
            <span className="mk-app-tbtn">JSON</span>
            <span className="mk-app-tbtn">Markdown</span>
          </div>
        </div>
      </BentoTile>

      <BentoTile span={7} title="To load a result, use Transfer Studio" body="Paste the SELECT as the source of a transfer. It still goes through Validate before a row is written." bleed>
        <div className="mk-app-surface mk-tui">
          <div className="mk-tui-head">
            <strong>Source extract</strong>
            <span className="mk-tui-seg" aria-hidden>
              <span>Table</span>
              <span className="is-active">SQL query</span>
              <span>Stored procedure</span>
            </span>
            <span className="mk-sr-only">SQL query selected</span>
          </div>
          <pre className="mk-tui-code" tabIndex={0} aria-label="The SELECT used as a transfer source (example)">
            <code>{QUERY_SQL}</code>
          </pre>
        </div>
      </BentoTile>
    </Bento>
  );
}

/* ── Migrations: where every row of the cutover load went ──────────── */

export function MigrationBento() {
  const ex = TRANSFER_EXAMPLES.migration;
  const r = MIGRATION_RUN;
  return (
    <Bento className="mk-kit-bento--eq mk-kit-bento--fill" label={`Where every row went: ${identity(r)}`}>
      <BentoTile span={4} title="Read" figure={fmtN(r.read)} body="Every row the load read from the legacy table." bleed>
        <div className="mk-app-surface mk-tui">
          <Kv
            label="The cutover load (example)"
            rows={[
              ["From", <code key="f">{ex.src.table}</code>],
              ["Into", <code key="t">{ex.dst.table}</code>],
              ["Route", `${ex.src.name} → ${ex.dst.name}`],
              ["Duration", ex.duration],
            ]}
          />
        </div>
      </BentoTile>

      <BentoTile span={4} op="=" tone="landed" title="Landed" figure={fmtN(r.landed)} body="Counted by the destination and compared row by row." bleed>
        <div className="mk-app-surface mk-tui">
          <div className="mk-tui-head">
            <span className="mk-app-pill mk-app-pill--t-proven">Proven equal</span>
            <span className="mk-tui-muted">Check: Full checksum</span>
          </div>
          <dl className="mk-tui-counts mk-tui-counts--rows">
            <div>
              <dt>In {ex.dst.table}</dt>
              <dd>{fmtN(r.landed)}</dd>
            </div>
            <div>
              <dt>Rows that differ</dt>
              <dd>0</dd>
            </div>
          </dl>
        </div>
      </BentoTile>

      <BentoTile span={4} op="+" tone="held" title="Held" figure={fmtN(r.held)} body="Refused by the destination, kept with the value and the reason." bleed>
        <div className="mk-app-surface mk-tui">
          <table className="mk-tui-table mk-tui-table--held">
            <caption className="mk-sr-only">Rows held on the example cutover load</caption>
            <thead>
              <tr>
                <th scope="col">Column</th>
                <th scope="col">Value</th>
                <th scope="col" className="is-num">
                  Rows
                </th>
              </tr>
            </thead>
            <tbody>
              <tr className="is-held">
                <th scope="row">
                  <code>created</code>
                </th>
                <td>
                  <code>NULL</code>
                </td>
                <td className="is-num">{fmtN(r.held)}</td>
              </tr>
            </tbody>
          </table>
          <p className="mk-tui-foot">
            <span>
              <code>CREATED_AT</code> is NOT NULL in the destination. {r.skipped === 0 ? "None skipped." : `${fmtN(r.skipped)} skipped by rule.`}
            </span>
          </p>
        </div>
      </BentoTile>
    </Bento>
  );
}

/* ── Warehouse loading: checked before the window, read back after ─── */

const WH_TYPES = ["units", "order_total", "ordered_at", "attributes"];
const WH_GATES = ["Destination write access", "Staging capacity", "Population fit", "Target table definition", "Create destination table"];

export function WarehouseBento() {
  const types = RECORDED_MAPPING.filter((m) => WH_TYPES.includes(m.col));
  const gates = RECORDED_CHECK_GROUPS.flatMap((g) => g.checks).filter((c) => WH_GATES.includes(c.name));
  const explained = jobById("5d20b7c4");
  return (
    <Bento className="mk-kit-bento--fill" label="How a warehouse load is checked">
      <BentoTile span={7} title="Exact types, the same map as any route" body="Decimals and timestamps keep their precision, and JSON lands as a semi-structured column." bleed>
        <div className="mk-app-surface mk-tui">
          <table className="mk-tui-table">
            <caption className="mk-sr-only">Source and destination types (example, from a recorded run)</caption>
            <thead>
              <tr>
                <th scope="col">Column</th>
                <th scope="col">{RECORDED_ROUTE.sourceEngine}</th>
                <th scope="col">{RECORDED_ROUTE.destEngine}</th>
              </tr>
            </thead>
            <tbody>
              {types.map((m) => (
                <tr key={m.col}>
                  <th scope="row">
                    <code>{m.col}</code>
                  </th>
                  <td>
                    <code>{m.from}</code>
                  </td>
                  <td>
                    <code>{m.to}</code>
                    {m.transform ? <span className="mk-tui-muted mk-tui-under">{m.transform}</span> : null}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </BentoTile>

      <BentoTile span={5} title="Checked before the load window" body="On the real warehouse: write rights, staging room, and the table you approved." bleed>
        <div className="mk-app-surface mk-tui">
          <ul className="mk-tui-gates" aria-label="Validate checks on a warehouse route (example)">
            {gates.map((c) => (
              <li key={c.name}>
                <Check size={14} aria-hidden />
                <span>{c.name}</span>
                <small>Passed</small>
              </li>
            ))}
          </ul>
        </div>
      </BentoTile>

      <BentoTile span={5} title="Upserts by key, checked first" body="Duplicate keys already in the destination block an upsert at Validate. A retried batch updates the same rows." bleed>
        <div className="mk-app-surface mk-tui">
          <div className="mk-tui-head">
            <span className="mk-status mk-status--stop">
              <OctagonX size={13} aria-hidden />1 blocking
            </span>
            <span className="mk-tui-muted">Into DW_DEMO.ORDER_LINES</span>
          </div>
          <p className="mk-tui-finding">
            <strong>Data integrity</strong> <code>order_line_id</code>
            <span>Duplicate keys already in the destination. An upsert on this key is blocked until they are resolved.</span>
          </p>
        </div>
      </BentoTile>

      <BentoTile span={7} title="Compared at the precision the column holds" body="A cast you approved can change a value. The Trust Report names it instead of calling the copy equal." bleed>
        <div className="mk-app-surface mk-tui">
          <div className="mk-tui-head">
            <strong>
              {explained.name} → {explained.dest}
            </strong>
            <span className="mk-app-pill mk-app-pill--t-explained">{explained.trust.verdict}</span>
            <span className="mk-tui-muted">Check: {explained.trust.check}</span>
          </div>
          <p className="mk-tui-note">{explained.trust.note}</p>
        </div>
      </BentoTile>
    </Bento>
  );
}

/* ── Recurring sync: how each run reads ────────────────────────────── */

export function SyncBento() {
  return (
    <Bento className="mk-kit-bento--fill" label="How each run reads">
      <BentoTile span={4} title="CDC" body="Changes from the database log, deletes included, applied by key. A consistent snapshot comes first." bleed>
        <div className="mk-app-surface mk-tui">
          <p className="mk-tui-muted">Proven in CI on these sources</p>
          <ul className="mk-tui-chips" aria-label="CDC sources proven in CI">
            {PROVEN_CDC_SOURCES.map((s) => (
              <li key={s} className="mk-app-chip">
                <Check size={12} aria-hidden />
                {s}
              </li>
            ))}
          </ul>
        </div>
      </BentoTile>

      <BentoTile span={4} title="A typed cursor" body="New and changed rows past the last run's cursor, updated in place by key." bleed>
        <div className="mk-app-surface mk-tui">
          <Kv
            label="Cursor settings (example, the Orders feed)"
            rows={[
              ["Mode", FEED.mode],
              ["Cursor", <code key="c">{FEED.cursor}</code>],
              ["Key", <code key="k">{FEED.key}</code>],
              ["Reads", "Past the last run's high-water mark"],
            ]}
          />
        </div>
      </BentoTile>

      <BentoTile span={4} title="Mirror" body="Rows gone from the source are flagged and kept, so the copy shows what was removed." bleed>
        <div className="mk-app-surface mk-tui">
          <Kv
            label="Mirror columns"
            rows={[
              [<code key="d">_deleted</code>, <code key="v">true</code>],
              [<code key="a">_deleted_at</code>, "When the row left the source"],
              ["Needs", "A key"],
            ]}
          />
        </div>
      </BentoTile>
    </Bento>
  );
}
