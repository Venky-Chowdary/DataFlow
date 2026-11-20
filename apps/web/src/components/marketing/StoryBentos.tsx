import { Check, Fingerprint } from "lucide-react";
import { Bento, BentoTile } from "./kit/Bento";
import { RECORDED_CHECK_GROUPS, RECORDED_MAPPING, RECORDED_ROUTE, StatusGlyph } from "./ProductPanels";
import { COVERAGE_RUNGS, RUN, RUN_HELD_KEYS, fmtN } from "./StoryPanels";
import { TRANSFER_EXAMPLES } from "./StoryScenes";
import { EXAMPLE_JOBS } from "./app-frame/AppFrame";

/*
 * Bentos for the product pages (pass 7). Every tile's UI is a fragment of a
 * screen the pages already draw in full, with the same example data:
 * Transfer Studio uses its own store_orders example; Job space uses the
 * example workspace and the home page's run.
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
