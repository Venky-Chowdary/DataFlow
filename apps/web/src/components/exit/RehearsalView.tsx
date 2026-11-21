/**
 * Page B: one exit rehearsal. A status header says what happened, when, and
 * the one thing to do next; a compact stepper follows the real run (a step
 * after a failure reads "Not run", never "Done"); one callout explains a
 * failure in the user's terms, with the server's wording kept for
 * administrators; then the results, the tables, the sealed files, the
 * evidence pack and sign-off.
 *
 * The verdict is "proven" only when every declared table is proven; the API
 * decides it. Sampled or count-only value checks are listed as what they are,
 * a projection is labelled as one, and the anchor reads "Not anchored" unless a
 * write-once store confirmed it.
 */

import { Fragment, useId, useRef, useState, type ChangeEvent } from "react";
import { DtIcon, type IconName } from "../DtIcon";
import { Button } from "../ui/Button";
import { Callout } from "../ui/Callout";
import { CopyIdChip } from "../ui/CopyIdChip";
import { HiddenFileInput } from "../ui/HiddenFileInput";
import { DurationTrendChart, WindowBar } from "../charts/ExitCharts";
import { SignoffPanel, type ParityBusy } from "../parity/SignoffPanel";
import { formatDateTime } from "../../lib/parity";
import { plural } from "../../lib/plural";
import {
  anchorPresentation,
  controlTotalText,
  coverageLines,
  formatBytes,
  formatDuration,
  growthText,
  pillClass,
  projectionText,
  rehearsalSignoff,
  rehearsalSignoffBlockedReason,
  rehearsalVerdictPresentation,
  shortHash,
  tableStatusPresentation,
  unprovableColumns,
  valueMethodPresentation,
  verifyPartsNote,
  type ExitRehearsal,
  type ExitVerifyResult,
  type RehearsalTable,
  isVerificationTimeout,
  reasonParts,
} from "../../lib/exitReadiness";
import {
  NO_BUCKET_TABLE,
  STALE_LINE,
  durationSummary,
  elapsedSeconds,
  formatMeasured,
  isNoExportBucket,
  primaryAction,
  rehearsalProblem,
  rehearsalSteps,
  type RehearsalStep,
} from "./rehearsalSummary";

export type RehearsalBusy = ParityBusy;

export interface RehearsalViewProps {
  rehearsal: ExitRehearsal;
  busy: RehearsalBusy;
  canSign: boolean;
  signDeniedReason?: string;
  errors?: { signoff?: string; verify?: string };
  verifyResult?: ExitVerifyResult | null;
  /** The automatic re-read of a running rehearsal stopped at its limit. */
  pollStopped?: boolean;
  onBack: () => void;
  onRefresh: () => void;
  onExportMarkdown: () => void;
  onDownloadSigned: () => void;
  onVerifySigned: () => void;
  onVerifyFile: (file: File) => void;
  onSign: (role: string, note: string) => void;
  onOpenJob?: (jobId: string) => void;
  /** Open the system (its PII review), for a rehearsal the PII gate held. */
  onOpenSystem?: (systemId: string) => void;
  /** Start a new rehearsal of this system. Absent: no re-run action is offered. */
  onRerun?: () => void;
  /** A re-run is being started. */
  rerunning?: boolean;
  /** Whether this person may start a rehearsal, and why not. */
  canRerun?: boolean;
  rerunDeniedReason?: string;
  /** Administrators see the server's wording of a failure (env var names) behind a disclosure. */
  isAdmin?: boolean;
}

function MatchText({ match, what }: { match: boolean | null; what: string }) {
  if (match == null) {
    return (
      <span className="df2-exit-match is-none" title={`${what} could not be measured on both sides`}>
        <DtIcon name="minus" size={12} /> Not measured
      </span>
    );
  }
  return match ? (
    <span className="df2-exit-match is-ok">
      <DtIcon name="check" size={12} /> Match
    </span>
  ) : (
    <span className="df2-exit-match is-bad">
      <DtIcon name="x" size={12} /> Differs
    </span>
  );
}

function totalsSummary(table: RehearsalTable): { text: string; match: boolean | null } {
  const totals = table.control_totals ?? [];
  // A table still running (or that never started), or a source that could not be re-read, measured no totals:
  // that is not "no numeric columns".
  if (!totals.length && (table.value_parity.method == null || table.status === "running" || table.status === "pending" || table.rows_source == null)) {
    return { text: "Not measured", match: null };
  }
  if (!totals.length) return { text: "No numeric columns", match: null };
  const measured = totals.filter((t) => t.match != null);
  const differ = totals.filter((t) => t.match === false).length;
  if (!measured.length) return { text: `${totals.length} not measured`, match: null };
  if (differ) return { text: `${differ} of ${totals.length} differ`, match: false };
  if (measured.length < totals.length) return { text: `${measured.length} of ${totals.length} match`, match: null };
  return { text: `${totals.length} of ${totals.length} match`, match: true };
}

function rowsText(table: RehearsalTable): string {
  const s = table.rows_source == null ? "?" : table.rows_source.toLocaleString("en-US");
  const e = table.rows_extracted == null ? "?" : table.rows_extracted.toLocaleString("en-US");
  if (table.rows_source == null && table.rows_extracted == null) return "Not counted";
  return table.rows_source === table.rows_extracted ? s : `${s} → ${e}`;
}

/** A table that never produced an extract (failed, or held by the PII gate): nothing in its row was measured. */
function neverExtracted(table: RehearsalTable): boolean {
  return (table.status === "failed" || table.status === "needs_pii_review") && table.part == null && table.rows_extracted == null;
}

/** A table's reason: its text, the server log reference to quote, and what to do after a verification timeout. */
function ReasonText({ reason }: { reason: string }) {
  if (isNoExportBucket(reason)) return <p className="df2-exit-table-reason">{NO_BUCKET_TABLE}</p>;
  const parts = reasonParts(reason);
  return (
    <>
      <p className="df2-exit-table-reason">
        {parts.text}
        {parts.reference && (
          <>
            {" "}
            <CopyIdChip id={parts.reference} label="Reference" compact />
          </>
        )}
      </p>
      {isVerificationTimeout(reason) && (
        <p className="df2-parity-muted">The verification stopped at its time limit, so nothing was proven or disproven: rehearse again.</p>
      )}
    </>
  );
}

function TableDetail({ table, onOpenJob }: { table: RehearsalTable; onOpenJob?: (jobId: string) => void }) {
  const unprovable = unprovableColumns(table.reason);
  const jobButton = table.job_id && onOpenJob && (
    <div className="df2-parity-actions">
      <Button size="sm" variant="ghost" onClick={() => onOpenJob(table.job_id!)} leadingIcon={<DtIcon name="jobs" size={14} />}>
        Open extract job
      </Button>
    </div>
  );
  if (neverExtracted(table)) {
    return (
      <div className="df2-exit-table-detail">
        <ReasonText reason={table.reason} />
        {jobButton}
      </div>
    );
  }
  return (
    <div className="df2-exit-table-detail">
      <ReasonText reason={table.reason} />
      {unprovable.length > 0 && (
        <ul className="df2-exit-diffs" aria-label={`Columns of ${table.name} that cannot be proven`}>
          {unprovable.map((u) => (
            <li key={`${u.kind}:${u.column}`}>
              <span className={pillClass("warn")}>Not provable</span>
              <span className="df2-mono">{u.column}</span>
              <span className={pillClass("muted")}>{u.label}</span>
            </li>
          ))}
        </ul>
      )}
      {table.differences.length > 0 && (
        <ul className="df2-exit-diffs" aria-label={`Differences in ${table.name}`}>
          {table.differences.map((d, i) => (
            <li key={`${d.kind}:${i}`}>
              <span className={pillClass("danger")}>
                {d.rows == null ? "" : `${d.rows_at_least ? "≥ " : ""}${d.rows.toLocaleString("en-US")} `}
                {d.kind.replace(/_/g, " ")}
              </span>
              <span>{d.detail}</span>
            </li>
          ))}
        </ul>
      )}
      <dl className="df2-exit-facts">
        <div>
          <dt>Key columns</dt>
          <dd className="df2-mono">{table.key_columns.length ? table.key_columns.join(", ") : "None (the row digest is the check)"}</dd>
        </div>
        <div>
          <dt>Key-set hash</dt>
          <dd className="df2-mono" title={table.key_set.source_sha256 ?? undefined}>
            {table.key_set.source_sha256 ? shortHash(table.key_set.source_sha256) : "Not measured"}
          </dd>
        </div>
        <div>
          <dt>Row-set hash</dt>
          <dd className="df2-mono" title={table.row_set.source_sha256 ?? undefined}>
            {table.row_set.source_sha256 ? shortHash(table.row_set.source_sha256) : "Not measured"}
          </dd>
        </div>
        <div>
          <dt>Value check</dt>
          <dd>
            {valueMethodPresentation(table.value_parity.method).label}
            {table.value_parity.unexplained ? ` · ${table.value_parity.unexplained.toLocaleString("en-US")} unexplained` : ""}
          </dd>
        </div>
        <div>
          <dt>Extract time</dt>
          <dd>{formatDuration(table.extract_seconds)}</dd>
        </div>
        {table.part && (
          <div>
            <dt>Part</dt>
            <dd title={table.part.sha256}>
              <span className="df2-mono df2-exit-part-name">{table.part.name}</span>
              <span>{formatBytes(table.part.size_bytes)} · {table.part.verified_on_read ? "verified on read" : "not verified on read"}</span>
            </dd>
          </div>
        )}
      </dl>
      {table.control_totals.length > 0 && (
        <div className="df2-parity-scroll">
          <table className="df2-table df2-table-compact df2-exit-totals">
            <caption className="df2-sr-only">Control totals for {table.name}</caption>
            <thead>
              <tr>
                <th scope="col">Column</th>
                <th scope="col" className="is-num">Source sum</th>
                <th scope="col" className="is-num">Extract sum</th>
                <th scope="col" className="is-num">Min / max (source)</th>
                <th scope="col">Result</th>
              </tr>
            </thead>
            <tbody>
              {table.control_totals.map((t) => (
                <tr key={t.column}>
                  <td className="df2-mono">{t.column}</td>
                  <td className={`is-num${t.masked ? "" : " df2-mono"}`}>{controlTotalText(t, "source", "sum")}</td>
                  <td className={`is-num${t.masked ? "" : " df2-mono"}`}>{controlTotalText(t, "extract", "sum")}</td>
                  <td className={`is-num${t.masked ? "" : " df2-mono"}`}>{controlTotalText(t, "source", "range")}</td>
                  <td><MatchText match={t.match} what={`The ${t.column} total`} /></td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
      {jobButton}
    </div>
  );
}

function VerifyResultCard({ result }: { result: ExitVerifyResult }) {
  const anchor = anchorPresentation(result.anchor);
  return (
    <div className={`df2-exit-verify ${result.ok ? "is-ok" : "is-bad"}`} role="status" aria-live="polite">
      <p className="df2-exit-verify-head">
        <span className={pillClass(result.ok ? "ok" : "danger")}>{result.ok ? "Pack verified" : "Pack did not verify"}</span>
        <span className="df2-parity-muted">Checked {formatDateTime(result.anchor.checked_at)}</span>
      </p>
      <ul className="df2-exit-checks">
        <li>
          <MatchText match={result.signature_ok} what="The signature" /> <span>Signature over the pack body</span>
        </li>
        <li>
          <MatchText match={result.merkle_ok} what="The Merkle root" /> <span>Merkle root recomputed from the parts</span>
        </li>
        {verifyPartsNote(result) && (
          <li>
            <span className="df2-exit-match is-none">
              <DtIcon name="minus" size={12} /> Not re-read
            </span>{" "}
            <span>{verifyPartsNote(result)}</span>
          </li>
        )}
        <li>
          <span className={pillClass(anchor.tone)}>{anchor.label}</span> <span>{anchor.detail}</span>
        </li>
      </ul>
      {result.errors.length > 0 && (
        <ul className="df2-exit-verify-errors">
          {result.errors.map((e, i) => (
            <li key={i}>{e}</li>
          ))}
        </ul>
      )}
    </div>
  );
}

const STEP_ICON: Record<RehearsalStep["state"], IconName | null> = {
  done: "check",
  running: "activity",
  failed: "x",
  held: "shield",
  waiting: "clock",
  not_started: null,
  not_run: "minus",
};

/** The run as a compact stepper: each step says its state in words, never by colour alone. */
function Stepper({ steps }: { steps: RehearsalStep[] }) {
  return (
    <ol className="df2-exit-steps" aria-label="Rehearsal steps">
      {steps.map((s) => {
        const icon = STEP_ICON[s.state];
        return (
          <li key={s.stage} className={`df2-exit-steps-item is-${s.state.replace("_", "-")}`}>
            <span className="df2-exit-steps-mark" aria-hidden>
              {icon ? <DtIcon name={icon} size={14} chrome="product" /> : null}
            </span>
            <span className="df2-exit-steps-text">
              <span className="df2-exit-steps-label">{s.label}</span>{" "}
              <span className="df2-exit-steps-state">{s.stateLabel}</span>
              {s.detail ? (
                <>
                  {" "}
                  <span className="df2-exit-steps-detail">{s.detail}</span>
                </>
              ) : null}
            </span>
          </li>
        );
      })}
    </ol>
  );
}

/** "Started Nov 19, 2026, 2:02 PM by qa@example.com · took 22 s". */
function metaLine(r: ExitRehearsal): string {
  const who = r.trigger === "schedule" ? " on schedule" : r.actor ? ` by ${r.actor}` : "";
  const elapsed = elapsedSeconds(r);
  const tail = r.verdict === "running" ? " · running" : elapsed != null ? ` · took ${formatMeasured(elapsed)}` : "";
  return `Started ${formatDateTime(r.started_at)}${who}${tail}`;
}

export function RehearsalView({
  rehearsal,
  busy,
  canSign,
  signDeniedReason,
  errors = {},
  verifyResult = null,
  pollStopped = false,
  onBack,
  onRefresh,
  onExportMarkdown,
  onDownloadSigned,
  onVerifySigned,
  onVerifyFile,
  onSign,
  onOpenJob,
  onOpenSystem,
  onRerun,
  rerunning = false,
  canRerun = true,
  rerunDeniedReason,
  isAdmin = false,
}: RehearsalViewProps) {
  const verdict = rehearsalVerdictPresentation(rehearsal.verdict);
  const { completeness, duration, seal, evidence, trend } = rehearsal;
  const anchor = anchorPresentation(evidence.anchor);
  const signed = rehearsal.signoff.status === "signed";
  const running = rehearsal.verdict === "running";
  const anyBusy = busy != null;
  const fileId = useId();
  const tablesId = useId();
  const titleId = useId();
  const signoffAnchor = `exit-signoff-${rehearsal.id}`;
  const fileRef = useRef<HTMLInputElement>(null);
  const steps = rehearsalSteps(rehearsal);
  const problem = rehearsalProblem(rehearsal);
  const time = durationSummary(rehearsal);
  // The API's `current` (contract 12.1): false once the system's definition changed after this rehearsal.
  const stale = rehearsal.current === false && !running;
  const action = primaryAction(rehearsal, { canRerun: Boolean(onRerun), canReviewPii: Boolean(onOpenSystem) });
  const showSignoff = rehearsal.verdict === "proven" || signed
    || rehearsal.signoff.approvals.length > 0 || rehearsal.signoff.invalidated_approvals.length > 0;
  // Until it is signed, the pack's state is the sign-off panel's to say.
  const showEvidence = signed || verifyResult != null || Boolean(errors.verify);
  const measuredPoints = trend.points.filter((p) => p.measured_seconds != null).length;
  const showTrend = measuredPoints >= 2 || trend.projection != null;
  const [open, setOpen] = useState<Set<string>>(
    () => new Set(
      rehearsal.tables
        .filter((t) => t.status === "incomplete" || (t.status === "failed" && !isNoExportBucket(t.reason)))
        .slice(0, 3)
        .map((t) => t.name),
    ),
  );
  const toggle = (name: string) =>
    setOpen((prev) => {
      const next = new Set(prev);
      if (next.has(name)) next.delete(name);
      else next.add(name);
      return next;
    });

  const pickFile = (e: ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    e.target.value = "";
    if (file) onVerifyFile(file);
  };

  const goToSignoff = () => {
    const target = typeof document !== "undefined" ? document.getElementById(signoffAnchor) : null;
    target?.scrollIntoView?.({ behavior: "smooth", block: "start" });
    target?.focus({ preventScroll: true });
  };

  // Value checks: the methods that ran on extracted tables. "None" on a table that was never
  // extracted is explained by the failure itself, so it is not listed again.
  const notExtracted = rehearsal.tables.filter(neverExtracted).length;
  const coverage = coverageLines(completeness.value_methods)
    .map((line) => (line.method === "none" ? { ...line, count: line.count - notExtracted } : line))
    .filter((line) => line.count > 0);

  const chips: Array<{ n: number; label: string; tone: "ok" | "warn" | "danger" }> = [
    { n: completeness.tables_proven, label: "proven", tone: "ok" },
    { n: completeness.tables_not_proven, label: "not proven", tone: "warn" },
    { n: completeness.tables_incomplete, label: "incomplete", tone: "danger" },
    { n: completeness.tables_failed, label: "failed", tone: "danger" },
    { n: completeness.tables_needs_pii_review ?? 0, label: "held for PII review", tone: "warn" },
  ];

  return (
    <div className="df2-parity df2-exit-run" aria-busy={anyBusy || running || undefined}>
      <nav className="df2-exit-crumbs" aria-label="Breadcrumb">
        <button type="button" className="df2-exit-crumb-link" onClick={onBack}>
          <DtIcon name="arrow-left" size={14} /> Exit readiness
        </button>
      </nav>

      <section className="df2-exit-rv-head" aria-labelledby={titleId}>
        <div className="df2-exit-rv-head-top">
          <div className="df2-exit-rv-title-block">
            <div className="df2-exit-rv-title-row">
              <span className={pillClass(verdict.tone)}>{verdict.label}</span>
              <h2 id={titleId} className="df2-exit-rv-title">
                {rehearsal.system_name} · rehearsal {rehearsal.n}
              </h2>
            </div>
            <p className="df2-exit-rv-meta">{metaLine(rehearsal)}</p>
          </div>
          <div className="df2-exit-rv-actions">
            {(running || pollStopped) && (
              <Button
                size="sm"
                variant="ghost"
                onClick={onRefresh}
                disabled={anyBusy}
                loading={busy?.kind === "refresh"}
                loadingLabel="Refreshing…"
                leadingIcon={<DtIcon name="refresh" size={14} />}
              >
                Refresh
              </Button>
            )}
            <Button
              size="sm"
              onClick={onExportMarkdown}
              disabled={anyBusy}
              loading={busy?.kind === "export"}
              loadingLabel="Downloading…"
              leadingIcon={<DtIcon name="download" size={14} />}
            >
              Download report
            </Button>
            {action === "rerun" && (
              <Button
                size="sm"
                variant="primary"
                onClick={onRerun}
                disabled={!canRerun || rerunning}
                loading={rerunning}
                loadingLabel="Starting…"
                title={!canRerun ? rerunDeniedReason : undefined}
                leadingIcon={<DtIcon name="play" size={14} />}
              >
                Re-run rehearsal
              </Button>
            )}
            {action === "pii" && onOpenSystem && (
              <Button size="sm" variant="primary" onClick={() => onOpenSystem(rehearsal.system_id)} leadingIcon={<DtIcon name="shield" size={14} />}>
                Open PII review
              </Button>
            )}
            {action === "signoff" && showSignoff && (
              <Button size="sm" variant="primary" onClick={goToSignoff}>
                Go to sign-off
              </Button>
            )}
          </div>
        </div>
        {stale && (
          <p className="df2-exit-rv-stale" role="status">
            <DtIcon name="warning" size={16} chrome="product" />
            <span>{STALE_LINE}</span>
          </p>
        )}
        <Stepper steps={steps} />
      </section>

      {problem && (
        <Callout
          tone={problem.tone}
          title={problem.title}
          role="status"
          isAdmin={isAdmin}
          adminDetails={problem.adminDetail || undefined}
          action={
            problem.jobId && onOpenJob ? (
              <Button size="sm" onClick={() => onOpenJob(problem.jobId!)} leadingIcon={<DtIcon name="jobs" size={14} />}>
                Open extract job
              </Button>
            ) : undefined
          }
        >
          {problem.body ? <p>{problem.body}</p> : null}
        </Callout>
      )}
      {rehearsal.pii_acknowledgement && (
        <Callout tone="neutral" title={`PII acknowledged by ${rehearsal.pii_acknowledgement.actor} on ${formatDateTime(rehearsal.pii_acknowledgement.at)}.`}>
          <p>{rehearsal.pii_acknowledgement.reason}</p>
        </Callout>
      )}
      {pollStopped && running && (
        <Callout tone="info" title="Stopped checking automatically while the rehearsal is still running." role="status">
          <p>Refresh to see whether it has finished.</p>
        </Callout>
      )}

      <section className="df2-parity-card df2-exit-results" aria-label="Results">
        <h3 className="df2-parity-h3">Results</h3>
        <dl className="df2-exit-results-list">
          <div>
            <dt>Tables</dt>
            <dd>
              <span>
                {completeness.tables_extracted.toLocaleString("en-US")} of {plural(completeness.tables_declared, "table")} extracted
              </span>
              {!running && completeness.tables_extracted > 0 && chips.some((c) => c.n > 0) && (
                <ul className="df2-exit-chips" aria-label="Tables by result">
                  {chips.filter((c) => c.n > 0).map((c) => (
                    <li key={c.label}><span className={pillClass(c.tone)}>{c.n.toLocaleString("en-US")} {c.label}</span></li>
                  ))}
                </ul>
              )}
            </dd>
          </div>
          {coverage.length > 0 && (
            <div>
              <dt>Value checks</dt>
              <dd>
                <ul className="df2-exit-coverage">
                  {coverage.map((line) => (
                    <li key={line.method} title={line.note}>
                      {line.label} on {plural(line.count, "table")}
                      {!line.proves && <span className={pillClass("warn")}>Not proof</span>}
                    </li>
                  ))}
                </ul>
              </dd>
            </div>
          )}
          <div>
            <dt>Exit time</dt>
            <dd>
              <span className={`df2-exit-fit is-${time.tone}`}>{time.text}</span>
              {time.measured && (
                <>
                  <span className="df2-exit-results-bar">
                    <WindowBar measured={duration.measured_seconds} window={duration.window_seconds} fits={duration.fits_window} />
                  </span>
                  <span className="df2-parity-muted">
                    Extract {formatMeasured(duration.extract_seconds)}, verify {formatMeasured(duration.verify_seconds)}. Measured from job clocks, not estimated.
                  </span>
                </>
              )}
            </dd>
          </div>
        </dl>
      </section>

      {showTrend && (
        <section className="df2-parity-card" aria-label="Exit time over rehearsals">
          <h3 className="df2-parity-h3">Exit time over rehearsals</h3>
          {growthText(trend.growth_pct_rows) && <p className="df2-parity-muted">{growthText(trend.growth_pct_rows)}.</p>}
          <DurationTrendChart trend={trend} windowSeconds={duration.window_seconds} />
          {trend.projection && (
            <p className="df2-exit-projection">
              <DtIcon name="trend" size={14} chrome="product" />
              <span>{projectionText(trend.projection)}</span>
            </p>
          )}
        </section>
      )}

      <section className="df2-parity-card" aria-labelledby={tablesId}>
        <h3 id={tablesId} className="df2-parity-h3">Tables</h3>
        <div className="df2-parity-scroll" tabIndex={0} role="region" aria-label="Tables, scrollable">
          <table className="df2-table df2-exit-tables">
            <caption className="df2-sr-only">Completeness per declared table</caption>
            <thead>
              <tr>
                <th scope="col">Table</th>
                <th scope="col">Status</th>
                <th scope="col" className="is-num">Rows</th>
                <th scope="col">Key set</th>
                <th scope="col">Row set</th>
                <th scope="col">Control totals</th>
                <th scope="col">Value check</th>
                <th scope="col"><span className="df2-sr-only">Details</span></th>
              </tr>
            </thead>
            <tbody>
              {rehearsal.tables.map((table) => {
                const st = tableStatusPresentation(table.status);
                const totals = totalsSummary(table);
                const method = valueMethodPresentation(table.value_parity.method);
                const unprovable = unprovableColumns(table.reason).length;
                const expanded = open.has(table.name);
                const detailId = `${tablesId}-${table.name.replace(/[^A-Za-z0-9_-]/g, "_")}`;
                const skipped = neverExtracted(table);
                return (
                  <Fragment key={`${table.schema ?? ""}.${table.name}`}>
                    <tr className={expanded ? "is-expanded" : undefined}>
                      <td><span className="df2-mono df2-exit-table-name">{table.schema ? `${table.schema}.` : ""}{table.name}</span></td>
                      <td><span className={pillClass(st.tone)}>{st.label}</span></td>
                      {skipped ? (
                        <td colSpan={5} className="df2-exit-skipped">
                          {table.status === "failed" ? "Not extracted, so nothing was checked." : "Held by the PII gate, so nothing was checked."}
                        </td>
                      ) : (
                        <>
                          <td className="is-num">{rowsText(table)}</td>
                          <td>{table.key_columns.length || table.key_set.match != null ? <MatchText match={table.key_set.match} what="The key set" /> : <span className="df2-parity-muted">No key</span>}</td>
                          <td><MatchText match={table.row_set.match} what="The row set" /></td>
                          <td>
                            <span className={`df2-exit-totals-text${totals.match === false ? " is-bad" : ""}`}>{totals.text}</span>
                          </td>
                          <td>
                            <span className="df2-exit-method">
                              {method.label}
                              {!method.proves && table.value_parity.method != null && table.value_parity.method !== "none" ? <span className="df2-parity-muted"> (not proof)</span> : null}
                              {unprovable > 0 ? (
                                <span className="df2-exit-unprovable"> · {plural(unprovable, "column")} not provable</span>
                              ) : null}
                            </span>
                          </td>
                        </>
                      )}
                      <td className="df2-exit-expand">
                        <Button
                          size="sm"
                          variant="ghost"
                          aria-expanded={expanded}
                          aria-controls={detailId}
                          aria-label={`${expanded ? "Hide" : "Show"} details for ${table.name}`}
                          onClick={() => toggle(table.name)}
                        >
                          <DtIcon name={expanded ? "chevron-up" : "chevron-down"} size={14} />
                        </Button>
                      </td>
                    </tr>
                    {expanded && (
                      <tr className="df2-exit-detail-row">
                        <td colSpan={8} id={detailId}>
                          <TableDetail table={table} onOpenJob={onOpenJob} />
                        </td>
                      </tr>
                    )}
                  </Fragment>
                );
              })}
            </tbody>
          </table>
        </div>
      </section>

      {(seal.parts_count > 0 || showEvidence) && (
        <div className={seal.parts_count > 0 && showEvidence ? "df2-parity-grid2" : "df2-exit-rv-single"}>
          {seal.parts_count > 0 && (
            <section className="df2-parity-card" aria-label="Sealed files">
              <h3 className="df2-parity-h3">Sealed files</h3>
              <dl className="df2-exit-facts">
                <div>
                  <dt>Merkle root</dt>
                  <dd>
                    {seal.merkle_root ? (
                      <>
                        <code className="df2-mono df2-exit-hash">{seal.merkle_root}</code>
                        <CopyIdChip id={seal.merkle_root} label="Root" compact className="df2-exit-hash-chip" />
                      </>
                    ) : (
                      "Not sealed"
                    )}
                  </dd>
                </div>
                <div>
                  <dt>Algorithm</dt>
                  <dd className="df2-mono">{seal.algorithm}</dd>
                </div>
                {rehearsal.definition_sha256 && (
                  <div>
                    <dt>Definition SHA-256</dt>
                    <dd className="df2-mono" title={`What this rehearsal proved: the source, tables, keys and window. ${rehearsal.definition_sha256}`}>
                      {shortHash(rehearsal.definition_sha256)}
                    </dd>
                  </div>
                )}
                <div>
                  <dt>Parts</dt>
                  <dd>{plural(seal.parts_count, "part")} · {formatBytes(seal.total_bytes)}</dd>
                </div>
                <div>
                  <dt>Stored in</dt>
                  <dd>
                    {seal.storage === "object_store"
                      ? "Private export bucket"
                      : seal.storage === "local_file"
                        ? "Exports folder on the server"
                        : seal.storage === "mixed"
                          ? "Export bucket and exports folder"
                          : "Not stored"}
                  </dd>
                </div>
              </dl>
              {seal.parts.length > 0 && (
                <details className="df2-exit-parts">
                  <summary>SHA-256 of every part ({seal.parts.length.toLocaleString("en-US")})</summary>
                  <div className="df2-parity-scroll">
                    <table className="df2-table df2-table-compact">
                      <caption className="df2-sr-only">Sealed parts</caption>
                      <thead>
                        <tr>
                          <th scope="col">Table</th>
                          <th scope="col" className="is-num">Size</th>
                          <th scope="col">SHA-256</th>
                          <th scope="col">Read back</th>
                        </tr>
                      </thead>
                      <tbody>
                        {seal.parts.map((p) => (
                          <tr key={`${p.table}:${p.name}`}>
                            <td className="df2-mono">{p.table}</td>
                            <td className="is-num">{formatBytes(p.size_bytes)}</td>
                            <td className="df2-mono" title={p.sha256}>{shortHash(p.sha256)}</td>
                            <td>{p.verified_on_read ? <MatchText match what="The part" /> : <MatchText match={false} what="The part" />}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </details>
              )}
            </section>
          )}

          {showEvidence && (
            <section className="df2-parity-card" aria-label="Evidence pack">
              <h3 className="df2-parity-h3">Evidence pack</h3>
              <dl className="df2-exit-facts">
                <div>
                  <dt>Signed</dt>
                  <dd>{signed ? `Yes, ${formatDateTime(rehearsal.signoff.signed_at)}` : "Not yet: every required role signs it"}</dd>
                </div>
                {evidence.content_sha256 && (
                  <div>
                    <dt>Content SHA-256</dt>
                    <dd>
                      <code className="df2-mono df2-exit-hash">{evidence.content_sha256}</code>
                      <CopyIdChip id={evidence.content_sha256} label="Digest" compact className="df2-exit-hash-chip" />
                    </dd>
                  </div>
                )}
                <div>
                  <dt>Write-once anchor</dt>
                  <dd>
                    <span className={pillClass(anchor.tone)}>{anchor.label}</span>
                    <span className="df2-exit-anchor-detail">{anchor.detail}</span>
                    {evidence.anchor?.location && (
                      <span className="df2-mono df2-exit-anchor-detail" title={evidence.anchor.location.key}>
                        s3://{evidence.anchor.location.bucket}/{evidence.anchor.location.key}
                      </span>
                    )}
                  </dd>
                </div>
              </dl>
              <div className="df2-parity-actions">
                {signed && (
                  <>
                    <Button
                      size="sm"
                      variant="primary"
                      onClick={onVerifySigned}
                      disabled={anyBusy}
                      loading={busy?.kind === "verify"}
                      loadingLabel="Verifying…"
                      leadingIcon={<DtIcon name="shield" size={14} />}
                    >
                      Verify signed pack
                    </Button>
                    <Button
                      size="sm"
                      onClick={onDownloadSigned}
                      disabled={anyBusy}
                      loading={busy?.kind === "pack"}
                      loadingLabel="Downloading…"
                      leadingIcon={<DtIcon name="download" size={14} />}
                    >
                      Download signed pack
                    </Button>
                  </>
                )}
                <label htmlFor={fileId} className="df2-sr-only">Evidence pack file to verify</label>
                <HiddenFileInput id={fileId} ref={fileRef} accept="application/json,.json" disabled={anyBusy} onChange={pickFile} />
                <Button size="sm" variant="ghost" disabled={anyBusy} onClick={() => fileRef.current?.click()} leadingIcon={<DtIcon name="upload" size={14} />}>
                  Verify a pack file…
                </Button>
              </div>
              {verifyResult && <VerifyResultCard result={verifyResult} />}
              {errors.verify && (
                <Callout tone="danger" title="Could not verify the evidence pack." role="alert">
                  <p>{errors.verify}</p>
                </Callout>
              )}
              <p className="df2-parity-muted">
                The pack is evidence of this rehearsal: what was extracted, how it was checked and who signed. It does not
                certify compliance with any regulation; your auditors decide what it satisfies.
              </p>
            </section>
          )}
        </div>
      )}

      {showSignoff && (
        <SignoffPanel
          subject={{
            anchorId: signoffAnchor,
            signoff: rehearsalSignoff(rehearsal),
            blockedReason: rehearsalSignoffBlockedReason(rehearsal),
          }}
          copy={{
            intro:
              "Each approval is written to the audit log with your role in this system's workspace, which must hold the "
              + "authority the role needs. When every required role has signed, the Exit Evidence Pack is signed with the "
              + "workspace proof key and anchored.",
            packItems: [
              "The verdict and completeness of every declared table, with how each value check was measured",
              "The measured duration against the window",
              "The SHA-256 of every extracted part and the Merkle root over them",
              "Approvals with names, roles and times",
            ],
            exportLabel: "Download report (.md)",
            exportTitle: "The rehearsal report as Markdown, as it reads now.",
            signedNote: "The signed pack carries this signature and the Merkle root; Verify checks both and reads the anchor back.",
            downloadTitle: "The signed Exit Evidence Pack (JSON). Verify checks its signature, Merkle root and anchor.",
            noRoles: "No roles are required to sign this rehearsal.",
            denied: "You don't have permission to sign this rehearsal.",
            notePlaceholder: "What you checked before signing",
          }}
          busy={busy}
          canSign={canSign}
          signDeniedReason={signDeniedReason}
          onSign={onSign}
          onExport={onExportMarkdown}
          onDownloadSigned={onDownloadSigned}
          error={errors.signoff}
        />
      )}
    </div>
  );
}
