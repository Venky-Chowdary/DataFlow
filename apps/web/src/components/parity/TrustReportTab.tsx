/**
 * Page A: Job → Trust Report.
 *
 * One verdict with a stated confidence, then each table, then each difference
 * with its cause, the AI's explanation, proposed fixes, and sign-off (Page C).
 *
 * The verdict, causes and parity figures come from the API's deterministic
 * check. The AI writes the explanation and proposes fixes from a fixed menu; a
 * person approves each fix here before anything is applied.
 *
 * Gate-8, the trust score and the conservation ledger already have their own
 * cards on this job; this tab links to them instead of drawing them twice.
 */

import { useCallback, useEffect, useId, useRef, useState } from "react";
import { DtIcon } from "../DtIcon";
import { Button } from "../ui/Button";
import { EmptyState } from "../ui/EmptyState";
import { LoadingBlock } from "../LoadingState";
import { useToast } from "../Toast";
import { DifferenceSplitBar, ParityRing, TableParityBars } from "../charts/ParityCharts";
import { SignoffPanel, type ParityBusy } from "./SignoffPanel";
import {
  approveParityFix,
  dismissParityFix,
  downloadParityReportMarkdown,
  downloadParitySignedPack,
  explainParityReport,
  fetchParityReport,
  signParityReport,
} from "../../lib/api";
import { downloadBlob } from "../../lib/downloadBlob";
import { PERMISSIONS, useWriteGate } from "../../lib/PermissionsContext";
import {
  PARITY_APPLYING_POLL_MS,
  PARITY_MAX_ROUNDS,
  badgeClass,
  causeChip,
  causeLabel,
  differenceTitle,
  differencesByTable,
  explanationIsStale,
  fixApproveLabel,
  fixAttemptsUsed,
  fixConsequence,
  fixStatusPresentation,
  formatClock,
  formatCount,
  formatDateTime,
  isFixActionable,
  newerReport,
  parityErrorMessage,
  rereadLabel,
  roleLabel,
  parityIsLowerBound,
  parityRowsChecked,
  shouldPollApplying,
  approveOutcome,
  assuranceTone,
  proofTone,
  trustScoreTone,
  verdictPresentation,
  noDifferencesText,
  type ParityDifference,
  type ParityFix,
  type ParityReport,
  type ParityTone,
} from "../../lib/parity";

/* ── Pieces ──────────────────────────────────────────────────── */

function DifferenceRow({ diff }: { diff: ParityDifference }) {
  const chip = causeChip(diff);
  return (
    <li className="df2-parity-diff">
      <span className="df2-parity-diff-chips">
        <span className={badgeClass(chip.tone)}>{chip.label}</span>
        {diff.protected && (
          <span className={badgeClass("muted")} title="Values in this column are protected and never shown.">
            Protected
          </span>
        )}
      </span>
      <div className="df2-parity-diff-body">
        <b>{differenceTitle(diff)}</b>
        <span className="df2-parity-diff-cause">
          <span className="df2-parity-cause-label">{causeLabel(diff.cause)}.</span> {diff.cause.detail}
        </span>
        {diff.protected ? (
          <span className="df2-parity-muted">Example values: protected</span>
        ) : diff.example ? (
          <span className="df2-parity-example">
            Source <code className="df2-mono">{diff.example.source ?? "NULL"}</code>
            {" → "}destination <code className="df2-mono">{diff.example.destination ?? "NULL"}</code>
          </span>
        ) : null}
        {diff.keys_sample.length > 0 && (
          <details className="df2-parity-keys">
            <summary>
              {diff.keys_sample.length < diff.rows
                ? `First ${diff.keys_sample.length} of ${diff.rows.toLocaleString("en-US")} keys`
                : `${diff.keys_sample.length} ${diff.keys_sample.length === 1 ? "key" : "keys"}`}
            </summary>
            <code className="df2-mono">{diff.keys_sample.join(", ")}</code>
          </details>
        )}
      </div>
      <span
        className="df2-parity-num df2-parity-muted"
        aria-label={diff.rows_at_least ? `at least ${diff.rows} rows` : `${diff.rows} rows`}
        title={diff.rows_at_least ? "Gate-8 found this difference without counting its rows; re-run to count them." : undefined}
      >
        {diff.rows_at_least ? "≥ " : ""}
        {diff.rows.toLocaleString("en-US")}
      </span>
    </li>
  );
}

/**
 * Who decided, when, what happened, and their note. A dismissal's result is its
 * note, so the note is shown once.
 */
function decisionLines(fix: ParityFix): { summary: string; note: string } {
  if (fix.status === "proposed") return { summary: "", note: "" };
  const note = (fix.note || "").trim();
  const result = (fix.result || "").trim();
  const when = fix.applied_at || fix.decided_at;
  const head = [
    fix.approved_by ? `${fix.status === "dismissed" ? "Dismissed" : "Approved"} by ${fix.approved_by}` : "",
    when ? formatDateTime(when) : "",
  ]
    .filter(Boolean)
    .join(" · ");
  const shownResult = result && result !== note ? result : "";
  return { summary: [head, shownResult].filter(Boolean).join(". "), note };
}

export interface FixCardProps {
  fix: ParityFix;
  busy: ParityBusy;
  canAct: boolean;
  onApprove: (fixId: string, note: string) => void;
  onDismiss: (fixId: string, note: string) => void;
}

/** A proposed fix, with a confirmation step in the card before it is applied. */
export function FixCard({ fix, busy, canAct, onApprove, onDismiss }: FixCardProps) {
  const [confirming, setConfirming] = useState<"approve" | "dismiss" | null>(null);
  const [note, setNote] = useState("");
  const noteId = useId();
  // Accepting records a risk on the signed report: the API refuses it without a note (422).
  const noteRequired = fix.action === "accept" && confirming === "approve";
  const noteMissing = noteRequired && !note.trim();
  const status = fixStatusPresentation(fix.status);
  const actionable = isFixActionable(fix);
  const decision = decisionLines(fix);
  const anyBusy = busy != null;
  const mine = busy != null && "id" in busy && busy.id === fix.id;

  // A fix that stops being actionable (applied elsewhere, refreshed) closes its confirmation.
  useEffect(() => {
    if (!actionable) setConfirming(null);
  }, [actionable]);

  return (
    <article className={`df2-parity-fix is-${fix.status}`} aria-label={`Fix: ${fix.title}`}>
      <header className="df2-parity-fix-head">
        <b>{fix.title}</b>
        <span className={badgeClass(status.tone)}>{status.label}</span>
      </header>
      <p className="df2-parity-fix-detail">{fix.detail}</p>
      {fix.preview && <pre className="df2-parity-code">{fix.preview}</pre>}

      {!fix.applicable && fix.status === "proposed" && (
        <p className="df2-parity-muted">
          <DtIcon name="alert" size={12} /> Can't be applied: {fix.not_applicable_reason || "the destination does not support it."}
        </p>
      )}
      {decision.summary && (
        <p className={fix.status === "failed" ? "df2-parity-notice is-danger" : "df2-parity-muted"}>{decision.summary}</p>
      )}
      {decision.note && <p className="df2-parity-muted">Note: “{decision.note}”</p>}

      {actionable && canAct && confirming === null && (
        <div className="df2-parity-actions">
          <Button
            variant="primary"
            size="sm"
            onClick={() => setConfirming("approve")}
            disabled={anyBusy}
          >
            {fixApproveLabel(fix.action)}
          </Button>
          <Button size="sm" onClick={() => setConfirming("dismiss")} disabled={anyBusy}>
            Dismiss
          </Button>
        </div>
      )}

      {actionable && canAct && confirming !== null && (
        <form
          className="df2-parity-confirm"
          aria-label={confirming === "approve" ? "Confirm this fix" : "Confirm dismissing this fix"}
          onSubmit={(e) => {
            e.preventDefault();
            if (anyBusy || noteMissing) return;
            if (confirming === "approve") onApprove(fix.id, note);
            else onDismiss(fix.id, note);
          }}
        >
          <p className="df2-parity-confirm-text">
            <b>{confirming === "approve" ? "Apply this fix?" : "Dismiss this fix?"}</b>{" "}
            {confirming === "approve"
              ? fixConsequence(fix)
              : "Nothing is changed. The differences stay as they are and still count against sign-off."}
          </p>
          <div className="df2-field">
            <label className="df2-label" htmlFor={noteId}>
              Note {noteRequired ? "(required: why is this acceptable?)" : "(optional)"}
            </label>
            <textarea
              id={noteId}
              className="df2-input df2-parity-note-input"
              rows={2}
              maxLength={500}
              value={note}
              onChange={(e) => setNote(e.target.value)}
              disabled={anyBusy}
              required={noteRequired}
              aria-required={noteRequired || undefined}
            />
          </div>
          <div className="df2-parity-actions">
            <Button
              type="submit"
              variant={confirming === "approve" ? "primary" : "default"}
              size="sm"
              disabled={anyBusy || noteMissing}
              title={noteMissing ? "Say why these rows are acceptable; the note is recorded with the acceptance." : undefined}
              loading={mine}
              loadingLabel={confirming === "approve" ? "Applying and re-checking…" : "Dismissing…"}
            >
              {confirming === "approve" ? "Confirm and apply" : "Confirm dismiss"}
            </Button>
            <Button size="sm" variant="ghost" onClick={() => setConfirming(null)} disabled={mine}>
              Cancel
            </Button>
          </div>
        </form>
      )}
    </article>
  );
}

/* ── The view ────────────────────────────────────────────────── */

export interface TrustReportViewProps {
  report: ParityReport;
  busy: ParityBusy;
  /** job.run: approve or dismiss fixes. */
  canFix: boolean;
  fixDeniedReason?: string;
  /** job.read: explain and sign. */
  canSign: boolean;
  signDeniedReason?: string;
  /** Asking for an explanation writes to the report; defaults to the sign-off gate. */
  canExplain?: boolean;
  /** Failures, keyed by area so each shows beside the control that failed. */
  errors?: { explain?: string; fix?: string; signoff?: string };
  onRefresh: () => void;
  onExplain: () => void;
  onApprove: (fixId: string, note: string) => void;
  onDismiss: (fixId: string, note: string) => void;
  onSign: (role: string, note: string) => void;
  onExport: () => void;
  /** Download the signed pack (JSON); offered only once the report is signed. */
  onDownloadSigned?: () => void;
  /** Links to the existing evidence cards for this job. */
  onOpenGate8?: () => void;
  onOpenDetail?: () => void;
  /** The automatic re-read while a fix is applying stopped at its limit; say so. */
  pollStopped?: boolean;
}

function evidenceItems(report: ParityReport): Array<{ label: string; value: string; tone: ParityTone; detail?: string }> {
  const e = report.evidence;
  return [
    {
      label: "Reconciliation",
      value: e.reconciliation_passed == null ? "Not captured" : e.reconciliation_passed ? "Passed" : "Did not pass",
      tone: e.reconciliation_passed == null ? "muted" : e.reconciliation_passed ? "ok" : "warn",
    },
    // Toned by value (review U1): a captured "none" assurance or a score of 40 is not green.
    { label: "Assurance", value: e.assurance_level ? e.assurance_level.replace(/_/g, " ") : "Not captured", tone: assuranceTone(e.assurance_level) },
    {
      label: "Trust score",
      value: e.trust_score == null ? "Not captured" : String(Math.round(e.trust_score)),
      tone: trustScoreTone(e.trust_score),
    },
    { label: "Protection proof", value: e.protection_proof || "None for this run", tone: proofTone(e.protection_proof) },
    { label: "Transform proof", value: e.transform_proof || "None for this run", tone: proofTone(e.transform_proof) },
    { label: "Re-read now", ...rereadLabel(e.reread) },
  ];
}

export function TrustReportView({
  report,
  busy,
  canFix,
  fixDeniedReason,
  canSign,
  signDeniedReason,
  canExplain = canSign,
  errors = {},
  onRefresh,
  onExplain,
  onApprove,
  onDismiss,
  onSign,
  onExport,
  onDownloadSigned,
  onOpenGate8,
  onOpenDetail,
  pollStopped = false,
}: TrustReportViewProps) {
  const verdict = verdictPresentation(report);
  const { parity, confidence } = report;
  const groups = differencesByTable(report);
  const anyBusy = busy != null;
  const openFixes = report.fixes.filter((f) => f.status === "proposed");
  const attemptsUsed = fixAttemptsUsed(report);
  const doneFixes = report.fixes.filter((f) => f.status !== "proposed");
  const ringTone = report.verdict === "unexplained" ? "warn" : report.verdict === "unverifiable" ? "idle" : "ok";
  const signoffAnchor = `parity-signoff-${report.job_id}`;
  const tablesTitleId = useId();

  return (
    <div className="df2-parity" aria-busy={anyBusy || undefined}>
      <header className="df2-parity-toolbar">
        <div className="df2-parity-toolbar-verdict">
          <span className={badgeClass(verdict.tone)}>{verdict.label}</span>
          <span className="df2-parity-muted">
            Checked {formatDateTime(report.generated_at)} · version {report.version}
          </span>
        </div>
        <div className="df2-parity-actions">
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
          <Button
            size="sm"
            onClick={onExport}
            disabled={anyBusy}
            loading={busy?.kind === "export"}
            loadingLabel="Exporting…"
            leadingIcon={<DtIcon name="download" size={14} />}
          >
            Export pack
          </Button>
          {/* Not an anchor: the app routes on location.hash, so a #fragment would navigate away. */}
          <Button
            size="sm"
            variant="primary"
            onClick={() => {
              const target = typeof document !== "undefined" ? document.getElementById(signoffAnchor) : null;
              target?.scrollIntoView?.({ behavior: "smooth", block: "start" });
              target?.focus({ preventScroll: true });
            }}
          >
            Go to sign-off
          </Button>
        </div>
      </header>

      {report.verdict === "unverifiable" && report.verdict_reason && (
        <p className="df2-parity-notice is-warn" role="status">
          <DtIcon name="alert" size={14} />
          <span>{report.verdict_reason}</span>
        </p>
      )}

      <div className="df2-parity-hero">
        <div className="df2-parity-card df2-parity-ring-card">
          <ParityRing
            pct={parity.proven_equal_pct}
            tone={ringTone}
            lowerBound={parityIsLowerBound(report)}
            checked={parityRowsChecked(report)}
          />
          <span className={badgeClass(verdict.tone)}>{verdict.label}</span>
        </div>
        <div className="df2-parity-card">
          <dl className="df2-parity-kpis">
            <div className="df2-parity-kpi">
              <dt>rows in source</dt>
              <dd>{formatCount(parity.rows_source)}</dd>
            </div>
            <div className="df2-parity-kpi">
              <dt>rows in destination</dt>
              <dd>{formatCount(parity.rows_destination)}</dd>
            </div>
            <div className="df2-parity-kpi">
              <dt>rows differ</dt>
              <dd>{formatCount(parity.rows_differ)}</dd>
            </div>
            <div className="df2-parity-kpi">
              <dt>explained</dt>
              <dd>
                {parity.explained.toLocaleString("en-US")} / {parity.rows_differ.toLocaleString("en-US")}
              </dd>
            </div>
          </dl>
          <p className="df2-parity-confidence">
            <b>How sure is this?</b> {confidence.statement}
          </p>
          {report.verdict !== "unverifiable" && report.verdict_reason && (
            <p className="df2-parity-muted">{report.verdict_reason}</p>
          )}
        </div>
      </div>

      <section className="df2-parity-card" aria-labelledby={tablesTitleId}>
        <h3 id={tablesTitleId} className="df2-parity-h3">Tables</h3>
        {report.tables.length ? (
          <TableParityBars tables={report.tables} />
        ) : (
          <p className="df2-parity-muted">No tables were checked in this run.</p>
        )}
      </section>

      <div className="df2-parity-grid2">
        <section className="df2-parity-card" aria-label="Differences">
          {groups.length === 0 ? (
            <>
              <h3 className="df2-parity-h3">Differences</h3>
              <p className="df2-parity-muted">{noDifferencesText(report)}</p>
            </>
          ) : (
            groups.map((group) => (
              <div key={group.table} className="df2-parity-diff-group">
                <h3 className="df2-parity-h3">
                  Differences in <span className="df2-mono">{group.table}</span>
                </h3>
                <ul className="df2-parity-diffs">
                  {group.differences.map((diff) => (
                    <DifferenceRow key={diff.id} diff={diff} />
                  ))}
                </ul>
              </div>
            ))
          )}
          {parity.rows_differ > 0 && <DifferenceSplitBar explained={parity.explained} unexplained={parity.unexplained} />}
        </section>

        <div className="df2-parity-stack">
          <section className="df2-parity-ai" aria-label="Explanation">
            <header className="df2-parity-ai-head">
              <span className="df2-parity-who">Explanation · grounded in run evidence</span>
              {report.explanation && (
                <span
                  className="df2-parity-source"
                  title={
                    report.explanation.source === "llm"
                      ? "Written by the configured AI model from the classifier output and catalog metadata."
                      : "Written from templates. No AI model was used."
                  }
                >
                  source: {report.explanation.source}
                </span>
              )}
            </header>
            {report.explanation ? (
              <>
                {report.explanation.text
                  .split(/\n{2,}/)
                  .filter((p) => p.trim())
                  .map((p, i) => (
                    <p key={i} className="df2-parity-ai-text">{p}</p>
                  ))}
                <span className="df2-parity-muted">Written {formatDateTime(report.explanation.generated_at)}</span>
                {explanationIsStale(report) && (
                  <p className="df2-parity-notice is-warn" role="status">
                    <DtIcon name="alert" size={14} />
                    <span>Written before the latest fix. The figures above are current; explain again to update these words.</span>
                  </p>
                )}
              </>
            ) : (
              <p className="df2-parity-ai-text">
                No explanation yet. Ask for one to get the likely cause of each difference in plain language.
              </p>
            )}
            {canExplain && (
              <div className="df2-parity-actions">
                <Button
                  size="sm"
                  onClick={onExplain}
                  disabled={anyBusy}
                  loading={busy?.kind === "explain"}
                  loadingLabel="Explaining…"
                  leadingIcon={<DtIcon name="sparkle" size={14} />}
                >
                  {report.explanation ? "Explain again" : "Explain"}
                </Button>
              </div>
            )}
            {errors.explain && (
              <p className="df2-parity-notice is-danger" role="alert">
                <DtIcon name="alert" size={14} />
                <span>{errors.explain}</span>
              </p>
            )}
          </section>

          {report.fixes.length > 0 && (
            <section className="df2-parity-card" aria-label="Proposed fixes">
              <header className="df2-parity-section-head">
                <h3 className="df2-parity-h3">Proposed fixes</h3>
                <span
                  className="df2-parity-muted df2-parity-num"
                  title="Every widen, re-sync or re-run that was applied or failed counts. After the limit, write fixes are switched off."
                >
                  {Math.min(attemptsUsed, PARITY_MAX_ROUNDS)} of {PARITY_MAX_ROUNDS} write fixes used
                </span>
              </header>
              {!canFix && openFixes.length > 0 && (
                <p className="df2-parity-muted">{fixDeniedReason || "You don't have permission to approve fixes."}</p>
              )}
              {[...openFixes, ...doneFixes].map((fix) => (
                <FixCard
                  key={fix.id}
                  fix={fix}
                  busy={busy}
                  canAct={canFix}
                  onApprove={onApprove}
                  onDismiss={onDismiss}
                />
              ))}
              {pollStopped && report.fixes.some((f) => f.status === "applying") && (
                <p className="df2-parity-notice" role="status">
                  <DtIcon name="clock" size={14} />
                  <span>
                    Stopped checking automatically: the fix is still being applied. Refresh the report to see whether
                    it has finished.
                  </span>
                </p>
              )}
              {errors.fix && (
                <p className="df2-parity-notice is-danger" role="alert">
                  <DtIcon name="alert" size={14} />
                  <span>{errors.fix}</span>
                </p>
              )}
            </section>
          )}
        </div>
      </div>

      {report.rounds.length > 0 && (
        <section className="df2-parity-card" aria-label="Fix rounds">
          <h3 className="df2-parity-h3">Fix rounds</h3>
          <ol className="df2-parity-rounds">
            {report.rounds.map((round) => {
              const fix = report.fixes.find((f) => f.id === round.fix_id);
              return (
                <li key={round.n} className="df2-parity-round">
                  <span className="df2-parity-round-n">Round {round.n}</span>
                  <span className="df2-parity-muted" title={formatDateTime(round.at)}>{formatClock(round.at)}</span>
                  <span>{fix?.title ?? "Fix applied"}</span>
                  <span className="df2-parity-num">
                    Unexplained {round.unexplained_before.toLocaleString("en-US")} → {round.unexplained_after.toLocaleString("en-US")}
                  </span>
                </li>
              );
            })}
          </ol>
        </section>
      )}

      <section className="df2-parity-card" aria-label="Evidence behind this report">
        <header className="df2-parity-section-head">
          <h3 className="df2-parity-h3">Evidence behind this report</h3>
          <div className="df2-parity-actions">
            {onOpenGate8 && (
              <Button size="sm" variant="ghost" onClick={onOpenGate8} leadingIcon={<DtIcon name="shield" size={14} />}>
                Open Gate-8 proof
              </Button>
            )}
            {onOpenDetail && (
              <Button size="sm" variant="ghost" onClick={onOpenDetail} leadingIcon={<DtIcon name="activity" size={14} />}>
                Open trust score and ledger
              </Button>
            )}
          </div>
        </header>
        <dl className="df2-parity-evidence">
          {evidenceItems(report).map((item) => (
            <div key={item.label}>
              <dt>{item.label}</dt>
              <dd>
                <span className={badgeClass(item.tone)} title={item.detail || undefined}>{item.value}</span>
              </dd>
            </div>
          ))}
        </dl>
      </section>

      <SignoffPanel
        report={report}
        busy={busy}
        canSign={canSign}
        signDeniedReason={signDeniedReason}
        onSign={onSign}
        onExport={onExport}
        onDownloadSigned={onDownloadSigned}
        error={errors.signoff}
      />
    </div>
  );
}

/* ── The tab: loading, errors, actions ───────────────────────── */

export interface TrustReportTabProps {
  jobId: string;
  /** The job's status; a run still in flight has no report yet. */
  jobStatus?: string;
  onOpenGate8?: () => void;
  onOpenDetail?: () => void;
}

// Statuses of a run that has not finished (job_status UNFINISHED_JOB_STATUSES): no report yet.
const IN_FLIGHT = new Set(["running", "pending", "queued", "retrying"]);

export function TrustReportTab({ jobId, jobStatus, onOpenGate8, onOpenDetail }: TrustReportTabProps) {
  const { toast } = useToast();
  // Deciding a fix is a repair decision (the same as Repair proposals' decide): job.manage.
  const fixGate = useWriteGate(PERMISSIONS.jobManage, "approve or dismiss a fix");
  // Explaining stores the explanation on the job (audited), so it is job.run (review finding 11).
  const explainGate = useWriteGate(PERMISSIONS.jobRun, "ask for an explanation of a trust report");
  // Signing attests the report: job.manage, and the API binds each sign-off role
  // to your role in the job's workspace (review finding 5).
  const signGate = useWriteGate(PERMISSIONS.jobManage, "sign off a trust report");
  const [report, setReport] = useState<ParityReport | null>(null);
  const [loadError, setLoadError] = useState("");
  const [loading, setLoading] = useState(false);
  const [busy, setBusy] = useState<ParityBusy>(null);
  const [errors, setErrors] = useState<{ explain?: string; fix?: string; signoff?: string }>({});
  // Responses for a job the person has moved away from are dropped.
  const jobRef = useRef(jobId);
  jobRef.current = jobId;
  const reportRef = useRef<ParityReport | null>(null);
  reportRef.current = report;
  const busyRef = useRef(false);
  const inFlight = IN_FLIGHT.has(String(jobStatus || "").toLowerCase());

  const accept = useCallback((forJob: string, next: ParityReport) => {
    if (jobRef.current !== forJob) return;
    setReport((current) => newerReport(current, next));
  }, []);

  const load = useCallback(
    async (signal?: AbortSignal) => {
      const forJob = jobId;
      try {
        const next = await fetchParityReport(forJob, { signal });
        accept(forJob, next);
        if (jobRef.current === forJob) setLoadError("");
      } catch (err) {
        if (signal?.aborted || jobRef.current !== forJob) return;
        setLoadError(parityErrorMessage(err, "Could not load the trust report."));
      }
    },
    [jobId, accept],
  );

  useEffect(() => {
    setReport(null);
    setErrors({});
    setLoadError("");
    setBusy(null);
    busyRef.current = false;
    if (inFlight || !jobId) return;
    const controller = new AbortController();
    setLoading(true);
    void load(controller.signal).finally(() => {
      if (!controller.signal.aborted) setLoading(false);
    });
    return () => controller.abort();
  }, [jobId, inFlight, load]);

  // A fix being applied (a re-sync job) settles on the server: re-read until it has, bounded.
  const applying = report?.fixes.some((f) => f.status === "applying") ?? false;
  const [pollStopped, setPollStopped] = useState(false);
  const [pollEpoch, setPollEpoch] = useState(0);
  useEffect(() => {
    setPollStopped(false);
    if (!applying) return;
    let reads = 0;
    const timer = setInterval(() => {
      if (busyRef.current) return;
      reads += 1;
      if (!shouldPollApplying(reportRef.current, reads)) {
        clearInterval(timer);
        // Still applying after the last automatic read: tell the person, never stop silently.
        if (reportRef.current?.fixes.some((f) => f.status === "applying")) setPollStopped(true);
        return;
      }
      void load();
    }, PARITY_APPLYING_POLL_MS);
    return () => clearInterval(timer);
  }, [applying, load, pollEpoch]);

  /**
   * Run one action at a time. Every control is disabled while it runs; the
   * response replaces the report only if it is newer, and a response that is
   * not newer (or a conflict) triggers a fresh read so the page shows the truth.
   */
  const run = useCallback(
    async (
      next: Exclude<ParityBusy, null>,
      area: "explain" | "fix" | "signoff" | null,
      call: () => Promise<ParityReport | void>,
      success?:
        | { title: string; message?: string }
        | ((result: ParityReport | void) => { title: string; message?: string; tone: "success" | "error" | "info" }),
    ) => {
      if (busyRef.current) return;
      busyRef.current = true;
      const forJob = jobId;
      const before = reportRef.current?.version ?? -1;
      setBusy(next);
      if (area) setErrors((e) => ({ ...e, [area]: undefined }));
      try {
        const result = await call();
        if (result) {
          accept(forJob, result);
          if (result.version <= before) await load();
        }
        if (success && jobRef.current === forJob) {
          toast(typeof success === "function" ? success(result) : { ...success, tone: "success" });
        }
      } catch (err) {
        if (jobRef.current !== forJob) return;
        const fallback =
          area === "explain"
            ? "Could not write an explanation."
            : area === "fix"
              ? "Could not complete this fix."
              : area === "signoff"
                ? "Could not record your sign-off."
                : next.kind === "export"
                  ? "Could not export the evidence pack."
                  : next.kind === "pack"
                    ? "Could not download the signed pack."
                  : "Could not refresh the trust report.";
        const message = parityErrorMessage(err, fallback);
        if (area) setErrors((e) => ({ ...e, [area]: message }));
        else toast({ title: fallback, message, tone: "error" });
        const status = (err as { status?: number })?.status;
        if (status === 409) await load();
      } finally {
        busyRef.current = false;
        if (jobRef.current === forJob) setBusy(null);
      }
    },
    [jobId, accept, load, toast],
  );

  if (!jobId) return null;

  if (inFlight) {
    return (
      <EmptyState
        compact
        icon="shield"
        title="The trust report appears when this run finishes"
        description="Datawrap compares source and destination after the load, then shows the verdict here."
      />
    );
  }

  if (!report && (loading || !loadError)) {
    return <LoadingBlock title="Building the trust report" hint="Comparing counts, checksums and sampled values for this run." size="sm" />;
  }

  if (!report) {
    return (
      <EmptyState
        compact
        icon="alert"
        role="alert"
        title="Could not load the trust report"
        description={loadError || "The trust report is not available. Try again."}
        action={
          <Button
            size="sm"
            onClick={() => {
              setLoadError("");
              setLoading(true);
              void load().finally(() => setLoading(false));
            }}
          >
            Try again
          </Button>
        }
      />
    );
  }

  return (
    <TrustReportView
      report={report}
      busy={busy}
      canFix={fixGate.allowed}
      fixDeniedReason={fixGate.reason}
      canSign={signGate.allowed}
      signDeniedReason={signGate.reason}
      canExplain={explainGate.allowed}
      errors={errors}
      pollStopped={pollStopped}
      onRefresh={() => {
        if (pollStopped) setPollEpoch((n) => n + 1); // a manual refresh restarts the bounded re-reads
        void run({ kind: "refresh" }, null, () => fetchParityReport(jobId));
      }}
      onExplain={() =>
        void run({ kind: "explain" }, "explain", () => explainParityReport(jobId), { title: "Explanation updated" })
      }
      onApprove={(fixId, note) =>
        void run(
          { kind: "approve", id: fixId },
          "fix",
          () =>
            approveParityFix(jobId, fixId, note, report.version, report.fixes.find((f) => f.id === fixId)?.preview_sha256),
          (result) =>
          // The toast follows what came back for the fix: a 200 can carry a failed fix.
          approveOutcome(result || null, fixId),
        )
      }
      onDismiss={(fixId, note) =>
        void run({ kind: "dismiss", id: fixId }, "fix", () => dismissParityFix(jobId, fixId, note, report.version), { title: "Fix dismissed" })
      }
      onSign={(role, note) =>
        void run({ kind: "sign" }, "signoff", () => signParityReport(jobId, role, note, report.version), {
          title: `Signed as ${roleLabel(role)}`,
        })
      }
      onExport={() =>
        void run({ kind: "export" }, null, async () => {
          const blob = await downloadParityReportMarkdown(jobId);
          downloadBlob(`datawrap-trust-report-${jobId}.md`, blob);
        })
      }
      onDownloadSigned={() =>
        void run({ kind: "pack" }, null, async () => {
          const blob = await downloadParitySignedPack(jobId);
          downloadBlob(`datawrap-parity-signed-pack-${jobId}.json`, blob);
        })
      }
      onOpenGate8={onOpenGate8}
      onOpenDetail={onOpenDetail}
    />
  );
}
