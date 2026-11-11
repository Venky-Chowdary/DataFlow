/**
 * Page B: one exit rehearsal. A real, timed extract with proof: the timeline of
 * stages, completeness per table (and how each value check was measured), the
 * measured duration against the window, the sealed parts and their Merkle
 * root, sign-off (Parity Studio's panel) and the Exit Evidence Pack.
 *
 * The verdict is "proven" only when every declared table is proven; the API
 * decides it. Sampled or count-only value checks are listed as what they are,
 * a projection is labelled as one, and the anchor reads "Not anchored" unless a
 * write-once store confirmed it.
 */

import { Fragment, useId, useRef, useState, type ChangeEvent } from "react";
import { DtIcon } from "../DtIcon";
import { Button } from "../ui/Button";
import { CopyIdChip } from "../ui/CopyIdChip";
import { HiddenFileInput } from "../ui/HiddenFileInput";
import { DurationTrendChart, WindowBar } from "../charts/ExitCharts";
import { SignoffPanel, type ParityBusy } from "../parity/SignoffPanel";
import { formatDateTime } from "../../lib/parity";
import {
  anchorPresentation,
  coverageLines,
  fitsText,
  formatBytes,
  formatDuration,
  formatWindow,
  growthText,
  pillClass,
  projectionText,
  rehearsalSignoff,
  rehearsalSignoffBlockedReason,
  rehearsalVerdictPresentation,
  shortHash,
  stageLabel,
  stageStatusPresentation,
  tableStatusPresentation,
  valueMethodPresentation,
  windowRatio,
  type ExitRehearsal,
  type ExitVerifyResult,
  type RehearsalTable,
} from "../../lib/exitReadiness";

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

function TableDetail({ table, onOpenJob }: { table: RehearsalTable; onOpenJob?: (jobId: string) => void }) {
  return (
    <div className="df2-exit-table-detail">
      <p className="df2-exit-table-reason">{table.reason}</p>
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
                  <td className="is-num df2-mono">{t.source?.sum ?? "—"}</td>
                  <td className="is-num df2-mono">{t.extract?.sum ?? "—"}</td>
                  <td className="is-num df2-mono">{t.source ? `${t.source.min} / ${t.source.max}` : "—"}</td>
                  <td><MatchText match={t.match} what={`The ${t.column} total`} /></td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
      <div className="df2-parity-actions">
        {table.job_id && onOpenJob && (
          <Button size="sm" variant="ghost" onClick={() => onOpenJob(table.job_id!)} leadingIcon={<DtIcon name="jobs" size={14} />}>
            Open extract job
          </Button>
        )}
      </div>
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
}: RehearsalViewProps) {
  const verdict = rehearsalVerdictPresentation(rehearsal.verdict);
  const { completeness, duration, seal, evidence, trend } = rehearsal;
  const ratio = windowRatio(duration.measured_seconds, duration.window_seconds);
  const coverage = coverageLines(completeness.value_methods);
  const anchor = anchorPresentation(evidence.anchor);
  const signed = rehearsal.signoff.status === "signed";
  const running = rehearsal.verdict === "running";
  const anyBusy = busy != null;
  const fileId = useId();
  const tablesId = useId();
  const signoffAnchor = `exit-signoff-${rehearsal.id}`;
  const fileRef = useRef<HTMLInputElement>(null);
  const [open, setOpen] = useState<Set<string>>(
    () => new Set(rehearsal.tables.filter((t) => t.status === "incomplete" || t.status === "failed").slice(0, 3).map((t) => t.name)),
  );
  const toggle = (name: string) =>
    setOpen((prev) => {
      const next = new Set(prev);
      if (next.has(name)) next.delete(name);
      else next.add(name);
      return next;
    });
  const reasonTone = rehearsal.verdict === "failed" || rehearsal.verdict === "incomplete" ? " is-danger" : "";

  const pickFile = (e: ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    e.target.value = "";
    if (file) onVerifyFile(file);
  };

  return (
    <div className="df2-parity df2-exit-run" aria-busy={anyBusy || running || undefined}>
      <nav className="df2-exit-crumbs" aria-label="Breadcrumb">
        <button type="button" className="df2-exit-crumb-link" onClick={onBack}>
          <DtIcon name="arrow-left" size={14} /> Exit readiness
        </button>
        <span aria-hidden>/</span>
        <span aria-current="page" className="df2-exit-crumb-current">
          {rehearsal.system_name} · rehearsal {rehearsal.n}
        </span>
      </nav>

      <header className="df2-parity-toolbar">
        <div className="df2-parity-toolbar-verdict">
          <span className={pillClass(verdict.tone)}>{verdict.label}</span>
          <span className="df2-parity-muted">
            Started {formatDateTime(rehearsal.started_at)}
            {rehearsal.trigger === "schedule" ? " on schedule" : rehearsal.actor ? ` by ${rehearsal.actor}` : ""}
            {rehearsal.finished_at ? ` · finished ${formatDateTime(rehearsal.finished_at)}` : ""}
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
            onClick={onExportMarkdown}
            disabled={anyBusy}
            loading={busy?.kind === "export"}
            loadingLabel="Exporting…"
            leadingIcon={<DtIcon name="download" size={14} />}
          >
            Export evidence
          </Button>
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

      {rehearsal.verdict_reason && rehearsal.verdict !== "proven" && (
        <p className={`df2-parity-notice${reasonTone}`} role="status">
          <DtIcon name={running ? "clock" : "alert"} size={14} />
          <span>{rehearsal.verdict_reason}</span>
        </p>
      )}
      {pollStopped && running && (
        <p className="df2-parity-notice" role="status">
          <DtIcon name="clock" size={14} />
          <span>Stopped checking automatically: the rehearsal is still running. Refresh to see whether it has finished.</span>
        </p>
      )}

      <section className="df2-parity-card" aria-label="Timeline">
        <h3 className="df2-parity-h3">Timeline</h3>
        <ol className="df2-exit-timeline">
          {rehearsal.timeline.map((step) => {
            const st = stageStatusPresentation(step.status);
            return (
              <li key={step.stage} className={`df2-exit-step is-${step.status}`}>
                <span className="df2-exit-step-mark" aria-hidden>
                  <DtIcon name={st.icon} size={14} />
                </span>
                <span className="df2-exit-step-body">
                  <b>{stageLabel(step.stage)}</b>
                  <span className="df2-parity-muted">
                    {st.label}
                    {step.seconds != null ? ` · ${formatDuration(step.seconds)}` : ""}
                  </span>
                </span>
              </li>
            );
          })}
        </ol>
      </section>

      <div className="df2-parity-grid2">
        <section className="df2-parity-card" aria-label="Completeness">
          <h3 className="df2-parity-h3">Completeness</h3>
          <p className="df2-exit-headline">
            <strong>
              {completeness.tables_extracted.toLocaleString("en-US")} of {completeness.tables_declared.toLocaleString("en-US")}
            </strong>{" "}
            declared tables extracted
          </p>
          <ul className="df2-exit-chips" aria-label="Tables by result">
            <li><span className={pillClass(completeness.tables_proven ? "ok" : "muted")}>{completeness.tables_proven.toLocaleString("en-US")} proven</span></li>
            {completeness.tables_not_proven > 0 && <li><span className={pillClass("warn")}>{completeness.tables_not_proven.toLocaleString("en-US")} not proven</span></li>}
            {completeness.tables_incomplete > 0 && <li><span className={pillClass("danger")}>{completeness.tables_incomplete.toLocaleString("en-US")} incomplete</span></li>}
            {completeness.tables_failed > 0 && <li><span className={pillClass("danger")}>{completeness.tables_failed.toLocaleString("en-US")} failed</span></li>}
          </ul>
          <p className="df2-parity-confidence">{completeness.statement}</p>
          <div className="df2-exit-coverage">
            <h4 className="df2-parity-h4">How values were checked</h4>
            {coverage.length === 0 ? (
              <p className="df2-parity-muted">{running ? "Not measured yet: the rehearsal is still running." : "No value check ran."}</p>
            ) : (
              <ul>
                {coverage.map((line) => (
                  <li key={line.method}>
                    <span className="df2-exit-coverage-count">{line.count.toLocaleString("en-US")}</span>
                    <span>
                      <b>{line.label}</b> <span className="df2-parity-muted">{line.note}</span>
                    </span>
                    {!line.proves && <span className={pillClass("warn")}>Not proof</span>}
                  </li>
                ))}
              </ul>
            )}
          </div>
          <p className="df2-exit-seal-line">
            {seal.merkle_root ? (
              <>
                Files sealed: Merkle root <code className="df2-mono" title={seal.merkle_root}>{shortHash(seal.merkle_root)}</code> over{" "}
                {seal.parts_count.toLocaleString("en-US")} {seal.parts_count === 1 ? "part" : "parts"}
              </>
            ) : (
              <span className="df2-parity-muted">Not sealed yet: the Merkle root is computed when every part is verified.</span>
            )}
          </p>
        </section>

        <section className="df2-parity-card" aria-label="Against the window">
          <h3 className="df2-parity-h3">Against the window</h3>
          <dl className="df2-parity-kpis df2-exit-window-kpis">
            <div className="df2-parity-kpi">
              <dt>measured</dt>
              <dd>{duration.measured_seconds == null ? "—" : formatDuration(duration.measured_seconds)}</dd>
            </div>
            <div className="df2-parity-kpi">
              <dt>window</dt>
              <dd>{formatWindow(duration.window_seconds)}</dd>
            </div>
            <div className="df2-parity-kpi">
              <dt>{duration.fits_window === false ? "over by" : "margin"}</dt>
              <dd>
                {duration.margin_pct == null
                  ? "—"
                  : duration.fits_window === false
                    ? formatDuration((duration.measured_seconds ?? 0) - duration.window_seconds)
                    : `${Math.floor(duration.margin_pct)}%`}
              </dd>
            </div>
          </dl>
          <div className="df2-exit-window-row">
            <WindowBar measured={duration.measured_seconds} window={duration.window_seconds} fits={duration.fits_window} size="lg" />
            <span className={`df2-exit-fit is-${duration.fits_window == null ? "none" : duration.fits_window ? "ok" : "bad"}`}>
              {fitsText(duration.fits_window)}
              {ratio ? ` · ${ratio.label}` : ""}
            </span>
          </div>
          <p className="df2-parity-confidence">{duration.statement}</p>
          <p className="df2-parity-muted">
            {duration.measured_seconds == null
              ? "Extract and verify are timed from the job clocks once they finish; nothing is estimated."
              : `Extract ${formatDuration(duration.extract_seconds)} · verify ${formatDuration(duration.verify_seconds)} · ${formatBytes(seal.total_bytes)} sealed. Measured from job clocks, not estimated.`}
          </p>
          {growthText(trend.growth_pct_rows) && <p className="df2-parity-muted">{growthText(trend.growth_pct_rows)}.</p>}
          <DurationTrendChart trend={trend} windowSeconds={duration.window_seconds} />
          {trend.projection && (
            <p className="df2-parity-notice is-muted" role="note">
              <DtIcon name="trend" size={14} />
              <span>{projectionText(trend.projection)}</span>
            </p>
          )}
        </section>
      </div>

      <section className="df2-parity-card" aria-labelledby={tablesId}>
        <h3 id={tablesId} className="df2-parity-h3">Tables</h3>
        <div className="df2-parity-scroll">
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
                const expanded = open.has(table.name);
                const detailId = `${tablesId}-${table.name.replace(/[^A-Za-z0-9_-]/g, "_")}`;
                return (
                  <Fragment key={`${table.schema ?? ""}.${table.name}`}>
                    <tr className={expanded ? "is-expanded" : undefined}>
                      <td><span className="df2-mono df2-exit-table-name">{table.schema ? `${table.schema}.` : ""}{table.name}</span></td>
                      <td><span className={pillClass(st.tone)} title={table.reason}>{st.label}</span></td>
                      <td className="is-num">{rowsText(table)}</td>
                      <td>{table.key_columns.length || table.key_set.match != null ? <MatchText match={table.key_set.match} what="The key set" /> : <span className="df2-parity-muted">No key</span>}</td>
                      <td><MatchText match={table.row_set.match} what="The row set" /></td>
                      <td>
                        <span className={`df2-exit-totals-text${totals.match === false ? " is-bad" : ""}`}>{totals.text}</span>
                      </td>
                      <td>
                        <span className="df2-exit-method">
                          {method.label}
                          {!method.proves && table.value_parity.method !== "none" ? <span className="df2-parity-muted"> (not proof)</span> : null}
                        </span>
                      </td>
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

      <div className="df2-parity-grid2">
        <section className="df2-parity-card" aria-label="Sealed extract">
          <h3 className="df2-parity-h3">Sealed extract</h3>
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
                  "Not sealed yet"
                )}
              </dd>
            </div>
            <div>
              <dt>Algorithm</dt>
              <dd className="df2-mono">{seal.algorithm}</dd>
            </div>
            <div>
              <dt>Parts</dt>
              <dd>{seal.parts_count.toLocaleString("en-US")} · {formatBytes(seal.total_bytes)}</dd>
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
                      : "Not stored yet"}
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

        <section className="df2-parity-card" aria-label="Evidence pack and anchor">
          <h3 className="df2-parity-h3">Evidence pack</h3>
          <dl className="df2-exit-facts">
            <div>
              <dt>Signed</dt>
              <dd>{signed ? `Yes, ${formatDateTime(rehearsal.signoff.signed_at)}` : "Not yet: every required role signs a proven rehearsal"}</dd>
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
            <HiddenFileInput id={fileId} ref={fileRef} accept="application/json,.json" disabled={anyBusy} onChange={pickFile} />
            <Button size="sm" variant="ghost" disabled={anyBusy} onClick={() => fileRef.current?.click()} leadingIcon={<DtIcon name="upload" size={14} />}>
              Verify a pack file…
            </Button>
          </div>
          {verifyResult && <VerifyResultCard result={verifyResult} />}
          {errors.verify && (
            <p className="df2-parity-notice is-danger" role="alert">
              <DtIcon name="alert" size={14} />
              <span>{errors.verify}</span>
            </p>
          )}
          <p className="df2-parity-muted">
            The pack is evidence of this rehearsal: what was extracted, how it was checked and who signed. It does not
            certify compliance with any regulation; your auditors decide what it satisfies.
          </p>
        </section>
      </div>

      <SignoffPanel
        subject={{
          anchorId: signoffAnchor,
          signoff: rehearsalSignoff(rehearsal),
          blockedReason: rehearsalSignoffBlockedReason(rehearsal),
        }}
        copy={{
          intro:
            "Each approval is written to the audit log with your role in this system's workspace, which must hold the "
            + "authority the role needs. Sign-off opens only for a proven rehearsal. When every required role has signed, "
            + "the Exit Evidence Pack is signed with the workspace proof key and anchored.",
          packItems: [
            "The verdict and completeness of every declared table, with how each value check was measured",
            "The measured duration against the window",
            "The SHA-256 of every extracted part and the Merkle root over them",
            "Approvals with names, roles and times",
          ],
          exportLabel: "Export report (Markdown)",
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
    </div>
  );
}
