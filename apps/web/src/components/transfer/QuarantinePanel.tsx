import { useEffect, useMemo, useState } from "react";
import { DtIcon } from "../DtIcon";
import { useToast } from "../Toast";
import { downloadJobQuarantineCsv, fetchJobQuarantine, proposeRepairFromQuarantine, replayJobQuarantine, type RepairMapping, type RepairProposal } from "../../lib/api";
import { RepairProposalDrawer } from "./RepairProposalDrawer";

type QuarantineRow = {
  row?: number;
  column?: string;
  target?: string;
  value?: string;
  reason?: string;
  policy?: string;
  values?: Record<string, string>;
  chars?: string[];
  suggested_transform?: string;
  /** Destination DLQ row id — required to stamp `_df_promoted_at` after Promote. */
  _df_qid?: string;
};

export interface QuarantinePanelProps {
  jobId: string;
  rejectedRows?: number;
  /** Distinct rows where a value was coerced to NULL (kept, but fidelity lost). */
  coercedNullRows?: number;
  initiallyOpen?: boolean;
  /** When true, always attempt load (failed preflight jobs often have issue rows). */
  autoLoad?: boolean;
  /**
   * Inline details from the job payload (SSE / complete). Used immediately so the
   * user sees findings even before the quarantine API round-trip, and as a fallback
   * when the API returns empty.
   */
  initialDetails?: QuarantineRow[];
  /** Closed-loop: preflight findings → Validate (Strip / remapping). */
  onOpenValidate?: () => void;
  /** Closed-loop: after successful write-time replay. */
  onReplayComplete?: (childJobId: string) => void;
  /** Job / Studio mappings so Approve can apply transforms (not audit-only). */
  repairMappings?: RepairMapping[];
  /** After approve / apply / reject — parent can deep-link to Validate. */
  onRepairDecided?: (proposal: RepairProposal) => void;
}

function summarizeReasons(rows: QuarantineRow[]) {
  const counts = new Map<string, number>();
  for (const r of rows) {
    const key = r.reason?.trim() || "Unknown validation failure";
    counts.set(key, (counts.get(key) ?? 0) + 1);
  }
  return [...counts.entries()].sort((a, b) => b[1] - a[1]).slice(0, 6);
}

function summarizeColumns(rows: QuarantineRow[]) {
  const counts = new Map<string, number>();
  for (const r of rows) {
    const key = (r.column || r.target || "unknown").trim();
    counts.set(key, (counts.get(key) ?? 0) + 1);
  }
  return [...counts.entries()].sort((a, b) => b[1] - a[1]).slice(0, 6);
}

/** Collect suggested transforms into replay overrides (e.g. strip_controls). */
function buildTransformOverrides(rows: QuarantineRow[]): Record<string, string> {
  const out: Record<string, string> = {};
  for (const r of rows) {
    const col = (r.column || r.target || "").trim();
    const xf = (r.suggested_transform || "").trim();
    if (!col || !xf) continue;
    // Prefer strip / coerce suggestions; last write wins per column.
    out[col] = xf;
  }
  return out;
}

/** Make invisible format-control chars visible in the UI / CSV preview. */
function formatQuarantineSample(value: unknown, chars?: string[]): string {
  let text = String(value ?? "");
  const replacements: Array<[RegExp, string]> = [
    [/\u200B/g, "⟦U+200B⟧"],
    [/\u200C/g, "⟦U+200C⟧"],
    [/\u200D/g, "⟦U+200D⟧"],
    [/\uFEFF/g, "⟦U+FEFF⟧"],
    [/\u0000/g, "⟦U+0000⟧"],
    [/\uFFFD/g, "⟦U+FFFD⟧"],
  ];
  for (const [re, label] of replacements) {
    text = text.replace(re, label);
  }
  const alreadyMarked = /⟦U\+[0-9A-F]+⟧/i.test(text);
  if (!alreadyMarked && chars?.length) {
    text = `${text} (${chars.join(", ")})`;
  }
  return text || "—";
}

function triggerBlobDownload(blob: Blob, filename: string) {
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  a.rel = "noopener";
  document.body.appendChild(a);
  a.click();
  a.remove();
  window.setTimeout(() => URL.revokeObjectURL(url), 1500);
}

export function QuarantinePanel({
  jobId,
  rejectedRows,
  coercedNullRows,
  initiallyOpen = false,
  autoLoad = false,
  initialDetails,
  onOpenValidate,
  onReplayComplete,
  repairMappings = [],
  onRepairDecided,
}: QuarantinePanelProps) {
  const { toast } = useToast();
  const [open, setOpen] = useState(initiallyOpen || autoLoad || Boolean(initialDetails?.length));
  const [loading, setLoading] = useState(false);
  const [loaded, setLoaded] = useState(Boolean(initialDetails?.length));
  const [exporting, setExporting] = useState(false);
  const [replaying, setReplaying] = useState(false);
  const [rows, setRows] = useState<QuarantineRow[]>(() => initialDetails ?? []);
  const [repairOpen, setRepairOpen] = useState(false);
  const [repairProposal, setRepairProposal] = useState<RepairProposal | null>(null);
  const [repairBusy, setRepairBusy] = useState(false);
  const [issueCount, setIssueCount] = useState(initialDetails?.length ?? 0);
  const [rowCount, setRowCount] = useState(rejectedRows ?? initialDetails?.length ?? 0);
  const [source, setSource] = useState<string>(initialDetails?.length ? "job" : "none");
  const [editIndex, setEditIndex] = useState<number | null>(null);
  const [editValue, setEditValue] = useState("");
  const [applySuggested, setApplySuggested] = useState(true);
  const [replayResult, setReplayResult] = useState<{
    job_id: string;
    rows_written: number;
    rejected: number;
  } | null>(null);
  const [destDlq, setDestDlq] = useState<import("../../lib/api").QuarantineInfo["dest_dlq"]>(undefined);

  const transformOverrides = useMemo(() => buildTransformOverrides(rows), [rows]);
  const hasStripSuggestion = Object.values(transformOverrides).some(
    (v) => v === "strip_controls" || v.includes("strip"),
  );

  const load = async () => {
    setLoading(true);
    try {
      const data = await fetchJobQuarantine(jobId);
      const apiRows = data.quarantine || [];
      const next = apiRows.length ? apiRows : (initialDetails ?? []);
      setRows(next);
      setIssueCount(data.issue_count ?? next.length);
      setRowCount(data.rejected_rows ?? rejectedRows ?? next.length);
      setSource(apiRows.length ? (data.source || "write") : (initialDetails?.length ? "job" : data.source || "none"));
      setDestDlq(data.dest_dlq);
      setOpen(true);
      setLoaded(true);
    } catch (e) {
      if (initialDetails?.length) {
        setRows(initialDetails);
        setIssueCount(initialDetails.length);
        setSource("job");
        setOpen(true);
        setLoaded(true);
      }
      toast({ title: "Could not load quarantine", message: (e as Error).message, tone: "error" });
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    if (autoLoad || initiallyOpen) {
      void load();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps -- load once per job
  }, [jobId, autoLoad, initiallyOpen]);

  useEffect(() => {
    if (rejectedRows != null && rejectedRows > 0 && !loaded) {
      setRowCount(rejectedRows);
    }
  }, [rejectedRows, loaded]);

  const download = async () => {
    setExporting(true);
    try {
      const data = await downloadJobQuarantineCsv(jobId, rows.length ? rows : undefined);
      triggerBlobDownload(data.blob, data.filename);
      toast({
        title: "Quarantine CSV downloaded",
        message: `${data.row_count.toLocaleString()} finding(s) saved as ${data.filename}`,
        tone: "success",
      });
    } catch (e) {
      toast({ title: "Could not export quarantine", message: (e as Error).message, tone: "error" });
    } finally {
      setExporting(false);
    }
  };

  const openEdit = (index: number) => {
    setEditIndex(index);
    setEditValue(String(rows[index]?.value ?? ""));
  };

  const saveEdit = () => {
    if (editIndex == null) return;
    setRows((prev) =>
      prev.map((r, i) => {
        if (i !== editIndex) return r;
        const col = r.column || "";
        const values = { ...(r.values || {}) };
        if (col) values[col] = editValue;
        return { ...r, value: editValue, values };
      }),
    );
    setEditIndex(null);
  };

  const proposeRepair = async () => {
    if (!rows.length) {
      toast({ title: "No findings", message: "Load quarantine rows before proposing a repair.", tone: "warning" });
      return;
    }
    setRepairBusy(true);
    try {
      const proposal = await proposeRepairFromQuarantine({
        job_id: jobId,
        rejected_details: rows.map((r) => ({
          column: r.column || r.target || "",
          target: r.target || r.column || "",
          reason: r.reason || "",
          value: r.value,
          suggested_transform: r.suggested_transform,
        })),
      });
      setRepairProposal(proposal);
      setRepairOpen(true);
      toast({
        title: "Repair proposed",
        message: `${proposal.actions.length} action(s) — review and approve. Audit trail saved.`,
        tone: "success",
      });
    } catch (e) {
      toast({ title: "Propose failed", message: (e as Error).message, tone: "error" });
    } finally {
      setRepairBusy(false);
    }
  };

  const replay = async () => {
    setReplaying(true);
    setReplayResult(null);
    try {
      const overrides = applySuggested && Object.keys(transformOverrides).length
        ? transformOverrides
        : undefined;
      const result = await replayJobQuarantine(jobId, {
        rows,
        transform_overrides: overrides,
      });
      setReplayResult({
        job_id: result.job_id,
        rows_written: result.rows_written,
        rejected: result.rejected,
      });
      toast({
        title: result.rejected ? "Promote finished with rejects" : "Promote / Replay finished",
        message: [
          `Wrote ${result.rows_written.toLocaleString()} row(s)`,
          result.rejected ? `${result.rejected} still rejected` : null,
          destDlq?.table && !result.rejected
            ? `DLQ rows stamped on ${destDlq.table}`
            : null,
        ]
          .filter(Boolean)
          .join(" · "),
        tone: result.rejected ? "warning" : "success",
      });
      onReplayComplete?.(result.job_id);
      await load();
    } catch (e) {
      toast({ title: "Replay failed", message: (e as Error).message, tone: "error" });
    } finally {
      setReplaying(false);
    }
  };

  const topReasons = summarizeReasons(rows);
  const topColumns = summarizeColumns(rows);
  const displayRowCount = rowCount || (rejectedRows ?? 0) || issueCount;
  const displayFindings = loaded ? issueCount : (rejectedRows ?? 0) || issueCount;
  const isWriteSource = source === "write" || source === "job";
  const isPreflight = source === "preflight";
  const canReplay = isWriteSource && rows.length > 0;

  return (
    <div className="df2-quarantine-panel">
      <div className="df2-quarantine-explainer">
        <DtIcon name="warning" size={18} />
        <div>
          <strong>Quarantine / bad-data findings</strong>
          <p>
            Bad cells are never silently dropped. Each finding records row, column, sample value, reason,
            and policy so you can strip controls, fix mappings, or edit and replay.
          </p>
          <div className="df2-quarantine-explainer-metrics" aria-label="Quarantine counts">
            <span className="df2-quarantine-explainer-count">
              <strong>{displayRowCount.toLocaleString()}</strong> row{displayRowCount === 1 ? "" : "s"} quarantined
            </span>
            <span className="df2-quarantine-explainer-count">
              <strong>{displayFindings.toLocaleString()}</strong> finding{displayFindings === 1 ? "" : "s"}
            </span>
            {coercedNullRows != null && coercedNullRows > 0 && (
              <span className="df2-quarantine-explainer-count">
                <strong>{coercedNullRows.toLocaleString()}</strong> coerced to NULL
              </span>
            )}
            {source !== "none" && loaded && (
              <span className="df2-quarantine-explainer-count">
                Source: {isPreflight ? "preflight / integrity" : "write-time reject"}
              </span>
            )}
            {destDlq?.table && (
              <span className="df2-quarantine-explainer-count" title="Destination dead-letter table">
                Dest DLQ: <code>{destDlq.table}</code>
                {destDlq.rows_written != null ? ` · ${destDlq.rows_written} written` : ""}
                {destDlq.open_rows != null ? ` · ${destDlq.open_rows} open` : ""}
              </span>
            )}
            {destDlq?.skipped && destDlq.reason && (
              <span className="df2-quarantine-explainer-count" title={destDlq.reason}>
                Dest DLQ skipped (control-plane only)
              </span>
            )}
            {destDlq?.error && (
              <span className="df2-quarantine-explainer-count" title={String(destDlq.error)}>
                Dest DLQ write error
              </span>
            )}
          </div>
        </div>
      </div>

      {/* Closed-loop next action — one obvious primary path */}
      <div className="df2-quarantine-next" role="region" aria-label="Next remediation step">
        <div className="df2-quarantine-next-copy">
          <strong>Next step</strong>
          {isPreflight ? (
            <p>
              Caught in Validate before write. Open Validate → apply <em>Strip controls</em> or fix
              mappings, then re-run. Replay is for write-time rejects only.
            </p>
          ) : canReplay ? (
            <p>
              Write-time rejects
              {destDlq?.table ? (
                <>
                  {" "}are also on destination table <code>{destDlq.table}</code>
                </>
              ) : null}
              . Edit bad cells if needed
              {hasStripSuggestion ? ", apply suggested strip transforms," : ","} then{" "}
              <strong>Promote / Replay</strong> — good rows already on the destination stay put.
              Promoted DLQ rows are stamped <code>_df_promoted_at</code>.
            </p>
          ) : (
            <p>
              Inspect findings and Export CSV. When write-time rejects are available, Promote/Replay
              rewrites only the quarantined rows
              {destDlq?.table ? <> and marks them on <code>{destDlq.table}</code></> : null}.
            </p>
          )}
        </div>
        <div className="df2-quarantine-next-actions">
          {onOpenValidate && (
            <button type="button" className="df2-btn df2-btn-sm df2-btn-primary" onClick={onOpenValidate}>
              <DtIcon name="gate" size={14} /> Open Validate (Strip / Fix)
            </button>
          )}
          {rows.length > 0 && (
            <button
              type="button"
              className="df2-btn df2-btn-sm df2-btn-secondary"
              onClick={() => void proposeRepair()}
              disabled={repairBusy}
              title="Create a durable repair proposal from quarantine findings (human approve required)"
            >
              <DtIcon name="sparkle" size={14} />
              {repairBusy ? "Proposing…" : "Propose repair"}
            </button>
          )}
          {canReplay && (
            <button
              type="button"
              className="df2-btn df2-btn-sm df2-btn-primary"
              onClick={() => void replay()}
              disabled={replaying || rows.length === 0}
            >
              <DtIcon name="transfer" size={14} />{" "}
              {replaying
                ? "Promoting…"
                : destDlq?.table
                  ? "Promote / Replay rows"
                  : "Replay quarantined rows"}
            </button>
          )}
          <button
            type="button"
            className="df2-btn df2-btn-sm df2-btn-secondary"
            onClick={() => void download()}
            disabled={exporting || (!rows.length && !displayFindings)}
          >
            <DtIcon name="download" size={14} /> {exporting ? "Exporting…" : "Export CSV"}
          </button>
          {!open && (
            <button
              type="button"
              className="df2-btn df2-btn-sm df2-btn-ghost"
              onClick={() => void load()}
              disabled={loading}
            >
              <DtIcon name="warning" size={14} />{" "}
              {loading ? "Loading…" : "Inspect findings"}
            </button>
          )}
        </div>
        {canReplay && Object.keys(transformOverrides).length > 0 && (
          <label className="df2-quarantine-apply-suggested">
            <input
              type="checkbox"
              checked={applySuggested}
              onChange={(e) => setApplySuggested(e.target.checked)}
            />
            Apply suggested transforms on replay
            {" "}
            <code>{Object.entries(transformOverrides).map(([c, t]) => `${c}→${t}`).join(", ")}</code>
          </label>
        )}
      </div>

      {open && (
        <section className="df2-quarantine-inspect is-open" aria-label="Quarantine findings">
          <header className="df2-quarantine-inspect-head">
            <div className="df2-quarantine-inspect-title">
              <DtIcon name="warning" size={14} />
              <strong>Quarantined rows</strong>
              <span className="df2-quarantine-inspect-count">
                {displayFindings.toLocaleString()} finding{displayFindings === 1 ? "" : "s"}
                {displayRowCount > 0 ? ` · ${displayRowCount.toLocaleString()} rows` : ""}
              </span>
            </div>
            <div className="df2-job-log-actions">
              <button type="button" className="df2-btn df2-btn-sm df2-btn-ghost" onClick={() => void load()} disabled={loading}>
                Refresh
              </button>
              <button type="button" className="df2-btn df2-btn-sm df2-btn-ghost" onClick={() => setOpen(false)}>Close</button>
            </div>
          </header>

          {(topReasons.length > 0 || topColumns.length > 0) && (
            <div className="df2-quarantine-summary">
              {topColumns.map(([col, count]) => (
                <span key={`c-${col}`} className="df2-quarantine-summary-chip">
                  <strong>{count.toLocaleString()}</strong> in {col}
                </span>
              ))}
              {topReasons.map(([reason, count]) => (
                <span key={`r-${reason}`} className="df2-quarantine-summary-chip">
                  <strong>{count.toLocaleString()}</strong> {reason}
                </span>
              ))}
            </div>
          )}

          {replayResult && (
            <div className="df2-quarantine-replay-result" role="status">
              <DtIcon name="check" size={14} />
              <span>
                Replay job <code>{replayResult.job_id.slice(0, 8)}</code> wrote{" "}
                {replayResult.rows_written.toLocaleString()} row(s)
                {replayResult.rejected > 0 ? `, ${replayResult.rejected.toLocaleString()} still rejected` : ""}.
              </span>
            </div>
          )}

          <div className="df2-quarantine-inspect-body" role="region" aria-label="Quarantine table">
            {loading && !loaded ? (
              <div className="df2-quarantine-inspect-empty">Loading quarantined rows…</div>
            ) : rows.length === 0 ? (
              <div className="df2-quarantine-inspect-empty">
                <p>
                  {displayRowCount > 0
                    ? `${displayRowCount.toLocaleString()} row(s) were marked quarantined on this job, but row-level findings were not persisted on the control plane.`
                    : "No row-level findings were stored for this job yet."}
                </p>
                <p>
                  What to do next: open <strong>Validate</strong> for Strip controls / Quarantine / Fix bad data,
                  confirm the API build includes write-time quarantine persistence, then re-run the transfer.
                </p>
              </div>
            ) : (
              <table className="df2-query-table df2-quarantine-table">
                <thead>
                  <tr>
                    <th>Row</th>
                    <th>Column</th>
                    <th>Target</th>
                    <th>Value</th>
                    <th>Reason</th>
                    <th>Suggested fix</th>
                    <th>Policy</th>
                    <th />
                  </tr>
                </thead>
                <tbody>
                  {rows.map((r, i) => (
                    <tr key={`${r.row}-${r.column}-${i}`}>
                      <td>{r.row ?? "—"}</td>
                      <td>{r.column || "—"}</td>
                      <td>{r.target || "—"}</td>
                      <td
                        className="df2-quarantine-value"
                        title={String(r.value ?? "")}
                      >
                        {formatQuarantineSample(r.value, r.chars)}
                      </td>
                      <td>{r.reason || "—"}</td>
                      <td className="df2-quarantine-fix" title={r.suggested_transform || ""}>
                        {r.suggested_transform || "—"}
                      </td>
                      <td>{r.policy || "—"}</td>
                      <td>
                        {canReplay && (
                          <button type="button" className="df2-btn df2-btn-sm df2-btn-ghost" onClick={() => openEdit(i)}>
                            Edit
                          </button>
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
        </section>
      )}

      {editIndex != null && (
        <div className="df2-quarantine-edit-backdrop" role="presentation" onClick={() => setEditIndex(null)}>
          <div
            className="df2-quarantine-edit-dialog"
            role="dialog"
            aria-modal="true"
            aria-labelledby="df2-quarantine-edit-title"
            onClick={(e) => e.stopPropagation()}
          >
            <h3 id="df2-quarantine-edit-title">Edit quarantined value</h3>
            <p>
              Fix the value for{" "}
              <strong>{rows[editIndex]?.column || "column"}</strong>
              {rows[editIndex]?.row != null ? ` (source row ${rows[editIndex].row})` : ""} before replaying.
            </p>
            <label className="df2-label" htmlFor="df2-quarantine-edit-input">Value</label>
            <input
              id="df2-quarantine-edit-input"
              className="df2-input"
              value={editValue}
              onChange={(e) => setEditValue(e.target.value)}
              autoFocus
            />
            <div className="df2-quarantine-edit-actions">
              <button type="button" className="df2-btn df2-btn-ghost" onClick={() => setEditIndex(null)}>Cancel</button>
              <button type="button" className="df2-btn df2-btn-primary" onClick={saveEdit}>Save</button>
            </div>
          </div>
        </div>
      )}
      <RepairProposalDrawer
        open={repairOpen}
        proposal={repairProposal}
        mappings={repairMappings}
        onClose={() => setRepairOpen(false)}
        onApplied={(updated, p) => {
          toast({
            title: "Repair applied",
            message: `${updated.length} mapping(s) updated from proposal ${p.id}. Opening Validate so you can re-run gates.`,
            tone: "success",
          });
          onRepairDecided?.(p);
        }}
        onDecided={(p) => {
          if (p.status === "proposed") {
            toast({
              title: "Continue in Validate",
              message: "Proposal kept open — Approve & apply once Studio mappings are loaded.",
              tone: "info",
            });
          } else {
            toast({
              title: p.status === "rejected" ? "Repair rejected" : "Repair decided",
              message: `${p.id} · ${p.status}.`,
              tone: p.status === "rejected" ? "warning" : "success",
            });
          }
          onRepairDecided?.(p);
        }}
      />
    </div>
  );
}
