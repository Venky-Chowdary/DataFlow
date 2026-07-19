import { useEffect, useState } from "react";
import { DtIcon } from "../DtIcon";
import { useToast } from "../Toast";
import { downloadJobQuarantineCsv, fetchJobQuarantine, replayJobQuarantine } from "../../lib/api";

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
}: QuarantinePanelProps) {
  const { toast } = useToast();
  const [open, setOpen] = useState(initiallyOpen || autoLoad || Boolean(initialDetails?.length));
  const [loading, setLoading] = useState(false);
  const [loaded, setLoaded] = useState(Boolean(initialDetails?.length));
  const [exporting, setExporting] = useState(false);
  const [replaying, setReplaying] = useState(false);
  const [rows, setRows] = useState<QuarantineRow[]>(() => initialDetails ?? []);
  const [issueCount, setIssueCount] = useState(initialDetails?.length ?? 0);
  const [rowCount, setRowCount] = useState(rejectedRows ?? initialDetails?.length ?? 0);
  const [source, setSource] = useState<string>(initialDetails?.length ? "job" : "none");
  const [editIndex, setEditIndex] = useState<number | null>(null);
  const [editValue, setEditValue] = useState("");
  const [replayResult, setReplayResult] = useState<{
    job_id: string;
    rows_written: number;
    rejected: number;
  } | null>(null);

  const load = async () => {
    setLoading(true);
    try {
      const data = await fetchJobQuarantine(jobId);
      const apiRows = data.quarantine || [];
      // Prefer API rows when present; keep inline job details as fail-safe.
      const next = apiRows.length ? apiRows : (initialDetails ?? []);
      setRows(next);
      setIssueCount(data.issue_count ?? next.length);
      setRowCount(data.rejected_rows ?? rejectedRows ?? next.length);
      setSource(apiRows.length ? (data.source || "write") : (initialDetails?.length ? "job" : data.source || "none"));
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

  const replay = async () => {
    setReplaying(true);
    setReplayResult(null);
    try {
      const result = await replayJobQuarantine(jobId, { rows });
      setReplayResult({
        job_id: result.job_id,
        rows_written: result.rows_written,
        rejected: result.rejected,
      });
      toast({
        title: "Quarantine replay finished",
        message: `Wrote ${result.rows_written.toLocaleString()} row(s)${result.rejected ? `, ${result.rejected} still rejected` : ""}.`,
        tone: result.rejected ? "warning" : "success",
      });
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
  const canReplay = source === "write" && rows.length > 0;

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
                Source: {source === "preflight" ? "preflight / integrity" : "write-time reject"}
              </span>
            )}
          </div>
          {source === "preflight" && (
            <p className="df2-label-hint" style={{ margin: "6px 0 0" }}>
              These rows were caught in Validate before write — Replay applies only to write-time
              rejects. Fix mappings or use Strip controls / Quarantine on the Validate step, then re-run.
            </p>
          )}
        </div>
      </div>

      {!open && (
        <button
          type="button"
          className="df2-btn df2-btn-sm df2-btn-primary"
          onClick={() => void load()}
          disabled={loading}
        >
          <DtIcon name="warning" size={14} />{" "}
          {loading
            ? "Loading…"
            : displayRowCount > 0
              ? `Inspect ${displayRowCount.toLocaleString()} quarantined row${displayRowCount === 1 ? "" : "s"}`
              : "Inspect quarantine / findings"}
        </button>
      )}

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
              {canReplay && (
                <button
                  type="button"
                  className="df2-btn df2-btn-sm df2-btn-primary"
                  onClick={() => void replay()}
                  disabled={replaying || rows.length === 0}
                >
                  <DtIcon name="transfer" size={14} /> {replaying ? "Replaying…" : "Replay"}
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
                  Export CSV stays available once findings are saved.
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
                      <td className="df2-quarantine-value" title={String(r.value ?? "")}>
                        {String(r.value ?? "")}
                        {r.chars?.length ? ` (${r.chars.join(", ")})` : ""}
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
    </div>
  );
}
