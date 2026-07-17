import { useState } from "react";
import { DtIcon } from "../DtIcon";
import { useToast } from "../Toast";
import { exportJobQuarantine, fetchJobQuarantine } from "../../lib/api";

export interface QuarantinePanelProps {
  jobId: string;
  rejectedRows?: number;
  initiallyOpen?: boolean;
}

type QuarantineRow = {
  row?: number;
  column?: string;
  target?: string;
  value?: string;
  reason?: string;
  policy?: string;
};

function summarizeReasons(rows: QuarantineRow[]) {
  const counts = new Map<string, number>();
  for (const r of rows) {
    const key = r.reason?.trim() || "Unknown validation failure";
    counts.set(key, (counts.get(key) ?? 0) + 1);
  }
  return [...counts.entries()].sort((a, b) => b[1] - a[1]).slice(0, 4);
}

export function QuarantinePanel({ jobId, rejectedRows, initiallyOpen = false }: QuarantinePanelProps) {
  const { toast } = useToast();
  const [open, setOpen] = useState(initiallyOpen);
  const [loading, setLoading] = useState(false);
  const [rows, setRows] = useState<QuarantineRow[]>([]);

  const load = async () => {
    setLoading(true);
    try {
      const data = await fetchJobQuarantine(jobId);
      setRows(data.quarantine);
      setOpen(true);
    } catch (e) {
      toast({ title: "Could not load quarantine", message: (e as Error).message, tone: "error" });
    } finally {
      setLoading(false);
    }
  };

  const download = async () => {
    try {
      const data = await exportJobQuarantine(jobId);
      if (data.download_url) {
        const a = document.createElement("a");
        a.href = data.download_url;
        a.download = data.filename || `quarantine-${jobId}.csv`;
        a.click();
      } else {
        toast({ title: "Export returned no file", tone: "warning" });
      }
    } catch (e) {
      toast({ title: "Could not export quarantine", message: (e as Error).message, tone: "error" });
    }
  };

  const topReasons = summarizeReasons(rows);

  return (
    <div className="df2-quarantine-panel">
      <div className="df2-quarantine-explainer">
        <DtIcon name="warning" size={18} />
        <div>
          <strong>What is quarantine?</strong>
          <p>
            Rows that fail validation during load are isolated — not silently dropped. Each row records the column,
            offending value, validation policy, and reason so you can fix source data or mapping rules and retry.
          </p>
          {rejectedRows != null && rejectedRows > 0 && (
            <span className="df2-quarantine-explainer-count">
              {rejectedRows.toLocaleString()} row{rejectedRows === 1 ? "" : "s"} quarantined for this job
            </span>
          )}
        </div>
      </div>

      {!open && (
        <button
          type="button"
          className="df2-btn df2-btn-sm"
          onClick={() => void load()}
          disabled={loading}
        >
          <DtIcon name="warning" size={14} /> {loading ? "Loading…" : rejectedRows ? `Inspect ${rejectedRows.toLocaleString()} quarantined rows` : "Inspect quarantine"}
        </button>
      )}

      {open && (
        <section className="df2-job-log-panel is-result is-open" aria-label="Quarantine">
          <header className="df2-job-log-panel-head">
            <div className="df2-job-log-panel-title">
              <DtIcon name="warning" size={14} />
              <strong>Quarantine detail</strong>
              <span className="df2-job-log-count">{rows.length} rows</span>
            </div>
            <div className="df2-job-log-actions">
              <button type="button" className="df2-btn df2-btn-sm df2-btn-secondary" onClick={() => void download()}>
                <DtIcon name="download" size={14} /> Export CSV
              </button>
              <button type="button" className="df2-btn df2-btn-sm df2-btn-ghost" onClick={() => setOpen(false)}>Close</button>
            </div>
          </header>

          {topReasons.length > 0 && (
            <div className="df2-quarantine-summary">
              {topReasons.map(([reason, count]) => (
                <span key={reason} className="df2-quarantine-summary-chip">
                  <strong>{count.toLocaleString()}</strong> {reason}
                </span>
              ))}
            </div>
          )}

          <div className="df2-job-log-panel-body" role="log">
            {rows.length === 0 ? (
              <div className="df2-job-log-empty">No quarantined rows recorded for this job.</div>
            ) : (
              <table className="df2-query-table">
                <thead>
                  <tr>
                    <th>Row</th>
                    <th>Column</th>
                    <th>Target</th>
                    <th>Value</th>
                    <th>Reason</th>
                    <th>Policy</th>
                  </tr>
                </thead>
                <tbody>
                  {rows.map((r, i) => (
                    <tr key={i}>
                      <td>{r.row ?? "—"}</td>
                      <td>{r.column || "—"}</td>
                      <td>{r.target || "—"}</td>
                      <td className="df2-quarantine-value" title={String(r.value ?? "")}>{String(r.value ?? "")}</td>
                      <td>{r.reason || "—"}</td>
                      <td>{r.policy || "—"}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
        </section>
      )}
    </div>
  );
}
