import { useState } from "react";
import { DtIcon } from "../DtIcon";
import { useToast } from "../Toast";
import { exportJobQuarantine, fetchJobQuarantine } from "../../lib/api";

export interface QuarantinePanelProps {
  jobId: string;
  rejectedRows?: number;
  initiallyOpen?: boolean;
}

export function QuarantinePanel({ jobId, rejectedRows, initiallyOpen = false }: QuarantinePanelProps) {
  const { toast } = useToast();
  const [open, setOpen] = useState(initiallyOpen);
  const [loading, setLoading] = useState(false);
  const [rows, setRows] = useState<{ row?: number; column?: string; target?: string; value?: string; reason?: string; policy?: string }[]>([]);

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

  return (
    <>
      {!open && (
        <button
          type="button"
          className="df2-btn df2-btn-sm"
          onClick={() => void load()}
          disabled={loading}
        >
          <DtIcon name="warning" size={14} /> {loading ? "Loading…" : rejectedRows ? `View ${rejectedRows.toLocaleString()} quarantined rows` : "View quarantine"}
        </button>
      )}
      {open && (
        <section className="df2-job-log-panel is-result is-open" aria-label="Quarantine">
          <header className="df2-job-log-panel-head">
            <div className="df2-job-log-panel-title">
              <DtIcon name="warning" size={14} />
              <strong>Quarantine</strong>
              <span className="df2-job-log-count">{rows.length} rows</span>
            </div>
            <div className="df2-job-log-actions">
              <button type="button" className="df2-btn df2-btn-sm df2-btn-secondary" onClick={() => void download()}>
                <DtIcon name="download" size={14} /> Export CSV
              </button>
              <button type="button" className="df2-btn df2-btn-sm df2-btn-ghost" onClick={() => setOpen(false)}>Close</button>
            </div>
          </header>
          <div className="df2-job-log-panel-body" role="log">
            {rows.length === 0 ? (
              <div className="df2-job-log-empty">No quarantined rows recorded.</div>
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
    </>
  );
}
