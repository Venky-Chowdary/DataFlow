import { DtIcon } from "../DtIcon";
import type { LoadHistoryReport } from "../../lib/types";

interface Props {
  report: LoadHistoryReport | null | undefined;
  /** Heading prefix for remediation cockpit numbering */
  title?: string;
  className?: string;
}

/**
 * Surfaces last-N load intelligence: null-rate / volume drift and quarantine
 * patterns that are new versus prior runs of the same route.
 */
export function LoadHistoryPanel({
  report,
  title = "Compared to prior loads",
  className = "",
}: Props) {
  if (!report) return null;
  const prior = report.prior_load_count ?? 0;
  const anomalies = report.anomalies ?? [];
  const novel = report.novel_quarantine_patterns ?? [];
  const warning = report.warning;
  const hasSignal = anomalies.length > 0 || novel.length > 0 || Boolean(warning);

  if (prior === 0 && !hasSignal) {
    return (
      <section className={`df2-load-history ${className}`.trim()} aria-label={title}>
        <header className="df2-load-history-head">
          <DtIcon name="activity" size={15} />
          <strong>{title}</strong>
          <span className="df2-load-history-badge">No prior loads</span>
        </header>
        <p className="df2-load-history-empty">
          This is the first profiled load for this source→destination route.
          Later runs compare against this baseline (null rates, volume, quarantine patterns).
        </p>
      </section>
    );
  }

  return (
    <section
      className={`df2-load-history ${hasSignal ? "has-findings" : ""} ${className}`.trim()}
      aria-label={title}
    >
      <header className="df2-load-history-head">
        <DtIcon name={hasSignal ? "alert" : "check"} size={15} />
        <strong>{title}</strong>
        <span className="df2-load-history-badge">
          {prior} prior load{prior === 1 ? "" : "s"}
          {report.compare_last_k ? ` · last ${Math.min(prior, report.compare_last_k)}` : ""}
        </span>
      </header>

      {warning ? <p className="df2-load-history-warn">{warning}</p> : null}

      {report.volume_note ? (
        <p className="df2-load-history-volume">{report.volume_note}</p>
      ) : null}

      {anomalies.length > 0 ? (
        <ul className="df2-load-history-list">
          {anomalies.slice(0, 8).map((msg) => (
            <li key={msg}>{msg}</li>
          ))}
        </ul>
      ) : (
        <p className="df2-load-history-ok">
          No material drift versus prior loads on this route.
        </p>
      )}

      {(report.column_findings?.length ?? 0) > 0 ? (
        <div className="df2-load-history-novel">
          <strong>Column signals vs prior loads</strong>
          <ul>
            {report.column_findings!.slice(0, 8).map((f) => (
              <li key={f.column}>
                <code>{f.column}</code>
                {(f.signals ?? []).slice(0, 2).map((s) => (
                  <span key={`${f.column}-${s.kind || s.message}`}> — {s.message || s.kind}</span>
                ))}
              </li>
            ))}
          </ul>
        </div>
      ) : null}

      {novel.length > 0 ? (
        <div className="df2-load-history-novel">
          <strong>New bad-data patterns vs prior loads</strong>
          <ul>
            {novel.slice(0, 6).map((n) => (
              <li key={`${n.column}-${n.reason}-${n.count}`}>
                <code>{n.column}</code>
                {n.reason ? ` — ${n.reason}` : ""}
                {typeof n.count === "number" ? ` (${n.count})` : ""}
              </li>
            ))}
          </ul>
        </div>
      ) : null}

      {(report.prior_runs_summary?.length ?? 0) > 0 ? (
        <details className="df2-load-history-runs">
          <summary>Prior load snapshots</summary>
          <ol>
            {[...(report.prior_runs_summary ?? [])].reverse().map((r, i) => (
              <li key={`${r.job_id || r.captured_at || i}`}>
                <span>{r.captured_at ? new Date(r.captured_at).toLocaleString() : "—"}</span>
                <span>{(r.row_count ?? 0).toLocaleString()} rows</span>
                <span>{(r.rejected_rows ?? 0).toLocaleString()} rejected</span>
                {r.job_id ? <span className="mono">{r.job_id.slice(0, 8)}</span> : null}
              </li>
            ))}
          </ol>
        </details>
      ) : null}
    </section>
  );
}
