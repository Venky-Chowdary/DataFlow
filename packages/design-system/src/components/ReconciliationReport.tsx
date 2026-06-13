export interface ReconciliationData {
  passed: boolean;
  source_rows: number;
  target_rows: number;
  source_checksum: string;
  target_checksum: string;
  message: string;
}

interface ReconciliationReportProps {
  report: ReconciliationData;
  tableName?: string;
}

/** Gate 8 signed audit report — plan component spec */
export function ReconciliationReport({ report, tableName }: ReconciliationReportProps) {
  return (
    <div className={["df-recon", report.passed ? "df-recon--pass" : "df-recon--fail"].join(" ")}>
      <div className="df-recon-header">
        <span className="df-recon-title">Reconciliation</span>
        <span className={["df-recon-badge", report.passed ? "df-recon-badge--pass" : "df-recon-badge--fail"].join(" ")}>
          {report.passed ? "Verified" : "Failed"}
        </span>
      </div>
      <p className="df-recon-message">{report.message}</p>
      <dl className="df-recon-grid">
        <div className="df-recon-item">
          <dt>Source rows</dt>
          <dd className="df-mono">{report.source_rows.toLocaleString()}</dd>
        </div>
        <div className="df-recon-item">
          <dt>Target rows</dt>
          <dd className="df-mono">{report.target_rows.toLocaleString()}</dd>
        </div>
        <div className="df-recon-item">
          <dt>Source checksum</dt>
          <dd className="df-mono">{report.source_checksum || "—"}</dd>
        </div>
        <div className="df-recon-item">
          <dt>Target checksum</dt>
          <dd className="df-mono">{report.target_checksum || "—"}</dd>
        </div>
        {tableName && (
          <div className="df-recon-item df-recon-item--wide">
            <dt>Target table</dt>
            <dd className="df-mono">{tableName}</dd>
          </div>
        )}
      </dl>
    </div>
  );
}
