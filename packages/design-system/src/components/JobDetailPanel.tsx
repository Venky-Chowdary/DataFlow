import { CheckpointTimeline, type CheckpointItem } from "./CheckpointTimeline";
import { ReconciliationReport, type ReconciliationData } from "./ReconciliationReport";

export interface JobDetailData {
  job_id: string;
  status: string;
  operation: string;
  source: string;
  destination: string;
  rows_processed: number;
  total_rows: number;
  current_chunk: number;
  total_chunks: number;
  table_name: string;
  driver: string;
  workflow_phase: string;
  message: string;
  created_at: string;
  checkpoints: CheckpointItem[];
  reconciliation: ReconciliationData | null;
}

interface JobDetailPanelProps {
  job: JobDetailData;
}

function formatDate(iso: string): string {
  try {
    return new Date(iso).toLocaleString();
  } catch {
    return iso;
  }
}

export function JobDetailPanel({ job }: JobDetailPanelProps) {
  return (
    <div className="df-job-detail">
      <div className="df-job-detail-header">
        <div>
          <div className="df-mono df-job-detail-id">{job.job_id}</div>
          <div className="df-file-meta">{formatDate(job.created_at)}</div>
        </div>
        <span className={["df-job-status", `df-job-status--${job.status === "completed" ? "completed" : job.status === "failed" ? "failed" : "running"}`].join(" ")}>
          {job.status}
        </span>
      </div>

      <dl className="df-recon-grid" style={{ marginBottom: 20 }}>
        <div className="df-recon-item">
          <dt>Operation</dt>
          <dd>{job.operation}</dd>
        </div>
        <div className="df-recon-item">
          <dt>Phase</dt>
          <dd className="df-mono">{job.workflow_phase}</dd>
        </div>
        <div className="df-recon-item df-recon-item--wide">
          <dt>Route</dt>
          <dd className="df-mono">
            {job.source} → {job.destination}
          </dd>
        </div>
        {job.table_name && (
          <div className="df-recon-item df-recon-item--wide">
            <dt>Target</dt>
            <dd className="df-mono">{job.table_name}</dd>
          </div>
        )}
        {job.driver && (
          <div className="df-recon-item">
            <dt>Driver</dt>
            <dd className="df-mono">{job.driver}</dd>
          </div>
        )}
      </dl>

      <CheckpointTimeline
        checkpoints={job.checkpoints}
        currentChunk={job.current_chunk}
        totalChunks={job.total_chunks}
        status={job.status}
      />

      {job.reconciliation && (
        <div style={{ marginTop: 24 }}>
          <ReconciliationReport report={job.reconciliation} tableName={job.table_name || undefined} />
        </div>
      )}

      {job.message && <p className="df-file-meta" style={{ marginTop: 16 }}>{job.message}</p>}
    </div>
  );
}
