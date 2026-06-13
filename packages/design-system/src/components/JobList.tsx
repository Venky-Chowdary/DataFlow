export interface JobListItem {
  job_id: string;
  status: string;
  operation: string;
  source: string;
  destination: string;
  rows_processed: number;
  created_at: string;
}

interface JobListProps {
  jobs: JobListItem[];
  emptyMessage?: string;
  selectedId?: string | null;
  onSelect?: (jobId: string) => void;
}

const STATUS_CLASS: Record<string, string> = {
  completed: "df-job-status--completed",
  running: "df-job-status--running",
  queued: "df-job-status--queued",
  failed: "df-job-status--failed",
  blocked: "df-job-status--failed",
};

function formatDate(iso: string): string {
  try {
    return new Date(iso).toLocaleString(undefined, {
      month: "short",
      day: "numeric",
      hour: "2-digit",
      minute: "2-digit",
    });
  } catch {
    return iso;
  }
}

export function JobList({ jobs, emptyMessage = "No jobs yet", selectedId, onSelect }: JobListProps) {
  if (jobs.length === 0) {
    return <div className="df-empty-state">{emptyMessage}</div>;
  }

  return (
    <div className="df-table-panel">
      <div className="df-table-scroll">
        <table className="df-data-table">
          <thead>
            <tr>
              <th>Job</th>
              <th>Operation</th>
              <th>Source → Destination</th>
              <th>Rows</th>
              <th>Status</th>
              <th>Created</th>
            </tr>
          </thead>
          <tbody>
            {jobs.map((job) => (
              <tr
                key={job.job_id}
                className={selectedId === job.job_id ? "df-data-table-row--selected" : ""}
                onClick={() => onSelect?.(job.job_id)}
                style={onSelect ? { cursor: "pointer" } : undefined}
              >
                <td className="df-mono">{job.job_id}</td>
                <td>{job.operation}</td>
                <td className="df-job-route">
                  <span>{job.source}</span>
                  <span className="df-operation-arrow">→</span>
                  <span>{job.destination}</span>
                </td>
                <td className="df-mono">{job.rows_processed.toLocaleString()}</td>
                <td>
                  <span className={["df-job-status", STATUS_CLASS[job.status] ?? ""].join(" ")}>
                    {job.status}
                  </span>
                </td>
                <td className="df-text-muted">{formatDate(job.created_at)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
