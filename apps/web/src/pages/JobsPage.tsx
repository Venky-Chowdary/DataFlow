import { DtIcon } from "../components/DtIcon";
import { TransferJob } from "../lib/types";

export function JobsPage({ jobs }: { jobs: TransferJob[] }) {
  return (
    <div className="dt-content">
      <div className="dt-page-header">
        <h1 className="dt-page-title">Transfer Jobs</h1>
        <p className="dt-page-subtitle">Complete audit trail of every data movement across your organization.</p>
      </div>
      <div className="dt-card">
        {jobs.length === 0 ? (
          <div className="dt-empty">
            <div className="dt-empty-icon"><DtIcon name="jobs" size={28} /></div>
            <h3 className="dt-empty-title">No transfer jobs yet</h3>
            <p className="dt-empty-text">Completed transfers will appear here with status, record counts, and timestamps.</p>
          </div>
        ) : (
          <div className="dt-table-wrap">
            <table className="dt-table">
              <thead>
                <tr>
                  <th>Source</th>
                  <th>Destination</th>
                  <th>Status</th>
                  <th>Records</th>
                  <th>Date</th>
                </tr>
              </thead>
              <tbody>
                {jobs.map((job) => (
                  <tr key={job._id}>
                    <td>
                      <div className="dt-font-semibold">{job.source_name}</div>
                      <div className="dt-text-sm dt-text-muted">{job.source_type}</div>
                    </td>
                    <td>
                      <div className="dt-font-medium">{job.destination_collection}</div>
                      <div className="dt-text-sm dt-text-muted dt-mono">{job.destination_database}</div>
                    </td>
                    <td>
                      <span className={`dt-badge ${
                        job.status === "completed" ? "dt-badge-success" :
                        job.status === "failed" ? "dt-badge-error" : "dt-badge-warning"
                      }`}>
                        {job.status}
                      </span>
                    </td>
                    <td>{job.records_processed?.toLocaleString() ?? "—"}</td>
                    <td className="dt-text-sm dt-text-muted">{new Date(job.created_at).toLocaleString()}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
}
