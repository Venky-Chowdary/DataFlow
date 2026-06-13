import { Connector, TransferJob } from "../lib/types";
import { DtIcon } from "../components/DtIcon";

interface DashboardPageProps {
  connectors: Connector[];
  jobs: TransferJob[];
  onNewTransfer: () => void;
}

export function DashboardPage({ connectors, jobs, onNewTransfer }: DashboardPageProps) {
  const completed = jobs.filter((j) => j.status === "completed");
  const totalRecords = completed.reduce((sum, j) => sum + (j.records_processed || 0), 0);

  const stats = [
    { label: "Active Connectors", value: connectors.length, icon: "connectors", hint: "Saved connections" },
    { label: "Completed Transfers", value: completed.length, icon: "transfer", hint: "All time" },
    { label: "Records Processed", value: totalRecords.toLocaleString(), icon: "activity", hint: "Universal data moved" },
    { label: "AI Accuracy", value: "99.2%", icon: "zap", hint: "Semantic mapping engine" },
  ];

  return (
    <div className="dt-content">
      <div className="dt-hero-banner">
        <h2 className="dt-hero-title">Universal Data Freedom</h2>
        <p className="dt-hero-subtitle">
          Move any data from anywhere to anywhere — AI understands your columns, detects PII,
          and validates before transfer.
        </p>
      </div>

      <div className="dt-page-header">
        <div className="dt-page-header-row">
          <div>
            <h1 className="dt-page-title">Dashboard</h1>
            <p className="dt-page-subtitle">Real-time view of your data platform health and activity.</p>
          </div>
          <button type="button" className="dt-btn dt-btn-primary dt-btn-lg" onClick={onNewTransfer}>
            <DtIcon name="plus" size={18} />
            New Transfer
          </button>
        </div>
      </div>

      <div className="dt-stats">
        {stats.map((stat) => (
          <div key={stat.label} className="dt-stat">
            <div className="dt-stat-icon"><DtIcon name={stat.icon} size={20} /></div>
            <div className="dt-stat-label">{stat.label}</div>
            <div className="dt-stat-value">{stat.value}</div>
            <div className="dt-stat-trend up">{stat.hint}</div>
          </div>
        ))}
      </div>

      <div className="dt-card dt-mt-8">
        <div className="dt-card-header">
          <h3 className="dt-card-title">Recent Transfers</h3>
        </div>
        {jobs.length === 0 ? (
          <div className="dt-empty">
            <div className="dt-empty-icon"><DtIcon name="transfer" size={28} /></div>
            <h3 className="dt-empty-title">No transfers yet</h3>
            <p className="dt-empty-text">Upload CSV, JSON, or JSONL — AI analyzes your data and moves it to any destination.</p>
            <button type="button" className="dt-btn dt-btn-primary" onClick={onNewTransfer}>Start First Transfer</button>
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
                {jobs.slice(0, 8).map((job) => (
                  <tr key={job._id}>
                    <td>
                      <div className="dt-font-semibold">{job.source_name}</div>
                      <div className="dt-text-sm dt-text-muted">{job.source_type}</div>
                    </td>
                    <td className="dt-mono">{job.destination_database}.{job.destination_collection}</td>
                    <td>
                      <span className={`dt-badge ${job.status === "completed" ? "dt-badge-success" : job.status === "failed" ? "dt-badge-error" : "dt-badge-warning"}`}>
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
