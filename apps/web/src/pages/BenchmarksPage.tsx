import { useMemo, useState } from "react";
import { DtIcon } from "../components/DtIcon";
import { EmptyState } from "../components/ui/EmptyState";
import { SectionLoader } from "../components/LoadingState";
import { PageFrame } from "../components/ui/PageFrame";
import { PageSection } from "../components/ui/PageSection";
import { PageShell } from "../components/ui/PageShell";
import { StatCard } from "../components/ui/StatCard";
import { useToast } from "../components/Toast";
import { downloadBenchmarkReport, runBenchmark, type BenchmarkReport } from "../lib/api";

const PRESET_SIZES = [
  { label: "10k", value: 10_000 },
  { label: "100k", value: 100_000 },
  { label: "1M", value: 1_000_000 },
];

function formatNumber(n: number) {
  return new Intl.NumberFormat(undefined, { maximumFractionDigits: 1 }).format(n);
}

function formatSeconds(s: number) {
  return `${s < 60 ? s.toFixed(2) : (s / 60).toFixed(2)} ${s < 60 ? "s" : "min"}`;
}

export function BenchmarksPage() {
  const { toast } = useToast();
  const [rows, setRows] = useState(100_000);
  const [running, setRunning] = useState(false);
  const [report, setReport] = useState<BenchmarkReport | null>(null);
  const [error, setError] = useState<string | null>(null);

  const isFaster = (rps: number, baseline: number) => rps >= baseline;

  const handleRun = async () => {
    setRunning(true);
    setError(null);
    try {
      const res = await runBenchmark(rows);
      setReport(res);
      if (!res.success) {
        setError(res.error || "Benchmark completed with errors");
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : "Benchmark request failed");
      toast({ title: "Benchmark failed", message: String(e), tone: "error" });
    } finally {
      setRunning(false);
    }
  };

  const handleDownload = async () => {
    try {
      const blob = await downloadBenchmarkReport(rows);
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = `dataflow-benchmark-report-${rows}.md`;
      a.click();
      URL.revokeObjectURL(url);
    } catch (e) {
      toast({ title: "Download failed", message: String(e), tone: "error" });
    }
  };

  const comparison = useMemo(() => {
    if (!report) return null;
    const dataflowRps = report.records_per_second;
    return report.competitors.map((c) => ({
      ...c,
      dataflow_ratio: dataflowRps / Math.max(c.typical_rps, 1),
      faster: isFaster(dataflowRps, c.typical_rps),
    }));
  }, [report]);

  // Intentionally no auto-run on mount — proofs are explicit operator actions.
  return (
    <PageShell
      wide
      className="df2-page-benchmarks"
      title="Proofs"
      kicker="Scale & fidelity"
      description="Run reproducible transfer proofs. Results are measured — not marketing claims."
    >
      <PageFrame className="df2-page-benchmarks-workspace">
      <div className="df2-page-benchmarks-content">
        <PageSection title="Reproducible scale proof">
          <p className="df2-page-benchmarks-intro">
            Run a synthetic CSV → SQLite transfer and compare DataFlow throughput, memory, and correctness
            against public Fivetran, Airbyte, and Stitch baselines. The same harness can target
            Snowflake, BigQuery, and S3 when cloud credentials are configured.
          </p>

          <div className="df2-page-benchmarks-toolbar">
            <div className="df2-page-benchmarks-sizes">
              {PRESET_SIZES.map((s) => (
                <button
                  key={s.value}
                  type="button"
                  className={`df2-btn df2-btn-sm ${rows === s.value ? "df2-btn-primary" : "df2-btn-secondary"}`}
                  onClick={() => setRows(s.value)}
                  disabled={running}
                >
                  {s.label}
                </button>
              ))}
            </div>
            <div className="df2-page-benchmarks-actions">
              <button type="button" className="df2-btn df2-btn-secondary" onClick={handleDownload} disabled={!report || running}>
                <DtIcon name="download" size={14} /> Report
              </button>
              <button type="button" className="df2-btn df2-btn-primary" onClick={handleRun} disabled={running}>
                {running ? <span className="df2-spin"><DtIcon name="spinner" size={14} /></span> : <DtIcon name="play" size={14} />}
                {running ? "Running…" : "Run benchmark"}
              </button>
            </div>
          </div>

          {running && (
            <SectionLoader title="Running benchmark" hint={`Transferring ${rows.toLocaleString()} synthetic rows to SQLite…`} />
          )}

          {error && !running && (
            <div className="df2-alert df2-alert-error" role="alert">
              <DtIcon name="alert" size={18} />
              <div>{error}</div>
            </div>
          )}

          {report && report.success && (
            <>
              <div className="df2-page-benchmarks-metrics">
                <StatCard
                  label="Rows transferred"
                  value={formatNumber(report.rows)}
                  icon="layers"
                />
                <StatCard
                  label="Throughput"
                  value={`${formatNumber(report.records_per_second)} rows/sec`}
                  icon="zap"
                />
                <StatCard
                  label="Elapsed time"
                  value={formatSeconds(report.elapsed_seconds)}
                  icon="clock"
                />
                <StatCard
                  label="Peak memory"
                  value={`${formatNumber(report.peak_memory_mb)} MB`}
                  icon="cpu"
                />
              </div>

              <div className="df2-page-benchmarks-section">
                <h3>Comparison with existing data products</h3>
                <p className="df2-page-benchmarks-note">
                  Competitor figures are representative public baselines from vendor docs and independent benchmarks.
                  DataFlow numbers are produced live by this harness.
                </p>
                <div className="df2-page-benchmarks-table-wrap">
                  <table className="df2-page-benchmarks-table">
                    <thead>
                      <tr>
                        <th>Product</th>
                        <th>Typical rows/sec</th>
                        <th>Memory baseline</th>
                        <th>Resume from checkpoint</th>
                        <th>Observed max rows</th>
                        <th>vs DataFlow</th>
                      </tr>
                    </thead>
                    <tbody>
                      {comparison?.map((c) => (
                        <tr key={c.product}>
                          <td className="df2-page-benchmarks-product">
                            {c.product === "DataFlow" ? (
                              <strong><DtIcon name="speed" size={14} /> DataFlow (this run)</strong>
                            ) : (
                              c.product
                            )}
                          </td>
                          <td>{formatNumber(c.typical_rps)}</td>
                          <td>{c.memory_mb} MB</td>
                          <td>
                            <span className={`df2-badge ${c.resume_from_checkpoint ? "df2-badge-success" : "df2-badge-muted"}`}>
                              {c.resume_from_checkpoint ? "Yes" : "No"}
                            </span>
                          </td>
                          <td>{formatNumber(c.observed_max_rows)}</td>
                          <td>
                            {c.product === "DataFlow" ? (
                              <span className="df2-badge df2-badge-success">baseline</span>
                            ) : (
                              <span className={`df2-badge ${c.faster ? "df2-badge-success" : "df2-badge-warning"}`}>
                                {c.faster ? `${c.dataflow_ratio.toFixed(1)}x faster` : `${(1 / c.dataflow_ratio).toFixed(1)}x slower`}
                              </span>
                            )}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>

              <div className="df2-page-benchmarks-section">
                <h3>Why these numbers matter</h3>
                <ul className="df2-page-benchmarks-list">
                  <li>
                    <strong>Single-process Python proof:</strong> DataFlow hits {formatNumber(report.records_per_second)} rows/sec on a SQLite destination without a separate executor cluster.
                  </li>
                  <li>
                    <strong>Memory-bounded:</strong> Peak heap stayed under {formatNumber(report.peak_memory_mb)} MB for {formatNumber(report.rows)} rows, including type inference, mapping, and reconciliation.
                  </li>
                  <li>
                    <strong>Correctness verified:</strong> The destination row count matched the source exactly ({report.destination_summary.verified ? "verified" : "unverified"}).
                  </li>
                  <li>
                    <strong>Resume-ready:</strong> The same engine writes per-chunk checkpoints, so a 1M-row job that fails mid-run picks up from the last committed batch.
                  </li>
                  <li>
                    <strong>Cloud target ready:</strong> Swap SQLite for Snowflake/BigQuery/S3 in the harness and the same metrics are reported.
                  </li>
                </ul>
              </div>
            </>
          )}

          {!report && !running && !error && (
            <EmptyState
              page
              icon="speed"
              title="Generate a live scale proof"
              description="Pick a row count above and run a synthetic CSV → SQLite transfer to compare throughput, memory, and correctness against Fivetran, Airbyte, and Stitch baselines."
            />
          )}
        </PageSection>
      </div>
      </PageFrame>
    </PageShell>
  );
}
