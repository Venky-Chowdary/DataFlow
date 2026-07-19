import { useEffect, useMemo, useState } from "react";
import { DtIcon } from "../components/DtIcon";
import { EmptyState } from "../components/ui/EmptyState";
import { SectionLoader } from "../components/LoadingState";
import { PageFrame } from "../components/ui/PageFrame";
import { PageSection } from "../components/ui/PageSection";
import { PageShell } from "../components/ui/PageShell";
import { StatCard } from "../components/ui/StatCard";
import { FilterTabs } from "../components/ui/FilterTabs";
import { useToast } from "../components/Toast";
import {
  downloadBenchmarkReport,
  fetchProofLedger,
  runBenchmark,
  runFidelityProof,
  type BenchmarkReport,
  type FidelityProofResult,
  type ProofLedger,
} from "../lib/api";

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

type Tab = "integrity" | "scale";

export function BenchmarksPage() {
  const { toast } = useToast();
  const [tab, setTab] = useState<Tab>("integrity");
  const [rows, setRows] = useState(100_000);
  const [running, setRunning] = useState(false);
  const [report, setReport] = useState<BenchmarkReport | null>(null);
  const [error, setError] = useState<string | null>(null);

  const [ledger, setLedger] = useState<ProofLedger | null>(null);
  const [ledgerLoading, setLedgerLoading] = useState(true);
  const [ledgerError, setLedgerError] = useState<string | null>(null);
  const [fidelityRunning, setFidelityRunning] = useState(false);
  const [fidelity, setFidelity] = useState<FidelityProofResult | null>(null);

  const loadLedger = async () => {
    setLedgerLoading(true);
    setLedgerError(null);
    try {
      setLedger(await fetchProofLedger());
    } catch (e) {
      setLedgerError(e instanceof Error ? e.message : "Could not load proof ledger");
    } finally {
      setLedgerLoading(false);
    }
  };

  useEffect(() => {
    void loadLedger();
  }, []);

  const isFaster = (rps: number, baseline: number) => rps >= baseline;

  const handleFidelity = async () => {
    setFidelityRunning(true);
    try {
      const result = await runFidelityProof();
      setFidelity(result);
      await loadLedger();
      toast({
        title: result.success ? "Fidelity proof passed" : "Fidelity proof failed",
        message: result.success
          ? `CSV→SQLite rich types verified in ${result.elapsed_ms ?? "—"} ms.`
          : result.error || "One or more fidelity checks failed.",
        tone: result.success ? "success" : "error",
      });
    } catch (e) {
      toast({ title: "Fidelity proof failed", message: String(e), tone: "error" });
    } finally {
      setFidelityRunning(false);
    }
  };

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

  const metrics = ledger?.metrics;

  return (
    <PageShell
      wide
      className="df2-page-benchmarks"
      title="Proofs"
      kicker="Integrity first"
      description="Migration proofs customers can reproduce — not connection pings or inflated connector counts."
    >
      <PageFrame className="df2-page-benchmarks-workspace">
        <div className="df2-page-benchmarks-content">
          <FilterTabs
            items={[
              { id: "integrity", label: "Integrity ledger", count: metrics?.production_sku_routes },
              { id: "scale", label: "Scale throughput" },
            ]}
            value={tab}
            onChange={(id) => setTab(id)}
            ariaLabel="Proof tabs"
          />

          {tab === "integrity" && (
            <>
              <PageSection title="Why DataFlow beats Airbyte on integrity">
                <p className="df2-page-benchmarks-intro">
                  Connection tests prove a socket opened. These proofs prove rows, types, quarantine, and
                  checksums survive the full write path — the bar for “any schema → anywhere.”
                </p>

                {ledgerLoading && (
                  <SectionLoader title="Loading proof ledger" hint="Gathering SKU inventory, drivers, and on-disk artifacts…" />
                )}
                {ledgerError && !ledgerLoading && (
                  <div className="df2-alert df2-alert-error" role="alert">
                    <DtIcon name="alert" size={18} />
                    <div>{ledgerError}</div>
                  </div>
                )}

                {ledger && !ledgerLoading && (
                  <>
                    <div className="df2-page-benchmarks-metrics">
                      <StatCard
                        label="Unique transfer drivers"
                        value={formatNumber(metrics?.unique_transfer_drivers ?? 0)}
                        icon="connectors"
                        sub="Real engines — not catalog brand aliases"
                      />
                      <StatCard
                        label="Catalog aliases (live)"
                        value={formatNumber(metrics?.catalog_transfer_ready_aliases ?? 0)}
                        icon="layers"
                        sub="Honest alias count over those drivers"
                      />
                      <StatCard
                        label="PRODUCTION_SKU routes"
                        value={formatNumber(metrics?.production_sku_routes ?? 0)}
                        icon="gate"
                        sub="Routes committed in CI when emulators are up"
                      />
                      <StatCard
                        label="Fidelity proofs passed"
                        value={`${metrics?.fidelity_proofs_passed ?? 0}/${metrics?.fidelity_proofs_on_disk ?? 0}`}
                        icon="shield"
                        sub="On-disk rich-type proofs under data/proofs/"
                      />
                    </div>

                    <div className="df2-page-benchmarks-toolbar">
                      <p className="df2-page-benchmarks-note" style={{ margin: 0, flex: 1 }}>
                        Run the canonical fidelity fixture: unicode, nulls, decimals, bool forms, and JSON via CSV→SQLite with strict reconciliation.
                      </p>
                      <button
                        type="button"
                        className="df2-btn df2-btn-primary"
                        onClick={() => void handleFidelity()}
                        disabled={fidelityRunning}
                      >
                        {fidelityRunning
                          ? <span className="df2-spin"><DtIcon name="spinner" size={14} /></span>
                          : <DtIcon name="play" size={14} />}
                        {fidelityRunning ? "Proving…" : "Run fidelity proof"}
                      </button>
                    </div>

                    {fidelity && (
                      <div className={`df2-alert ${fidelity.success ? "df2-alert-success" : "df2-alert-error"}`} role="status">
                        <DtIcon name={fidelity.success ? "check" : "alert"} size={18} />
                        <div>
                          <strong>{fidelity.route}</strong> — {fidelity.success ? "passed" : "failed"}
                          {fidelity.elapsed_ms != null ? ` in ${fidelity.elapsed_ms} ms` : ""}
                          {fidelity.checks?.length ? ` · checks: ${fidelity.checks.join(", ")}` : ""}
                          {fidelity.proof_file ? ` · artifact ${fidelity.proof_file}` : ""}
                        </div>
                      </div>
                    )}

                    <div className="df2-page-benchmarks-section">
                      <h3>DataFlow vs Airbyte — integrity dimensions</h3>
                      <div className="df2-page-benchmarks-table-wrap">
                        <table className="df2-page-benchmarks-table">
                          <thead>
                            <tr>
                              <th>Dimension</th>
                              <th>DataFlow</th>
                              <th>Airbyte</th>
                              <th>Proof surface</th>
                            </tr>
                          </thead>
                          <tbody>
                            {ledger.vs_airbyte.map((row) => (
                              <tr key={row.dimension}>
                                <td><strong>{row.dimension}</strong></td>
                                <td>{row.dataflow}</td>
                                <td>{row.airbyte}</td>
                                <td><code>{row.proof}</code></td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    </div>

                    <div className="df2-page-benchmarks-section">
                      <h3>PRODUCTION_SKU — committed migration routes</h3>
                      <p className="df2-page-benchmarks-note">
                        These {ledger.production_sku.length} routes are the CI-committed set. Capability math
                        ({metrics?.live_route_combinations ?? "—"} combinations) is larger; SKU is what we prove.
                      </p>
                      <div className="df2-page-benchmarks-table-wrap">
                        <table className="df2-page-benchmarks-table">
                          <thead>
                            <tr>
                              <th>Route</th>
                              <th>Source</th>
                              <th>Destination</th>
                              <th>Status</th>
                            </tr>
                          </thead>
                          <tbody>
                            {ledger.production_sku.map((r) => (
                              <tr key={r.route}>
                                <td>{r.route}</td>
                                <td>{r.source_kind}/{r.source_format}</td>
                                <td>{r.dest_kind}/{r.dest_format}</td>
                                <td><span className="df2-badge df2-badge-success">{r.status}</span></td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    </div>

                    <div className="df2-page-benchmarks-section">
                      <h3>Recent proof artifacts</h3>
                      {ledger.recent_proofs.length === 0 ? (
                        <EmptyState
                          icon="shield"
                          title="No proofs on disk yet"
                          description="Run the fidelity proof above to write the first artifact under data/proofs/."
                          compact
                        />
                      ) : (
                        <div className="df2-page-benchmarks-table-wrap">
                          <table className="df2-page-benchmarks-table">
                            <thead>
                              <tr>
                                <th>When</th>
                                <th>Tier</th>
                                <th>Route</th>
                                <th>Rows</th>
                                <th>Result</th>
                                <th>Checks</th>
                              </tr>
                            </thead>
                            <tbody>
                              {ledger.recent_proofs.map((p) => (
                                <tr key={p.id}>
                                  <td>{new Date(p.mtime).toLocaleString()}</td>
                                  <td>{p.tier || "—"}</td>
                                  <td>{p.route || "—"}</td>
                                  <td>{p.rows != null ? formatNumber(p.rows) : "—"}</td>
                                  <td>
                                    <span className={`df2-badge ${p.success ? "df2-badge-success" : "df2-badge-warning"}`}>
                                      {p.success ? "pass" : "fail"}
                                    </span>
                                  </td>
                                  <td>{(p.checks || []).join(", ") || "—"}</td>
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </div>
                      )}
                    </div>

                    <div className="df2-page-benchmarks-section">
                      <h3>How to verify</h3>
                      <ul className="df2-page-benchmarks-list">
                        {ledger.how_to_verify.map((item) => (
                          <li key={item}>{item}</li>
                        ))}
                      </ul>
                    </div>
                  </>
                )}
              </PageSection>
            </>
          )}

          {tab === "scale" && (
            <PageSection title="Reproducible scale proof">
              <div className="df2-alert df2-alert-info df2-page-benchmarks-workload" role="note">
                <DtIcon name="alert" size={16} />
                <div>
                  <strong>Workload class: synthetic CSV → SQLite on this API host</strong>
                  <p>
                    These numbers measure local file→SQLite throughput (often ~10k rows in under a second).
                    They are <em>not</em> MongoDB→Snowflake or other warehouse runs — those depend on network RTT,
                    warehouse size, COPY vs INSERT, and transform/quarantine work. Always trust the rows/sec shown
                    on the live job theater for a real transfer.
                  </p>
                </div>
              </div>
              <p className="df2-page-benchmarks-intro">
                Secondary to integrity: synthetic CSV → SQLite throughput vs public Fivetran / Airbyte / Stitch
                baselines. Speed without quarantine and checksums is not a migration proof.
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
                    <StatCard label="Rows transferred" value={formatNumber(report.rows)} icon="layers" />
                    <StatCard label="Throughput" value={`${formatNumber(report.records_per_second)} rows/sec`} icon="zap" />
                    <StatCard label="Elapsed time" value={formatSeconds(report.elapsed_seconds)} icon="clock" />
                    <StatCard label="Peak memory" value={`${formatNumber(report.peak_memory_mb)} MB`} icon="cpu" />
                  </div>

                  <div className="df2-page-benchmarks-section">
                    <h3>Throughput baselines (public figures)</h3>
                    <p className="df2-page-benchmarks-note">
                      Competitor RPS figures are representative public baselines. DataFlow numbers here are measured
                      live for <strong>CSV → SQLite only</strong> — not warehouse routes (Mongo→Snowflake, etc.).
                      Prefer the Integrity ledger for migration trust, and the job theater for this-job throughput.
                    </p>
                    <div className="df2-page-benchmarks-table-wrap">
                      <table className="df2-page-benchmarks-table">
                        <thead>
                          <tr>
                            <th>Product</th>
                            <th>Typical rows/sec</th>
                            <th>Resume</th>
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
                              <td>
                                <span className={`df2-badge ${c.resume_from_checkpoint ? "df2-badge-success" : "df2-badge-muted"}`}>
                                  {c.resume_from_checkpoint ? "Yes" : "No"}
                                </span>
                              </td>
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
                </>
              )}

              {!report && !running && !error && (
                <EmptyState
                  page
                  icon="speed"
                  title="Generate a live scale proof"
                  description="Pick a row count and run CSV → SQLite. For migration trust, use the Integrity ledger tab."
                />
              )}
            </PageSection>
          )}
        </div>
      </PageFrame>
    </PageShell>
  );
}
