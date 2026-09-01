import { useEffect, useMemo, useState } from "react";
import { DtIcon } from "../components/DtIcon";
import { Button } from "../components/ui/Button";
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
  runDesktopLab,
  runDesktopLabCross,
  runFidelityProof,
  type BenchmarkReport,
  type DesktopLabCrossReport,
  type DesktopLabReport,
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
  const [labRunning, setLabRunning] = useState(false);
  const [lab, setLab] = useState<DesktopLabReport | null>(null);
  const [crossRunning, setCrossRunning] = useState(false);
  const [cross, setCross] = useState<DesktopLabCrossReport | null>(null);

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

  const handleDesktopLab = async () => {
    setLabRunning(true);
    try {
      const result = await runDesktopLab();
      setLab(result);
      await loadLedger();
      toast({
        title: result.success ? "Desktop lab passed" : "Desktop lab incomplete",
        message: `${result.catalog_slots_duplex_passed} of ${result.catalog_slots} catalog slots passed as source and dest. Unique engines: ${result.unique_engines_duplex_passed}. Hosted twins share a driver.`,
        tone: result.success ? "success" : "error",
      });
    } catch (e) {
      toast({ title: "Desktop lab failed", message: String(e), tone: "error" });
    } finally {
      setLabRunning(false);
    }
  };

  const handleDesktopLabCross = async () => {
    setCrossRunning(true);
    try {
      const result = await runDesktopLabCross();
      setCross(result);
      toast({
        title: result.success ? "Unique-engine matrix passed" : "Unique-engine matrix incomplete",
        message: `${result.passed} of ${result.pairs} live src×dst pairs passed. Seeded ${result.unique_engines_seeded?.length ?? 0} unique engines. Not 80 catalog aliases.`,
        tone: result.success ? "success" : "error",
      });
    } catch (e) {
      toast({ title: "Unique-engine matrix failed", message: String(e), tone: "error" });
    } finally {
      setCrossRunning(false);
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
              { id: "integrity", label: "Integrity ledger", count: metrics?.production_sku_sold ?? metrics?.production_sku_routes },
              { id: "scale", label: "Scale throughput" },
            ]}
            value={tab}
            onChange={(id) => setTab(id)}
            ariaLabel="Proof tabs"
          />

          {tab === "integrity" && (
            <>
              <PageSection title="Why integrity proofs beat connect() theater">
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
                        label="SKU sold on this host"
                        value={formatNumber(metrics?.production_sku_sold ?? 0)}
                        icon="gate"
                        sub={`${metrics?.production_sku_sold ?? 0} of ${metrics?.production_sku_routes ?? 0} claimed — validate_transfer + driver present`}
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

                    <div className="df2-page-benchmarks-toolbar">
                      <p className="df2-page-benchmarks-note" style={{ margin: 0, flex: 1 }}>
                        Desktop lab option: bind 80 catalog connectors and run Map, cell transform,
                        ShapeEngine (trim+upper), Validate, dest write, source read, and shaped payload
                        reconcile. Hosted twins share a driver — this is not 80 unique engines.
                      </p>
                      <Button
                        variant="primary"
                        onClick={() => void handleDesktopLab()}
                        disabled={labRunning || crossRunning}
                        loading={labRunning}
                        loadingLabel="Running desktop lab…"
                      >
                        Run desktop lab
                      </Button>
                    </div>

                    <div className="df2-page-benchmarks-toolbar">
                      <p className="df2-page-benchmarks-note" style={{ margin: 0, flex: 1 }}>
                        Unique-engine matrix: every live unique engine as source × dest. Default is
                        Postgres, MySQL, Mongo, SQLite, MinIO S3 (25 pairs). Not 80×80 catalog aliases.
                        Extended adds SQL Server, Redis, Elasticsearch, object-store emulators, and
                        warehouses when those ports answer. A closed port is skipped — never fake green.
                        SaaS tiles without a desktop backend stay omitted.
                      </p>
                      <Button
                        variant="secondary"
                        onClick={() => void handleDesktopLabCross()}
                        disabled={labRunning || crossRunning}
                        loading={crossRunning}
                        loadingLabel="Running unique-engine matrix…"
                      >
                        Run unique-engine matrix
                      </Button>
                    </div>

                    {cross && (
                      <div className={`df2-alert ${cross.success ? "df2-alert-success" : "df2-alert-error"}`} role="status">
                        <DtIcon name={cross.success ? "check" : "alert"} size={18} />
                        <div>
                          <strong>{cross.passed} of {cross.pairs} unique-engine pairs</strong>
                          {" "}passed
                          {cross.failed ? ` · failed ${cross.failed}` : ""}
                          {cross.skipped ? ` · skipped ${cross.skipped}` : ""}
                          {cross.unique_engines_seeded?.length
                            ? ` · seeded ${cross.unique_engines_seeded.join(", ")}`
                            : ""}
                        </div>
                      </div>
                    )}

                    {cross?.routes && cross.routes.some((r) => r.status === "skipped") && (
                      <div className="df2-page-benchmarks-section">
                        <h3>Unique-engine pairs that skipped</h3>
                        <div className="df2-page-benchmarks-table-wrap">
                          <table className="df2-page-benchmarks-table">
                            <thead>
                              <tr>
                                <th>Source</th>
                                <th>Destination</th>
                                <th>Reason</th>
                              </tr>
                            </thead>
                            <tbody>
                              {cross.routes.filter((r) => r.status === "skipped").map((row) => (
                                <tr key={`${row.source}-${row.destination}-skip`}>
                                  <td><code>{row.source}</code></td>
                                  <td><code>{row.destination}</code></td>
                                  <td>{row.error || "skipped"}</td>
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </div>
                      </div>
                    )}

                    {cross?.routes && cross.routes.some((r) => r.status === "failed") && (
                      <div className="df2-page-benchmarks-section">
                        <h3>Unique-engine pairs that failed</h3>
                        <div className="df2-page-benchmarks-table-wrap">
                          <table className="df2-page-benchmarks-table">
                            <thead>
                              <tr>
                                <th>Source</th>
                                <th>Destination</th>
                                <th>Error</th>
                              </tr>
                            </thead>
                            <tbody>
                              {cross.routes.filter((r) => r.status === "failed").map((row) => (
                                <tr key={`${row.source}-${row.destination}`}>
                                  <td><code>{row.source}</code></td>
                                  <td><code>{row.destination}</code></td>
                                  <td>{row.error || "failed"}</td>
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </div>
                      </div>
                    )}

                    {lab && (
                      <div className={`df2-alert ${lab.success ? "df2-alert-success" : "df2-alert-error"}`} role="status">
                        <DtIcon name={lab.success ? "check" : "alert"} size={18} />
                        <div>
                          <strong>{lab.catalog_slots_duplex_passed} of {lab.catalog_slots} catalog slots</strong>
                          {" "}passed as source and dest
                          {lab.unique_engines_duplex_passed != null
                            ? ` · unique engines ${lab.unique_engines_duplex_passed}`
                            : ""}
                          {lab.failed ? ` · failed ${lab.failed}` : ""}
                          {lab.skipped ? ` · skipped ${lab.skipped}` : ""}
                        </div>
                      </div>
                    )}

                    {lab?.connectors && lab.connectors.length > 0 && (
                      <div className="df2-page-benchmarks-section">
                        <h3>Desktop lab — source and dest</h3>
                        <div className="df2-page-benchmarks-table-wrap">
                          <table className="df2-page-benchmarks-table">
                            <thead>
                              <tr>
                                <th>Catalog</th>
                                <th>Driver</th>
                                <th>Kind</th>
                                <th>Dest</th>
                                <th>Source</th>
                              </tr>
                            </thead>
                            <tbody>
                              {lab.connectors.map((row) => (
                                <tr key={row.catalog_id}>
                                  <td><code>{row.catalog_id}</code></td>
                                  <td>{row.driver}</td>
                                  <td>{row.role === "unique_engine" ? "Unique engine" : row.role === "hosted_twin" ? "Hosted twin" : "Format alias"}</td>
                                  <td>{row.dest_status}</td>
                                  <td>{row.source_status}</td>
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </div>
                      </div>
                    )}

                    <div className="df2-page-benchmarks-section">
                      <h3>Integrity dimensions vs industry ELT baselines</h3>
                      <div className="df2-page-benchmarks-table-wrap">
                        <table className="df2-page-benchmarks-table df2-page-benchmarks-table--prose">
                          <thead>
                            <tr>
                              <th>Dimension</th>
                              <th>Datawrap</th>
                              <th>Industry ELT</th>
                              <th>Proof surface</th>
                            </tr>
                          </thead>
                          <tbody>
                            {ledger.integrity_comparison.map((row) => (
                              <tr key={row.dimension}>
                                <td><strong>{row.dimension}</strong></td>
                                <td>{row.dataflow}</td>
                                <td>{row.industry_elt}</td>
                                <td><code>{row.proof}</code></td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    </div>

                    <div className="df2-page-benchmarks-section">
                      <h3>PRODUCTION_SKU — sold vs claimed</h3>
                      <p className="df2-page-benchmarks-note">
                        {metrics?.production_sku_note
                          || `Sold ${metrics?.production_sku_sold ?? 0} of ${ledger.production_sku.length} claimed routes on this host. Catalog tiles are not this list.`}
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
                                <td>
                                  <span
                                    className={
                                      r.status === "sold"
                                        ? "df2-badge df2-badge-success"
                                        : r.status === "driver_missing"
                                          ? "df2-badge df2-badge-warning"
                                          : "df2-badge df2-badge-error"
                                    }
                                    title={r.driver_gap || undefined}
                                  >
                                    {r.status === "sold"
                                      ? "sold now"
                                      : r.status === "driver_missing"
                                        ? "driver missing"
                                        : "refused"}
                                  </span>
                                </td>
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
                                  <td className="df2-page-benchmarks-cell-wrap">{p.route || "—"}</td>
                                  <td>{p.rows != null ? formatNumber(p.rows) : "—"}</td>
                                  <td>
                                    <span className={`df2-badge ${p.success ? "df2-badge-success" : "df2-badge-warning"}`}>
                                      {p.success ? "pass" : "fail"}
                                    </span>
                                  </td>
                                  <td className="df2-page-benchmarks-cell-wrap">{(p.checks || []).join(", ") || "—"}</td>
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
                Secondary to integrity: synthetic CSV → SQLite throughput vs illustrative mid-market ELT
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
                  <Button
                    variant="secondary"
                    onClick={handleDownload}
                    disabled={!report || running}
                    leadingIcon={<DtIcon name="download" size={14} />}
                  >
                    Report
                  </Button>
                  <Button
                    variant="primary"
                    onClick={handleRun}
                    disabled={running}
                    loading={running}
                    loadingLabel="Running…"
                    leadingIcon={<DtIcon name="play" size={14} />}
                  >
                    Run benchmark
                  </Button>
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
                      Competitor RPS figures are representative public baselines. Datawrap numbers here are measured
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
                            <th>vs Datawrap</th>
                          </tr>
                        </thead>
                        <tbody>
                          {comparison?.map((c) => (
                            <tr key={c.product}>
                              <td className="df2-page-benchmarks-product">
                                {c.product === "Datawrap" ? (
                                  <strong><DtIcon name="speed" size={14} /> Datawrap (this run)</strong>
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
                                {c.product === "Datawrap" ? (
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
