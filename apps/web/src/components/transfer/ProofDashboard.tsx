import type { PreflightResult } from "../../lib/types";

interface ProofDashboardProps {
  preflight: PreflightResult | null;
  running?: boolean;
}

export function ProofDashboard({ preflight, running = false }: ProofDashboardProps) {
  const proof = preflight?.proof_bundle;
  const decision = proof?.transfer_decision?.decision ?? (preflight?.passed ? "approve" : "review");
  const confidenceBand = (proof?.confidence_band ?? "medium").toUpperCase();
  const qualityGrade = (proof?.quality_grade ?? "good").toUpperCase();
  const readiness = preflight?.readiness_score ?? 0;
  const semanticScore = proof?.semantic_mapping_score ?? 0;
  const qualityScore = proof?.quality_score ?? 0;
  const complianceRisk = proof?.compliance?.risk_score ?? 0;
  const statusTone = running
    ? "live"
    : decision === "block"
      ? "warn"
      : decision === "review"
        ? "warn"
        : preflight?.passed
          ? "ok"
          : preflight
            ? "warn"
            : "info";

  const chips = [
    { label: "Proof decision", value: decision.toUpperCase() },
    { label: "Confidence", value: confidenceBand },
    { label: "Quality", value: qualityGrade },
    { label: "Compliance risk", value: complianceRisk.toFixed(2) },
  ];

  return (
    <section className={`df2-proof-dashboard ${statusTone}`} aria-label="Proof dashboard">
      <div className="df2-proof-dashboard-head">
        <div>
          <p className="df2-proof-dashboard-kicker">Enterprise proof command center</p>
          <h3 className="df2-proof-dashboard-title">Route intelligence and trust posture</h3>
        </div>
        <div className="df2-proof-dashboard-trust">
          <span className="df2-status-chip">Deterministic safety gates</span>
          <span className="df2-status-chip">Operator review ready</span>
        </div>
      </div>

      <div className="df2-proof-dashboard-grid">
        <div className="df2-proof-dashboard-stat">
          <span>Readiness</span>
          <strong>{readiness.toFixed(0)}%</strong>
        </div>
        <div className="df2-proof-dashboard-stat">
          <span>Semantic confidence</span>
          <strong>{semanticScore.toFixed(2)}</strong>
        </div>
        <div className="df2-proof-dashboard-stat">
          <span>Sample quality</span>
          <strong>{qualityScore.toFixed(2)}</strong>
        </div>
        <div className="df2-proof-dashboard-stat">
          <span>Gate score</span>
          <strong>{preflight ? `${preflight.passed_count}/${preflight.total_gates}` : "—"}</strong>
        </div>
      </div>

      <div className="df2-proof-dashboard-chips">
        {chips.map((chip) => (
          <span key={chip.label} className="df2-proof-chip">
            <small>{chip.label}</small>
            <strong>{chip.value}</strong>
          </span>
        ))}
      </div>

      <p className="df2-proof-dashboard-summary">
        {proof?.evidence_summary ?? "No proof bundle available yet — run preflight to surface deterministic transfer evidence."}
      </p>
    </section>
  );
}
