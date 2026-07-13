import { Spinner } from "../LoadingState";
import type { PreflightResult } from "../../lib/types";

interface ProofDashboardProps {
  preflight: PreflightResult | null;
  running?: boolean;
  /** @deprecated kept for callers — dashboard always renders expanded */
  defaultOpen?: boolean;
}

/**
 * Always-visible proof summary for Validate / Run.
 * No disclosure — primary dashboard content stays on screen when space exists.
 */
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
    { label: "Decision", value: decision.toUpperCase() },
    { label: "Confidence", value: confidenceBand },
    { label: "Quality", value: qualityGrade },
    { label: "Compliance", value: complianceRisk.toFixed(2) },
  ];

  return (
    <section className={`df2-proof-dashboard df2-proof-dashboard-open ${statusTone}`} aria-label="Proof dashboard">
      <div className="df2-proof-dashboard-head">
        <div>
          <p className="df2-proof-dashboard-kicker">Proof dashboard</p>
          <h3 className="df2-proof-dashboard-title">
            {running ? (
              <>
                <Spinner size="sm" label="" />
                <span>Running validation…</span>
              </>
            ) : preflight ? (
              <span>
                {readiness.toFixed(0)}% ready · {decision.toUpperCase()} · {preflight.passed_count}/{preflight.total_gates} gates
              </span>
            ) : (
              <span>Route intelligence and trust posture</span>
            )}
          </h3>
        </div>
      </div>

      <div className="df2-proof-dashboard-grid">
        <div className="df2-proof-dashboard-stat">
          <span>Readiness</span>
          <strong>{running ? "—" : `${readiness.toFixed(0)}%`}</strong>
        </div>
        <div className="df2-proof-dashboard-stat">
          <span>Semantic</span>
          <strong>{running ? "—" : semanticScore.toFixed(2)}</strong>
        </div>
        <div className="df2-proof-dashboard-stat">
          <span>Quality</span>
          <strong>{running ? "—" : qualityScore.toFixed(2)}</strong>
        </div>
        <div className="df2-proof-dashboard-stat">
          <span>Gates</span>
          <strong>{preflight ? `${preflight.passed_count}/${preflight.total_gates}` : "—"}</strong>
        </div>
      </div>

      <div className="df2-proof-dashboard-chips">
        {chips.map((chip) => (
          <span key={chip.label} className="df2-proof-chip">
            <small>{chip.label}</small>
            <strong>{running ? "—" : chip.value}</strong>
          </span>
        ))}
      </div>

      <p className="df2-proof-dashboard-summary">
        {running
          ? "Executing schema, mapping, transform, and data-integrity gates against the source and destination."
          : (proof?.evidence_summary ?? "Run preflight to surface deterministic transfer evidence.")}
      </p>
    </section>
  );
}
