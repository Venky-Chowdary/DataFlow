import { Spinner } from "../LoadingState";
import type { PreflightResult } from "../../lib/types";

interface ProofDashboardProps {
  preflight: PreflightResult | null;
  running?: boolean;
  /** Start expanded — default collapsed for density */
  defaultOpen?: boolean;
}

export function ProofDashboard({ preflight, running = false, defaultOpen }: ProofDashboardProps) {
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

  const openByDefault = defaultOpen ?? (!running && Boolean(preflight && !preflight.passed));

  return (
    <details
      key={`proof-${running ? "live" : preflight?.passed ? "ok" : "warn"}-${preflight?.passed_count ?? 0}`}
      className={`df2-proof-dashboard df2-disclosure ${statusTone}`}
      open={!running && openByDefault}
      aria-label="Proof dashboard"
    >
      <summary className="df2-proof-dashboard-summary-bar">
        <div className="df2-proof-dashboard-summary-main">
          {running ? (
            <>
              <Spinner size="sm" label="" />
              <strong>Validating…</strong>
              <span className="df2-proof-dashboard-summary-meta">Safety gates in progress</span>
            </>
          ) : (
            <>
              <strong>{preflight ? `${readiness.toFixed(0)}% ready` : "Proof summary"}</strong>
              <span className="df2-proof-dashboard-summary-meta">
                {preflight
                  ? `${decision.toUpperCase()} · ${preflight.passed_count}/${preflight.total_gates} gates · ${confidenceBand}`
                  : "Run preflight to surface evidence"}
              </span>
            </>
          )}
        </div>
        <span className="df2-disclosure-chevron" aria-hidden>Details</span>
      </summary>

      <div className="df2-proof-dashboard-body">
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
            : (proof?.evidence_summary ?? "No proof bundle available yet — run preflight to surface deterministic transfer evidence.")}
        </p>
      </div>
    </details>
  );
}
