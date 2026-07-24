import { DtIcon } from "./DtIcon";
import { Spinner } from "./LoadingState";
import { PreflightResult } from "../lib/types";
import { useEffect, useState } from "react";
import { CORE_ENGINE_GATE_IDS, gateLabel } from "../lib/preflightGates";

const CORE_GATE_ORDER = [...CORE_ENGINE_GATE_IDS];

function formatElapsed(ms: number): string {
  const s = ms / 1000;
  return s < 10 ? `${s.toFixed(1)}s` : `${Math.round(s)}s`;
}

function formatDuration(ms: number | undefined): string {
  if (ms == null || Number.isNaN(ms)) return "";
  if (ms < 10) return "<10ms";
  if (ms < 1000) return `${Math.round(ms)}ms`;
  return `${(ms / 1000).toFixed(1)}s`;
}

interface PreflightTimelineProps {
  result: PreflightResult;
  running?: boolean;
  confidenceThreshold?: number;
  compact?: boolean;
  hideActions?: boolean;
  onApproveMappings?: () => void;
  onRerun?: () => void;
  onUseBalanced?: () => void;
}

export function PreflightTimeline({
  result,
  running,
  confidenceThreshold = 0.85,
  compact = false,
  hideActions = false,
  onApproveMappings,
  onRerun,
  onUseBalanced,
}: PreflightTimelineProps) {
  const [elapsedMs, setElapsedMs] = useState(0);
  const [revealCount, setRevealCount] = useState(0);

  useEffect(() => {
    if (!running) {
      setElapsedMs(0);
      return;
    }
    const t0 = performance.now();
    const timer = window.setInterval(() => setElapsedMs(performance.now() - t0), 100);
    return () => window.clearInterval(timer);
  }, [running]);

  useEffect(() => {
    if (running || !result.gates?.length) {
      setRevealCount(0);
      return;
    }
    setRevealCount(0);
    let i = 0;
    let timer = 0;
    const advance = () => {
      i += 1;
      setRevealCount(i);
      if (i >= result.gates.length) return;
      const pace = Math.min(900, Math.max(140, Number(result.gates[i - 1]?.duration_ms) || 180));
      timer = window.setTimeout(advance, pace);
    };
    timer = window.setTimeout(advance, 60);
    return () => window.clearTimeout(timer);
  }, [running, result.run_id, result.gates]);

  const gates = running
    ? CORE_GATE_ORDER.map((id) => ({
        id,
        status: "pending" as const,
        message: "Queued — waiting for engine result",
        duration_ms: 0,
      }))
    : result.gates.length > 0
      ? result.gates.map((g, i) =>
          i < revealCount
            ? g
            : {
                ...g,
                status: "pending" as const,
                message: "Result ready — revealing…",
              },
        )
      : [];

  const passCount = (result.gates || []).filter((g) => g.status === "pass").length;
  const blockCount = (result.gates || []).filter((g) => g.status === "block").length;
  const skipCount = (result.gates || []).filter((g) => g.status === "skip").length;

  const proof = result.proof_bundle;
  const decision = proof?.transfer_decision?.decision ?? (result.passed ? "approve" : "review");
  const decisionLabel = decision.toUpperCase();
  const decisionTone = decision === "block" ? "#dc2626" : decision === "review" ? "#f59e0b" : "#16a34a";
  const stateClass = result.passed ? "passed" : result.blockers.length ? "blocked" : "";
  const mappingBlocked = result.blockers.some((b) => b.id.includes("mapping"));
  const schemaPolicyBlocked = result.blockers.some((b) => b.id.includes("schema_policy"));
  const decisionTitle = decision === "block"
    ? "Blocked by proof guardrails"
    : decision === "review"
      ? "Human review required"
      : "Proof approved";
  const decisionReason = proof?.transfer_decision?.reason || "No blocking issues detected";
  const proofNotes = proof?.semantic_notes?.slice(0, 3) || [];
  const confidenceBand = proof?.confidence_band?.toUpperCase() || "MEDIUM";
  const qualityGrade = proof?.quality_grade?.toUpperCase() || "GOOD";
  const evidenceSummary = proof?.evidence_summary || "Deterministic proof signals ready for operator review.";

  const blockerIssuePreview = (details?: Record<string, unknown>): string | null => {
    const rawIssues = details?.issues;
    if (!Array.isArray(rawIssues) || rawIssues.length === 0) return null;
    const firstIssue = rawIssues[0];
    return typeof firstIssue === "string" ? firstIssue : null;
  };

  return (
    <div className={`df2-preflight ${stateClass}${compact ? " is-compact" : ""}${running ? " is-validating" : ""}`}>
      {!compact && (
      <div className="df2-preflight-head">
        <div className="df2-preflight-score">
          <svg viewBox="0 0 80 80" aria-hidden>
            <circle cx="40" cy="40" r="34" className="df2-score-track" />
            <circle
              cx="40"
              cy="40"
              r="34"
              className="df2-score-fill"
              strokeDasharray={`${((running ? Math.min(92, 18 + elapsedMs / 80) : result.readiness_score) / 100) * 213.6} 213.6`}
              transform="rotate(-90 40 40)"
            />
          </svg>
          <div className="df2-preflight-score-val">
            {running ? (
              <>
                <span style={{ fontSize: 14 }}>{formatElapsed(elapsedMs)}</span>
              </>
            ) : (
              <>
                <span>{result.readiness_score}</span>
                <small>%</small>
              </>
            )}
          </div>
        </div>
        <div>
          <h3 className="df2-preflight-title">
            {running ? "Engine running G1–G8…" : result.passed ? "Ready to transfer" : "Validation — action needed"}
          </h3>
          <p className="df2-preflight-sub">
            {running
              ? `Wall-clock ${formatElapsed(elapsedMs)} · real gates, not a fake step animation`
              : `${passCount} passed · ${blockCount} blocked · ${skipCount} skipped · ${result.total_gates} total`}
            {result.passed ? " · you can execute the transfer" : !running ? " · fix items below, then re-run" : ""}
          </p>
          {proof && (
            <div style={{ display: "grid", gap: 8, marginTop: 10 }}>
              <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
                <span
                  style={{
                    display: "inline-flex",
                    alignItems: "center",
                    padding: "4px 8px",
                    borderRadius: 999,
                    background: decisionTone,
                    color: "white",
                    fontSize: 11,
                    fontWeight: 700,
                    letterSpacing: 0.5,
                  }}
                >
                  PROOF {decisionLabel}
                </span>
                <span style={{ fontSize: 12, color: "#475569", fontWeight: 700 }}>
                  Semantic {proof.semantic_mapping_score.toFixed(2)}
                </span>
                <span style={{ fontSize: 12, color: "#475569", fontWeight: 700 }}>
                  Quality {proof.quality_score.toFixed(2)}
                </span>
                <span style={{ fontSize: 12, color: "#475569", fontWeight: 700 }}>
                  Compliance {proof.compliance.risk_score.toFixed(2)}
                </span>
              </div>
              <div
                style={{
                  display: "grid",
                  gap: 6,
                  padding: 10,
                  borderRadius: 10,
                  background: "#f8fafc",
                  border: "1px solid #e2e8f0",
                  fontSize: 12,
                  color: "#0f172a",
                }}
              >
                <strong>{decisionTitle}</strong>
                <span>{decisionReason}</span>
                <span>
                  Confidence band: {confidenceBand} · Quality grade: {qualityGrade}
                </span>
                <span>{evidenceSummary}</span>
                {proofNotes.length > 0 && (
                  <ul style={{ margin: 0, paddingLeft: 16 }}>
                    {proofNotes.map((note) => (
                      <li key={note}>{note}</li>
                    ))}
                  </ul>
                )}
              </div>
            </div>
          )}
          {result.blockers.length > 0 && (
            <ul className="df2-preflight-blocker-list">
              {result.blockers.map((b) => (
                <li key={b.id}>
                  <span>{b.message}</span>
                  {blockerIssuePreview(b.details) && (
                    <small style={{ display: "block", marginTop: 2, color: "#64748b" }}>
                      {blockerIssuePreview(b.details)}
                    </small>
                  )}
                </li>
              ))}
            </ul>
          )}
        </div>
      </div>
      )}

      {compact && !running && (
        <div className="df2-preflight-compact-head">
          <div>
            <h3 className="df2-preflight-title">
              {result.passed ? "All checks passed" : "Checks need attention"}
            </h3>
            <div className="df2-preflight-compact-summary">
              <span className="ok">{passCount} passed</span>
              {blockCount > 0 && <span className="block">{blockCount} blocked</span>}
              {skipCount > 0 && <span className="skip">{skipCount} skipped</span>}
            </div>
          </div>
          <span className="df2-preflight-compact-score">{result.passed_count}/{result.total_gates} passed</span>
        </div>
      )}

      {!hideActions && !result.passed && !running && (
        <div className="df2-preflight-fix-panel">
          <strong>How to fix</strong>
          <div className="df2-preflight-fix-actions">
            {mappingBlocked && onApproveMappings && (
              <button type="button" className="df2-btn df2-btn-primary df2-btn-sm" onClick={onApproveMappings}>
                <DtIcon name="check" size={14} /> Approve all column mappings
              </button>
            )}
            {mappingBlocked && onUseBalanced && confidenceThreshold >= 0.85 && (
              <button type="button" className="df2-btn df2-btn-sm" onClick={onUseBalanced}>
                Use Balanced validation (75% threshold)
              </button>
            )}
            {schemaPolicyBlocked && (
              <span className="df2-preflight-fix-hint">
                Turn off &quot;Backfill new fields&quot; or switch schema policy to Column changes.
              </span>
            )}
            {onRerun && (
              <button type="button" className="df2-btn df2-btn-sm" onClick={onRerun}>
                <DtIcon name="gate" size={14} /> Re-run validation
              </button>
            )}
          </div>
        </div>
      )}

      {running && (
        <div className="df2-validate-stage" role="status" aria-live="polite">
          <div className="df2-validate-stage-glow" aria-hidden />
          <div className="df2-validate-stage-core">
            <Spinner size="sm" label="" />
            <h3>Validating route</h3>
            <p>Engine evaluating G1–G8 · {formatElapsed(elapsedMs)} elapsed</p>
            <div className="df2-preflight-progress is-indeterminate" role="status">
              <div className="df2-mapping-progress-meta">
                <strong>{formatElapsed(elapsedMs)}</strong>
                <span>Wall clock · not fake %</span>
              </div>
              <div className="df2-mapping-progress-track">
                <span className="df2-mapping-progress-fill is-animating" style={{ width: "42%" }} />
              </div>
            </div>
          </div>
        </div>
      )}

      <div className="df2-preflight-track">
        {gates.map((gate, i) => (
          <div
            key={gate.id}
            className={`df2-preflight-step ${gate.status}`}
            style={{ animationDelay: `${i * 40}ms` }}
            title={gate.message}
          >
            <div className="df2-preflight-marker">
              {gate.status === "pass" && <DtIcon name="check" size={12} />}
              {gate.status === "block" && <DtIcon name="x" size={12} />}
              {gate.status === "running" && <Spinner size="sm" label="" />}
              {gate.status === "skip" && <span>—</span>}
              {gate.status === "pending" && <span>·</span>}
            </div>
            <div className="df2-preflight-step-copy">
              <div className="df2-preflight-step-title">
                {gateLabel(gate.id)}
                {gate.status !== "pending" && gate.duration_ms > 0 ? (
                  <span style={{ marginLeft: 8, color: "#94a3b8", fontWeight: 500, fontSize: 11 }}>
                    {formatDuration(gate.duration_ms)}
                  </span>
                ) : null}
              </div>
              <div className="df2-preflight-step-msg">{gate.message}</div>
            </div>
          </div>
        ))}
      </div>

      {result.blockers.length > 0 && !running && (
        <details className="df2-disclosure df2-preflight-diagnostics" open>
          <summary className="df2-preflight-diagnostics-head">
            <DtIcon name="alert" size={14} />
            <strong>Blockers</strong>
            <span className="df2-preflight-diagnostics-count">{result.blockers.length}</span>
            <span className="df2-disclosure-chevron" aria-hidden />
          </summary>
          <ul className="df2-preflight-diagnostics-list">
            {result.blockers.map((b) => (
              <li key={b.id}>
                <strong>{gateLabel(b.id)}:</strong> {b.message}
                {b.guidance && (
                  <div className="df2-preflight-diagnostics-guidance">
                    {b.guidance.why && <p><strong>Why:</strong> {b.guidance.why}</p>}
                    {b.guidance.fix && <p><strong>Fix:</strong> {b.guidance.fix}</p>}
                    {b.guidance.examples && b.guidance.examples.length > 0 && (
                      <ul>
                        {b.guidance.examples.map((ex, i) => <li key={i}>{ex}</li>)}
                      </ul>
                    )}
                  </div>
                )}
                {b.details && typeof b.details === "object" && (
                  <ul className="df2-preflight-diagnostics-sub">
                    {Object.entries(b.details).map(([k, v]) => {
                      const value = Array.isArray(v) ? v.slice(0, 3).join(", ") : String(v);
                      if (!value || value === "[object Object]" || k === "guidance") return null;
                      return (
                        <li key={k}>
                          <span>{k}:</span> {value}
                        </li>
                      );
                    })}
                  </ul>
                )}
              </li>
            ))}
          </ul>
        </details>
      )}
    </div>
  );
}
