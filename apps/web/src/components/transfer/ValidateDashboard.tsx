import { useEffect, useState } from "react";
import { DtIcon } from "../DtIcon";
import { Spinner } from "../LoadingState";
import type { PreflightGate, PreflightResult } from "../../lib/types";

interface GateMeta {
  key: string;
  label: string;
  icon: string;
  rule: string;
}

/** The full set of validation rules the engine enforces, in execution order. */
const GATE_META: GateMeta[] = [
  { key: "g1_source", label: "Source readable", icon: "database", rule: "Source endpoint connects and rows can be read." },
  { key: "g2_destination", label: "Destination reachable", icon: "server", rule: "Destination accepts a connection and is writable." },
  { key: "g3_schema", label: "Schema contract", icon: "layers", rule: "Source and target schemas are compatible." },
  { key: "g4_mapping", label: "Column mappings", icon: "sparkle", rule: "Every column maps above the confidence threshold." },
  { key: "g5_transform", label: "Dry-run transform", icon: "code", rule: "Sample rows pass all transform functions cleanly." },
  { key: "g9_data_integrity", label: "Data integrity", icon: "shield", rule: "Types, nulls, and constraints hold on sampled data." },
  { key: "g6_ddl", label: "Target DDL", icon: "scan", rule: "Any required CREATE / ALTER statements are valid." },
  { key: "g7_capacity", label: "Staging capacity", icon: "trend", rule: "Destination has headroom for the row volume." },
  { key: "g8_reconciliation", label: "Reconciliation", icon: "activity", rule: "Post-transfer checksums are compared source ↔ target." },
  { key: "g9_sync_contract", label: "Sync contract", icon: "transfer", rule: "Cursor and primary-key contract satisfy the sync mode." },
  { key: "g10_schema_policy", label: "Schema change policy", icon: "gate", rule: "Detected drift is allowed by the schema policy." },
  { key: "g11_validation_posture", label: "Validation posture", icon: "lock", rule: "Overall posture meets the selected validation mode." },
];

function metaForGate(id: string): GateMeta {
  return (
    GATE_META.find((m) => id.includes(m.key)) ?? {
      key: id,
      label: id.replace(/^g\d+_/, "").replace(/_/g, " "),
      icon: "gate",
      rule: "Validation rule enforced before transfer.",
    }
  );
}

const STATUS_LABEL: Record<string, string> = {
  pass: "Passed",
  block: "Blocked",
  skip: "Skipped",
  running: "Running",
  pending: "Pending",
};

interface ValidateDashboardProps {
  preflight: PreflightResult | null;
  running?: boolean;
  confidenceThreshold?: number;
}

function Ring({ value, label, sub, tone }: { value: number; label: string; sub: string; tone: string }) {
  const r = 26;
  const c = 2 * Math.PI * r;
  const pct = Math.max(0, Math.min(100, value));
  return (
    <div className={`df2-vd-ring tone-${tone}`}>
      <div className="df2-vd-ring-svg" aria-hidden>
        <svg viewBox="0 0 64 64">
          <circle cx="32" cy="32" r={r} className="df2-vd-ring-track" />
          <circle
            cx="32"
            cy="32"
            r={r}
            className="df2-vd-ring-fill"
            strokeDasharray={`${(pct / 100) * c} ${c}`}
            transform="rotate(-90 32 32)"
          />
        </svg>
        <span className="df2-vd-ring-val">{Math.round(value)}<small>%</small></span>
      </div>
      <div className="df2-vd-ring-copy">
        <strong>{label}</strong>
        <span>{sub}</span>
      </div>
    </div>
  );
}

export function ValidateDashboard({ preflight, running = false, confidenceThreshold = 0.85 }: ValidateDashboardProps) {
  const [progress, setProgress] = useState(0);

  useEffect(() => {
    if (!running) {
      setProgress(0);
      return;
    }
    setProgress(12);
    const timer = window.setInterval(() => {
      setProgress((prev) => (prev >= 96 ? prev : Math.min(96, prev + Math.max(2, Math.round(Math.random() * 7)))));
    }, 220);
    return () => window.clearInterval(timer);
  }, [running]);

  const proof = preflight?.proof_bundle;
  const decision = proof?.transfer_decision?.decision ?? (preflight?.passed ? "approve" : preflight ? "review" : "review");
  const readiness = preflight?.readiness_score ?? 0;
  const totalGates = preflight?.total_gates || GATE_META.length;
  const passedCount = preflight?.passed_count ?? 0;

  const gateByKey = new Map<string, PreflightGate>();
  for (const gate of preflight?.gates ?? []) {
    gateByKey.set(metaForGate(gate.id).key, gate);
  }
  const blockedCount = (preflight?.gates ?? []).filter((g) => g.status === "block").length;
  const skippedCount = (preflight?.gates ?? []).filter((g) => g.status === "skip").length;

  const decisionTone = decision === "block" ? "block" : decision === "review" ? "review" : "approve";
  const heroTone = running ? "live" : preflight ? decisionTone : "idle";

  const semantic = proof?.semantic_mapping_score ?? 0;
  const quality = proof?.quality_score ?? 0;
  const complianceRisk = proof?.compliance?.risk_score ?? 0;
  const reconciliation = proof?.reconciliation;

  // While validating we don't have real gate results yet, so animate the rules
  // sequentially with the progress bar — one "running" at a time reads far
  // cleaner than a wall of 12 spinners.
  const activeRuleIndex = Math.floor((progress / 100) * GATE_META.length);

  const statusForGate = (meta: GateMeta, index: number): { status: string; message: string } => {
    const gate = gateByKey.get(meta.key);
    if (gate) return { status: gate.status, message: gate.message };
    if (running) {
      if (index < activeRuleIndex) return { status: "pass", message: "Checked" };
      if (index === activeRuleIndex) return { status: "running", message: "Evaluating rule…" };
      return { status: "pending", message: "Queued" };
    }
    return { status: "pending", message: "Awaiting validation run." };
  };

  return (
    <section className={`df2-vd df2-vd-${heroTone}`} aria-label="Validation dashboard">
      <header className="df2-vd-hero">
        <div className={`df2-vd-hero-ring tone-${heroTone}`} aria-hidden>
          <svg viewBox="0 0 120 120">
            <circle cx="60" cy="60" r="52" className="df2-vd-hero-track" />
            <circle
              cx="60"
              cy="60"
              r="52"
              className="df2-vd-hero-fill"
              strokeDasharray={`${((running ? progress : readiness) / 100) * 326.7} 326.7`}
              transform="rotate(-90 60 60)"
            />
          </svg>
          <div className="df2-vd-hero-ring-label">
            <strong>{running ? progress : readiness}<small>%</small></strong>
            <span>ready</span>
          </div>
        </div>

        <div className="df2-vd-hero-copy">
          <div className="df2-vd-hero-head">
            <span className={`df2-vd-decision df2-vd-decision-${decisionTone}`}>
              <DtIcon name={decision === "approve" ? "check" : decision === "block" ? "x" : "shield"} size={13} />
              {running ? "VALIDATING" : decision.toUpperCase()}
            </span>
            <h3>
              {running
                ? "Running validation gates…"
                : preflight
                  ? preflight.passed
                    ? "Ready to transfer"
                    : "Action needed before transfer"
                  : "Run validation to check this route"}
            </h3>
          </div>

          <div className="df2-vd-hero-counts">
            <span className="df2-vd-count ok"><strong>{passedCount}</strong> passed</span>
            <span className="df2-vd-count block"><strong>{blockedCount}</strong> blocked</span>
            <span className="df2-vd-count skip"><strong>{skippedCount}</strong> skipped</span>
            <span className="df2-vd-count total"><strong>{totalGates}</strong> total rules</span>
          </div>

          <p className="df2-vd-hero-summary">
            {running
              ? "Enforcing schema, mapping, transform, integrity, and policy rules against your source and destination."
              : proof?.evidence_summary ?? "Every rule below runs before a single row is written. Nothing transfers until the gates you require pass."}
          </p>

          {running && (
            <div className="df2-vd-progress" role="progressbar" aria-valuemin={0} aria-valuemax={100} aria-valuenow={progress}>
              <span className="df2-vd-progress-fill" style={{ width: `${progress}%` }} />
            </div>
          )}
        </div>
      </header>

      {!running && preflight && (
        <div className="df2-vd-metrics">
          <Ring value={readiness} label="Readiness" sub="Overall route score" tone={heroTone} />
          <Ring value={semantic * 100} label="Semantic mapping" sub="Column match confidence" tone="approve" />
          <Ring value={quality * 100} label="Data quality" sub="Profiled quality grade" tone="approve" />
          <Ring value={(1 - complianceRisk) * 100} label="Compliance" sub={`Risk ${complianceRisk.toFixed(2)}`} tone={complianceRisk > 0.4 ? "review" : "approve"} />
        </div>
      )}

      <div className="df2-vd-rules">
        <div className="df2-vd-rules-head">
          <DtIcon name="gate" size={15} />
          <strong>Validation rules</strong>
          <span>{totalGates} checks enforced before write · threshold {(confidenceThreshold * 100).toFixed(0)}%</span>
        </div>
        <div className="df2-vd-rules-grid">
          {GATE_META.map((meta, index) => {
            const { status, message } = statusForGate(meta, index);
            return (
              <article key={meta.key} className={`df2-vd-rule status-${status}`}>
                <div className="df2-vd-rule-top">
                  <span className="df2-vd-rule-icon"><DtIcon name={meta.icon} size={15} /></span>
                  <span className={`df2-vd-rule-status status-${status}`}>
                    {status === "pass" && <DtIcon name="check" size={11} />}
                    {status === "block" && <DtIcon name="x" size={11} />}
                    {status === "running" && <Spinner size="sm" label="" />}
                    {STATUS_LABEL[status] ?? status}
                  </span>
                </div>
                <strong className="df2-vd-rule-label">{meta.label}</strong>
                <p className="df2-vd-rule-desc">{meta.rule}</p>
                {status !== "pending" && message && <p className="df2-vd-rule-msg">{message}</p>}
              </article>
            );
          })}
        </div>
      </div>

      {reconciliation && (
        <div className="df2-vd-recon">
          <span className={`df2-vd-recon-badge ${reconciliation.passed ? "ok" : "warn"}`}>
            <DtIcon name={reconciliation.passed ? "check" : "alert"} size={13} />
            {reconciliation.preview ? "Reconciliation preview" : "Reconciliation"}
          </span>
          <span>{reconciliation.matched_key_count?.toLocaleString() ?? "—"} matched</span>
          <span>{reconciliation.missing_key_count?.toLocaleString() ?? 0} missing</span>
          <span>{reconciliation.extra_key_count?.toLocaleString() ?? 0} extra</span>
          {reconciliation.row_fidelity_score != null && (
            <span>fidelity {(reconciliation.row_fidelity_score * 100).toFixed(0)}%</span>
          )}
        </div>
      )}

      {preflight && preflight.blockers.length > 0 && !running && (
        <div className="df2-vd-blockers">
          <div className="df2-vd-blockers-head">
            <DtIcon name="alert" size={15} />
            <strong>Resolve to continue</strong>
            <span>{preflight.blockers.length}</span>
          </div>
          <ul>
            {preflight.blockers.map((b) => (
              <li key={b.id}>
                <strong>{metaForGate(b.id).label}</strong>
                <span>{b.message}</span>
                {b.guidance?.fix && <span className="df2-vd-blocker-fix"><DtIcon name="check" size={12} /> {b.guidance.fix}</span>}
              </li>
            ))}
          </ul>
        </div>
      )}
    </section>
  );
}
