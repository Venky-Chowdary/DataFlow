import { Fragment, useEffect, useState } from "react";
import { DtIcon } from "../DtIcon";
import { Spinner } from "../LoadingState";
import { Button } from "../ui/Button";
import { explainPreflight } from "../../lib/api";
import type {
  CoercionColumn,
  PreflightGate,
  PreflightResult,
  ValidationExplanation,
  ValidationSuggestedAction,
} from "../../lib/types";

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
  destType?: string;
  validationMode?: string;
  /** Apply a one-click AI suggestion to the Studio (change type, add transform, navigate). */
  onApplyAction?: (action: ValidationSuggestedAction) => void;
}

const SEVERITY_LABEL: Record<string, string> = {
  block: "Block",
  warn: "Warn",
  ok: "OK",
};

const ACTION_ICON: Record<string, string> = {
  change_target_type: "layers",
  add_transform: "code",
  review_mappings: "sparkle",
  rerun_mapping: "transfer",
  check_connection: "server",
};

/** Per-column value-aware coercion table with expandable offending-value rows. */
function CoercionTable({ columns }: { columns: CoercionColumn[] }) {
  const [expanded, setExpanded] = useState<Set<string>>(new Set());
  const toggle = (key: string) =>
    setExpanded((prev) => {
      const next = new Set(prev);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next;
    });

  return (
    <div className="df2-vd-coerce">
      <div className="df2-vd-coerce-head">
        <DtIcon name="scan" size={15} />
        <strong>Type coercion preview</strong>
        <span>{columns.length} column{columns.length === 1 ? "" : "s"} coerced against sampled values — expand a row to see the offending values.</span>
      </div>
      <div className="df2-vd-coerce-table-wrap">
        <table className="df2-vd-coerce-table">
          <thead>
            <tr>
              <th aria-label="Expand" />
              <th>Column</th>
              <th>Source → Target</th>
              <th className="df2-vd-num">Sampled</th>
              <th className="df2-vd-num">OK</th>
              <th className="df2-vd-num">NULLed</th>
              <th className="df2-vd-num">Failed</th>
              <th>Severity</th>
            </tr>
          </thead>
          <tbody>
            {columns.map((col) => {
              const key = `${col.source}→${col.target}`;
              const nulled = (col.nulls ?? 0) + (col.sentinel_nulls ?? 0);
              const isOpen = expanded.has(key);
              const hasDetail = col.sample_failures.length > 0 || Boolean(col.suggested_fix);
              return (
                <Fragment key={key}>
                  <tr
                    className={`df2-vd-coerce-row sev-${col.severity}${hasDetail ? " has-detail" : ""}${isOpen ? " is-open" : ""}`}
                    onClick={hasDetail ? () => toggle(key) : undefined}
                    aria-expanded={hasDetail ? isOpen : undefined}
                  >
                    <td className="df2-vd-coerce-caret">
                      {hasDetail && <DtIcon name={isOpen ? "chevron-down" : "chevron-right"} size={14} />}
                    </td>
                    <td className="df2-vd-coerce-col">
                      <strong>{col.source}</strong>
                      {col.target !== col.source && <span>→ {col.target}</span>}
                    </td>
                    <td className="df2-vd-coerce-types">
                      <code>{col.source_type}</code>
                      <DtIcon name="arrow-right" size={11} />
                      <code>{col.target_type}</code>
                    </td>
                    <td className="df2-vd-num">{col.sampled.toLocaleString()}</td>
                    <td className="df2-vd-num df2-vd-ok">{col.ok.toLocaleString()}</td>
                    <td className="df2-vd-num df2-vd-nulled">{nulled.toLocaleString()}</td>
                    <td className="df2-vd-num df2-vd-failed">{col.failed.toLocaleString()}</td>
                    <td>
                      <span className={`df2-vd-sev sev-${col.severity}`}>
                        <DtIcon
                          name={col.severity === "block" ? "x" : col.severity === "warn" ? "alert" : "check"}
                          size={11}
                        />
                        {SEVERITY_LABEL[col.severity] ?? col.severity}
                      </span>
                    </td>
                  </tr>
                  {isOpen && hasDetail && (
                    <tr className={`df2-vd-coerce-detail sev-${col.severity}`}>
                      <td colSpan={8}>
                        {col.suggested_fix && (
                          <p className="df2-vd-coerce-fix">
                            <DtIcon name="sparkle" size={13} /> {col.suggested_fix}
                          </p>
                        )}
                        {col.sample_failures.length > 0 && (
                          <div className="df2-vd-coerce-samples">
                            <span className="df2-vd-coerce-samples-title">Offending values</span>
                            <table>
                              <thead>
                                <tr>
                                  <th className="df2-vd-num">Row</th>
                                  <th>Value</th>
                                  <th>Reason</th>
                                </tr>
                              </thead>
                              <tbody>
                                {col.sample_failures.map((f, i) => (
                                  <tr key={`${f.row}-${i}`}>
                                    <td className="df2-vd-num">{f.row}</td>
                                    <td><code>{f.value === "" ? "∅ empty" : f.value}</code></td>
                                    <td>{f.reason}</td>
                                  </tr>
                                ))}
                              </tbody>
                            </table>
                          </div>
                        )}
                      </td>
                    </tr>
                  )}
                </Fragment>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
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

export function ValidateDashboard({
  preflight,
  running = false,
  confidenceThreshold = 0.85,
  destType,
  validationMode,
  onApplyAction,
}: ValidateDashboardProps) {
  const [progress, setProgress] = useState(0);
  const [explain, setExplain] = useState<ValidationExplanation | null>(null);
  const [explaining, setExplaining] = useState(false);
  const [explainError, setExplainError] = useState<string | null>(null);

  // A new preflight run invalidates any prior explanation.
  useEffect(() => {
    setExplain(null);
    setExplainError(null);
  }, [preflight]);

  const runExplain = async () => {
    if (!preflight) return;
    setExplaining(true);
    setExplainError(null);
    try {
      const result = await explainPreflight({
        preflight,
        dest_type: destType,
        validation_mode: validationMode,
        use_llm: true,
      });
      setExplain(result);
    } catch (e) {
      setExplainError(e instanceof Error ? e.message : "Could not generate an explanation.");
    } finally {
      setExplaining(false);
    }
  };

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
  const sampleCompare = reconciliation?.sample_compare;
  const mismatches = sampleCompare?.mismatches ?? [];

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

      {!running && preflight && (
        <div className="df2-vd-assist">
          <div className="df2-vd-assist-head">
            <div className="df2-vd-assist-title">
              <span className="df2-vd-assist-icon"><DtIcon name="sparkle" size={16} /></span>
              <div>
                <strong>Explain &amp; fix with AI</strong>
                <span>Turn the validation result into a plain-language explanation with one-click fixes.</span>
              </div>
            </div>
            <Button
              variant={explain ? "ghost" : "primary"}
              onClick={() => void runExplain()}
              loading={explaining}
              loadingLabel="Analyzing…"
              leadingIcon={<DtIcon name="sparkle" size={14} />}
            >
              {explain ? "Re-analyze" : "Explain & fix with AI"}
            </Button>
          </div>

          {explainError && (
            <div className="df2-vd-assist-error" role="alert">
              <DtIcon name="alert" size={14} />
              <span>{explainError}</span>
            </div>
          )}

          {explaining && !explain && (
            <div className="df2-vd-assist-loading">
              <Spinner size="sm" label="" /> Reviewing gates, columns, and offending values…
            </div>
          )}

          {explain && (
            <div className="df2-vd-assist-body">
              <div className="df2-vd-assist-meta">
                <span className={`df2-vd-provider provider-${explain.assistant_provider === "deterministic" ? "det" : "llm"}`}>
                  <DtIcon name={explain.assistant_provider === "deterministic" ? "shield" : "sparkle"} size={11} />
                  {explain.assistant_provider === "deterministic" ? "deterministic" : explain.assistant_provider}
                </span>
                <span className="df2-vd-assist-summary">{explain.summary}</span>
              </div>
              {explain.narrative && (
                <div className="df2-vd-assist-narrative">
                  {explain.narrative.split("\n").filter(Boolean).map((line, i) => (
                    <p key={i}>{line}</p>
                  ))}
                </div>
              )}
              {explain.suggested_actions.length > 0 && (
                <div className="df2-vd-assist-actions">
                  <span className="df2-vd-assist-actions-title">Suggested fixes</span>
                  <div className="df2-vd-chip-row">
                    {explain.suggested_actions.map((action, i) => (
                      <button
                        key={`${action.kind}-${action.column ?? ""}-${i}`}
                        type="button"
                        className={`df2-vd-chip kind-${action.kind}`}
                        onClick={() => onApplyAction?.(action)}
                        disabled={!onApplyAction}
                        title={action.label}
                      >
                        <DtIcon name={ACTION_ICON[action.kind] ?? "sparkle"} size={13} />
                        {action.label}
                      </button>
                    ))}
                  </div>
                </div>
              )}
              {explain.suggested_actions.length === 0 && explain.passed && (
                <p className="df2-vd-assist-clean">
                  <DtIcon name="check" size={13} /> No fixes needed — all gates passed.
                </p>
              )}
            </div>
          )}
        </div>
      )}

      {!running && preflight?.coercion_report?.columns?.length ? (
        <CoercionTable columns={preflight.coercion_report.columns} />
      ) : null}

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

      {sampleCompare && !sampleCompare.skipped && (
        <div className="df2-vd-diff">
          <div className="df2-vd-diff-head">
            <DtIcon name="scan" size={15} />
            <strong>Row-level value check</strong>
            <span>
              {sampleCompare.compared.toLocaleString()} cell{sampleCompare.compared === 1 ? "" : "s"} compared, source read-back vs. destination read-back
            </span>
          </div>
          {mismatches.length === 0 ? (
            <p className="df2-vd-diff-clean">
              <DtIcon name="check" size={13} /> Every sampled value matched exactly — no drift between source and destination.
            </p>
          ) : (
            <div className="df2-vd-diff-table-wrap">
              <table className="df2-vd-diff-table">
                <thead>
                  <tr>
                    <th>Row</th>
                    <th>Column</th>
                    <th>Source value</th>
                    <th>Destination value</th>
                  </tr>
                </thead>
                <tbody>
                  {mismatches.map((m, i) => (
                    <tr key={`${m.row}-${m.source}-${i}`}>
                      <td>{m.row}</td>
                      <td title={`${m.source} → ${m.target}`}>{m.source}</td>
                      <td className="df2-vd-diff-source">{m.source_value || "—"}</td>
                      <td className="df2-vd-diff-target">{m.target_value || "—"}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
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
