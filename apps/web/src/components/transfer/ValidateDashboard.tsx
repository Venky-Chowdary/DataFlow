import { Fragment, useEffect, useMemo, useRef, useState } from "react";
import { DtIcon } from "../DtIcon";
import { Spinner } from "../LoadingState";
import { Button } from "../ui/Button";
import { explainPreflight, fetchRepairProposal, proposeRepairFromPreflight, type CellPreviewResult, type RepairMapping, type RepairProposal } from "../../lib/api";
import type {
  CoercionColumn,
  PreflightGate,
  PreflightResult,
  ValidationExplanation,
  ValidationSuggestedAction,
} from "../../lib/types";
import { isLocalPreflight } from "../../lib/localPreflight";
import {
  CORE_ENGINE_GATE_IDS,
  GATE_CATALOG,
  gateCatalogEntry,
} from "../../lib/preflightGates";
import { BadDataFixDrawer, type BadDataIssue } from "./BadDataFixDrawer";
import { LoadHistoryPanel } from "./LoadHistoryPanel";
import { RepairProposalDrawer } from "./RepairProposalDrawer";

type GateMeta = {
  key: string;
  label: string;
  icon: string;
  rule: string;
};

const GATE_META: GateMeta[] = GATE_CATALOG.map((g) => ({
  key: g.id,
  label: g.label,
  icon: g.icon,
  rule: g.rule,
}));

function metaForGate(id: string): GateMeta {
  const entry = gateCatalogEntry(id);
  return { key: entry.id, label: entry.label, icon: entry.icon, rule: entry.rule };
}

/** Core engine gates shown while Validate is pending / running. */
const CORE_ENGINE_KEYS = new Set<string>(CORE_ENGINE_GATE_IDS);
const CORE_GATE_META = GATE_META.filter((g) => CORE_ENGINE_KEYS.has(g.key));

function formatDuration(ms: number | undefined): string {
  if (ms == null || Number.isNaN(ms)) return "";
  if (ms < 10) return "<10 ms";
  if (ms < 1000) return `${Math.round(ms)} ms`;
  return `${(ms / 1000).toFixed(1)} s`;
}

function formatElapsed(ms: number): string {
  const s = ms / 1000;
  return s < 10 ? `${s.toFixed(1)} s` : `${Math.round(s)} s`;
}

function issueTextsFromDetails(details?: Record<string, unknown> | null): string[] {
  if (!details) return [];
  const typed = details.issue_texts;
  if (Array.isArray(typed)) {
    return typed.map((t) => String(t)).filter(Boolean).slice(0, 12);
  }
  const errors = details.errors;
  if (Array.isArray(errors)) {
    return errors.map((e) => {
      if (typeof e === "string") return e;
      if (e && typeof e === "object") {
        const row = e as Record<string, unknown>;
        const msg = String(row.message ?? row.error ?? row.reason ?? "");
        const col = String(row.column ?? row.source ?? row.field ?? "");
        return col && msg ? `${col}: ${msg}` : msg || JSON.stringify(e);
      }
      return String(e);
    }).filter(Boolean).slice(0, 12);
  }
  const issues = details.issues;
  if (Array.isArray(issues)) {
    return issues.map((e) => (typeof e === "string" ? e : String((e as { message?: string })?.message ?? e))).filter(Boolean).slice(0, 12);
  }
  // Privilege probe honesty on G2
  const probe = details.privilege_probe;
  if (probe && typeof probe === "object") {
    const p = probe as Record<string, unknown>;
    const lines: string[] = [];
    if (p.method) lines.push(`Probe: ${String(p.method)}`);
    if (p.status) lines.push(`Privilege status: ${String(p.status)}`);
    if (p.engine) lines.push(`Engine: ${String(p.engine)}`);
    if (p.detail && String(p.status) !== "ok") lines.push(String(p.detail));
    if (lines.length) return lines.slice(0, 8);
  }
  // G4 / G6 structured detail keys that are not named "issues"
  const extras: string[] = [];
  for (const key of ["low_confidence", "ambiguous_mappings", "unmapped", "sample_duplicates", "encoding_issues"] as const) {
    const arr = details[key];
    if (!Array.isArray(arr)) continue;
    for (const item of arr.slice(0, 6)) {
      if (typeof item === "string") extras.push(item);
      else if (item && typeof item === "object") {
        const row = item as Record<string, unknown>;
        const src = String(row.source ?? row.column ?? row.field ?? "");
        const msg = String(row.message ?? row.reason ?? row.target ?? JSON.stringify(item));
        extras.push(src ? `${src}: ${msg}` : msg);
      }
    }
  }
  return extras.filter(Boolean).slice(0, 12);
}

type PrivilegeProbeMeta = {
  status?: string;
  method?: string;
  engine?: string;
  detail?: string;
  can_write?: boolean | null;
  can_create_table?: boolean | null;
};

function privilegeProbeFromDetails(details?: Record<string, unknown> | null): PrivilegeProbeMeta | null {
  const raw = details?.privilege_probe;
  if (!raw || typeof raw !== "object") return null;
  const p = raw as Record<string, unknown>;
  return {
    status: p.status != null ? String(p.status) : undefined,
    method: p.method != null ? String(p.method) : undefined,
    engine: p.engine != null ? String(p.engine) : undefined,
    detail: p.detail != null ? String(p.detail) : undefined,
    can_write: typeof p.can_write === "boolean" ? p.can_write : p.can_write == null ? null : Boolean(p.can_write),
    can_create_table:
      typeof p.can_create_table === "boolean"
        ? p.can_create_table
        : p.can_create_table == null
          ? null
          : Boolean(p.can_create_table),
  };
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
  /** Apply strip_controls across mappings and re-run preflight. Returns what changed. */
  onStripControlChars?: () => void | Promise<RemediationOpResult | void>;
  /** True when text mappings already carry strip_controls (Execute will sanitize). */
  stripControlsApplied?: boolean;
  /** Soften to quarantine-friendly posture, strip, and re-run. Returns what changed. */
  onQuarantineAndRerun?: () => void | Promise<RemediationOpResult | void>;
  /** Cell-level will-quarantine / will-coerce preview from sample rows. */
  cellPreview?: CellPreviewResult | null;
  /** Jump back to Map so the operator can fix coerced column mappings. */
  onReviewMappings?: () => void;
  /** Open Mapping proof drawer — how columns match, confidence evidence, fidelity risks. */
  onOpenMappingProof?: () => void;
  /** Compact Map proof KPIs for Validate (exact overlaps / risks / mode). */
  mappingProofSummary?: {
    destMode?: string;
    mappedCount?: number;
    exactOverlaps?: number;
    riskCount?: number;
    reviewCount?: number;
    avgConfidence?: number;
    maxConfidence?: number;
  } | null;
  /** Trigger preflight from the dashboard (same as the rail CTA). */
  onRunPreflight?: () => void;
  /** Current Studio mappings for durable repair apply. */
  repairMappings?: RepairMapping[];
  /** After Approve & apply — merge updated mappings into Studio. */
  onRepairMappingsApplied?: (mappings: RepairMapping[]) => void;
  /** Optional job id stamped onto the repair proposal. */
  repairJobId?: string;
  /** Open an existing repair proposal (Jobs → Studio deep-link). */
  seedRepairProposalId?: string | null;
  /** Clear seed after the drawer has opened (or failed). */
  onSeedRepairConsumed?: () => void;
}

/** Plain-language report of what a Validate remediation button just did. */
export type RemediationOpResult = {
  kind: "strip_controls" | "quarantine_strip";
  /** Human title for the log. */
  title: string;
  /** Ordered steps the operator can read (what / why / next). */
  steps: string[];
  /** Columns that received strip_controls (source → target). */
  columnsChanged: string[];
  /** Encoding columns that triggered the remediation. */
  columnsFlagged?: string[];
  validationMode?: string;
};

function extractBadDataIssues(preflight: PreflightResult | null): BadDataIssue[] {
  if (!preflight) return [];
  const out: BadDataIssue[] = [];
  const pushFrom = (items: unknown[]) => {
    for (const item of items) {
      if (typeof item === "string") {
        if (/format-control|replacement character|encoding|control/i.test(item)) {
          out.push({ message: item });
        }
        continue;
      }
      if (item && typeof item === "object") {
        const row = item as Record<string, unknown>;
        const message = String(row.message ?? row.error ?? "");
        if (!message && !row.chars) continue;
        if (message && !/format-control|replacement|encoding|control/i.test(message) && !row.chars) {
          continue;
        }
        out.push({
          column: row.column != null ? String(row.column) : undefined,
          row: typeof row.row === "number" ? row.row : undefined,
          message: message || "Encoding / control-character issue",
          chars: Array.isArray(row.chars) ? row.chars.map(String) : undefined,
          sample: row.sample != null ? String(row.sample) : undefined,
        });
      }
    }
  };
  for (const b of preflight.blockers) {
    const details = b.details || {};
    if (Array.isArray(details.errors)) pushFrom(details.errors);
    if (Array.isArray(details.issues)) pushFrom(details.issues);
    if (Array.isArray(details.encoding_issues)) pushFrom(details.encoding_issues);
    if (/format-control|replacement character/i.test(b.message)) {
      out.push({ message: b.message });
    }
  }
  for (const g of preflight.gates) {
    const details = g.details || {};
    if (Array.isArray(details.encoding_issues)) pushFrom(details.encoding_issues);
    if (g.status === "block") {
      if (Array.isArray(details.errors)) pushFrom(details.errors);
      if (Array.isArray(details.issues)) pushFrom(details.issues);
    }
    if (Array.isArray(details.warnings)) pushFrom(details.warnings);
  }
  // Dedupe by message+column
  const seen = new Set<string>();
  return out.filter((i) => {
    const key = `${i.column ?? ""}|${i.message}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
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
  normalize_control_chars: "layers",
  quarantine_and_rerun: "shield",
  open_bad_data_fix: "shield",
};

/** Per-column value-aware coercion table with expandable offending-value rows. */
function CoercionTable({ columns }: { columns: CoercionColumn[] }) {
  const [expanded, setExpanded] = useState<Set<string>>(new Set());
  const [showAllConversions, setShowAllConversions] = useState(false);
  // Clean string→typed conversions (severity ok) stay collapsed by default so
  // Strip/Quarantine are not buried — detail is one click away.
  const actionable = columns.filter((c) => c.severity === "block" || c.severity === "warn");
  const clean = columns.filter((c) => c.severity === "ok");
  const visible = showAllConversions ? columns : actionable;
  const toggle = (key: string) =>
    setExpanded((prev) => {
      const next = new Set(prev);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next;
    });

  if (columns.length === 0) return null;

  return (
    <div className={`df2-vd-coerce${actionable.length === 0 ? " is-clean" : ""}`}>
      <div className="df2-vd-coerce-head">
        <DtIcon name={actionable.length ? "scan" : "check"} size={15} />
        <strong>Type coercion preview</strong>
        <span>
          {actionable.length > 0
            ? `${actionable.length} column${actionable.length === 1 ? "" : "s"} need review`
            : `${clean.length} column${clean.length === 1 ? "" : "s"} convert cleanly on the sample`}
          {clean.length > 0 && actionable.length > 0
            ? ` · ${clean.length} convert cleanly`
            : ""}
          {actionable.length > 0 ? " — expand a row for offending values / wire form." : "."}
        </span>
        {clean.length > 0 && (
          <button
            type="button"
            className="df2-btn df2-btn-ghost df2-btn-sm df2-vd-coerce-toggle"
            onClick={() => setShowAllConversions((v) => !v)}
          >
            {showAllConversions ? "Hide clean conversions" : "Show all type conversions"}
          </button>
        )}
      </div>
      {visible.length === 0 ? (
        <p className="df2-vd-coerce-empty-hint">
          No blocking coercions. Use <strong>Show all type conversions</strong> to inspect
          raw → destination wire forms (for example ISO timestamps → DATETIME).
        </p>
      ) : (
      <div className="df2-vd-coerce-table-wrap">
        <table className="df2-vd-coerce-table">
          <thead>
            <tr>
              <th aria-label="Expand" />
              <th>Column</th>
              <th>Source → Target</th>
              <th>Wire form</th>
              <th className="df2-vd-num">Sampled</th>
              <th className="df2-vd-num">OK</th>
              <th className="df2-vd-num">NULLed</th>
              <th className="df2-vd-num">Failed</th>
              <th>Severity</th>
            </tr>
          </thead>
          <tbody>
            {visible.map((col) => {
              const key = `${col.source}→${col.target}`;
              const nulled = (col.nulls ?? 0) + (col.sentinel_nulls ?? 0);
              const isOpen = expanded.has(key);
              const hasDetail =
                col.sample_failures.length > 0
                || (col.wire_examples?.length ?? 0) > 0
                || Boolean(col.suggested_fix);
              const wireHint = col.sample_wire_form
                || col.wire_examples?.[0]?.wire_form
                || null;
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
                    <td className="df2-vd-coerce-wire">
                      {wireHint ? <code title={wireHint}>{wireHint}</code> : <span className="df2-vd-muted">—</span>}
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
                        {(col.wire_normalize ?? 0) > 0 ? " · normalize" : ""}
                        {(col.wire_failures ?? 0) > 0 ? ` · ${col.wire_failures} wire fail` : ""}
                      </span>
                    </td>
                  </tr>
                  {isOpen && hasDetail && (
                    <tr className={`df2-vd-coerce-detail sev-${col.severity}`}>
                      <td colSpan={9}>
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
                                  <th>Wire</th>
                                  <th>Reason</th>
                                </tr>
                              </thead>
                              <tbody>
                                {col.sample_failures.map((f, i) => (
                                  <tr key={`${f.row}-${i}`}>
                                    <td className="df2-vd-num">{f.row}</td>
                                    <td><code>{f.value === "" ? "∅ empty" : f.value}</code></td>
                                    <td>{f.wire_form ? <code>{f.wire_form}</code> : "—"}</td>
                                    <td>{f.reason}</td>
                                  </tr>
                                ))}
                              </tbody>
                            </table>
                          </div>
                        )}
                        {(col.wire_examples?.length ?? 0) > 0 && (
                          <div className="df2-vd-coerce-samples">
                            <span className="df2-vd-coerce-samples-title">
                              Destination wire normalize (ISO → DATETIME bind)
                            </span>
                            <table>
                              <thead>
                                <tr>
                                  <th className="df2-vd-num">Row</th>
                                  <th>Raw</th>
                                  <th>Wire form</th>
                                </tr>
                              </thead>
                              <tbody>
                                {(col.wire_examples ?? []).map((f, i) => (
                                  <tr key={`w-${f.row}-${i}`}>
                                    <td className="df2-vd-num">{f.row}</td>
                                    <td><code>{f.value}</code></td>
                                    <td><code>{f.wire_form ?? "—"}</code></td>
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
      )}
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
  onStripControlChars,
  stripControlsApplied = false,
  onQuarantineAndRerun,
  cellPreview = null,
  onReviewMappings,
  onOpenMappingProof,
  mappingProofSummary = null,
  onRunPreflight,
  repairMappings = [],
  onRepairMappingsApplied,
  repairJobId = "",
  seedRepairProposalId = null,
  onSeedRepairConsumed,
}: ValidateDashboardProps) {
  const [elapsedMs, setElapsedMs] = useState(0);
  const [revealCount, setRevealCount] = useState(0);
  const [explain, setExplain] = useState<ValidationExplanation | null>(null);
  const [explaining, setExplaining] = useState(false);
  const [repairOpen, setRepairOpen] = useState(false);
  const [repairProposal, setRepairProposal] = useState<RepairProposal | null>(null);
  const [repairBusy, setRepairBusy] = useState(false);
  const [explainError, setExplainError] = useState<string | null>(null);
  const [badDataOpen, setBadDataOpen] = useState(false);
  const [remediating, setRemediating] = useState(false);
  const [assistExpanded, setAssistExpanded] = useState(true);
  const [copiedRunId, setCopiedRunId] = useState(false);
  const [remediationLog, setRemediationLog] = useState<
    Array<{ at: string; action: string; detail: string; outcome: string; steps?: string[] }>
  >([]);
  const pendingVerifyRef = useRef(false);
  const verifiedRunRef = useRef<string | null>(null);
  const lastOpRef = useRef<RemediationOpResult | null>(null);
  const badDataIssues = useMemo(() => extractBadDataIssues(preflight), [preflight]);
  const hasEncodingIssue = badDataIssues.length > 0;
  const runId = preflight?.run_id;

  const typeMismatchColumns = useMemo(() => {
    const found: Array<{ source: string; target: string }> = [];
    const seen = new Set<string>();
    const texts: string[] = [];
    for (const b of preflight?.blockers || []) {
      texts.push(b.message || "");
      const details = b.details as { errors?: unknown[]; issues?: unknown[]; issue_texts?: unknown[] } | undefined;
      for (const list of [details?.errors, details?.issues, details?.issue_texts]) {
        for (const item of list || []) {
          texts.push(typeof item === "string" ? item : JSON.stringify(item));
        }
      }
    }
    for (const g of preflight?.gates || []) {
      if (g.status === "block") texts.push(g.message || "");
    }
    const re = /([A-Za-z_][\w]*)\s*\([^)]+\)\s*→\s*([A-Za-z_][\w]*)\s*\([^)]+\)/g;
    for (const text of texts) {
      let m: RegExpExecArray | null;
      while ((m = re.exec(text))) {
        const key = `${m[1]}→${m[2]}`;
        if (seen.has(key)) continue;
        seen.add(key);
        found.push({ source: m[1], target: m[2] });
      }
    }
    return found;
  }, [preflight?.blockers, preflight?.gates]);

  const isTypeMismatchBlock = typeMismatchColumns.length > 0
    || Boolean(
      preflight?.blockers.some((b) =>
        /invalid (decimal|integer|boolean)|cannot be cast|does not safely become|lossy type/i.test(b.message),
      ),
    );
  const isPrivilegeBlock = Boolean(
    preflight?.blockers.some((b) => {
      const probe = privilegeProbeFromDetails(b.details);
      return (
        (b.id === "g2_destination" || /g2_destination/i.test(b.id || ""))
        && (probe?.status === "denied"
          || /privilege|INSERT|CREATE|ACL|IAM|PutObject|has_privileges|GRANT/i.test(b.message || ""))
      );
    })
    || preflight?.gates.some((g) => {
      if (g.id !== "g2_destination" || g.status !== "block") return false;
      const probe = privilegeProbeFromDetails(g.details);
      return probe?.status === "denied"
        || /privilege|INSERT|CREATE|ACL|IAM|PutObject|has_privileges|GRANT/i.test(g.message || "");
    }),
  );
  const isConnectionBlock = Boolean(
    !isPrivilegeBlock && (
      preflight?.blockers.some((b) =>
        /g1_source/i.test(b.id || "")
        || /authentication failed|destination error|source error|not reachable|connection refused|credential/i.test(b.message || ""),
      )
      || preflight?.gates.some((g) =>
        (g.id === "g1_source")
        && g.status === "block",
      )
      || preflight?.gates.some((g) =>
        g.id === "g2_destination"
        && g.status === "block"
        && !privilegeProbeFromDetails(g.details)?.status
        && /not reachable|destination error|authentication|connection refused/i.test(g.message || ""),
      )
    ),
  );

  const pushRemediation = (action: string, detail: string, outcome: string, steps?: string[]) => {
    setRemediationLog((prev) => [
      {
        at: new Date().toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" }),
        action,
        detail,
        outcome,
        steps,
      },
      ...prev,
    ].slice(0, 8));
  };
  const encodingBlocks = Boolean(
    preflight?.blockers.some((b) => /format-control|replacement character|encoding/i.test(b.message))
    || preflight?.gates.some((g) => g.status === "block" && /format-control|replacement|encoding/i.test(g.message)),
  );
  const showEncodingRemediation = !isTypeMismatchBlock && !isConnectionBlock && !isPrivilegeBlock && (hasEncodingIssue || encodingBlocks);

  // Auto-open the Fix bad data drawer when dry-run is blocked by encoding/control chars.
  useEffect(() => {
    if (!running && encodingBlocks && hasEncodingIssue) {
      setBadDataOpen(true);
    }
  }, [running, encodingBlocks, hasEncodingIssue, preflight?.passed_count, preflight?.blockers?.length]);

  // Jobs → Studio: open a durable repair proposal in the Validate drawer.
  useEffect(() => {
    if (!seedRepairProposalId) return;
    let cancelled = false;
    void (async () => {
      try {
        const proposal = await fetchRepairProposal(seedRepairProposalId);
        if (cancelled) return;
        // Skip terminal proposals — they would open a no-op drawer.
        if (proposal.status === "applied" || proposal.status === "rejected" || proposal.status === "failed") {
          pushRemediation(
            "Repair already decided",
            `${proposal.id} · ${proposal.status}`,
            proposal.status,
          );
          return;
        }
        setRepairProposal(proposal);
        setRepairOpen(true);
        pushRemediation(
          "Opened repair from Jobs",
          proposal.summary || proposal.id,
          proposal.status,
        );
      } catch (e) {
        if (!cancelled) {
          pushRemediation(
            "Could not open repair proposal",
            (e as Error).message || seedRepairProposalId,
            "Failed",
          );
        }
      } finally {
        if (!cancelled) onSeedRepairConsumed?.();
      }
    })();
    return () => {
      cancelled = true;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [seedRepairProposalId]);

  // A new preflight run invalidates any prior explanation.
  useEffect(() => {
    setExplain(null);
    setExplainError(null);
    setAssistExpanded(true);
  }, [preflight?.run_id]);

  // After a remediation re-run, close out "waiting for re-validation" entries.
  useEffect(() => {
    if (!preflight || running || !pendingVerifyRef.current) return;
    const runKey = preflight.run_id || `${preflight.passed_count}-${preflight.total_gates}-${preflight.readiness_score}`;
    if (verifiedRunRef.current === runKey) return;
    verifiedRunRef.current = runKey;
    pendingVerifyRef.current = false;
    const dry = preflight.gates?.find((g) => /dry_run|integrity/i.test(g.id));
    const cleared = Boolean(preflight.passed || dry?.status === "pass");
    const outcome = cleared ? "Verified OK — ready to Execute" : "Still blocked — remap columns on Map";
    const op = lastOpRef.current;
    const resultSteps: string[] = [];
    if (op?.steps?.length) {
      resultSteps.push(...op.steps);
    }
    if (cleared) {
      resultSteps.push(
        `Re-validation: ${preflight.passed_count ?? 0}/${preflight.total_gates ?? 0} gates passed.`,
        op?.kind === "strip_controls" || op?.kind === "quarantine_strip"
          ? "Jobs quarantine stays empty unless cells still fail during Execute — Strip cleaned them before write."
          : "Execute is unlocked when all gates pass.",
      );
    } else {
      resultSteps.push(
        `Still blocked: ${dry?.message || "see Validation rules"}.`,
        "Quarantine/Strip cannot fix wrong column type mappings — use Map.",
      );
    }
    const detail = cleared
      ? (op
        ? `${op.title} succeeded. ${op.columnsChanged.length} mapping(s) now use strip_controls.`
        : "Dry-run / integrity now passes — Execute unlocks when all gates pass.")
      : `Dry-run still blocked: ${dry?.message || "see Validation rules"}.`;
    setRemediationLog((prev) => {
      const next = prev.map((row, idx) =>
        idx === 0 && /waiting for re-validation/i.test(row.outcome)
          ? { ...row, detail, outcome, steps: resultSteps }
          : row,
      );
      if (next[0]?.outcome === outcome && /waiting for re-validation/i.test(prev[0]?.outcome || "")) {
        return next;
      }
      return [
        {
          at: new Date().toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" }),
          action: "Re-validation result",
          detail,
          outcome,
          steps: resultSteps,
        },
        ...next,
      ].slice(0, 8);
    });
  }, [preflight, running]);

  const copyRunId = async () => {
    if (!runId) return;
    try {
      await navigator.clipboard.writeText(runId);
      setCopiedRunId(true);
      window.setTimeout(() => setCopiedRunId(false), 1600);
    } catch {
      /* ignore */
    }
  };

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

  // When Validate fails, auto-run explain so Strip / Quarantine / widen chips
  // appear immediately — operators should not have to click "Run analysis".
  useEffect(() => {
    if (running || !preflight || preflight.passed || explaining || explain) return;
    void runExplain();
    // eslint-disable-next-line react-hooks/exhaustive-deps -- only on new failed preflight
  }, [preflight?.run_id, preflight?.passed, running]);

  // Honest timer: wall-clock elapsed while the API runs — never invent gate steps.
  useEffect(() => {
    if (!running) {
      setElapsedMs(0);
      return;
    }
    const t0 = performance.now();
    setElapsedMs(0);
    const timer = window.setInterval(() => {
      setElapsedMs(performance.now() - t0);
    }, 100);
    return () => window.clearInterval(timer);
  }, [running]);

  // Show every gate result as soon as the engine returns — operators must be able
  // to read rules/status before Execute. duration_ms still appears on each card.
  useEffect(() => {
    if (running || !preflight?.gates?.length) {
      setRevealCount(0);
      return;
    }
    setRevealCount(preflight.gates.length);
  }, [running, preflight?.run_id, preflight?.gates]);

  const proof = preflight?.proof_bundle;
  const decision = proof?.transfer_decision?.decision
    ?? (preflight?.passed ? "approve" : preflight ? "review" : "pending");
  const readiness = preflight?.readiness_score ?? 0;
  const totalGates = preflight?.total_gates || GATE_META.length;
  const passedCount = preflight?.passed_count ?? 0;

  const gateByKey = new Map<string, PreflightGate>();
  for (const gate of preflight?.gates ?? []) {
    gateByKey.set(gate.id, gate);
    gateByKey.set(metaForGate(gate.id).key, gate);
  }
  const blockedCount = (preflight?.gates ?? []).filter((g) => g.status === "block").length;
  const skippedCount = (preflight?.gates ?? []).filter((g) => g.status === "skip").length;
  /** Prefer live engine gates so PENDING cards aren't shown for rules that never ran. */
  const displayGates: GateMeta[] = running
    ? CORE_GATE_META
    : (preflight?.gates?.length
      ? preflight.gates.map((g) => metaForGate(g.id))
      : CORE_GATE_META);

  const decisionTone = decision === "block"
    ? "block"
    : decision === "review"
      ? "review"
      : decision === "pending"
        ? "pending"
        : "approve";
  const heroTone = running ? "live" : preflight ? decisionTone : "idle";

  const semantic = proof?.semantic_mapping_score ?? 0;
  const quality = proof?.quality_score ?? 0;
  const complianceRisk = proof?.compliance?.risk_score ?? 0;
  const qualityGrade = (proof?.quality_grade ?? "").toLowerCase();
  const confidenceBand = (proof?.confidence_band ?? "").toLowerCase();
  const localPreflight = isLocalPreflight(preflight);
  const proofWarnings = proof?.transfer_decision?.warnings ?? [];
  const reconciliation = proof?.reconciliation;
  const sampleCompare = reconciliation?.sample_compare;
  const mismatches = sampleCompare?.mismatches ?? [];
  const dryGate = preflight?.gates?.find((g) => /dry_run|integrity/i.test(g.id));
  const sampleScanned = Number(dryGate?.details?.sample_rows_scanned ?? dryGate?.details?.sample_size ?? 0) || null;
  const engineMsTotal = (preflight?.gates ?? []).reduce((sum, g) => sum + (Number(g.duration_ms) || 0), 0);

  // While validating: all gates pending/queued — never fake a green "pass" mid-flight.
  // After results: reveal in order using real duration_ms.
  const statusForGate = (
    meta: GateMeta,
    index: number,
  ): { status: string; message: string; issues: string[]; durationMs?: number; privilegeProbe: PrivilegeProbeMeta | null } => {
    const gate = gateByKey.get(meta.key) ?? gateByKey.get(meta.key.replace(/^g\d+_/, ""));
    if (running) {
      return {
        status: "pending",
        message: "Queued — engine returns all gate results when the pass finishes",
        issues: [],
        privilegeProbe: null,
      };
    }
    if (gate) {
      const revealed = index < revealCount;
      if (!revealed && revealCount < (preflight?.gates?.length ?? 0)) {
        return { status: "pending", message: "Result ready — revealing…", issues: [], privilegeProbe: null };
      }
      const privilegeProbe = privilegeProbeFromDetails(gate.details);
      const issues = gate.status === "block" ? issueTextsFromDetails(gate.details) : [];
      // On pass, still surface probe method/status as non-blocking issues for G2 honesty.
      if (gate.status === "pass" && privilegeProbe?.method && meta.key === "g2_destination") {
        const soft: string[] = [`Probe: ${privilegeProbe.method}`];
        if (privilegeProbe.status === "unavailable" && privilegeProbe.detail) {
          soft.push(privilegeProbe.detail);
        }
        return {
          status: gate.status,
          message: gate.message,
          issues: soft,
          durationMs: gate.duration_ms,
          privilegeProbe,
        };
      }
      return {
        status: gate.status,
        message: gate.message,
        issues,
        durationMs: gate.duration_ms,
        privilegeProbe,
      };
    }
    return { status: "pending", message: "Awaiting validation run.", issues: [], privilegeProbe: null };
  };

  const runStrip = async () => {
    if (!onStripControlChars) return;
    setRemediating(true);
    pendingVerifyRef.current = true;
    const flagged = badDataIssues.map((i) => i.column).filter(Boolean);
    try {
      const result = await onStripControlChars();
      if (result) {
        lastOpRef.current = result;
        pushRemediation(
          result.title,
          result.columnsChanged.length
            ? `Updated ${result.columnsChanged.length} mapping(s): ${result.columnsChanged.slice(0, 12).join(", ")}${result.columnsChanged.length > 12 ? "…" : ""}.`
            : "No text mappings needed strip_controls (typed casts left unchanged).",
          "Applied — waiting for re-validation",
          result.steps,
        );
      } else {
        pushRemediation(
          "Strip control characters",
          flagged.length
            ? `Removing format-control chars from flagged columns: ${flagged.slice(0, 8).join(", ")}${flagged.length > 8 ? "…" : ""}.`
            : "Applied strip_controls on text mappings and re-running validation.",
          "Applied — waiting for re-validation",
          [
            "Set transform = strip_controls on non-typed mappings.",
            "Re-run Validate with the updated mappings.",
            "Jobs quarantine only if cells still fail at write after Strip.",
          ],
        );
      }
      setBadDataOpen(false);
    } finally {
      setRemediating(false);
    }
  };

  const runQuarantine = async () => {
    if (!onQuarantineAndRerun) return;
    setRemediating(true);
    pendingVerifyRef.current = true;
    const flagged = badDataIssues.map((i) => i.column).filter(Boolean);
    try {
      const result = await onQuarantineAndRerun();
      if (result) {
        lastOpRef.current = result;
        pushRemediation(
          result.title,
          [
            result.validationMode ? `Mode: ${result.validationMode}.` : null,
            result.columnsChanged.length
              ? `${result.columnsChanged.length} mapping(s) updated: ${result.columnsChanged.slice(0, 12).join(", ")}${result.columnsChanged.length > 12 ? "…" : ""}.`
              : "No strip_controls changes (typed casts left as-is).",
          ].filter(Boolean).join(" "),
          "Applied — waiting for re-validation",
          result.steps,
        );
      } else {
        // Handler navigated to Map (type mismatch) — no strip applied.
        pendingVerifyRef.current = false;
        pushRemediation(
          "Quarantine + strip controls",
          flagged.length
            ? `Could not auto-fix flagged columns (${flagged.slice(0, 6).join(", ")}). Remap types on Map instead.`
            : "Blocked by a type/mapping issue — quarantine cannot change column types. Open Map to remap.",
          "Redirected to Map",
          [
            "Quarantine/Strip only sanitize encoding (U+200B / control chars).",
            "Wrong target types (e.g. text → NUMBER) must be remapped on Map, then Validate again.",
          ],
        );
      }
      setBadDataOpen(false);
    } finally {
      setRemediating(false);
    }
  };

  const proposeDurableRepair = async () => {
    if (!preflight) return;
    setRepairBusy(true);
    try {
      const proposal = await proposeRepairFromPreflight({
        preflight: preflight as unknown as Record<string, unknown>,
        coercion_report: (preflight.coercion_report || {}) as unknown as Record<string, unknown>,
        job_id: repairJobId,
      });
      setRepairProposal(proposal);
      setRepairOpen(true);
    } catch (e) {
      pushRemediation(
        "Repair propose failed",
        (e as Error).message || "Could not create repair proposal",
        "Failed",
      );
    } finally {
      setRepairBusy(false);
    }
  };

  const handleSuggestedAction = (action: ValidationSuggestedAction) => {
    if (action.kind === "normalize_control_chars") {
      if (onStripControlChars) {
        void runStrip();
        return;
      }
      setBadDataOpen(true);
      return;
    }
    if (action.kind === "quarantine_and_rerun") {
      if (onQuarantineAndRerun) {
        void runQuarantine();
        return;
      }
      setBadDataOpen(true);
      return;
    }
    if (action.kind === "open_bad_data_fix") {
      setBadDataOpen(true);
      return;
    }
    pendingVerifyRef.current = true;
    pushRemediation(
      action.label || action.kind,
      [
        action.column ? `Column: ${action.column}` : null,
        action.target ? `Target: ${action.target}` : null,
        action.transform ? `Transform: ${action.transform}` : null,
        action.to_type ? `Type → ${action.to_type}` : null,
      ].filter(Boolean).join(" · ") || "AI suggested fix applied to mappings.",
      "Applied — re-running validation",
      [
        `Action: ${action.kind.replace(/_/g, " ")}.`,
        action.column || action.target
          ? `Scope: ${[action.column, action.target].filter(Boolean).join(" → ")}.`
          : "Scope: matching Studio mappings.",
        action.to_type ? `Set destination type to ${action.to_type}.` : null,
        action.transform ? `Set transform to ${action.transform}.` : null,
        "Re-validate after apply — Execute unlocks only when gates pass.",
      ].filter(Boolean) as string[],
    );
    onApplyAction?.(action);
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
              strokeDasharray={`${((running ? Math.min(92, 18 + elapsedMs / 80) : readiness) / 100) * 326.7} 326.7`}
              transform="rotate(-90 60 60)"
            />
          </svg>
          <div className="df2-vd-hero-ring-label">
            <strong>{running ? formatElapsed(elapsedMs) : readiness}<small>{running ? "" : "%"}</small></strong>
            <span>{running ? "elapsed" : "ready"}</span>
          </div>
        </div>

        <div className="df2-vd-hero-copy">
          <div className="df2-vd-hero-head">
            <span className={`df2-vd-decision df2-vd-decision-${decisionTone}`}>
              <DtIcon
                name={
                  running ? "activity"
                    : decision === "approve" ? "check"
                      : decision === "block" ? "x"
                        : decision === "pending" ? "gate"
                          : "shield"
                }
                size={13}
              />
              {running ? "VALIDATING" : decision === "pending" ? "NOT RUN" : decision.toUpperCase()}
            </span>
            <h3>
              {running
                ? "Engine running G1–G8…"
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

          {!running && preflight && (qualityGrade || confidenceBand) && (
            <div className="df2-vd-proof-chips" aria-label="Proof grade">
              {qualityGrade ? (
                <span className={`df2-vd-proof-chip grade-${qualityGrade}`} title="Overall proof quality grade from the engine">
                  Quality grade · {qualityGrade}
                </span>
              ) : null}
              {confidenceBand ? (
                <span className={`df2-vd-proof-chip band-${confidenceBand}`} title="Mapping / evidence confidence band">
                  Confidence · {confidenceBand}
                </span>
              ) : null}
              {typeof quality === "number" && quality > 0 ? (
                <span className="df2-vd-proof-chip is-score" title="Numeric quality score (0–1)">
                  Score · {(quality * 100).toFixed(0)}%
                </span>
              ) : null}
              {typeof semantic === "number" && semantic > 0 ? (
                <span className="df2-vd-proof-chip is-score" title="Semantic mapping score">
                  Semantic · {(semantic * 100).toFixed(0)}%
                </span>
              ) : null}
            </div>
          )}

          <p className="df2-vd-hero-summary">
            {running
              ? "Real preflight is evaluating source, destination, schema, mapping confidence, dry-run sample, DDL, capacity, and reconcile plan. Progress is wall-clock time — not a fake step animation."
              : proof?.evidence_summary ?? "Every rule below runs before a single row is written. Nothing transfers until the gates you require pass."}
          </p>

          {running && (
            <div className="df2-vd-progress is-indeterminate" role="status" aria-live="polite">
              <span className="df2-vd-progress-fill" style={{ width: "40%" }} />
            </div>
          )}
          {!running && preflight && engineMsTotal > 0 && (
            <p className="df2-vd-hero-engine-meta">
              Engine reported {formatDuration(engineMsTotal)} across {preflight.gates.length} gates
              {sampleScanned != null && sampleScanned > 0
                ? ` · dry-run sampled ${sampleScanned.toLocaleString()} preview rows (must cover the same integrity window Execute uses; full table proven after write in Job Theater)`
                : " · dry-run uses the Transfer Studio preview sample, not the full table"}
            </p>
          )}
          {!running && preflight?.passed && !stripControlsApplied && onStripControlChars && (
            <p className="df2-vd-hero-engine-meta" role="status">
              Text mappings do not yet include <code>strip_controls</code>. If a prior job failed on
              U+200B / format-control characters, click <strong>Strip controls &amp; re-run</strong> before
              Execute — green Validate on a clean preview is not the same as sanitizing the full load.
            </p>
          )}
        </div>
      </header>

      {!running && localPreflight && (
        <div className="df2-vd-local-banner" role="status" aria-label="Local browser validation">
          <DtIcon name="shield" size={16} />
          <div>
            <strong>Local browser validation only</strong>
            <p>
              Destination reachability, DDL, and reconciliation were not executed against a live system.
              Treat this as demo-grade until the API runs the same route.
            </p>
            {proofWarnings.length > 0 && (
              <ul>
                {proofWarnings.map((w) => (
                  <li key={w}>{w}</li>
                ))}
              </ul>
            )}
          </div>
        </div>
      )}

      {/* Context-aware remediation: type mismatches → Remap/Widen; encoding → Strip/Quarantine. */}
      {!running && preflight && !preflight.passed && (
        <div className="df2-vd-assist-actions df2-vd-assist-remediate df2-vd-remediate-bar" aria-label="Suggested fixes">
          <span className="df2-vd-assist-actions-title">Suggested fixes</span>
          <div className="df2-vd-chip-row">
            {isPrivilegeBlock && (
              <button
                type="button"
                className="df2-vd-chip kind-check_connection"
                disabled={remediating}
                onClick={() => {
                  window.location.hash = "#/connectors";
                }}
              >
                <DtIcon name="shield" size={13} />
                Grant write privilege
              </button>
            )}
            {isPrivilegeBlock && onRunPreflight && (
              <button
                type="button"
                className="df2-vd-chip kind-rerun"
                disabled={remediating || running}
                onClick={() => void onRunPreflight()}
              >
                <DtIcon name="activity" size={13} />
                Re-validate after grant
              </button>
            )}
            {isConnectionBlock && (
              <button
                type="button"
                className="df2-vd-chip kind-check_connection"
                disabled={remediating}
                onClick={() => {
                  window.location.hash = "#/connectors";
                }}
              >
                <DtIcon name="server" size={13} />
                Fix connector credentials
              </button>
            )}
            {isConnectionBlock && onRunPreflight && (
              <button
                type="button"
                className="df2-vd-chip kind-rerun"
                disabled={remediating || running}
                onClick={() => void onRunPreflight()}
              >
                <DtIcon name="activity" size={13} />
                Re-test &amp; re-validate
              </button>
            )}
            {isTypeMismatchBlock && typeMismatchColumns.slice(0, 4).map((col) => (
              <button
                key={`${col.source}-${col.target}`}
                type="button"
                className="df2-vd-chip kind-change_target_type"
                disabled={remediating || !onApplyAction}
                onClick={() =>
                  onApplyAction?.({
                    kind: "change_target_type",
                    label: `Remap ${col.source} → VARCHAR`,
                    column: col.source,
                    target: col.target,
                    to_type: "VARCHAR",
                  })
                }
              >
                <DtIcon name="layers" size={13} />
                Remap {col.source} → VARCHAR
              </button>
            ))}
            {isTypeMismatchBlock && onReviewMappings && (
              <button
                type="button"
                className="df2-vd-chip kind-review_mappings"
                onClick={onReviewMappings}
                disabled={remediating}
              >
                <DtIcon name="layers" size={13} />
                Review mappings
              </button>
            )}
            {showEncodingRemediation && onStripControlChars && (
              <button
                type="button"
                className="df2-vd-chip kind-normalize_control_chars"
                onClick={() => void runStrip()}
                disabled={remediating}
              >
                <DtIcon name="layers" size={13} />
                Strip controls &amp; re-run
              </button>
            )}
            {showEncodingRemediation && onQuarantineAndRerun && (
              <button
                type="button"
                className="df2-vd-chip kind-quarantine_and_rerun"
                onClick={() => void runQuarantine()}
                disabled={remediating}
              >
                <DtIcon name="shield" size={13} />
                Quarantine &amp; re-run
              </button>
            )}
            {showEncodingRemediation && (
              <button
                type="button"
                className="df2-vd-chip kind-open_bad_data_fix"
                onClick={() => setBadDataOpen(true)}
                disabled={remediating}
              >
                <DtIcon name="shield" size={13} />
                Fix bad data…
              </button>
            )}
            {onOpenMappingProof && (
              <button
                type="button"
                className="df2-vd-chip kind-mapping_proof"
                onClick={onOpenMappingProof}
                disabled={remediating}
              >
                <DtIcon name="sparkle" size={13} />
                Mapping proof
              </button>
            )}
            {!isTypeMismatchBlock && onReviewMappings && (
              <button
                type="button"
                className="df2-vd-chip kind-review_mappings"
                onClick={onReviewMappings}
                disabled={remediating}
              >
                <DtIcon name="layers" size={13} />
                Review mappings
              </button>
            )}
          </div>
          <p className="df2-vd-cell-preview-hint">
            {isPrivilegeBlock
              ? "Write privilege is denied (or CREATE is missing). Grant the privilege named in the G2 gate (INSERT/CREATE, ACL, IAM, index/write) on the destination — then Re-validate. Re-testing connector login alone will not fix a privilege deny."
              : isConnectionBlock
              ? "Destination (or source) authentication failed. Open Connectors for this saved connection, click Test until it passes (connection string or username/password — one place only), then return here and Re-validate. Strip/Quarantine cannot fix credentials."
              : isTypeMismatchBlock
              ? "This block is a type mismatch (e.g. text → NUMBER). Remap/Widen to VARCHAR — Strip controls and Quarantine cannot change column types. After Remap, Validate again; Execute unlocks when gates pass."
              : showEncodingRemediation
                ? "Strip controls removes format-control characters, then re-validates. Quarantine keeps unfit cells out of the destination after Validate passes (never silent drop)."
                : "Open Review mappings or Mapping proof. Strip/Quarantine only apply to encoding issues."}
          </p>
        </div>
      )}

      {remediationLog.length > 0 && (
        <div className="df2-vd-remediation-log" aria-label="What was fixed">
          <div className="df2-vd-remediation-log-head">
            <DtIcon name="check" size={14} />
            <strong>What we changed</strong>
            <span>Exact remediations applied in this Validate session</span>
          </div>
          <ol>
            {remediationLog.map((entry, i) => (
              <li key={`${entry.at}-${entry.action}-${i}`}>
                <time>{entry.at}</time>
                <div>
                  <strong>{entry.action}</strong>
                  <p>{entry.detail}</p>
                  {entry.steps && entry.steps.length > 0 && (
                    <ul className="df2-vd-remediation-steps">
                      {entry.steps.map((step, si) => (
                        <li key={si}>{step}</li>
                      ))}
                    </ul>
                  )}
                  <em>{entry.outcome}</em>
                </div>
              </li>
            ))}
          </ol>
        </div>
      )}

      {cellPreview && (cellPreview.quarantine_count > 0 || cellPreview.coerce_count > 0) && (() => {
        const quarantineOnly = cellPreview.quarantine_count > 0;
        const coerceOnly = cellPreview.coerce_count > 0 && cellPreview.quarantine_count === 0;
        const coercedPairs = Array.from(
          new Map(
            cellPreview.cells
              .filter((c) => c.status === "coerced")
              .map((c) => [`${c.source}→${c.target}`, { source: c.source, target: c.target }]),
          ).values(),
        ).slice(0, 6);
        return (
          <div
            className={`df2-vd-cell-preview${coerceOnly ? " is-info" : " is-warn"}`}
            aria-label="Sample cell transform preview"
          >
            <div className="df2-vd-cell-preview-head">
              <strong>{coerceOnly ? "Type coercions in sample" : "Sample cells need attention"}</strong>
              <span>
                {cellPreview.quarantine_count} will quarantine · {cellPreview.coerce_count} will coerce ·{" "}
                {cellPreview.sample_rows_scanned} rows scanned
              </span>
            </div>
            <p className="df2-vd-cell-preview-hint">
              {coerceOnly ? (
                <>
                  This is <strong>not a failed validation</strong> and not silent data loss.
                  Coerce means a value will be converted to fit the destination type
                  (example: boolean <code>false</code> written into a text column becomes the string <code>&quot;false&quot;</code>).
                  {!preflight && (
                    <> The ring shows 0% ready because you have not run preflight yet — use <strong>Run preflight</strong> to score the gates.</>
                  )}
                </>
              ) : (
                <>
                  Quarantine isolates unfit cells for Inspect / CSV export after Run — they are not silently deleted.
                  Fix mappings or types below, then re-run preflight.
                </>
              )}
            </p>
            {coercedPairs.length > 0 && (
              <div className="df2-vd-cell-preview-pairs">
                <span className="df2-vd-assist-actions-title">Columns being coerced</span>
                <div className="df2-vd-chip-row">
                  {coercedPairs.map((p) => (
                    <span key={`${p.source}-${p.target}`} className="df2-vd-chip is-static">
                      {p.source} → {p.target}
                    </span>
                  ))}
                </div>
              </div>
            )}
            <ul className="df2-vd-cell-preview-list">
              {cellPreview.cells.slice(0, 8).map((cell, i) => (
                <li key={`${cell.source}-${cell.row}-${i}`} className={`df2-vd-cell-preview-item is-${cell.status}`}>
                  <span className="df2-vd-cell-preview-status">{cell.status}</span>
                  <span>
                    row {cell.row + 1} · {cell.source}→{cell.target}
                    {cell.message ? ` — ${cell.message}` : ""}
                    {cell.coerced != null ? ` → ${cell.coerced}` : ""}
                  </span>
                  {cell.raw ? <code title={cell.raw}>{cell.raw.slice(0, 48)}</code> : null}
                </li>
              ))}
            </ul>
            <div className="df2-vd-cell-preview-actions">
              {onOpenMappingProof && (
                <button type="button" className="df2-btn df2-btn-sm df2-btn-secondary" onClick={onOpenMappingProof}>
                  <DtIcon name="sparkle" size={14} /> Mapping proof
                </button>
              )}
              {onReviewMappings && (
                <button type="button" className="df2-btn df2-btn-sm df2-btn-secondary" onClick={onReviewMappings}>
                  <DtIcon name="layers" size={14} /> Review mappings
                </button>
              )}
              {onRunPreflight && !preflight && !running && (
                <button type="button" className="df2-btn df2-btn-sm df2-btn-primary" onClick={onRunPreflight}>
                  <DtIcon name="gate" size={14} /> Run preflight
                </button>
              )}
              {onStripControlChars && showEncodingRemediation && (
                <button
                  type="button"
                  className="df2-btn df2-btn-sm df2-btn-ghost"
                  onClick={() => void runStrip()}
                  disabled={remediating}
                >
                  <DtIcon name="layers" size={14} /> Strip controls &amp; re-run
                </button>
              )}
              {onQuarantineAndRerun && showEncodingRemediation && (
                <button
                  type="button"
                  className="df2-btn df2-btn-sm df2-btn-ghost"
                  onClick={() => void runQuarantine()}
                  disabled={remediating}
                >
                  <DtIcon name="shield" size={14} /> Quarantine &amp; re-check
                </button>
              )}
            </div>
          </div>
        );
      })()}

      {!running && mappingProofSummary && onOpenMappingProof && (
        <div className="df2-vd-map-proof-card" aria-label="Column match proof summary">
          <div className="df2-vd-map-proof-card-head">
            <div>
              <strong>Column matches</strong>
              <span>
                {mappingProofSummary.destMode === "create_new"
                  ? "Create-new — DDL on first write"
                  : "Matched to destination schema"}
                {" · "}
                every pair has confidence evidence and fidelity risks
              </span>
            </div>
            <button type="button" className="df2-btn df2-btn-sm df2-btn-secondary" onClick={onOpenMappingProof}>
              <DtIcon name="sparkle" size={14} /> Open mapping proof
            </button>
          </div>
          <div className="df2-vd-map-proof-kpis">
            <div>
              <span>Pairs</span>
              <strong>{mappingProofSummary.mappedCount ?? 0}</strong>
            </div>
            <div>
              <span>Exact overlaps</span>
              <strong>{mappingProofSummary.exactOverlaps ?? 0}</strong>
            </div>
            <div>
              <span>Avg / max conf</span>
              <strong>
                {mappingProofSummary.avgConfidence != null
                  ? `${Math.round(mappingProofSummary.avgConfidence * 100)}%`
                  : "—"}
                {" / "}
                {mappingProofSummary.maxConfidence != null
                  ? `${Math.round(mappingProofSummary.maxConfidence * 100)}%`
                  : "—"}
              </strong>
            </div>
            <div>
              <span>Risks / review</span>
              <strong>
                {mappingProofSummary.riskCount ?? 0} / {mappingProofSummary.reviewCount ?? 0}
              </strong>
            </div>
          </div>
        </div>
      )}

      {!running && preflight && (
        <div className="df2-vd-metrics">
          <Ring value={readiness} label="Readiness" sub="Overall route score" tone={heroTone} />
          <Ring value={semantic * 100} label="Semantic mapping" sub="Column match confidence" tone="approve" />
          <Ring value={quality * 100} label="Data quality" sub="Profiled quality grade" tone="approve" />
          <Ring value={(1 - complianceRisk) * 100} label="Compliance" sub={`Risk ${complianceRisk.toFixed(2)}`} tone={complianceRisk > 0.4 ? "review" : "approve"} />
        </div>
      )}

      {!running && preflight && (
        <div className={`df2-vd-assist${assistExpanded ? " is-expanded" : " is-collapsed"}`}>
          <div className="df2-vd-assist-head">
            <button
              type="button"
              className="df2-vd-assist-toggle"
              onClick={() => setAssistExpanded((v) => !v)}
              aria-expanded={assistExpanded}
              aria-controls="df2-vd-assist-panel"
              id="df2-vd-assist-trigger"
            >
              <span className="df2-vd-assist-icon" aria-hidden>
                <DtIcon name="sparkle" size={16} />
              </span>
              <span className="df2-vd-assist-copy">
                <strong>Explain &amp; fix with AI</strong>
                <span>
                  {assistExpanded
                    ? "Plain-language explanation with one-click fixes."
                    : explain
                      ? "Analysis ready — open to review suggested fixes."
                      : "Open to analyze this validation result."}
                </span>
              </span>
              <span className={`df2-vd-assist-chevron${assistExpanded ? " is-open" : ""}`} aria-hidden>
                <DtIcon name="chevron-down" size={16} />
              </span>
            </button>
            <div className="df2-vd-assist-head-actions">
              <Button
                size="sm"
                variant={explain ? "secondary" : "primary"}
                disabled={!preflight || explaining}
                onClick={(e) => {
                  e.stopPropagation();
                  setAssistExpanded(true);
                  void runExplain();
                }}
                loading={explaining}
                loadingLabel="Analyzing…"
                leadingIcon={<DtIcon name="sparkle" size={14} />}
              >
                {explain ? "Re-analyze" : "Run analysis"}
              </Button>
            </div>
          </div>

          {assistExpanded && (
            <div
              className="df2-vd-assist-panel"
              id="df2-vd-assist-panel"
              role="region"
              aria-labelledby="df2-vd-assist-trigger"
            >
              {runId && (
                <div className="df2-vd-run-id">
                  <DtIcon name="activity" size={13} />
                  <span>Validation run</span>
                  <code>{runId}</code>
                  <button type="button" className="df2-vd-run-id-copy" onClick={() => void copyRunId()}>
                    {copiedRunId ? "Copied" : "Copy run ID"}
                  </button>
                </div>
              )}

              {explainError && (
                <div className="df2-vd-assist-error" role="alert">
                  <DtIcon name="alert" size={14} />
                  <span>{explainError}</span>
                </div>
              )}

              {(hasEncodingIssue || encodingBlocks) && (
                <div className="df2-vd-assist-actions df2-vd-assist-remediate">
                  <span className="df2-vd-assist-actions-title">Bad data remediation</span>
                  <div className="df2-vd-chip-row">
                    <button
                      type="button"
                      className="df2-vd-chip kind-open_bad_data_fix"
                      onClick={() => setBadDataOpen(true)}
                    >
                      <DtIcon name="shield" size={13} />
                      Fix bad data…
                    </button>
                    {onStripControlChars && (
                      <button
                        type="button"
                        className="df2-vd-chip kind-normalize_control_chars"
                        onClick={() => void runStrip()}
                        disabled={remediating}
                      >
                        <DtIcon name="layers" size={13} />
                        Strip controls &amp; re-run
                      </button>
                    )}
                    {onQuarantineAndRerun && (
                      <button
                        type="button"
                        className="df2-vd-chip kind-quarantine_and_rerun"
                        onClick={() => void runQuarantine()}
                        disabled={remediating}
                      >
                        <DtIcon name="shield" size={13} />
                        Quarantine &amp; re-run
                      </button>
                    )}
                  </div>
                </div>
              )}

              {explaining && !explain && (
                <div className="df2-vd-assist-loading">
                  <Spinner size="sm" label="" /> Reviewing gates, columns, and offending values…
                </div>
              )}

              {explain && preflight?.passed && /validation blocked|integrity failed/i.test(explain.summary || "") && (
                <div className="df2-vd-assist-body">
                  <p className="df2-vd-assist-clean">
                    Gates are green after remediation. The prior “blocked” explanation was from before Strip / Quarantine —
                    click Re-analyze for an updated summary. ISO→DATETIME notes are bind normalizations (0 failed), not blockers.
                  </p>
                </div>
              )}

              {explain && !(preflight?.passed && /validation blocked|integrity failed/i.test(explain.summary || "")) && (
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
                  {(explain.issues?.length ?? 0) > 0 && (
                    <div className="df2-vd-explain-issues" aria-label="Validation issues">
                      <span className="df2-vd-assist-actions-title">Issues</span>
                      <ul>
                        {explain.issues.map((issue, i) => (
                          <li key={`${issue.gate}-${issue.title}-${i}`} className={`sev-${issue.severity}`}>
                            <strong>{issue.title}</strong>
                            <span className="df2-vd-explain-gate">{issue.gate}</span>
                            {issue.what && <p>{issue.what}</p>}
                            {issue.why && <p className="df2-vd-explain-why"><em>Why:</em> {issue.why}</p>}
                            {issue.fix && <p className="df2-vd-explain-fix"><em>Fix:</em> {issue.fix}</p>}
                            {issue.columns?.length > 0 && (
                              <div className="df2-vd-chip-row">
                                {issue.columns.slice(0, 8).map((col) => (
                                  <span key={col} className="df2-vd-chip is-static">{col}</span>
                                ))}
                              </div>
                            )}
                          </li>
                        ))}
                      </ul>
                    </div>
                  )}
                  {(explain.column_fixes?.length ?? 0) > 0 && (
                    <div className="df2-vd-column-fixes" aria-label="Column fixes">
                      <span className="df2-vd-assist-actions-title">Column fixes</span>
                      <div className="df2-vd-column-fixes-table-wrap">
                        <table className="df2-vd-column-fixes-table">
                          <thead>
                            <tr>
                              <th>Column</th>
                              <th>Types</th>
                              <th>Failed</th>
                              <th>Suggestion</th>
                              <th />
                            </tr>
                          </thead>
                          <tbody>
                            {explain.column_fixes.map((fix) => (
                              <tr key={`${fix.column}-${fix.target ?? ""}`} className={`sev-${fix.severity}`}>
                                <td>
                                  <strong>{fix.column}</strong>
                                  {fix.target ? <small> → {fix.target}</small> : null}
                                </td>
                                <td>
                                  <span>{fix.source_type || "—"}</span>
                                  <span aria-hidden> → </span>
                                  <span>{fix.target_type || "—"}</span>
                                </td>
                                <td>{fix.failed}/{fix.sampled}</td>
                                <td>{fix.suggested_fix || "Review mapping"}</td>
                                <td>
                                  {(fix.suggested_target_type || fix.suggested_transform) && onApplyAction ? (
                                    <button
                                      type="button"
                                      className="df2-vd-chip"
                                      onClick={() =>
                                        handleSuggestedAction({
                                          kind: fix.suggested_transform ? "add_transform" : "change_target_type",
                                          column: fix.column,
                                          target: fix.target,
                                          to_type: fix.suggested_target_type ?? undefined,
                                          transform: fix.suggested_transform ?? undefined,
                                          label: fix.suggested_fix || `Fix ${fix.column}`,
                                        })
                                      }
                                    >
                                      Apply
                                    </button>
                                  ) : null}
                                </td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
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
                            onClick={() => handleSuggestedAction(action)}
                            disabled={
                              !onApplyAction
                              && action.kind !== "open_bad_data_fix"
                              && action.kind !== "normalize_control_chars"
                              && action.kind !== "quarantine_and_rerun"
                            }
                            title={action.label}
                          >
                            <DtIcon name={ACTION_ICON[action.kind] ?? "sparkle"} size={13} />
                            {action.label}
                          </button>
                        ))}
                        <button
                          type="button"
                          className="df2-vd-chip kind-review_mappings"
                          onClick={() => void proposeDurableRepair()}
                          disabled={repairBusy || !preflight}
                          title="Persist a human-gated repair proposal with audit trail"
                        >
                          <DtIcon name="sparkle" size={13} />
                          {repairBusy ? "Proposing…" : "Propose durable repair"}
                        </button>
                      </div>
                    </div>
                  )}
                  {explain.suggested_actions.length === 0 && !explain.passed && (
                    <div className="df2-vd-assist-actions">
                      <div className="df2-vd-chip-row">
                        <button
                          type="button"
                          className="df2-vd-chip kind-review_mappings"
                          onClick={() => void proposeDurableRepair()}
                          disabled={repairBusy || !preflight}
                        >
                          <DtIcon name="sparkle" size={13} />
                          {repairBusy ? "Proposing…" : "Propose durable repair"}
                        </button>
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
        </div>
      )}

      {!running && preflight?.coercion_report?.columns?.length ? (
        <CoercionTable columns={preflight.coercion_report.columns} />
      ) : null}

      {!running && preflight?.load_history_report ? (
        <LoadHistoryPanel
          report={preflight.load_history_report}
          title="Compared to prior loads"
          className="df2-vd-load-history"
        />
      ) : null}

      <div className="df2-vd-rules">
        <div className="df2-vd-rules-head">
          <DtIcon name="gate" size={15} />
          <strong>Validation rules</strong>
          <span>{totalGates} checks enforced before write · threshold {(confidenceThreshold * 100).toFixed(0)}%</span>
        </div>
        <div className="df2-vd-rules-grid">
          {displayGates.map((meta, index) => {
            const { status, message, issues, durationMs, privilegeProbe } = statusForGate(meta, index);
            return (
              <article key={`${meta.key}-${index}`} className={`df2-vd-rule status-${status}`}>
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
                {status !== "pending" && privilegeProbe && (privilegeProbe.method || privilegeProbe.status) && (
                  <div className={`df2-vd-priv-probe status-${privilegeProbe.status || "unknown"}`}>
                    {privilegeProbe.status && (
                      <span className="df2-vd-priv-chip">{privilegeProbe.status}</span>
                    )}
                    {privilegeProbe.method && (
                      <span className="df2-vd-priv-method">{privilegeProbe.method}</span>
                    )}
                    {privilegeProbe.engine && (
                      <span className="df2-vd-priv-engine">{privilegeProbe.engine}</span>
                    )}
                  </div>
                )}
                {status !== "pending" && durationMs != null && durationMs > 0 && (
                  <p className="df2-vd-rule-dur">Engine time {formatDuration(durationMs)}</p>
                )}
                {issues.length > 0 && (
                  <ul className="df2-vd-rule-issues">
                    {issues.slice(0, 4).map((issue) => (
                      <li key={issue}>{issue}</li>
                    ))}
                    {issues.length > 4 && <li>+{issues.length - 4} more</li>}
                  </ul>
                )}
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
            <strong>Fix before Run</strong>
            <span>{preflight.blockers.length}</span>
          </div>
          <p className="df2-vd-blocker-precaution">
            Schema mismatches, bad data, and type hazards are blocked here on purpose.
            Resolve each item below (why + fix), re-validate, then Execute — Run should
            only surface operational issues like timeouts or connectivity.
          </p>
          <ul>
            {preflight.blockers.map((b) => {
              const issues = issueTextsFromDetails(b.details);
              const blockingCols = (preflight.coercion_report?.columns ?? []).filter((c) => c.severity === "block");
              const showIssueList = issues.length > 0 && !(b.id.includes("dry_run") && blockingCols.length > 0);
              return (
                <li key={b.id}>
                  <strong>{metaForGate(b.id).label}</strong>
                  <span>{b.message}</span>
                  {showIssueList && (
                    <ul className="df2-vd-blocker-issues">
                      {issues.slice(0, 6).map((issue) => (
                        <li key={issue}>{issue}</li>
                      ))}
                    </ul>
                  )}
                  {blockingCols.length > 0 && b.id.includes("dry_run") && (
                    <div className="df2-vd-blocker-actions">
                      <span className="df2-vd-assist-actions-title">
                        Fix on Validate (remaps off incompatible typed columns)
                      </span>
                      <div className="df2-vd-chip-row">
                        {blockingCols.slice(0, 6).map((col) => (
                          <button
                            key={`${col.source}-${col.target}`}
                            type="button"
                            className="df2-vd-chip kind-change_target_type"
                            disabled={!onApplyAction || !col.suggested_target_type}
                            title={
                              col.suggested_fix
                              || `Remap ${col.source} to a ${col.suggested_target_type} column (Widen alone does not ALTER DDL)`
                            }
                            onClick={() =>
                              onApplyAction?.({
                                kind: "change_target_type",
                                label: `Remap ${col.source} → ${col.suggested_target_type}`,
                                column: col.source,
                                target: col.target,
                                to_type: col.suggested_target_type ?? undefined,
                              })
                            }
                          >
                            <DtIcon name="layers" size={13} />
                            Remap {col.source} → {col.suggested_target_type ?? "VARCHAR"}
                          </button>
                        ))}
                      </div>
                    </div>
                  )}
                  {b.guidance?.fix && (
                    <span className="df2-vd-blocker-fix">
                      <DtIcon name="check" size={12} /> {b.guidance.fix}
                    </span>
                  )}
                  {b.guidance?.why && !b.id.includes("dry_run") && (
                    <span className="df2-vd-blocker-why">{b.guidance.why}</span>
                  )}
                  {(encodingBlocks || hasEncodingIssue) && (b.id.includes("dry_run") || /format-control|replacement character/i.test(b.message)) && (
                    <div className="df2-vd-blocker-actions">
                      <button
                        type="button"
                        className="df2-vd-chip kind-open_bad_data_fix"
                        onClick={() => setBadDataOpen(true)}
                      >
                        <DtIcon name="shield" size={13} />
                        Fix bad data…
                      </button>
                    </div>
                  )}
                  {isTypeMismatchBlock && b.id.includes("dry_run") && typeMismatchColumns.length > 0 && (
                    <div className="df2-vd-blocker-actions">
                      <span className="df2-vd-assist-actions-title">
                        Type mismatch — Remap (Strip/Quarantine will not clear this)
                      </span>
                      <div className="df2-vd-chip-row">
                        {typeMismatchColumns.slice(0, 6).map((col) => (
                          <button
                            key={`block-${col.source}-${col.target}`}
                            type="button"
                            className="df2-vd-chip kind-change_target_type"
                            disabled={!onApplyAction}
                            onClick={() =>
                              onApplyAction?.({
                                kind: "change_target_type",
                                label: `Remap ${col.source} → VARCHAR`,
                                column: col.source,
                                target: col.target,
                                to_type: "VARCHAR",
                              })
                            }
                          >
                            <DtIcon name="layers" size={13} />
                            Remap {col.source} → VARCHAR
                          </button>
                        ))}
                      </div>
                    </div>
                  )}
                </li>
              );
            })}
          </ul>
        </div>
      )}

      <BadDataFixDrawer
        open={badDataOpen}
        onClose={() => setBadDataOpen(false)}
        issues={badDataIssues.length ? badDataIssues : [{ message: "format-control character detected — normalize before transfer" }]}
        applying={remediating}
        onStripControls={() => void runStrip()}
        onQuarantineContinue={() => void runQuarantine()}
        onExplainWithAI={() => {
          setBadDataOpen(false);
          void runExplain();
        }}
      />
      <RepairProposalDrawer
        open={repairOpen}
        proposal={repairProposal}
        mappings={repairMappings}
        onClose={() => setRepairOpen(false)}
        onApplied={(updated) => {
          onRepairMappingsApplied?.(updated);
          pendingVerifyRef.current = true;
          pushRemediation(
            "Repair applied",
            `${updated.length} mapping(s) updated from approved proposal ${repairProposal?.id || ""}`,
            "Applied — re-run Validate",
          );
        }}
        onDecided={(p) => {
          pushRemediation(
            p.status === "rejected" ? "Repair rejected" : "Repair approved",
            p.summary || p.id,
            p.status,
          );
        }}
      />
    </section>
  );
}
