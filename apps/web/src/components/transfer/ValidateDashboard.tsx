import { Fragment, useEffect, useMemo, useRef, useState } from "react";
import { DtIcon } from "../DtIcon";
import { Spinner } from "../LoadingState";
import { Button } from "../ui/Button";
import { explainPreflight, fetchRepairProposal, proposeRepairFromPreflight, type CellPreviewResult, type RepairMapping, type RepairProposal } from "../../lib/api";
import { readSession } from "../../lib/session";
import { SYNC_MODE_META } from "../../lib/transferConstants";
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
  blockerTitle,
  gateCatalogEntry,
  gateLabel,
  isInternalGateId,
} from "../../lib/preflightGates";
import { EngineStageTicker } from "../EngineStageTicker";
import {
  buildDisplayBlockers,
  buildExecutiveSummary,
  rankAndDedupeSuggestedActions,
  findDuplicateKeyRoot,
  isDeclaredFidelityCollapse,
  isEncodingIntegritySignal,
  isSampleUniquenessOnly,
  partitionCoercionColumns,
  partitionExplainIssues,
  remapToTypeForMismatch,
} from "../../lib/validateIssueGrouping";
import { buildValidateDecisionPath } from "../../lib/validateDecisionPath";
import { buildValidateHonestyControls, schemaDriftAllowsAcknowledge, schemaDriftCompatibilityHeadline, schemaDriftRequiresRemap } from "../../lib/validateHonestyControls";
import { isFkOrphanBlockerText, isFkOrphanCtaKind } from "../../lib/fkOrphanCta";
import {
  callableExtractNote,
  destExistsPrimaryCta,
  destOnlyPreserveColumns,
  extraSourceColumnsFromContract,
  shapeContractFromPreflight,
} from "../../lib/destExistsShape";
import { populationFitSummary } from "../../lib/populationFit";
import { ringDasharray, validateRingPercent } from "../../lib/progressRing";
import { BadDataFixDrawer, type BadDataIssue } from "./BadDataFixDrawer";
import { Gate8ProofCard, type Gate8Reconciliation } from "./Gate8ProofCard";
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

/**
 * Title for one assist / explain issue card. The assistant falls back to the
 * blocker's internal id when a proof-bundle blocker has no gate catalog entry,
 * so name the cause from its own text instead of showing `proof_0`.
 */
function explainIssueTitle(issue: { gate: string; title?: string; what?: string }): string {
  if (isInternalGateId(issue.gate)) {
    return blockerTitle(issue.gate, issue.what || issue.title);
  }
  return issue.title || gateLabel(issue.gate);
}

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
  // Privilege / Redshift staging probe honesty on G2
  const probe = details.privilege_probe;
  const staging = details.redshift_staging_probe;
  const lines: string[] = [];
  if (probe && typeof probe === "object") {
    const p = probe as Record<string, unknown>;
    if (p.method) lines.push(`Probe: ${String(p.method)}`);
    if (p.status) lines.push(`Privilege status: ${String(p.status)}`);
    if (p.engine) lines.push(`Engine: ${String(p.engine)}`);
    if (p.detail && String(p.status) !== "ok") lines.push(String(p.detail));
  }
  if (staging && typeof staging === "object") {
    const s = staging as Record<string, unknown>;
    if (s.status) lines.push(`COPY staging: ${String(s.status)}`);
    if (s.detail && String(s.status) !== "ok") lines.push(String(s.detail));
  }
  if (lines.length) return lines.slice(0, 8);
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

function stagingProbeFromDetails(details?: Record<string, unknown> | null): PrivilegeProbeMeta | null {
  const raw = details?.redshift_staging_probe;
  if (!raw || typeof raw !== "object") return null;
  const p = raw as Record<string, unknown>;
  return {
    status: p.status != null ? String(p.status) : undefined,
    method: p.method != null ? String(p.method) : undefined,
    engine: p.engine != null ? String(p.engine) : undefined,
    detail: p.detail != null ? String(p.detail) : undefined,
  };
}

const STATUS_LABEL: Record<string, string> = {
  pass: "Passed",
  block: "Blocked",
  skip: "Skipped",
  warn: "Review",
  running: "Running",
  pending: "Pending",
};

interface ValidateDashboardProps {
  preflight: PreflightResult | null;
  running?: boolean;
  confidenceThreshold?: number;
  destType?: string;
  validationMode?: string;
  /** Current sync mode — duplicate-key copy must not tell append operators to "switch to append". */
  syncMode?: string;
  /** Destination Advanced: write via `{table}_df_staging` then promote. */
  writeViaStaging?: boolean;
  /** Apply a one-click AI suggestion to the Studio (change type, add transform, navigate). */
  onApplyAction?: (action: ValidationSuggestedAction) => void;
  /** Apply strip_controls across mappings and re-run preflight. Returns what changed. */
  onStripControlChars?: () => void | Promise<RemediationOpResult | void>;
  /** True when text mappings already carry strip_controls (Execute will sanitize). */
  stripControlsApplied?: boolean;
  /** Soften to quarantine-friendly posture, strip, and re-run. Returns what changed. */
  onQuarantineAndRerun?: () => void | Promise<RemediationOpResult | void>;  /** Cell-level will-quarantine / will-coerce preview from sample rows. */
  cellPreview?: CellPreviewResult | null;
  /** Jump back to Map so the operator can fix coerced / identity column mappings. */
  onReviewMappings?: (opts?: { focusSource?: string }) => void;
  /** Reload live destination types — G15 pending / dest_unknown. */
  onReloadDestSchema?: () => void;
  /**
   * Open Destination → Advanced settings where primary key and sync mode live.
   * Used for duplicate-identity blockers (Map alone cannot change the sync contract).
   */
  onOpenIdentitySettings?: () => void;
  /** Sample-unique key suggestions (honest: Validate sample only). */
  uniqueKeySuggestions?: Array<{
    column: string;
    uniqueCount: number;
    sampleRows: number;
  }>;
  /** Sample-unique composite suggestions (false-PK when single col duplicates). */
  compositeKeySuggestions?: Array<{
    columns: string[];
    uniqueCount: number;
    sampleRows: number;
  }>;
  /** Apply a suggested primary key, then open Advanced / re-validate. */
  onApplyPrimaryKey?: (column: string) => void;
  /** Open Mapping proof drawer — evidence only (Column matches card). */
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
    /** Calibrated evidence-class counts from mapping proof rows. */
    classCounts?: Record<string, number>;
  } | null;
  /** Trigger preflight from the dashboard (same as the rail CTA). */
  onRunPreflight?: () => void;
  /**
   * Operator attested governance policy allows moving detected PII.
   * Re-runs Validate with compliance_acknowledged=true.
   */
  onAcknowledgeCompliance?: () => void;
  /**
   * Operator acknowledged schema drift under manual_review for this run.
   * Re-runs Validate with schema_drift_acknowledged=true.
   */
  onAcknowledgeSchemaDrift?: () => void;
  /**
   * Operator acknowledged destination FK mapping risk for this run.
   * Re-runs Validate with fk_risk_acknowledged=true — does not claim RI proven.
   */
  onAcknowledgeFkRisk?: () => void;
  /**
   * Module 16 — opt-in full-table population orphan scan (only path to RI proven).
   * Expensive; default off. Sample Validate never equals population RI.
   */
  runPopulationOrphanScan?: boolean;
  onRunPopulationOrphanScanChange?: (enabled: boolean) => void;
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
  /**
   * Controlled Fix-bad-data drawer (rail / Studio actions). When omitted, the
   * dashboard owns open state internally.
   */
  badDataFixOpen?: boolean;
  onBadDataFixOpenChange?: (open: boolean) => void;
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
        if (isEncodingIntegritySignal(item)) {
          out.push({ message: item });
        }
        continue;
      }
      if (item && typeof item === "object") {
        const row = item as Record<string, unknown>;
        const message = String(row.message ?? row.error ?? "");
        if (!message && !row.chars) continue;
        if (message && !isEncodingIntegritySignal(message) && !row.chars) {
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
  for (const b of preflight.blockers ?? []) {
    if (!b) continue;
    const details = b.details || {};
    if (Array.isArray(details.errors)) pushFrom(details.errors);
    if (Array.isArray(details.issues)) pushFrom(details.issues);
    if (Array.isArray(details.encoding_issues)) pushFrom(details.encoding_issues);
    if (isEncodingIntegritySignal(b.message)) {
      out.push({ message: b.message });
    }
  }
  for (const g of preflight.gates ?? []) {
    if (!g) continue;
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
  map_column: "layers",
  review_mappings: "layers",
  rerun_mapping: "transfer",
  check_connection: "server",
  normalize_control_chars: "shield",
  quarantine_and_rerun: "shield",
  open_bad_data_fix: "shield",
  fix_source_keys: "settings",
  confirm_or_remap: "layers",
  reload_dest_schema: "scan",
  confirm_add: "layers",
  continue_validate: "check",
  fix_orphans: "alert",
  run_population_orphan_scan: "scan",
};

/** Encoding remediations collapse to one Fix-bad-data CTA (drawer owns Strip/Quarantine). */
const ENCODING_ACTION_KINDS = new Set([
  "normalize_control_chars",
  "quarantine_and_rerun",
  "open_bad_data_fix",
]);

function collapseEncodingSuggestedActions(
  actions: ValidationSuggestedAction[],
): ValidationSuggestedAction[] {
  let sawEncoding = false;
  const out: ValidationSuggestedAction[] = [];
  for (const action of actions) {
    if (ENCODING_ACTION_KINDS.has(action.kind)) {
      if (!sawEncoding) {
        sawEncoding = true;
        out.push({ kind: "open_bad_data_fix", label: "Fix bad data…" });
      }
      continue;
    }
    out.push(action);
  }
  return rankAndDedupeSuggestedActions(out);
}

/** Per-column value-aware coercion table with expandable offending-value rows. */
function CoercionTable({ columns }: { columns: CoercionColumn[] }) {
  const [expanded, setExpanded] = useState<Set<string>>(new Set());
  const [showAllConversions, setShowAllConversions] = useState(false);
  const [showIsoNormalize, setShowIsoNormalize] = useState(false);
  // Clean string→typed conversions (severity ok) stay collapsed by default so
  // Strip/Quarantine are not buried — detail is one click away.
  // ISO timestamp wire-normalize warns are grouped (not six separate dramas).
  const { isoNormalize, otherActionable, clean } = partitionCoercionColumns(columns);
  const actionable = [...otherActionable, ...(showIsoNormalize ? isoNormalize : [])];
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
    <div className={`df2-vd-coerce${otherActionable.length === 0 && isoNormalize.length === 0 ? " is-clean" : ""}`}>
      <div className="df2-vd-coerce-head">
        <DtIcon name={otherActionable.length ? "scan" : "check"} size={15} />
        <strong>Type coercion preview</strong>
        <span>
          {otherActionable.length > 0
            ? `${otherActionable.length} column${otherActionable.length === 1 ? "" : "s"} need review`
            : `${clean.length} column${clean.length === 1 ? "" : "s"} convert cleanly on the sample (declared types align)`}
          {isoNormalize.length > 0
            ? ` · ${isoNormalize.length} timestamp normalize (informational)`
            : ""}
          {clean.length > 0 && otherActionable.length > 0
            ? ` · ${clean.length} sample-clean`
            : ""}
          {otherActionable.length > 0
            ? " — expand a row for offending values, wire form, or declared fidelity collapse."
            : "."}
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
      {isoNormalize.length > 0 && !showAllConversions && (
        <div className="df2-vd-iso-group" role="status">
          <div className="df2-vd-iso-group-head">
            <DtIcon name="activity" size={14} />
            <strong>Timestamp normalize at write</strong>
            <span>{isoNormalize.length} column{isoNormalize.length === 1 ? "" : "s"} · no data loss expected</span>
          </div>
          <p className="df2-vd-iso-group-note">ISO-8601 → destination TIMESTAMP bind</p>
          <div className="df2-vd-chip-row">
            {isoNormalize.map((col) => (
              <span key={`${col.source}-${col.target}`} className="df2-vd-chip is-static">{col.source}</span>
            ))}
          </div>
          <button
            type="button"
            className="df2-btn df2-btn-ghost df2-btn-sm"
            onClick={() => setShowIsoNormalize((v) => !v)}
          >
            {showIsoNormalize ? "Hide per-column normalize rows" : "Show per-column normalize rows"}
          </button>
        </div>
      )}
      {visible.length === 0 && isoNormalize.length === 0 ? (
        <p className="df2-vd-coerce-empty-hint">
          No blocking coercions. Use <strong>Show all type conversions</strong> to inspect
          raw → destination wire forms (for example ISO timestamps → DATETIME).
        </p>
      ) : visible.length === 0 ? (
        <p className="df2-vd-coerce-empty-hint">
          No blocking coercions — timestamp normalize notes are informational above.
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
                || (col.wrap_examples?.length ?? 0) > 0
                || Boolean(col.suggested_fix);
              const wireHint = col.sample_wire_form
                || col.wire_examples?.[0]?.wire_form
                || col.wrap_examples?.[0]?.wire_form
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
                      {col.framing?.kind === "structured_serialization" && (
                        <span className="df2-vd-coerce-frame" title="Shape-preserving structured serialization">
                          Structured serialization
                        </span>
                      )}
                      {isDeclaredFidelityCollapse(col) && (
                        <span
                          className="df2-vd-coerce-frame is-collapse"
                          title={col.framing?.label || col.suggested_fix || "Declared type path collapses fidelity"}
                        >
                          {col.failed === 0 && (col.ok ?? 0) > 0
                            ? "Sample coerces · declared collapse"
                            : "Fidelity collapse"}
                        </span>
                      )}
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
                        {(col.json_scalar_wraps ?? 0) > 0 ? " · JSON wrap" : ""}
                        {(col.wire_failures ?? 0) > 0 ? ` · ${col.wire_failures} wire fail` : ""}
                      </span>
                    </td>
                  </tr>
                  {isOpen && hasDetail && (
                    <tr className={`df2-vd-coerce-detail sev-${col.severity}`}>
                      <td colSpan={9}>
                        {col.framing?.kind === "structured_serialization" && (
                          <p className="df2-vd-coerce-fix is-info">
                            <DtIcon name="layers" size={13} />
                            {" "}
                            {col.framing.label || "Structured-data serialization"}
                            {" · "}
                            Source {col.framing.source_shape || "object/array"}
                            {" → "}
                            Target {col.framing.target_shape || "JSON"}
                            {col.framing.shape_preserved ? " · shape preserved" : ""}
                            {col.framing.sample_round_trip ? " · sample wire verified" : ""}
                          </p>
                        )}
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
                        {(col.wrap_examples?.length ?? 0) > 0 && (
                          <div className="df2-vd-coerce-samples">
                            <span className="df2-vd-coerce-samples-title">
                              Bare scalar → JSON string wrap (domain change — Accept risk if intentional)
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
                                {(col.wrap_examples ?? []).map((f, i) => (
                                  <tr key={`j-${f.row}-${i}`}>
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

function MetricChip({
  value,
  label,
  tone,
  emptyLabel,
}: {
  value: number | null;
  label: string;
  tone: string;
  emptyLabel?: string;
}) {
  const measured = value != null && Number.isFinite(value);
  return (
    <div className={`df2-vd-metric tone-${tone}${measured ? "" : " is-empty"}`}>
      <span className="df2-vd-metric-label">{label}</span>
      <span className="df2-vd-metric-val">
        {measured ? `${Math.round(value)}%` : (emptyLabel || "—")}
      </span>
    </div>
  );
}

export function ValidateDashboard({
  preflight,
  running = false,
  confidenceThreshold = 0.85,
  destType,
  validationMode,
  syncMode,
  writeViaStaging = false,
  onApplyAction,
  onStripControlChars,
  stripControlsApplied = false,
  onQuarantineAndRerun,
  cellPreview = null,
  onReviewMappings,
  onReloadDestSchema,
  onOpenIdentitySettings,
  uniqueKeySuggestions = [],
  onApplyPrimaryKey,
  compositeKeySuggestions = [],
  onOpenMappingProof,
  mappingProofSummary = null,
  onRunPreflight,
  onAcknowledgeCompliance,
  onAcknowledgeSchemaDrift,
  onAcknowledgeFkRisk,
  runPopulationOrphanScan = false,
  onRunPopulationOrphanScanChange,
  repairMappings = [],
  onRepairMappingsApplied,
  repairJobId = "",
  seedRepairProposalId = null,
  onSeedRepairConsumed,
  badDataFixOpen,
  onBadDataFixOpenChange,
}: ValidateDashboardProps) {
  const [elapsedMs, setElapsedMs] = useState(0);
  const [revealCount, setRevealCount] = useState(0);
  const [explain, setExplain] = useState<ValidationExplanation | null>(null);
  const [explaining, setExplaining] = useState(false);
  const [repairOpen, setRepairOpen] = useState(false);
  const [repairProposal, setRepairProposal] = useState<RepairProposal | null>(null);
  const [repairBusy, setRepairBusy] = useState(false);
  const [explainError, setExplainError] = useState<string | null>(null);
  const [internalBadDataOpen, setInternalBadDataOpen] = useState(false);
  const badDataControlled = typeof onBadDataFixOpenChange === "function";
  const badDataOpen = badDataControlled ? Boolean(badDataFixOpen) : internalBadDataOpen;
  const setBadDataOpen = (open: boolean) => {
    if (badDataControlled) onBadDataFixOpenChange?.(open);
    else setInternalBadDataOpen(open);
  };
  const [remediating, setRemediating] = useState(false);
  const [assistExpanded, setAssistExpanded] = useState(true);
  const [revealCellPii, setRevealCellPii] = useState(false);
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
    const found: Array<{ source: string; target: string; sourceType?: string; targetType?: string; toType: string }> = [];
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
    const re = /([A-Za-z_][\w]*)\s*\(([^)]+)\)\s*→\s*([A-Za-z_][\w]*)\s*\(([^)]+)\)/g;
    for (const text of texts) {
      let m: RegExpExecArray | null;
      while ((m = re.exec(text))) {
        const key = `${m[1]}→${m[3]}`;
        if (seen.has(key)) continue;
        seen.add(key);
        const sourceType = (m[2] || "").trim();
        const targetType = (m[4] || "").trim();
        found.push({
          source: m[1],
          target: m[3],
          sourceType,
          targetType,
          toType: remapToTypeForMismatch(sourceType, targetType),
        });
      }
    }
    return found;
  }, [preflight?.blockers, preflight?.gates]);

  const isTypeMismatchBlock = typeMismatchColumns.length > 0
    || Boolean(
      preflight?.blockers.some((b) =>
        /invalid (decimal|integer|boolean)|cannot be cast|does not safely become|lossy type|lossy coercion/i.test(b.message),
      ),
    );
  const isPrivilegeBlock = Boolean(
    preflight?.blockers.some((b) => {
      const probe = privilegeProbeFromDetails(b.details);
      const staging = stagingProbeFromDetails(b.details);
      return (
        (b.id === "g2_destination" || /g2_destination/i.test(b.id || ""))
        && (probe?.status === "denied"
          || staging?.status === "denied"
          || /privilege|INSERT|CREATE|ACL|IAM|PutObject|has_privileges|GRANT|staging bucket/i.test(b.message || ""))
      );
    })
    || preflight?.gates.some((g) => {
      if (g.id !== "g2_destination" || g.status !== "block") return false;
      const probe = privilegeProbeFromDetails(g.details);
      const staging = stagingProbeFromDetails(g.details);
      return probe?.status === "denied"
        || staging?.status === "denied"
        || /privilege|INSERT|CREATE|ACL|IAM|PutObject|has_privileges|GRANT|staging bucket/i.test(g.message || "");
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

  const pushRemediation = (action: string, detail: string, outcome: string, steps?: string[]) => {    setRemediationLog((prev) => [
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
    preflight?.blockers.some((b) => isEncodingIntegritySignal(b.message))
    || preflight?.gates.some((g) => g.status === "block" && isEncodingIntegritySignal(g.message)),
  );
  const showEncodingRemediation = !isTypeMismatchBlock && !isConnectionBlock && !isPrivilegeBlock && (hasEncodingIssue || encodingBlocks);
  const isFkOrphanBlock = Boolean(
    preflight?.blockers.some((b) =>
      (b.id === "constraint_fk" || /constraint_fk/i.test(b.id || ""))
      && isFkOrphanBlockerText(`${b.message || ""} ${JSON.stringify(b.details || {})}`),
    )
    || preflight?.gates.some((g) =>
      g.status === "block"
      && (g.id === "constraint_fk" || /constraint_fk/i.test(g.id || ""))
      && isFkOrphanBlockerText(`${g.message || ""} ${JSON.stringify(g.details || {})}`),
    ),
  );
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

  const destShape = useMemo(() => shapeContractFromPreflight(preflight), [preflight]);
  const destShapeExtras = useMemo(() => extraSourceColumnsFromContract(destShape), [destShape]);
  const destShapePreserve = useMemo(() => destOnlyPreserveColumns(destShape), [destShape]);
  const destShapeCta = useMemo(() => destExistsPrimaryCta(destShape), [destShape]);
  const callableNote = useMemo(() => callableExtractNote(preflight), [preflight]);
  const showDestShape = Boolean(
    destShape
    && (
      destShapeExtras.length
      || destShapePreserve.length
      || destShapeCta
      || (destShape.shape && destShape.shape !== "create_new_table")
    ),
  );

  // After a remediation re-run, close out "waiting for re-validation" entries.
  useEffect(() => {
    if (!preflight || running || !pendingVerifyRef.current) return;
    const runKey = preflight.run_id || `${preflight.passed_count}-${preflight.total_gates}-${preflight.readiness_score}`;
    if (verifiedRunRef.current === runKey) return;
    verifiedRunRef.current = runKey;
    pendingVerifyRef.current = false;
    const dry = preflight.gates?.find((g) => /dry_run|integrity/i.test(g.id));
    // Execute unlock requires decision=approve — passed/review-grade must not greenwash.
    const decision = preflight.proof_bundle?.transfer_decision?.decision;
    const executeUnlocked =
      Boolean(preflight.passed)
      && decision === "approve"
      && !String(preflight.run_id || "").startsWith("pf_local_");
    const reviewGrade = Boolean(preflight.passed) && decision !== "approve";
    const dryPass = dry?.status === "pass";
    const outcome = executeUnlocked
      ? "Preflight approved — Execute unlocked; Gate-8 post-write proof still pending"
      : reviewGrade
        ? "Review-grade preflight — Execute stays locked until decision is approve"
        : dryPass
          ? "Dry-run/integrity improved — still blocked by other gates"
          : "Still blocked — remap columns on Map";
    const op = lastOpRef.current;
    const resultSteps: string[] = [];
    if (op?.steps?.length) {
      resultSteps.push(...op.steps);
    }
    if (executeUnlocked) {
      resultSteps.push(
        `Re-validation: ${preflight.passed_count ?? 0}/${preflight.total_gates ?? 0} gates passed.`,
        op?.kind === "strip_controls" || op?.kind === "quarantine_strip"
          ? "Jobs quarantine stays empty unless cells still fail during Execute — Strip cleaned them before write."
          : "Execute is unlocked (decision approve).",
      );
    } else {
      resultSteps.push(
        reviewGrade
          ? `Review-grade: ${preflight.passed_count ?? 0}/${preflight.total_gates ?? 0} gates passed — complete acknowledgments or Map fixes.`
          : `Still blocked: ${preflight.passed_count ?? 0}/${preflight.total_gates ?? 0} gates passed` +
            (dryPass ? " (dry-run/integrity ok)." : `. ${dry?.message || "see Validation rules"}.`),
        "Quarantine/Strip cannot fix wrong column type mappings — use Map.",
      );
    }
    const detail = executeUnlocked
      ? (op
        ? `${op.title} succeeded. ${op.columnsChanged.length} mapping(s) now use strip_controls.`
        : "All gates approved — Execute unlocked.")
      : reviewGrade
        ? `Gates cleared at review-grade (${preflight.passed_count ?? 0}/${preflight.total_gates ?? 0}). Execute stays locked until decision approve.`
        : dryPass
          ? `Dry-run/integrity passes, but preflight is not fully clear (${preflight.passed_count ?? 0}/${preflight.total_gates ?? 0}). Execute stays locked.`
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
    lastOpRef.current = null;
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
  // Missing transfer_decision must never default to approve — Execute requires
  // an explicit API decision === "approve". Fall back to review when gates pass
  // without a proof decision so the hero cannot greenwash.
  const decision = proof?.transfer_decision?.decision
    ?? (preflight ? "review" : "pending");
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

  /**
   * The transformed image the gates judged. Present only when an approved
   * Transform (pre-load) recipe ran before them, so its absence is itself the
   * truth: the gates read the source as declared.
   */
  const transformImage = preflight?.transform_image ?? null;
  const transformRetyped = Object.entries(transformImage?.retyped_columns ?? {})
    .sort(([a], [b]) => a.localeCompare(b));

  const semantic = proof?.semantic_mapping_score ?? 0;
  const qualityRaw = proof?.quality_score;
  const qualityNotProfiled =
    qualityRaw == null
    || String(proof?.quality_grade || "").toLowerCase() === "not_profiled";
  const quality = qualityNotProfiled ? null : Number(qualityRaw);
  const complianceRisk = proof?.compliance?.risk_score ?? 0;
  const localPreflight = isLocalPreflight(preflight);
  const proofWarnings = proof?.transfer_decision?.warnings ?? [];
  const reconciliation = proof?.reconciliation;
  const sampleCompare = reconciliation?.sample_compare;
  const mismatches = sampleCompare?.mismatches ?? [];
  const dryGate = preflight?.gates?.find((g) => /dry_run|integrity/i.test(g.id));
  const sampleScanned = Number(dryGate?.details?.sample_rows_scanned ?? dryGate?.details?.sample_size ?? 0) || null;
  const engineMsTotal = (preflight?.gates ?? []).reduce((sum, g) => sum + (Number(g.duration_ms) || 0), 0);
  // Exact Studio sync ids — avoid /append/i matching accidental substrings.
  const appendLikeSync = [
    "full_refresh_append",
    "incremental_append",
  ].includes(syncMode || "");
  const upsertLikeSync = [
    "incremental_deduped",
    "cdc",
    "scd2",
    "mirror",
  ].includes(syncMode || "");
  const syncMeta = syncMode ? SYNC_MODE_META[syncMode] : null;
  const uniquenessSampleOnly = isSampleUniquenessOnly(preflight);
  const heroReadyLabel = running
    ? "elapsed"
    : decision === "approve"
      ? (uniquenessSampleOnly ? "sample-only" : "execute-ready")
      : decision === "block"
        ? "blocked"
        : decision === "review"
          ? "review"
          : "pending";
  const heroRing = validateRingPercent({
    running,
    passed: preflight?.passed,
    decision,
    passedCount,
    blockedCount,
    readinessScore: readiness,
  });
  const complianceAck = proof?.compliance?.acknowledgment as
    | { actor?: string; at?: string; reason?: string }
    | undefined;
  const schemaDriftAck = (() => {
    const gate = preflight?.gates?.find((g) => g.id === "schema_drift");
    const fromGate = gate?.details?.acknowledgment as
      | { actor?: string; at?: string; reason?: string }
      | undefined;
    if (fromGate?.actor) return fromGate;
    const top = (preflight as { schema_drift?: { acknowledgment?: { actor?: string; at?: string; reason?: string } } } | null)
      ?.schema_drift?.acknowledgment;
    return top?.actor ? top : undefined;
  })();

  const executiveSummary = useMemo(
    () => buildExecutiveSummary(preflight, syncMode),
    [preflight, syncMode],
  );
  const duplicateRoot = useMemo(
    () => findDuplicateKeyRoot(preflight, syncMode),
    [preflight, syncMode],
  );
  const displayBlockers = useMemo(
    () => (preflight ? buildDisplayBlockers(preflight, syncMode) : []),
    [preflight, syncMode],
  );
  const orphanBarActions = useMemo(() => {
    const fromBlockers = displayBlockers.flatMap((b) => b.suggested_actions || []);
    const fromEngine = (preflight?.blockers ?? []).flatMap((b) => b.guidance?.suggested_actions || []);
    return rankAndDedupeSuggestedActions(
      [...fromBlockers, ...fromEngine].filter((a) => isFkOrphanCtaKind(String(a.kind))),
    );
  }, [displayBlockers, preflight]);
  const fidelityRoot = useMemo(
    () => displayBlockers.find((d) => d.kind === "fidelity_root") ?? null,
    [displayBlockers],
  );
  const decisionPath = useMemo(() => {
    if (!preflight || running) return null;
    const executeUnlocked =
      Boolean(preflight.passed)
      && String(decision || "").toLowerCase() === "approve"
      && !preflight.proof_bundle?.risk_contracts?.incomplete;
    return buildValidateDecisionPath(preflight, { syncMode, executeUnlocked });
  }, [preflight, syncMode, running, decision]);
  const honestyControls = useMemo(
    () => buildValidateHonestyControls(preflight, {
      populationScanRequested: runPopulationOrphanScan,
    }),
    [preflight, runPopulationOrphanScan],
  );
  const populationFit = useMemo(
    () => populationFitSummary(preflight?.population_fit),
    [preflight],
  );
  const explainParts = useMemo(
    () => (explain?.issues?.length ? partitionExplainIssues(explain.issues) : null),
    [explain],
  );

  // While validating: all gates pending/queued — never fake a green "pass" mid-flight.
  // After results: reveal in order using real duration_ms.
  const statusForGate = (
    meta: GateMeta,
    index: number,
  ): {
    status: string;
    message: string;
    issues: string[];
    durationMs?: number;
    privilegeProbe: PrivilegeProbeMeta | null;
    evidenceScope: Record<string, unknown> | null;
  } => {
    const gate = gateByKey.get(meta.key) ?? gateByKey.get(meta.key.replace(/^g\d+_/, ""));
    if (running) {
      return {
        status: "pending",
        message: "Queued — engine returns all gate results when the pass finishes",
        issues: [],
        privilegeProbe: null,
        evidenceScope: null,
      };
    }
    if (gate) {
      const revealed = index < revealCount;
      if (!revealed && revealCount < (preflight?.gates?.length ?? 0)) {
        return {
          status: "pending",
          message: "Result ready — revealing…",
          issues: [],
          privilegeProbe: null,
          evidenceScope: null,
        };
      }
      const privilegeProbe = privilegeProbeFromDetails(gate.details);
      const stagingProbe = stagingProbeFromDetails(gate.details);
      const issues = gate.status === "block" ? issueTextsFromDetails(gate.details) : [];
      const evidenceScope = (gate.details?.evidence_scope
        && typeof gate.details.evidence_scope === "object")
        ? (gate.details.evidence_scope as Record<string, unknown>)
        : null;
      // On pass, still surface probe method/status as non-blocking issues for G2 honesty.
      if (gate.status === "pass" && (privilegeProbe?.method || stagingProbe?.status) && meta.key === "g2_destination") {
        const soft: string[] = [];
        if (privilegeProbe?.method) soft.push(`Probe: ${privilegeProbe.method}`);
        if (privilegeProbe?.status === "unavailable" && privilegeProbe.detail) {
          soft.push(privilegeProbe.detail);
        }
        if (stagingProbe?.status) soft.push(`COPY staging: ${stagingProbe.status}`);
        if (stagingProbe?.status && stagingProbe.status !== "ok" && stagingProbe.detail) {
          soft.push(stagingProbe.detail);
        }
        return {
          status: gate.status,
          message: gate.message,
          issues: soft,
          durationMs: gate.duration_ms,
          privilegeProbe,
          evidenceScope,
        };
      }
      return {
        status: gate.status,
        message: gate.message,
        issues,
        durationMs: gate.duration_ms,
        privilegeProbe,
        evidenceScope,
      };
    }
    return {
      status: "pending",
      message: "Awaiting validation run.",
      issues: [],
      privilegeProbe: null,
      evidenceScope: null,
    };
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
        // Handler redirected — identity → Advanced, type mismatch → Map.
        pendingVerifyRef.current = false;
        const identityRedirect = Boolean(duplicateRoot);
        pushRemediation(
          "Quarantine + strip controls",
          identityRedirect
            ? "Duplicate identity keys cannot be quarantined. Open Destination → Advanced to change primary key or sync mode."
            : flagged.length
              ? `Could not auto-fix flagged columns (${flagged.slice(0, 6).join(", ")}). Remap types on Map instead.`
              : "Blocked by a type/mapping issue — quarantine cannot change column types. Open Map to remap.",
          identityRedirect ? "Redirected to identity settings" : "Redirected to Map",
          identityRedirect
            ? [
                "Quarantine/Strip only sanitize encoding (U+200B / control chars).",
                "Duplicate keys need a unique primary key or a sync mode that does not require uniqueness.",
              ]
            : [
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
    // Encoding remediations share one surface — BadDataFixDrawer — so Strip /
    // Quarantine are not duplicated next to every “Fix bad data…” opener.
    if (action.kind === "normalize_control_chars" || action.kind === "open_bad_data_fix") {
      setBadDataOpen(true);
      return;
    }
    if (action.kind === "quarantine_and_rerun") {
      // Identity duplicates survive Quarantine — route to Destination → Advanced.
      if (duplicateRoot && onOpenIdentitySettings) {
        onOpenIdentitySettings();
        return;
      }
      setBadDataOpen(true);
      return;
    }
    if (action.kind === "fix_source_keys") {
      if (onOpenIdentitySettings) {
        onOpenIdentitySettings();
        return;
      }
      onReviewMappings?.(
        action.column ? { focusSource: action.column } : undefined,
      );
      return;
    }
    if (action.kind === "review_mappings" || action.kind === "rerun_mapping"
      || action.kind === "confirm_add") {
      onReviewMappings?.(
        action.column ? { focusSource: action.column } : undefined,
      );
      return;
    }
    if (action.kind === "confirm_or_remap") {
      pendingVerifyRef.current = true;
      pushRemediation(
        action.label || "Confirm this pair",
        action.column
          ? `Confirm false-friend pair on ${action.column} — re-running Validate.`
          : "Confirm false-friend pair(s) — re-running Validate.",
        "Applied — re-running validation",
        [
          "Confirm this pair stamps false_friend_confirmed.",
          "Approve eligible does not clear qty≠amt / user≠customer.",
          "Re-validate after confirm — Execute unlocks only when gates pass.",
        ],
      );
      onApplyAction?.(action);
      return;
    }
    if (action.kind === "reload_dest_schema") {
      if (onReloadDestSchema) {
        onReloadDestSchema();
        return;
      }
      onRunPreflight?.();
      return;
    }
    if (action.kind === "continue_validate") {
      onRunPreflight?.();
      return;
    }
    if (action.kind === "run_population_orphan_scan") {
      pendingVerifyRef.current = true;
      pushRemediation(
        action.label || "Run population orphan scan",
        "Opt-in full-table anti-join — only path to RI proven. Sample Validate never claims referential integrity.",
        "Running population orphan scan",
        [
          "Enables the same honesty checkbox as Run population orphan scan on next Validate.",
          "Sample orphan probe is not population RI proof.",
          "Zero orphans on the population scan is required before RI proven.",
        ],
      );
      onApplyAction?.(action);
      return;
    }
    if (action.kind === "fix_orphans") {
      // Do not claim applied / re-validating — DataFlow cannot invent parent rows.
      onApplyAction?.(action);
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
        <div
          className={`df2-vd-hero-ring tone-${heroTone}${heroRing.indeterminate ? " is-indeterminate" : ""}${!heroRing.indeterminate && heroRing.pct >= 100 ? " is-complete" : ""}`}
          aria-hidden
        >
          <svg viewBox="0 0 72 72">
            <circle cx="36" cy="36" r="30" className="df2-vd-hero-track" />
            <circle
              cx="36"
              cy="36"
              r="30"
              className="df2-vd-hero-fill"
              pathLength={100}
              strokeDasharray={ringDasharray(heroRing.pct, { indeterminate: heroRing.indeterminate })}
              transform="rotate(-90 36 36)"
            />
          </svg>
          <div className="df2-vd-hero-ring-label">
            <strong>
              {running ? formatElapsed(elapsedMs) : heroRing.pct}
              <small>{running ? "" : "%"}</small>
            </strong>
            <span>{heroReadyLabel}</span>
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
                ? "Validating route — nine engine stages"
                : preflight
                  ? executiveSummary?.title ?? (
                    decision === "approve" && preflight.passed
                      ? "Execute-ready · not migration proven"
                      : preflight.passed
                        ? "Review before Execute"
                        : "Action needed before transfer"
                  )
                  : "Run validation to check this route"}
            </h3>
            {running && <EngineStageTicker running />}
          </div>

          <div className="df2-vd-hero-counts">
            <span className="df2-vd-count ok"><strong>{passedCount}</strong> passed</span>
            <span
              className="df2-vd-count block"
              title={
                displayBlockers.length > 0 && displayBlockers.length !== blockedCount
                  ? `${displayBlockers.length} root cause(s) · ${blockedCount} gate check(s)`
                  : undefined
              }
            >
              <strong>{displayBlockers.length > 0 ? displayBlockers.length : blockedCount}</strong>
              {displayBlockers.length > 0
                ? " root cause(s)"
                : " blocked"}
            </span>
            <span
              className="df2-vd-count skip"
              title={skippedCount > 0 ? "Skipped gates are N/A — they do not shrink a finished ring" : undefined}
            >
              <strong>{skippedCount}</strong> skipped
            </span>
          </div>

          {!running && transformImage && (
            <div className="df2-vd-xform" role="note" aria-label="Transform evidence">
              <p className="df2-vd-xform-head">
                <DtIcon name="layers" size={14} />
                {" "}Gates judged the transformed rows, not the raw source
                <code className="df2-vd-xform-hash" title="Recipe identity Execute is held to">
                  recipe {transformImage.recipe_hash || "—"}
                </code>
              </p>
              <ul className="df2-vd-xform-facts">
                <li>
                  <strong>{(transformImage.sample_rows_in ?? 0).toLocaleString()}</strong> sampled row(s)
                  read · <strong>{(transformImage.sample_rows_out ?? 0).toLocaleString()}</strong> reached
                  the gates
                </li>
                {Boolean(transformImage.sample_rows_removed) && (
                  <li>
                    <strong>{(transformImage.sample_rows_removed ?? 0).toLocaleString()}</strong> removed by
                    transform — absent by instruction, not quarantined and not lost
                  </li>
                )}
                {Boolean(transformImage.sample_rows_diverted) && (
                  <li>
                    <strong>{(transformImage.sample_rows_diverted ?? 0).toLocaleString()}</strong> diverted by
                    transform to quarantine, with the rule's reason
                  </li>
                )}
                {transformRetyped.length > 0 && (
                  <li>
                    Re-read carrier(s) after transform:{" "}
                    {transformRetyped.map(([column, carrier]) => `${column} → ${carrier}`).join(", ")}
                    {" — "}columns no step wrote keep their declared source type
                  </li>
                )}
              </ul>
              <p className="df2-vd-xform-limit">
                Sample-scoped evidence: these counts describe the rows Validate held, never the whole
                population. The source is not modified — the recipe runs on the read, and Execute is
                refused if its identity changes.
              </p>
            </div>
          )}

          {!running && preflight && executiveSummary && !preflight.passed && (
            <div className="df2-vd-exec-summary" role="alert">
              <p className="df2-vd-exec-summary-sub">{executiveSummary.subtitle}</p>
              {executiveSummary.untilLines.length > 0 && (
                <div className="df2-vd-exec-until">
                  <span className="df2-vd-exec-until-label">Cannot execute until</span>
                  <ul>
                    {executiveSummary.untilLines.map((line) => (
                      <li key={line}>{line}</li>
                    ))}
                  </ul>
                </div>
              )}
            </div>
          )}

          {!running && preflight && (
            (
              (Array.isArray(preflight.constraint_hints) && preflight.constraint_hints.some((hint) => {
                if (typeof hint === "string") return true;
                const sev = String((hint as Record<string, unknown>).severity || "info").toLowerCase();
                return sev === "info";
              }))
              || preflight.snowflake_warehouse_advice?.message
              || (preflight.referential_integrity && preflight.referential_integrity.coverage
                && preflight.referential_integrity.coverage !== "none")
            )
          ) && (
            <details className="df2-vd-hero-details">
              <summary>Coverage & proof notes</summary>
            <div className="df2-vd-soft-hints" role="note">
              <p className="df2-vd-soft-hints-label">Constraint coverage & advisories</p>
              {preflight.referential_integrity?.note && (
                <p
                  className="df2-vd-soft-hint"
                  title="Schema FK coverage is not population orphan proof"
                >
                  RI · {preflight.referential_integrity.coverage || "none"}
                  {preflight.referential_integrity.proven ? " · proven" : " · not proven"}
                  {" — "}
                  {preflight.referential_integrity.note}
                </p>
              )}
              {preflight.snowflake_warehouse_advice?.message && (
                <p className="df2-vd-soft-hint" title={preflight.snowflake_warehouse_advice.honesty || undefined}>
                  {preflight.snowflake_warehouse_advice.message}
                </p>
              )}
              {(preflight.constraint_hints || []).slice(0, 4).map((hint, i) => {
                if (typeof hint !== "string") {
                  const sev = String((hint as Record<string, unknown>).severity || "info").toLowerCase();
                  if (sev === "block" || sev === "ack_required") return null;
                }
                const textHint = typeof hint === "string"
                  ? hint
                  : String((hint as Record<string, unknown>).message || (hint as Record<string, unknown>).title || JSON.stringify(hint));
                return (
                  <p key={`hint-${i}`} className="df2-vd-soft-hint">{textHint}</p>
                );
              })}
            </div>
            </details>
          )}

          {running && (
            <p className="df2-vd-hero-summary">
              Evaluating source, destination, schema, mapping, dry-run, DDL, capacity, and reconcile. The timer is wall-clock — not a guessed percent.
            </p>
          )}
          {!running && preflight?.passed && (
            <p className="df2-vd-hero-summary">
              {decision === "review"
                ? (executiveSummary?.subtitle
                  ?? "Checks passed · review-grade — confirm API Validate before Execute")
                : (executiveSummary?.subtitle
                  ?? (uniquenessSampleOnly
                    ? "Gates passed on sample · population uniqueness not proven — Execute re-probes; not migration proven."
                    : "Execute-ready · not migration proven. Review cards below; Gate-8 proof is after write."))}
            </p>
          )}
          {!running && preflight && engineMsTotal > 0 && (
            <p className="df2-vd-hero-engine-meta">
              {formatDuration(engineMsTotal)} · {preflight.gates.length} gates
              {sampleScanned != null && sampleScanned > 0
                ? ` · ${sampleScanned.toLocaleString()} preview rows`
                : " · preview sample"}
            </p>
          )}
          {!running && preflight?.passed && !stripControlsApplied && onStripControlChars && (
            <p className="df2-vd-hero-engine-meta" role="status">
              Tip: strip format-control characters via <strong>Fix bad data…</strong> before Execute if a prior job failed on U+200B.
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

      {callableNote && !running && (
        <div className="df2-vd-callable" role="status">
          <DtIcon name="database" size={16} />
          <div>
            <strong>Callable extract — result-set snapshot</strong>
            <p>{callableNote}</p>
          </div>
        </div>
      )}

      {showDestShape && destShape && !running && (
        <div className="df2-vd-shape" role="status">
          <DtIcon name="layers" size={16} />
          <div className="df2-vd-shape-copy">
            <strong>{destShape.headline || "Dest-exists shape classified"}</strong>
            <p>
              {destShape.detail
                || "Writes are name-addressed. Dest-only columns stay off SET. CDC remains at-least-once upsert."}
            </p>
            {destShapeExtras.length > 0 && (
              <p>
                Extra source: {destShapeExtras.join(", ")} — remap or omit, never silent drop.
              </p>
            )}
            {destShapePreserve.length > 0 && (
              <p>
                Dest-only preserve: {destShapePreserve.join(", ")} — off SET.
              </p>
            )}
          </div>
          {destShapeCta && (
            <Button
              size="sm"
              variant="primary"
              leadingIcon={<DtIcon name={ACTION_ICON[destShapeCta.kind] ?? "layers"} size={14} />}
              onClick={() => handleSuggestedAction({
                kind: destShapeCta.kind,
                label: destShapeCta.label,
                column: destShapeCta.column,
              })}
            >
              {destShapeCta.label}
            </Button>
          )}
        </div>
      )}

      {/* One root cause → real Button CTAs (no Mapping proof here — evidence lives on Column matches). */}
      {!running && preflight && !preflight.passed && (
        <div className="df2-vd-assist-actions df2-vd-assist-remediate df2-vd-remediate-bar" aria-label="Suggested fixes">
          <span className="df2-vd-assist-actions-title">Suggested fixes</span>
          <div className="df2-vd-fix-actions">
            {duplicateRoot ? (
              <>
                {onOpenIdentitySettings && (
                  <Button
                    size="sm"
                    variant="primary"
                    disabled={remediating}
                    leadingIcon={<DtIcon name="settings" size={14} />}
                    onClick={onOpenIdentitySettings}
                    title={duplicateRoot.fixHint}
                  >
                    {duplicateRoot.primaryKey
                      ? `Fix identity (${duplicateRoot.primaryKey})`
                      : "Fix identity / sync mode"}
                  </Button>
                )}
                {uniqueKeySuggestions && uniqueKeySuggestions.length > 0 && onApplyPrimaryKey && (
                  <>
                    {uniqueKeySuggestions.slice(0, 2).map((s) => (
                      <Button
                        key={s.column}
                        size="sm"
                        variant="secondary"
                        disabled={remediating}
                        leadingIcon={<DtIcon name="check" size={14} />}
                        onClick={() => onApplyPrimaryKey(s.column)}
                        title={`Unique in ${s.sampleRows}-row sample — not full-table proof`}
                      >
                        Try PK · {s.column}
                      </Button>
                    ))}
                  </>
                )}
                {compositeKeySuggestions && compositeKeySuggestions.length > 0 && onApplyPrimaryKey && (
                  <>
                    {compositeKeySuggestions.slice(0, 2).map((s) => {
                      const joined = s.columns.join(",");
                      const label = s.columns.join(" + ");
                      return (
                        <Button
                          key={joined}
                          size="sm"
                          variant="secondary"
                          disabled={remediating}
                          leadingIcon={<DtIcon name="check" size={14} />}
                          onClick={() => onApplyPrimaryKey(joined)}
                          title={`Composite unique in ${s.sampleRows}-row sample — prefer when single-col is a false PK`}
                        >
                          Try composite · {label}
                        </Button>
                      );
                    })}
                  </>
                )}
                <Button
                  size="sm"
                  variant="ghost"
                  disabled={remediating || repairBusy || !preflight}
                  leadingIcon={<DtIcon name="sparkle" size={14} />}
                  onClick={() => void proposeDurableRepair()}
                >
                  Propose durable repair
                </Button>
              </>
            ) : (
              <>
                {isPrivilegeBlock && (
                  <Button
                    size="sm"
                    variant="primary"
                    disabled={remediating}
                    leadingIcon={<DtIcon name="shield" size={14} />}
                    onClick={() => {
                      window.location.hash = "#/connectors";
                    }}
                  >
                    Grant write privilege
                  </Button>
                )}
                {isPrivilegeBlock && onRunPreflight && (
                  <Button
                    size="sm"
                    variant="secondary"
                    disabled={remediating || running}
                    leadingIcon={<DtIcon name="activity" size={14} />}
                    onClick={() => void onRunPreflight()}
                  >
                    Re-validate after grant
                  </Button>
                )}
                {isConnectionBlock && (
                  <Button
                    size="sm"
                    variant="primary"
                    disabled={remediating}
                    leadingIcon={<DtIcon name="server" size={14} />}
                    onClick={() => {
                      window.location.hash = "#/connectors";
                    }}
                  >
                    Fix connector credentials
                  </Button>
                )}
                {isConnectionBlock && onRunPreflight && (
                  <Button
                    size="sm"
                    variant="secondary"
                    disabled={remediating || running}
                    leadingIcon={<DtIcon name="activity" size={14} />}
                    onClick={() => void onRunPreflight()}
                  >
                    Re-test &amp; re-validate
                  </Button>
                )}
                {isTypeMismatchBlock && (() => {
                  // Fidelity root already owns the Map CTA — avoid N Remap chips
                  // that restate the same TEXT→INTEGER collapse.
                  if (fidelityRoot) {
                    return onReviewMappings ? (
                      <Button
                        key="fidelity-open-map"
                        size="sm"
                        variant="primary"
                        disabled={remediating}
                        leadingIcon={<DtIcon name="layers" size={14} />}
                        onClick={() => onReviewMappings()}
                        title={fidelityRoot.fix || "Open Map to remap or Accept risk"}
                      >
                        Open Map · remap / Accept risk
                      </Button>
                    ) : null;
                  }
                  const isNoopTextRemap = (sug: string, cur: string) => {
                    const s = (sug || "").toUpperCase().trim();
                    const c = (cur || "").toUpperCase().trim();
                    if (!s || s === c) return true;
                    // Hide invent-VARCHAR for unbounded TEXT sinks — not a fidelity fix.
                    if (/^VARCHAR$/.test(s) && /^(TEXT|STRING|CLOB|LONGTEXT)\b/.test(c)) return true;
                    // Unbounded TEXT↔TEXT only; keep VARCHAR(n)↔TEXT / width widens.
                    const unbound = /^(TEXT|STRING|CLOB|LONGTEXT|NTEXT)$/;
                    const sBase = s.replace(/\s+.*$/, "");
                    const cBase = c.replace(/\s+.*$/, "");
                    return unbound.test(sBase) && unbound.test(cBase);
                  };
                  // Prefer Decision Kernel validation_findings (SSOT) over raw
                  // coercion_report when both exist — Map/Validate/Proof share rank.
                  const kernelFindings = (preflight?.validation_findings ?? [])
                    .filter((f) => f && (f.blocking === true || f.severity === "high")
                      && String(f.suggested_target_type || "").trim())
                    .filter((f) => !isNoopTextRemap(
                      String(f.suggested_target_type),
                      String(f.target_type || ""),
                    ))
                    .slice(0, 2);
                  const coercionBlocks = (preflight?.coercion_report?.columns ?? [])
                    .filter((c) => c.severity === "block" && c.suggested_target_type)
                    .filter((c) => !isNoopTextRemap(String(c.suggested_target_type), String(c.target_type || "")))
                    .slice(0, 2);
                  const remapCols = kernelFindings.length > 0
                    ? kernelFindings.map((f) => ({
                      source: String(f.source_column || ""),
                      target: String(f.target_column || f.source_column || ""),
                      toType: String(f.suggested_target_type || "VARCHAR"),
                    }))
                    : coercionBlocks.length > 0
                    ? coercionBlocks.map((c) => ({
                      source: c.source,
                      target: c.target,
                      toType: c.suggested_target_type || "VARCHAR",
                    }))
                    : typeMismatchColumns
                      .filter((c) => !isNoopTextRemap(c.toType, c.targetType || ""))
                      .slice(0, 2)
                      .map((c) => ({
                      source: c.source,
                      target: c.target,
                      toType: c.toType,
                    }));
                  return remapCols.map((col) => (
                    <Button
                      key={`${col.source}-${col.target}`}
                      size="sm"
                      variant="primary"
                      disabled={remediating || !onApplyAction}
                      leadingIcon={<DtIcon name="layers" size={14} />}
                      title={`Remap ${col.source} off typed ${col.target} → ${col.toType}`}
                      onClick={() =>
                        onApplyAction?.({
                          kind: "change_target_type",
                          label: `Remap ${col.source} → ${col.toType}`,
                          column: col.source,
                          target: col.target,
                          to_type: col.toType,
                        })
                      }
                    >
                      Remap {col.source} → {col.toType}
                    </Button>
                  ));
                })()}
                {isTypeMismatchBlock && !fidelityRoot && onReviewMappings && (
                  <Button
                    size="sm"
                    variant="secondary"
                    disabled={remediating}
                    leadingIcon={<DtIcon name="layers" size={14} />}
                    onClick={() => onReviewMappings()}
                    title="Open Map to review type mismatches"
                  >
                    Open Map
                  </Button>
                )}
                {showEncodingRemediation && (
                  <Button
                    size="sm"
                    variant="primary"
                    disabled={remediating}
                    leadingIcon={<DtIcon name="shield" size={14} />}
                    onClick={() => setBadDataOpen(true)}
                  >
                    Fix bad data…
                  </Button>
                )}
                {isFkOrphanBlock && orphanBarActions.map((action, i) => (
                  <Button
                    key={`${action.kind}-${action.column ?? ""}-${i}`}
                    size="sm"
                    variant={i === 0 ? "primary" : "secondary"}
                    disabled={remediating || (!onApplyAction && action.kind !== "fix_orphans")}
                    leadingIcon={<DtIcon name={ACTION_ICON[action.kind] ?? "alert"} size={14} />}
                    onClick={() => handleSuggestedAction(action)}
                    title={action.label}
                  >
                    {action.label}
                  </Button>
                ))}
                {!isTypeMismatchBlock
                  && !showEncodingRemediation
                  && !isPrivilegeBlock
                  && !isConnectionBlock
                  && !isFkOrphanBlock
                  && onReviewMappings && (
                  <Button
                    size="sm"
                    variant="primary"
                    disabled={remediating}
                    leadingIcon={<DtIcon name="layers" size={14} />}
                    onClick={() => onReviewMappings()}
                  >
                    Open Map to fix
                  </Button>
                )}
                {!duplicateRoot && !isPrivilegeBlock && !isConnectionBlock && (
                  <Button
                    size="sm"
                    variant="ghost"
                    disabled={remediating || repairBusy || !preflight}
                    leadingIcon={<DtIcon name="sparkle" size={14} />}
                    onClick={() => void proposeDurableRepair()}
                  >
                    Propose durable repair
                  </Button>
                )}
              </>
            )}
          </div>
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
            id="df2-vd-cell-preview"
            className={`df2-vd-cell-preview${coerceOnly ? " is-info" : " is-warn"}`}
            aria-label="Sample cell transform preview"
          >
            <div className="df2-vd-cell-preview-head">
              <strong>{coerceOnly ? "Type coercions in sample" : "Sample cells need attention"}</strong>
              <span>
                {cellPreview.quarantine_count} will quarantine · {cellPreview.coerce_count} will coerce ·{" "}
                {cellPreview.sample_rows_scanned} rows scanned
              </span>
              <Button
                size="sm"
                variant="ghost"
                onClick={() => {
                  if (revealCellPii) {
                    setRevealCellPii(false);
                    return;
                  }
                  const role = (readSession()?.role || "").toLowerCase();
                  if (role && !["admin", "owner", "editor", "operator"].includes(role)) {
                    window.alert("Reveal requires editor/admin role for this workspace.");
                    return;
                  }
                  if (window.confirm("Reveal unmasked cell preview values? Confirm only on a private screen.")) {
                    setRevealCellPii(true);
                  }
                }}
              >
                {revealCellPii ? "Hide PII" : "Reveal PII"}
              </Button>
            </div>
            <p className="df2-vd-cell-preview-hint">
              {coerceOnly ? (
                <>
                  This is <strong>not a failed validation</strong>.
                  Coerce means a value is converted to fit the destination type
                  (example: boolean <code>false</code> → text <code>&quot;false&quot;</code>).
                  Type-fit coercions keep the value; if a cell shows <strong>NULL</strong> or
                  fidelity collapse in the table below, that is <strong>not</strong> full fidelity — review before Execute.
                  {!preflight && (
                    <> The ring shows 0% ready because you have not run preflight yet — use <strong>Run preflight</strong> to score the gates.</>
                  )}
                </>
              ) : (
                <>
                  Quarantine <strong>holds unfit rows out</strong> of the primary write for Inspect / CSV
                  export after Run — they are not silently deleted and are not NULL-invented in the table.
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
                  {cell.raw ? (
                    <code title={revealCellPii ? cell.raw : "Preview values are masked when they look like PII"}>
                      {revealCellPii
                        ? cell.raw.slice(0, 80)
                        : (
                          /email|phone|ssn|name|address|mobile/i.test(cell.source)
                          || /@/.test(cell.raw)
                            ? (
                              /@/.test(cell.raw)
                                ? (() => {
                                  const [local, domain] = cell.raw.split("@");
                                  if (!domain) return `${cell.raw.slice(0, 2)}***`;
                                  return `${local.slice(0, 1)}${"*".repeat(Math.max(1, local.length - 1))}@${domain}`;
                                })()
                                : `${cell.raw.slice(0, 2)}***${cell.raw.slice(-2)}`
                            )
                            : cell.raw.slice(0, 48)
                        )}
                    </code>
                  ) : null}
                </li>
              ))}
            </ul>
            <div className="df2-vd-cell-preview-actions">
              {onRunPreflight && !preflight && !running && (
                <Button
                  size="sm"
                  variant="primary"
                  onClick={onRunPreflight}
                  leadingIcon={<DtIcon name="gate" size={14} />}
                >
                  Run preflight
                </Button>
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
                  : mappingProofSummary.destMode === "schema_pending"
                    ? "Schema pending — confirm destination before create-new"
                    : mappingProofSummary.destMode === "schema_incomplete"
                      ? "Schema incomplete — reload destination columns"
                    : "Matched to destination schema"}
                {" · "}
                every pair has confidence evidence and fidelity risks
              </span>
            </div>
            <Button
              size="sm"
              variant="secondary"
              onClick={onOpenMappingProof}
              leadingIcon={<DtIcon name="sparkle" size={14} />}
            >
              Open mapping proof
            </Button>
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
          {mappingProofSummary.classCounts
            && Object.keys(mappingProofSummary.classCounts).length > 0 && (
            <div className="df2-vd-map-proof-classes" aria-label="Calibrated confidence classes">
              {Object.entries(mappingProofSummary.classCounts)
                .sort((a, b) => b[1] - a[1])
                .slice(0, 5)
                .map(([label, n]) => (
                  <span key={label} className="df2-vd-proof-chip is-score" title="Evidence class from calibrated confidence">
                    {label} · {n}
                  </span>
                ))}
            </div>
          )}
        </div>
      )}

      {!running && preflight && (
        <div className="df2-vd-metrics" aria-label="Proof metrics">
          {/* Readiness lives in the hero ring — do not duplicate as a MetricChip. */}
          <MetricChip
            value={semantic * 100}
            label="Semantic"
            tone={decision === "approve" ? "approve" : heroTone}
          />
          <MetricChip
            value={quality == null ? null : quality * 100}
            label="Quality"
            tone={
              // Never paint green for not_profiled / 0% — that greenwashes Approve.
              qualityNotProfiled || quality == null || quality <= 0
                ? "review"
                : decision === "approve" && quality >= 0.7
                  ? "approve"
                  : "review"
            }
            emptyLabel="n/a"
          />
          <MetricChip
            value={(1 - complianceRisk) * 100}
            label={
              proof?.compliance?.requires_review && decision !== "approve"
                ? "Compliance review"
                : "Compliance"
            }
            tone={
              decision === "approve" && !(proof?.compliance?.requires_review) && complianceRisk <= 0.4
                ? "approve"
                : "review"
            }
          />
        </div>
      )}

      {!running && preflight && (
        <div className="df2-vd-sync-stack">
          <div className="df2-vd-sync-contract" role="status">
            <DtIcon name={appendLikeSync ? "alert" : "layers"} size={14} />
            <div>
              <strong>Sync · {syncMeta?.label || syncMode || "not set"}</strong>
              <p>
                {syncMeta?.detail
                  || "Open Destination → Advanced to choose overwrite, append, upsert, or CDC."}
                {appendLikeSync
                  ? " Re-runs may duplicate rows — prefer overwrite or upsert if that is not intended."
                  : ""}
                {upsertLikeSync
                  ? " Delivery is at-least-once upsert; exactly-once and at-most-once are not claimed."
                  : ""}
              </p>
            </div>
            {onOpenIdentitySettings && (
              <Button size="sm" variant="secondary" onClick={() => onOpenIdentitySettings()}>
                Sync / identity
              </Button>
            )}
          </div>
          {writeViaStaging && (
            <div className="df2-vd-sync-contract" role="status">
              <DtIcon name="shield" size={14} />
              <div>
                <strong>Staging on · promote clean rows</strong>
                <p>
                  Writes land in <code>{"{table}_df_staging"}</code> first; only clean rows promote to the primary table.
                  Bad rows stay in staging + DLQ — Validate does not prove the final table until Execute finishes promote.
                </p>
              </div>
            </div>
          )}
        </div>
      )}

      {!running && complianceAck?.actor && (
        <div className="df2-vd-enterprise-trust" role="note">
          <DtIcon name="shield" size={15} />
          <div>
            <strong>PII acknowledgment on file</strong>
            {complianceAck.actor}
            {complianceAck.at ? ` · ${complianceAck.at}` : ""}
            {complianceAck.reason ? ` — ${complianceAck.reason}` : ""}
          </div>
        </div>
      )}

      {!running && schemaDriftAck?.actor && (
        <div className="df2-vd-enterprise-trust" role="note">
          <DtIcon name="shield" size={15} />
          <div>
            <strong>Schema drift acknowledgment on file</strong>
            {schemaDriftAck.actor}
            {schemaDriftAck.at ? ` · ${schemaDriftAck.at}` : ""}
            {schemaDriftAck.reason ? ` — ${schemaDriftAck.reason}` : ""}
          </div>
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
                    ? (executiveSummary?.aiPromptHint
                      || "Plain-language explanation with one-click fixes.")
                    : explain
                      ? "Analysis ready — open to review suggested fixes."
                      : executiveSummary?.aiPromptHint
                        || "Open to analyze this validation result."}
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
                  {(explainParts && (
                    (explainParts.blockers.length > 0)
                    || (explainParts.warnings.length > 0)
                    || explainParts.isoGroup
                    || (proofWarnings.length > 0)
                  )) && (
                    <div className="df2-vd-explain-issues" aria-label="Validation issues">
                      {explainParts.blockers.length > 0 && (
                        <>
                          <span className="df2-vd-assist-actions-title">Blockers</span>
                          <ul>
                            {explainParts.blockers.map((issue, i) => (
                              <li key={`block-${issue.gate}-${issue.title}-${i}`} className="sev-block">
                                <strong>{explainIssueTitle(issue)}</strong>
                                {!isInternalGateId(issue.gate) && (
                                  <span className="df2-vd-explain-gate is-muted" title={issue.gate}>
                                    {gateLabel(issue.gate)}
                                    <code>{issue.gate}</code>
                                  </span>
                                )}
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
                        </>
                      )}
                      {(explainParts.isoGroup || explainParts.warnings.length > 0 || proofWarnings.length > 0) && (
                        <>
                          <span className="df2-vd-assist-actions-title">Warnings</span>
                          <ul className="df2-vd-explain-warnings">
                            {explainParts.isoGroup && (
                              <li className="sev-warning df2-vd-iso-issue">
                                <strong>{explainParts.isoGroup.title}</strong>
                                <span className="df2-vd-explain-gate is-muted">{explainParts.isoGroup.subtitle}</span>
                                <p>{explainParts.isoGroup.wireNote}</p>
                                {explainParts.isoGroup.columns.length > 0 && (
                                  <div className="df2-vd-chip-row">
                                    {explainParts.isoGroup.columns.map((col) => (
                                      <span key={col} className="df2-vd-chip is-static">{col}</span>
                                    ))}
                                  </div>
                                )}
                              </li>
                            )}
                            {explainParts.warnings.map((issue, i) => (
                              <li key={`warn-${issue.gate}-${issue.title}-${i}`} className={`sev-${issue.severity}`}>
                                <strong>{explainIssueTitle(issue)}</strong>
                                {!isInternalGateId(issue.gate) && (
                                  <span className="df2-vd-explain-gate is-muted" title={issue.gate}>
                                    {gateLabel(issue.gate)}
                                    <code>{issue.gate}</code>
                                  </span>
                                )}
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
                            {proofWarnings.map((w, i) => (
                              <li key={`proof-warn-${i}`} className="sev-warning">
                                <strong>Proof warning</strong>
                                <p>{w}</p>
                              </li>
                            ))}
                          </ul>
                        </>
                      )}
                    </div>
                  )}
                  {(explain.column_fixes?.length ?? 0) > 0 && (() => {
                    const isoFixRe = /ISO timestamps?/i;
                    const actionableFixes = explain.column_fixes.filter(
                      (fix) => !(fix.failed === 0 && isoFixRe.test(fix.suggested_fix || "")),
                    );
                    const isoFixes = explain.column_fixes.filter(
                      (fix) => fix.failed === 0 && isoFixRe.test(fix.suggested_fix || ""),
                    );
                    if (actionableFixes.length === 0 && isoFixes.length === 0) return null;
                    return (
                    <div className="df2-vd-column-fixes" aria-label="Column fixes">
                      <span className="df2-vd-assist-actions-title">Column fixes</span>
                      {isoFixes.length > 0 && (
                        <div className="df2-vd-iso-group" role="status">
                          <div className="df2-vd-iso-group-head">
                            <DtIcon name="activity" size={14} />
                            <strong>Timestamp normalize</strong>
                            <span>{isoFixes.length} column{isoFixes.length === 1 ? "" : "s"} · informational</span>
                          </div>
                          <div className="df2-vd-chip-row">
                            {isoFixes.map((fix) => (
                              <span key={fix.column} className="df2-vd-chip is-static">{fix.column}</span>
                            ))}
                          </div>
                        </div>
                      )}
                      {actionableFixes.length > 0 && (
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
                            {actionableFixes.map((fix) => (
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
                      )}
                    </div>
                    );
                  })()}
                  {explain.suggested_actions.length > 0 && (
                    <div className="df2-vd-assist-actions">
                      <span className="df2-vd-assist-actions-title">Suggested fixes</span>
                      <div className="df2-vd-fix-actions">
                        {collapseEncodingSuggestedActions(
                          explain.suggested_actions.filter((action) =>
                            action.kind !== "open_mapping_proof"
                            && action.kind !== "mapping_proof"
                            // Identity CTAs already live in Suggested fixes bar + rail.
                            && !(duplicateRoot && (
                              action.kind === "fix_source_keys"
                              || action.kind === "quarantine_and_rerun"
                              || action.kind === "review_mappings"
                            ))
                            // Encoding / strip CTAs belong only when encoding is the root cause.
                            && !(ENCODING_ACTION_KINDS.has(action.kind) && (showEncodingRemediation || isTypeMismatchBlock))
                            // Type-mismatch Map CTA already lives in the top Suggested fixes bar.
                            && !(isTypeMismatchBlock && (
                              action.kind === "review_mappings"
                              || action.kind === "change_target_type"
                              || action.kind === "open_mapping"
                            ))
                            && !(isFkOrphanBlock && (
                              action.kind === "fix_orphans"
                              || action.kind === "run_population_orphan_scan"
                              || action.kind === "review_mappings"
                            )),
                          ),
                        )
                          .map((action, i) => (
                          <Button
                            key={`${action.kind}-${action.column ?? ""}-${i}`}
                            size="sm"
                            variant={i === 0 ? "primary" : "secondary"}
                            onClick={() => handleSuggestedAction(action)}
                            disabled={
                              !onApplyAction
                              && action.kind !== "open_bad_data_fix"
                              && action.kind !== "normalize_control_chars"
                              && action.kind !== "quarantine_and_rerun"
                              && action.kind !== "fix_source_keys"
                              && action.kind !== "review_mappings"
                              && action.kind !== "fix_orphans"
                              && action.kind !== "run_population_orphan_scan"
                            }
                            title={action.label}
                            leadingIcon={<DtIcon name={ACTION_ICON[action.kind] ?? "sparkle"} size={14} />}
                          >
                            {action.label}
                          </Button>
                        ))}
                      </div>
                    </div>
                  )}
                  {explain.suggested_actions.length === 0 && !explain.passed && !duplicateRoot && (
                    <div className="df2-vd-assist-actions">
                      <div className="df2-vd-fix-actions">
                        <Button
                          size="sm"
                          variant="secondary"
                          onClick={() => void proposeDurableRepair()}
                          disabled={repairBusy || !preflight}
                          leadingIcon={<DtIcon name="sparkle" size={14} />}
                          loading={repairBusy}
                          loadingLabel="Proposing…"
                        >
                          Propose durable repair
                        </Button>
                      </div>
                    </div>
                  )}
                  {explain.suggested_actions.length === 0 && explain.passed && decision === "approve" && (
                    <p className="df2-vd-assist-clean">
                      <DtIcon name="check" size={13} /> No fixes needed — preflight approved for Execute.
                    </p>
                  )}
                  {explain.suggested_actions.length === 0 && explain.passed && decision !== "approve" && (
                    <p className="df2-vd-assist-clean">
                      <DtIcon name="alert" size={13} /> Gates look clear at review-grade — complete
                      acknowledgments or Map risk acceptance before Execute unlocks.
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
            const { status, message, issues, durationMs, privilegeProbe, evidenceScope } = statusForGate(meta, index);
            const scopeCoverage = evidenceScope?.coverage != null ? String(evidenceScope.coverage) : "";
            const scopeSample = typeof evidenceScope?.sample_rows === "number"
              ? evidenceScope.sample_rows
              : null;
            const scopeCols = typeof evidenceScope?.columns === "number"
              ? evidenceScope.columns
              : null;
            const scopeNote = evidenceScope?.note != null ? String(evidenceScope.note) : "";
            return (
              <article
                key={`${meta.key}-${index}`}
                className={`df2-vd-rule status-${status}${status === "pass" || status === "skip" ? " is-compact" : ""}`}
              >
                <div className="df2-vd-rule-top">
                  <span className="df2-vd-rule-icon"><DtIcon name={meta.icon} size={15} /></span>
                  <span className={`df2-vd-rule-status status-${status}`}>
                    {status === "pass" && <DtIcon name="check" size={11} />}
                    {status === "block" && <DtIcon name="x" size={11} />}
                    {status === "warn" && <DtIcon name="alert" size={11} />}
                    {status === "running" && <Spinner size="sm" label="" />}
                    {STATUS_LABEL[status] ?? status}
                  </span>
                </div>
                <strong className="df2-vd-rule-label">{meta.label}</strong>
                {(status === "block" || status === "warn" || status === "running" || status === "pending") && (
                  <p className="df2-vd-rule-desc">{meta.rule}</p>
                )}
                {status !== "pending" && status !== "pass" && message && (
                  <p className="df2-vd-rule-msg">{message}</p>
                )}
                {status === "pass" && message && (
                  <p className="df2-vd-rule-msg is-compact-msg" title={message}>{message}</p>
                )}
                {status !== "pending" && evidenceScope && (
                  <p className="df2-vd-rule-scope" title={scopeNote || undefined}>
                    <span className={`df2-vd-scope-chip cov-${scopeCoverage || "na"}`}>
                      {scopeCoverage === "sample"
                        ? "Sample"
                        : scopeCoverage === "full_schema"
                          ? "Full schema"
                          : scopeCoverage === "full_selected"
                            ? "Full selected"
                            : scopeCoverage === "pending"
                              ? "Pending"
                              : "Scope"}
                    </span>
                    {scopeSample != null && (
                      <span>{Number(scopeSample).toLocaleString()} row{Number(scopeSample) === 1 ? "" : "s"}</span>
                    )}
                    {scopeCols != null && (
                      <span>{Number(scopeCols).toLocaleString()} col{Number(scopeCols) === 1 ? "" : "s"}</span>
                    )}
                  </p>
                )}
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
                    {privilegeProbe.can_create_table != null && (
                      <span className="df2-vd-priv-method">
                        create={privilegeProbe.can_create_table ? "yes" : "no"}
                      </span>
                    )}
                    {privilegeProbe.can_write != null && (
                      <span className="df2-vd-priv-method">
                        write={privilegeProbe.can_write ? "yes" : "no"}
                      </span>
                    )}
                  </div>
                )}
                {status !== "pending" && /g2_destination|destination/i.test(meta.key) && (() => {
                  const gate = gateByKey.get(meta.key);
                  const exists = gate?.details?.table_exists;
                  const label =
                    exists === true
                      ? "table exists"
                      : exists === false
                        ? "create-new"
                        : /unknown|existence/i.test(message || "")
                          ? "existence unknown"
                          : null;
                  if (!label) return null;
                  return (
                    <div className="df2-vd-priv-probe status-unknown">
                      <span className="df2-vd-priv-chip">{label}</span>
                    </div>
                  );
                })()}
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
        <div id="df2-vd-gate8">
          <Gate8ProofCard
            report={reconciliation as Gate8Reconciliation}
            className="df2-vd-gate8"
            compact
            jobId={repairJobId || undefined}
            onOpenValidate={
              onReviewMappings
                ? () => onReviewMappings()
                : undefined
            }
            onOpenValidateLabel="Open Map"
            onOpenQuarantine={
              cellPreview && (cellPreview.quarantine_count > 0 || cellPreview.coerce_count > 0)
                ? () => document.getElementById("df2-vd-cell-preview")?.scrollIntoView({ behavior: "smooth", block: "start" })
                : undefined
            }
            onRerun={onRunPreflight ? () => { void onRunPreflight(); } : undefined}
            onRerunLabel="Re-run Validate"
          />
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
              <DtIcon name="check" size={13} />{" "}
              {sampleCompare.alignment === "key_aligned"
                ? "Every keyed sample value matched — sample-scoped only; not full population proof."
                : sampleCompare.alignment === "positional_only"
                  || sampleCompare.alignment === "unproven_identity"
                  || sampleCompare.identity_warning
                  ? `Sample values matched positionally — identity not proven${
                    sampleCompare.identity_warning
                      ? ` (${sampleCompare.identity_warning})`
                      : ""
                  }.`
                  : sampleCompare.error
                    ? `Read-back reported an error: ${sampleCompare.error}`
                    : "Every sampled value matched — confirm a primary key is present before treating this as keyed fidelity proof."}
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

      {decisionPath && !running && (
        <div
          className={`df2-vd-decision-path${decisionPath.executeUnlocked ? " is-ready" : " is-blocked"}`}
          aria-label="Migration decision path"
        >
          <div className="df2-vd-decision-path-head">
            <DtIcon name="gate" size={15} />
            <strong>{decisionPath.headline}</strong>
            {decisionPath.migrationProven ? (
              <span className="df2-vd-decision-path-badge is-proven">migration proven</span>
            ) : (
              <span className="df2-vd-decision-path-badge">not migration proven</span>
            )}
          </div>
          <p className="df2-vd-decision-path-note">{decisionPath.note}</p>
          <ol className="df2-vd-decision-path-steps">
            {decisionPath.steps.map((step) => (
              <li key={step.id} className={`is-${step.status}`} data-step={step.id}>
                <span className="df2-vd-decision-path-step-label">{step.label}</span>
                <span className="df2-vd-decision-path-step-summary">{step.summary}</span>
                {step.detail ? (
                  <span className="df2-vd-decision-path-step-detail">{step.detail}</span>
                ) : null}
              </li>
            ))}
          </ol>
          {(decisionPath.decisions?.length ?? 0) > 1 && (
            <div className="df2-vd-decision-path-more" aria-label="Additional root causes">
              <strong>Additional root causes</strong>
              <ul>
                {decisionPath.decisions!.slice(1).map((d) => (
                  <li key={d.key}>
                    <span>{d.title}</span>
                    <span className="df2-vd-decision-path-step-detail">
                      {d.steps.find((s) => s.id === "business_impact")?.summary
                        || d.steps.find((s) => s.id === "recommended_actions")?.summary
                        || ""}
                    </span>
                  </li>
                ))}
              </ul>
            </div>
          )}
        </div>
      )}

      {!running && (
        <div className="df2-vd-honesty" aria-label="Validate honesty controls">
          <div className="df2-vd-honesty-head">
            <DtIcon name="shield" size={15} />
            <strong>Coverage honesty</strong>
            {honestyControls.referentialIntegrity.proven ? (
              <span className="df2-vd-decision-path-badge is-proven">RI proven</span>
            ) : (
              <span className="df2-vd-decision-path-badge">RI not proven</span>
            )}
          </div>
          <p className="df2-vd-honesty-note">{honestyControls.note}</p>
          <ul className="df2-vd-honesty-list">
            <li>
              <strong>Referential integrity</strong>
              <span>{honestyControls.referentialIntegrity.headline}</span>
            </li>
            <li>
              <strong>ConversionClass</strong>
              <span>{honestyControls.conversionClasses.headline}</span>
            </li>
            {populationFit ? (
              <li>
                <strong>Population fit</strong>
                <span>
                  {populationFit.headline}
                  {populationFit.offenders.length > 0 &&
                  populationFit.offenders[0].exampleRows.length > 0 ? (
                    <em>
                      {" "}· first at row{" "}
                      {populationFit.offenders[0].exampleRows.slice(0, 3).join(", ")}
                    </em>
                  ) : null}
                </span>
              </li>
            ) : null}
            <li>
              <strong>Historical success</strong>
              <span>{honestyControls.historicalSuccess.headline}</span>
            </li>
            <li>
              <strong>Decision Artifact</strong>
              <span
                title={honestyControls.decisionArtifact.contentHash || undefined}
              >
                {honestyControls.decisionArtifact.headline}
                {honestyControls.decisionArtifact.schemaVersion
                  ? ` · ${honestyControls.decisionArtifact.schemaVersion}`
                  : ""}
              </span>
            </li>
            {honestyControls.ddlIdentityHash ? (
              <li>
                <strong>DDL identity</strong>
                <span title={honestyControls.ddlIdentityHash}>
                  Map→DDL fingerprint {honestyControls.ddlIdentityHash.slice(0, 12)}…
                </span>
              </li>
            ) : null}
          </ul>
          {onRunPopulationOrphanScanChange && (
            <label className="df2-vd-honesty-toggle">
              <input
                type="checkbox"
                checked={runPopulationOrphanScan}
                onChange={(e) => onRunPopulationOrphanScanChange(e.target.checked)}
                disabled={Boolean(running)}
              />
              <span>
                Run population orphan scan on next Validate
                <em> (expensive full-table anti-join — only path to RI proven)</em>
              </span>
            </label>
          )}
          {onRunPopulationOrphanScanChange && runPopulationOrphanScan && onRunPreflight && (
            <div className="df2-vd-honesty-actions">
              <Button size="sm" variant="secondary" onClick={() => onRunPreflight()}>
                Re-run Validate with population scan
              </Button>
            </div>
          )}
        </div>
      )}

      {preflight && displayBlockers.length > 0 && !running && (
        <div className="df2-vd-blockers">
          <div className="df2-vd-blockers-head">
            <DtIcon name="alert" size={15} />
            <strong>Fix before Run</strong>
            <span>{displayBlockers.length}</span>
          </div>
          <p className="df2-vd-blocker-precaution">
            Schema mismatches, bad data, and type hazards are blocked here on purpose.
            Follow the decision path above (root cause → risk contract → execute), re-validate,
            then Execute — Run should only surface operational issues like timeouts or connectivity.
            {duplicateRoot
              ? " Duplicate identity keys may fail several gates; they are grouped as one cause here while each gate card below still records its own check."
              : ""}
          </p>
          <ul>
            {displayBlockers.map((item) => {
              if (item.kind === "duplicate_root" || item.kind === "fidelity_root") {
                const isFidelity = item.kind === "fidelity_root";
                return (
                  <li key={item.key} className="df2-vd-blocker-root">
                    <strong>{item.title}</strong>
                    {item.impact && <span className="df2-vd-blocker-impact">{item.impact}</span>}
                    <span>{item.message}</span>
                    {item.gateChips && item.gateChips.length > 0 && (
                      <div className="df2-vd-root-gates" aria-label="Affected checks">
                        <span className="df2-vd-root-gates-label">Affected checks</span>
                        <div className="df2-vd-chip-row">
                          {item.gateChips.map((chip) => (
                            <span key={chip.id} className="df2-vd-chip is-static" title={chip.id}>
                              {chip.label}
                            </span>
                          ))}
                        </div>
                      </div>
                    )}
                    {item.issues && item.issues.length > 0 && (
                      <ul className="df2-vd-blocker-issues">
                        {item.issues.slice(0, 4).map((issue) => (
                          <li key={issue}>{issue}</li>
                        ))}
                      </ul>
                    )}
                    <div className="df2-vd-blocker-actions df2-vd-fix-actions">
                      {!isFidelity && onOpenIdentitySettings && (
                        <Button
                          size="sm"
                          variant="primary"
                          leadingIcon={<DtIcon name="settings" size={14} />}
                          onClick={onOpenIdentitySettings}
                          title={item.fix || duplicateRoot?.fixHint}
                        >
                          {duplicateRoot?.primaryKey
                            ? `Fix identity (${duplicateRoot.primaryKey})`
                            : "Fix identity / sync mode"}
                        </Button>
                      )}
                      {isFidelity && onReviewMappings && (
                        <Button
                          size="sm"
                          variant="primary"
                          leadingIcon={<DtIcon name="layers" size={14} />}
                          onClick={() => onReviewMappings()}
                          title={item.fix || "Open Map to remap or Accept risk"}
                        >
                          Open Map · remap / Accept risk
                        </Button>
                      )}
                    </div>
                    {item.fix && (
                      <p className="df2-vd-blocker-fix-note">{item.fix}</p>
                    )}
                    {item.why && <span className="df2-vd-blocker-why">{item.why}</span>}
                  </li>
                );
              }

              // Root kinds have no `source` payload. Fail closed — never read
              // `.details` on undefined (Transfer Studio crash).
              const b = item.source;
              if (!b) {
                return (
                  <li key={item.key}>
                    <strong>{item.title}</strong>
                    <span>{item.message}</span>
                    {item.fix && (
                      <p className="df2-vd-blocker-fix-note">{item.fix}</p>
                    )}
                    {item.why && <span className="df2-vd-blocker-why">{item.why}</span>}
                  </li>
                );
              }
              const issues = issueTextsFromDetails(b.details);
              const blockingCols = (preflight.coercion_report?.columns ?? []).filter((c) => c.severity === "block");
              const showIssueList = issues.length > 0 && !(b.id.includes("dry_run") && blockingCols.length > 0);
              return (
                <li key={item.key}>
                  <strong>{item.title}</strong>
                  <span>{item.message}</span>
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
                      <div className="df2-vd-fix-actions">
                        {blockingCols.slice(0, 6).map((col) => (
                          <Button
                            key={`${col.source}-${col.target}`}
                            size="sm"
                            variant="primary"
                            disabled={!onApplyAction || !col.suggested_target_type}
                            title={
                              col.suggested_fix
                              || `Remap ${col.source} to a ${col.suggested_target_type} column (Widen alone does not ALTER DDL)`
                            }
                            leadingIcon={<DtIcon name="layers" size={14} />}
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
                            Remap {col.source} → {col.suggested_target_type ?? "VARCHAR"}
                          </Button>
                        ))}
                      </div>
                    </div>
                  )}
                  {item.fix && (
                    <p className="df2-vd-blocker-fix-note">{item.fix}</p>
                  )}
                  {item.why && !b.id.includes("dry_run") && (
                    <span className="df2-vd-blocker-why">{item.why}</span>
                  )}
                  {schemaDriftCompatibilityHeadline(b.details) && (
                    <p className="df2-vd-blocker-fix-note">
                      {schemaDriftCompatibilityHeadline(b.details)}
                    </p>
                  )}
                  {/* Only the API's own ack flag may offer an unlocking approval.
                      A PII-shaped message with the flag false is a finding the
                      operator cannot approve away — say so instead of a button
                      that re-validates to the same block. */}
                  {b.details?.compliance_ack_required === false
                    && /pii\/compliance|compliance review/i.test(b.message) && (
                    <p className="df2-vd-blocker-fix-note">
                      PII approval does not clear this blocker — resolve the data or
                      schema cause named above, then re-run Validate.
                    </p>
                  )}
                  {b.details?.compliance_ack_required === true && onAcknowledgeCompliance && (
                    <div className="df2-vd-blocker-actions df2-vd-fix-actions">
                      <Button
                        size="sm"
                        variant="primary"
                        leadingIcon={<DtIcon name="shield" size={14} />}
                        onClick={() => onAcknowledgeCompliance()}
                        disabled={running}
                        title="Confirm your data governance policy allows moving these PII fields for this transfer"
                      >
                        Approve PII for this transfer
                      </Button>
                      {onReviewMappings && (
                        <Button
                          size="sm"
                          variant="secondary"
                          onClick={() => onReviewMappings()}
                        >
                          Review mappings
                        </Button>
                      )}
                    </div>
                  )}
                  {schemaDriftAllowsAcknowledge(b.details) && onAcknowledgeSchemaDrift && (
                    <div className="df2-vd-blocker-actions df2-vd-fix-actions">
                      <Button
                        size="sm"
                        variant="primary"
                        leadingIcon={<DtIcon name="shield" size={14} />}
                        onClick={() => onAcknowledgeSchemaDrift()}
                        disabled={running}
                        title="Record that you reviewed schema drift and chose to keep existing mappings for this run"
                      >
                        Acknowledge drift for this run
                      </Button>
                      {onReviewMappings && (
                        <Button
                          size="sm"
                          variant="secondary"
                          onClick={() => onReviewMappings()}
                        >
                          Open Map to include columns
                        </Button>
                      )}
                    </div>
                  )}
                  {schemaDriftRequiresRemap(b.details) && onReviewMappings && (
                    <div className="df2-vd-blocker-actions df2-vd-fix-actions">
                      <Button
                        size="sm"
                        variant="primary"
                        leadingIcon={<DtIcon name="layers" size={14} />}
                        onClick={() => onReviewMappings()}
                        disabled={running}
                        title="Hard-breaking schema change — remap or re-sign the contract. Acknowledge cannot green this gate."
                      >
                        Open Map to fix breaking change
                      </Button>
                    </div>
                  )}
                  {(
                    b.id === "constraint_fk"
                    || b.details?.remediation_kind === "acknowledge_fk_risk"
                    || /foreign key|fk_column_unmapped|destination_fk_metadata/i.test(b.message)
                  ) && onAcknowledgeFkRisk && (
                    <div className="df2-vd-blocker-actions df2-vd-fix-actions">
                      <Button
                        size="sm"
                        variant="primary"
                        leadingIcon={<DtIcon name="shield" size={14} />}
                        onClick={() => onAcknowledgeFkRisk()}
                        disabled={running}
                        title="Acknowledge FK mapping risk for this run — does not prove population referential integrity"
                      >
                        Acknowledge FK risk for this run
                      </Button>
                      {onReviewMappings && (
                        <Button
                          size="sm"
                          variant="secondary"
                          onClick={() => onReviewMappings()}
                        >
                          Map foreign-key columns
                        </Button>
                      )}
                    </div>
                  )}
                  {/* Encoding Fix CTA lives in the Suggested fixes bar + rail only. */}
                  {isTypeMismatchBlock && b.id.includes("dry_run") && typeMismatchColumns.length > 0 && (
                    <div className="df2-vd-blocker-actions">
                      <span className="df2-vd-assist-actions-title">
                        Type mismatch — Remap (Strip/Quarantine will not clear this)
                      </span>
                      <div className="df2-vd-fix-actions">
                        {typeMismatchColumns.slice(0, 6).map((col) => (
                          <Button
                            key={`block-${col.source}-${col.target}`}
                            size="sm"
                            variant="primary"
                            disabled={!onApplyAction}
                            leadingIcon={<DtIcon name="layers" size={14} />}
                            onClick={() =>
                              onApplyAction?.({
                                kind: "change_target_type",
                                label: `Remap ${col.source} → ${col.toType}`,
                                column: col.source,
                                target: col.target,
                                to_type: col.toType,
                              })
                            }
                          >
                            Remap {col.source} → {col.toType}
                          </Button>
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
        onOpenIdentitySettings={onOpenIdentitySettings}
        onOpenMap={() =>
          onReviewMappings?.(
            duplicateRoot?.primaryKey
              ? { focusSource: duplicateRoot.primaryKey }
              : undefined,
          )
        }
        onApplied={(updated) => {
          onRepairMappingsApplied?.(updated);
          pendingVerifyRef.current = true;
          pushRemediation(
            "Repair applied",
            `${updated.length} mapping(s) updated from approved proposal ${repairProposal?.id || ""}`,
            "Applied — re-running validation",
          );
        }}
        onDecided={(p) => {
          pushRemediation(
            p.status === "rejected" ? "Repair rejected" : "Repair reviewed",
            p.apply_result?.message
              || p.summary
              || p.id,
            p.status,
          );
        }}
      />
    </section>
  );
}
