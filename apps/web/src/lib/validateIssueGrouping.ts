/**
 * Validate storytelling helpers — present root causes clearly without changing
 * engine gate outcomes. Duplicate identity keys often fail G9 + G6 + G8; ISO
 * timestamp bind notes are warnings, not blockers.
 *
 * ``remapToTypeForMismatch`` mirrors backend
 * ``validation_assistant._remap_to_type_for_mismatch`` so one-click Fix CTAs
 * do not invent bare VARCHAR for UUID/ObjectId/DECIMAL/temporal blockers.
 */
import { gateLabel } from "./preflightGates.js";
import type {
  CoercionColumn,
  PreflightGate,
  PreflightResult,
  ValidationIssue,
  ValidationSuggestedAction,
} from "./types.js";

const DUPLICATE_GATE_IDS = new Set([
  "g9_data_integrity",
  "g6_target_ddl",
  "g6_ddl",
  "g8_reconciliation",
]);

/** Mirror apps/api/services/validation_assistant._remap_to_type_for_mismatch. */
export function remapToTypeForMismatch(sourceType: string, targetType: string): string {
  const src = (sourceType || "").trim();
  const tgt = (targetType || "").trim();
  const srcU = src.toUpperCase();
  const tgtU = tgt.toUpperCase();
  // Include CHAR(n) / NCHAR — backend normalize_logical_type maps these to string.
  const stringSink = /^(N?VAR)?CHAR|TEXT|STRING|JSON|LONGTEXT|CLOB|NVARCHAR|VARCHAR2/.test(tgtU);
  if ((srcU.includes("UUID") || srcU.includes("GUID") || srcU.includes("UNIQUEIDENTIFIER")) && stringSink) {
    return "UUID";
  }
  if (srcU.includes("OBJECTID") && stringSink) return "OBJECTID";
  const specialty = srcU.match(
    /^(INET|CIDR|IPV4|IPV6|IP|MACADDR8?|HSTORE|LTREE|PG_LSN|OBJECTID)\b/,
  );
  if (specialty && stringSink) return specialty[1];
  if (/FLOAT|DOUBLE|REAL/.test(srcU) && /DECIMAL|NUMBER|NUMERIC|INT/.test(tgtU)) {
    return "DOUBLE";
  }
  if (/DECIMAL|NUMERIC|NUMBER/.test(srcU) && /INT|FLOAT|DOUBLE|REAL/.test(tgtU)) {
    return src || "DECIMAL";
  }
  if (
    /TIMESTAMP|DATETIME|DATE|TIME/.test(srcU)
    && /TIMESTAMP|DATETIME|DATE|TIME|VARCHAR|TEXT|STRING|CHAR/.test(tgtU)
  ) {
    return src || "TIMESTAMP";
  }
  if (/VARCHAR|TEXT|STRING|CHAR/.test(srcU) && /INT|DECIMAL|NUMBER|FLOAT|DOUBLE/.test(tgtU)) {
    // Keep physical typed sink (backend suggest_remap → TEXT widen for create-new;
    // never stamp bare VARCHAR onto INT/DECIMAL and green empty invent).
    return tgt || "TEXT";
  }
  // Same-logical text/json create-new twins — keep destination type, never invent VARCHAR.
  if (
    (/VARCHAR|TEXT|STRING|CHAR|CLOB/.test(srcU) && /VARCHAR|TEXT|STRING|CHAR|CLOB/.test(tgtU))
    || (/JSON/.test(srcU) && /JSON/.test(tgtU))
  ) {
    return tgt || src || "TEXT";
  }
  return "VARCHAR";
}

const DUPLICATE_RE =
  /duplicate\s+(?:key|id|target\s+key)|keys?\s+repeat|primary\s+key\s+candidate.*duplicate|expect_column_unique/i;

export interface DuplicateKeyRoot {
  title: string;
  impact: string;
  fixHint: string;
  primaryKey: string | null;
  duplicateCount: number | null;
  sampleRows: number | null;
  gateIds: string[];
  gateLabels: string[];
  messages: string[];
  /** Original blocker ids absorbed into this root (for list filtering). */
  absorbedBlockerIds: string[];
}

export interface IsoNormalizeGroup {
  title: string;
  subtitle: string;
  columns: string[];
  wireNote: string;
  issues: ValidationIssue[];
}

export interface ExecutiveSummary {
  title: string;
  subtitle: string;
  untilLines: string[];
  rootCauseCount: number;
  readinessCaption: string;
  railLine: string;
  aiPromptHint: string | null;
}

export interface DisplayBlocker {
  key: string;
  kind: "duplicate_root" | "fidelity_root" | "blocker";
  title: string;
  message: string;
  impact?: string;
  gateChips?: { id: string; label: string }[];
  issues?: string[];
  fix?: string;
  why?: string;
  quarantinePolicy?: string;
  rollbackPolicy?: string;
  /** Sample rows examined for this root (honesty — not population proof). */
  affectedRowsSample?: number | null;
  /** Estimated population when known. */
  estimatedTotalRows?: number | null;
  /** Confidence band / score when present on the root. */
  confidenceNote?: string | null;
  /** Whether any rollback strategy is executable (usually false except staging discard). */
  rollbackExecutable?: boolean | null;
  suggested_actions?: ValidationSuggestedAction[];
  /** Original blocker for dry-run / encoding action hooks. */
  source?: PreflightResult["blockers"][number];
}

const FIDELITY_RE =
  /fidelity.?collapse|lossy|precision.?loss|scale.?truncat|nested.?shape.?collapse|declared type path collapses|width.?truncat|timezone.?shift/i;

const FIDELITY_GATE_IDS = new Set([
  "g3_type_compat",
  "g3_type_compatibility",
  "g4_transform",
  "g5_sample",
  "g5_sample_validation",
  "g8_reconciliation",
]);

export function isFidelityCollapseSignal(
  message: string,
  details?: Record<string, unknown> | null,
  gateId?: string,
): boolean {
  const blob = textBlob(message, details);
  if (FIDELITY_RE.test(blob)) return true;
  if (details?.fidelity_collapse === true) return true;
  const framing = asRecord(details?.framing);
  const kind = String(framing?.kind || details?.kind || "").toLowerCase();
  if (
    kind === "fidelity_collapse"
    || kind === "nested_shape_collapse"
    || kind === "nested_document_serialization"
  ) {
    return true;
  }
  if (gateId && FIDELITY_GATE_IDS.has(gateId) && /loss|truncat|collapse|\bcast\b/i.test(blob)) {
    return true;
  }
  return false;
}

/** Collapse multi-gate fidelity/lossy failures into one operator-facing root. */
export function findFidelityCollapseRoot(
  preflight: PreflightResult | null | undefined,
): {
  title: string;
  impact: string;
  fixHint: string;
  gateIds: string[];
  messages: string[];
  absorbedBlockerIds: string[];
} | null {
  if (!preflight) return null;
  const gateHits = (preflight.gates ?? []).filter(
    (g) => g && g.status === "block" && isFidelityCollapseSignal(g.message, g.details, g.id),
  );
  const blockerHits = (preflight.blockers ?? []).filter((b) =>
    Boolean(b && isFidelityCollapseSignal(b.message, b.details, b.id)),
  );
  if (gateHits.length + blockerHits.length < 2) return null;
  const gateIds = [...new Set([
    ...gateHits.map((g) => g.id),
    ...blockerHits.map((b) => b.id).filter((id) => FIDELITY_GATE_IDS.has(id)),
  ])];
  if (gateIds.length < 2 && blockerHits.length < 2) return null;
  const messages = [
    ...gateHits.map((g) => g.message),
    ...blockerHits.map((b) => b.message),
  ].filter(Boolean);
  return {
    title: "Lossy / fidelity collapse across type path",
    impact:
      "Declared types or nested shapes collapse fidelity on write — Accept risk on Map or remap carriers before Execute.",
    fixHint:
      "Open Map → review Approve/Review/Accept risk tiers → remap width/scale or ack intentional loss → re-run Validate.",
    gateIds,
    messages,
    absorbedBlockerIds: blockerHits.map((b) => b.id),
  };
}

function asRecord(value: unknown): Record<string, unknown> | null {
  return value && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : null;
}

function textBlob(message: string, details?: Record<string, unknown> | null): string {
  const parts = [message];
  if (!details) return parts.join(" ");
  for (const key of ["issue_texts", "errors", "issues", "warnings"] as const) {
    const raw = details[key];
    if (Array.isArray(raw)) {
      for (const item of raw) {
        if (typeof item === "string") parts.push(item);
        else if (item && typeof item === "object" && "message" in item) {
          parts.push(String((item as { message?: unknown }).message ?? ""));
        }
      }
    } else if (typeof raw === "string") {
      parts.push(raw);
    }
  }
  return parts.join(" ");
}

/**
 * True when the text describes format-control / character-encoding integrity
 * failures — not when a column is merely named ``encoding_id``.
 * Keep in sync with ``preflight_rules`` encoding keywords (no bare "encoding").
 */
export const ENCODING_INTEGRITY_RE =
  /format-control(?:\s+character)?|replacement character|encoding anomaly|character encoding|U\+200B|zero-width|strip_controls/i;

export function isEncodingIntegritySignal(text: string): boolean {
  return ENCODING_INTEGRITY_RE.test(text || "");
}

export function isDuplicateIdentitySignal(
  message: string,
  details?: Record<string, unknown> | null,
  gateId?: string,
): boolean {
  const blob = textBlob(message, details);
  if (DUPLICATE_RE.test(blob)) return true;
  if (gateId && DUPLICATE_GATE_IDS.has(gateId)) {
    if (typeof details?.duplicate_keys === "number" && details.duplicate_keys > 0) return true;
    if (Array.isArray(details?.sample_duplicates) && details.sample_duplicates.length > 0) return true;
  }
  return false;
}

function extractPrimaryKey(details?: Record<string, unknown> | null): string | null {
  if (!details) return null;
  const pk = details.primary_key;
  if (typeof pk === "string" && pk.trim()) return pk.trim();
  const rec = asRecord(pk);
  if (rec) {
    const target = rec.target ?? rec.source ?? rec.column;
    if (typeof target === "string" && target.trim()) return target.trim();
  }
  const m = textBlob("", details).match(/\bon\s+([A-Za-z_][\w.]*)/i);
  return m?.[1] ?? null;
}

function extractDuplicateCount(details?: Record<string, unknown> | null): number | null {
  if (!details) return null;
  if (typeof details.duplicate_keys === "number") return details.duplicate_keys;
  if (Array.isArray(details.sample_duplicates)) return details.sample_duplicates.length;
  const blob = textBlob("", details);
  const m =
    blob.match(/(\d+)\s+duplicate/i)
    || blob.match(/(\d+)\s+failures?/i)
    || blob.match(/(\d+)\s+keys?\s+repeat/i);
  return m ? Number(m[1]) : null;
}

function extractSampleRows(gate: PreflightGate | undefined, details?: Record<string, unknown> | null): number | null {
  const d = details ?? gate?.details;
  if (!d) return null;
  for (const key of ["target_rows", "sample_rows_scanned", "sample_size", "sample_rows"] as const) {
    const n = d[key];
    if (typeof n === "number" && n > 0) return n;
  }
  return null;
}

/** Collapse G9/G6/G8 duplicate-key failures into one operator-facing root cause. */
export function findDuplicateKeyRoot(
  preflight: PreflightResult | null | undefined,
  syncMode?: string,
): DuplicateKeyRoot | null {
  if (!preflight) return null;

  const gateHits = (preflight.gates ?? []).filter(
    (g) => g && g.status === "block" && isDuplicateIdentitySignal(g.message, g.details, g.id),
  );
  const blockerHits = (preflight.blockers ?? []).filter((b) =>
    Boolean(b && isDuplicateIdentitySignal(b.message, b.details, b.id)),
  );

  if (gateHits.length + blockerHits.length < 1) return null;
  // Only collapse when the same root shows up on more than one surface,
  // or a single gate clearly reports duplicate keys (still show as root).
  const gateIds = [
    ...new Set([
      ...gateHits.map((g) => g.id),
      ...blockerHits.map((b) => b.id).filter((id) => DUPLICATE_GATE_IDS.has(id) || /integrity|ddl|reconcil/i.test(id)),
    ]),
  ];
  if (gateIds.length === 0 && blockerHits.length === 0) return null;

  const detailSources = [
    ...gateHits.map((g) => g.details),
    ...blockerHits.map((b) => b.details),
  ].filter(Boolean) as Record<string, unknown>[];

  let primaryKey: string | null = null;
  let duplicateCount: number | null = null;
  let sampleRows: number | null = null;
  for (const d of detailSources) {
    primaryKey = primaryKey ?? extractPrimaryKey(d);
    duplicateCount = duplicateCount ?? extractDuplicateCount(d);
    sampleRows = sampleRows ?? extractSampleRows(undefined, d);
  }
  for (const g of gateHits) {
    sampleRows = sampleRows ?? extractSampleRows(g, g.details);
  }

  const messages = [
    ...gateHits.map((g) => g.message),
    ...blockerHits.map((b) => b.message),
  ].filter(Boolean);

  const impactParts: string[] = [];
  if (duplicateCount != null && primaryKey) {
    impactParts.push(`${duplicateCount.toLocaleString()} duplicate key(s) on ${primaryKey}`);
  } else if (duplicateCount != null) {
    impactParts.push(`${duplicateCount.toLocaleString()} duplicate key(s)`);
  } else if (primaryKey) {
    impactParts.push(`Duplicate values on identity column ${primaryKey}`);
  } else {
    impactParts.push("Duplicate identity keys in the Validate sample");
  }
  if (sampleRows != null) {
    impactParts.push(`${sampleRows.toLocaleString()}-row sample`);
  }

  const labels = gateIds.map((id) => gateLabel(id));
  const sync = (syncMode || "").toLowerCase();
  const appendLike =
    sync.includes("append")
    && !sync.includes("upsert")
    && !/overwrite|cdc|mirror|scd/.test(sync);
  const fixHint = appendLike
    ? (
      "Append sync can keep duplicate source rows only when that column is not the destination "
      + "PRIMARY KEY. If Validate or Execute still blocks, open Destination → Advanced and clear "
      + "Primary key (or pick a unique column), or dedupe the source."
    )
    : (
      "Open Destination → Advanced and set Primary key to a unique column "
      + "(or switch to Full refresh · Append / Overwrite if uniqueness is not required). "
      + "Map Approve cannot dedupe rows — then Re-run Validate."
    );
  return {
    title: "Duplicate identity keys",
    impact: impactParts.join(" · "),
    fixHint,
    primaryKey,
    duplicateCount,
    sampleRows,
    gateIds,
    gateLabels: labels,
    messages: [...new Set(messages)],
    absorbedBlockerIds: blockerHits.map((b) => b.id),
  };
}

export function isIsoNormalizeIssue(issue: ValidationIssue): boolean {
  const blob = `${issue.title}\n${issue.what}\n${issue.fix}\n${(issue.detail_messages || []).join("\n")}`;
  if (/type normalize at write/i.test(issue.title)) return true;
  if (/ISO timestamps?/i.test(blob) && /normaliz/i.test(blob)) return true;
  if (issue.severity === "warning" && /ISO-?8601|ISO timestamps?/i.test(blob)) return true;
  return false;
}

export function isIsoNormalizeCoercion(col: CoercionColumn): boolean {
  if (col.severity === "block") return false;
  // JSON scalar wraps are a domain change — never bury under "normalize, no loss".
  if ((col.json_scalar_wraps ?? 0) > 0) return false;
  if ((col.wire_normalize ?? 0) > 0 && col.failed === 0) return true;
  const fix = col.suggested_fix || "";
  return /ISO timestamps?/i.test(fix) && /normaliz/i.test(fix);
}

export function groupIsoNormalizeIssues(issues: ValidationIssue[]): {
  isoGroup: IsoNormalizeGroup | null;
  remaining: ValidationIssue[];
} {
  const iso: ValidationIssue[] = [];
  const remaining: ValidationIssue[] = [];
  for (const issue of issues) {
    if (isIsoNormalizeIssue(issue)) iso.push(issue);
    else remaining.push(issue);
  }
  if (iso.length === 0) return { isoGroup: null, remaining };

  const columns = [
    ...new Set(iso.flatMap((i) => i.columns || []).filter(Boolean)),
  ];
  // Infer column names from "Column 'x' →" when columns[] is empty.
  if (columns.length === 0) {
    for (const issue of iso) {
      const m = `${issue.what} ${issue.fix}`.match(/Column\s+'([^']+)'/i);
      if (m?.[1]) columns.push(m[1]);
    }
  }

  return {
    isoGroup: {
      title: "Timestamp normalize at write",
      subtitle: `${columns.length || iso.length} column${(columns.length || iso.length) === 1 ? "" : "s"} · no data loss expected`,
      columns: [...new Set(columns)],
      wireNote: "ISO-8601 → destination TIMESTAMP bind (seconds precision as shown)",
      issues: iso,
    },
    remaining,
  };
}

export function partitionExplainIssues(issues: ValidationIssue[]): {
  blockers: ValidationIssue[];
  warnings: ValidationIssue[];
  isoGroup: IsoNormalizeGroup | null;
} {
  const { isoGroup, remaining } = groupIsoNormalizeIssues(issues);
  const blockers: ValidationIssue[] = [];
  const warnings: ValidationIssue[] = [];
  for (const issue of remaining) {
    if (issue.severity === "block" || issue.severity === "error") blockers.push(issue);
    else warnings.push(issue);
  }
  return { blockers, warnings, isoGroup };
}

/** True when samples look green but declared types still collapse fidelity. */
export function isDeclaredFidelityCollapse(col: CoercionColumn): boolean {
  if (col.fidelity_collapse) return true;
  const kind = (col.framing?.kind || "").toLowerCase();
  return (
    kind === "fidelity_collapse"
    || kind === "nested_shape_collapse"
    || kind === "nested_document_serialization"
  );
}

export function partitionCoercionColumns(columns: CoercionColumn[]): {
  isoNormalize: CoercionColumn[];
  otherActionable: CoercionColumn[];
  clean: CoercionColumn[];
} {
  const isoNormalize: CoercionColumn[] = [];
  const otherActionable: CoercionColumn[] = [];
  const clean: CoercionColumn[] = [];
  for (const col of columns) {
    // Declared fidelity collapse must never land in "convert cleanly" — even if
    // a stale client payload still says severity=ok (Airbyte sample-green class).
    if (isDeclaredFidelityCollapse(col)) {
      otherActionable.push(col);
      continue;
    }
    if (col.severity === "ok") {
      clean.push(col);
      continue;
    }
    if (isIsoNormalizeCoercion(col)) isoNormalize.push(col);
    else otherActionable.push(col);
  }
  return { isoNormalize, otherActionable, clean };
}

export function buildDisplayBlockers(
  preflight: PreflightResult,
  syncMode?: string,
): DisplayBlocker[] {
  // Engine Root Cause SSOT — prefer when present (Module 2).
  const engineRoots = preflight.root_causes ?? [];
  if (engineRoots.length > 0) {
    const absorbed = new Set(engineRoots.flatMap((r) => r.absorbed_blocker_ids ?? []));
    const items: DisplayBlocker[] = engineRoots.map((r) => ({
      key: r.root_id,
      kind: r.kind === "duplicate_identity"
        ? "duplicate_root"
        : r.kind === "fidelity_collapse" || r.kind === "mapping_confidence"
          ? "fidelity_root"
          : "blocker",
      title: r.title,
      message: r.summary,
      impact: r.business_impact,
      gateChips: (r.impacted_gates ?? []).map((id) => ({ id, label: gateLabel(id) })),
      issues: [
        ...(r.affected_columns?.length
          ? [`Columns: ${r.affected_columns.slice(0, 8).join(", ")}${r.affected_columns.length > 8 ? "…" : ""}`]
          : []),
        ...(typeof r.affected_rows_sample === "number"
          ? [`Sample rows: ${r.affected_rows_sample}`]
          : []),
        ...(typeof r.estimated_total_rows === "number"
          ? [`Estimated rows: ${r.estimated_total_rows.toLocaleString()}`]
          : []),
        ...(r.alternative_fixes ?? []).slice(0, 3),
      ],
      fix: r.recommended_fix,
      quarantinePolicy: r.quarantine_policy,
      rollbackPolicy: r.rollback_policy,
      affectedRowsSample: typeof r.affected_rows_sample === "number" ? r.affected_rows_sample : null,
      estimatedTotalRows: typeof r.estimated_total_rows === "number" ? r.estimated_total_rows : null,
      confidenceNote: r.risk_level ? `Risk level: ${r.risk_level}` : null,
      // Engine roots document DOCUMENT_ONLY — staging discard is the only executable product path.
      rollbackExecutable: String(r.rollback_policy || "").toUpperCase() === "DISCARD_STAGING",
      why: [
        r.business_impact,
        r.recovery_strategy,
        r.quarantine_policy ? `Quarantine: ${r.quarantine_policy}` : "",
        r.rollback_policy ? `Rollback: ${r.rollback_policy}` : "",
      ].filter(Boolean).join(" "),
    }));
    for (const b of preflight.blockers ?? []) {
      if (!b) continue;
      if (absorbed.has(b.id)) continue;
      if ((b.details as { root_cause?: boolean } | undefined)?.root_cause) continue;
      items.push({
        key: b.id,
        kind: "blocker",
        title: gateLabel(b.id),
        message: b.message,
        issues: Array.isArray(b.details?.issue_texts)
          ? (b.details.issue_texts as string[])
          : undefined,
        fix: b.guidance?.fix,
        why: b.guidance?.why,
        suggested_actions: b.guidance?.suggested_actions,
        source: b,
      });
    }
    return items;
  }

  const root = findDuplicateKeyRoot(preflight, syncMode);
  const fidelityRoot = findFidelityCollapseRoot(preflight);
  const absorbed = new Set([
    ...(root?.absorbedBlockerIds ?? []),
    ...(fidelityRoot?.absorbedBlockerIds ?? []),
  ]);
  const items: DisplayBlocker[] = [];

  if (root) {
    items.push({
      key: "duplicate-identity-keys",
      kind: "duplicate_root",
      title: root.title,
      message: root.messages[0] || root.impact,
      impact: root.impact,
      gateChips: root.gateIds.map((id) => ({ id, label: gateLabel(id) })),
      issues: root.messages.slice(1),
      fix: root.fixHint,
      why: "The same identity-key problem failed Data integrity, Target DDL, and Sample reconciliation — one root cause, three gate checks.",
    });
  }

  if (fidelityRoot) {
    items.push({
      key: "fidelity-collapse",
      kind: "fidelity_root",
      title: fidelityRoot.title,
      message: fidelityRoot.messages[0] || fidelityRoot.impact,
      impact: fidelityRoot.impact,
      gateChips: fidelityRoot.gateIds.map((id) => ({ id, label: gateLabel(id) })),
      issues: fidelityRoot.messages.slice(1),
      fix: fidelityRoot.fixHint,
      why: "The same lossy carrier path failed multiple gates — one Map risk decision, not N separate warnings.",
    });
  }

  for (const b of preflight.blockers ?? []) {
    if (!b) continue;
    if (absorbed.has(b.id)) continue;
    if (root && isDuplicateIdentitySignal(b.message, b.details, b.id)) continue;
    if (fidelityRoot && isFidelityCollapseSignal(b.message, b.details, b.id)) continue;
    items.push({
      key: b.id,
      kind: "blocker",
      title: gateLabel(b.id),
      message: b.message,
      issues: Array.isArray(b.details?.issue_texts)
        ? (b.details.issue_texts as string[])
        : undefined,
      fix: b.guidance?.fix,
      why: b.guidance?.why,
      suggested_actions: b.guidance?.suggested_actions,
      source: b,
    });
  }
  return items;
}

/** True when G9 only sampled uniqueness — Execute may still run a full source probe. */
export function isSampleUniquenessOnly(
  preflight: PreflightResult | null | undefined,
): boolean {
  if (!preflight) return false;
  const g9 = (preflight.gates ?? []).find((g) => g.id === "g9_data_integrity");
  const details = (g9?.details || {}) as Record<string, unknown>;
  const probe = details.source_uniqueness_probe as
    | { ran?: boolean; coverage?: string }
    | undefined;
  if (probe && (probe.ran === false || String(probe.coverage || "").toLowerCase() === "sample")) {
    return true;
  }
  const scope = details.evidence_scope as { coverage?: string; note?: string } | undefined;
  if (scope && String(scope.coverage || "").toLowerCase() === "sample") {
    return true;
  }
  const blob = `${g9?.message || ""} ${scope?.note || ""} ${JSON.stringify(details)}`;
  return /population uniqueness not proven/i.test(blob);
}

export function buildExecutiveSummary(
  preflight: PreflightResult | null | undefined,
  syncMode?: string,
): ExecutiveSummary | null {
  if (!preflight) return null;
  const root = findDuplicateKeyRoot(preflight, syncMode);
  const displayBlockers = buildDisplayBlockers(preflight, syncMode);
  const rootCauseCount = displayBlockers.length;
  const blockedGates = (preflight.gates ?? []).filter((g) => g.status === "block").length;
  const passed = preflight.passed_count ?? 0;
  const total = preflight.total_gates || (preflight.gates?.length ?? 0);
  const readinessCaption = `${passed}/${total} gates · readiness is the share of gates that passed`;
  const complianceOnly = Boolean(
    preflight.proof_bundle?.transfer_decision?.compliance_only
    || displayBlockers.every((b) =>
      /pii\/compliance|compliance review/i.test(b.message)
      || b.source?.details?.compliance_ack_required === true
    )
  ) && rootCauseCount > 0 && blockedGates === 0;
  const sampleUniqueness = isSampleUniquenessOnly(preflight);

  if (preflight.passed) {
    const decision = preflight.proof_bundle?.transfer_decision?.decision;
    // Missing decision must never claim Execute unlocked (align with rail / isGovernedExecuteReady).
    if (decision !== "approve") {
      return {
        title: "Review before Execute",
        subtitle: `${passed}/${total} checks passed · ${
          decision === "review" ? "review-grade / local preflight" : "awaiting transfer decision"
        } — confirm API Validate before Execute`,
        untilLines: ["Confirm API preflight with decision approve (not browser-local only)"],
        rootCauseCount: 0,
        readinessCaption,
        railLine: "Review-grade — Execute not fully unlocked",
        aiPromptHint: "Why is this transfer still in review after Validate?",
      };
    }
    if (sampleUniqueness) {
      return {
        title: "Execute-ready · uniqueness sample-only",
        subtitle:
          `${passed}/${total} checks passed · population uniqueness not proven on Validate — `
          + "Execute re-probes the source; append/create-new may warn, upsert/PK will block",
        untilLines: [
          "Prefer upsert + unique primary key when identity must be unique",
          "Re-run Validate after source probe wiring if this route should fail closed earlier",
        ],
        rootCauseCount: 0,
        readinessCaption,
        railLine: "Execute unlocked · uniqueness not population-proven",
        aiPromptHint: "Why did Validate pass uniqueness on a sample but Execute fail on duplicates?",
      };
    }
    return {
      title: "Execute-ready · not migration proven",
      subtitle: `${passed}/${total} checks passed · Execute unlocked — Gate-8 post-write proof still required`,
      untilLines: [],
      rootCauseCount: 0,
      readinessCaption,
      railLine: "Execute unlocked · migration_proven pending write",
      aiPromptHint: null,
    };
  }

  if (complianceOnly) {
    return {
      title: "Approve PII to unlock Execute",
      subtitle: `${passed}/${total} engine gates passed · compliance acknowledgment required`,
      untilLines: ["Confirm governance policy allows moving detected PII fields"],
      rootCauseCount: 1,
      readinessCaption,
      railLine: "Awaiting PII / compliance approval",
      aiPromptHint: "Why is PII review required for this transfer?",
    };
  }

  const untilLines: string[] = [];
  if (root) untilLines.push("Duplicate identity keys resolved");
  if (displayBlockers.some((b) => b.kind === "fidelity_root")) {
    untilLines.push("Lossy / fidelity risk acknowledged or remapped on Map");
  }
  for (const item of displayBlockers) {
    if (item.kind === "duplicate_root" || item.kind === "fidelity_root") continue;
    untilLines.push(item.title);
  }
  // Cap bullets so the hero stays scannable.
  const until = untilLines.slice(0, 4);

  return {
    title: "Validation blocked",
    subtitle: `${rootCauseCount} blocking issue${rootCauseCount === 1 ? "" : "s"} · ${passed}/${total} checks passed · Execute stays locked`,
    untilLines: until,
    rootCauseCount,
    readinessCaption,
    railLine: root
      ? `Blocked by duplicate identity keys${root.primaryKey ? ` on ${root.primaryKey}` : ""}`
      : blockedGates > 0
        ? `Blocked by ${blockedGates} gate${blockedGates === 1 ? "" : "s"}`
        : "Validation blocked — fix issues before Execute",
    aiPromptHint: root ? "Why are duplicate IDs blocking this transfer?" : null,
  };
}


/** Cap and dedupe Validate remediation CTAs — one primary per kind/column family. */
export function rankAndDedupeSuggestedActions(
  actions: ValidationSuggestedAction[] | null | undefined,
  max = 6,
): ValidationSuggestedAction[] {
  if (!actions?.length) return [];
  const priority = (kind: string): number => {
    switch (kind) {
      case "fix_source_keys":
        return 0;
      case "change_target_type":
        return 1;
      case "add_transform":
        return 2;
      case "open_bad_data_fix":
      case "normalize_control_chars":
      case "quarantine_and_rerun":
        return 3;
      case "map_column":
      case "review_mappings":
      case "rerun_mapping":
        return 4;
      case "check_connection":
        return 5;
      default:
        return 9;
    }
  };
  const sorted = [...actions].sort((a, b) => priority(String(a.kind)) - priority(String(b.kind)));
  const seenExact = new Set<string>();
  const seenFamily = new Set<string>();
  const out: ValidationSuggestedAction[] = [];
  for (const action of sorted) {
    const kind = String(action.kind || "");
    const col = String(action.column || action.target || "");
    const exact = [kind, col, String(action.to_type || ""), String(action.transform || ""), String(action.label || "")].join("|");
    if (seenExact.has(exact)) continue;
    const family =
      kind === "map_column" || kind === "review_mappings" || kind === "rerun_mapping"
        ? `map:${col || "*"}`
        : kind === "normalize_control_chars" || kind === "quarantine_and_rerun" || kind === "open_bad_data_fix"
          ? "encoding"
          : `${kind}:${col || "*"}`;
    if (seenFamily.has(family)) continue;
    seenExact.add(exact);
    seenFamily.add(family);
    out.push(action);
    if (out.length >= max) break;
  }
  return out;
}

