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
    return "VARCHAR";
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
  kind: "duplicate_root" | "blocker";
  title: string;
  message: string;
  impact?: string;
  gateChips?: { id: string; label: string }[];
  issues?: string[];
  fix?: string;
  why?: string;
  suggested_actions?: ValidationSuggestedAction[];
  /** Original blocker for dry-run / encoding action hooks. */
  source?: PreflightResult["blockers"][number];
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
    (g) => g.status === "block" && isDuplicateIdentitySignal(g.message, g.details, g.id),
  );
  const blockerHits = (preflight.blockers ?? []).filter((b) =>
    isDuplicateIdentitySignal(b.message, b.details, b.id),
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
  const root = findDuplicateKeyRoot(preflight, syncMode);
  const absorbed = new Set(root?.absorbedBlockerIds ?? []);
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

  for (const b of preflight.blockers) {
    if (absorbed.has(b.id)) continue;
    if (root && isDuplicateIdentitySignal(b.message, b.details, b.id)) continue;
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
    return {
      title: "Ready to transfer",
      subtitle: `${passed}/${total} checks passed · Execute unlocked`,
      untilLines: [],
      rootCauseCount: 0,
      readinessCaption,
      railLine: "Ready to execute",
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
  for (const item of displayBlockers) {
    if (item.kind === "duplicate_root") continue;
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
