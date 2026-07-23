/**
 * Validate storytelling helpers — present root causes clearly without changing
 * engine gate outcomes. Duplicate identity keys often fail G9 + G6 + G8; ISO
 * timestamp bind notes are warnings, not blockers.
 */
import { gateLabel } from "./preflightGates.js";
import type {
  CoercionColumn,
  PreflightGate,
  PreflightResult,
  ValidationIssue,
} from "./types.js";

const DUPLICATE_GATE_IDS = new Set([
  "g9_data_integrity",
  "g6_target_ddl",
  "g6_ddl",
  "g8_reconciliation",
]);

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
export function findDuplicateKeyRoot(preflight: PreflightResult | null | undefined): DuplicateKeyRoot | null {
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
  return {
    title: "Duplicate identity keys",
    impact: impactParts.join(" · "),
    fixHint:
      "Dedupe the source sample, pick the real primary key, or switch sync mode if append without uniqueness is intended — then re-run Validate.",
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

export function partitionCoercionColumns(columns: CoercionColumn[]): {
  isoNormalize: CoercionColumn[];
  otherActionable: CoercionColumn[];
  clean: CoercionColumn[];
} {
  const isoNormalize: CoercionColumn[] = [];
  const otherActionable: CoercionColumn[] = [];
  const clean: CoercionColumn[] = [];
  for (const col of columns) {
    if (col.severity === "ok") {
      clean.push(col);
      continue;
    }
    if (isIsoNormalizeCoercion(col)) isoNormalize.push(col);
    else otherActionable.push(col);
  }
  return { isoNormalize, otherActionable, clean };
}

export function buildDisplayBlockers(preflight: PreflightResult): DisplayBlocker[] {
  const root = findDuplicateKeyRoot(preflight);
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
      source: b,
    });
  }
  return items;
}

export function buildExecutiveSummary(preflight: PreflightResult | null | undefined): ExecutiveSummary | null {
  if (!preflight) return null;
  const root = findDuplicateKeyRoot(preflight);
  const displayBlockers = buildDisplayBlockers(preflight);
  const rootCauseCount = displayBlockers.length;
  const blockedGates = (preflight.gates ?? []).filter((g) => g.status === "block").length;
  const passed = preflight.passed_count ?? 0;
  const total = preflight.total_gates || (preflight.gates?.length ?? 0);
  const readinessCaption = `${passed}/${total} gates · readiness is the share of gates that passed`;

  if (preflight.passed) {
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
