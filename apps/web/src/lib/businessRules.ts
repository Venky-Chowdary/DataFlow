/**
 * Apply a compiled rule workbook onto studio mappings.
 *
 * The server already classified and bound names. This module only merges
 * those artifacts onto EditableMapping the way an operator would have typed
 * them — never inventing a dest column, never dropping an unused dest.
 */

import type { EditableMapping, MappingBusinessRule, MappingTransform } from "./mapping";
import { applyDeclaredSourceZone, applyTransformChange } from "./mapping";
import type { ShapeStepWire } from "./shape";

export interface CompiledRule {
  source_table?: string;
  source_column: string;
  map_source?: string;
  dest_table?: string;
  dest_column: string;
  rule_text: string;
  kind: string;
  kind_label: string;
  interpretation?: string;
  action?: string;
  plane: string;
  confidence: number;
  contract?: { type: string; value?: string; values?: string[]; op?: string; pattern?: string } | null;
  transform?: string;
  engine_transform?: string;
  timezone?: string;
  code_crosswalk?: Record<string, string> | null;
  shape_step?: ShapeStepWire | null;
  status: string;
  issues?: string[];
  bind_method?: string;
  bind_score?: number;
  date_format?: string;
  named_rule?: string;
  resolved_rule?: string;
  on_fail?: string;
  unknown_code_policy?: { action: string; value?: string };
  provenance?: { sheet?: string; row?: number };
}

export interface RuleCoverage {
  detected: number;
  executable: number;
  review: number;
  conflict: number;
  writes: number;
  validations: number;
  percent: number;
}

export interface HeaderRoleEvidence {
  header: string;
  role: string;
  method: string;
  confidence: number;
  reason: string;
  sheet?: string;
}

export interface RuleCompileReport {
  filename: string;
  rule_count: number;
  buckets: {
    executable: number;
    needs_confirmation: number;
    conflict: number;
  };
  coverage?: RuleCoverage;
  unused_dest_columns: string[];
  unused_dest_count: number;
  unmapped_source_columns?: string[];
  unmapped_source_count?: number;
  truncated_rows?: number;
  header_roles?: HeaderRoleEvidence[];
  sheet_kinds?: Array<{ sheet: string; kind: string; rows: number }>;
  bind_methods?: Record<string, number>;
  lookup_coverage?: Array<{ source: string; dest: string; pairs: number }>;
  named_rules?: string[];
  matcher?: string;
  sync_mode?: string;
  source_tables?: string[];
  source_catalog_tables?: string[];
  dest_tables?: string[];
  dest_catalog_tables?: string[];
  contracts?: CompiledRule[];
  projection?: Array<{
    source_table: string;
    columns: Array<{ source_column: string; dest_column: string; dest_table?: string }>;
  }>;
  shape_steps_by_table?: Array<{ source_table: string; steps: ShapeStepWire[] }>;
  shape_steps: ShapeStepWire[];
  rules: CompiledRule[];
  honesty: string;
}

function asTransform(value: string | undefined): MappingTransform {
  const known: MappingTransform[] = [
    "none", "trim", "upper", "lower", "date_iso", "time_iso", "hash_pii",
    "cast_number", "cast_integer", "cast_boolean", "parse_json", "binary",
    "phone", "email", "currency", "percentage", "strip_controls",
    "identity_specialty", "assume_timezone", "omit",
  ];
  return (known as string[]).includes(value || "") ? (value as MappingTransform) : "none";
}

export function namedRuleDisplay(rule: CompiledRule): string {
  if (rule.named_rule) {
    const text = (rule.resolved_rule || rule.rule_text || "").trim();
    return text ? `${rule.named_rule} · ${text}` : rule.named_rule;
  }
  return rule.rule_text || "(direct)";
}

export function proofRuleClaim(report: RuleCompileReport): string {
  const executable = report.coverage?.executable ?? report.buckets.executable;
  const review = report.coverage?.review ?? report.buckets.needs_confirmation;
  const conflict = report.coverage?.conflict ?? report.buckets.conflict;
  if (review || conflict) {
    return `Migration compiled ${executable} executable business rule(s); review remains`;
  }
  return `Migration verified against ${executable} business rules`;
}

function coverageFromRules(rules: CompiledRule[]): RuleCoverage {
  const detected = rules.length;
  const executable = rules.filter((rule) => rule.status === "executable").length;
  const review = rules.filter((rule) => rule.status === "needs_confirmation").length;
  const conflict = rules.filter((rule) => rule.status === "conflict").length;
  return {
    detected,
    executable,
    review,
    conflict,
    writes: rules.filter((rule) => (
      rule.status === "executable" && !["contract", "omit", "join"].includes(rule.kind)
    )).length,
    validations: rules.filter((rule) => rule.status === "executable" && rule.kind === "contract").length,
    percent: detected ? Math.round((100 * executable) / detected) : 0,
  };
}

export function ruleToEvidence(rule: CompiledRule): MappingBusinessRule {
  const expanded = namedRuleDisplay(rule);
  return {
    kind: rule.kind,
    kindLabel: rule.kind_label,
    text: expanded,
    status: rule.status,
    confidence: rule.confidence,
    sheet: rule.provenance?.sheet,
    row: rule.provenance?.row,
    issues: rule.issues,
  };
}

function applyOne(mapping: EditableMapping, rule: CompiledRule): EditableMapping {
  const evidence = ruleToEvidence(rule);
  const transform = asTransform(rule.transform);
  const lockedDest = Boolean(
    mapping.approved
    && mapping.target
    && rule.dest_column
    && mapping.target.toLowerCase() !== rule.dest_column.toLowerCase(),
  );
  if (lockedDest) {
    return {
      ...mapping,
      businessRule: { ...evidence, status: "conflict" },
      requiresReview: true,
      reason: `Workbook dest “${rule.dest_column}” conflicts with locked “${mapping.target}”.`,
    };
  }
  let next: EditableMapping = {
    ...mapping,
    target: rule.status === "executable" && rule.dest_column
      ? rule.dest_column
      : mapping.target,
    businessRule: evidence,
    reason: rule.rule_text
      ? `${rule.kind_label}: ${rule.rule_text}`
      : rule.kind_label,
    requiresReview: rule.status !== "executable" || mapping.requiresReview,
  };
  if (rule.status === "executable" && rule.code_crosswalk) {
    next = { ...next, codeCrosswalk: { ...rule.code_crosswalk } };
  }
  const namedZone = (rule.timezone || "").trim()
    || ((rule.engine_transform || "").toLowerCase().startsWith("assume_timezone:")
      ? String(rule.engine_transform).slice("assume_timezone:".length).trim()
      : "");
  if (rule.status === "executable" && namedZone) {
    next = applyDeclaredSourceZone(next, namedZone);
  } else if (rule.status === "executable" && transform && transform !== "none") {
    next = applyTransformChange(next, transform);
  }
  if (rule.status === "executable" && rule.confidence >= 0.9 && next.target) {
    next = { ...next, approved: true, requiresReview: false };
  }
  return next;
}

export function mergeBusinessRules(
  mappings: EditableMapping[],
  report: RuleCompileReport | null,
  options?: { sourceTable?: string },
): EditableMapping[] {
  if (!report?.rules?.length) return mappings;
  const tableFold = (options?.sourceTable || "").trim().toLowerCase();
  const next = mappings.map((m) => ({ ...m }));
  const indexBySource = new Map<string, number>();
  next.forEach((m, i) => {
    if (m.source) indexBySource.set(m.source.toLowerCase(), i);
  });

  for (const rule of report.rules) {
    if (rule.kind === "contract" || rule.plane === "validate") continue;
    if (tableFold) {
      const ruleTable = (rule.source_table || "").trim().toLowerCase();
      if (ruleTable && ruleTable !== tableFold) continue;
    }
    const source = (rule.map_source || rule.source_column || "").trim();
    if (!source) continue;
    const idx = indexBySource.get(source.toLowerCase());
    if (idx === undefined) {
      if (rule.status !== "executable") continue;
      const created: EditableMapping = applyOne(
        {
          source,
          target: rule.dest_column || "",
          confidence: rule.confidence,
          approved: false,
          reason: rule.kind_label,
          transform: "none",
        },
        rule,
      );
      next.push(created);
      indexBySource.set(source.toLowerCase(), next.length - 1);
      continue;
    }
    const existing = next[idx];
    const dest = (rule.dest_column || "").trim();
    const operatorLocked = Boolean(
      existing.approved
      && existing.target
      && dest
      && existing.target.toLowerCase() !== dest.toLowerCase()
      && !existing.businessRule,
    );
    if (operatorLocked) {
      next[idx] = applyOne(existing, rule);
      continue;
    }
    if (
      rule.status === "executable"
      && dest
      && existing.target
      && existing.target.toLowerCase() !== dest.toLowerCase()
    ) {
      const created = applyOne(
        {
          source,
          target: "",
          confidence: rule.confidence,
          approved: false,
          reason: rule.kind_label,
          transform: "none",
        },
        rule,
      );
      next.push(created);
      continue;
    }
    next[idx] = applyOne(existing, rule);
  }
  return next;
}

export function ruleReportSummary(report: RuleCompileReport): string {
  const { executable, needs_confirmation, conflict } = report.buckets;
  const unused = report.unused_dest_count
    ? ` · ${report.unused_dest_count} dest column(s) unused (not written)`
    : "";
  const unmapped = report.unmapped_source_count
    ? ` · ${report.unmapped_source_count} source column(s) unmapped (remap or omit)`
    : "";
  const truncated = report.truncated_rows
    ? ` · ${report.truncated_rows} row(s) past ingest cap`
    : "";
  const inferred = (report.header_roles || []).some((item) => item.method !== "alias")
    ? " · headers inferred from file + schema"
    : "";
  const named = report.named_rules?.length
    ? ` · ${report.named_rules.length} named rule(s)`
    : "";
  const projection = report.projection?.length
    ? ` · ${report.projection.length} source table(s) projected`
    : "";
  const coverage = report.coverage
    ? ` · rule coverage ${report.coverage.percent}%`
    : "";
  return (
    `${report.rule_count} rule(s) · ${executable} executable · `
    + `${needs_confirmation} need review · ${conflict} conflict${coverage}${unused}${unmapped}${truncated}${inferred}${named}${projection}`
  );
}

const MAX_COMPILED_SHAPE_STEPS = 100;

function shapeStepKey(step: ShapeStepWire): string {
  const to = step.options && typeof step.options === "object"
    ? String((step.options as { to?: unknown }).to ?? "")
    : "";
  return `${step.source_table || ""}|${step.op}|${step.column || ""}|${to}`;
}

/** Append compiled shape steps without wiping operator-authored ones.

A named source table keeps the other selected tables' steps out of this
recipe. Untagged (operator) steps always merge.
*/
export function mergeCompiledShapeSteps(
  existing: ShapeStepWire[],
  compiled: ShapeStepWire[],
  options?: { sourceTable?: string },
): ShapeStepWire[] {
  const tableFold = (options?.sourceTable || "").trim().toLowerCase();
  const seen = new Set(existing.map(shapeStepKey));
  const extra: ShapeStepWire[] = [];
  for (const step of compiled) {
    if (tableFold) {
      const stepTable = (step.source_table || "").trim().toLowerCase();
      if (stepTable && stepTable !== tableFold) continue;
    }
    const key = shapeStepKey(step);
    if (seen.has(key)) continue;
    seen.add(key);
    extra.push(step);
  }
  return [...existing, ...extra].slice(0, MAX_COMPILED_SHAPE_STEPS);
}

export function ruleStatusLabel(status: string): string {
  if (status === "executable") return "Applied";
  if (status === "conflict") return "Conflict";
  return "Needs review";
}

export function ruleActionLabel(action?: string, status?: string): string {
  const token = (action || "").trim() || (
    status === "executable" ? "auto" : status === "conflict" ? "conflict" : "review"
  );
  if (token === "auto") return "Auto";
  if (token === "conflict") return "Conflict";
  return "Review";
}

export function ruleConfidenceLabel(confidence: number): string {
  if (!Number.isFinite(confidence) || confidence <= 0) return "—";
  return `${Math.round(confidence * 100)}%`;
}

const VALIDATION_DEST = /^v\d+$/i;
const SOURCE_MISSING = /source column .+ is not on the selected source/i;

/**
 * A review row the operator may accept as Direct: both columns bound, the
 * dest is not a validation id, and the compiler did not refuse the source.
 * Contracts and joins stay review — accepting them would invent a write.
 */
export function canAcceptAsDirect(rule: CompiledRule): boolean {
  if (rule.status !== "needs_confirmation") return false;
  if (rule.kind === "contract" || rule.kind === "join") return false;
  const source = (rule.source_column || "").trim();
  const dest = (rule.dest_column || "").trim();
  if (!source || !dest) return false;
  if (VALIDATION_DEST.test(dest)) return false;
  if ((rule.issues || []).some((issue) => SOURCE_MISSING.test(issue))) return false;
  return true;
}

/** Operator accept: the pair is a rename / passthrough. No invented transform. */
export function acceptRuleAsDirect(
  report: RuleCompileReport,
  index: number,
): RuleCompileReport {
  const current = report.rules[index];
  if (!current || !canAcceptAsDirect(current)) return report;
  const rules = report.rules.map((rule, i) => (
    i === index
      ? {
          ...rule,
          kind: "direct",
          kind_label: "Direct map",
          interpretation: "Direct copy",
          action: "auto",
          plane: "map",
          transform: "none",
          status: "executable",
          confidence: Math.max(rule.confidence, 0.9),
          issues: [],
        }
      : rule
  ));
  const buckets = {
    executable: rules.filter((rule) => rule.status === "executable").length,
    needs_confirmation: rules.filter((rule) => rule.status === "needs_confirmation").length,
    conflict: rules.filter((rule) => rule.status === "conflict").length,
  };
  return {
    ...report,
    rules,
    buckets,
    coverage: coverageFromRules(rules),
    rule_count: rules.length,
  };
}
