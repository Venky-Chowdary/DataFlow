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
  plane: string;
  confidence: number;
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
  unknown_code_policy?: { action: string; value?: string };
  provenance?: { sheet?: string; row?: number };
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

export function ruleToEvidence(rule: CompiledRule): MappingBusinessRule {
  const expanded = rule.named_rule && rule.resolved_rule
    ? `${rule.rule_text} → ${rule.resolved_rule}`
    : rule.rule_text;
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
): EditableMapping[] {
  if (!report?.rules?.length) return mappings;
  const next = mappings.map((m) => ({ ...m }));
  const indexBySource = new Map<string, number>();
  next.forEach((m, i) => {
    if (m.source) indexBySource.set(m.source.toLowerCase(), i);
  });

  for (const rule of report.rules) {
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
  return (
    `${report.rule_count} rule(s) · ${executable} executable · `
    + `${needs_confirmation} need review · ${conflict} conflict${unused}${unmapped}${truncated}${inferred}${named}`
  );
}

const MAX_COMPILED_SHAPE_STEPS = 100;

function shapeStepKey(step: ShapeStepWire): string {
  const to = step.options && typeof step.options === "object"
    ? String((step.options as { to?: unknown }).to ?? "")
    : "";
  return `${step.op}|${step.column || ""}|${to}`;
}

/** Append compiled shape steps without wiping operator-authored ones. */
export function mergeCompiledShapeSteps(
  existing: ShapeStepWire[],
  compiled: ShapeStepWire[],
): ShapeStepWire[] {
  const seen = new Set(existing.map(shapeStepKey));
  const extra: ShapeStepWire[] = [];
  for (const step of compiled) {
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
