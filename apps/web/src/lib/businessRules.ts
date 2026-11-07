/**
 * Apply a compiled rule workbook onto studio mappings.
 *
 * The server already classified and bound names. This module only merges
 * those artifacts onto EditableMapping the way an operator would have typed
 * them — never inventing a dest column, never dropping an unused dest.
 *
 * Create-new rematch invents source-named dests (`fname → fname`) and auto-
 * approves them. That is not an operator lock. Compiled dest names from the
 * workbook (`fname → first_name`) replace those identity fallbacks.
 */

import type { EditableMapping, MappingBusinessRule, MappingTransform, RuleContractWire } from "./mapping";
import { applyDeclaredSourceZone, applyTransformChange } from "./mapping";
import type { ShapeStepWire } from "./shape";

export interface CompiledRule {
  rule_id?: string;
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
  enforced_at_execute?: boolean;
  enforced_post_load?: boolean;
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
  contracts_enforced_at_execute?: number;
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
  compiler_version?: string;
  input_sha256?: string;
  report_fingerprint?: string;
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
  const executable = report.coverage?.executable ?? report.buckets?.executable ?? 0;
  const review = report.coverage?.review ?? report.buckets?.needs_confirmation ?? 0;
  const conflict = report.coverage?.conflict ?? report.buckets?.conflict ?? 0;
  const rules = report.rules || [];
  const validateOnly = rules.filter((rule) => rule.status === "executable"
    && rule.kind === "contract"
    && !rule.enforced_at_execute && !rule.enforced_post_load).length;
  const postLoad = rules.filter((rule) => rule.status === "executable"
    && rule.kind === "contract"
    && !rule.enforced_at_execute && rule.enforced_post_load).length;
  const enforced = Math.max(0, executable - validateOnly);
  const parts = [
    validateOnly
      ? `${validateOnly} validation rule(s) are checked at Validate only, not enforced at Run`
      : "",
    postLoad ? `${postLoad} rule(s) are checked after load` : "",
  ].filter(Boolean);
  const suffix = parts.length ? `; ${parts.join("; ")}` : "";
  if (review || conflict) {
    return `Migration compiled ${enforced} executable business rule(s); review remains${suffix}`;
  }
  return `Migration verified against ${enforced} business rules${suffix}`;
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
    ruleId: rule.rule_id || undefined,
  };
}

export function isAutoIdentityDest(mapping: EditableMapping): boolean {
  const target = (mapping.target || "").trim();
  if (!target) return true;
  return target.toLowerCase() === (mapping.source || "").trim().toLowerCase();
}

/** Operator (or dest-exists) chose a dest name. Auto identity is not a choice. */
function destIsOperatorLocked(mapping: EditableMapping, dest: string): boolean {
  if (!mapping.approved || !mapping.target || !dest) return false;
  if (mapping.target.toLowerCase() === dest.toLowerCase()) return false;
  if (isAutoIdentityDest(mapping)) return false;
  return true;
}

function applyOne(mapping: EditableMapping, rule: CompiledRule): EditableMapping {
  const evidence = ruleToEvidence(rule);
  const transform = asTransform(rule.transform);
  // An executable row whose transform this path cannot run must not silently
  // degrade to a direct copy — it stays on the review plane.
  const unknownTransform = Boolean(
    rule.status === "executable"
    && (rule.transform || "").trim()
    && rule.transform !== "none"
    && transform === "none",
  );
  const lockedDest = destIsOperatorLocked(mapping, rule.dest_column);
  if (lockedDest) {
    return {
      ...mapping,
      approved: false,
      businessRule: { ...evidence, status: "conflict" },
      requiresReview: true,
      reason: `Workbook dest “${rule.dest_column}” conflicts with locked “${mapping.target}”.`,
    };
  }
  let next: EditableMapping = {
    ...mapping,
    approved: rule.status === "executable" ? mapping.approved : false,
    target: rule.status === "executable" && rule.dest_column
      ? rule.dest_column
      : mapping.target,
    businessRule: unknownTransform
      ? {
          ...evidence,
          issues: [
            ...(evidence.issues || []),
            `Transform "${rule.transform}" is not executable on this path`,
          ],
        }
      : evidence,
    reason: rule.rule_text
      ? `${rule.kind_label}: ${rule.rule_text}`
      : rule.kind_label,
    requiresReview: rule.status !== "executable" || unknownTransform || mapping.requiresReview,
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
  if (
    rule.status === "executable" && !unknownTransform
    && rule.confidence >= 0.9 && next.target
  ) {
    next = { ...next, approved: true, requiresReview: false };
  }
  return next;
}

/** Rule-contract payload the writer evaluates per cell at Execute. */
function ruleContractWire(rule: CompiledRule): RuleContractWire {
  const contract = rule.contract || { type: "" };
  return {
    rule_id: rule.rule_id || "",
    type: contract.type || "",
    value: contract.value,
    values: contract.values,
    op: contract.op,
    pattern: contract.pattern,
    rule_text: rule.rule_text || "",
  };
}

/**
 * Attach execute-enforced contracts to the mapping that writes their dest.
 * A contract with no dest binds by source column. One that matches nothing
 * stays a Validate-only row — never smuggled onto an unrelated write.
 */
function attachRuleContracts(
  next: EditableMapping[],
  report: RuleCompileReport,
  tableFold: string,
): void {
  for (const rule of report.rules) {
    if (rule.kind !== "contract") continue;
    if (
      rule.status !== "executable"
      || !(rule.enforced_at_execute || rule.enforced_post_load)
    ) continue;
    if (tableFold) {
      const ruleTable = (rule.source_table || "").trim().toLowerCase();
      if (ruleTable && ruleTable !== tableFold) continue;
    }
    const dest = (rule.dest_column || "").trim().toLowerCase();
    const source = (rule.map_source || rule.source_column || "").trim().toLowerCase();
    const idx = next.findIndex((m) => (
      dest
        ? (m.target || "").trim().toLowerCase() === dest
        : source && (m.source || "").trim().toLowerCase() === source
    ));
    if (idx < 0) continue;
    const existing = next[idx].ruleContracts || [];
    if (existing.some((c) => c.rule_id === (rule.rule_id || ""))) continue;
    next[idx] = {
      ...next[idx],
      ruleContracts: [...existing, ruleContractWire(rule)],
    };
  }
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
    const operatorLocked = destIsOperatorLocked(existing, dest) && !existing.businessRule;
    if (operatorLocked) {
      next[idx] = applyOne(existing, rule);
      continue;
    }
    if (
      rule.status === "executable"
      && dest
      && existing.target
      && existing.target.toLowerCase() !== dest.toLowerCase()
      && !isAutoIdentityDest(existing)
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
  attachRuleContracts(next, report, tableFold);
  // Stamp the compile-report fingerprint on every carried rule so the Execute
  // wire links back to the business_rules.compiled audit event.
  const fingerprint = (report.report_fingerprint || "").trim();
  if (fingerprint) {
    for (let i = 0; i < next.length; i += 1) {
      const br = next[i].businessRule;
      if (br && !br.reportFingerprint) {
        next[i] = {
          ...next[i],
          businessRule: { ...br, reportFingerprint: fingerprint },
        };
      }
    }
  }
  return next;
}

export interface RuleCensus {
  total: number;
  mapping: number;
  validation: number;
  namedMapping: number;
  namedValidation: number;
  executable: number;
  review: number;
  conflict: number;
  coveragePercent: number;
}

export function ruleCensus(report: RuleCompileReport): RuleCensus {
  const rules = report.rules || [];
  const mapping = rules.filter((rule) => rule.kind !== "contract").length;
  const validation = rules.filter((rule) => rule.kind === "contract").length;
  const namedMapping = rules.filter((rule) => rule.named_rule && rule.kind !== "contract").length;
  const namedValidation = rules.filter((rule) => rule.named_rule && rule.kind === "contract").length;
  const executable = report.coverage?.executable ?? report.buckets?.executable ?? 0;
  const review = report.coverage?.review ?? report.buckets?.needs_confirmation ?? 0;
  const conflict = report.coverage?.conflict ?? report.buckets?.conflict ?? 0;
  const total = report.coverage?.detected ?? report.rule_count ?? rules.length;
  return {
    total,
    mapping,
    validation,
    namedMapping,
    namedValidation,
    executable,
    review,
    conflict,
    coveragePercent: report.coverage?.percent
      ?? (total ? Math.round((100 * executable) / total) : 0),
  };
}

export function ruleMixSummary(report: RuleCompileReport): string {
  const census = ruleCensus(report);
  return `${census.total} rules: ${census.mapping} mapping, ${census.validation} validation`;
}

export function compiledExpression(rule: CompiledRule): string {
  if (rule.resolved_rule) return rule.resolved_rule;
  const options = rule.shape_step?.options && typeof rule.shape_step.options === "object"
    ? rule.shape_step.options as Record<string, unknown>
    : {};
  if (typeof options.expression === "string" && options.expression.trim()) {
    return options.expression;
  }
  if (rule.code_crosswalk) {
    return Object.entries(rule.code_crosswalk).map(([key, value]) => `${key} → ${value}`).join("; ");
  }
  const contract = rule.contract;
  if (contract?.type === "compare") return `${contract.op} ${contract.value}`;
  if (contract?.type === "contains") return `contains ${contract.value}`;
  if (contract?.type === "in_set") return `in (${(contract.values || []).join(", ")})`;
  if (contract?.type === "pattern") return contract.pattern || "pattern";
  if (contract?.type) return contract.type;
  return rule.rule_text || "";
}

export function ruleReportSummary(report: RuleCompileReport): string {
  const census = ruleCensus(report);
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
  const projection = report.projection?.length
    ? ` · ${report.projection.length} source table(s) projected`
    : "";
  const coverage = ` · rule coverage ${census.coveragePercent}%`;
  return (
    `${ruleMixSummary(report)} · `
    + `${census.executable} executable · ${census.review} review · ${census.conflict} conflict`
    + `${coverage}${unused}${unmapped}${truncated}${inferred}${projection}`
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
  if (status === "executable") return "Ready";
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
const DIRECT_KINDS = new Set(["direct", "rename", "passthrough"]);
const IDENTITY_TRANSFORMS = new Set(["", "none", "identity"]);
const REVIEW_CONFIDENCE = /confidence|low[- ]confidence|fuzzy/i;

/**
 * A review row the operator may accept as Direct — only when the compiled
 * meaning really is a rename/passthrough: a direct-ish kind, an identity
 * transform, both columns bound, and no issue beyond review confidence.
 * Anything the compiler could not parse (unknown, contract, lookup, …)
 * stays review — accepting it would invent a write.
 */
export function canAcceptAsDirect(rule: CompiledRule): boolean {
  if (rule.status !== "needs_confirmation") return false;
  if (!DIRECT_KINDS.has((rule.kind || "").toLowerCase())) return false;
  if (!IDENTITY_TRANSFORMS.has((rule.transform || "").toLowerCase())) return false;
  const source = (rule.source_column || "").trim();
  const dest = (rule.dest_column || "").trim();
  if (!source || !dest) return false;
  if (VALIDATION_DEST.test(dest)) return false;
  if ((rule.issues || []).some((issue) => SOURCE_MISSING.test(issue))) return false;
  if ((rule.issues || []).some((issue) => !REVIEW_CONFIDENCE.test(issue))) return false;
  return true;
}

export function ruleEdgeLabel(rule: CompiledRule): string {
  if (rule.kind === "contract") {
    return rule.dest_column || rule.source_column || "—";
  }
  return `${rule.source_column || "—"} → ${rule.dest_column || "—"}`;
}

export function ruleReviewHint(rule: CompiledRule, canAcceptHere: boolean): string {
  if (rule.kind === "contract" && rule.status === "executable") {
    return "Checked on the destination during the write; not a Transform step.";
  }
  if (rule.kind === "contract") {
    return "Validate contract — confirm the column binding. This is not a Transform write.";
  }
  if (rule.kind === "join") {
    return "Joins need you to choose how rows match; they stay in review.";
  }
  if (canAcceptAsDirect(rule)) {
    if (!canAcceptHere) {
      return "Clear rename / passthrough the compiler left for you. Accept it as Direct on Map, or remap there.";
    }
    return "Clear rename / passthrough the compiler left for you. Accept as Direct to put it on Map, or leave it and remap there.";
  }
  if (
    DIRECT_KINDS.has((rule.kind || "").toLowerCase())
    && (rule.issues || []).some((issue) => /is not on the selected (source|destination)/i.test(issue))
  ) {
    return "A column this rule names is not on the selected source or destination. Remap it on Map, or fix the workbook and re-upload.";
  }
  if (rule.status !== "executable") {
    return "Not compiled — rewrite this rule in a supported form, or add an explicit Transform step, then re-upload.";
  }
  return "The compiler will not invent an algorithm from this sentence. Confirm or remap on Map.";
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
