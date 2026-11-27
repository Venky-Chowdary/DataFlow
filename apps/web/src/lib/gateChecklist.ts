/**
 * Validate as a ranked checklist (UI pass 2, item 2).
 *
 * The engine's gate statuses are pass / block / warn / skip / running. A skip
 * means two different things, and they must not look alike:
 *
 * - the check could not run (a probe was unavailable, timed out, was refused):
 *   nothing was proven, so it is never counted with the passed checks, and
 *   the person is offered a re-run;
 * - the check does not apply to this route (no foreign keys declared, a
 *   schemaless destination): nothing to do.
 *
 * Groups, in the order a person works them: Blocking, Could not run, Needs
 * review, Passed (collapsed, with not-applicable checks counted beside it).
 * Inside a check, findings that share a cause are one line with a count
 * ("Value too long for VARCHAR(120) · 3 columns"). Nothing here changes a
 * gate's outcome; it only orders and words what the engine returned.
 */
import { gateDisplayCode, gateLabel, GATE_CATALOG } from "./preflightGates";
import { plural } from "./plural";

export type CheckOutcome = "blocking" | "could_not_run" | "review" | "passed" | "after_run" | "not_applicable" | "pending";

export interface GateLike {
  id: string;
  status?: string | null;
  message?: string | null;
  details?: Record<string, unknown> | null;
}

/**
 * The outcome of one gate, from the engine's structured fields only (never
 * its message text, review UI2 #4):
 *
 * - `rule_id` ending ".post_write": a proof that runs after the load (control
 *   totals, destination foreign keys): "Checked after the run";
 * - `unmeasured`, `remediation_kind: retry_validate` or an error: the check
 *   was stopped, nothing was proven: "Could not run";
 * - `declared: false`, `applicable: false`, a schemaless destination, or a
 *   run that recreates no table (G19): "Not applicable";
 * - any other skip, and any status the UI does not know: "Could not run"
 *   (never counted as passed).
 */
export function gateOutcome(gate: GateLike): CheckOutcome {
  const status = String(gate.status || "").toLowerCase();
  if (status === "pass") return "passed";
  if (status === "block") return "blocking";
  if (status === "warn" || status === "review") return "review";
  if (status === "running" || status === "pending") return "pending";
  if (status !== "skip") return "could_not_run";
  const d = gate.details || {};
  if (String(d.rule_id || "").endsWith(".post_write")) return "after_run";
  if (d.unmeasured === true || d.remediation_kind === "retry_validate" || d.error) return "could_not_run";
  if (d.declared === false || d.applicable === false || d.schemaless === true) return "not_applicable";
  if ("dest_recreated" in d && (d.dest_recreated === false || d.destination_table_exists !== true)) return "not_applicable";
  return "could_not_run";
}

export interface FindingGroup {
  /** The shared cause, sentence case. */
  cause: string;
  /** Objects (columns, tables, keys) the cause was found on, in order, unique. */
  objects: string[];
  /** Findings in this group (an object can carry the cause more than once). */
  count: number;
}

/** One finding: what is wrong, and the object it is on when the engine named one in a field. */
export interface Finding {
  cause: string;
  object?: string;
}

function text(v: unknown): string {
  return typeof v === "string" ? v.trim() : typeof v === "number" ? String(v) : "";
}

/** A finding from one structured entry ({column, message} and the engine's aliases). */
function fromEntry(entry: unknown): Finding | null {
  if (typeof entry === "string") return entry.trim() ? { cause: entry.trim() } : null;
  if (!entry || typeof entry !== "object") return null;
  const e = entry as Record<string, unknown>;
  const object = text(e.column) || text(e.source) || text(e.field) || text(e.table) || undefined;
  const cause = text(e.message) || text(e.error) || text(e.reason) || text(e.issue) || text(e.detail);
  if (!cause) return object ? { cause: "Needs a look", object } : null;
  return object ? { cause, object } : { cause };
}

/**
 * A gate's findings from its structured fields only (re-review G): objects
 * come from fields such as `column` / `source`, never parsed out of message
 * text; a plain string finding is shown exactly as the engine wrote it.
 */
export function findingsFromDetails(details: Record<string, unknown> | null | undefined): Finding[] {
  if (!details) return [];
  const out: Finding[] = [];
  const take = (list: unknown) => {
    if (!Array.isArray(list)) return;
    for (const item of list.slice(0, 200)) {
      const f = fromEntry(item);
      if (f) out.push(f);
    }
  };
  for (const key of ["issues_detail", "errors", "issues", "issue_texts", "low_confidence", "ambiguous_mappings", "unmapped",
    "sample_duplicates", "encoding_issues"]) {
    take(details[key]);
  }
  if (Array.isArray(details.unmeasured_columns)) {
    for (const col of details.unmeasured_columns) {
      const c = text(col);
      if (c) out.push({ cause: "Required-column coverage not measured", object: c });
    }
  }
  return out;
}

/** Group findings that share the same cause text; order follows first occurrence. */
export function groupFindingsByCause(findings: (Finding | string)[]): FindingGroup[] {
  const groups = new Map<string, FindingGroup>();
  for (const raw of findings) {
    const f = typeof raw === "string" ? { cause: raw.trim() } : raw;
    if (!f.cause) continue;
    const key = f.cause;
    const group = groups.get(key) ?? { cause: f.cause, objects: [], count: 0 };
    group.count += 1;
    if (f.object && !group.objects.includes(f.object)) group.objects.push(f.object);
    groups.set(key, group);
  }
  return [...groups.values()];
}

/** Objects shown before "and N more" (the rest behind "Show all"). */
export const OBJECTS_SHOWN = 4;

/**
 * "Value too long for VARCHAR(120): email, full_name, city" — a blocker
 * always names its objects (review UI2 #8); a long list is cut after
 * OBJECTS_SHOWN with "and N more", the full list one click away.
 */
export function findingGroupLine(group: FindingGroup, all = false): string {
  if (group.objects.length) {
    const shown = all ? group.objects : group.objects.slice(0, OBJECTS_SHOWN);
    const more = group.objects.length - shown.length;
    return `${group.cause}: ${shown.join(", ")}${more > 0 ? ` and ${more} more` : ""}`;
  }
  return group.count > 1 ? `${group.cause} (${plural(group.count, "time")})` : group.cause;
}

export interface ChecklistRow {
  id: string;
  code: string;
  label: string;
  rule: string;
  outcome: CheckOutcome;
  message: string;
  findings: FindingGroup[];
  /** How much the check looked at ("Checked on a sample of 5 rows"); empty when the engine did not say. */
  scope: string;
  durationMs?: number;
}

/** The engine's evidence scope (details.evidence_scope) in words. */
export function scopeLine(details: Record<string, unknown> | null | undefined): string {
  const scope = details && typeof details.evidence_scope === "object" ? details.evidence_scope as Record<string, unknown> : null;
  if (!scope) return "";
  const coverage = String(scope.coverage || "");
  const rows = typeof scope.sample_rows === "number" ? scope.sample_rows : null;
  const columns = typeof scope.columns === "number" ? scope.columns : null;
  if (coverage === "sample") return rows != null ? `Checked on a sample of ${plural(rows, "row")}` : "Checked on a sample";
  if (coverage === "full_selected") return "Checked on every selected row";
  if (coverage === "full_schema") return columns != null ? `Checked against the full schema (${plural(columns, "column")})` : "Checked against the full schema";
  return "";
}

export interface ChecklistGroup {
  id: Exclude<CheckOutcome, "not_applicable">;
  title: string;
  rows: ChecklistRow[];
  /** Passed starts collapsed: it is evidence, not work. */
  collapsedByDefault: boolean;
}

export interface Checklist {
  groups: ChecklistGroup[];
  notApplicable: ChecklistRow[];
  pending: ChecklistRow[];
  counts: Record<CheckOutcome, number>;
}

const GROUPS: { id: ChecklistGroup["id"]; title: string }[] = [
  { id: "pending", title: "Running" },
  { id: "blocking", title: "Blocking" },
  { id: "could_not_run", title: "Could not run" },
  { id: "review", title: "Needs review" },
  { id: "after_run", title: "Checked after the run" },
  { id: "passed", title: "Passed" },
];

function ruleFor(id: string): string {
  const entry = GATE_CATALOG.find((g) => g.id === id || g.aliases?.includes(id));
  return entry?.rule ?? "";
}

/**
 * Rows ranked into groups. `findingsOf` returns the gate's finding lines (the
 * dashboard's existing issue extraction), so grouping never re-reads details
 * differently from the panel it replaces.
 */
export function buildChecklist(
  gates: GateLike[] | null | undefined,
  findingsOf: (gate: GateLike) => (Finding | string)[] = (gate) => findingsFromDetails(gate.details),
  order: (a: string, b: string) => number = () => 0,
): Checklist {
  const counts: Record<CheckOutcome, number> = {
    blocking: 0, could_not_run: 0, review: 0, passed: 0, after_run: 0, not_applicable: 0, pending: 0,
  };
  const rows: ChecklistRow[] = [...(gates || [])]
    .sort((a, b) => order(a.id, b.id))
    .map((gate) => {
      const outcome = gateOutcome(gate);
      counts[outcome] += 1;
      return {
        id: gate.id,
        code: gateDisplayCode(gate.id),
        label: gateLabel(gate.id),
        rule: ruleFor(gate.id),
        outcome,
        message: String(gate.message || "").trim(),
        findings: outcome === "passed" || outcome === "not_applicable" || outcome === "after_run" ? [] : groupFindingsByCause(findingsOf(gate)),
        scope: outcome === "not_applicable" ? "" : scopeLine(gate.details),
        durationMs: typeof (gate as { duration_ms?: number }).duration_ms === "number"
          ? (gate as { duration_ms?: number }).duration_ms
          : undefined,
      };
    });
  return {
    groups: GROUPS.map((g) => ({
      ...g,
      rows: rows.filter((r) => r.outcome === g.id),
      collapsedByDefault: g.id === "passed" || g.id === "after_run",
    })).filter((g) => g.rows.length > 0),
    notApplicable: rows.filter((r) => r.outcome === "not_applicable"),
    pending: rows.filter((r) => r.outcome === "pending"),
    counts,
  };
}

/** "2 blocking · 1 could not run · 3 to review · 16 passed · 4 not applicable" (zeros left out). */
export function checklistSummary(counts: Record<CheckOutcome, number>): string {
  const parts: string[] = [];
  if (counts.blocking) parts.push(`${counts.blocking.toLocaleString()} blocking`);
  if (counts.could_not_run) parts.push(`${counts.could_not_run.toLocaleString()} could not run`);
  if (counts.review) parts.push(`${counts.review.toLocaleString()} to review`);
  if (counts.pending) parts.push(`${counts.pending.toLocaleString()} running`);
  if (counts.after_run) parts.push(`${counts.after_run.toLocaleString()} checked after the run`);
  if (counts.passed) parts.push(`${counts.passed.toLocaleString()} passed`);
  if (counts.not_applicable) parts.push(`${counts.not_applicable.toLocaleString()} not applicable`);
  return parts.join(" · ");
}

/** The word on each row's status, never colour alone. */
export const OUTCOME_WORD: Record<CheckOutcome, string> = {
  blocking: "Failed",
  could_not_run: "Could not run",
  review: "Review",
  after_run: "Checked after the run",
  passed: "Passed",
  not_applicable: "Not applicable",
  pending: "Running",
};
