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

export type CheckOutcome = "blocking" | "could_not_run" | "review" | "passed" | "not_applicable" | "pending";

export interface GateLike {
  id: string;
  status?: string | null;
  message?: string | null;
  details?: Record<string, unknown> | null;
}

/**
 * Words the engine uses when a check was stopped rather than answered
 * (packages/preflight gates.py "hosted G13 unavailable", probe timeouts,
 * refused catalog reads). A skip without them is "does not apply".
 */
const COULD_NOT_RUN = /\b(unavailable|could not|couldn't|cannot|can't|failed to|timed? ?out|time-?out|unreachable|not reachable|refused|denied|error|exception|not available)\b/i;

export function gateOutcome(gate: GateLike): CheckOutcome {
  const status = String(gate.status || "").toLowerCase();
  if (status === "pass") return "passed";
  if (status === "block") return "blocking";
  if (status === "warn" || status === "review") return "review";
  if (status === "skip") {
    const details = gate.details || {};
    const errored = Boolean(details.error) || String(details.status || "").toLowerCase() === "unavailable";
    return errored || COULD_NOT_RUN.test(String(gate.message || "")) ? "could_not_run" : "not_applicable";
  }
  return "pending";
}

export interface FindingGroup {
  /** The shared cause, sentence case. */
  cause: string;
  /** Objects (columns, tables, keys) the cause was found on, in order, unique. */
  objects: string[];
  /** Findings in this group (an object can carry the cause more than once). */
  count: number;
}

function sentence(text: string): string {
  const t = text.trim().replace(/\s+/g, " ");
  return t ? t.charAt(0).toUpperCase() + t.slice(1) : t;
}

/**
 * Group "object: cause" lines by cause. A line with no object prefix is its
 * own cause. Order follows the first occurrence of each cause.
 */
export function groupFindingsByCause(lines: string[]): FindingGroup[] {
  const groups = new Map<string, FindingGroup>();
  for (const raw of lines) {
    const line = String(raw || "").trim();
    if (!line) continue;
    // "email: value too long" / "column email: ..." / "orders.email — ..."
    const m = /^(?:column\s+)?([A-Za-z_][\w.$"`[\]]{0,80})\s*(?::|—|–)\s+(.+)$/.exec(line);
    const object = m ? m[1].replace(/["`[\]]/g, "") : "";
    const cause = sentence(m ? m[2] : line).replace(/\.$/, "");
    const key = cause.toLowerCase();
    const group = groups.get(key) ?? { cause, objects: [], count: 0 };
    group.count += 1;
    if (object && !group.objects.includes(object)) group.objects.push(object);
    groups.set(key, group);
  }
  return [...groups.values()];
}

/** "Value too long for VARCHAR(120) · 3 columns" (objects counted when more than one). */
export function findingGroupLine(group: FindingGroup, noun = "column"): string {
  if (group.objects.length > 1) return `${group.cause} · ${plural(group.objects.length, noun)}`;
  if (group.objects.length === 1) return `${group.cause} · ${group.objects[0]}`;
  return group.count > 1 ? `${group.cause} · ${plural(group.count, "time")}` : group.cause;
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
  id: Exclude<CheckOutcome, "not_applicable" | "pending">;
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
  { id: "blocking", title: "Blocking" },
  { id: "could_not_run", title: "Could not run" },
  { id: "review", title: "Needs review" },
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
  findingsOf: (gate: GateLike) => string[] = () => [],
  order: (a: string, b: string) => number = () => 0,
): Checklist {
  const counts: Record<CheckOutcome, number> = {
    blocking: 0, could_not_run: 0, review: 0, passed: 0, not_applicable: 0, pending: 0,
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
        findings: outcome === "passed" || outcome === "not_applicable" ? [] : groupFindingsByCause(findingsOf(gate)),
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
      collapsedByDefault: g.id === "passed",
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
  if (counts.passed) parts.push(`${counts.passed.toLocaleString()} passed`);
  if (counts.not_applicable) parts.push(`${counts.not_applicable.toLocaleString()} not applicable`);
  return parts.join(" · ");
}

/** The word on each row's status, never colour alone. */
export const OUTCOME_WORD: Record<CheckOutcome, string> = {
  blocking: "Failed",
  could_not_run: "Could not run",
  review: "Review",
  passed: "Passed",
  not_applicable: "Not applicable",
  pending: "Running",
};
