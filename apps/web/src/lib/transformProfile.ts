/**
 * What a column profile is worth *drawing*, decided in one testable place.
 *
 * The Transform (pre-load) panel charts each column's findings so an operator
 * sees the shape of the problem before picking an operation. The counts are
 * deliberately drawn as separate bars rather than one stacked share bar: a cell
 * can be padded *and* hold inner whitespace, so a stacked bar would claim a
 * partition of the sample that does not exist. Each bar states one count out of
 * the sampled rows and nothing more.
 */

import type { ShapeColumnProfile } from "./shape";

export interface ColumnFinding {
  kind: string;
  /** Operator-facing name of what was found. */
  label: string;
  count: number;
  /** Why it matters before the load. */
  hint: string;
}

/** Every finding in one column, widest first, empty when the column is clean. */
export function columnFindings(profile: ShapeColumnProfile): ColumnFinding[] {
  const findings: ColumnFinding[] = [];
  if (profile.blanks) {
    findings.push({
      kind: "blank",
      label: "Blank",
      count: profile.blanks,
      hint: "Empty or whitespace-only. Lands as NULL unless the destination refuses NULL.",
    });
  }
  if (profile.untrimmed) {
    findings.push({
      kind: "padded",
      label: "Leading/trailing space",
      count: profile.untrimmed,
      hint: "Padding counts toward a VARCHAR width and breaks key matching on upsert.",
    });
  }
  if (profile.inner_whitespace) {
    findings.push({
      kind: "inner-space",
      label: "Repeated inner space",
      count: profile.inner_whitespace,
      hint: "Two values that look identical to a person are distinct to the destination.",
    });
  }
  if (profile.non_printable) {
    findings.push({
      kind: "control",
      label: "Control character",
      count: profile.non_printable,
      hint: "Non-printable bytes survive the load and corrupt later exports.",
    });
  }
  if (profile.unnormalized_unicode) {
    findings.push({
      kind: "unicode",
      label: "Un-normalised Unicode",
      count: profile.unnormalized_unicode,
      hint: "The same accented text in two encodings will not de-duplicate.",
    });
  }
  const sentinels = Object.entries(profile.sentinels);
  for (const [token, count] of sentinels) {
    if (!count) continue;
    findings.push({
      kind: `sentinel:${token}`,
      label: `Placeholder “${token}”`,
      count,
      hint: "A word standing in for absence. It lands as text unless it is made null here.",
    });
  }
  return findings.sort((a, b) => b.count - a.count || a.label.localeCompare(b.label));
}

/** Percent of the sample one finding covers, clamped so a bar is always visible. */
export function findingShare(count: number, rows: number): number {
  if (rows <= 0 || count <= 0) return 0;
  const share = (count / rows) * 100;
  if (share > 100) return 100;
  return share < 2 ? 2 : share;
}

/** Columns holding at least one finding — the panel's "needs a decision" count. */
export function columnsNeedingAttention(profiles: ShapeColumnProfile[]): number {
  return profiles.filter((profile) => columnFindings(profile).length > 0).length;
}

/**
 * How the profile reads a column, in words an operator can act on.
 *
 * `logical_type` is the engine's own word; a column that is numeric in every
 * sampled row but carried as text is the case that later fails at the write, so
 * it is named here rather than left to be inferred from two numbers.
 */
export function readsAsSummary(profile: ShapeColumnProfile): string {
  const parts = [profile.logical_type];
  if (profile.max_scale) parts.push(`up to ${profile.max_scale} decimal place(s)`);
  if (profile.max_length) parts.push(`longest ${profile.max_length} char(s)`);
  if (profile.ambiguous_date_order) parts.push("ambiguous day/month order");
  return parts.join(" · ");
}

/** True when text holds only numbers — a carrier decision Map is about to make. */
export function isNumericText(profile: ShapeColumnProfile): boolean {
  if (profile.logical_type !== "string" && profile.logical_type !== "text") return false;
  return profile.non_blank > 0 && profile.numeric_like === profile.non_blank;
}
