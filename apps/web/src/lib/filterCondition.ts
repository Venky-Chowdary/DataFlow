/**
 * A filter condition the inspector can build without asking the operator
 * to write the expression language. Advanced mode keeps the raw text.
 * The composed string is what the row engine already compiles.
 */

export interface FilterConditionParts {
  field: string;
  operator: string;
  value: string;
}

export const FILTER_OPERATORS: ReadonlyArray<{ id: string; label: string }> = [
  { id: "=", label: "equals" },
  { id: "<>", label: "does not equal" },
  { id: ">", label: "greater than" },
  { id: "<", label: "less than" },
  { id: ">=", label: "at least" },
  { id: "<=", label: "at most" },
  { id: "contains", label: "contains" },
  { id: "is_null", label: "is blank" },
  { id: "not_null", label: "is not blank" },
];

const COMPARISON = new Set(["=", "==", "<>", "!=", "<", "<=", ">", ">="]);

export function filterOperatorNeedsValue(operator: string): boolean {
  return operator !== "is_null" && operator !== "not_null";
}

function quote(value: string): string {
  return `'${value.replace(/'/g, "''")}'`;
}

/** Empty text opens the builder. A comparison round-trips. Anything else stays advanced. */
export function parseFilterCondition(expression: string): FilterConditionParts | null {
  const text = expression.trim();
  if (!text) return { field: "", operator: "<>", value: "" };
  let match = text.match(/^is_null\(\[([^\]\n]+)\]\)$/);
  if (match) return { field: match[1].trim(), operator: "is_null", value: "" };
  match = text.match(/^not\s+is_null\(\[([^\]\n]+)\]\)$/);
  if (match) return { field: match[1].trim(), operator: "not_null", value: "" };
  match = text.match(/^contains\(\[([^\]\n]+)\],\s*'((?:[^']|'')*)'\)$/);
  if (match) {
    return { field: match[1].trim(), operator: "contains", value: match[2].replace(/''/g, "'") };
  }
  match = text.match(/^\[([^\]\n]+)\]\s*(<>|<=|>=|!=|==|=|<|>)\s*'((?:[^']|'')*)'$/);
  if (!match || !COMPARISON.has(match[2])) return null;
  const operator = match[2] === "!=" ? "<>" : match[2] === "==" ? "=" : match[2];
  return { field: match[1].trim(), operator, value: match[3].replace(/''/g, "'") };
}

export function composeFilterCondition(parts: FilterConditionParts): string {
  const field = parts.field.trim();
  if (!field || field.includes("[") || field.includes("]")) return "";
  if (parts.operator === "is_null") return `is_null([${field}])`;
  if (parts.operator === "not_null") return `not is_null([${field}])`;
  if (parts.operator === "contains") return `contains([${field}], ${quote(parts.value)})`;
  if (!COMPARISON.has(parts.operator)) return "";
  return `[${field}] ${parts.operator} ${quote(parts.value)}`;
}

// --- AND/OR groups ---------------------------------------------------------
//
// The guided builder speaks flat groups: every condition joined by one
// combinator. Beats the one-comparison ceiling of ELT rivals while staying
// honest — a mixed `a and b or c` or a nested `(a or b) and c` is genuinely
// ambiguous without parens, so it stays in advanced mode.

export type FilterCombinator = "and" | "or";

export interface FilterGroup {
  combinator: FilterCombinator;
  conditions: FilterConditionParts[];
}

/** Empty text opens an empty group. A flat same-combinator chain round-trips.
 *  Mixed/nested expressions stay advanced (return null). */
export function parseFilterGroup(expression: string): FilterGroup | null {
  const text = expression.trim();
  if (!text) return { combinator: "and", conditions: [{ field: "", operator: "<>", value: "" }] };
  // Split on top-level `and`/`or`. There are no parens or quotes containing
  // the keywords to guard against deeper than "not inside '…'": scan and
  // split only on keyword boundaries outside string literals.
  const parts: string[] = [];
  const combinators: FilterCombinator[] = [];
  let depth = 0;
  let inStr = false;
  let start = 0;
  for (let i = 0; i < text.length; i++) {
    const ch = text[i];
    if (inStr) {
      if (ch === "'") {
        if (text[i + 1] === "'") i++;
        else inStr = false;
      }
      continue;
    }
    if (ch === "'") inStr = true;
    else if (ch === "(" || ch === "[") depth++;
    else if (ch === ")" || ch === "]") depth--;
    else if (depth === 0) {
      const rest = text.slice(i);
      const m = rest.match(/^\s+(and|or)\s+/i);
      if (m) {
        parts.push(text.slice(start, i));
        combinators.push(m[1].toLowerCase() as FilterCombinator);
        i += m[0].length - 1;
        start = i + 1;
      }
    }
  }
  parts.push(text.slice(start));
  if (parts.some((p) => !p.trim())) return null;
  const combinator = combinators[0] ?? "and";
  if (combinators.some((c) => c !== combinator)) return null; // mixed — ambiguous
  const conditions = parts.map((p) => parseFilterCondition(p));
  if (conditions.some((c) => c == null)) return null;
  return { combinator, conditions: conditions as FilterConditionParts[] };
}

export function composeFilterGroup(group: FilterGroup): string {
  const clauses = group.conditions
    .map(composeFilterCondition)
    .filter((c) => c.trim().length > 0);
  if (clauses.length === 0) return "";
  if (clauses.length === 1) return clauses[0];
  return clauses.join(` ${group.combinator} `);
}
