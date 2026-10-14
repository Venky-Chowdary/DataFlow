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
