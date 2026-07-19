/** CSS class for logical / SQL data types — drives badges in the UI. */

export type TypeFamily =
  | "int"
  | "decimal"
  | "bool"
  | "temporal"
  | "json"
  | "uuid"
  | "binary"
  | "string";

/** Canonical destination types offered in Map — covers warehouse + document common cases. */
export const LOGICAL_TYPE_OPTIONS: { value: string; label: string; family: TypeFamily }[] = [
  { value: "VARCHAR", label: "VARCHAR — text", family: "string" },
  { value: "TEXT", label: "TEXT — long text", family: "string" },
  { value: "INTEGER", label: "INTEGER — 32-bit", family: "int" },
  { value: "BIGINT", label: "BIGINT — 64-bit", family: "int" },
  { value: "SMALLINT", label: "SMALLINT", family: "int" },
  { value: "DECIMAL", label: "DECIMAL — precise number", family: "decimal" },
  { value: "NUMERIC", label: "NUMERIC", family: "decimal" },
  { value: "FLOAT", label: "FLOAT", family: "decimal" },
  { value: "DOUBLE", label: "DOUBLE", family: "decimal" },
  { value: "BOOLEAN", label: "BOOLEAN", family: "bool" },
  { value: "DATE", label: "DATE", family: "temporal" },
  { value: "TIME", label: "TIME", family: "temporal" },
  { value: "TIMESTAMP", label: "TIMESTAMP", family: "temporal" },
  { value: "TIMESTAMPTZ", label: "TIMESTAMPTZ", family: "temporal" },
  { value: "JSON", label: "JSON / document", family: "json" },
  { value: "JSONB", label: "JSONB", family: "json" },
  { value: "ARRAY", label: "ARRAY", family: "json" },
  { value: "UUID", label: "UUID", family: "uuid" },
  { value: "BINARY", label: "BINARY / bytes", family: "binary" },
  { value: "BYTEA", label: "BYTEA", family: "binary" },
];

export function typeFamily(rawType: string | undefined): TypeFamily {
  const t = (rawType || "string").toLowerCase();
  if (/int|bigint|smallint|number\(/.test(t)) return "int";
  if (/decimal|numeric|float|double|real|bignumeric|number$/.test(t)) return "decimal";
  if (/bool/.test(t)) return "bool";
  if (/timestamp|datetime|date|time/.test(t)) return "temporal";
  if (/json|variant|object|array|super|map|struct/.test(t)) return "json";
  if (/uuid|guid/.test(t)) return "uuid";
  if (/binary|blob|bytea|bytes|varbinary/.test(t)) return "binary";
  return "string";
}

export function typeBadgeClass(rawType: string | undefined): string {
  return `df2-type-${typeFamily(rawType)}`;
}

/** Options for a select, always including the current value if custom. */
export function destTypeSelectOptions(current?: string): { value: string; label: string }[] {
  const base = LOGICAL_TYPE_OPTIONS.map(({ value, label }) => ({ value, label }));
  const cur = (current || "").trim();
  if (!cur) return base;
  const upper = cur.toUpperCase();
  const matched = base.find((o) => o.value.toUpperCase() === upper);
  if (matched) return base;
  return [{ value: cur, label: `${cur} — current` }, ...base];
}

/** Normalize a type string to a select option value when possible. */
export function normalizeDestTypeValue(current?: string): string {
  const cur = (current || "").trim();
  if (!cur) return "VARCHAR";
  const upper = cur.toUpperCase();
  const matched = LOGICAL_TYPE_OPTIONS.find((o) => o.value.toUpperCase() === upper);
  return matched ? matched.value : cur;
}
