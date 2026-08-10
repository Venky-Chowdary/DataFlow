/**
 * Transfer-wire sentinels for "no value here".
 *
 * The engine carries SQL NULL as an explicit token so an empty string stays
 * distinguishable from NULL. Those tokens are wire spellings, never something
 * an operator should read: a Postgres preview rendered a grid of literal
 * `__DF_SQL_NULL__` cells.
 */
export const SQL_NULL_SENTINEL = "__DF_SQL_NULL__";
export const MISSING_SENTINEL = "__DF_MISSING__";

/** Human label for a wire sentinel, or null when the value is a real value. */
export function nullWireLabel(raw: unknown): string | null {
  if (typeof raw !== "string") return null;
  const trimmed = raw.trim();
  if (trimmed === SQL_NULL_SENTINEL) return "NULL";
  if (trimmed === MISSING_SENTINEL) return "absent";
  return null;
}
