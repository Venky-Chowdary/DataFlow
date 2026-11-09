/**
 * Duplicate rows on an append route (Story 2: mapping and schema decisions).
 *
 * Append means "insert the batch": on a table with no enforced key a rerun of
 * Full refresh · Append stores another copy of every row (Airbyte's
 * full_refresh_append contract). A table that enforces the key refuses that
 * insert, and Validate (G6, `g6_target_ddl.append_key_collision`) names the
 * keyed choices. The route option `duplicate_policy` is the one carrier the
 * engine, Validate and the writer read (apps/api/services/duplicate_policy.py),
 * and reconciliation then closes the population by key census.
 */

export type DuplicateRowsMode = "insert" | "dedupe_skip" | "upsert";

export const DUPLICATE_ROWS_OPTIONS: ReadonlyArray<{
  id: DuplicateRowsMode;
  label: string;
  detail: string;
}> = [
  {
    id: "insert",
    label: "Insert every row (default)",
    detail:
      "Each run inserts its rows again. A table with no enforced key stores another copy on every rerun; a table that enforces the key refuses the run, and Validate says so before anything is written.",
  },
  {
    id: "dedupe_skip",
    label: "Skip keys already stored",
    detail:
      "Rows whose key is already in the destination are left as stored; only new keys insert. Needs a key the destination enforces or one you declare.",
  },
  {
    id: "upsert",
    label: "Update keys already stored",
    detail:
      "Rows whose key is already in the destination replace the stored row; new keys insert. Needs a key the destination enforces or one you declare.",
  },
];

export const APPEND_KEY_COLLISION_RULE_ID = "g6_target_ddl.append_key_collision";
/** Validate could not read the stored keys of a destination that enforces them. */
export const APPEND_COLLISION_UNREADABLE_RULE_ID = "g6_target_ddl.append_collision_unreadable";
const DUPLICATE_ROWS_FIX_RULE_IDS = new Set([APPEND_KEY_COLLISION_RULE_ID, APPEND_COLLISION_UNREADABLE_RULE_ID]);

export function isAppendSyncMode(syncMode: string | undefined): boolean {
  return /append/.test(String(syncMode ?? ""));
}

export function normalizeDuplicateRowsMode(value: unknown): DuplicateRowsMode {
  const m = String(value ?? "").trim().toLowerCase().replace(/-/g, "_");
  if (m === "dedupe_skip" || m === "skip_duplicates") return "dedupe_skip";
  if (m === "upsert" || m === "merge") return "upsert";
  return "insert";
}

/** Destination option keys for the request: only a keyed choice on an append route is sent. */
export function duplicateRowsDestExtra(
  mode: DuplicateRowsMode | string | undefined,
  syncMode: string | undefined,
): Record<string, string> {
  const m = normalizeDuplicateRowsMode(mode);
  return m !== "insert" && isAppendSyncMode(syncMode) ? { duplicate_policy: m } : {};
}

type Gate = { id?: string; status?: string; details?: Record<string, unknown> | null };

/** A stored-key collision (or an unreadable one) owned by Duplicate rows, not a source duplicate. */
export function isDuplicateRowsFixRule(details: Record<string, unknown> | null | undefined): boolean {
  return DUPLICATE_ROWS_FIX_RULE_IDS.has(String(details?.rule_id ?? ""));
}

/** Validate refused an append whose stored keys collide or could not be read; Duplicate rows owns the fix. */
export function appendKeyCollisionBlocked(preflight: unknown): boolean {
  const gates = (preflight as { gates?: Gate[] } | null | undefined)?.gates;
  if (!Array.isArray(gates)) return false;
  return gates.some(
    (g) =>
      String(g?.id ?? "") === "g6_target_ddl"
      && String(g?.status ?? "") === "block"
      && DUPLICATE_ROWS_FIX_RULE_IDS.has(
        String((g?.details as { rule_id?: unknown } | null | undefined)?.rule_id ?? ""),
      ),
  );
}
