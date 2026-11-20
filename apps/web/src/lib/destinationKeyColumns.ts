/**
 * The destination key columns Map protects (rule 4, D369), tied to the
 * destination table they were read from.
 *
 * Studio used to keep one bare list, set only by a successful probe. Typing
 * another table, a probe that failed for it, or clearing the destination left
 * the previous table's key in place: Map then protected table A's key on
 * table B and left B's own key unprotected. Keys are now stored with the
 * probe key (connector, engine, database, schema, table) and are read only for
 * that same destination; any other destination has none until it is probed.
 * The server also checks them against its own catalog read.
 */
export interface DestKeyColumns {
  probeKey: string;
  columns: string[];
}

export const NO_DEST_KEYS: DestKeyColumns = { probeKey: "", columns: [] };

/** What a probe of ``probeKey`` says the table's key is ([] for a table that does not exist). */
export function destKeyColumnsFromProbe(
  probeKey: string,
  probe: { tableExists: boolean | null; primaryKey?: string[] | null; identity?: string[] | null },
): DestKeyColumns {
  if (!probeKey || probe.tableExists === false) return { probeKey, columns: [] };
  const columns = [...(probe.primaryKey ?? []), ...(probe.identity ?? [])]
    .map((c) => String(c ?? "").trim())
    .filter(Boolean);
  return { probeKey, columns: [...new Set(columns)] };
}

/** The key columns for the destination Studio points at now; [] unless read for it. */
export function destKeyColumnsFor(state: DestKeyColumns, currentProbeKey: string): string[] {
  return state.probeKey && state.probeKey === currentProbeKey ? state.columns : [];
}
