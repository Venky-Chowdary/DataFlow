/**
 * Value-equality for the destination schema Studio holds.
 *
 * Map re-runs whenever `destColumns` / `destSchemaMap` change, and React compares
 * them by identity. A destination that keeps answering the same thing — an
 * unreachable host answering "not connected", or a healthy table answering the
 * same DDL — therefore re-ran Map on every probe, and Map probes again: one
 * probe every ~3s for as long as the operator stayed on the step, with the
 * reload control pinned on "Reading destination…". Only commit a probe result
 * that actually differs from the one already held.
 */
export function sameColumnList(a: string[], b: string[]): boolean {
  if (a === b) return true;
  if (a.length !== b.length) return false;
  return a.every((v, i) => v === b[i]);
}

export function sameSchemaMap(
  a: Record<string, string>,
  b: Record<string, string>,
): boolean {
  if (a === b) return true;
  const ak = Object.keys(a);
  const bk = Object.keys(b);
  if (ak.length !== bk.length) return false;
  return ak.every((k) => a[k] === b[k]);
}

/**
 * Did the probe settle what Studio asked it? Anything else — the connector
 * refusing, or an answer that cannot decide whether the table is there — leaves
 * the destination unknown and must not re-arm the automatic probe immediately.
 */
export function destProbeSettled(
  connected: boolean,
  tableExists: boolean | null,
): boolean {
  return connected && tableExists !== null;
}

/**
 * Destination-object existence Map/Validate may act on.
 *
 * A file export has no catalog and no table to reload. Leaving that as
 * ``null`` printed "Destination schema not loaded" on a route the write
 * already treats as create-new. A database dest keeps the probe's tri-state.
 */
export function destCatalogExists(
  destKindMode: string,
  destTableExists: boolean | null | undefined,
): boolean | null {
  if (destKindMode === "file_export") return false;
  return destTableExists ?? null;
}
