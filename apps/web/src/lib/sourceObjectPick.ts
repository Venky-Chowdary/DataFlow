/** True when the typed source name is absent from the discovered catalog.

A missing name is an honest refuse — Studio will not invent Map columns.
Empty catalog is not proof of absence (probe may have failed or been truncated).
*/
export function sourceTableMissingFromCatalog(
  typed: string,
  discovered: readonly string[],
): boolean {
  const want = typed.trim().toLowerCase();
  if (!want || discovered.length === 0) return false;
  return !discovered.some((raw) => {
    const name = raw.trim().toLowerCase();
    if (!name) return false;
    const leaf = name.split(".").pop() || name;
    const wantLeaf = want.split(".").pop() || want;
    return (
      name === want
      || leaf === want
      || wantLeaf === name
      || name.endsWith(`.${want}`)
      || want.endsWith(`.${name}`)
    );
  });
}

/** New schedule without mappings opens Studio — the beat will not invent Map. */
export function scheduleCreateOpensStudio(sched: {
  mapping_count?: number;
  mappings?: unknown[];
}): boolean {
  const count = sched.mapping_count
    ?? (Array.isArray(sched.mappings) ? sched.mappings.length : 0);
  return count === 0;
}
