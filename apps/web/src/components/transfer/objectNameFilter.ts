/** Split a comma-separated box into names already chosen and the token still being typed. */
export function objectNameDraft(value: string): { query: string; chosen: string[] } {
  const parts = String(value || "").split(",");
  const query = (parts.pop() ?? "").trim();
  const chosen = parts.map((part) => part.trim()).filter(Boolean);
  return { query, chosen };
}

/** Picking a catalog name replaces only the token in progress, and keeps earlier names. */
export function objectNameAfterPick(value: string, picked: string): string {
  const name = picked.trim();
  const { chosen } = objectNameDraft(value);
  return [...chosen, name].filter(Boolean).join(", ");
}

/** Every comma-separated name is on the catalog, so the menu must not say the list is missing. */
export function multiObjectNamesSettled(value: string, options: string[]): boolean {
  const tokens = String(value || "").split(",").map((part) => part.trim()).filter(Boolean);
  if (tokens.length < 2) return false;
  const known = new Set(options.map((name) => name.toLowerCase()));
  return tokens.every((token) => known.has(token.toLowerCase()));
}

/** Catalog names that match the typed value — exact first, never dropped. */
export function filterObjectNames(options: string[], query: string, limit = 200): string[] {
  const q = query.trim().toLowerCase();
  if (!q) return options.slice(0, limit);
  const exact: string[] = [];
  const starts: string[] = [];
  const contains: string[] = [];
  for (const name of options) {
    const n = name.toLowerCase();
    if (n === q) exact.push(name);
    else if (n.startsWith(q)) starts.push(name);
    else if (n.includes(q)) contains.push(name);
  }
  return [...exact, ...starts, ...contains].slice(0, limit);
}
