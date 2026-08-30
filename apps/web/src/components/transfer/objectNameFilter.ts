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
