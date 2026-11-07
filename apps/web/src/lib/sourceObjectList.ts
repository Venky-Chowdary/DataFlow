/**
 * Comma-separated source object names. The schema browser and the combobox
 * share this list so a catalog click cannot invent a second selection model.
 */

/** Append one catalog object. A name already present is left unchanged. */
export function appendSourceObject(current: string, name: string): string {
  const trimmed = name.trim();
  if (!trimmed) return current;
  const parts = current.split(",").map((part) => part.trim()).filter(Boolean);
  if (parts.includes(trimmed)) return current;
  return parts.length ? `${parts.join(", ")}, ${trimmed}` : trimmed;
}
