/** Keyboard movement for the schema tree. Enter activates the focused button. */

export function nextTreeIndex(current: number, key: string, length: number): number | null {
  if (length <= 0) return null;
  const index = current < 0 ? 0 : current;
  if (key === "ArrowDown") return Math.min(length - 1, index + 1);
  if (key === "ArrowUp") return Math.max(0, index - 1);
  if (key === "Home") return 0;
  if (key === "End") return length - 1;
  return null;
}

export const SCHEMA_TRUNCATION_COPY =
  "This list is capped. Objects past the cap are not shown — type a name you already know, or narrow the schema.";
