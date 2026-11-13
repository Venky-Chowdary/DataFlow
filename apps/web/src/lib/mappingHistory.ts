/**
 * Undo / redo for Map edits. Every change the operator makes to the mapping
 * list (pick, drop, auto-map, clear, approve, transform…) is one step.
 *
 * Snapshots are the mapping arrays themselves: edits are immutable updates, so
 * a snapshot shares every untouched row with its neighbours and costs one
 * array of references, not a deep copy of a 1,000-row table.
 */
export interface History<T> {
  past: T[];
  future: T[];
}

export const HISTORY_LIMIT = 100;

export function emptyHistory<T>(): History<T> {
  return { past: [], future: [] };
}

/** Record ``current`` before an edit replaces it; a new edit drops the redo branch. */
export function recordEdit<T>(h: History<T>, current: T, limit = HISTORY_LIMIT): History<T> {
  const past = [...h.past, current];
  return { past: past.length > limit ? past.slice(past.length - limit) : past, future: [] };
}

export function undoStep<T>(h: History<T>, current: T): { history: History<T>; value: T } | null {
  if (h.past.length === 0) return null;
  const value = h.past[h.past.length - 1];
  return { history: { past: h.past.slice(0, -1), future: [current, ...h.future] }, value };
}

export function redoStep<T>(h: History<T>, current: T): { history: History<T>; value: T } | null {
  if (h.future.length === 0) return null;
  const [value, ...future] = h.future;
  return { history: { past: [...h.past, current], future }, value };
}

/** Ctrl/Cmd+Z → undo, Shift+Ctrl/Cmd+Z or Ctrl+Y → redo; else null. */
export function historyKey(e: {
  key: string;
  ctrlKey: boolean;
  metaKey: boolean;
  shiftKey: boolean;
  altKey: boolean;
}): "undo" | "redo" | null {
  if (e.altKey || !(e.ctrlKey || e.metaKey)) return null;
  const key = e.key.toLowerCase();
  if (key === "z") return e.shiftKey ? "redo" : "undo";
  if (key === "y" && e.ctrlKey && !e.metaKey && !e.shiftKey) return "redo";
  return null;
}
