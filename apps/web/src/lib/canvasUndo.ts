/** Structural undo for a recipe. The stack stores committed snapshots, not keystrokes. */

export interface UndoStack<T> {
  past: T[];
  present: T;
  future: T[];
  cap: number;
}

export function cloneSnapshot<T>(value: T): T {
  return JSON.parse(JSON.stringify(value)) as T;
}

export function sameSnapshot<T>(a: T, b: T): boolean {
  return JSON.stringify(a) === JSON.stringify(b);
}

export function createUndoStack<T>(present: T, cap = 100): UndoStack<T> {
  return { past: [], present: cloneSnapshot(present), future: [], cap: Math.max(1, cap) };
}

export function undoPush<T>(stack: UndoStack<T>, next: T): UndoStack<T> {
  if (sameSnapshot(stack.present, next)) return stack;
  const past = [...stack.past, cloneSnapshot(stack.present)];
  const trimmed = past.length > stack.cap ? past.slice(past.length - stack.cap) : past;
  return { ...stack, past: trimmed, present: cloneSnapshot(next), future: [] };
}

export function undo<T>(stack: UndoStack<T>): UndoStack<T> {
  if (stack.past.length === 0) return stack;
  const previous = stack.past[stack.past.length - 1];
  return {
    ...stack,
    past: stack.past.slice(0, -1),
    present: cloneSnapshot(previous),
    future: [cloneSnapshot(stack.present), ...stack.future],
  };
}

export function redo<T>(stack: UndoStack<T>): UndoStack<T> {
  if (stack.future.length === 0) return stack;
  const [next, ...rest] = stack.future;
  const past = [...stack.past, cloneSnapshot(stack.present)];
  const trimmed = past.length > stack.cap ? past.slice(past.length - stack.cap) : past;
  return { ...stack, past: trimmed, present: cloneSnapshot(next), future: rest };
}
