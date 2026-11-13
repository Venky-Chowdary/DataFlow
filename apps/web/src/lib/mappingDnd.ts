/**
 * Drag a source column onto a destination column — by pointer or keyboard.
 *
 * The keyboard path is the WAI-ARIA "grab and move" pattern: Space/Enter picks
 * the source up, arrow keys move over destination columns (Home/End jump),
 * Enter/Space drops, Escape cancels. Every transition returns the sentence a
 * screen reader announces through the polite live region, so the keyboard user
 * hears exactly what a pointer user sees.
 */
export interface DndTarget {
  name: string;
  /** Spoken after the name: "BIGINT, required, unmapped". */
  describe: string;
}

export type DndState =
  | { mode: "idle" }
  | { mode: "dragging"; source: string; over: number; via: "keyboard" | "pointer" };

export type DndAction =
  | { type: "pickUp"; source: string; via: "keyboard" | "pointer"; startAt?: number }
  | { type: "move"; delta: number }
  | { type: "moveTo"; index: number }
  | { type: "drop" }
  | { type: "cancel" };

export interface DndResult {
  state: DndState;
  announce?: string;
  drop?: { source: string; target: string };
}

export const IDLE: DndState = { mode: "idle" };

function clamp(i: number, n: number): number {
  return Math.max(0, Math.min(n - 1, i));
}

function overSentence(t: DndTarget | undefined, i: number, n: number): string {
  if (!t) return "No destination columns to drop on.";
  return `Over ${t.name}${t.describe ? `, ${t.describe}` : ""}. ${i + 1} of ${n}.`;
}

export function dndReduce(state: DndState, action: DndAction, targets: DndTarget[]): DndResult {
  const n = targets.length;
  if (action.type === "pickUp") {
    if (n === 0) {
      return { state: IDLE, announce: `No destination columns to map ${action.source} onto.` };
    }
    const over = clamp(action.startAt ?? 0, n);
    return {
      state: { mode: "dragging", source: action.source, over, via: action.via },
      announce: action.via === "keyboard"
        ? `Picked up ${action.source}. Arrow keys choose a destination column, Enter drops, Escape cancels. ${overSentence(targets[over], over, n)}`
        : `Dragging ${action.source}.`,
    };
  }
  if (state.mode !== "dragging") return { state };
  if (action.type === "move" || action.type === "moveTo") {
    const over = clamp(action.type === "move" ? state.over + action.delta : action.index, n);
    if (over === state.over && action.type === "move") return { state };
    return { state: { ...state, over }, announce: overSentence(targets[over], over, n) };
  }
  if (action.type === "drop") {
    const t = targets[state.over];
    if (!t) return { state: IDLE, announce: `Cancelled. ${state.source} is unchanged.` };
    return {
      state: IDLE,
      announce: `Dropped ${state.source} on ${t.name}.`,
      drop: { source: state.source, target: t.name },
    };
  }
  return { state: IDLE, announce: `Cancelled. ${state.source} is unchanged.` };
}

/** Keyboard → action while a drag is in progress (null = not ours). */
export function dndKeyAction(key: string, pageSize = 10): DndAction | null {
  switch (key) {
    case "ArrowDown":
    case "ArrowRight":
      return { type: "move", delta: 1 };
    case "ArrowUp":
    case "ArrowLeft":
      return { type: "move", delta: -1 };
    case "PageDown":
      return { type: "move", delta: pageSize };
    case "PageUp":
      return { type: "move", delta: -pageSize };
    case "Home":
      return { type: "moveTo", index: 0 };
    case "End":
      return { type: "moveTo", index: Number.MAX_SAFE_INTEGER };
    case "Enter":
    case " ":
      return { type: "drop" };
    case "Escape":
      return { type: "cancel" };
    default:
      return null;
  }
}
