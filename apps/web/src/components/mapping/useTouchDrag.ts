import { useRef, useState, type PointerEvent } from "react";
import type { DndAction } from "../../lib/mappingDnd";

interface DragStudio {
  dispatchDnd: (action: DndAction) => void;
  pointerOver: (name: string | null) => void;
  pointerDrop: (source: string, target: string) => void;
}

const EDGE_PX = 48;
const STEP_PX = 24;

/**
 * Scroll a container the finger holds near its top or bottom edge.
 *
 * A finger over a destination row that is fully visible means "drop here", so
 * nothing moves under it; a row the edge clips is revealed by exactly the
 * hidden part. Only with no row under the finger does the edge scroll a step.
 *
 * ``armed`` (one set per drag) holds the containers the finger has been inside
 * away from their edges: a drag picked up in an edge zone does not scroll the
 * page out from under the finger until it has left that zone once.
 */
export function edgeScroll(clientY: number, row: Element | null, armed?: Set<string>): void {
  for (const sel of [".df2-ms-panel-list", ".df2-ms-scroll"]) {
    const el = document.querySelector<HTMLElement>(sel);
    if (!el || el.scrollHeight <= el.clientHeight) continue;
    const r = el.getBoundingClientRect();
    if (clientY < r.top || clientY > r.bottom) continue;
    // A short list keeps most of its height as a drop zone.
    const edge = Math.min(EDGE_PX, r.height / 5);
    const nearTop = clientY < r.top + edge;
    const nearBottom = clientY > r.bottom - edge;
    if (!nearTop && !nearBottom) {
      armed?.add(sel);
      continue;
    }
    if (armed && !armed.has(sel)) continue;
    if (row && el.contains(row)) {
      const rr = row.getBoundingClientRect();
      if (nearBottom && rr.bottom > r.bottom) el.scrollTop += Math.min(STEP_PX, rr.bottom - r.bottom);
      else if (nearTop && rr.top < r.top) el.scrollTop -= Math.min(STEP_PX, r.top - rr.top);
      continue;
    }
    el.scrollTop += nearTop ? -STEP_PX : STEP_PX;
  }
}

function destRowAt(x: number, y: number): Element | null {
  return document.elementFromPoint(x, y)?.closest?.("[data-dest-column]") ?? null;
}

/**
 * Drag a source onto a destination column with a finger or a pen.
 *
 * HTML5 drag and drop does not fire for touch in mobile browsers, so touch and
 * pen use pointer events: the grip captures the pointer, every move hit-tests
 * the point for a destination row (``data-dest-column``), the panel scrolls
 * when the finger rests at its edge, and lifting the finger drops (or cancels
 * when it is not over a column). The mouse keeps native drag and drop and the
 * keyboard keeps its own path; all three end in the same drop.
 */
/** A press becomes a drag after this much movement… */
export const PICKUP_DISTANCE_PX = 8;
/** …or after being held this long without lifting. */
export const PICKUP_HOLD_MS = 150;

export function useTouchDrag(studio: DragStudio, onStart: () => void) {
  const overRef = useRef<string | null>(null);
  const armedRef = useRef<Set<string>>(new Set());
  // A press that has not become a drag yet: a tap or a flick never picks up.
  const pressRef = useRef<{ source: string; x: number; y: number; timer: number } | null>(null);
  const [touching, setTouching] = useState<string | null>(null);

  const clearPress = () => {
    if (pressRef.current) window.clearTimeout(pressRef.current.timer);
    pressRef.current = null;
  };

  const pickUp = (source: string) => {
    clearPress();
    overRef.current = null;
    armedRef.current = new Set();
    setTouching(source);
    onStart();
    studio.dispatchDnd({ type: "pickUp", source, via: "pointer" });
  };

  const handlers = (source: string) => ({
    onPointerDown(e: PointerEvent<HTMLElement>) {
      if (e.pointerType === "mouse") return;
      e.preventDefault();
      try {
        e.currentTarget.setPointerCapture(e.pointerId);
      } catch {
        /* A synthetic pointer may not be capturable; hit-testing still works. */
      }
      clearPress();
      const timer = window.setTimeout(() => {
        if (pressRef.current?.source === source) pickUp(source);
      }, PICKUP_HOLD_MS);
      pressRef.current = { source, x: e.clientX, y: e.clientY, timer };
    },
    onPointerMove(e: PointerEvent<HTMLElement>) {
      if (e.pointerType === "mouse") return;
      const press = pressRef.current;
      if (press?.source === source) {
        if (Math.hypot(e.clientX - press.x, e.clientY - press.y) < PICKUP_DISTANCE_PX) return;
        pickUp(source);
      } else if (touching !== source) {
        return;
      }
      edgeScroll(e.clientY, destRowAt(e.clientX, e.clientY), armedRef.current);
      // Hit-test after any scroll: the row under the finger is the drop target.
      const name = destRowAt(e.clientX, e.clientY)?.getAttribute("data-dest-column") ?? null;
      overRef.current = name;
      studio.pointerOver(name);
    },
    onPointerUp(e: PointerEvent<HTMLElement>) {
      if (e.pointerType === "mouse") return;
      if (pressRef.current?.source === source) {
        clearPress(); // a tap: nothing was picked up
        return;
      }
      if (touching !== source) return;
      setTouching(null);
      const name = overRef.current;
      overRef.current = null;
      // pointerDrop drops only while this pointer drag is still active: a drag
      // cancelled meanwhile (Escape) drops nothing.
      if (name) studio.pointerDrop(source, name);
      else studio.dispatchDnd({ type: "cancel" });
    },
    onPointerCancel(e: PointerEvent<HTMLElement>) {
      if (e.pointerType === "mouse") return;
      clearPress();
      if (touching !== source) return;
      setTouching(null);
      overRef.current = null;
      studio.dispatchDnd({ type: "cancel" });
    },
  });

  return { handlers, touching };
}
