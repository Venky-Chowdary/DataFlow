import { useRef, useState, type PointerEvent } from "react";
import type { DndAction } from "../../lib/mappingDnd";

interface DragStudio {
  dispatchDnd: (action: DndAction) => void;
  pointerOver: (name: string | null) => void;
  pointerDrop: (source: string, target: string) => void;
}

const EDGE_PX = 48;
const STEP_PX = 24;

/** Scroll a container that the finger is holding near its top or bottom edge. */
function autoScroll(clientY: number): void {
  for (const sel of [".df2-ms-panel-list", ".df2-ms-scroll"]) {
    const el = document.querySelector<HTMLElement>(sel);
    if (!el || el.scrollHeight <= el.clientHeight) continue;
    const r = el.getBoundingClientRect();
    if (clientY < r.top || clientY > r.bottom) continue;
    // A short list keeps most of its height as a drop zone.
    const edge = Math.min(EDGE_PX, r.height / 5);
    if (clientY < r.top + edge) el.scrollTop -= STEP_PX;
    else if (clientY > r.bottom - edge) el.scrollTop += STEP_PX;
  }
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
export function useTouchDrag(studio: DragStudio, onStart: () => void) {
  const overRef = useRef<string | null>(null);
  const [touching, setTouching] = useState<string | null>(null);

  const handlers = (source: string) => ({
    onPointerDown(e: PointerEvent<HTMLElement>) {
      if (e.pointerType === "mouse") return;
      e.preventDefault();
      try {
        e.currentTarget.setPointerCapture(e.pointerId);
      } catch {
        /* A synthetic pointer may not be capturable; hit-testing still works. */
      }
      overRef.current = null;
      setTouching(source);
      onStart();
      studio.dispatchDnd({ type: "pickUp", source, via: "pointer" });
    },
    onPointerMove(e: PointerEvent<HTMLElement>) {
      if (e.pointerType === "mouse" || touching !== source) return;
      const hit = document.elementFromPoint(e.clientX, e.clientY);
      const name = hit?.closest?.("[data-dest-column]")?.getAttribute("data-dest-column") ?? null;
      overRef.current = name;
      studio.pointerOver(name);
      autoScroll(e.clientY);
    },
    onPointerUp(e: PointerEvent<HTMLElement>) {
      if (e.pointerType === "mouse" || touching !== source) return;
      setTouching(null);
      const name = overRef.current;
      overRef.current = null;
      if (name) studio.pointerDrop(source, name);
      else studio.dispatchDnd({ type: "cancel" });
    },
    onPointerCancel(e: PointerEvent<HTMLElement>) {
      if (e.pointerType === "mouse" || touching !== source) return;
      setTouching(null);
      overRef.current = null;
      studio.dispatchDnd({ type: "cancel" });
    },
  });

  return { handlers, touching };
}
