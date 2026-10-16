import { useEffect, useRef, type PointerEvent as ReactPointerEvent } from "react";

/**
 * One pointer-drag session. Capture stays on the handle, and the returned
 * stop removes the listeners. Callers must run that stop on unmount.
 */
export function bindPointerDrag(
  target: HTMLElement,
  pointerId: number,
  onMove: (event: PointerEvent) => void,
): () => void {
  try {
    target.setPointerCapture(pointerId);
  } catch {
    // The pointer can end before the browser accepts capture.
  }
  const end = () => {
    target.removeEventListener("pointermove", onMove);
    target.removeEventListener("pointerup", end);
    target.removeEventListener("pointercancel", end);
    if (typeof target.hasPointerCapture === "function" && target.hasPointerCapture(pointerId)) {
      try {
        target.releasePointerCapture(pointerId);
      } catch {
        // Already released.
      }
    }
  };
  target.addEventListener("pointermove", onMove);
  target.addEventListener("pointerup", end);
  target.addEventListener("pointercancel", end);
  return end;
}

/** Starts a drag and stops it if the owner unmounts mid-gesture. */
export function usePointerDrag(): (
  event: ReactPointerEvent<HTMLElement>,
  onMove: (event: PointerEvent) => void,
) => void {
  const stop = useRef<(() => void) | null>(null);
  useEffect(() => () => {
    stop.current?.();
    stop.current = null;
  }, []);
  return (event, onMove) => {
    event.preventDefault();
    stop.current?.();
    stop.current = bindPointerDrag(event.currentTarget, event.pointerId, onMove);
  };
}
