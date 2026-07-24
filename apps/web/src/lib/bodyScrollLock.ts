/**
 * Refcounted body scroll lock for stacked Dialog / Drawer overlays.
 * Without a count, closing an inner panel restores overflow while an outer
 * panel is still open — page scrolls under the remaining overlay.
 */

let lockCount = 0;
let savedOverflow = "";

export function lockBodyScroll(): () => void {
  if (typeof document === "undefined") {
    return () => undefined;
  }
  if (lockCount === 0) {
    savedOverflow = document.body.style.overflow;
    document.body.style.overflow = "hidden";
  }
  lockCount += 1;
  let released = false;
  return () => {
    if (released) return;
    released = true;
    lockCount = Math.max(0, lockCount - 1);
    if (lockCount === 0) {
      document.body.style.overflow = savedOverflow;
      savedOverflow = "";
    }
  };
}
