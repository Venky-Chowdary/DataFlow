import { useEffect } from "react";

/**
 * Call `refresh` on an interval while the document is visible, and again when
 * the tab becomes visible or the window regains focus. Settings surfaces use
 * this so Team, notifications, and audit stay current without a manual reload.
 */
export function useVisibleRefresh(refresh: () => void, intervalMs: number, enabled = true): void {
  useEffect(() => {
    if (!enabled) return;
    const tick = () => {
      if (typeof document !== "undefined" && document.hidden) return;
      refresh();
    };
    const id = window.setInterval(tick, intervalMs);
    const onWake = () => {
      if (typeof document !== "undefined" && document.hidden) return;
      refresh();
    };
    document.addEventListener("visibilitychange", onWake);
    window.addEventListener("focus", onWake);
    return () => {
      window.clearInterval(id);
      document.removeEventListener("visibilitychange", onWake);
      window.removeEventListener("focus", onWake);
    };
  }, [refresh, intervalMs, enabled]);
}
