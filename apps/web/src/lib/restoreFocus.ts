import { useEffect, useRef, type RefObject } from "react";

import { isTopOverlay, pushOverlay } from "./overlayStack";

/**
 * Return focus to whatever opened an overlay when it closes (UX-15, WCAG
 * 2.4.3). Dialogs and drawers capture the trigger when they open and call the
 * returned function when they close. Focus is only moved when it was lost
 * (it sits on <body> or inside the overlay being removed), so a dialog opened
 * on top, or a control the user already moved to, keeps it. A trigger that
 * left the page meanwhile falls back to the main landmark.
 */
/**
 * The last few focused elements. An overlay whose content autofocuses (a
 * search box) has already pulled focus inside by the time its open effect
 * runs, so the trigger is the most recent focus outside the panel.
 */
const recentFocus: HTMLElement[] = [];
if (typeof document !== "undefined") {
  document.addEventListener(
    "focusin",
    (event) => {
      const target = event.target;
      if (!(target instanceof HTMLElement)) return;
      const at = recentFocus.indexOf(target);
      if (at >= 0) recentFocus.splice(at, 1);
      recentFocus.push(target);
      if (recentFocus.length > 12) recentFocus.shift();
    },
    true,
  );
}

function triggerOutside(panel: HTMLElement | null | undefined): HTMLElement | null {
  const active = document.activeElement;
  if (active instanceof HTMLElement && active !== document.body && !(panel && panel.contains(active))) return active;
  for (let i = recentFocus.length - 1; i >= 0; i -= 1) {
    const el = recentFocus[i];
    if (el.isConnected && el !== document.body && !(panel && panel.contains(el))) return el;
  }
  return null;
}

export function captureFocusForRestore(explicit?: HTMLElement | null, panel?: () => HTMLElement | null): () => void {
  const trigger = explicit ?? (typeof document !== "undefined" ? triggerOutside(panel?.()) : null);
  const lost = () => {
    const current = document.activeElement;
    if (!current || current === document.body || !current.isConnected) return true;
    const owner = panel?.();
    return Boolean(owner && owner.contains(current));
  };
  const restore = () => {
    if (!lost()) return;
    if (trigger?.isConnected) {
      trigger.focus({ preventScroll: true });
      return;
    }
    document.querySelector<HTMLElement>("main[tabindex]")?.focus({ preventScroll: true });
  };
  return () => {
    restore();
    // The panel may still be in the document when an effect cleanup runs.
    if (typeof window !== "undefined" && typeof window.requestAnimationFrame === "function") {
      window.requestAnimationFrame(restore);
    }
  };
}

/**
 * Focus contract for a hand-rolled overlay: move focus into ``panelRef`` when
 * ``open`` turns true, give it back to the trigger when it turns false, and
 * (optionally) close on Escape.
 */
export function useOverlayFocus(
  open: boolean,
  panelRef: RefObject<HTMLElement | null>,
  onEscape?: () => void,
): void {
  const escapeRef = useRef(onEscape);
  useEffect(() => {
    escapeRef.current = onEscape;
  }, [onEscape]);
  useEffect(() => {
    if (!open) return;
    const restoreFocus = captureFocusForRestore(null, () => panelRef.current);
    const frame = window.requestAnimationFrame(() => {
      if (!panelRef.current?.contains(document.activeElement)) panelRef.current?.focus({ preventScroll: true });
    });
    const onKey = (e: KeyboardEvent) => {
      if (e.key !== "Escape" || e.defaultPrevented || !escapeRef.current) return;
      if (!isTopOverlay(panelRef.current)) return;
      e.preventDefault();
      escapeRef.current();
    };
    const popOverlay = panelRef.current ? pushOverlay(panelRef.current) : () => {};
    document.addEventListener("keydown", onKey);
    return () => {
      popOverlay();
      document.removeEventListener("keydown", onKey);
      window.cancelAnimationFrame(frame);
      restoreFocus();
    };
    // panelRef is a stable ref object; only open/close re-runs this.
  }, [open]);
}
