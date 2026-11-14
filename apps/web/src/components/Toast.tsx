import { createContext, useCallback, useContext, useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import { createPortal } from "react-dom";
import { DtIcon } from "./DtIcon";
import { Dialog } from "./ui/Dialog";
import { ErrorReferenceChip } from "./ErrorReferenceChip";
import { splitReference } from "../lib/errorCopy";
import {
  mergeToastStack,
  toastFingerprint,
  type ToastTone,
} from "../lib/toastDedupe";

export type { ToastTone };

export interface ToastItem {
  id: string;
  key: string;
  title: string;
  message?: string;
  tone: ToastTone;
  createdAt: number;
}

interface ToastContextValue {
  toast: (opts: { title: string; message?: string; tone?: ToastTone }) => void;
  dismiss: (id: string) => void;
  dismissMatching: (title: string) => void;
}

const ToastContext = createContext<ToastContextValue | null>(null);

const TONE_ICON: Record<ToastTone, string> = {
  info: "activity",
  success: "check",
  warning: "gate",
  error: "x",
};

export function ToastProvider({ children }: { children: ReactNode }) {
  const [items, setItems] = useState<ToastItem[]>([]);
  const [review, setReview] = useState<ToastItem[]>([]);
  const reviewRef = useRef<ToastItem[]>([]);
  const timers = useRef(new Map<string, number>());

  useEffect(() => () => {
    for (const handle of timers.current.values()) window.clearTimeout(handle);
    timers.current.clear();
  }, []);

  const dismiss = useCallback((id: string) => {
    const handle = timers.current.get(id);
    if (handle != null) {
      window.clearTimeout(handle);
      timers.current.delete(id);
    }
    setItems((prev) => prev.filter((t) => t.id !== id));
  }, []);

  const scheduleDismiss = useCallback(
    (id: string, tone: ToastTone) => {
      const prior = timers.current.get(id);
      if (prior != null) window.clearTimeout(prior);
      if (reviewRef.current.some((item) => item.id === id)) {
        timers.current.delete(id);
        return;
      }
      const holdMs = tone === "error" ? 16000 : tone === "warning" ? 10000 : 4500;
      timers.current.set(id, window.setTimeout(() => {
        if (!reviewRef.current.some((item) => item.id === id)) dismiss(id);
      }, holdMs));
    },
    [dismiss],
  );

  const openReview = useCallback((next: ToastItem[]) => {
    reviewRef.current = next;
    for (const item of next) {
      const handle = timers.current.get(item.id);
      if (handle != null) window.clearTimeout(handle);
      timers.current.delete(item.id);
    }
    setReview(next);
  }, []);

  const closeReview = useCallback(() => {
    const previous = reviewRef.current;
    reviewRef.current = [];
    setReview([]);
    for (const item of previous) scheduleDismiss(item.id, item.tone);
  }, [scheduleDismiss]);

  const dismissMatching = useCallback((title: string) => {
    const match = title.trim();
    setItems((prev) => {
      for (const item of prev) {
        if (item.title !== match) continue;
        const handle = timers.current.get(item.id);
        if (handle != null) {
          window.clearTimeout(handle);
          timers.current.delete(item.id);
        }
      }
      return prev.filter((item) => item.title !== match);
    });
  }, []);

  const toast = useCallback(
    ({ title, message, tone = "info" }: { title: string; message?: string; tone?: ToastTone }) => {
      const cleanTitle = title.trim();
      const cleanMessage = message?.trim() || undefined;
      const key = toastFingerprint({ title: cleanTitle, message: cleanMessage, tone });
      const now = Date.now();
      const incoming: ToastItem = {
        id: reviewRef.current.find((item) => item.key === key)?.id ?? crypto.randomUUID(),
        key,
        title: cleanTitle,
        message: cleanMessage,
        tone,
        createdAt: now,
      };
      let shownId = incoming.id;
      setItems((prev) => {
        const merged = mergeToastStack(prev, incoming, now, undefined, prev.length + 1);
        shownId = merged.shownId;
        return merged.items;
      });
      scheduleDismiss(shownId, tone);
    },
    [scheduleDismiss],
  );

  const value = useMemo(() => ({ toast, dismiss, dismissMatching }), [toast, dismiss, dismissMatching]);

  // D-BP-20: pages that declare a toast dock ([data-df2-toast-dock], e.g. the
  // studio header chrome) take the host as an inline pill strip — no floating
  // overlay, so toasts can never cover content and cannot reflow the page.
  const [dock, setDock] = useState<HTMLElement | null>(null);
  useEffect(() => {
    const sync = () => {
      const el = Array.from(document.querySelectorAll<HTMLElement>("[data-df2-toast-dock]"))
        .find((candidate) => {
          if (candidate.closest('[hidden], [inert], [aria-hidden="true"]')) return false;
          for (let ancestor: HTMLElement | null = candidate; ancestor; ancestor = ancestor.parentElement) {
            const style = window.getComputedStyle(ancestor);
            if (style.display === "none" || style.visibility === "hidden" || style.visibility === "collapse") return false;
          }
          return true;
        }) ?? null;
      setDock((prev) => (prev === el ? prev : el));
    };
    sync();
    // The dock mounts/unmounts inside children without re-rendering this
    // provider (children prop is referentially stable), so a bare
    // querySelector-on-render misses it — a toast emitted before the dock
    // existed would float forever. Observe instead (D-BP-20 residual).
    const mo = new MutationObserver(sync);
    mo.observe(document.body, {
      childList: true, subtree: true, attributes: true,
      attributeFilter: ["hidden", "inert", "aria-hidden", "style", "class"],
    });
    return () => mo.disconnect();
  }, []);

  // D-BP-27: a docked lane can never render a partially visible toast — cap
  // visible to the newest 2 and collapse the rest into a "+N more" chip.
  const visible = items.slice(-2);
  const hiddenCount = Math.max(0, items.length - visible.length);

  const host = (
    <div className="dt-toast-host" aria-live="polite" aria-relevant="additions" aria-atomic="false"
      aria-hidden={review.length > 0 || undefined} inert={review.length > 0 || undefined}
      style={review.length > 0 ? { display: "none" } : undefined}>
        {visible.map((t) => (
          <div key={t.id} className={`dt-toast dt-toast--${t.tone}`} role={t.tone === "error" ? "alert" : "status"}>
            <span className="dt-toast-icon" aria-hidden>
              <DtIcon name={TONE_ICON[t.tone]} size={18} />
            </span>
            <div className="dt-toast-body">
              <strong className="dt-toast-title">{t.title}</strong>
              {t.message && <span className="dt-toast-message">{splitReference(t.message).text}</span>}
              {t.message && <ErrorReferenceChip reference={splitReference(t.message).reference} />}
            </div>
            <button type="button" className="dt-toast-close" onClick={() => openReview([t])}
              aria-label="View notification details" aria-haspopup="dialog" title="View notification details">
              <DtIcon name="book" size={16} />
            </button>
            <button type="button" className="dt-toast-close" onClick={() => dismiss(t.id)} aria-label="Dismiss notification" title="Dismiss notification">
              <DtIcon name="x" size={16} />
            </button>
            <span className={`dt-toast-timer dt-toast-timer--${t.tone}`} aria-hidden />
          </div>
        ))}
        {hiddenCount > 0 && (
          <button type="button" className="dt-toast-more" onClick={() => openReview([...items].reverse())}
            aria-label={`Review ${hiddenCount} more notifications`} aria-haspopup="dialog" title="Review notifications">
            +{hiddenCount} more
          </button>
        )}
    </div>
  );

  return (
    <ToastContext.Provider value={value}>
      {children}
      {createPortal(host, dock ?? document.body)}
      <Dialog open={review.length > 0} onClose={closeReview} title="Notification details" size="md" className="dt-toast-review">
        {review.map((item) => (
          <section key={item.id} className={`dt-toast-review-item dt-toast-review-item--${item.tone}`}>
            <h3>{item.title}</h3>
            {item.message && <p>{splitReference(item.message).text}</p>}
            {item.message && <ErrorReferenceChip reference={splitReference(item.message).reference} />}
          </section>
        ))}
      </Dialog>
    </ToastContext.Provider>
  );
}

export function useToast() {
  const ctx = useContext(ToastContext);
  if (!ctx) {
    if ((import.meta as { env?: { DEV?: boolean } }).env?.DEV) {
      console.warn("useToast called without ToastProvider — toasts are no-ops until provider mounts.");
    }
    return {
      toast: ({ title, message, tone }: { title: string; message?: string; tone?: ToastTone }) => {
        console.warn("[toast]", tone ?? "info", title, message ?? "");
      },
      dismiss: (_id: string) => {},
      dismissMatching: (_title: string) => {},
    };
  }
  return ctx;
}
