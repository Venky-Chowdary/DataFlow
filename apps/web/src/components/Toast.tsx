import { createContext, useCallback, useContext, useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import { createPortal } from "react-dom";
import { DtIcon } from "./DtIcon";
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
  const timers = useRef(new Map<string, number>());

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
      const holdMs = tone === "error" ? 16000 : tone === "warning" ? 10000 : 4500;
      timers.current.set(id, window.setTimeout(() => dismiss(id), holdMs));
    },
    [dismiss],
  );

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
        id: crypto.randomUUID(),
        key,
        title: cleanTitle,
        message: cleanMessage,
        tone,
        createdAt: now,
      };
      let shownId = incoming.id;
      setItems((prev) => {
        const merged = mergeToastStack(prev, incoming, now);
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
      const el = document.querySelector<HTMLElement>("[data-df2-toast-dock]");
      setDock((prev) => (prev === el ? prev : el));
    };
    sync();
    // The dock mounts/unmounts inside children without re-rendering this
    // provider (children prop is referentially stable), so a bare
    // querySelector-on-render misses it — a toast emitted before the dock
    // existed would float forever. Observe instead (D-BP-20 residual).
    const mo = new MutationObserver(sync);
    mo.observe(document.body, { childList: true, subtree: true });
    return () => mo.disconnect();
  }, []);

  // D-BP-27: a docked lane can never render a partially visible toast — cap
  // visible to the newest 2 and collapse the rest into a "+N more" chip.
  const visible = dock ? items.slice(-2) : items;
  const hiddenCount = dock ? Math.max(0, items.length - visible.length) : 0;

  const host = (
    <div className="dt-toast-host" aria-live="polite" aria-relevant="additions" aria-atomic="false">
        {visible.map((t) => (
          <div key={t.id} className={`dt-toast dt-toast--${t.tone}`} role={t.tone === "error" ? "alert" : "status"}>
            <span className="dt-toast-icon" aria-hidden>
              <DtIcon name={TONE_ICON[t.tone]} size={18} />
            </span>
            <div className="dt-toast-body">
              <strong className="dt-toast-title">{t.title}</strong>
              {t.message && <span className="dt-toast-message">{t.message}</span>}
            </div>
            <button type="button" className="dt-toast-close" onClick={() => dismiss(t.id)} aria-label="Dismiss notification">
              <DtIcon name="x" size={16} />
            </button>
            <span className={`dt-toast-timer dt-toast-timer--${t.tone}`} aria-hidden />
          </div>
        ))}
        {hiddenCount > 0 && (
          <div className="dt-toast-more" role="status">
            +{hiddenCount} more
          </div>
        )}
    </div>
  );

  return (
    <ToastContext.Provider value={value}>
      {children}
      {dock ? createPortal(host, dock) : host}
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
