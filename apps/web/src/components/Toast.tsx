import { createContext, useCallback, useContext, useMemo, useRef, useState, type ReactNode } from "react";
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

  const value = useMemo(() => ({ toast, dismiss }), [toast, dismiss]);

  return (
    <ToastContext.Provider value={value}>
      {children}
      <div className="dt-toast-host" aria-live="polite" aria-relevant="additions" aria-atomic="false">
        {items.map((t) => (
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
      </div>
    </ToastContext.Provider>
  );
}

export function useToast() {
  const ctx = useContext(ToastContext);
  if (!ctx) {
    if (import.meta.env.DEV) {
      console.warn("useToast called without ToastProvider — toasts are no-ops until provider mounts.");
    }
    return {
      toast: ({ title, message, tone }: { title: string; message?: string; tone?: ToastTone }) => {
        console.warn("[toast]", tone ?? "info", title, message ?? "");
      },
      dismiss: (_id: string) => {},
    };
  }
  return ctx;
}
