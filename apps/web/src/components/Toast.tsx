import { createContext, useCallback, useContext, useMemo, useState, type ReactNode } from "react";
import { DtIcon } from "./DtIcon";

export type ToastTone = "info" | "success" | "warning" | "error";

export interface ToastItem {
  id: string;
  title: string;
  message?: string;
  tone: ToastTone;
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

  const dismiss = useCallback((id: string) => {
    setItems((prev) => prev.filter((t) => t.id !== id));
  }, []);

  const toast = useCallback(
    ({ title, message, tone = "info" }: { title: string; message?: string; tone?: ToastTone }) => {
      const id = crypto.randomUUID();
      setItems((prev) => [...prev.slice(-3), { id, title, message, tone }]);
      window.setTimeout(() => dismiss(id), tone === "error" ? 7000 : 4200);
    },
    [dismiss]
  );

  const value = useMemo(() => ({ toast, dismiss }), [toast, dismiss]);

  return (
    <ToastContext.Provider value={value}>
      {children}
      <div className="dt-toast-host" aria-live="polite" aria-relevant="additions">
        {items.map((t) => (
          <div key={t.id} className={`dt-toast dt-toast--${t.tone}`} role="status">
            <span className="dt-toast-icon" aria-hidden>
              <DtIcon name={TONE_ICON[t.tone]} size={18} />
            </span>
            <div className="dt-toast-body">
              <strong className="dt-toast-title">{t.title}</strong>
              {t.message && <span className="dt-toast-message">{t.message}</span>}
            </div>
            <button type="button" className="dt-toast-close" onClick={() => dismiss(t.id)} aria-label="Dismiss">
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
  if (!ctx) throw new Error("useToast requires ToastProvider");
  return ctx;
}
