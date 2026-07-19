import {
  createContext,
  useCallback,
  useContext,
  useMemo,
  useRef,
  useState,
  type ReactNode,
} from "react";
import { Dialog } from "./Dialog";
import { Button } from "./Button";
import { DtIcon } from "../DtIcon";

export type ConfirmTone = "danger" | "warning" | "default";

export interface ConfirmOptions {
  title: string;
  message?: string;
  confirmLabel?: string;
  cancelLabel?: string;
  tone?: ConfirmTone;
}

interface ConfirmContextValue {
  confirm: (options: ConfirmOptions) => Promise<boolean>;
}

const ConfirmContext = createContext<ConfirmContextValue | null>(null);

type Pending = ConfirmOptions & {
  resolve: (value: boolean) => void;
};

/**
 * In-app confirmation dialogs (never browser `window.confirm`).
 * Use `useConfirm()` from any screen inside ConfirmProvider.
 */
export function ConfirmProvider({ children }: { children: ReactNode }) {
  const [pending, setPending] = useState<Pending | null>(null);
  const pendingRef = useRef<Pending | null>(null);

  const close = useCallback((result: boolean) => {
    const current = pendingRef.current;
    pendingRef.current = null;
    setPending(null);
    current?.resolve(result);
  }, []);

  const confirm = useCallback((options: ConfirmOptions) => {
    return new Promise<boolean>((resolve) => {
      if (pendingRef.current) {
        pendingRef.current.resolve(false);
      }
      const next: Pending = { ...options, resolve };
      pendingRef.current = next;
      setPending(next);
    });
  }, []);

  const value = useMemo(() => ({ confirm }), [confirm]);
  const tone = pending?.tone ?? "default";
  const confirmVariant = tone === "danger" ? "danger" : "primary";

  return (
    <ConfirmContext.Provider value={value}>
      {children}
      <Dialog
        open={Boolean(pending)}
        onClose={() => close(false)}
        size="md"
        ariaLabel={pending?.title || "Confirm"}
        className={`df2-confirm-dialog df2-confirm-${tone}`}
        title={
          <span className="df2-confirm-title">
            {tone === "danger" && (
              <span className="df2-confirm-icon" aria-hidden>
                <DtIcon name="gate" size={18} />
              </span>
            )}
            {pending?.title}
          </span>
        }
        footer={
          <>
            <Button variant="ghost" size="sm" onClick={() => close(false)}>
              {pending?.cancelLabel || "Cancel"}
            </Button>
            <Button
              variant={confirmVariant}
              size="sm"
              autoFocus
              onClick={() => close(true)}
            >
              {pending?.confirmLabel || "Confirm"}
            </Button>
          </>
        }
      >
        {pending?.message ? (
          <p className="df2-confirm-message">{pending.message}</p>
        ) : (
          <p className="df2-confirm-message df2-confirm-message-muted">Please confirm to continue.</p>
        )}
      </Dialog>
    </ConfirmContext.Provider>
  );
}

export function useConfirm() {
  const ctx = useContext(ConfirmContext);
  if (!ctx) {
    if (import.meta.env.DEV) {
      console.warn("useConfirm called without ConfirmProvider — falling back to window.confirm");
    }
    return {
      confirm: async (options: ConfirmOptions) =>
        typeof window !== "undefined"
          ? window.confirm([options.title, options.message].filter(Boolean).join("\n\n"))
          : false,
    };
  }
  return ctx;
}
