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

export interface ConfirmFact {
  label: string;
  value: string;
}

export interface ConfirmOptions {
  title: string;
  message?: string;
  confirmLabel?: string;
  cancelLabel?: string;
  tone?: ConfirmTone;
  /** What the confirm will run. Rendered as a definition list, not folded into the message. */
  facts?: ConfirmFact[];
}

export interface AskField {
  name: string;
  label: string;
  defaultValue?: string;
  required?: boolean;
}

export interface AskOptions {
  title: string;
  message?: string;
  confirmLabel?: string;
  cancelLabel?: string;
  tone?: ConfirmTone;
  fields: AskField[];
}

interface ConfirmContextValue {
  confirm: (options: ConfirmOptions) => Promise<boolean>;
  ask: (options: AskOptions) => Promise<Record<string, string> | null>;
}

const ConfirmContext = createContext<ConfirmContextValue | null>(null);

type Pending =
  | (ConfirmOptions & { kind: "confirm"; resolve: (value: boolean) => void })
  | (AskOptions & { kind: "ask"; resolve: (value: Record<string, string> | null) => void });

/**
 * In-app confirmation and prompt dialogs.
 * Use `useConfirm()` from any screen inside ConfirmProvider.
 * `confirm` resolves false and `ask` resolves null when no provider is mounted.
 */
export function ConfirmProvider({ children }: { children: ReactNode }) {
  const [pending, setPending] = useState<Pending | null>(null);
  const pendingRef = useRef<Pending | null>(null);
  const [fieldValues, setFieldValues] = useState<Record<string, string>>({});

  const settle = useCallback((current: Pending, result: boolean | Record<string, string> | null) => {
    if (current.kind === "ask") {
      current.resolve(result && typeof result === "object" ? result : null);
      return;
    }
    current.resolve(result === true);
  }, []);

  const close = useCallback((result: boolean | Record<string, string> | null) => {
    const current = pendingRef.current;
    pendingRef.current = null;
    setPending(null);
    setFieldValues({});
    if (current) settle(current, result);
  }, [settle]);

  const open = useCallback((next: Pending, fields?: AskField[]) => {
    if (pendingRef.current) settle(pendingRef.current, false);
    pendingRef.current = next;
    setPending(next);
    if (fields) {
      setFieldValues(Object.fromEntries(fields.map((field) => [field.name, field.defaultValue ?? ""])));
    } else {
      setFieldValues({});
    }
  }, [settle]);

  const confirm = useCallback((options: ConfirmOptions) => {
    return new Promise<boolean>((resolve) => {
      open({ ...options, kind: "confirm", resolve });
    });
  }, [open]);

  const ask = useCallback((options: AskOptions) => {
    return new Promise<Record<string, string> | null>((resolve) => {
      open({ ...options, kind: "ask", resolve }, options.fields);
    });
  }, [open]);

  const value = useMemo(() => ({ confirm, ask }), [confirm, ask]);
  const tone = pending?.tone ?? "default";
  const confirmVariant = tone === "danger" ? "danger" : "primary";
  const asking = pending?.kind === "ask";
  const missingRequired = asking
    && pending.fields.some((field) => field.required && !(fieldValues[field.name] || "").trim());

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
              autoFocus={!asking}
              disabled={Boolean(missingRequired)}
              onClick={() => close(asking ? fieldValues : true)}
            >
              {pending?.confirmLabel || "Confirm"}
            </Button>
          </>
        }
      >
        {pending?.kind === "confirm" && pending.facts && pending.facts.length > 0 && (
          <dl className="df2-confirm-facts">
            {pending.facts.map((fact) => (
              <div key={fact.label}>
                <dt>{fact.label}</dt>
                <dd>{fact.value}</dd>
              </div>
            ))}
          </dl>
        )}
        {asking && (
          <div
            className="df2-confirm-fields"
            onKeyDown={(event) => {
              if (event.key === "Enter" && !missingRequired) {
                event.preventDefault();
                close(fieldValues);
              }
            }}
          >
            {pending.fields.map((field, index) => (
              <label key={field.name} htmlFor={`df2-confirm-field-${field.name}`}>
                <span>{field.label}</span>
                <input
                  id={`df2-confirm-field-${field.name}`}
                  value={fieldValues[field.name] ?? ""}
                  autoFocus={index === 0}
                  required={field.required}
                  onChange={(event) => {
                    const next = event.target.value;
                    setFieldValues((current) => ({ ...current, [field.name]: next }));
                  }}
                />
              </label>
            ))}
          </div>
        )}
        {pending?.message ? (
          <p className="df2-confirm-message">{pending.message}</p>
        ) : (
          <p className="df2-confirm-message df2-confirm-message-muted">Please confirm to continue.</p>
        )}
      </Dialog>
    </ConfirmContext.Provider>
  );
}

export function useConfirm(): ConfirmContextValue {
  const ctx = useContext(ConfirmContext);
  if (!ctx) {
    if ((import.meta as { env?: { DEV?: boolean } }).env?.DEV) {
      console.warn("useConfirm called without ConfirmProvider — the dialog is refused until the provider mounts");
    }
    return {
      confirm: async () => false,
      ask: async () => null,
    };
  }
  return ctx;
}
