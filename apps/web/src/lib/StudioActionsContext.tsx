import { createContext, useCallback, useContext, useRef, type ReactNode } from "react";

/** Studio remediations dispatched from Data Pilot (or Validate AI chips). */
export type StudioActionKind =
  | "normalize_control_chars"
  | "open_bad_data_fix"
  | "quarantine_and_rerun"
  | "review_mappings"
  | "rerun_preflight"
  | string;

export interface StudioAction {
  kind: StudioActionKind;
  label?: string;
  run_id?: string;
}

type StudioActionHandler = (action: StudioAction) => void | Promise<void>;

interface StudioActionsValue {
  registerStudioHandler: (handler: StudioActionHandler | null) => void;
  dispatchStudioAction: (action: StudioAction) => void;
  /** True when Transfer Studio has registered a live handler. */
  hasStudioHandler: () => boolean;
}

const StudioActionsContext = createContext<StudioActionsValue>({
  registerStudioHandler: () => {},
  dispatchStudioAction: () => {},
  hasStudioHandler: () => false,
});

export function StudioActionsProvider({ children }: { children: ReactNode }) {
  const handlerRef = useRef<StudioActionHandler | null>(null);
  const queueRef = useRef<StudioAction[]>([]);

  const flushQueue = useCallback(() => {
    const handler = handlerRef.current;
    if (!handler || !queueRef.current.length) return;
    const pending = queueRef.current.splice(0, queueRef.current.length);
    for (const action of pending) {
      void handler(action);
    }
  }, []);

  const registerStudioHandler = useCallback((handler: StudioActionHandler | null) => {
    handlerRef.current = handler;
    if (handler) flushQueue();
  }, [flushQueue]);

  const dispatchStudioAction = useCallback((action: StudioAction) => {
    if (handlerRef.current) {
      void handlerRef.current(action);
      return;
    }
    // Transfer may not be mounted yet — queue until registerStudioHandler runs.
    queueRef.current.push(action);
  }, []);

  const hasStudioHandler = useCallback(() => Boolean(handlerRef.current), []);

  return (
    <StudioActionsContext.Provider value={{ registerStudioHandler, dispatchStudioAction, hasStudioHandler }}>
      {children}
    </StudioActionsContext.Provider>
  );
}

export function useStudioActions() {
  return useContext(StudioActionsContext);
}
