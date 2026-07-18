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
}

const StudioActionsContext = createContext<StudioActionsValue>({
  registerStudioHandler: () => {},
  dispatchStudioAction: () => {},
});

export function StudioActionsProvider({ children }: { children: ReactNode }) {
  const handlerRef = useRef<StudioActionHandler | null>(null);

  const registerStudioHandler = useCallback((handler: StudioActionHandler | null) => {
    handlerRef.current = handler;
  }, []);

  const dispatchStudioAction = useCallback((action: StudioAction) => {
    void handlerRef.current?.(action);
  }, []);

  return (
    <StudioActionsContext.Provider value={{ registerStudioHandler, dispatchStudioAction }}>
      {children}
    </StudioActionsContext.Provider>
  );
}

export function useStudioActions() {
  return useContext(StudioActionsContext);
}
