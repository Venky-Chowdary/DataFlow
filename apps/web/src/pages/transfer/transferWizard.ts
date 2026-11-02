import { STEP_DESTINATION, STEP_MAP, STEP_RUN, STEP_SHAPE, STEP_SOURCE, STEP_VALIDATE } from "./studioConstants";

/**
 * The transfer wizard has one writer. Pipeline code advances with `go`.
 * Back is a guard: it closes the transform overlay first, then moves to the
 * previous section, and it never clears connectors, mappings, or shape steps.
 * WizardSteps is the only step control. The route bar is a label, not a second stepper.
 */
export interface WizardSnapshot {
  step: number;
  transformOpen: boolean;
}

export const WIZARD_START: WizardSnapshot = {
  step: STEP_SOURCE,
  transformOpen: false,
};

export type WizardAction =
  | { type: "go"; step: number }
  | { type: "back"; canvas: boolean }
  | { type: "open-transform" }
  | { type: "close-transform" };

export function transferWizardReducer(state: WizardSnapshot, action: WizardAction): WizardSnapshot {
  switch (action.type) {
    case "open-transform":
      return { ...state, transformOpen: true };
    case "close-transform":
      return { ...state, transformOpen: false };
    case "go":
      // Any explicit navigation leaves the overlay. A step change that left
      // the overlay mounted is what made Back look dead.
      return { step: action.step, transformOpen: false };
    case "back":
      return backWizard(state, action.canvas);
    default:
      return state;
  }
}

function backWizard(state: WizardSnapshot, canvas: boolean): WizardSnapshot {
  if (state.transformOpen) return { ...state, transformOpen: false };
  if (!canvas) return state;
  if (state.step === STEP_RUN) return { step: STEP_MAP, transformOpen: false };
  if (state.step === STEP_VALIDATE) return { step: STEP_MAP, transformOpen: false };
  if (state.step === STEP_MAP || state.step === STEP_SHAPE) return { step: STEP_DESTINATION, transformOpen: false };
  if (state.step === STEP_DESTINATION) return { step: STEP_SOURCE, transformOpen: false };
  return state;
}
