import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { STEP_DESTINATION, STEP_MAP, STEP_RUN, STEP_SHAPE, STEP_SOURCE, STEP_VALIDATE } from "./studioConstants.js";
import { transferWizardReducer, WIZARD_START } from "./transferWizard.js";

describe("transfer wizard", () => {
  it("closes the transform overlay before it changes section", () => {
    const open = transferWizardReducer({ step: STEP_SOURCE, transformOpen: true }, { type: "back", canvas: true });
    assert.equal(open.transformOpen, false);
    assert.equal(open.step, STEP_SOURCE);

    const fromDest = transferWizardReducer({ step: STEP_DESTINATION, transformOpen: true }, { type: "back", canvas: true });
    assert.equal(fromDest.step, STEP_DESTINATION);
    assert.equal(fromDest.transformOpen, false);
  });

  it("walks canvas review back to destination and then source", () => {
    const review = transferWizardReducer({ step: STEP_MAP, transformOpen: false }, { type: "back", canvas: true });
    assert.deepEqual(review, { step: STEP_DESTINATION, transformOpen: false });
    const source = transferWizardReducer(review, { type: "back", canvas: true });
    assert.deepEqual(source, { step: STEP_SOURCE, transformOpen: false });
  });

  it("walks run and validate back onto review", () => {
    assert.equal(transferWizardReducer({ step: STEP_RUN, transformOpen: false }, { type: "back", canvas: true }).step, STEP_MAP);
    assert.equal(transferWizardReducer({ step: STEP_VALIDATE, transformOpen: false }, { type: "back", canvas: true }).step, STEP_MAP);
    assert.equal(transferWizardReducer({ step: STEP_SHAPE, transformOpen: false }, { type: "back", canvas: true }).step, STEP_DESTINATION);
  });

  it("does not move a classic step; classic buttons name their target", () => {
    const stayed = transferWizardReducer({ step: STEP_MAP, transformOpen: false }, { type: "back", canvas: false });
    assert.equal(stayed.step, STEP_MAP);
  });

  it("go lands on the requested step and clears the overlay", () => {
    const next = transferWizardReducer(
      { step: STEP_SOURCE, transformOpen: true },
      { type: "go", step: STEP_DESTINATION },
    );
    assert.deepEqual(next, { step: STEP_DESTINATION, transformOpen: false });
    assert.deepEqual(transferWizardReducer(WIZARD_START, { type: "open-transform" }).transformOpen, true);
  });
});
