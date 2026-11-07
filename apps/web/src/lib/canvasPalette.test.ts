import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { filterPalette, paletteActions } from "./canvasPalette.js";

describe("command palette", () => {
  it("indexes operations and step numbers, not column names", () => {
    const actions = paletteActions({
      operations: [{ op: "trim", summary: "Remove leading and trailing whitespace" }],
      stepCount: 2,
      canContinue: true,
      canAdd: true,
    });
    const labels = actions.map((action) => action.label).join("\n");
    assert.match(labels, /Run preview/);
    assert.match(labels, /Continue/);
    assert.match(labels, /Jump to step 1/);
    assert.match(labels, /Add Remove leading and trailing whitespace/);
    assert.equal(labels.toLowerCase().includes("customer"), false);
    assert.equal(filterPalette(actions, "trim").some((action) => action.id === "add-trim"), true);
    assert.equal(filterPalette(actions, "nope").length, 0);
  });

  it("hides add and continue when the operator cannot take them", () => {
    const actions = paletteActions({
      operations: [{ op: "trim", summary: "Trim" }],
      stepCount: 0,
      canContinue: false,
      canAdd: false,
    });
    assert.equal(actions.some((action) => action.id.startsWith("add-")), false);
    assert.equal(actions.some((action) => action.id === "continue"), false);
    assert.equal(actions.some((action) => action.id === "preview"), true);
  });
});
