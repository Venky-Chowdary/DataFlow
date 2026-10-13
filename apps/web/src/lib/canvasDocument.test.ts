import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  classifyChainLink,
  insertIndexForDrop,
  layoutFor,
  planCards,
  reorderByDrag,
  stepsToFlow,
  stepsWithoutPlanOps,
} from "./canvasDocument.js";
import { stepFromDraft, type ShapeOperation, type ShapeStepWire } from "./shape.js";

const trim: ShapeOperation = {
  op: "trim",
  summary: "Remove leading and trailing whitespace",
  active: false,
  family: "cleanse",
  needs_column: true,
  options: [],
  required: [],
  expression_option: null,
};

const filter: ShapeOperation = {
  op: "filter_rows",
  summary: "Keep only rows the condition matches",
  active: true,
  family: "rows",
  needs_column: false,
  options: ["condition", "keep"],
  required: ["condition"],
  expression_option: "condition",
};

describe("canvas document", () => {
  it("round-trips an ordered recipe into a chain and back through a disconnect", () => {
    const steps: ShapeStepWire[] = [
      { op: "trim", column: "name" },
      { op: "derive_column", options: { to: "domain", expression: "lower([email])" } },
      { op: "filter_rows", options: { condition: "[name] <> ''" } },
    ];
    const flow = stepsToFlow(steps, [trim, filter], "customers", false);
    assert.equal(flow.nodes.length, 3);
    assert.equal(flow.edges.length, 2);
    assert.equal(flow.edges[0].source, "step-0");
    assert.equal(flow.edges[0].target, "step-1");
    assert.equal(flow.nodes[0].family, "cleanse");
    assert.equal(flow.nodes[1].changesRowCount, false);
    assert.equal(flow.nodes[2].changesRowCount, true);
    assert.equal(flow.nodes[2].family, "rows");
    const reordered = reorderByDrag(steps, 2, 0);
    assert.equal(reordered[0].op, "filter_rows");
    assert.equal(reordered[1].op, "trim");
    assert.equal(reorderByDrag(steps, 0, 0), steps);
    assert.equal(reorderByDrag(steps, 5, 0), steps);
  });

  it("refuses a self, backward, or skip link and accepts only the next step", () => {
    assert.equal(classifyChainLink(3, 0, 1).ok, true);
    assert.equal(classifyChainLink(3, 0, 0).ok, false);
    assert.match(classifyChainLink(3, 0, 0).reason, /itself/);
    assert.match(classifyChainLink(3, 2, 0).reason, /forward/);
    assert.match(classifyChainLink(3, 0, 2).reason, /one chain/);
    assert.match(classifyChainLink(3, -1, 0).reason, /not on this recipe/);
    assert.equal(insertIndexForDrop(0, 400), 0);
    assert.equal(insertIndexForDrop(3, 0), 0);
    assert.equal(insertIndexForDrop(3, 148), 1);
    assert.equal(insertIndexForDrop(3, 0), 0);
    assert.equal(insertIndexForDrop(3, 900), 3);
  });

  it("dims steps that belong to another table and disables the chain when sync refuses", () => {
    const steps: ShapeStepWire[] = [
      { op: "trim", column: "name", source_table: "orders" },
      { op: "trim", column: "email", source_table: "customers" },
    ];
    const flow = stepsToFlow(steps, [trim], "customers", false);
    assert.equal(flow.nodes[0].dimmed, true);
    assert.equal(flow.nodes[1].dimmed, false);
    const refused = stepsToFlow(steps, [trim], "customers", true);
    assert.equal(refused.nodes.every((node) => node.disabled), true);
  });

  it("keeps post-load operations off the recipe", () => {
    const cards = planCards(["join", "aggregate"]);
    assert.equal(cards[0].caption, "Runs after load as SQL");
    assert.equal(cards[0].href, "#/transforms");
    const cleaned = stepsWithoutPlanOps(
      [{ op: "trim", column: "name" }, { op: "join" }],
      ["join", "aggregate"],
    );
    assert.deepEqual(cleaned, [{ op: "trim", column: "name" }]);
  });

  it("lays out 100 nodes as a list without cycles", () => {
    const steps = Array.from({ length: 100 }, (_, index) => ({ op: "trim", column: `c${index}` }));
    const started = Date.now();
    const flow = stepsToFlow(steps, [trim], "", false);
    assert.ok(Date.now() - started < 50);
    assert.equal(flow.nodes.length, 100);
    assert.equal(flow.edges.length, 99);
    const ids = new Set(flow.edges.map((edge) => `${edge.source}>${edge.target}`));
    assert.equal(ids.size, 99);
    assert.equal(layoutFor(100).length, 100);
  });
});

describe("step draft shared by the form and the canvas", () => {
  it("refuses a filter without a condition and builds the same trim wire", () => {
    const missing = stepFromDraft(filter, "", {}, "", "refuse");
    assert.equal(missing.step, null);
    assert.match(missing.error, /Condition/);
    const built = stepFromDraft(trim, "email", {}, "Tidy", "divert");
    assert.deepEqual(built.step, {
      op: "trim",
      column: "email",
      options: {},
      label: "Tidy",
      on_error: "divert",
    });
  });
});
