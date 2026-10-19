import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  chainDocument,
  classifyChainLink,
  classifyPortLink,
  columnsBeforeStep,
  documentKind,
  stepExpressionIssues,
  draftWithDefaults,
  edgeRoute,
  viewportForChain,
  executableRecipe,
  insertIndexAtPoint,
  positionClearOf,
  insertIndexForDrop,
  layoutFor,
  LAYOUT_PITCH,
  NODE_WIDTH,
  planCards,
  reorderByDrag,
  spliceIndexAtPoint,
  stepsToFlow,
  stepsWithoutPlanOps,
} from "./canvasDocument.js";
import { streamPorts } from "./canvasRegistry.js";
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

  it("keeps a separator edge between every pair of a full six-step flow", () => {
    // D-BP batch: the canvas must never drop a connector inside the chain —
    // a 6-step recipe renders exactly 5 order edges, step i -> step i+1.
    const steps: ShapeStepWire[] = [
      { op: "trim", column: "name" },
      { op: "derive_column", options: { to: "domain", expression: "lower([email])" } },
      { op: "filter_rows", options: { condition: "[name] <> ''" } },
      { op: "replace", column: "name", options: { search: "a", replacement: "b" } },
      { op: "null_if", column: "name", options: { values: ["x"] } },
      { op: "default_if_null", column: "name", options: { value: "d" } },
    ];
    const flow = stepsToFlow(steps, [trim, filter], "customers", false);
    assert.equal(flow.nodes.length, 6);
    assert.equal(flow.edges.length, 5);
    for (let i = 0; i < 5; i += 1) {
      assert.equal(flow.edges[i].id, `order-${i}`);
      assert.equal(flow.edges[i].source, `step-${i}`);
      assert.equal(flow.edges[i].target, `step-${i + 1}`);
    }
  });

  it("refuses a self, backward, or skip link and accepts only the next step", () => {
    assert.equal(classifyChainLink(3, 0, 1).ok, true);
    assert.equal(classifyChainLink(3, 0, 0).ok, false);
    assert.match(classifyChainLink(3, 0, 0).reason, /itself/);
    assert.match(classifyChainLink(3, 2, 0).reason, /forward/);
    assert.match(classifyChainLink(3, 0, 2).reason, /one chain/);
    assert.match(classifyChainLink(3, 0, 2).reason, /one input and one output/);
    assert.match(classifyChainLink(3, -1, 0).reason, /not on this recipe/);
    assert.equal(insertIndexForDrop(0, 400), 0);
    assert.equal(insertIndexForDrop(3, 0), 0);
    assert.equal(insertIndexForDrop(3, LAYOUT_PITCH / 2), 1);
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

  it("places a drop on an edge between two nodes and defaults a column step", () => {
    const positions = layoutFor(2);
    const gap = { x: positions[0].x + NODE_WIDTH + 24, y: positions[0].y + 36 };
    assert.equal(spliceIndexAtPoint(positions, gap), 1);
    assert.equal(insertIndexAtPoint(2, gap, positions), 1);
    assert.equal(spliceIndexAtPoint(positions, { x: 48, y: 10 }), null);
    assert.equal(insertIndexAtPoint(0, { x: 10, y: 10 }, []), 0);
    assert.deepEqual(positionClearOf({ x: 40, y: 28 }, []), { x: 40, y: 28 });
    assert.deepEqual(positionClearOf({ x: 24, y: 28 }, [{ x: 24, y: 28 }]), { x: 40, y: 44 });
    assert.deepEqual(positionClearOf({ x: 80, y: 28 }, [{ x: 24, y: 28 }]), { x: 80, y: 28 });
    const moved = reorderByDrag(
      [{ op: "trim", column: "a" }, { op: "trim", column: "b" }, { op: "lower", column: "a" }],
      2,
      1,
    );
    assert.deepEqual(moved.map((step) => step.op), ["trim", "lower", "trim"]);
    assert.deepEqual(draftWithDefaults(trim, ["email", "name"]), { column: "email", options: {} });
    const filterDraft = draftWithDefaults(filter, ["email"]);
    assert.equal(filterDraft.column, "");
    assert.equal(filterDraft.options.keep, true);
    assert.equal(filterDraft.options.condition, undefined);
  });

  it("holds a chain and a dag, and keeps a blocked join out of the recipe", () => {
    const steps: ShapeStepWire[] = [
      { op: "trim", column: "name" },
      { op: "filter_rows", options: { condition: "[name] <> ''" } },
    ];
    const chain = chainDocument(steps);
    assert.equal(documentKind(chain), "chain");
    assert.equal(chain.edges[0].sourcePort, "out");
    assert.equal(chain.edges[0].targetPort, "in");
    assert.equal(executableRecipe(chain, new Set(["trim", "filter_rows"])).length, 2);
    const flow = stepsToFlow(steps, [trim, filter], "", false);
    assert.equal(flow.nodes[0].icon, "trim");
    assert.equal(flow.nodes[0].status, "valid");
    assert.equal(flow.nodes[1].stripe, "rows");
    assert.ok(flow.nodes[0].ports.some((port) => port.kind === "field" && port.visible === false));
    const join = {
      id: "join-1",
      op: "join",
      ports: [
        { id: "left", role: "in" as const, kind: "row" as const, label: "Left", visible: true },
        { id: "right", role: "in" as const, kind: "row" as const, label: "Right", visible: true },
        { id: "out", role: "out" as const, kind: "row" as const, label: "Joined", visible: true },
      ],
    };
    const after = {
      id: "step-2",
      op: "trim",
      ports: streamPorts(),
      step: { op: "trim", column: "city" },
    };
    const dag = {
      nodes: [...chain.nodes, join, after],
      edges: [
        ...chain.edges,
        { id: "to-join", source: "step-1", sourcePort: "out", target: "join-1", targetPort: "left" },
        { id: "from-join", source: "join-1", sourcePort: "out", target: "step-2", targetPort: "in" },
      ],
    };
    assert.equal(documentKind(dag), "dag");
    const recipe = executableRecipe(dag, new Set(["trim", "filter_rows"]));
    assert.deepEqual(recipe.map((step) => step.op), ["trim", "filter_rows"]);
    const refused = classifyPortLink({
      nodes: [
        { ...flow.nodes[0], executable: false },
        flow.nodes[1],
      ],
      source: "step-0",
      sourcePort: "out",
      target: "step-1",
      targetPort: "in",
    });
    assert.equal(refused.ok, false);
    assert.match(refused.reason, /not executable/);
    const accepted = classifyPortLink({
      nodes: flow.nodes,
      source: "step-0",
      sourcePort: "out",
      target: "step-1",
      targetPort: "in",
    });
    assert.equal(accepted.ok, true);
  });
});

describe("readable fit and columns still on the row", () => {
  it("keeps fit at or above 0.75 and pans when the chain is wider than the stage", () => {
    const one = viewportForChain({ x: 24, y: 28, width: 248, height: 160 }, 900, 500);
    assert.ok(one.zoom >= 0.75);
    assert.ok(one.zoom <= 1.5);
    const chain = viewportForChain({ x: 24, y: 28, width: 888, height: 160 }, 640, 420);
    assert.equal(chain.zoom, 0.75);
    assert.ok(chain.x <= 28);
  });

  it("draws a straight edge on one row and a rounded orthogonal edge across rows", () => {
    assert.equal(edgeRoute(28, 40), "straight");
    assert.equal(edgeRoute(28, 180), "smoothstep");
  });

  it("hides a column an earlier step dropped, and keeps one a later step still needs", () => {
    const steps = [
      { op: "trim", column: "customer_email" },
      { op: "drop_column", column: "order_id" },
      { op: "filter_rows", options: { condition: "[status] <> ''", keep: true } },
    ];
    const source = ["order_id", "customer_email", "order_amt", "order_date", "status"];
    assert.deepEqual(columnsBeforeStep(steps, 0, source), source);
    assert.deepEqual(columnsBeforeStep(steps, 2, source), ["customer_email", "order_amt", "order_date", "status"]);
    assert.equal(columnsBeforeStep(steps, 2, source).includes("status"), true);
    assert.equal(columnsBeforeStep(steps, 2, source).includes("order_id"), false);
    const dropped = [
      ...steps.slice(0, 2),
      { op: "filter_rows", options: { condition: "[order_id] <> ''", keep: true } },
    ];
    assert.deepEqual(stepExpressionIssues(steps, [trim, filter], source), []);
    assert.equal(stepExpressionIssues(dropped, [trim, filter], source)[0]?.index, 2);
    assert.match(stepExpressionIssues(dropped, [trim, filter], source)[0]?.message ?? "", /order_id/);
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
