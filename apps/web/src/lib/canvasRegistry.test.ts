import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  FALLBACK_CANVAS_NODES,
  iconFor,
  plannedSpecs,
  registryFromCatalog,
  runnableSpecs,
  setDocumentHash,
  specsByFamily,
  statusLabel,
  stepPorts,
} from "./canvasRegistry.js";
import type { ShapeCatalog, ShapeOperation } from "./shape.js";

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
  options: ["condition"],
  required: ["condition"],
  expression_option: "condition",
};

function catalog(extra?: Partial<ShapeCatalog>): ShapeCatalog {
  return {
    operations: [trim, filter],
    functions: [],
    max_steps: 40,
    max_preview_rows: 12,
    error_policies: [],
    post_load_only: { operations: ["join"], reason: "Needs the whole population." },
    ...extra,
  };
}

describe("canvas node registry", () => {
  it("gives every requested operation its own icon and keeps set nodes disabled", () => {
    const specs = registryFromCatalog(catalog());
    const wanted = ["source", "target", "filter_rows", "derive_column", "cast_column", "trim", "mask", "hash_identity", "dedupe", "split_column", "unnest_json", "sql", "join", "lookup", "aggregate", "sort", "rank", "window", "router", "union", "normalizer", "mapplet"];
    const pictures = wanted.map((op) => iconFor(op));
    assert.equal(new Set(pictures).size, pictures.length);
    const join = specs.find((spec) => spec.op === "join");
    assert.ok(join);
    assert.equal(join?.executable, false);
    assert.equal(join?.icon, "join");
    assert.equal(join?.ports.filter((port) => port.role === "in" && port.kind === "row").length, 2);
    assert.ok(join?.ports.some((port) => port.kind === "field" && port.visible === false));
    assert.equal(runnableSpecs(specs).some((spec) => spec.op === "join"), false);
    assert.equal(plannedSpecs(specs).some((spec) => spec.op === "join"), true);
    assert.equal(specs.find((spec) => spec.op === "source")?.placement, "wizard");
    assert.equal(runnableSpecs(specs).map((spec) => spec.op).sort().join(), "filter_rows,trim");
  });

  it("promotes a set node when the catalog marks it executable and hides empty families", () => {
    const specs = registryFromCatalog(catalog({ set_operations: ["join"] }));
    const join = specs.find((spec) => spec.op === "join");
    assert.equal(join?.executable, true);
    const fromEngine = registryFromCatalog(catalog({
      set_operations: [{
        op: "join",
        summary: "Join",
        executable: true,
        execution: [],
        family: "set",
        inputs: [],
        outputs: [],
        options: [],
        required: [],
      }],
    }));
    assert.equal(fromEngine.find((spec) => spec.op === "join")?.executable, true);
    const refused = registryFromCatalog(catalog({
      set_operations: [{
        op: "join",
        summary: "Join",
        executable: false,
        execution: [],
        family: "set",
        inputs: [],
        outputs: [],
        options: [],
        required: [],
      }],
    }));
    assert.equal(refused.find((spec) => spec.op === "join")?.executable, false);
    const found = runnableSpecs(specs, "join");
    assert.equal(found.length, 1);
    assert.equal(specsByFamily(found).map((group) => group.family).join(), "set");
    assert.equal(runnableSpecs(specs, "no-such-step").length, 0);
  });

  it("matches the API set_hash for the same document", () => {
    assert.equal(setDocumentHash(null), "");
    assert.equal(setDocumentHash({ nodes: [] }), "");
    const hash = setDocumentHash({
      nodes: [{ op: "Join", id: "join_orders", on: "id" }],
      edges: [{ target: "b", source: "a", id: "e1" }],
    });
    assert.equal(hash, "a062324f0e6f5ef0");
    const again = setDocumentHash({
      edges: [{ id: "e1", source: "a", target: "b" }],
      nodes: [{ id: "join_orders", on: "id", op: "join" }],
    });
    assert.equal(again, hash);
    assert.equal(FALLBACK_CANVAS_NODES.some((node) => node.op === "router"), true);
  });

  it("gives Divert a kept port and a diverted port", () => {
    const divert = stepPorts("divert_rows").filter((port) => port.kind === "row");
    assert.deepEqual(divert.map((port) => port.label), ["Input", "Kept", "Diverted"]);
    const stream = stepPorts("trim").filter((port) => port.kind === "row");
    assert.deepEqual(stream.map((port) => port.label), ["Input", "Output"]);
  });

  it("names a failing node Invalid and a clean node Valid", () => {
    assert.equal(statusLabel("valid"), "Valid");
    assert.equal(statusLabel("error"), "Invalid");
    assert.equal(statusLabel("warning"), "Warning");
    assert.equal(statusLabel("running"), "Running");
  });
});
