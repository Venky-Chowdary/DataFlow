import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  composeFilterCondition,
  parseFilterCondition,
} from "./filterCondition.js";

describe("filter condition builder", () => {
  it("round-trips a comparison, a blank check, and contains", () => {
    const comparison = { field: "status", operator: "<>", value: "" };
    assert.equal(composeFilterCondition(comparison), "[status] <> ''");
    assert.deepEqual(parseFilterCondition("[status] <> ''"), comparison);
    assert.equal(composeFilterCondition({ field: "status", operator: "is_null", value: "" }), "is_null([status])");
    assert.deepEqual(parseFilterCondition("is_null([status])"), { field: "status", operator: "is_null", value: "" });
    assert.equal(
      composeFilterCondition({ field: "status", operator: "contains", value: "void" }),
      "contains([status], 'void')",
    );
    assert.deepEqual(parseFilterCondition("not is_null([order_id])"), {
      field: "order_id",
      operator: "not_null",
      value: "",
    });
  });

  it("keeps a compound expression in advanced mode", () => {
    assert.equal(parseFilterCondition("[status] <> '' and [region] = 'east'"), null);
    assert.deepEqual(parseFilterCondition(""), { field: "", operator: "<>", value: "" });
    assert.equal(composeFilterCondition({ field: "", operator: "<>", value: "" }), "");
  });
});
