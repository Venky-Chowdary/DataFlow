import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  composeFilterCondition,
  composeFilterGroup,
  parseFilterCondition,
  parseFilterGroup,
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

describe("filter AND/OR group builder", () => {
  it("round-trips a flat AND chain and a flat OR chain", () => {
    const andExpr = "[status] <> 'void' and [region] = 'east'";
    const group = parseFilterGroup(andExpr);
    assert.deepEqual(group, {
      combinator: "and",
      conditions: [
        { field: "status", operator: "<>", value: "void" },
        { field: "region", operator: "=", value: "east" },
      ],
    });
    assert.equal(composeFilterGroup(group!), andExpr);

    const orExpr = "contains([email], '@acme') or is_null([email])";
    const orGroup = parseFilterGroup(orExpr);
    assert.equal(orGroup?.combinator, "or");
    assert.equal(orGroup?.conditions.length, 2);
    assert.equal(composeFilterGroup(orGroup!), orExpr);
  });

  it("keeps mixed AND/OR and nested expressions in advanced mode", () => {
    assert.equal(parseFilterGroup("[a] = '1' and [b] = '2' or [c] = '3'"), null);
    assert.equal(parseFilterGroup("([a] = '1' or [b] = '2') and [c] = '3'"), null);
    assert.equal(parseFilterGroup("length([name]) > 3"), null);
  });

  it("keeps the keyword inside a quoted value intact", () => {
    const expr = "[note] = 'rock and roll' and [state] = 'CA'";
    const group = parseFilterGroup(expr);
    assert.equal(group?.conditions.length, 2);
    assert.equal(group?.conditions[0].value, "rock and roll");
    assert.equal(composeFilterGroup(group!), expr);
  });
});
