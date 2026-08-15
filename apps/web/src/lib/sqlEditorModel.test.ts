/**
 * Run: npx --yes tsx --test src/lib/sqlEditorModel.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { bindNamesFromTokens, diagnoseSql, tokenizeSql } from "./sqlEditorModel.ts";

describe("sqlEditorModel", () => {
  it("colors keywords, binds, strings, and comments", () => {
    const tokens = tokenizeSql("CALL get_orders(:since) -- night\n-- x\nSELECT 'ok'");
    const kinds = tokens.filter((t) => t.kind !== "ws").map((t) => t.kind);
    assert.ok(kinds.includes("keyword"));
    assert.ok(kinds.includes("bind"));
    assert.ok(kinds.includes("comment"));
    assert.deepEqual(bindNamesFromTokens(tokens), ["since"]);
  });

  it("refuses stacked statements and DML in query mode", () => {
    const stacked = diagnoseSql("SELECT 1; DROP TABLE t", { mode: "query" });
    assert.equal(stacked.ok, false);
    const dml = diagnoseSql("INSERT INTO t SELECT 1", { mode: "query" });
    assert.equal(dml.ok, false);
    const ok = diagnoseSql("SELECT id FROM customers WHERE ts > :since", {
      mode: "query",
      bound: { since: "2024-01-01" },
    });
    assert.equal(ok.ok, true);
    assert.equal(ok.statement, "SELECT");
  });

  it("procedure mode wants CALL and surfaces unbound binds", () => {
    const queryAsProc = diagnoseSql("SELECT id FROM t", { mode: "procedure", dialect: "mysql" });
    assert.equal(queryAsProc.ok, false);
    const unbound = diagnoseSql("CALL get_orders(:since)", { mode: "procedure", dialect: "mysql" });
    assert.equal(unbound.ok, false);
    assert.match(unbound.error, /since/);
    const bound = diagnoseSql("CALL get_orders(:since)", {
      mode: "procedure",
      dialect: "mysql",
      bound: { since: "2024-01-01" },
    });
    assert.equal(bound.ok, true);
  });
});
