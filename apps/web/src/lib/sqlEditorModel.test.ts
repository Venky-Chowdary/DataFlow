/**
 * Run: npx --yes tsx --test src/lib/sqlEditorModel.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { highlightSql } from "./queryHighlight.ts";
import { extractBindParams } from "./sqlIntel.ts";
import { diagnoseSql } from "./sqlEditorModel.ts";

describe("sqlEditorModel", () => {
  it("highlights via queryHighlight SSOT (keywords, binds, comments)", () => {
    const html = highlightSql("CALL get_orders(:since) -- night\nSELECT 'ok'");
    assert.match(html, /qe-tok--keyword/);
    assert.match(html, /qe-tok--bind/);
    assert.match(html, /qe-tok--comment/);
    assert.deepEqual(extractBindParams("CALL get_orders(:since) -- night"), ["since"]);
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
