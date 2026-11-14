/** UX-40: the source picker truncated "Orders Postgres — postgre…". */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { connectorOptionLabel } from "./ConnectorSelect.js";
import { connectorNameWithEngine } from "../../lib/connectorTypes.js";

describe("connector option label", () => {
  it("leads with the name, then the engine's own name", () => {
    assert.equal(connectorOptionLabel({ name: "Orders", type: "postgresql", last_test_ok: true }), "Orders (PostgreSQL)");
  });
  it("adds a test warning only when there is one", () => {
    assert.equal(connectorOptionLabel({ name: "Wh", type: "mysql", last_test_ok: false }), "Wh (MySQL) · test failed");
    assert.equal(connectorOptionLabel({ name: "Wh", type: "mysql", last_test_ok: null }), "Wh (MySQL) · not tested");
  });
  it("does not repeat the engine the name already says (case-insensitive)", () => {
    assert.equal(
      connectorOptionLabel({ name: "Warehouse (SQLite)", type: "sqlite", last_test_ok: null }),
      "Warehouse (SQLite) · not tested",
    );
    assert.equal(connectorOptionLabel({ name: "orders postgresql replica", type: "postgresql", last_test_ok: true }), "orders postgresql replica");
    assert.equal(connectorOptionLabel({ name: "Finance mart", type: "sqlite", last_test_ok: true }), "Finance mart (SQLite)");
  });
});

describe("connectorNameWithEngine", () => {
  it("appends the engine in the picker's own style unless the name has it", () => {
    assert.equal(connectorNameWithEngine("Orders", "postgresql", "dot"), "Orders · PostgreSQL");
    assert.equal(connectorNameWithEngine("Snowflake QA PAT", "snowflake", "dot"), "Snowflake QA PAT");
    assert.equal(connectorNameWithEngine("", "mysql"), "MySQL");
  });
});
