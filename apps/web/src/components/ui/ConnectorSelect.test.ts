/** UX-40: the source picker truncated "Orders Postgres — postgre…". */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { connectorOptionLabel } from "./ConnectorSelect.js";

describe("connector option label", () => {
  it("leads with the name, then the engine's own name", () => {
    assert.equal(connectorOptionLabel({ name: "Orders", type: "postgresql", last_test_ok: true }), "Orders (PostgreSQL)");
  });
  it("adds a test warning only when there is one", () => {
    assert.equal(connectorOptionLabel({ name: "Wh", type: "mysql", last_test_ok: false }), "Wh (MySQL) · test failed");
    assert.equal(connectorOptionLabel({ name: "Wh", type: "mysql", last_test_ok: null }), "Wh (MySQL) · not tested");
  });
});
