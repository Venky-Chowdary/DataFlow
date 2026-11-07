/**
 * Run: npx --yes tsx --test apps/web/src/lib/sourceStreams.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { summarizeStreamReadFailures } from "./sourceStreams.js";

describe("summarizeStreamReadFailures", () => {
  it("states one shared connector error once", () => {
    const message = summarizeStreamReadFailures([
      { name: "customers", error: "connection refused" },
      { name: "orders", error: "connection refused" },
    ]);
    assert.match(message, /None of the 2 streams/);
    assert.match(message, /customers, orders/);
    assert.match(message, /Fix the connector, then retry/);
    assert.doesNotMatch(message, /connection refused/);
  });

  it("keeps distinct errors next to the stream they belong to", () => {
    const message = summarizeStreamReadFailures([
      { name: "customers", error: "permission denied" },
      { name: "orders", error: "relation does not exist" },
    ]);
    assert.match(message, /customers: permission denied/);
    assert.match(message, /orders: relation does not exist/);
  });
});
