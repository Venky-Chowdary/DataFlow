/**
 * Route-bar count formatting — avoid clipping 100,000 → "100".
 * Run: npx --yes tsx --test apps/web/src/lib/formatRouteRowCount.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { formatRouteRowCount } from "./formatRouteRowCount.js";

describe("formatRouteRowCount", () => {
  it("keeps small counts fully locale-formatted", () => {
    const r = formatRouteRowCount(100);
    assert.equal(r.short, "100 rows");
    assert.match(r.full, /100/);
  });

  it("compacts 100000 to 100k without truncating the k", () => {
    const r = formatRouteRowCount(100_000);
    assert.equal(r.short, "100k rows");
    assert.match(r.full, /100,?000/);
  });

  it("compacts 1.5M style counts", () => {
    const r = formatRouteRowCount(1_500_000);
    assert.equal(r.short, "1.5M rows");
  });
});
