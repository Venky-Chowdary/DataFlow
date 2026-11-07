/**
 * Run: npx --yes tsx --test apps/web/src/lib/visibleRefresh.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { readFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

describe("visible refresh helper", () => {
  it("skips hidden documents and wakes on focus/visibility", () => {
    const src = readFileSync(
      path.resolve(path.dirname(fileURLToPath(import.meta.url)), "visibleRefresh.ts"),
      "utf8",
    );
    assert.match(src, /document\.hidden/);
    assert.match(src, /visibilitychange/);
    assert.match(src, /window\.addEventListener\("focus"/);
  });
});
