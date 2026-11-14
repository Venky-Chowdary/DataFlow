/**
 * UX-39: Destination showed "Route ready" before any destination was chosen.
 * UX-34: the completion toast said "Gate-8 proof stays on Job Theater".
 */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, it } from "node:test";

const src = join(dirname(fileURLToPath(import.meta.url)), "..");

describe("Studio copy", () => {
  it("the route callout waits for a database destination to be chosen", () => {
    const studio = readFileSync(join(src, "pages", "TransferPage.tsx"), "utf8");
    assert.match(studio, /\{transferPlan && \(destKindMode !== "database" \|\| Boolean\(connectorId\)\) && \(\s*<div className=\{`df2-plan-callout/);
  });

  it("the completion toast names where the proof is, without internal names", () => {
    const app = readFileSync(join(src, "DataTransferApp.tsx"), "utf8");
    assert.doesNotMatch(app, /Gate-8 proof stays on Job Theater/);
    assert.match(app, /The Trust Report and post-write proof are on the job in Jobs\./);
  });
});
