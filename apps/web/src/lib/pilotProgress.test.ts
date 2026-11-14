/**
 * UX-42: Pilot took about 5 s per answer and showed only "Looking that up…".
 * It now says what it is doing from the first moment ("Reading your
 * connections…") and moves through the steps that question takes.
 */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, it } from "node:test";
import { PILOT_PROGRESS_STEP_MS, pilotProgressLabel, pilotProgressSteps } from "./pilotProgress.js";

const src = join(dirname(fileURLToPath(import.meta.url)), "..");

describe("pilot progress", () => {
  it("every question starts by reading the connections", () => {
    for (const q of ["Copy orders from PG to MySQL", "How many rows are in customers?", "What is Datawrap?", ""]) {
      assert.equal(pilotProgressSteps(q)[0], "Reading your connections…");
    }
  });
  it("a transfer question names the plan and preflight steps", () => {
    assert.deepEqual(pilotProgressSteps("Copy ux_audit.orders from Orders Postgres to Warehouse MySQL"), [
      "Reading your connections…",
      "Reading the source and destination tables…",
      "Running preflight checks…",
      "Writing up the plan…",
    ]);
  });
  it("a data question names the query step", () => {
    assert.deepEqual(pilotProgressSteps("How many rows are in ux_audit.customers?"), [
      "Reading your connections…",
      "Querying the table…",
      "Writing up the answer…",
    ]);
  });
  it("advances with time and holds on the last step", () => {
    const q = "Copy orders from A to B";
    assert.equal(pilotProgressLabel(q, 0), "Reading your connections…");
    assert.equal(pilotProgressLabel(q, PILOT_PROGRESS_STEP_MS), "Reading the source and destination tables…");
    assert.equal(pilotProgressLabel(q, PILOT_PROGRESS_STEP_MS * 50), "Writing up the plan…");
  });
  it("both Pilot surfaces show it, announced politely, instead of 'Looking that up…'", () => {
    for (const file of ["pages/PilotPage.tsx", "components/AICopilot.tsx"]) {
      const text = readFileSync(join(src, file), "utf8");
      assert.doesNotMatch(text, /Looking that up…/, file);
      assert.match(text, /usePilotProgress\(/, file);
      assert.match(text, /aria-live="polite"/, file);
    }
  });
});
