/** UX-18: Pilot's start tiles are answered from this workspace, never demo data. */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, it } from "node:test";
import { WORKSPACE_IDEAS } from "./automationIdeas.js";

describe("Pilot start tiles", () => {
  it("offer only prompts Pilot answers from live workspace state", () => {
    assert.deepEqual(WORKSPACE_IDEAS.map((i) => i.prompt), [
      "Give me a workspace briefing",
      "Show my connectors",
      "Show my recent jobs",
      "What can you do?",
    ]);
    for (const idea of WORKSPACE_IDEAS) {
      assert.doesNotMatch(`${idea.title} ${idea.prompt} ${idea.description}`, /logistics|HR data|payments|retail|airport/i);
    }
  });

  it("the page uses them, and its placeholder no longer suggests counting airports", () => {
    const page = readFileSync(join(dirname(fileURLToPath(import.meta.url)), "..", "pages", "PilotPage.tsx"), "utf8");
    assert.match(page, /const ideas = WORKSPACE_IDEAS;/);
    assert.doesNotMatch(page, /count airports/);
  });
});
