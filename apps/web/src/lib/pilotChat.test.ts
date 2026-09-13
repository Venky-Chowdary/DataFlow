import assert from "node:assert/strict";
import test from "node:test";

import { pilotActionChipLabel, showsFollowUpPrompts } from "./pilotChat";

test("only the newest turn offers follow-up prompts", () => {
  const suggested = ["show my jobs"];
  // Last of three.
  assert.equal(showsFollowUpPrompts(2, 3, suggested), true);
  // Earlier turns keep their answer but drop the chips, so a thread does not
  // grow a column of stale suggestions that re-ask settled questions.
  assert.equal(showsFollowUpPrompts(0, 3, suggested), false);
  assert.equal(showsFollowUpPrompts(1, 3, suggested), false);
  // A single turn is also the newest one.
  assert.equal(showsFollowUpPrompts(0, 1, suggested), true);
});

test("no prompts means no chip row", () => {
  assert.equal(showsFollowUpPrompts(0, 1, undefined), false);
  assert.equal(showsFollowUpPrompts(0, 1, []), false);
});

test("an action chip falls back to the screen name it opens", () => {
  assert.equal(pilotActionChipLabel({ label: "Open Jobs" }), "Open Jobs");
  assert.equal(pilotActionChipLabel({ screen: "jobs" }), "Open Jobs");
  assert.equal(pilotActionChipLabel({}), "Action");
});
