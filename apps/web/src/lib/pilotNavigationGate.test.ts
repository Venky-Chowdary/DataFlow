import assert from "node:assert/strict";
import test from "node:test";

import { applyPilotSafeActions, isNavigableScreen } from "./pilotChat";
import type { Screen } from "./types";

function record() {
  const seen: Screen[] = [];
  return { seen, onNavigate: (s: Screen) => seen.push(s) };
}

test("an explained answer never navigates the operator away from it", () => {
  const { seen, onNavigate } = record();
  applyPilotSafeActions(
    [{ type: "navigate", screen: "help", label: "Open Transfer Studio guide" }],
    onNavigate,
    [{ name: "explain_product", success: true }],
  );
  assert.deepEqual(seen, []);
});

test("navigation runs only when the turn actually ran the navigate tool", () => {
  const { seen, onNavigate } = record();
  applyPilotSafeActions([{ type: "navigate", screen: "jobs" }], onNavigate, [
    { name: "navigate", success: true },
  ]);
  assert.deepEqual(seen, ["jobs"]);
});

test("a failed navigate tool does not move the operator", () => {
  const { seen, onNavigate } = record();
  applyPilotSafeActions([{ type: "navigate", screen: "jobs" }], onNavigate, [
    { name: "navigate", success: false },
  ]);
  assert.deepEqual(seen, []);
});

test("only one navigation happens per turn, and never to a non-screen", () => {
  const { seen, onNavigate } = record();
  applyPilotSafeActions(
    [
      { type: "navigate", screen: "help" },
      { type: "navigate", screen: "jobs" },
      { type: "navigate", screen: "connectors" },
    ],
    onNavigate,
    [{ name: "navigate", success: true }],
  );
  assert.deepEqual(seen, ["jobs"]);
});

test("mutating and studio suggestions are never auto-run", () => {
  const { seen, onNavigate } = record();
  applyPilotSafeActions(
    [
      { type: "navigate", screen: "jobs", risk: "mutate" },
      { type: "studio", screen: "transfer" },
    ],
    onNavigate,
    [{ name: "navigate", success: true }],
  );
  assert.deepEqual(seen, []);
});

test("public marketing and unknown targets are not navigable screens", () => {
  assert.equal(isNavigableScreen("landing"), false);
  assert.equal(isNavigableScreen("help"), false);
  assert.equal(isNavigableScreen(undefined), false);
  assert.equal(isNavigableScreen("docs"), true);
  assert.equal(isNavigableScreen("jobs"), true);
});
