import assert from "node:assert/strict";
import test from "node:test";

import { citedSources, pilotSourceHash, pilotSourceLabel } from "./pilotSources";

test("a section citation links the help article the router can resolve", () => {
  assert.equal(
    pilotSourceHash("#/help/quarantine-and-replay#what-is-quarantine"),
    "#/help/quarantine-and-replay",
  );
  assert.equal(pilotSourceHash("#/help/sync-modes"), "#/help/sync-modes");
  // Anything that is not a help article is not turned into a link.
  assert.equal(pilotSourceHash("https://example.com/docs"), undefined);
  assert.equal(pilotSourceHash(undefined), undefined);
});

test("citation label falls back to doc and section when no title is given", () => {
  assert.equal(pilotSourceLabel({ title: "Sync modes → Append" }), "Sync modes → Append");
  assert.equal(
    pilotSourceLabel({ doc: "Sync modes", section: "Append" }),
    "Sync modes › Append",
  );
  assert.equal(pilotSourceLabel({ href: "#/help/sync-modes" }), "");
});

test("unlabelled sources are dropped so no empty chip is rendered", () => {
  assert.deepEqual(citedSources(undefined), []);
  assert.deepEqual(
    citedSources([{ href: "#/help/sync-modes" }, { title: "Sync modes → Append" }]),
    [{ title: "Sync modes → Append" }],
  );
});
