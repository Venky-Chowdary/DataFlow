/**
 * Run: npx --yes tsx --test apps/web/src/lib/workspaceHydrate.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";

import {
  WORKSPACE_HYDRATE_FALLBACK_MS,
  isStaleGeneration,
  shouldStartWorkspaceHydrate,
} from "./workspaceHydrate.js";

describe("workspace list hydrate", () => {
  it("does not start the first read before a workspace is named", () => {
    assert.equal(shouldStartWorkspaceHydrate("", 0), false);
    assert.equal(shouldStartWorkspaceHydrate("   ", 400), false);
  });

  it("starts as soon as the browser names a workspace", () => {
    assert.equal(shouldStartWorkspaceHydrate("ws-ops", 0), true);
  });

  it("falls back so a tenant with no workspace still hydrates", () => {
    assert.equal(shouldStartWorkspaceHydrate("", WORKSPACE_HYDRATE_FALLBACK_MS), true);
    assert.equal(shouldStartWorkspaceHydrate("", WORKSPACE_HYDRATE_FALLBACK_MS - 1), false);
  });

  it("drops an unscoped response that finishes after a scoped reload", () => {
    assert.equal(isStaleGeneration(1, 2), true);
    assert.equal(isStaleGeneration(2, 2), false);
  });
});
