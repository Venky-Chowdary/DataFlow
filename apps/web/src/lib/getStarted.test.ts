/** UX-20: "Get started" opened Sign in instead of Sign up. */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, it } from "node:test";
import { getStartedStage } from "./getStarted.js";

describe("Get started", () => {
  it("sends a stranger to Sign up when the deployment offers it", () => {
    assert.equal(getStartedStage(false, true), "signup");
  });
  it("falls back to Sign in only when there is no self-serve sign-up", () => {
    assert.equal(getStartedStage(false, false), "login");
  });
  it("takes a signed-in visitor straight to the app", () => {
    assert.equal(getStartedStage(true, true), "app");
  });
  it("the landing page's Get started uses it", () => {
    const app = readFileSync(join(dirname(fileURLToPath(import.meta.url)), "..", "DataTransferApp.tsx"), "utf8");
    assert.match(app, /onGetStarted=\{\(\) => void getStarted\(\)\}/);
    assert.doesNotMatch(app, /onGetStarted=\{\(\) => requestApp\("transfer"\)\}/);
  });
});
