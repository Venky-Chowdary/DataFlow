import assert from "node:assert/strict";
import { test } from "node:test";

import { ApiError } from "./api";
import { classifySignInFailure } from "./signInFailure";

test("a refused password is a sign-in failure, not an outage", () => {
  assert.equal(classifySignInFailure(new ApiError("Invalid email or password.", 401)), "auth");
  // Even when the reason mentions the deployment, an answer is not an outage.
  assert.equal(
    classifySignInFailure(new ApiError("Invalid email or password. Re-set the API password.", 401)),
    "auth",
  );
});

test("only a request that never got an answer is an unreachable control plane", () => {
  assert.equal(classifySignInFailure(new TypeError("Failed to fetch")), "api");
  assert.equal(classifySignInFailure(new Error("request timed out")), "api");
});

test("503 is a deployment that has no identities yet", () => {
  assert.equal(classifySignInFailure(new ApiError("No users configured", 503)), "config");
});

test("an unrecognised throw reads as a sign-in failure", () => {
  assert.equal(classifySignInFailure("something"), "auth");
  assert.equal(classifySignInFailure(new ApiError("Forbidden", 403)), "auth");
});
