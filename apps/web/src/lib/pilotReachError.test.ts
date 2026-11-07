import assert from "node:assert/strict";
import { test } from "node:test";

import { formatPilotReachError } from "./api";

test("a Pilot rate limit is explained, not reported as a generic failure", () => {
  const msg = formatPilotReachError(new Error("429: Too many Pilot messages in a short time. Try again in 4 s."));
  assert.match(msg, /faster than Pilot accepts/);
});

test("a 500 never echoes the server's error text", () => {
  const msg = formatPilotReachError(new Error("500: password authentication failed for user admin"));
  assert.doesNotMatch(msg, /password/);
});
