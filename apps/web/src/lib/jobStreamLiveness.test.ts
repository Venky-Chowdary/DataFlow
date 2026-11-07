import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { describe, it } from "node:test";
import {
  STREAM_CATCHUP_SILENCE_MS,
  streamDropNotice,
  streamSilenceNeedsCatchUp,
} from "./jobStreamLiveness.js";

describe("job stream catch-up", () => {
  it("a quiet stream reads the job document instead of keeping the last frame", () => {
    const opened = 1_000;
    assert.equal(streamSilenceNeedsCatchUp(opened, opened + STREAM_CATCHUP_SILENCE_MS - 1), false);
    assert.equal(streamSilenceNeedsCatchUp(opened, opened + STREAM_CATCHUP_SILENCE_MS), true);
  });

  it("a dropped socket does not call the transfer failed", () => {
    const theater = readFileSync(new URL("../components/JobTheater.tsx", import.meta.url), "utf8");
    const api = readFileSync(new URL("./api.ts", import.meta.url), "utf8");
    assert.equal(streamDropNotice().includes("failed"), false);
    assert.match(theater, /streamDropNotice\(\)/);
    assert.equal(theater.includes('status: "failed"'), false);
    assert.match(api, /streamSilenceNeedsCatchUp\(/);
  });
});
