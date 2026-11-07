/**
 * Auto-refresh must survive a failed read (R7: the schedules drawer stopped
 * refreshing after one failed request until the page was reloaded).
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { createPoller } from "./poller.js";

function fakeTimers() {
  const queue: Array<{ fn: () => void; ms: number }> = [];
  return {
    queue,
    setTimer: (fn: () => void, ms: number) => {
      const t = { fn, ms };
      queue.push(t);
      return t;
    },
    clearTimer: (h: unknown) => {
      const i = queue.indexOf(h as { fn: () => void; ms: number });
      if (i >= 0) queue.splice(i, 1);
    },
    async fire() {
      const t = queue.shift();
      assert.ok(t, "no read scheduled");
      t.fn();
      await new Promise((r) => setImmediate(r));
    },
  };
}

describe("createPoller", () => {
  it("keeps polling after a failed read", async () => {
    const timers = fakeTimers();
    const outcomes = [new Error("502"), 2, 3];
    const seen: number[] = [];
    let errors = 0;
    createPoller<number>({
      read: async () => {
        const next = outcomes.shift();
        if (next instanceof Error) throw next;
        return next as number;
      },
      delayMs: () => 30_000,
      onResult: (v) => seen.push(v),
      onError: () => errors++,
      setTimer: timers.setTimer,
      clearTimer: timers.clearTimer,
    });
    await timers.fire(); // fails
    assert.equal(errors, 1);
    assert.equal(timers.queue.length, 1, "a failed read must schedule the next one");
    await timers.fire();
    await timers.fire();
    assert.deepEqual(seen, [2, 3]);
  });

  it("stops when the result is final, and on stop()", async () => {
    const timers = fakeTimers();
    let reads = 0;
    const p = createPoller<string>({
      read: async () => (++reads >= 2 ? "succeeded" : "running"),
      delayMs: () => 1_000,
      onResult: () => {},
      isDone: (v) => v === "succeeded",
      setTimer: timers.setTimer,
      clearTimer: timers.clearTimer,
    });
    await timers.fire();
    await timers.fire();
    assert.equal(timers.queue.length, 0);
    p.stop();
    p.poke();
    assert.equal(timers.queue.length, 0);
  });

  it("poke reads now", async () => {
    const timers = fakeTimers();
    const p = createPoller<number>({
      read: async () => 1,
      delayMs: () => 30_000,
      onResult: () => {},
      setTimer: timers.setTimer,
      clearTimer: timers.clearTimer,
    });
    p.poke();
    assert.equal(timers.queue.length, 1);
    assert.equal(timers.queue[0].ms, 0);
  });
});
