/** QA C9 review: a destination recovery is confirmed with what it will do, naming the table. */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { recoveryConfirmOptions } from "./scheduleRecovery.js";

const sched = { name: "orders nightly", dest_table: "orders_copy" };

describe("recoveryConfirmOptions", () => {
  it("a keyless rebuild is a danger confirm that says it replaces the table", () => {
    const opts = recoveryConfirmOptions(
      { action: "rebuild", destructive: true, effect: "Replaces orders_copy: every row it holds now is dropped…" },
      sched,
    );
    assert.equal(opts.tone, "danger");
    assert.equal(opts.title, "Replace orders_copy?");
    assert.equal(opts.confirmLabel, "Replace and rebuild");
    assert.match(opts.message || "", /Replaces orders_copy/);
    assert.deepEqual(opts.facts?.find((f) => f.label === "Replaces the table")?.value, "Yes");
    assert.deepEqual(opts.facts?.find((f) => f.label === "Destination table")?.value, "orders_copy");
  });

  it("a keyed rebuild says it keeps rows and is not a danger confirm", () => {
    const opts = recoveryConfirmOptions(
      { action: "rebuild", destructive: false, effect: "Re-reads the full source and upserts every row into orders_copy on id" },
      sched,
    );
    assert.equal(opts.tone, "warning");
    assert.equal(opts.facts?.find((f) => f.label === "Replaces the table")?.value, "No");
  });

  it("a rebaseline of a removed table warns that only later rows come back", () => {
    const opts = recoveryConfirmOptions(
      { action: "rebaseline", destructive: false, destination_missing: true,
        effect: "Accepts that orders_copy was removed. The next run recreates orders_copy with rows after seq 7 only" },
      sched,
    );
    assert.equal(opts.tone, "warning");
    assert.equal(opts.title, "Accept orders_copy as it is?");
    assert.match(opts.message || "", /rows after seq 7 only/);
  });
});
