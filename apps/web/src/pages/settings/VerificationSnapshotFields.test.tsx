import assert from "node:assert/strict";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, it } from "node:test";
import { snapshotModeOf, snapshotPayload } from "../../lib/verificationSnapshot.js";
import { VerificationSnapshotFields } from "./VerificationSnapshotFields.js";

describe("this workspace's verification snapshot (review 3, item 6)", () => {
  it("offers the deployment default, on and off, and a hold capped by the deployment", () => {
    const markup = renderToStaticMarkup(createElement(VerificationSnapshotFields, {
      mode: "off", minutes: "10", onMode: () => {}, onMinutes: () => {},
    }));
    assert.match(markup, /Consistent snapshot during verification/);
    assert.match(markup, /Deployment default/);
    assert.match(markup, /<option value="off" selected="">Off for this workspace<\/option>/);
    assert.match(markup, /Never longer than the deployment&#x27;s cap/);
    assert.match(markup, /value="10"/);
  });

  it("saves this workspace's choice, or null to follow the deployment", () => {
    assert.deepEqual(snapshotPayload("off", "10"), { verification_snapshot: false, verification_snapshot_max_minutes: 10 });
    assert.deepEqual(snapshotPayload("on", "5000"), { verification_snapshot: true, verification_snapshot_max_minutes: 1440 });
    assert.deepEqual(snapshotPayload("default", ""), { verification_snapshot: null, verification_snapshot_max_minutes: null });
    assert.equal(snapshotModeOf({ verification_snapshot: null }), "default");
    assert.equal(snapshotModeOf({ verification_snapshot: false }), "off");
  });
});
