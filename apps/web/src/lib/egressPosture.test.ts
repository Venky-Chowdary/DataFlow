import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { connectorEgressState } from "./egressPosture.ts";

describe("connector egress card", () => {
  it("a saved allowlist is on for that workspace", () => {
    assert.equal(
      connectorEgressState("ws-1", { workspace_id: "ws-1", egress_allowlist: ["10.1.0.0/16"] }, { egress_allowlist_enabled: false }),
      "on",
    );
  });

  it("an empty allowlist is off", () => {
    assert.equal(
      connectorEgressState("ws-1", { workspace_id: "ws-1", egress_allowlist: [] }, { egress_allowlist_enabled: true }),
      "off",
    );
  });

  it("no tenant for this workspace is off", () => {
    assert.equal(connectorEgressState("ws-1", null, { egress_allowlist_enabled: false, scope_workspace_id: "ws-1" }), "off");
  });

  it("an unscoped posture is not painted as off while a workspace is selected", () => {
    assert.equal(
      connectorEgressState("ws-1", undefined, { egress_allowlist_enabled: false }),
      "unknown",
    );
  });

  it("another workspace's posture is not this card", () => {
    assert.equal(
      connectorEgressState("ws-1", undefined, { egress_allowlist_enabled: false, scope_workspace_id: "ws-2" }),
      "unknown",
    );
  });
});
