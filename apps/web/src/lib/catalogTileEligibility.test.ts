/**
 * Run: npx --yes tsx --test apps/web/src/lib/catalogTileEligibility.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { catalogTileBlocked, catalogTileSelectable } from "./catalogTileEligibility.js";

describe("catalog tile eligibility", () => {
  it("does not open Planned tiles that still ship status=live or beta", () => {
    const plannedLive = {
      certification_tier: "planned",
      effective_status: "planned",
      status: "live",
      transfer_ready: false,
    };
    const betaStub = {
      certification_tier: "planned",
      effective_status: "planned",
      status: "beta",
      transfer_ready: false,
    };
    assert.equal(catalogTileSelectable(plannedLive), false);
    assert.equal(catalogTileSelectable(betaStub), false);
    assert.equal(catalogTileBlocked(plannedLive), true);
  });

  it("opens Certified, source-only, connect-only, and environment-gap tiles on the marketplace", () => {
    assert.equal(catalogTileSelectable({ transfer_ready: true, certification_tier: "certified" }), true);
    assert.equal(catalogTileSelectable({ certification_tier: "source_only", source_ready: true }), true);
    assert.equal(catalogTileSelectable({ connect_only: true, certification_tier: "connect_only" }), true);
    assert.equal(
      catalogTileSelectable({
        certification_tier: "certified",
        transfer_ready: false,
        environment_gap: true,
      }),
      true,
    );
  });

  it("transfer pickers require a real side — env-gap Certified is not transfer-ready", () => {
    const envGap = {
      certification_tier: "certified",
      transfer_ready: false,
      environment_gap: true,
    };
    assert.equal(catalogTileSelectable(envGap, true), false);
    assert.equal(
      catalogTileSelectable({ transfer_ready: false, connect_only: true }, true),
      true,
    );
  });
});
