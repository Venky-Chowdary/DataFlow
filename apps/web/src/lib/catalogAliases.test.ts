/**
 * Run: npx --yes tsx --test apps/web/src/lib/catalogAliases.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { collapseHostedAliasTiles } from "./catalogAliases.js";
import { getConnectorSetupGuide } from "./connectorSetupGuide.js";

describe("catalog aliases", () => {
  it("keeps one Snowflake tile for cloud and edition aliases", () => {
    const tiles = collapseHostedAliasTiles([
      { id: "excel", driver_type: "excel" },
      { id: "snowflake", driver_type: "snowflake" },
      { id: "snowflake_aws", driver_type: "snowflake", is_hosted_alias: true, alias_of: "snowflake" },
      { id: "snowflake_azure", driver_type: "snowflake", is_hosted_alias: true, alias_of: "snowflake" },
      { id: "snowflake_gcp", driver_type: "snowflake", is_hosted_alias: true, alias_of: "snowflake" },
      { id: "snowflake_standard", driver_type: "snowflake", is_hosted_alias: true, alias_of: "snowflake" },
      { id: "snowflake_enterprise", driver_type: "snowflake", is_hosted_alias: true, alias_of: "snowflake" },
      { id: "sftp", driver_type: "sftp" },
    ]);
    assert.deepEqual(
      tiles.map((t) => t.id),
      ["excel", "snowflake", "sftp"],
    );
  });
});

describe("setup guide", () => {
  it("tells operators to pick one Snowflake tile and copy the org-account", () => {
    const guide = getConnectorSetupGuide("snowflake");
    assert.match(guide.steps.join(" "), /org-account/i);
    assert.match(guide.steps.join(" "), /AWS, Azure, GCP/i);
    assert.match(guide.steps.join(" "), /250001/);
  });
});
