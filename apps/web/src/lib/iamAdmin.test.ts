import assert from "node:assert/strict";
import { describe, it } from "node:test";

import {
  API_KEY_LIFETIME_OPTIONS,
  auditExportFilename,
  buildScimGroupMappingsPayload,
  filterWorkspaceApiKeys,
  scopeOptions,
} from "./iamAdmin.js";

describe("IAM admin helpers", () => {
  it("returns sorted unique actor scopes", () => {
    assert.deepEqual(
      scopeOptions(["workspace.manage", "audit.read", "workspace.manage", " "]),
      ["audit.read", "workspace.manage"],
    );
  });

  it("trims SCIM mapping fields and builds the API payload", () => {
    assert.deepEqual(
      buildScimGroupMappingsPayload([
        { group: " Finance ", workspace_id: " ws-1 ", role: " admin " },
      ]),
      { ok: true, mappings: { Finance: { workspace_id: "ws-1", role: "admin" } } },
    );
  });

  it("rejects blank groups and case-insensitive duplicates", () => {
    assert.equal(buildScimGroupMappingsPayload([{ group: " ", workspace_id: "ws", role: "viewer" }]).ok, false);
    assert.equal(
      buildScimGroupMappingsPayload([
        { group: "Finance", workspace_id: "ws-1", role: "viewer" },
        { group: " finance ", workspace_id: "ws-2", role: "editor" },
      ]).ok,
      false,
    );
  });

  it("rejects missing workspace or role and roles the endpoint does not support", () => {
    assert.equal(buildScimGroupMappingsPayload([{ group: "Finance", workspace_id: " ", role: "viewer" }]).ok, false);
    assert.equal(buildScimGroupMappingsPayload([{ group: "Finance", workspace_id: "ws", role: " " }]).ok, false);
    assert.equal(buildScimGroupMappingsPayload([{ group: "Finance", workspace_id: "ws", role: "operator" }]).ok, false);
  });

  it("uses one canonical API-key lifetime list", () => {
    assert.deepEqual(API_KEY_LIFETIME_OPTIONS.map(({ value }) => value), [
      "7d", "30d", "60d", "90d", "365d", "never",
    ]);
  });

  it("excludes IAM service-account and SCIM keys from the workspace API-key list", () => {
    assert.deepEqual(
      filterWorkspaceApiKeys([
        { id: "api-1", name: "Workspace key", prefix: "dfk_api", kind: "api_key" },
        { id: "service-1", name: "Service account", prefix: "dfk_svc", kind: "service_account" },
        { id: "scim-1", name: "SCIM token", prefix: "dfk_scim", kind: "scim" },
        { id: "legacy-1", name: "Legacy key", prefix: "dfk_old" },
      ]).map(({ id }) => id),
      ["api-1", "legacy-1"],
    );
  });

  it("keeps the existing audit filename prefix with an unambiguous timestamp", () => {
    assert.equal(
      auditExportFilename("ndjson", new Date("2026-02-03T04:05:06.789Z")),
      "datawrap-audit-20260203-040506.ndjson",
    );
  });
});
