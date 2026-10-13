import assert from "node:assert/strict";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, it } from "node:test";
import { ServiceAccountList } from "./ServiceAccountList.js";
import type { ServiceAccount } from "../../lib/api.js";

describe("ServiceAccountList", () => {
  it("renders kind, expiry, and scopes without exposing secret or hash fields", () => {
    const account = {
      id: "svc-1",
      name: "Nightly import",
      prefix: "df_svc",
      kind: "service_account",
      role: "editor",
      scopes: ["job.run", "workspace.read"],
      expired: true,
      key: "secret-value-must-not-render",
      secret_hash: "hash-value-must-not-render",
    } as ServiceAccount;
    const markup = renderToStaticMarkup(createElement(ServiceAccountList, {
      accounts: [account],
      onRotate: () => {},
      onRevoke: () => {},
    }));
    assert.match(markup, /service account/);
    assert.match(markup, /Expired/);
    assert.match(markup, /job\.run, workspace\.read/);
    assert.match(markup, /df2-api-key-card-title/);
    assert.match(markup, /df2-api-key-card-meta-row/);
    assert.match(markup, /Prefix:/);
    assert.doesNotMatch(markup, /secret-value-must-not-render|hash-value-must-not-render/);
  });

  it("labels SCIM credentials separately", () => {
    const markup = renderToStaticMarkup(createElement(ServiceAccountList, {
      accounts: [{ id: "scim-1", name: "SCIM", prefix: "df_scim", kind: "scim" }],
      onRotate: () => {},
      onRevoke: () => {},
    }));
    assert.match(markup, />SCIM</);
  });
});
