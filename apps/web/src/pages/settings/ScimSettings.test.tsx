import assert from "node:assert/strict";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, it } from "node:test";
import { ScimGroupMappingRows } from "./ScimGroupMappingRows.js";

describe("ScimGroupMappingRows", () => {
  it("renders inline validation errors", () => {
    const markup = renderToStaticMarkup(createElement(ScimGroupMappingRows, {
      rows: [{ group: "", workspace_id: "", role: "viewer" }],
      workspaces: [],
      error: "Enter an IdP group name.",
      onChange: () => {},
      onRemove: () => {},
    }));
    assert.match(markup, /role="alert"/);
    assert.match(markup, /Enter an IdP group name\./);
    assert.match(markup, /df2-scim-mapping-row/);
    assert.match(markup, /df2-scim-mapping-remove/);
    assert.match(markup, /IdP group name/);
  });
});
