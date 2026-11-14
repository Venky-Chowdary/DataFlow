/**
 * W3 round 2 / ERR-37: the source step titles a read failure from the API's
 * error code, so a source-read failure shows its specific title instead of the
 * generic "Couldn’t read the source".
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";

import { SourceStepAside, schemaErrorTitle } from "./SourceStepAside";

function render(message: string, code?: string) {
  return renderToStaticMarkup(
    createElement(SourceStepAside, {
      sourceKind: "database",
      parsed: null,
      samplePreviewRows: [],
      sourceConnector: undefined,
      sourceColumns: [],
      sourceSchema: {},
      cloudPath: "",
      dbConnectors: [],
      cloudConnectors: [],
      sourceIntrospectError: message,
      sourceIntrospectErrorCode: code,
    }),
  );
}

describe("source step error title", () => {
  it("renders the specific title for a source table the role cannot read", () => {
    const html = render(
      "Datawrap couldn't read table \"orders\" on the source: the connection's database role doesn't have SELECT on it. Reference: 8f2c4d1e9a7b4c3d",
      "access.source_read_denied",
    );
    assert.match(html, /No permission to read this table/);
    assert.doesNotMatch(html, /Couldn’t read the source/);
    // The reference is its own chip, not part of the paragraph.
    assert.match(html, /8f2c4d1e9a7b4c3d/);
  });

  it("names a missing table, and a failed sign-in by its family", () => {
    assert.match(render("Table \"orders\" does not exist on the source.", "object.table_not_found"),
      /The table is not on this source/);
    assert.equal(schemaErrorTitle("auth.password_rejected"), "Sign-in rejected");
    assert.equal(schemaErrorTitle("network.refused"), "Can't reach the database");
  });

  it("does not guess from the text when no code came back", () => {
    assert.match(render("relation \"orders\" does not exist"), /Couldn’t read the source/);
  });
});
