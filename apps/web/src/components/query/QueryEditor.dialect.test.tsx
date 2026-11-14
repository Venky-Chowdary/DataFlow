/**
 * W6 round 2, item 4: when the connector fixes the SQL dialect, the editor
 * shows it as a read-only label, not a full-width select that repeats the
 * connector. A document connector (MongoDB: filter JSON or shell JS) and no
 * connector keep the choice.
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { renderToStaticMarkup } from "react-dom/server";
import { QueryEditor } from "./QueryEditor.js";

const render = (connectorType?: string) =>
  renderToStaticMarkup(<QueryEditor value="SELECT 1" onChange={() => {}} connectorType={connectorType} />);

describe("query language follows the connector", () => {
  it("PostgreSQL fixes the dialect: a read-only label, no select", () => {
    const html = render("postgresql");
    assert.doesNotMatch(html, /<select[^>]*aria-label="Query language"/);
    assert.match(html, /class="df2-query-editor-lang-fixed"[^>]*>PostgreSQL</);
    // The hint does not repeat the dialect name next to the label.
    assert.doesNotMatch(html, /PostgreSQL · SELECT/);
  });

  it("MongoDB keeps the JSON / shell choice, and no connector keeps every dialect", () => {
    assert.match(render("mongodb"), /<select[^>]*aria-label="Query language"/);
    assert.match(render(undefined), /<select[^>]*aria-label="Query language"/);
  });
});
