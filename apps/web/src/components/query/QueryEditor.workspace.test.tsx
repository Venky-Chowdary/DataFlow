/**
 * W10: in the Query workspace the action bar owns Run / Format / Explain, so
 * the editor renders a slim header (syntax, counters, Insert, Clear), tells
 * keyboard users how to leave the text area, and has no snippet row.
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { renderToStaticMarkup } from "react-dom/server";
import { QueryEditor } from "./QueryEditor.js";

const render = (variant?: "full" | "workspace") =>
  renderToStaticMarkup(
    <QueryEditor value="SELECT 1" onChange={() => {}} connectorType="postgresql" onRun={() => {}} onExplain={() => {}} variant={variant} fill={variant === "workspace"} />,
  );

describe("QueryEditor workspace variant", () => {
  it("drops the duplicate Run / Format / Explain and the snippet row", () => {
    const html = render("workspace");
    assert.doesNotMatch(html, /df2-query-editor-action--run/);
    assert.doesNotMatch(html, />Format</);
    assert.doesNotMatch(html, /df2-query-editor-snippets/);
    assert.match(html, /aria-label="Insert a snippet at the caret"/);
    assert.match(html, /data-fill="true"/);
  });

  it("describes the shortcuts and the Escape, Tab exit to assistive tech", () => {
    const html = render("workspace");
    assert.match(html, /aria-describedby="query-editor-keys"/);
    assert.match(html, /press Escape, then Tab, to leave the editor/);
  });

  it("the default editor keeps its own toolbar", () => {
    const html = render();
    assert.match(html, /df2-query-editor-action--run/);
    assert.match(html, /df2-query-editor-snippets/);
  });
});
