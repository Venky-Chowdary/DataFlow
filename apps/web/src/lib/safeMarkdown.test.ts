/**
 * Run: npx --yes tsx --test apps/web/src/lib/safeMarkdown.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";

import { renderSafeMarkdown } from "./safeMarkdown.js";

describe("renderSafeMarkdown", () => {
  it("never lets model text become markup", () => {
    const html = renderSafeMarkdown('<img src=x onerror="alert(1)">');
    assert.ok(!html.includes("<img"));
    assert.ok(html.includes("&lt;img"));
  });

  it("escapes html inside table cells and code blocks", () => {
    const table = renderSafeMarkdown("| a |\n| --- |\n| <script>x</script> |");
    assert.ok(!table.includes("<script>"));
    assert.ok(table.includes("&lt;script&gt;"));

    const code = renderSafeMarkdown("```sql\nSELECT '<b>' FROM t\n```");
    assert.ok(!code.includes("<b>"));
    assert.ok(code.includes("&lt;b&gt;"));
  });

  it("renders a grouped aggregate as a real table", () => {
    // The single most common Pilot answer: "count of orders by status".
    const html = renderSafeMarkdown(
      "Result:\n\n| status | c_0 |\n| --- | ---: |\n| open | 12 |\n| closed | 4 |",
    );
    assert.ok(html.includes("<table"));
    assert.ok(html.includes("<th"));
    assert.ok(html.includes("<td"));
    assert.ok(html.includes("open"));
    assert.ok(html.includes("12"));
    // No leftover pipe characters from the source markup.
    assert.ok(!html.includes("| status |"));
  });

  it("honours column alignment so measures are not ragged-left", () => {
    const html = renderSafeMarkdown("| a | n |\n| :--- | ---: |\n| x | 1 |");
    assert.ok(html.includes("df2-md-right"));
  });

  it("marks empty cells so NULL is not an ambiguous gap", () => {
    const html = renderSafeMarkdown("| a | b |\n| --- | --- |\n| x |  |");
    assert.ok(html.includes("df2-md-empty"));
  });

  it("renders the citable query as a code block, not broken backticks", () => {
    const html = renderSafeMarkdown("Query:\n```sql\nSELECT count(*) FROM orders\n```");
    assert.ok(html.includes("<pre><code>"));
    assert.ok(html.includes("SELECT count(*) FROM orders"));
    assert.ok(html.includes("df2-md-code-lang"));
    // The old renderer turned fence backticks into stray <code> tags.
    assert.ok(!html.includes("<code>`"));
  });

  it("does not apply inline marks inside code", () => {
    const html = renderSafeMarkdown("```\na ** b ** c\n```");
    assert.ok(!html.includes("<strong>"));
  });

  it("renders bullet lists including nested detail lines", () => {
    const html = renderSafeMarkdown("- one\n- two\n  – detail");
    assert.ok(html.includes("<ul"));
    assert.equal((html.match(/<li/g) || []).length, 3);
    assert.ok(html.includes("df2-md-sub"));
  });

  it("keeps bold and inline code in prose", () => {
    const html = renderSafeMarkdown("Moved **5** rows from `orders`");
    assert.ok(html.includes("<strong>5</strong>"));
    assert.ok(html.includes("<code>orders</code>"));
  });

  it("separates paragraphs on blank lines", () => {
    const html = renderSafeMarkdown("one\n\ntwo");
    assert.equal((html.match(/<p>/g) || []).length, 2);
  });

  it("treats a lone pipe line as prose, not a table", () => {
    const html = renderSafeMarkdown("a | b");
    assert.ok(!html.includes("<table"));
  });
});
