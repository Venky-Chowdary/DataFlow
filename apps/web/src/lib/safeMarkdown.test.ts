import { describe, expect, it } from "vitest";

import { renderSafeMarkdown } from "./safeMarkdown";

describe("renderSafeMarkdown", () => {
  it("never lets model text become markup", () => {
    const html = renderSafeMarkdown('<img src=x onerror="alert(1)">');
    expect(html).not.toContain("<img");
    expect(html).toContain("&lt;img");
  });

  it("escapes html inside table cells and code blocks", () => {
    const table = renderSafeMarkdown("| a |\n| --- |\n| <script>x</script> |");
    expect(table).not.toContain("<script>");
    expect(table).toContain("&lt;script&gt;");

    const code = renderSafeMarkdown("```sql\nSELECT '<b>' FROM t\n```");
    expect(code).not.toContain("<b>");
    expect(code).toContain("&lt;b&gt;");
  });

  it("renders a grouped aggregate as a real table", () => {
    // The single most common Pilot answer: "count of orders by status".
    const html = renderSafeMarkdown(
      "Result:\n\n| status | c_0 |\n| --- | ---: |\n| open | 12 |\n| closed | 4 |",
    );
    expect(html).toContain("<table");
    expect(html).toContain("<th");
    expect(html).toContain("<td");
    expect(html).toContain("open");
    expect(html).toContain("12");
    // No leftover pipe characters from the source markup.
    expect(html).not.toContain("| status |");
  });

  it("honours column alignment so measures are not ragged-left", () => {
    const html = renderSafeMarkdown("| a | n |\n| :--- | ---: |\n| x | 1 |");
    expect(html).toContain('df2-md-right');
  });

  it("marks empty cells so NULL is not an ambiguous gap", () => {
    const html = renderSafeMarkdown("| a | b |\n| --- | --- |\n| x |  |");
    expect(html).toContain("df2-md-empty");
  });

  it("renders the citable query as a code block, not broken backticks", () => {
    const html = renderSafeMarkdown("Query:\n```sql\nSELECT count(*) FROM orders\n```");
    expect(html).toContain("<pre><code>");
    expect(html).toContain("SELECT count(*) FROM orders");
    expect(html).toContain("df2-md-code-lang");
    // The old renderer turned fence backticks into stray <code> tags.
    expect(html).not.toContain("<code>`");
  });

  it("does not apply inline marks inside code", () => {
    const html = renderSafeMarkdown("```\na ** b ** c\n```");
    expect(html).not.toContain("<strong>");
  });

  it("renders bullet lists including nested detail lines", () => {
    const html = renderSafeMarkdown("- one\n- two\n  – detail");
    expect(html).toContain("<ul");
    expect((html.match(/<li/g) || []).length).toBe(3);
    expect(html).toContain("df2-md-sub");
  });

  it("keeps bold and inline code in prose", () => {
    const html = renderSafeMarkdown("Moved **5** rows from `orders`");
    expect(html).toContain("<strong>5</strong>");
    expect(html).toContain("<code>orders</code>");
  });

  it("separates paragraphs on blank lines", () => {
    const html = renderSafeMarkdown("one\n\ntwo");
    expect((html.match(/<p>/g) || []).length).toBe(2);
  });

  it("treats a lone pipe line as prose, not a table", () => {
    const html = renderSafeMarkdown("a | b");
    expect(html).not.toContain("<table");
  });
});
