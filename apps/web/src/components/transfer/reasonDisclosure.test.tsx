/**
 * The full Why text on Map must be reachable by keyboard and screen reader —
 * a title attribute is not enough (WCAG 2.1.1 / 1.4.13).
 * Run: npx --yes tsx --test apps/web/src/components/transfer/reasonDisclosure.test.tsx
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import React from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { ReasonDisclosure } from "./ReasonDisclosure.tsx";

const LONG = `Long reason ${"x".repeat(120)}`;
const SHORT = "Direct copy";

describe("ReasonDisclosure", () => {
  it("long text renders a toggle bound to the wrapper, closed by default", () => {
    const html = renderToStaticMarkup(
      <ReasonDisclosure id="reason-email" label="email" fullText={LONG}>
        {LONG}
      </ReasonDisclosure>,
    );
    assert.match(html, /aria-expanded="false"/);
    assert.match(html, /aria-controls="reason-email"/);
    assert.match(html, /aria-label="Show full reason for email"/);
    // The full text is in the DOM even while visually clamped.
    assert.ok(html.includes(LONG));
  });

  it("defaultOpen renders expanded with the Hide label", () => {
    const html = renderToStaticMarkup(
      <ReasonDisclosure id="reason-email" label="email" fullText={LONG} defaultOpen>
        {LONG}
      </ReasonDisclosure>,
    );
    assert.match(html, /aria-expanded="true"/);
    assert.match(html, /aria-label="Hide full reason for email"/);
    assert.match(html, /df2-column-reason-text[^"]*is-open|is-open[^"]*df2-column-reason-text/);
    assert.match(html, />Less</);
  });

  it("short text renders no toggle", () => {
    const html = renderToStaticMarkup(
      <ReasonDisclosure id="reason-email" label="email" fullText={SHORT}>
        {SHORT}
      </ReasonDisclosure>,
    );
    assert.ok(!html.includes("aria-expanded"), html);
    assert.ok(!html.includes(">More</"), html);
  });

  it("defaults to a useId id — aria-controls matches and holds no whitespace", () => {
    const html = renderToStaticMarkup(
      <ReasonDisclosure label="email" fullText={LONG}>
        {LONG}
      </ReasonDisclosure>,
    );
    const wrapperId = html.match(/id="([^"]+)"[^>]*class="[^"]*df2-column-reason-text/)?.[1]
      ?? html.match(/class="df2-column-reason-text[^"]*"[^>]*id="([^"]+)"/)?.[1]
      ?? html.match(/id="([^"]+)"/)?.[1];
    assert.ok(wrapperId, html);
    assert.doesNotMatch(wrapperId, /\s/, `id has whitespace: ${wrapperId}`);
    assert.ok(html.includes(`aria-controls="${wrapperId}"`), html);
  });

  it("Map clamps only the rule-text span — kind and provenance stay whole", () => {
    const panel = readFileSync(
      join(import.meta.dirname, "../ColumnReviewPanel.tsx"),
      "utf8",
    );
    // The business-rule text span carries the clamp class and data-clamp.
    assert.match(panel, /className="df2-column-rule-text" data-clamp/);
    // Kind and provenance spans are never clamped.
    assert.match(panel, /className="df2-column-rule-kind"(?!.*data-clamp)/);
    assert.match(panel, /className="df2-column-rule-prov"(?!.*data-clamp)/);
    // No caller-built id prop — ids come from useId inside the component.
    assert.doesNotMatch(panel, /<ReasonDisclosure\s+id=/);
  });
});
