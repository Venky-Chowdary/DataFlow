/**
 * The full Why text on Map must be reachable by keyboard and screen reader —
 * a title attribute is not enough (WCAG 2.1.1 / 1.4.13).
 * Run: npx --yes tsx --test apps/web/src/components/transfer/reasonDisclosure.test.tsx
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import React from "react";
import { renderToStaticMarkup } from "react-dom/server";
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
});
