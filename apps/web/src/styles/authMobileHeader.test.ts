/**
 * Regression for 60789b3f (loop review 8, item 8): at <=960px the brand
 * panel must collapse to a compact header ABOVE the card — Back stays
 * top-left — instead of `order: 2` stacking it below the form. Asserts the
 * stylesheet contract plus the DOM shape the CSS relies on.
 */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, it } from "node:test";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";

import { AuthBrandPanel } from "../components/AuthBrandPanel";
import { ForgotPasswordPage } from "../pages/SignupPage";

const STYLES_DIR = dirname(fileURLToPath(import.meta.url));
const CSS = readFileSync(join(STYLES_DIR, "marketing-editorial.css"), "utf8");

/** Top-level @media blocks, as {condition, body} pairs. */
function mediaBlocks(css: string): { condition: string; body: string }[] {
  const blocks: { condition: string; body: string }[] = [];
  const re = /@media([^{]+)\{/g;
  let match: RegExpExecArray | null;
  while ((match = re.exec(css))) {
    let depth = 1;
    let i = re.lastIndex;
    while (i < css.length && depth > 0) {
      if (css[i] === "{") depth += 1;
      else if (css[i] === "}") depth -= 1;
      i += 1;
    }
    blocks.push({ condition: match[1].trim(), body: css.slice(re.lastIndex, i - 1) });
    re.lastIndex = i;
  }
  return blocks;
}

function ruleBody(body: string, selector: string): string | null {
  const i = body.indexOf(`${selector} {`);
  if (i === -1) return null;
  const end = body.indexOf("}", i);
  return end === -1 ? null : body.slice(i + selector.length + 1, end);
}

describe("mobile auth header (<=960px)", () => {
  const narrow = mediaBlocks(CSS).filter((b) => /max-width:\s*960px/.test(b.condition));

  it("keeps the brand panel above the card (order: 0, not 2)", () => {
    const body = narrow.find((b) => b.body.includes(".lp-login-brand"))?.body;
    assert.ok(body, "no <=960px block styles .lp-login-brand");
    const rule = ruleBody(body, ".lp-login-brand");
    assert.ok(rule);
    assert.match(rule, /order:\s*0/);
    assert.doesNotMatch(rule, /order:\s*2/);
  });

  it("collapses the heavy copy (lead/proof/metrics/dest/foot hidden)", () => {
    for (const sel of [".lp-login-brand-kicker", ".lp-login-brand-lead",
                       ".lp-login-proof", ".lp-login-metrics",
                       ".lp-login-brand-dest", ".lp-login-brand-foot"]) {
      const esc = sel.replace(".", "\\.");
      const hidden = narrow.some((b) =>
        new RegExp(`${esc}\\b[^;{}]*\\{[^}]*display:\\s*none`).test(b.body));
      assert.ok(hidden, `${sel} is not hidden at <=960px`);
    }
  });

  it("renders Back inside the brand header, before the card, in DOM order", () => {
    const markup = renderToStaticMarkup(
      createElement(AuthBrandPanel, { title: "T", lead: "L", onBack: () => {} }),
    );
    const head = markup.indexOf("lp-login-brand-head");
    const back = markup.indexOf("lp-login-back");
    assert.ok(head > -1 && back > -1, "Back control missing");
    assert.ok(back > head, "Back must live inside .lp-login-brand-head");
    // The brand section precedes the auth card in the full page.
    const page = renderToStaticMarkup(
      createElement(ForgotPasswordPage, { onSignIn: () => {}, onBack: () => {} }),
    );
    const brand = page.indexOf("lp-login-brand");
    const card = page.indexOf("lp-login-auth");
    assert.ok(brand > -1 && card > -1);
    assert.ok(brand < card, "brand panel must precede the card");
    assert.ok(page.indexOf("lp-login-back") < card, "Back must render above the card");
  });
});
