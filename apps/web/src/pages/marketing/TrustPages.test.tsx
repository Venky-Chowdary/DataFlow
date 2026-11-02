import assert from "node:assert/strict";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, it } from "node:test";
import { StatusPage, SubprocessorsPage } from "./TrustPages.js";
import { PUBLIC_PAGE_META, hashForPublicRoute, publicRouteFromHash } from "../../lib/publicNavigation.js";

describe("trust pages", () => {
  it("routes status, dpa, and subprocessors through the canonical public router", () => {
    for (const route of ["status", "dpa", "subprocessors"] as const) {
      assert.equal(publicRouteFromHash(hashForPublicRoute(route)), route);
      assert.ok(PUBLIC_PAGE_META[route].title);
    }
  });

  it("status page renders a checking state, never a hard-coded green", () => {
    const html = renderToStaticMarkup(createElement(StatusPage));
    assert.match(html, /Checking systems/);
    assert.doesNotMatch(html, /All systems operational/);
  });

  it("subprocessor list is marked draft and names each provider's trigger", () => {
    const html = renderToStaticMarkup(createElement(SubprocessorsPage));
    assert.match(html, /Draft pending legal review/);
    for (const name of ["Railway", "MongoDB", "Resend", "OpenAI", "Amazon Web Services"]) {
      assert.match(html, new RegExp(name));
    }
    assert.match(html, /Only if a workspace admin enables a cloud model/);
  });
});
