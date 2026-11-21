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

  it("subprocessor list is available on request and the page is noindex", () => {
    const html = renderToStaticMarkup(createElement(SubprocessorsPage));
    assert.match(html, /available on request/);
    assert.match(html, /contact@datawrap\.io/);
    assert.doesNotMatch(html, /Draft/i);
    assert.equal(PUBLIC_PAGE_META.subprocessors.robots, "noindex");
    assert.equal(PUBLIC_PAGE_META.dpa.robots, "noindex");
  });

  it("trust pages name no hosting provider and no old brand (bundle brand gate)", () => {
    // The same /dataflow|railway/i the built bundle is scanned with
    // (scripts/check_bundle_brand.mjs): the hosting provider is a deployment
    // fact for the order form and DPA, not for the public bundle.
    for (const Page of [StatusPage, SubprocessorsPage]) {
      const html = renderToStaticMarkup(createElement(Page));
      assert.doesNotMatch(html, /dataflow|railway/i, Page.name);
    }
  });
});
