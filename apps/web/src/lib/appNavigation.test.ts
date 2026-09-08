/**
 * Run: npx --yes tsx --test apps/web/src/lib/appNavigation.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  focusFromHash,
  isSignedInHelpHash,
  screenFromHash,
  signedInHelpArticleFromHash,
  signedInScreenFromHash,
} from "./appNavigation.js";

describe("screenFromHash aliases", () => {
  it("maps Pipelines label URL to schedules screen", () => {
    assert.equal(screenFromHash("#/pipelines"), "schedules");
    assert.equal(screenFromHash("#/pipeline"), "schedules");
    assert.equal(focusFromHash("#/pipelines")?.screen, "schedules");
  });

  it("keeps canonical screen ids", () => {
    assert.equal(screenFromHash("#/schedules"), "schedules");
    assert.equal(screenFromHash("#/jobs"), "jobs");
    assert.equal(screenFromHash("#/transfer"), "transfer");
  });

  it("maps overview alias to dashboard but leaves marketing #/home alone", () => {
    assert.equal(screenFromHash("#/overview"), "dashboard");
    assert.equal(screenFromHash("#/home"), null);
  });

  it("maps Proofs nav label URL to benchmarks screen", () => {
    assert.equal(screenFromHash("#/proofs"), "benchmarks");
    assert.equal(screenFromHash("#/proof"), "benchmarks");
  });
});

describe("signed-in Help hashes stay in the workspace", () => {
  it("maps public help hashes to the docs screen", () => {
    assert.equal(isSignedInHelpHash("#/help"), true);
    assert.equal(isSignedInHelpHash("#/help/getting-started"), true);
    assert.equal(isSignedInHelpHash("#/help-faq"), true);
    assert.equal(isSignedInHelpHash("#/pricing"), false);
    assert.equal(signedInScreenFromHash("#/help"), "docs");
    assert.equal(signedInScreenFromHash("#/help/preflight-gates"), "docs");
    assert.equal(signedInScreenFromHash("#/docs"), "docs");
    assert.equal(signedInScreenFromHash("#/pricing"), null);
  });

  it("opens the cited article, not the Docs walkthrough home", () => {
    assert.equal(signedInHelpArticleFromHash("#/help/preflight-gates"), "help-preflight-gates");
    assert.equal(
      signedInHelpArticleFromHash("#/help/data-pilot#what-is-quarantine"),
      "help-data-pilot",
    );
    assert.equal(signedInHelpArticleFromHash("#/help-faq"), "help-faq");
    assert.equal(signedInHelpArticleFromHash("#/help"), "help");
    assert.equal(signedInHelpArticleFromHash("#/docs"), null);
    assert.equal(signedInHelpArticleFromHash("#/pilot"), null);
  });
});
