/**
 * Run: npx --yes tsx --test apps/web/src/lib/appNavigation.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  emailedJobLinkUrl,
  focusFromHash,
  hashForScreen,
  inviteLinkToken,
  isInviteLinkRoute,
  isResetLinkRoute,
  resetLinkToken,
  isSignedInHelpHash,
  normalizedAppHash,
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

describe("isResetLinkRoute", () => {
  it("claims the emailed path form at boot", () => {
    assert.equal(isResetLinkRoute("/reset-password", ""), true);
  });

  it("claims the stripped-token hash form so the router leaves it alone", () => {
    assert.equal(isResetLinkRoute("/", "#/reset-password"), true);
  });

  it("does not claim ordinary routes", () => {
    assert.equal(isResetLinkRoute("/", ""), false);
    assert.equal(isResetLinkRoute("/", "#/dashboard"), false);
    assert.equal(isResetLinkRoute("/login", "#/login"), false);
    assert.equal(isResetLinkRoute("/reset-passwords", "#/reset-passwords"), false);
  });
});

describe("resetLinkToken", () => {
  it("reads the token from the URL fragment", () => {
    assert.equal(resetLinkToken("/reset-password", "#token=abc.def-123"), "abc.def-123");
  });
  it("rejects query-string and non-reset routes", () => {
    assert.equal(resetLinkToken("/reset-password", ""), "");
    assert.equal(resetLinkToken("/reset-password", "#/other"), "");
    assert.equal(resetLinkToken("/login", "#token=x"), "");
    assert.equal(resetLinkToken("/reset-passwords", "#token=x"), "");
  });
});

it("the Transform Library survives a reload and a shared link", () => {
  assert.equal(screenFromHash(hashForScreen("transform-library")), "transform-library");
  assert.equal(screenFromHash("#/library"), "transform-library");
});

describe("workspace invitation links", () => {
  it("reads the token from the fragment of /accept-invite only", () => {
    assert.equal(inviteLinkToken("/accept-invite", "#token=abc_DEF-123"), "abc_DEF-123");
    assert.equal(inviteLinkToken("/reset-password", "#token=abc"), "");
    assert.equal(inviteLinkToken("/accept-invite", ""), "");
    assert.equal(resetLinkToken("/accept-invite", "#token=abc"), "");
  });

  it("keeps the stripped #/accept-invite route on the invite stage", () => {
    assert.equal(isInviteLinkRoute("/accept-invite", "#token=x"), true);
    assert.equal(isInviteLinkRoute("/", "#/accept-invite"), true);
    assert.equal(isInviteLinkRoute("/", "#/reset-password"), false);
  });
});

describe("boot hash normalisation keeps a jobs deep link (fresh-load race)", () => {
  it("replace on the same screen keeps ?jobId", () => {
    assert.equal(normalizedAppHash("jobs", "#/jobs?jobId=older", true), "#/jobs?jobId=older");
    assert.equal(
      normalizedAppHash("jobs", "#/jobs?jobId=older&panel=quarantine", true),
      "#/jobs?jobId=older&panel=quarantine",
    );
  });
  it("navigating or switching screens drops the focus", () => {
    assert.equal(normalizedAppHash("jobs", "#/jobs?jobId=older", false), "#/jobs");
    assert.equal(normalizedAppHash("dashboard", "#/jobs?jobId=older", true), "#/overview");
  });
  it("replace on Exit readiness keeps the rehearsal / system ids", () => {
    assert.equal(normalizedAppHash("exit", "#/exit?rehearsal=exr_1", true), "#/exit?rehearsal=exr_1");
    assert.equal(normalizedAppHash("exit", "#/exit?system=sys_1", true), "#/exit?system=sys_1");
    assert.equal(normalizedAppHash("exit", "#/exit?rehearsal=exr_1", false), "#/exit");
    assert.equal(normalizedAppHash("jobs", "#/exit?rehearsal=exr_1", true), "#/jobs");
  });
});

/** D339: notification e-mails link to https://<web origin>/jobs/<id>. nginx
 *  serves the SPA for that path, but the app routes on the hash only, so the
 *  link opened Overview and the job was nowhere: a dead end. */
describe("emailedJobLinkUrl", () => {
  it("turns /jobs/<id> into the Jobs screen focused on that job", () => {
    assert.equal(emailedJobLinkUrl("/jobs/6afb747e5eafee38493e1030", "", ""), "/#/jobs?jobId=6afb747e5eafee38493e1030");
    assert.equal(emailedJobLinkUrl("/jobs/job-9/", "", ""), "/#/jobs?jobId=job-9");
    // The focus reads back through the existing deep-link parser.
    const url = emailedJobLinkUrl("/jobs/job-9", "", "");
    assert.deepEqual(focusFromHash(url.slice(1)), { screen: "jobs", jobId: "job-9", panel: undefined });
  });

  it("leaves every other path alone", () => {
    for (const path of ["/", "/jobs", "/jobs/", "/jobs/a/b", "/reset-password", "/api/v1/jobs/x"]) {
      assert.equal(emailedJobLinkUrl(path, "", ""), "", path);
    }
    // An id with characters a job id never has is not a job link.
    assert.equal(emailedJobLinkUrl("/jobs/%3Cscript%3E", "", ""), "");
  });
});
