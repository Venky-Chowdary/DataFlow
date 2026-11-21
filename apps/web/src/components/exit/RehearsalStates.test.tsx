/**
 * The rehearsal page's state mapping (owner review, 20 Nov): a step after a
 * failed extract never reads "Done"; durations show only when measured; the
 * definition-changed notice is said once with a re-run; the export-bucket
 * failure is said in the user's terms with the server wording kept for
 * administrators; the window is one sentence when nothing was measured.
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import React from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { RehearsalView, type RehearsalViewProps } from "./RehearsalView.js";
import {
  NO_BUCKET_TITLE,
  STALE_LINE,
  durationSummary,
  measuredStepDuration,
  primaryAction,
  rehearsalProblem,
  rehearsalSteps,
  startErrorCopy,
} from "./rehearsalSummary.js";
import {
  NO_EXPORT_BUCKET_REASON,
  incompleteRehearsal,
  noExportBucketRehearsal,
  notProvenRehearsal,
  provenRehearsal,
  runningRehearsalRaw,
  signedRehearsal,
} from "./exitFixtures.js";
import { realFailed, realPiiHeld, realSigned, realStale } from "./realExitPayloads.js";
import { normalizeRehearsal, type ExitRehearsal } from "../../lib/exitReadiness.js";

const noop = () => {};

function text(html: string): string {
  return html
    .replace(/<title>[\s\S]*?<\/title>/g, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/&#x27;/g, "'")
    .replace(/&quot;/g, '"')
    .replace(/&amp;/g, "&")
    .replace(/\s+/g, " ");
}

function view(rehearsal: ExitRehearsal, over: Partial<RehearsalViewProps> = {}): string {
  return renderToStaticMarkup(
    <RehearsalView rehearsal={rehearsal} busy={null} canSign onBack={noop} onRefresh={noop} onExportMarkdown={noop}
      onDownloadSigned={noop} onVerifySigned={noop} onVerifyFile={noop} onSign={noop} onRerun={noop} {...over} />,
  );
}

const states = (r: ExitRehearsal) => rehearsalSteps(r).map((s) => `${s.label}: ${s.stateLabel}${s.detail ? ` (${s.detail})` : ""}`);

describe("rehearsal steps follow the real run", () => {
  it("failed extract with nothing extracted: every later step is Not run, whatever the API's timeline says", () => {
    const r = noExportBucketRehearsal();
    // The API reports verify "done · 22 s" and seal "pending · 0 s" for this run.
    assert.equal(r.timeline.find((s) => s.stage === "verify")?.status, "done");
    assert.deepEqual(states(r), [
      "Inventory: Done",
      "Extract: Failed (0 of 1 table extracted)",
      "Verify: Not run (Nothing was extracted)",
      "Seal: Not run",
      "Sign-off: Not run",
    ]);
    const steps = rehearsalSteps(r);
    const failedAt = steps.findIndex((s) => s.state === "failed");
    assert.ok(steps.slice(failedAt + 1).every((s) => s.state === "not_run"));
  });

  it("partial failure: verify and seal ran on what was extracted, sign-off did not", () => {
    assert.deepEqual(states(realFailed), [
      "Inventory: Done",
      "Extract: Failed (1 of 2 tables extracted)",
      "Verify: Done (under 1 s)",
      "Seal: Done (1 part)",
      "Sign-off: Not run",
    ]);
  });

  it("held by the PII gate with nothing extracted: Held, then Not run", () => {
    assert.deepEqual(states(realPiiHeld).slice(1, 3), ["Extract: Held (0 of 1 table extracted)", "Verify: Not run (Nothing was extracted)"]);
  });

  it("running: steps not reached read Not started; proven: sign-off waits with its count; signed: done", () => {
    assert.deepEqual(states(normalizeRehearsal(runningRehearsalRaw())).slice(1), [
      "Extract: Running",
      "Verify: Not started",
      "Seal: Not started",
      "Sign-off: Not started",
    ]);
    assert.equal(states(provenRehearsal())[4], "Sign-off: Waiting (1 of 3 signed)");
    assert.match(states(signedRehearsal())[4], /^Sign-off: Done \(/);
  });

  it("a duration shows only when the step was timed: no 0 s from identical start and end", () => {
    assert.equal(measuredStepDuration({ seconds: 0, started_at: "2026-11-19T14:02:33Z", finished_at: "2026-11-19T14:02:33Z" }), "");
    assert.equal(measuredStepDuration({ seconds: null, started_at: null, finished_at: null }), "");
    assert.equal(measuredStepDuration({ seconds: 22, started_at: "2026-11-19T14:02:11Z", finished_at: "2026-11-19T14:02:33Z" }), "22 s");
    for (const r of [noExportBucketRehearsal(), realFailed, provenRehearsal(), normalizeRehearsal(runningRehearsalRaw())]) {
      const t = text(view(r));
      assert.doesNotMatch(t, /Pending · |\b0 s\b/, r.system_name);
    }
  });
});

describe("the failure callout", () => {
  it("no export bucket: user terms in the title, the server sentence only for administrators", () => {
    const r = noExportBucketRehearsal();
    const problem = rehearsalProblem(r)!;
    assert.equal(problem.title, NO_BUCKET_TITLE);
    assert.equal(problem.adminDetail, NO_EXPORT_BUCKET_REASON);

    const user = view(r, { isAdmin: false });
    const t = text(user);
    assert.match(t, /Extract failed: no export bucket is set for this workspace\. An administrator needs to set one up\. Then re-run the rehearsal\./);
    assert.doesNotMatch(user, /DATAWRAP_|The request was refused|Details for administrators|table\(s\)/);
    // The failed table's row says it in a few words, once.
    assert.match(t, /qav4_seed Failed Not extracted, so nothing was checked\./);
    assert.equal(t.match(/export bucket/g)?.length, 1);

    const admin = view(r, { isAdmin: true });
    assert.match(admin, /<summary>Details for administrators<\/summary>/);
    assert.match(admin, /DATAWRAP_EXPORT_BUCKET/);
    assert.match(admin, /DATAWRAP_LOCAL_FILE_DESTINATIONS=on/);
  });

  it("no left stripes and no legacy notices on the page", () => {
    for (const r of [noExportBucketRehearsal(), realFailed, realStale, realPiiHeld, incompleteRehearsal(), notProvenRehearsal()]) {
      const html = view(r);
      assert.doesNotMatch(html, /df2-parity-notice/);
      assert.equal((html.match(/class="df-callout /g) ?? []).length <= 2, true);
    }
  });

  it("a refused start reads in the user's terms", () => {
    const refused = startErrorCopy(NO_EXPORT_BUCKET_REASON);
    assert.doesNotMatch(refused.title + refused.body, /DATAWRAP_/);
    assert.equal(refused.adminDetail, NO_EXPORT_BUCKET_REASON);
    assert.deepEqual(startErrorCopy("This system already has a running rehearsal."), {
      title: "Could not start the rehearsal.", body: "This system already has a running rehearsal.", adminDetail: "",
    });
  });
});

describe("definition changed", () => {
  it("is said once, in one line, with the re-run as the header's primary action", () => {
    const r = noExportBucketRehearsal();
    const t = text(view(r));
    assert.equal(t.split(STALE_LINE).length - 1, 1);
    assert.doesNotMatch(t, /it does not prove|Rehearse it again/);
    assert.equal(primaryAction(r, { canRerun: true, canReviewPii: true }), "rerun");
    // A proven rehearsal of an older definition re-runs instead of going to sign-off.
    assert.equal(primaryAction(realStale, { canRerun: true, canReviewPii: true }), "rerun");
    assert.equal(primaryAction({ ...realStale, current: true }, { canRerun: true, canReviewPii: true }), "signoff");
    assert.equal(primaryAction(realSigned, { canRerun: true, canReviewPii: true }), null);
    assert.equal(primaryAction(realPiiHeld, { canRerun: true, canReviewPii: true }), "pii");
  });

  it("re-run is disabled with the reason when the person may not start a rehearsal", () => {
    const html = view(noExportBucketRehearsal(), { canRerun: false, rerunDeniedReason: "Needs job.run" });
    assert.match(html, /<button[^>]*disabled=""[^>]*title="Needs job.run"[^>]*>[\s\S]*?Re-run rehearsal|<button[^>]*title="Needs job.run"[^>]*disabled=""[^>]*>[\s\S]*?Re-run rehearsal/);
  });
});

describe("the window: measured vs not measured", () => {
  it("nothing measured is one plain sentence, no dashes", () => {
    const sum = durationSummary(noExportBucketRehearsal());
    assert.deepEqual(sum, { text: "Not measured: nothing was extracted. The window is 4 h.", measured: false, compared: false, tone: "none" });
    const t = text(view(noExportBucketRehearsal()));
    assert.match(t, /Exit time Not measured: nothing was extracted\. The window is 4 h\./);
    assert.doesNotMatch(t, /—|margin|Merkle|Not sealed/);
  });

  it("measured and proven is compared; measured but not proven is not", () => {
    assert.deepEqual(durationSummary(provenRehearsal()), { text: "61 h of the 30 d window: fits, 91% to spare.", measured: true, compared: true, tone: "ok" });
    const over = durationSummary({ ...provenRehearsal(), duration: { ...provenRehearsal().duration, measured_seconds: 40 * 3600, window_seconds: 36 * 3600, fits_window: false, margin_pct: -11 } });
    assert.equal(over.text, "40 h against the 36 h window: over by 4 h.");
    assert.equal(over.tone, "bad");
    assert.deepEqual(durationSummary(incompleteRehearsal()), {
      text: "Took 40 h. Only a proven rehearsal is compared with the 36 h window.", measured: true, compared: false, tone: "none",
    });
    assert.match(durationSummary(normalizeRehearsal(runningRehearsalRaw())).text, /^Measured when the rehearsal finishes/);
  });
});

describe("board status of a changed definition", () => {
  it("only a proof is demoted: a failed rehearsal of an older definition still reads Failed", async () => {
    const { ExitBoardView } = await import("./ExitBoardView.js");
    const { boardFixture, FIXTURE_NOW } = await import("./exitFixtures.js");
    const board = boardFixture();
    const base = board.systems[0];
    board.systems = [
      { ...base, id: "exs_failedstale1", name: "Failed stale", state: "failed", stale: true, overdue: false, completeness_label: "Failed, 0 of 1 table extracted" },
      { ...base, id: "exs_provenstale1", name: "Proven stale", state: "proven", stale: true, overdue: false },
    ];
    const html = renderToStaticMarkup(
      <ExitBoardView readiness={board} query="" onQueryChange={noop} filter="all" onFilterChange={noop} selectedId={null}
        onOpenSystem={noop} onRunRehearsal={noop} onDeclare={noop} canDeclare canRun now={FIXTURE_NOW} />,
    );
    const row = (id: string) => text(html.match(new RegExp(`<tr[^>]*id="exit-system-${id}"[\\s\\S]*?</tr>`))![0]);
    assert.match(row("exs_failedstale1"), /Failed, 0 of 1 table extracted .* Failed\s*$/);
    assert.doesNotMatch(row("exs_failedstale1"), /Not proven/);
    assert.match(row("exs_provenstale1"), /Not proven\s*$/);
  });
});
