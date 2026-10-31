import assert from "node:assert/strict";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, it } from "node:test";
import { StagingRollbackPanel, stagingDiscardRecorded } from "./StagingRollbackPanel.js";

const DETAIL = "Failed to discard staging table `orders_df_staging` — fail closed";

function render(props: Partial<Parameters<typeof StagingRollbackPanel>[0]> = {}) {
  return renderToStaticMarkup(
    createElement(StagingRollbackPanel, {
      stagingTable: "orders_df_staging",
      busy: false,
      onDiscard: () => {},
      ...props,
    }),
  );
}

describe("StagingRollbackPanel (QA: Discard staging 409 showed no error)", () => {
  it("shows a refused discard on the panel, with the server's reason", () => {
    const markup = render({ error: DETAIL });
    assert.match(markup, /role="alert"/);
    assert.match(markup, /Discard refused/);
    assert.match(markup, /fail closed/);
    assert.match(markup, /orders_df_staging/);
    // Still offered: the operator can retry once the cause is fixed.
    assert.match(markup, /<button[^>]*>Discard staging<\/button>/);
  });

  it("offers the discard with no alert before any attempt", () => {
    const markup = render();
    assert.doesNotMatch(markup, /role="alert"/);
    assert.match(markup, /Discard staging table/);
  });

  it("replaces the button once the job records the discard", () => {
    const markup = render({ discarded: true });
    assert.doesNotMatch(markup, /<button/);
    assert.match(markup, /Staging table discarded/);
  });

  it("reads the discard the job recorded", () => {
    const exec = { status: "executed", strategy: "DISCARD_STAGING", staging_table: "orders_df_staging" };
    assert.equal(stagingDiscardRecorded(exec, "orders_df_staging"), true);
    assert.equal(stagingDiscardRecorded({ ...exec, staging_table: "other_df_staging" }, "orders_df_staging"), false);
    assert.equal(stagingDiscardRecorded({ ...exec, status: "refused" }, "orders_df_staging"), false);
    assert.equal(stagingDiscardRecorded(undefined, "orders_df_staging"), false);
  });
});
