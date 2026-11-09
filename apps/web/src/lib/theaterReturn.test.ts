import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, it } from "node:test";

import { theaterReturnToValidate } from "./theaterReturn.ts";
import { resolveValidateStudioPrimary } from "./validateStudioPrimary.ts";

const approved = {
  passed: true,
  run_id: "pf_api_1",
  gates: [],
  blockers: [],
  proof_bundle: { transfer_decision: { decision: "approve" } },
} as never;

function primaryAfterReturn(result: { success?: boolean } | null) {
  const launch = { jobId: "job-1", rows: 5 };
  const back = theaterReturnToValidate(result);
  return {
    back,
    primary: resolveValidateStudioPrimary({
      preflight: back.clearVerdict ? null : approved,
      transferLaunch: back.clearLaunch ? null : launch,
    }),
  };
}

describe("theaterReturnToValidate (QA1: rerun from the Run page)", () => {
  it("a completed run's rerun re-validates fresh and gives Execute back", () => {
    const { back, primary } = primaryAfterReturn({ success: true });
    assert.deepEqual(back, { clearLaunch: true, clearVerdict: true, rerunValidate: true });
    assert.notEqual(primary.kind, "open_live");
  });

  it("a failed run's rerun re-validates fresh too", () => {
    const { back } = primaryAfterReturn({ success: false });
    assert.equal(back.rerunValidate, true);
    assert.equal(back.clearLaunch, true);
  });

  it("a run still in flight keeps Open live progress", () => {
    const { back, primary } = primaryAfterReturn(null);
    assert.deepEqual(back, { clearLaunch: false, clearVerdict: false, rerunValidate: false });
    assert.equal(primary.kind, "open_live");
  });
});

describe("TransferPage wires the Run page rerun through theaterReturnToValidate", () => {
  const page = readFileSync(
    join(dirname(fileURLToPath(import.meta.url)), "..", "pages", "TransferPage.tsx"),
    "utf8",
  );

  it("leaving Theater for Validate applies the finished-run rule, not failure-only", () => {
    assert.match(page, /theaterReturnToValidate\(result\)/);
    assert.doesNotMatch(page, /if \(runFailed\) \{\s*setTransferLaunch\(null\)/);
  });

  it("Validate re-runs for a pending rerun", () => {
    assert.match(page, /freshValidatePending[\s\S]{0,200}void executePreflight\(\)/);
  });

  it("every Run footer Back after a result takes the same path", () => {
    const backs = page.match(/onClick=\{\(\) => \(result \? backFromTheaterToValidate\(\) : setStep\(STEP_VALIDATE\)\)\}/g) || [];
    assert.equal(backs.length, 2);
  });
});
