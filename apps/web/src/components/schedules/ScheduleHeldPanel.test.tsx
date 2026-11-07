/** QA C9: a held schedule says why and offers the destination recovery. */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { renderToStaticMarkup } from "react-dom/server";
import { ScheduleHeldPanel } from "./ScheduleHeldPanel.js";
import type { PipelineSchedule } from "../../lib/types.js";

const held = {
  id: "s1",
  enabled: true,
  state: "stalled",
  next_run_at: null,
  stalled_reason:
    "Parked on a finding (DESTINATION_CONTINUITY): Verification failed for table 'orders': the table no longer exists.",
  needs_approval: true,
  approval_code: "DESTINATION_CONTINUITY",
} as unknown as PipelineSchedule;

const noop = () => undefined;

describe("ScheduleHeldPanel", () => {
  it("names the reason and offers rebuild and rebaseline", () => {
    const html = renderToStaticMarkup(<ScheduleHeldPanel schedule={held} onRecoverDestination={noop} />);
    assert.match(html, /role="alert"/);
    assert.match(html, /the table no longer exists/);
    assert.match(html, /aria-label="Recover destination"/);
    assert.match(html, /Rebuild from source/);
    assert.match(html, /Accept as it is \(rebaseline\)/);
    assert.match(html, /aria-label="Recovery reason"/);
  });

  it("keeps both actions disabled until a reason is given", () => {
    const html = renderToStaticMarkup(<ScheduleHeldPanel schedule={held} onRecoverDestination={noop} />);
    assert.equal((html.match(/<button[^>]*disabled/g) || []).length, 2);
  });

  it("does not offer a destination recovery for another finding", () => {
    const other = { ...held, approval_code: "SOURCE_DRIFT", stalled_reason: "Parked on a finding (SOURCE_DRIFT)" };
    const html = renderToStaticMarkup(
      <ScheduleHeldPanel schedule={other as PipelineSchedule} onRecoverDestination={noop} />,
    );
    assert.match(html, /SOURCE_DRIFT/);
    assert.doesNotMatch(html, /Recover destination/);
  });

  it("says why a viewer cannot recover", () => {
    const html = renderToStaticMarkup(
      <ScheduleHeldPanel schedule={held} onRecoverDestination={noop} manageRefusal="Viewers cannot change schedules" />,
    );
    assert.match(html, /title="Viewers cannot change schedules"/);
  });
});
