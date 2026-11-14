/**
 * QA enterprise (557f556c) #4: retention_days was stored but never applied.
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { autoPurgeHint, purgeConfirmCopy, RETENTION_HINT, retentionHint } from "./jobRetention.js";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

describe("job retention copy", () => {
  it("states that nothing runs on a schedule", () => {
    assert.match(RETENTION_HINT, /Nothing is deleted on a schedule/);
    assert.doesNotMatch(RETENTION_HINT, /not wired/);
  });

  it("says so when the scheduled purge is on (Security 23)", () => {
    assert.equal(retentionHint(false), RETENTION_HINT);
    assert.match(retentionHint(true), /once a day/);
    assert.match(retentionHint(true), /audit records and quarantine are never touched/);
    assert.doesNotMatch(retentionHint(true), /Nothing is deleted on a schedule/);
  });

  it("confirms with the dry-run count", () => {
    const c = purgeConfirmCopy({ would_remove: 2, cutoff: "2026-10-01T00:00:00+00:00", retention_days: 30, confirm_token: "t" });
    assert.equal(c.title, "Delete 2 finished jobs?");
    assert.match(c.message, /before 2026-10-01/);
    assert.equal(purgeConfirmCopy({ would_remove: 0, cutoff: "2026-10-01", retention_days: 30, confirm_token: "t" }).nothing, true);
  });

  it("never says 'deleted daily' beside 'Nothing is deleted on a schedule' (UX-24)", () => {
    // Field hint and section hint agree for both states of the switch.
    for (const on of [false, true]) {
      const field = autoPurgeHint(on);
      const section = retentionHint(on);
      if (on) {
        assert.match(field, /once a day/);
        assert.match(section, /once a day/);
        assert.doesNotMatch(`${field} ${section}`, /Nothing is deleted on a schedule/);
      } else {
        assert.match(field, /nothing is deleted on a schedule/i);
        assert.match(section, /Nothing is deleted on a schedule/);
        assert.doesNotMatch(`${field} ${section}`, /daily|once a day/);
      }
    }
    const settings = readFileSync(join(dirname(fileURLToPath(import.meta.url)), "..", "pages", "SettingsPage.tsx"), "utf8");
    assert.doesNotMatch(settings, /deleted daily in every workspace/);
    assert.match(settings, /autoPurgeHint\(retentionAutoPurge\)/);
  });
});
