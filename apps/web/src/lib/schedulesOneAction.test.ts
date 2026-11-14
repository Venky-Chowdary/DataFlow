/** UX-36: Schedules offered both "Run now" and "Sync now", and a row menu that only repeated Sync now. */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, it } from "node:test";

const src = join(dirname(fileURLToPath(import.meta.url)), "..");
const read = (rel: string) => readFileSync(join(src, rel), "utf8");

describe("one run action per schedule", () => {
  it("the drawer has Run now and no Sync now", () => {
    const drawer = read("components/PipelineDetailDrawer.tsx");
    assert.match(drawer, /"Run now"/);
    assert.doesNotMatch(drawer, />\s*Sync now\s*</);
    assert.doesNotMatch(drawer, /onSync/);
  });
  it("a row has one Run now and no menu that repeats it", () => {
    const card = read("components/ui/PipelineCard.tsx");
    assert.doesNotMatch(card, /df2-pipeline-row-menu/);
    assert.doesNotMatch(card, />\s*Sync now\s*</);
    assert.match(card, /Run now/);
  });
  it("Run now takes the confirmed sync when a contract is signed, else runs directly", () => {
    const page = read("pages/SchedulesPage.tsx");
    assert.match(page, /return sched\?\.contract_id \? handleScheduleSync\(id, "sync"\) : handleRunNow\(id\);/);
    assert.match(page, /title: mode === "full" \? "Re-sync from the start" : "Run now"/);
  });
});
