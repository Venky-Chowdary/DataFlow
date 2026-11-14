/**
 * UX-22: viewer permissions were inconsistent. Refusals lived only in
 * tooltips, "Sync now" was enabled for a viewer and then refused, and a viewer
 * could build a whole Studio transfer before Validate refused it.
 */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, it } from "node:test";

const src = join(dirname(fileURLToPath(import.meta.url)), "..");
const read = (rel: string) => readFileSync(join(src, rel), "utf8");

describe("viewer refusals", () => {
  it("Schedules offers Run now only to someone who may run it", () => {
    assert.match(read("pages/SchedulesPage.tsx"), /onSync=\{jobRun\.allowed \? \(\) => void runSchedule\(sched\.id\) : undefined\}/);
  });

  it("the pipeline drawer states the refusal on the page", () => {
    const drawer = read("components/PipelineDetailDrawer.tsx");
    assert.match(drawer, /\{runRefusal \? \([\s\S]{0,120}<p className="df2-permission-inline" data-testid="pipeline-run-refusal">\{runRefusal\}<\/p>/);
  });

  it("Jobs disables Sync now and Re-sync for a viewer and says why", () => {
    const jobs = read("pages/JobsPage.tsx");
    assert.equal((jobs.match(/disabled=\{retrying \|\| !jobRunGate\.allowed\}/g) ?? []).length, 2);
    assert.match(jobs, /data-testid="jobs-sync-refusal">\{jobRunGate\.reason\}/);
  });

  it("Studio tells a viewer up front and refuses at Map, not after Validate", () => {
    const studio = read("pages/TransferPage.tsx");
    assert.match(studio, /useWriteGate\(PERMISSIONS\.jobPlan, "map, validate or run a transfer"\)/);
    assert.match(studio, /allowed=\{jobPlan\.allowed && jobRun\.allowed\}/);
    const go = studio.slice(studio.indexOf("const goToMapping = async () => {"));
    assert.match(go.slice(0, 400), /if \(!jobPlan\.allowed\) \{[\s\S]*toast\(\{ title: "View only", message: jobPlan\.reason/);
  });
});
