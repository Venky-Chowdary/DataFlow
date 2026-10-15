import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, it } from "node:test";

const here = dirname(fileURLToPath(import.meta.url));
const contract = JSON.parse(readFileSync(join(here, "schedulesApiContract.json"), "utf8")) as {
  operations: Array<{ id: string; method: string; client: string }>;
};

describe("schedules API contract", () => {
  it("typed client calls the schedule routes and the studio page keeps classic", () => {
    const api = readFileSync(join(here, "api.ts"), "utf8");
    const page = readFileSync(join(here, "../pages/SchedulesPage.tsx"), "utf8");
    const studio = readFileSync(join(here, "../components/schedules/SchedulesStudio.tsx"), "utf8");
    const flags = readFileSync(join(here, "useStudioFlags.ts"), "utf8");
    for (const operation of contract.operations) {
      assert.match(
        api,
        new RegExp(operation.client.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")),
        operation.id,
      );
    }
    assert.match(api, /export async function fetchSchedules/);
    assert.match(api, /export async function createSchedule/);
    assert.match(api, /export async function updateSchedule/);
    assert.match(api, /export async function runScheduleNow/);
    assert.match(api, /export async function stageScheduleSync/);
    assert.match(api, /export async function fetchScheduleHistory/);
    assert.match(page, /useStudioFlags\(\)/);
    assert.match(page, /studioFlags\.canvas/);
    assert.match(page, /optOutCanvas/);
    assert.match(page, /df2-pipeline-row/);
    assert.match(page, /<PipelineCard/);
    assert.match(studio, /fetchScheduleHistory|ScheduleRunHistory/);
    assert.match(studio, /Classic UI/);
    assert.match(studio, /onRun/);
    assert.match(studio, /onToggle/);
    assert.match(studio, /onSync/);
    assert.match(studio, /onOpenJob/);
    assert.doesNotMatch(studio, /MOCK_SCHEDULE|fixtureSchedules|hardcodedSchedules/);
    assert.match(flags, /optOutCanvas/);
    assert.match(flags, /writeLocalStudioFlag\(STUDIO_CANVAS_KEY, false/);
  });
});
