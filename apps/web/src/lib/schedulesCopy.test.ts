/**
 * UX-36 (part): Schedules copy. The header said "same-engine" syncs and named
 * other vendors, the form said "Add a connector first" while connections
 * existed, and the drawer cited "Google Dual Run".
 */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, it } from "node:test";

const src = join(dirname(fileURLToPath(import.meta.url)), "..");
const read = (rel: string) => readFileSync(join(src, rel), "utf8");

describe("Schedules copy", () => {
  it("the header describes what a schedule is, without same-engine or vendor names", () => {
    const page = read("pages/SchedulesPage.tsx");
    assert.doesNotMatch(page, /same-engine syncs|ADF\/Informatica/);
    assert.match(page, /Recurring syncs between your connections/);
  });

  it("the form asks to choose a connection when connections exist", () => {
    const form = read("components/schedules/ScheduleForm.tsx");
    assert.doesNotMatch(form, /placeholder="Add a connector first"/);
    assert.match(form, /connectors\.length \? "Choose the source connection" : "Add a connection first"/);
    assert.match(form, /connectors\.length \? "Choose the destination connection" : "Add a connection first"/);
  });

  it("the drawer does not cite a third-party product", () => {
    assert.doesNotMatch(read("components/PipelineDetailDrawer.tsx"), /Google Dual Run/);
  });
});
