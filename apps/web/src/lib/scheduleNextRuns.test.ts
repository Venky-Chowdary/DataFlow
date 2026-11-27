/**
 * The schedule form's "Next 5 runs" says what the server will do.
 * Expected instants were produced by apps/api/services/cron_schedule.next_run
 * (the scheduler's own function) for each case.
 * Run: npx tsx --test apps/web/src/lib/scheduleNextRuns.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { readFileSync } from "node:fs";
import { describeEvery, parseCron, cronNextRuns, previewNextRuns, unevenMinuteStep } from "./scheduleNextRuns.ts";

const FIXTURE = JSON.parse(readFileSync(new URL("./__fixtures__/cron_next_runs.json", import.meta.url), "utf8")) as {
  cases: { cron: string; zone: string; after: string; runs: string[]; error?: string }[];
};

const SERVER: [string, string, string, string[]][] = [
  ["10 10 * * *", "UTC", "2026-11-21T10:00:00Z", ["2026-11-21T10:10:00Z", "2026-11-22T10:10:00Z", "2026-11-23T10:10:00Z", "2026-11-24T10:10:00Z", "2026-11-25T10:10:00Z"]],
  // Vixie: both day fields restricted, either matches.
  ["0 9 1 * MON", "UTC", "2026-11-21T10:00:00Z", ["2026-11-23T09:00:00Z", "2026-11-30T09:00:00Z", "2026-12-01T09:00:00Z", "2026-12-07T09:00:00Z", "2026-12-14T09:00:00Z"]],
  ["0 2 * * 7", "UTC", "2026-11-21T10:00:00Z", ["2026-11-22T02:00:00Z", "2026-11-29T02:00:00Z", "2026-12-06T02:00:00Z", "2026-12-13T02:00:00Z", "2026-12-20T02:00:00Z"]],
  // 02:30 does not exist on 14 March 2027 in New York: no run that day.
  ["30 2 * * *", "America/New_York", "2027-03-13T00:00:00Z", ["2027-03-13T07:30:00Z", "2027-03-15T06:30:00Z", "2027-03-16T06:30:00Z", "2027-03-17T06:30:00Z", "2027-03-18T06:30:00Z"]],
  // 01:30 happens twice on 1 November 2026 in New York: two runs.
  ["30 1 * * *", "America/New_York", "2026-10-31T00:00:00Z", ["2026-10-31T05:30:00Z", "2026-11-01T05:30:00Z", "2026-11-01T06:30:00Z", "2026-11-02T06:30:00Z", "2026-11-03T06:30:00Z"]],
  ["15 9 * * MON-FRI", "Europe/London", "2026-03-27T00:00:00Z", ["2026-03-27T09:15:00Z", "2026-03-30T08:15:00Z", "2026-03-31T08:15:00Z", "2026-04-01T08:15:00Z", "2026-04-02T08:15:00Z"]],
  ["0 0 31 * *", "Asia/Kolkata", "2026-11-21T10:00:00Z", ["2026-12-30T18:30:00Z", "2027-01-30T18:30:00Z", "2027-03-30T18:30:00Z", "2027-05-30T18:30:00Z", "2027-07-30T18:30:00Z"]],
];

describe("differential: the scheduler's own next_run over a random corpus (review UI2 #9)", () => {
  it(`agrees on all ${FIXTURE.cases.length} cases (crons x zones x DST gaps and overlaps)`, () => {
    assert.ok(FIXTURE.cases.length > 300);
    const mismatches: string[] = [];
    for (const c of FIXTURE.cases) {
      const after = new Date(c.after);
      let got: string[] | string;
      try {
        const cron = parseCron(c.cron);
        const runs = cronNextRuns(cron, c.zone, after, 5).map((d) => d.toISOString().replace(".000", ""));
        const p = previewNextRuns({ mode: "cron", preset: "", cron: c.cron, timeZone: c.zone, now: after });
        got = p.kind === "invalid" && p.never ? "CronNeverFiresError" : runs;
      } catch {
        got = "CronError";
      }
      const want = c.error ? c.error : c.runs;
      if (JSON.stringify(got) !== JSON.stringify(want)) mismatches.push(`${c.cron} @ ${c.zone} after ${c.after}: got ${JSON.stringify(got)} want ${JSON.stringify(want)}`);
    }
    assert.deepEqual(mismatches.slice(0, 8), [], `${mismatches.length} mismatches`);
  });
});

describe("previewNextRuns matches the scheduler", () => {
  for (const [cron, timeZone, now, expected] of SERVER) {
    it(`${cron} in ${timeZone}`, () => {
      const preview = previewNextRuns({ mode: "cron", preset: "daily", cron, timeZone, now: new Date(now) });
      assert.equal(preview.kind, "clock");
      if (preview.kind !== "clock") return;
      assert.deepEqual(preview.runs.map((d) => d.toISOString().replace(".000", "")), expected);
    });
  }

  it("notes a clock change inside the five runs", () => {
    const p = previewNextRuns({ mode: "cron", preset: "", cron: "15 9 * * MON-FRI", timeZone: "Europe/London", now: new Date("2026-03-27T00:00:00Z") });
    assert.equal(p.kind === "clock" && p.dstChange?.toISOString(), "2026-03-30T08:15:00.000Z");
    const q = previewNextRuns({ mode: "cron", preset: "", cron: "10 10 * * *", timeZone: "UTC", now: new Date("2026-11-21T10:00:00Z") });
    assert.equal(q.kind === "clock" && q.dstChange, null);
  });
});

describe("refuses what the server refuses, in plain words", () => {
  const invalid = (cron: string, timeZone = "UTC") => previewNextRuns({ mode: "cron", preset: "", cron, timeZone, now: new Date("2026-11-21T10:00:00Z") });
  it("needs exactly five fields (no seconds, no @daily)", () => {
    assert.deepEqual(invalid("0 0 * * * *"), { kind: "invalid", message: "A cron expression has 5 fields (minute hour day month weekday); this one has 6." });
    assert.equal(invalid("@daily").kind, "invalid");
  });
  it("bounds, ranges and dates that never come", () => {
    assert.match((invalid("61 * * * *") as { message: string }).message, /minute must be between 0 and 59/);
    assert.match((invalid("0 0 * * 5-1") as { message: string }).message, /starts after it ends/);
    assert.match((invalid("0 0 30 2 *") as { message: string }).message, /No date in the next four years/);
    assert.match((invalid("0 0 * * *", "Mars/Olympus") as { message: string }).message, /not a time zone/);
    assert.equal(invalid("   ").kind, "empty");
  });
  it("parses the server's aliases", () => {
    assert.deepEqual([...parseCron("0 0 * JAN-MAR SUN,7").dow], [0]);
    assert.deepEqual([...parseCron("0 0 * JAN-MAR *").months], [1, 2, 3]);
  });
});

describe("rolling cadences", () => {
  it("*/7 is every 7 minutes from the previous run, not a clock cron", () => {
    assert.equal(unevenMinuteStep("*/7 * * * *"), 7);
    assert.equal(unevenMinuteStep("*/5 * * * *"), null);
    const p = previewNextRuns({ mode: "cron", preset: "", cron: "*/7 * * * *", timeZone: "UTC", now: new Date("2026-11-21T10:00:00Z"), storedNextRunAt: "2026-11-21T10:05:00Z" });
    assert.equal(p.kind, "rolling");
    if (p.kind !== "rolling") return;
    assert.equal(p.runs[0].toISOString(), "2026-11-21T10:05:00.000Z", "an unchanged cadence keeps its stored slot");
    assert.equal(p.runs[4].toISOString(), "2026-11-21T10:33:00.000Z");
    const changed = previewNextRuns({ mode: "cron", preset: "", cron: "*/7 * * * *", timeZone: "UTC", now: new Date("2026-11-21T10:00:00Z"), storedNextRunAt: "2026-11-21T10:05:00Z", cadenceChanged: true });
    assert.equal(changed.kind === "rolling" && changed.runs[0].toISOString(), "2026-11-21T10:07:00.000Z", "a changed cadence re-anchors at save, like the server");
  });
  it("a paused schedule has no runs", () => {
    assert.deepEqual(previewNextRuns({ mode: "cron", preset: "", cron: "0 9 * * *", timeZone: "UTC", paused: true }), { kind: "paused" });
  });
  it("presets roll from the stored slot, or from save when there is none", () => {
    const daily = previewNextRuns({ mode: "preset", preset: "daily", cron: "", timeZone: "UTC", now: new Date("2026-11-21T10:00:00Z") });
    assert.equal(daily.kind === "rolling" && daily.fromLastRun, false);
    assert.equal(daily.kind === "rolling" && daily.runs[0].toISOString(), "2026-11-22T10:00:00.000Z");
    assert.equal(describeEvery(60), "every hour");
    assert.equal(describeEvery(7 * 24 * 60), "every 7 days");
    assert.equal(describeEvery(7), "every 7 minutes");
  });
});
