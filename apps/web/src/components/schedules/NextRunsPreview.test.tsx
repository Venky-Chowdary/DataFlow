/**
 * The schedule form's "Next 5 runs" (UI pass 2, item 5).
 * Run: npx tsx --test apps/web/src/components/schedules/NextRunsPreview.test.tsx
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import React from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { NextRunsPreview } from "./NextRunsPreview.tsx";

const now = new Date("2026-11-21T10:00:00Z");
const html = (props: Partial<React.ComponentProps<typeof NextRunsPreview>>) =>
  renderToStaticMarkup(React.createElement(NextRunsPreview, { mode: "cron", preset: "daily", cron: "", timeZone: "UTC", now, ...props }));

describe("NextRunsPreview", () => {
  it("lists five runs in the schedule's zone, as times a screen reader can read", () => {
    const out = html({ cron: "0 9 * * MON-FRI", timeZone: "Europe/London" });
    assert.match(out, /<h4[^>]*>.*Next 5 runs<span class="df2-next-runs-zone">Europe\/London<\/span><\/h4>/);
    const times = [...out.matchAll(/<time dateTime="([^"]+)">([^<]+)<\/time>/g)].map((m) => [m[1], m[2]]);
    assert.equal(times.length, 5);
    assert.deepEqual(times[0], ["2026-11-23T09:00:00.000Z", "Mon 23 Nov, 09:00"]);
    assert.match(out, /aria-live="polite"/);
  });

  it("says why a cron cannot run, instead of listing nothing", () => {
    assert.match(html({ cron: "0 0 * * * *" }), /class="df2-next-runs-error".*has 5 fields.*this one has 6/);
    assert.match(html({ cron: "" }), /Enter a cron expression to see when it runs\./);
  });

  it("explains rolling cadences: no fixed clock time", () => {
    const preset = html({ mode: "preset", preset: "hourly" });
    assert.match(preset, /Runs every hour, counted from the previous run \(the first one after you save\)\. There is no fixed clock time\./);
    assert.match(html({ cron: "*/7 * * * *" }), /Runs every 7 minutes/);
  });

  it("notes a clock change among the five", () => {
    const out = html({ cron: "15 9 * * MON-FRI", timeZone: "Europe/London", now: new Date("2026-03-27T00:00:00Z") });
    assert.match(out, /Clocks change in Europe\/London before Mon 30 Mar, 09:15; the times above include it\./);
  });
});
