/**
 * Parity charts: accessible summaries, drawn to scale, theme tokens only.
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { renderToStaticMarkup } from "react-dom/server";
import {
  DifferenceSplitBar,
  ParityRing,
  ParityTrendChart,
  TableParityBars,
  TableRunHeatmap,
  heatmapSummary,
  trendSummary,
} from "./ParityCharts.js";
import { summaryFixture, unexplainedReport } from "../parity/parityFixtures.js";

function noLiteralColours(html: string) {
  assert.doesNotMatch(html, /(fill|stroke|stop-color|background)[=:]\s*"?#[0-9a-fA-F]{3,8}/);
  assert.doesNotMatch(html, /rgb\(/);
}

describe("ParityRing", () => {
  it("states the figure and draws it unrounded, so 99.97% is not a closed ring", () => {
    const html = renderToStaticMarkup(<ParityRing pct={99.97} tone="warn" />);
    assert.match(html, /role="img"/);
    assert.match(html, /aria-label="99\.97% of rows proven equal"/);
    assert.match(html, /stroke-dasharray="99\.97 100"/);
    assert.doesNotMatch(html, /is-complete/);
    assert.match(html, /tone-warn/);
    noLiteralColours(html);
  });

  it("closes only at 100% and says unknown when there is no figure", () => {
    assert.match(renderToStaticMarkup(<ParityRing pct={100} />), /stroke-dasharray="100 0"/);
    const unknown = renderToStaticMarkup(<ParityRing pct={null} />);
    assert.match(unknown, /aria-label="Share of rows proven equal is unknown"/);
    assert.match(unknown, /tone-idle/);
    assert.match(unknown, />Unknown</);
  });
});

describe("TableParityBars", () => {
  const html = renderToStaticMarkup(<TableParityBars tables={unexplainedReport().tables} />);

  it("draws each bar to scale and states the figure beside it", () => {
    assert.match(html, /width="99\.600%"/);
    assert.match(html, /width="100\.000%"/);
    assert.match(html, />≥ 99\.85%</);
    assert.match(html, />99\.6%</);
  });

  it("shows how each table was checked and what is left", () => {
    assert.match(html, />Full checksum</);
    assert.match(html, />Count \+ sampled values</);
    assert.match(html, />4 to resolve</);
    assert.match(html, /is-warn/);
    assert.match(html, /<caption class="df2-sr-only">Parity per table<\/caption>/);
    noLiteralColours(html);
  });
});

describe("DifferenceSplitBar", () => {
  it("splits explained from unexplained to scale with a gap", () => {
    const html = renderToStaticMarkup(<DifferenceSplitBar explained={8} unexplained={4} />);
    assert.match(html, /aria-label="8 explained, 4 unexplained"/);
    // Fallback width 320, 2px gap: 318 usable, two thirds explained.
    assert.match(html, /class="df2-parity-split-ok" x="0" y="8" width="212"/);
    assert.match(html, /class="df2-parity-split-bad" x="214" y="8" width="106"/);
    noLiteralColours(html);
  });

  it("draws an empty track when nothing differs", () => {
    const html = renderToStaticMarkup(<DifferenceSplitBar explained={0} unexplained={0} />);
    assert.match(html, /df2-parity-split-empty/);
    assert.doesNotMatch(html, /df2-parity-split-bad/);
  });
});

describe("ParityTrendChart", () => {
  const summary = summaryFixture();
  const html = renderToStaticMarkup(<ParityTrendChart runs={summary.runs} />);

  it("summarises the trend in words", () => {
    assert.equal(
      trendSummary(summary.runs),
      "Parity over the last 12 runs: between 99.5% and 100%; latest 99.94%. 2 of 12 runs had unexplained differences. " +
        "1 of 12 runs drifted from their route's earlier runs.",
    );
    assert.match(html, /role="img"/);
    assert.match(html, /aria-label="Parity over the last 12 runs/);
  });

  it("labels the min and max ticks at their scale positions", () => {
    // Height 180, padding top 18 / bottom 30: 100% sits at y=18, 99.5% at y=150.
    assert.match(html, /y="22"[^>]*>100%</);
    assert.match(html, /y="154"[^>]*>99\.5%</);
    assert.match(html, /<line class="df2-parity-grid"[^>]*y1="18"/);
  });

  it("breaks the line where a run has no figure and emphasises the last point", () => {
    const d = /class="df2-parity-trend-line" d="([^"]+)"/.exec(html)![1];
    assert.equal(d.split("M").length - 1, 2, "an unknown run must split the line in two");
    assert.equal((html.match(/df2-parity-point is-flagged/g) || []).length, 2);
    assert.match(html, /df2-parity-point is-flagged is-last/);
    assert.match(html, /<title>orders_daily · run 25: 99\.5% · 6 unexplained · drift: an outlier against this route&#x27;s earlier runs<\/title>/);
    noLiteralColours(html);
  });

  it("marks a drifting run with a word on its point, and only that run", () => {
    assert.equal((html.match(/class="df2-parity-drift"[^>]*>drift</g) || []).length, 1);
    // Run 25 sits at x=186 on the 360px default width.
    assert.match(html, /<text class="df2-parity-drift" x="186" /);
    const calm = renderToStaticMarkup(<ParityTrendChart runs={summary.runs.map((r) => ({ ...r, drift: null }))} />);
    assert.doesNotMatch(calm, /df2-parity-drift/);
    assert.doesNotMatch(trendSummary(summary.runs.map((r) => ({ ...r, drift: false }))), /drifted/);
  });

  it("says so when no run has a figure", () => {
    const empty = renderToStaticMarkup(
      <ParityTrendChart runs={summary.runs.map((r) => ({ ...r, proven_equal_pct: null }))} />,
    );
    assert.match(empty, /No parity figures for the last 12 runs yet\./);
  });
});

describe("TableRunHeatmap", () => {
  const summary = summaryFixture();
  const html = renderToStaticMarkup(<TableRunHeatmap heatmap={summary.heatmap} runs={summary.runs} />);

  it("names the drifting table in its summary", () => {
    assert.equal(heatmapSummary(summary.heatmap), "2 tables over 12 runs. Unexplained differences: payments in 2 runs.");
    assert.match(html, /aria-label="2 tables over 12 runs\. Unexplained differences: payments in 2 runs\."/);
  });

  it("draws one cell per table and run, with a legend and a non-colour mark", () => {
    assert.equal((html.match(/class="df2-parity-heat-cell /g) || []).length, 24);
    assert.equal((html.match(/is-unexplained-large/g) || []).length, 2); // cell + legend swatch
    assert.equal((html.match(/df2-parity-heat-mark/g) || []).length, 2);
    for (const label of ["Proven", "All explained", "Unexplained, small", "Unexplained, large", "Not checked"]) {
      assert.match(html, new RegExp(`</span>${label}</li>`));
    }
    assert.match(html, /<title>payments · orders_daily · run 25: Unexplained, large<\/title>/);
    noLiteralColours(html);
  });

  it("renders an unknown cell state as not checked instead of failing", () => {
    const odd = { ...summary.heatmap, cells: [summary.heatmap.cells[0], summary.heatmap.cells[1].map(() => "mystery" as never)] };
    const out = renderToStaticMarkup(<TableRunHeatmap heatmap={odd} />);
    assert.match(out, /is-none/);
    assert.match(out, /: Mystery<\/title>/);
  });
});
