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
    assert.match(unknown, /aria-label="Share of rows proven equal is unknown \(not compared\)"/);
    assert.match(unknown, /tone-idle/);
  });

  it("without a figure: a compact dash and \"not compared\", never a sampled bound (trust-report polish)", () => {
    for (const html of [
      renderToStaticMarkup(<ParityRing pct={null} />),
      // The report may call it a lower bound; with no figure there is no bound to state.
      renderToStaticMarkup(<ParityRing pct={null} lowerBound />),
    ]) {
      assert.match(html, /class="df2-parity-ring-value">—</);
      assert.match(html, /class="df2-parity-ring-caption">not compared</);
      assert.doesNotMatch(html, />Unknown</, "a word does not fit inside the ring");
      assert.doesNotMatch(html, /sampled bound/);
      assert.doesNotMatch(html, /At least/);
    }
    // The caller can say why there is no figure when a comparison did run.
    const undecided = renderToStaticMarkup(<ParityRing pct={null} caption="not determined" />);
    assert.match(undecided, /class="df2-parity-ring-caption">not determined</);
  });
});

describe("ParityRing on a sampled check (review finding 9)", () => {
  it("reads at least, and says the figure is a sampled bound", () => {
    const html = renderToStaticMarkup(<ParityRing pct={99.85} lowerBound />);
    assert.match(html, />≥ 99\.85%</);
    assert.match(html, />rows equal \(sampled bound\)</);
    assert.match(html, /aria-label="At least 99\.85% of rows equal, a 95% confidence bound from a sample"/);
    assert.doesNotMatch(html, /proven equal/);
  });

  it("a full compare keeps the exact figure", () => {
    const html = renderToStaticMarkup(<ParityRing pct={99.85} />);
    assert.match(html, />99\.85%</);
    assert.doesNotMatch(html, /≥/);
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
    // Unexplained cells carry a dot; the legend repeats each state's shape.
    assert.equal((html.match(/df2-parity-heat-mark/g) || []).length, 2 + 2);
    for (const label of ["Proven", "All explained", "Unexplained, small", "Unexplained, large", "Not checked"]) {
      assert.match(html, new RegExp(`</svg>${label}</li>`));
    }
    assert.match(html, /<title>payments · orders_daily · run 25: Unexplained, large<\/title>/);
    noLiteralColours(html);
  });

  it("gives every state its own shape, never colour alone (review U3)", () => {
    const cells = summary.heatmap.cells.flat();
    const glyphs = (cls: string) => (html.match(new RegExp(`df2-parity-heat-glyph is-${cls}"`, "g")) || []).length;
    // One shape per cell of that state, plus one in the legend.
    assert.equal(glyphs("proven"), cells.filter((c) => c === "proven").length + 1);
    assert.equal(glyphs("explained"), cells.filter((c) => c === "explained").length + 1);
    assert.equal(glyphs("none"), cells.filter((c) => c === "none").length + 1);
    assert.equal(glyphs("sampled"), 1);
  });

  it("offers the cells as a table a screen reader can walk (review U3)", () => {
    assert.match(html, /aria-describedby="([^"]+)"/);
    const id = /aria-describedby="([^"]+)"/.exec(html)![1];
    assert.match(html, new RegExp(`<table id="${id.replace(/[:]/g, "\\:")}" class="df2-sr-only">`));
    assert.match(html, /<caption>Parity per table and run<\/caption>/);
    assert.match(html, /<th scope="row">payments<\/th>/);
    assert.match(html, /<td>Unexplained, large<\/td>/);
  });

  it("renders an unknown cell state as not checked instead of failing", () => {
    const odd = { ...summary.heatmap, cells: [summary.heatmap.cells[0], summary.heatmap.cells[1].map(() => "mystery" as never)] };
    const out = renderToStaticMarkup(<TableRunHeatmap heatmap={odd} />);
    assert.match(out, /is-none/);
    assert.match(out, /: Mystery<\/title>/);
  });
});

describe("a sample that bounds nothing (re-review 3, H4)", () => {
  it("rings rows checked of the total, never a percentage", () => {
    const html = renderToStaticMarkup(<ParityRing pct={null} checked={{ checked: 2000, total: 1_000_000 }} />);
    assert.match(html, /aria-label="2,000 of 1,000,000 rows checked; the other rows were not compared"/);
    assert.match(html, />2,000</);
    assert.doesNotMatch(html, /%/);
    noLiteralColours(html);
  });

  it("draws a sampled run as a hollow baseline mark, not as a value", () => {
    const base = summaryFixture();
    const runs = base.runs.map((r, i) =>
      i === base.runs.length - 1
        ? { ...r, proven_equal_pct: null, method: "sampled" as const, rows_checked: 50, rows_total: 900_000 }
        : { ...r, method: "full" as const },
    );
    const html = renderToStaticMarkup(<ParityTrendChart runs={runs} />);
    assert.match(html, /is-sampled/);
    assert.match(html, /50 of 900,000 rows checked; not compared in full, so no figure/);
    assert.match(trendSummary(runs), /1 of \d+ runs were only sampled and have no figure/);
  });
});

describe("a run Gate-8 failed (re-review 4, finding 3)", () => {
  const base = summaryFixture();
  const cells = [base.heatmap.cells[0].map((c, i) => (i === 4 ? ("failed" as const) : c)), base.heatmap.cells[1]];
  const heatmap = { ...base.heatmap, cells };
  const html = renderToStaticMarkup(<TableRunHeatmap heatmap={heatmap} runs={base.runs} />);

  it("draws a failed-verification cell with its own shape and legend, not as not checked", () => {
    assert.match(html, /<title>orders · orders_daily · run 24: Failed verification<\/title>/);
    assert.equal((html.match(/df2-parity-heat-glyph is-failed"/g) || []).length, 1 + 1);
    assert.match(html, /<\/svg>Failed verification<\/li>/);
    noLiteralColours(html);
  });

  it("names the failed runs in the summary", () => {
    assert.equal(
      heatmapSummary(heatmap),
      "2 tables over 12 runs. Unexplained differences: payments in 2 runs. Failed verification: orders in 1 run.",
    );
  });
});
