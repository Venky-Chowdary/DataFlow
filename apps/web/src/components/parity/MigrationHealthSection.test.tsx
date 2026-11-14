/**
 * Dashboard → Migration health (Page B): KPIs, trend and heatmap from the
 * summary, a plain empty state, and an error that says what to do.
 */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { describe, it } from "node:test";
import { renderToStaticMarkup } from "react-dom/server";
import { MigrationHealthSection, MigrationHealthView } from "./MigrationHealthSection.js";
import { summaryFixture } from "./parityFixtures.js";

const here = path.dirname(fileURLToPath(import.meta.url));

function text(html: string): string {
  return html.replace(/<[^>]+>/g, " ").replace(/&#x27;/g, "'").replace(/\s+/g, " ");
}

describe("Migration health section", () => {
  it("shows the KPIs, the parity trend and the tables × runs heatmap", () => {
    const html = renderToStaticMarkup(
      <MigrationHealthView state="ready" summary={summaryFixture()} limit={30} onOpenJob={() => {}} />,
    );
    const t = text(html);
    assert.match(t, /Migration health/);
    assert.match(t, /Last 30 runs/);
    assert.match(t, /Runs fully proven 96\.4%/);
    assert.match(t, /Unexplained differences 2/);
    assert.match(t, /Failed verification 1/);
    assert.match(t, /Sign-offs 11/);
    assert.match(t, /Most-drifting table: payments/);
    assert.match(t, /Parity trend/);
    assert.match(t, /Tables × runs/);
    assert.match(html, /aria-label="Parity over the last 12 runs/);
    assert.match(html, /aria-label="2 tables over 12 runs\. Unexplained differences: payments in 2 runs\."/);
    assert.match(t, /Open the latest run's trust report/);
  });

  it("asks for a first transfer when there are no runs", () => {
    const html = renderToStaticMarkup(
      <MigrationHealthView
        state="ready"
        summary={summaryFixture({ runs: [], heatmap: { tables: [], job_ids: [], cells: [] } })}
        limit={30}
        onStartTransfer={() => {}}
      />,
    );
    const t = text(html);
    assert.match(t, /Run a transfer to see parity here\./);
    assert.match(t, /Start a transfer/);
    assert.doesNotMatch(t, /Runs fully proven/);
  });

  it("says what went wrong and offers a retry", () => {
    const t = text(
      renderToStaticMarkup(
        <MigrationHealthView
          state="error"
          summary={null}
          error="Could not load migration health. The server reported an error (HTTP 503). Try again in a moment."
          limit={30}
          onRetry={() => {}}
        />,
      ),
    );
    assert.match(t, /HTTP 503\)\. Try again in a moment\./);
    assert.match(t, /Try again/);
  });

  it("starts in a loading state", () => {
    assert.match(text(renderToStaticMarkup(<MigrationHealthSection />)), /Loading migration health/);
  });

  it("is on the Overview page and opens a run on its Trust Report tab", () => {
    const dashboard = readFileSync(path.join(here, "..", "..", "pages", "DashboardPage.tsx"), "utf8");
    assert.match(dashboard, /<MigrationHealthSection/);
    assert.match(dashboard, /onOpenJob\(jobId, "trust-report"\)/);
    const jobs = readFileSync(path.join(here, "..", "..", "pages", "JobsPage.tsx"), "utf8");
    assert.match(jobs, /focusPanel === "trust-report"/);
    assert.match(jobs, /\{ id: "trust" as const, label: "Trust Report"/);
    assert.match(jobs, /<TrustReportTab/);
  });
});

describe("Migration health KPI row (W6)", () => {
  const html = renderToStaticMarkup(
    <MigrationHealthView state="ready" summary={summaryFixture()} limit={30} onOpenJob={() => {}} />,
  );

  it("is one row of four stat tiles, each a label, a value and a caption or sparkline", () => {
    const row = /<dl class="df2-stat-tiles df2-parity-health-kpis" aria-label="[^"]+">([\s\S]*?)<\/dl>/.exec(html);
    assert.ok(row, "one stat-tile row");
    const tiles = row![1].match(/<div class="df2-stat-tile"/g) || [];
    assert.equal(tiles.length, 4);
    assert.equal((row![1].match(/df2-stat-tile-label/g) || []).length, 4);
    assert.equal((row![1].match(/df2-stat-tile-value/g) || []).length, 4);
    assert.equal((row![1].match(/df2-stat-tile-foot/g) || []).length, 4);
    assert.ok((row![1].match(/<svg class="df2-spark/g) || []).length >= 2, "proven and unexplained carry a sparkline");
    // The most-drifting table is a callout with a link, not a fifth tile.
    assert.doesNotMatch(row![1], /drifting/i);
    assert.match(html, /class="df2-parity-drift-callout"[\s\S]*Most-drifting table:[\s\S]*<button[^>]*>Open its latest drifting run/);
  });

  it("status is a small dot, never a coloured tile background", () => {
    assert.match(html, /data-tone="warn"[\s\S]*?df2-stat-tile-dot/);
    assert.doesNotMatch(html, /style="[^"]*background/);
  });
});
