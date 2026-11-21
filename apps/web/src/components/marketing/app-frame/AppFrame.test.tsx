import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, it } from "node:test";
import { EXAMPLE_JOBS, JobsScene, TrustDrawer, VERDICT_NAME, exampleJobKpis } from "./AppFrame.js";
import { EvidencePanel } from "../ProductPanels.js";
import { EXAMPLE_READ, EXAMPLE_STATEMENT } from "../StatementHero.js";
import { DriftReview } from "../StoryPanels.js";
import { McpScene, SchedulesScene } from "../StoryScenes.js";

const here = dirname(fileURLToPath(import.meta.url));
const homeRun = EXAMPLE_JOBS.find((j) => j.id === "6af912d7");

describe("example workspace: one consistent story", () => {
  it("the home run is the statement's run: proven equal, finished with rows held", () => {
    assert.ok(homeRun);
    assert.equal(homeRun.trust.source, EXAMPLE_READ);
    assert.equal(homeRun.rows, EXAMPLE_STATEMENT.landed);
    assert.equal(homeRun.held, EXAMPLE_STATEMENT.held);
    assert.equal(homeRun.trust.source - homeRun.rows - (homeRun.held ?? 0), EXAMPLE_STATEMENT.skipped);
    assert.equal(homeRun.verdict, "proven");
    assert.equal(homeRun.trust.verdict, "Proven equal");
    assert.equal(homeRun.trust.check, "Full checksum");
    // Rows were held, so the run did not end as plain "completed".
    assert.equal(homeRun.state, "completed_with_quarantine");
  });

  it("only a full comparison with no difference is called proven", () => {
    for (const j of EXAMPLE_JOBS) {
      if (j.verdict !== "proven") continue;
      assert.equal(j.trust.differ, 0, j.id);
      assert.equal(j.trust.compared, j.trust.destination, j.id);
      assert.notEqual(j.trust.check, "Random sample", j.id);
    }
    for (const j of EXAMPLE_JOBS) assert.equal(j.trust.verdict, VERDICT_NAME[j.verdict]);
  });

  it("the Jobs KPIs are computed from the runs, not typed in", () => {
    const k = exampleJobKpis();
    assert.equal(k.proven, EXAMPLE_JOBS.filter((j) => j.verdict === "proven").length);
    assert.equal(k.total, EXAMPLE_JOBS.length);
    const markup = renderToStaticMarkup(createElement(JobsScene));
    assert.match(markup, new RegExp(`${k.proven} of ${k.total}`));
    assert.doesNotMatch(markup, /5 of 6/);
    assert.doesNotMatch(markup, /Need attention/);
    assert.match(markup, /Needs attention/);
    assert.match(markup, /Quarantined/);
  });

  it("the Trust Report never calls a sample proven", () => {
    const sampled = EXAMPLE_JOBS.find((j) => j.verdict === "no_difference_found");
    assert.ok(sampled);
    const markup = renderToStaticMarkup(createElement(TrustDrawer, { job: sampled }));
    assert.doesNotMatch(markup, /proven/i);
    assert.match(markup, /rows sampled/);
  });

  it("the sign-off panel gives the home run the same verdict as everywhere else", () => {
    const markup = renderToStaticMarkup(createElement(EvidencePanel));
    assert.match(markup, /48,213,906/);
    assert.match(markup, /Proven equal/);
    assert.doesNotMatch(markup, /Explained/);
  });

  it("a paused schedule shows no runs after the pause until the mapping is approved", () => {
    const drift = renderToStaticMarkup(createElement(DriftReview));
    assert.match(drift, /08:26/);
    assert.doesNotMatch(drift, /12:0[579]/);
    const scene = renderToStaticMarkup(createElement(SchedulesScene));
    assert.match(scene, /No run writes until/);
    assert.doesNotMatch(scene, /12:0[79]/);
  });

  it("the MCP server URL uses the same host as the window", () => {
    const markup = renderToStaticMarkup(createElement(McpScene));
    assert.match(markup, /https:\/\/app\.datawrap\.io\/api\/v1\/mcp/);
    assert.doesNotMatch(markup, /www\.datawrap\.io/);
  });
});

describe("app frame legibility", () => {
  it("no frame text is set under 12px, and none is scaled down", () => {
    const css = readFileSync(join(here, "../../../styles/marketing-appframe.css"), "utf8");
    const small = [...css.matchAll(/font-size:\s*(\d+(?:\.\d+)?)px/g)].map((m) => Number(m[1])).filter((n) => n < 12);
    assert.deepEqual(small, []);
    const src = readFileSync(join(here, "AppFrame.tsx"), "utf8");
    const appWindow = src.slice(src.indexOf("export function AppWindow"), src.indexOf("/* ── Example data"));
    assert.doesNotMatch(appWindow, /scale\(/);
  });
});
