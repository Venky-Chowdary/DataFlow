/**
 * UX-16: charts are images with a text alternative (svg-img-alt).
 * UX-35: the throughput y-axis reads 0 at the bottom and the maximum at the
 * top; an empty chart labels only its zero line.
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { StatusDonut, ThroughputChart } from "./OverviewCharts.js";

function axisLabels(html: string): { y: number; text: string }[] {
  return [...html.matchAll(/<text x="[^"]+" y="([\d.]+)" class="df2-chart-axis"[^>]*>([^<]*)<\/text>/g)].map((m) => ({
    y: Number(m[1]),
    text: m[2],
  }));
}

const week = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];

describe("throughput chart", () => {
  it("puts zero at the bottom and the maximum at the top", () => {
    const html = renderToStaticMarkup(
      createElement(ThroughputChart, { series: week.map((label, i) => ({ label, rows: i === 3 ? 15000 : 0 })) as never }),
    );
    const labels = axisLabels(html).sort((a, b) => b.y - a.y); // bottom first
    assert.deepEqual(labels.map((l) => l.text), ["0", "7.5k", "15.0k"]);
  });

  it("labels only the zero line when nothing has run", () => {
    const html = renderToStaticMarkup(createElement(ThroughputChart, { series: week.map((label) => ({ label, rows: 0 })) as never }));
    assert.deepEqual(axisLabels(html).map((l) => l.text), ["0"]);
  });

  it("describes the data for screen readers", () => {
    const html = renderToStaticMarkup(
      createElement(ThroughputChart, { series: week.map((label, i) => ({ label, rows: i * 10 })) as never }),
    );
    assert.match(html, /<svg[^>]*role="img"[^>]*aria-label="Rows written per day, last 7 days: Mon 0, Tue 10/);
  });

  it("gives the status donut a text alternative", () => {
    const html = renderToStaticMarkup(
      createElement(StatusDonut, {
        slices: [{ key: "ok", label: "Completed", count: 3, color: "green" }] as never,
        centerLabel: "Jobs",
        centerValue: "3",
      }),
    );
    assert.match(html, /<svg[^>]*role="img"[^>]*aria-label="Jobs: 3\. Completed 3"/);
  });
});
