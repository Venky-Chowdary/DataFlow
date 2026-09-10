import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, it } from "node:test";

const stylesDir = dirname(fileURLToPath(import.meta.url));

function sheet(name: string): string {
  return readFileSync(join(stylesDir, name), "utf8");
}

describe("shared alert / jobs / studio surfaces use tokens", () => {
  it("canonical alert-error and alert-info read danger/brand tokens", () => {
    const ui = sheet("enterprise-ui.css");
    const dataflow = sheet("dataflow-ui.css");
    const premium = sheet("premium-theme.css");
    const platform = sheet("enterprise-platform.css");
    const combined = [ui, dataflow, premium, platform].join("\n");

    assert.match(combined, /\.df2-alert-error[\s\S]{0,280}--df-danger-bg/);
    assert.match(combined, /\.df2-alert-error[\s\S]{0,280}--df-danger-border/);
    assert.match(combined, /\.df2-alert-info[\s\S]{0,280}--df-brand-muted/);
    assert.doesNotMatch(dataflow, /\.df2-alert-error[\s\S]{0,200}#fef2f2/);
    assert.doesNotMatch(platform, /\.df2-alert-error[\s\S]{0,200}#fef2f2/);
    assert.doesNotMatch(premium, /\.df2-alert-error[\s\S]{0,200}#fef2f2/);
    assert.doesNotMatch(platform, /\.df2-alert-info[\s\S]{0,200}#eff6ff/);
  });

  it("jobs warn chips and dest plan-callout warn use warning tokens", () => {
    const ui = sheet("enterprise-ui.css");
    const studio = sheet("transfer-studio.css");
    assert.match(
      ui,
      /\.df2-jobs-v3-summary-metrics article\.is-metric-quarantine\.is-warn[\s\S]{0,220}--df-warning-bg/,
    );
    assert.match(ui, /\.df2-result-metric\.is-warn[\s\S]{0,220}--df-warning-bg/);
    assert.match(studio, /\.df2-plan-callout\.is-warn[\s\S]{0,220}--df-warning-bg/);
    assert.match(ui, /\.is-metric-dest\.is-warn[\s\S]{0,160}--df-warning-bg/);
    assert.match(ui, /\.is-metric-writer\.is-warn[\s\S]{0,160}--df-warning-bg/);
    assert.doesNotMatch(
      ui,
      /\.df2-jobs-v3-summary-metrics article\.is-metric-quarantine\.is-warn[\s\S]{0,160}#fffbeb/,
    );
    assert.doesNotMatch(studio, /\.df2-plan-callout\.is-warn[\s\S]{0,160}#fffbeb/);
    assert.doesNotMatch(ui, /\.is-metric-dest\.is-warn[\s\S]{0,120}#fffbeb/);
  });

  it("indigo leftover pulse-dot is gone from premium-theme", () => {
    const premium = sheet("premium-theme.css");
    assert.match(premium, /\.df2-pulse-dot[\s\S]{0,80}--df-brand/);
    assert.doesNotMatch(premium, /#2563eb/);
  });

  it("Transfer Source upload and kind tiles do not flatten to #fff", () => {
    const platform = sheet("enterprise-platform.css");
    const polish = sheet("shell-polish.css");
    const studio = sheet("transfer-studio.css");
    assert.match(platform, /\.df2-upload \{[\s\S]{0,220}--df-surface/);
    assert.doesNotMatch(platform, /\.df2-upload \{[\s\S]{0,220}#ffffff !important/);
    assert.match(polish, /\.df2-source-kind-tile \{[\s\S]{0,280}--df-surface/);
    assert.doesNotMatch(polish, /\.df2-source-kind-tile \{[\s\S]{0,280}linear-gradient\(180deg, #fff/);
    assert.match(polish, /\.df2-structure-preview \{[\s\S]{0,220}--df-surface-muted/);
    assert.match(studio, /\.df2-dest-connector-card \{[\s\S]{0,420}--df-surface/);
    assert.doesNotMatch(studio, /\.df2-dest-connector-card \{[\s\S]{0,420}background: #fff !important/);
    assert.match(studio, /\.df2-source-aside \{[\s\S]{0,280}--df-surface/);
    assert.doesNotMatch(studio, /\.df2-source-aside \{[\s\S]{0,280}linear-gradient\(180deg, #fafbfc/);
  });

  it("studio wizard chrome does not flatten to #fff", () => {
    const studio = sheet("transfer-studio.css");
    const ui = sheet("enterprise-ui.css");
    const premium = sheet("premium-theme.css");
    assert.match(studio, /\.df2-transfer-studio-chrome \{[\s\S]{0,220}--df-surface/);
    assert.doesNotMatch(studio, /\.df2-transfer-studio-chrome \{[\s\S]{0,240}#ffffff/);
    assert.doesNotMatch(studio, /\.df2-transfer-studio-chrome \{[\s\S]{0,200}background: #fff;/);
    assert.match(ui, /\.df2-transfer-studio-chrome \{[\s\S]{0,200}--df-surface/);
    assert.doesNotMatch(ui, /\.df2-transfer-studio-chrome \{[\s\S]{0,240}255, 255, 255/);
    assert.match(ui, /\.df2-route-bar \{[\s\S]{0,220}--df-surface/);
    assert.match(premium, /\.df2-wizard \{[\s\S]{0,280}--df-surface/);
    assert.doesNotMatch(premium, /\.df2-wizard \{[\s\S]{0,280}linear-gradient\(180deg, #fff/);
    assert.match(studio, /\.df2-wizard-studio \.df2-wizard-step\.active \{[\s\S]{0,80}--df-brand-muted/);
  });
});
