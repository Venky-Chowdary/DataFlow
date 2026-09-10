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

  it("Validate / Map / theater hosts do not flatten to #fff", () => {
    const ui = sheet("enterprise-ui.css");
    const studio = sheet("transfer-studio.css");
    const map = sheet("column-workbench.css");

    assert.match(ui, /\.df2-validate-dashboard-host,\s*\n\.df2-validate-step \{\s*\n\s*background: var\(--df-surface\)/);
    assert.doesNotMatch(ui, /\.df2-validate-dashboard-host,\s*\n\.df2-validate-step \{\s*\n\s*background: #fff;/);
    assert.match(ui, /\.df2-run-step \.df2-theater-v3,[\s\S]{0,280}--df-surface/);
    assert.doesNotMatch(ui, /\.df2-run-step \.df2-theater-v3,[\s\S]{0,280}background: #fff;/);
    assert.match(ui, /\.df2-theater-v3-metric\.is-warn[\s\S]{0,160}--df-warning-bg/);
    assert.match(ui, /\.df2-theater-v3-metric\.is-danger[\s\S]{0,160}--df-danger-bg/);
    assert.doesNotMatch(ui, /\.df2-theater-v3-metric\.is-warn[\s\S]{0,120}#fffbeb/);
    assert.match(
      ui,
      /\.df2-page-jobs \.df2-jobs-v3-detail,[\s\S]{0,160}--df-surface/,
    );
    assert.doesNotMatch(
      ui,
      /\.df2-page-jobs \.df2-jobs-v3-detail,[\s\S]{0,160}linear-gradient\(180deg, #fff/,
    );

    assert.match(studio, /\.df2-validate-rail-panel \{[\s\S]{0,160}--df-surface/);
    assert.doesNotMatch(studio, /\.df2-validate-rail-panel \{[\s\S]{0,160}background: #fff;/);
    assert.match(studio, /\.df2-validate-rail-panel\.review[\s\S]{0,80}--df-warning-bg/);
    assert.match(studio, /\.df2-vd-ring \{[\s\S]{0,200}--df-surface/);
    assert.doesNotMatch(studio, /\.df2-vd-ring \{[\s\S]{0,200}background: #ffffff;/);
    assert.match(studio, /\.df2-vd-rules \{[\s\S]{0,120}--df-surface/);
    assert.match(studio, /\.df2-theater-v3-sla-card \{[\s\S]{0,180}--df-surface/);
    assert.doesNotMatch(studio, /\.df2-theater-v3-sla-card \{[\s\S]{0,180}background: #ffffff;/);

    assert.match(map, /\.df2-column-review-editor \{[\s\S]{0,220}--df-surface/);
    assert.doesNotMatch(map, /\.df2-column-review-editor \{[\s\S]{0,220}background: #fff;/);
    assert.match(map, /\.df2-map-step-metric \{[\s\S]{0,200}--df-surface/);
  });

  it("Validate inner proof cards do not flatten to #fff", () => {
    const studio = sheet("transfer-studio.css");
    assert.match(studio, /\.df2-vd-diff \{[\s\S]{0,120}--df-surface/);
    assert.doesNotMatch(studio, /\.df2-vd-diff \{[\s\S]{0,120}background: #fff;/);
    assert.match(studio, /\.df2-vd-coerce \{[\s\S]{0,120}--df-surface/);
    assert.doesNotMatch(studio, /\.df2-vd-coerce \{[\s\S]{0,120}background: #fff;/);
    assert.match(studio, /\.df2-vd-chip \{[\s\S]{0,220}--df-surface/);
    assert.doesNotMatch(studio, /\.df2-vd-chip \{[\s\S]{0,220}background: #ffffff;/);
    assert.doesNotMatch(studio, /\.df2-vd-chip \.dt-nav-icon \{[^}]*#2563eb/);
    assert.match(studio, /\.df2-vd-explain-issues li \{[\s\S]{0,140}--df-surface/);
    assert.match(studio, /\.df2-vd-explain-issues li\.sev-warning[\s\S]{0,80}--df-warning-bg/);
    assert.match(studio, /\.df2-vd-explain-issues li\.sev-block[\s\S]{0,80}--df-danger-bg/);
    assert.match(studio, /\.df2-vd-decision-path-steps li \{[\s\S]{0,180}--df-surface/);
    assert.match(studio, /\.df2-vd-decision-path\.is-blocked[\s\S]{0,80}--df-warning-bg/);
    assert.match(studio, /\.df2-vd-hs-metric \{[\s\S]{0,180}--df-surface/);
    assert.match(studio, /\.df2-vd-coerce-row\.sev-warn \{[^}]*--df-warning-bg/);
    assert.match(studio, /\.df2-vd-map-proof-kpis > div \{[\s\S]{0,120}--df-surface/);
  });
});
