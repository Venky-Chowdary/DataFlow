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

  it("Jobs / modal / drawer / overview chrome do not flatten to #fff", () => {
    const ui = sheet("enterprise-ui.css");
    const polish = sheet("shell-polish.css");

    assert.match(ui, /\.df2-jobs-v3-list \{[\s\S]{0,280}--df-surface/);
    assert.doesNotMatch(ui, /\.df2-jobs-v3-list \{[\s\S]{0,280}background: #fff;/);
    assert.match(ui, /\.df2-job-row \{[\s\S]{0,420}--df-surface/);
    assert.doesNotMatch(ui, /\.df2-job-row \{[\s\S]{0,420}background: #fff;/);
    assert.match(ui, /\.df2-job-row\.is-active \{[\s\S]{0,120}--df-brand-soft/);
    assert.match(ui, /\.df2-modal \{[\s\S]{0,160}--df-surface/);
    assert.doesNotMatch(ui, /\.df2-modal \{[\s\S]{0,160}background: #fff;/);
    assert.match(ui, /\.df2-drawer \{[\s\S]{0,180}--df-surface/);
    assert.doesNotMatch(ui, /\.df2-drawer \{[\s\S]{0,180}background: #fff;/);
    assert.match(
      ui,
      /\.df2-modal-header \{[\s\S]{0,240}--df-surface/,
    );
    assert.doesNotMatch(ui, /\.df2-modal-header \{[\s\S]{0,240}linear-gradient\(180deg, #fff/);
    assert.match(
      ui,
      /\.df2-drawer-header \{[\s\S]{0,240}--df-surface/,
    );
    assert.doesNotMatch(ui, /\.df2-drawer-header \{[\s\S]{0,240}linear-gradient\(180deg, #fff/);
    assert.match(
      ui,
      /\.df2-overview-enterprise \.df2-glass-panel-head \{[\s\S]{0,160}--df-surface/,
    );
    assert.doesNotMatch(
      ui,
      /\.df2-overview-enterprise \.df2-glass-panel-head \{[\s\S]{0,160}linear-gradient\(180deg, #fff/,
    );
    assert.match(ui, /\.df2-overview-ops-chip \{[\s\S]{0,220}--df-surface/);

    assert.match(polish, /\.df2-connector-card \{[\s\S]{0,220}--df-surface/);
    assert.doesNotMatch(polish, /\.df2-connector-card \{[\s\S]{0,220}background: #fff;/);
    assert.match(polish, /\.df2-app \.df2-glass-panel,[\s\S]{0,80}--df-surface/);
    assert.doesNotMatch(polish, /\.df2-app \.df2-glass-panel,[\s\S]{0,80}#fff !important/);
  });

  it("Query / Docs / Settings / MCP hosts do not flatten to #fff", () => {
    const ui = sheet("enterprise-ui.css");
    const mcp = sheet("mcp-enterprise.css");
    const settings = sheet("settings-enterprise.css");
    const docs = sheet("docs-page.css");
    const query = sheet("query-playground.css");

    assert.match(
      ui,
      /\.df2-page-docs \.df2-docs-panel \{[\s\S]{0,160}--df-surface/,
    );
    assert.doesNotMatch(
      ui,
      /\.df2-page-pipelines \.df2-glass-panel,[\s\S]{0,200}background: #fff;/,
    );
    assert.match(ui, /\.df2-mcp-endpoint-card \{[\s\S]{0,360}--df-surface/);
    assert.doesNotMatch(ui, /\.df2-mcp-endpoint-card \{[\s\S]{0,360}linear-gradient\(180deg, #f0fdfa/);
    assert.match(ui, /\.df2-chart-placeholder-caption \{[\s\S]{0,280}--df-surface/);
    assert.doesNotMatch(ui, /\.df2-chart-placeholder-caption \{[\s\S]{0,280}255, 255, 255/);

    assert.match(mcp, /\.df2-mcp-panel \{[\s\S]{0,140}--df-surface/);
    assert.doesNotMatch(mcp, /\.df2-mcp-panel \{[\s\S]{0,140}background: #fff;/);
    assert.match(mcp, /\.df2-mcp-hero \{[\s\S]{0,320}--df-surface/);
    assert.match(settings, /\.df2-settings-summary-item \{[\s\S]{0,160}--df-surface/);
    assert.doesNotMatch(settings, /\.df2-settings-summary-item \{[\s\S]{0,160}background: #fff;/);
    assert.match(settings, /\.df2-settings-log-level--warn \{[\s\S]{0,80}--df-warning-bg/);
    assert.match(docs, /\.df2-page-docs \.df2-docs-architecture \{[\s\S]{0,80}--df-surface/);
    assert.match(docs, /\.docs-space \{[\s\S]{0,520}--df-surface/);
    assert.match(docs, /\.docs-space-main \{[\s\S]{0,180}--df-surface/);
    assert.doesNotMatch(docs, /\.docs-space-main \{[\s\S]{0,180}background: #fff;/);
    assert.match(query, /\.df2-query-editor-action \{[\s\S]{0,200}--df-surface/);
  });

  it("Docs inner cards and schedule chrome do not flatten to #fff", () => {
    const docs = sheet("docs-page.css");
    const ui = sheet("enterprise-ui.css");

    assert.match(docs, /\.docs-card,[\s\S]{0,200}--df-surface/);
    assert.doesNotMatch(docs, /\.docs-card,[\s\S]{0,200}background: #fff;/);
    assert.match(docs, /\.docs-faq-item \{[\s\S]{0,160}--df-surface/);
    assert.doesNotMatch(docs, /\.docs-faq-item \{[\s\S]{0,160}background: #fff;/);
    assert.match(docs, /\.docs-sidebar \{[\s\S]{0,480}--df-surface/);
    assert.match(docs, /\.docs-search-results \{[\s\S]{0,160}--df-surface/);
    assert.match(docs, /\.docs-toc \{[\s\S]{0,160}--df-surface/);
    assert.match(docs, /\.docs-space-start \{[\s\S]{0,220}--df-surface/);
    assert.doesNotMatch(docs, /\.docs-space-start \{[\s\S]{0,220}#fff 100%/);
    assert.match(docs, /\.docs-space-algorithm \{[\s\S]{0,200}--df-surface/);
    assert.doesNotMatch(docs, /\.docs-space-algorithm \{[\s\S]{0,200}#fff,/);
    assert.match(docs, /\.docs-sidebar--space \{[\s\S]{0,280}--df-surface-muted/);
    assert.doesNotMatch(docs, /\.docs-sidebar--space \{[\s\S]{0,280}background: #f8fafc;/);
    assert.match(docs, /\.docs-toc--inline \{[\s\S]{0,160}--df-surface-muted/);

    const layout = sheet("marketing-layout.css");
    assert.match(layout, /\.docs-space-algorithm-steps li \{[\s\S]{0,280}--df-surface/);
    assert.doesNotMatch(layout, /\.docs-space-algorithm-steps li \{[\s\S]{0,280}background: #fff !important;/);
    assert.match(layout, /\.docs-space-start \{[\s\S]{0,240}--df-surface/);
    assert.doesNotMatch(layout, /\.docs-space-start \{[\s\S]{0,240}#fff 72%/);

    const editorial = sheet("marketing-editorial.css");
    assert.match(editorial, /\.docs-space \{[\s\S]{0,280}--df-surface/);
    assert.doesNotMatch(editorial, /\.docs-space \{[\s\S]{0,280}background: #fff;/);
    assert.match(editorial, /\.docs-space-algorithm \{[\s\S]{0,240}--df-surface-muted/);
    assert.doesNotMatch(editorial, /\.docs-space-algorithm \{[\s\S]{0,240}#f8fafc;/);
    assert.match(editorial, /\.docs-space-algorithm-steps li \{[\s\S]{0,80}--df-surface/);
    assert.doesNotMatch(editorial, /\.docs-space-algorithm-steps li \{[\s\S]{0,80}background: #fff;/);
    assert.match(editorial, /\.docs-sidebar--space \{[\s\S]{0,280}--df-surface-muted/);

    assert.match(ui, /\.df2-pipe-card \{[\s\S]{0,280}--df-surface/);
    assert.doesNotMatch(ui, /\.df2-pipe-card \{[\s\S]{0,280}background: #fff;/);
    assert.match(ui, /\.df2-pipe-card\.is-active \{[\s\S]{0,160}--df-surface/);
    assert.doesNotMatch(ui, /\.df2-pipe-card\.is-active \{[\s\S]{0,160}#fff 70%/);
    assert.match(ui, /\.df2-sched-toggle \{[\s\S]{0,160}--df-surface/);
    assert.doesNotMatch(ui, /\.df2-sched-toggle \{[\s\S]{0,160}background: #fff;/);
    assert.match(ui, /\.df2-sched-seg \{[\s\S]{0,160}--df-surface/);
    assert.match(ui, /\.df2-sched-switch-row \{[\s\S]{0,200}--df-surface/);
    assert.doesNotMatch(ui, /\.df2-sched-switch-row \{[\s\S]{0,200}background: #fff;/);
    assert.match(ui, /\.df2-sched-run \{[\s\S]{0,240}--df-surface/);
    assert.doesNotMatch(ui, /\.df2-sched-run \{[\s\S]{0,240}background: #fff;/);
    assert.match(ui, /\.df2-pipeline-rows-head \{[\s\S]{0,200}--df-surface-muted/);
    assert.doesNotMatch(ui, /\.df2-pipeline-rows-head \{[\s\S]{0,200}--df-surface-subtle/);
    assert.match(ui, /\.df2-contract-rows-head \{[\s\S]{0,200}--df-surface-muted/);
    assert.doesNotMatch(ui, /\.df2-contract-rows-head \{[\s\S]{0,200}--df-surface-subtle/);
  });

  it("Trust score, ledger, popover, and quarantine dialog do not flatten to #fff", () => {
    const ui = sheet("enterprise-ui.css");

    assert.match(ui, /\.df2-status-popover \{[\s\S]{0,240}--df-surface/);
    assert.doesNotMatch(ui, /\.df2-status-popover \{[\s\S]{0,240}background: #fff;/);
    assert.match(ui, /\.df2-status-popover-icon\.warn \{[\s\S]{0,80}--df-warning-bg/);
    assert.doesNotMatch(ui, /\.df2-status-popover-icon\.warn \{[\s\S]{0,80}#fffbeb/);

    assert.match(ui, /\.df2-trust-score \{[\s\S]{0,200}--df-surface/);
    assert.doesNotMatch(ui, /\.df2-trust-score \{[\s\S]{0,200}background: #fff;/);
    assert.match(ui, /\.df2-trust-score\.is-ok \{[\s\S]{0,160}--df-success-bg/);
    assert.match(ui, /\.df2-trust-score\.is-warn \{[\s\S]{0,160}--df-warning-bg/);
    assert.match(ui, /\.df2-trust-score\.is-danger \{[\s\S]{0,160}--df-danger-bg/);
    assert.doesNotMatch(ui, /\.df2-trust-score\.is-warn \{[\s\S]{0,160}%, white\)/);

    assert.match(ui, /\.df2-conservation-ledger \{[\s\S]{0,200}--df-surface/);
    assert.doesNotMatch(ui, /\.df2-conservation-ledger \{[\s\S]{0,200}background: #fff;/);
    assert.match(ui, /\.df2-conservation-ledger\.is-ok \{[\s\S]{0,160}--df-success-bg/);
    assert.match(ui, /\.df2-conservation-ledger\.is-warn \{[\s\S]{0,160}--df-warning-bg/);
    assert.match(ui, /\.df2-conservation-ledger\.is-danger \{[\s\S]{0,160}--df-danger-bg/);
    assert.match(ui, /\.df2-conservation-ledger-compare article \{[\s\S]{0,200}--df-surface/);
    assert.doesNotMatch(ui, /\.df2-conservation-ledger-compare article \{[\s\S]{0,200}background: #fff;/);
    assert.match(ui, /\.df2-conservation-ledger-chips li \{[\s\S]{0,180}--df-surface/);

    assert.match(ui, /\.df2-quarantine-edit-dialog \{[\s\S]{0,200}--df-surface/);
    assert.doesNotMatch(ui, /\.df2-quarantine-edit-dialog \{[\s\S]{0,200}background: #fff;/);

    assert.match(ui, /\.df2-overview-attention \{[\s\S]{0,280}--df-warning-bg/);
    assert.match(ui, /\.df2-freshness-slo\.is-warn \{[\s\S]{0,160}--df-warning-bg/);
    assert.match(ui, /\.df2-freshness-slo\.is-critical \{[\s\S]{0,160}--df-danger-bg/);
  });

  it("Form controls and table cells do not flatten to #fff", () => {
    const dataflow = sheet("dataflow-ui.css");
    const polish = sheet("shell-polish.css");
    const premium = sheet("premium-theme.css");
    const ui = sheet("enterprise-ui.css");

    assert.match(dataflow, /\.df2-input,\s*\n\.df2-select \{[\s\S]{0,200}--df-surface/);
    assert.doesNotMatch(dataflow, /\.df2-input,\s*\n\.df2-select \{[\s\S]{0,200}background: #fff;/);

    assert.match(polish, /\.df2-input,\s*\n\.df2-textarea \{[\s\S]{0,120}--df-surface/);
    assert.doesNotMatch(polish, /\.df2-input,\s*\n\.df2-textarea \{[\s\S]{0,120}background: #fff !important;/);
    assert.match(polish, /background-color: var\(--df-surface\) !important;/);
    assert.doesNotMatch(polish, /background-color: #fff !important;/);
    assert.match(polish, /\.df2-table-search \{[\s\S]{0,280}--df-surface/);
    assert.doesNotMatch(polish, /\.df2-table-search \{[\s\S]{0,280}background: #fff;/);
    assert.match(polish, /\.df2-structure-table th \{[\s\S]{0,80}--df-surface-muted/);
    assert.match(polish, /\.df2-command-search:focus-within,[\s\S]{0,80}--df-surface/);
    assert.doesNotMatch(polish, /\.df2-command-search:focus-within,[\s\S]{0,80}background: #fff !important;/);
    assert.match(polish, /\.df2-system-pill \{[\s\S]{0,160}--df-surface/);
    assert.match(polish, /\.df2-system-pill\.degraded \{[\s\S]{0,80}--df-warning-bg/);

    assert.match(premium, /\.df2-command-search:focus-within \{[\s\S]{0,80}--df-surface/);
    assert.match(premium, /\.df2-system-pill\.degraded \{[\s\S]{0,80}--df-warning-bg/);

    assert.match(ui, /\.df2-quarantine-table td \{[\s\S]{0,80}--df-surface/);
    assert.doesNotMatch(ui, /\.df2-quarantine-table td \{[\s\S]{0,80}background: #ffffff;/);
    assert.match(ui, /\.df2-jobs-v3-quarantine \.df2-quarantine-table td,[\s\S]{0,160}--df-surface/);
    assert.doesNotMatch(ui, /\.df2-jobs-v3-quarantine \.df2-quarantine-table td,[\s\S]{0,160}background: #ffffff;/);
    assert.match(ui, /\.df2-app \.df2-quarantine-table th \{[\s\S]{0,80}--df-warning-bg/);
    assert.doesNotMatch(ui, /\.df2-app \.df2-quarantine-table th \{[\s\S]{0,80}#fef3c7/);
    assert.match(ui, /\.df2-app \.df2-quarantine-table tbody tr:hover td \{[\s\S]{0,80}--df-warning-bg/);
    assert.doesNotMatch(ui, /\.df2-app \.df2-quarantine-table tbody tr:hover td \{[\s\S]{0,80}#fffbeb/);
  });

  it("Theater, preflight, and rail panels do not flatten to #fff", () => {
    const polish = sheet("shell-polish.css");
    const studio = sheet("transfer-studio.css");

    assert.match(polish, /\.df2-theater \{[\s\S]{0,200}--df-surface/);
    assert.doesNotMatch(polish, /\.df2-theater \{[\s\S]{0,200}background: #fff !important;/);
    assert.match(polish, /\.df2-theater-log \{[\s\S]{0,80}--df-surface-muted/);
    assert.match(polish, /\.df2-preflight \{[\s\S]{0,200}--df-surface/);
    assert.doesNotMatch(polish, /\.df2-preflight \{[\s\S]{0,200}background: #fff !important;/);
    assert.match(polish, /\.df2-preflight\.passed \{[\s\S]{0,160}--df-surface/);
    assert.doesNotMatch(polish, /\.df2-preflight\.passed \{[\s\S]{0,160}#fff 100%/);
    assert.match(polish, /\.df2-preflight\.blocked \{[\s\S]{0,160}--df-warning-bg/);
    assert.match(polish, /\.df2-preflight-gate\.pass \{[\s\S]{0,80}--df-success-bg/);
    assert.match(polish, /\.df2-preflight-gate\.fail \{[\s\S]{0,80}--df-danger-bg/);
    assert.match(polish, /\.df2-rail-panel \{[\s\S]{0,200}--df-surface/);
    assert.doesNotMatch(polish, /\.df2-rail-panel \{[\s\S]{0,200}background: #fff !important;/);
    assert.match(polish, /\.df2-rail-stage \{[\s\S]{0,160}--df-surface-muted/);
    assert.match(polish, /\.df2-rail-stage\.warn \{[\s\S]{0,80}--df-warning-bg/);
    assert.match(polish, /\.df2-rail-alert \{[\s\S]{0,80}--df-warning-bg/);
    assert.match(polish, /\.df2-result-banner\.success \{[\s\S]{0,160}--df-surface/);
    assert.doesNotMatch(polish, /\.df2-result-banner\.success \{[\s\S]{0,160}#fff 100%/);

    assert.match(studio, /\.df2-run-step \.df2-theater-v3-metric \{[\s\S]{0,80}--df-surface/);
    assert.doesNotMatch(studio, /\.df2-run-step \.df2-theater-v3-metric \{[\s\S]{0,80}background: #ffffff;/);
  });

  it("Notify strip, coerce samples, and model cards do not flatten to #fff", () => {
    const studio = sheet("transfer-studio.css");
    const polish = sheet("shell-polish.css");
    const dataflow = sheet("dataflow-ui.css");
    const ui = sheet("enterprise-ui.css");

    assert.match(studio, /\.df2-notify-strip \{[\s\S]{0,160}--df-surface/);
    assert.doesNotMatch(studio, /\.df2-notify-strip \{[\s\S]{0,160}background: #ffffff;/);
    assert.match(studio, /\.df2-notify-strip-item \{[\s\S]{0,200}--df-surface-muted/);
    assert.match(studio, /\.df2-notify-strip-item\.is-ok \{[^}]*--df-success-bg/);
    assert.match(studio, /\.df2-notify-strip-item\.is-fail \{[^}]*--df-danger-bg/);
    assert.match(studio, /\.df2-vd-blocker-fix \{[\s\S]{0,160}--df-surface/);
    assert.doesNotMatch(studio, /\.df2-vd-blocker-fix \{[\s\S]{0,160}background: #ffffff;/);
    assert.match(studio, /\.df2-vd-coerce-samples table \{[\s\S]{0,160}--df-surface/);
    assert.doesNotMatch(studio, /\.df2-vd-coerce-samples table \{[\s\S]{0,160}background: #ffffff;/);
    assert.match(studio, /\.df2-vd-coerce-samples th \{[\s\S]{0,200}--df-surface-muted/);
    assert.match(
      studio,
      /\.df2-result-fidelity-metrics article,\s*\n\.df2-data-integrity-metric \{[\s\S]{0,200}--df-surface/,
    );
    assert.doesNotMatch(
      studio,
      /\.df2-result-fidelity-metrics article,\s*\n\.df2-data-integrity-metric \{[\s\S]{0,200}background: #ffffff;/,
    );

    assert.match(polish, /\.df2-model-card \{[\s\S]{0,240}--df-surface/);
    assert.doesNotMatch(polish, /\.df2-model-card \{[\s\S]{0,240}background: #fff !important;/);
    assert.match(polish, /\.df2-model-card\.ready \{[\s\S]{0,80}--df-brand-muted/);
    assert.match(polish, /\.df2-model-route strong \{[\s\S]{0,200}--df-surface/);
    assert.match(dataflow, /\.df2-model-card \{[\s\S]{0,160}--df-surface/);
    assert.match(dataflow, /\.df2-model-card\.ready \{[\s\S]{0,80}--df-brand-muted/);
    assert.match(ui, /\.df2-result-more \{[\s\S]{0,120}--df-surface/);
    assert.doesNotMatch(ui, /\.df2-result-more \{[\s\S]{0,120}background: #fff;/);
  });

  it("Inspector, map drawer, dest type tiles, and wizard footers do not flatten to #fff", () => {
    const studio = sheet("transfer-studio.css");
    const polish = sheet("shell-polish.css");

    assert.match(studio, /\.df2-map-visual-drawer-body \{[\s\S]{0,160}--df-surface/);
    assert.doesNotMatch(studio, /\.df2-map-visual-drawer-body \{[\s\S]{0,160}background: #fff;/);
    assert.match(studio, /\.df2-inspector-panel \{[\s\S]{0,160}--df-surface/);
    assert.doesNotMatch(studio, /\.df2-inspector-panel \{[\s\S]{0,160}background: #fff;/);
    assert.match(studio, /\.df2-inspector-guide \{[\s\S]{0,80}--df-brand-muted/);
    assert.doesNotMatch(studio, /\.df2-inspector-guide \{[\s\S]{0,80}#fff 100%/);
    assert.match(
      studio,
      /\.df2-page-transfer-studio \.df2-dest-type-tile \{[\s\S]{0,280}--df-surface/,
    );
    assert.doesNotMatch(
      studio,
      /\.df2-page-transfer-studio \.df2-dest-type-tile \{[\s\S]{0,280}background: #fff;/,
    );
    assert.match(
      studio,
      /\.df2-page-transfer-studio \.df2-dest-type-tile\.active \{[\s\S]{0,80}--df-brand-muted/,
    );
    assert.match(
      studio,
      /\.df2-map-dialog \.df2-column-review\.is-dialog \.df2-column-review-editor \{[\s\S]{0,220}--df-surface/,
    );
    assert.match(studio, /padding: 4px 12px !important;\s*\n\s*background: var\(--df-surface\)/);
    assert.match(
      studio,
      /\.df2-page-transfer-studio \.df2-wizard-footer \.df2-btn-ghost,[\s\S]{0,160}--df-surface/,
    );
    assert.match(
      studio,
      /\.df2-validate-step > \.df2-studio-actions,[\s\S]{0,220}--df-surface/,
    );
    assert.doesNotMatch(
      studio,
      /\.df2-validate-step > \.df2-studio-actions,[\s\S]{0,220}background: #fff;/,
    );
    assert.match(studio, /\.df2-theater-v3-footer \{[\s\S]{0,120}--df-surface/);
    assert.doesNotMatch(studio, /\.df2-theater-v3-footer \{[\s\S]{0,120}background: #fff;/);

    assert.match(polish, /\.df2-card-footer\.df2-wizard-footer \{[\s\S]{0,200}--df-surface-muted/);
    assert.doesNotMatch(polish, /\.df2-card-footer\.df2-wizard-footer \{[\s\S]{0,200}background: #f8fafc;/);
    assert.match(polish, /\.df2-card-footer,\s*\n\.df2-wizard-footer \{[\s\S]{0,160}--df-surface-muted/);
    assert.doesNotMatch(polish, /\.df2-card-footer,\s*\n\.df2-wizard-footer \{[\s\S]{0,160}background: #fafbfc;/);
  });

  it("Dest policy, schema preview, and dest form chips do not flatten to #fff", () => {
    const studio = sheet("transfer-studio.css");
    const dataflow = sheet("dataflow-ui.css");

    assert.match(studio, /\.df2-dest-step \.df2-dest-schema-preview \{[\s\S]{0,280}--df-surface/);
    assert.doesNotMatch(studio, /\.df2-dest-step \.df2-dest-schema-preview \{[\s\S]{0,280}background: #fff;/);
    assert.match(studio, /\.df2-dest-step \.df2-policy-console \{[\s\S]{0,160}--df-surface-muted/);
    assert.doesNotMatch(studio, /\.df2-dest-step \.df2-policy-console \{[\s\S]{0,160}background: #fafbfc;/);
    assert.match(
      studio,
      /\.df2-dest-step\.is-advanced \.df2-policy-console \{[\s\S]{0,160}--df-surface/,
    );
    assert.doesNotMatch(
      studio,
      /\.df2-dest-step\.is-advanced \.df2-policy-console \{[\s\S]{0,160}background: #fff;/,
    );
    assert.match(
      studio,
      /\.df2-dest-advanced-drawer \.df2-policy-option \{[\s\S]{0,280}--df-surface/,
    );
    assert.doesNotMatch(
      studio,
      /\.df2-dest-advanced-drawer \.df2-policy-option \{[\s\S]{0,280}background: #fff;/,
    );
    assert.match(studio, /\.df2-object-combobox-menu \{[\s\S]{0,420}--df-surface/);
    assert.doesNotMatch(studio, /\.df2-object-combobox-menu \{[\s\S]{0,420}background: #fff;/);
    assert.match(studio, /\.df2-dest-type-chip \{[\s\S]{0,280}--df-surface/);
    assert.doesNotMatch(studio, /\.df2-dest-type-chip \{[\s\S]{0,280}background: #fff;/);
    assert.match(studio, /\.df2-dest-type-chip\.active \{[\s\S]{0,80}--df-brand-muted/);
    assert.match(studio, /\.df2-dest-engine-search \{[\s\S]{0,420}--df-surface/);
    assert.match(studio, /\.df2-dest-engine-select \{[\s\S]{0,160}--df-surface/);

    assert.match(dataflow, /\.df2-policy-option \{[\s\S]{0,160}--df-surface/);
    assert.doesNotMatch(dataflow, /\.df2-policy-option \{[\s\S]{0,160}background: #fff;/);
    assert.match(dataflow, /\.df2-policy-option\.active \{[\s\S]{0,80}--df-brand-muted/);
    assert.doesNotMatch(dataflow, /\.df2-policy-option\.active \{[\s\S]{0,80}#f0fdfa/);
    assert.match(dataflow, /\.df2-policy-console \{[\s\S]{0,160}--df-surface/);
    assert.doesNotMatch(dataflow, /rgba\(255, 255, 255, 0\.95\)/);
    assert.match(dataflow, /\.df2-app \.df2-policy-option\.active \{[\s\S]{0,80}--df-brand-muted/);
  });

  it("Result route, metrics, and sections do not flatten to #fff", () => {
    const studio = sheet("transfer-studio.css");
    const ui = sheet("enterprise-ui.css");

    assert.match(studio, /\.df2-result-route-card,\s*\n\.df2-result-route \{[\s\S]{0,200}--df-surface/);
    assert.doesNotMatch(studio, /\.df2-result-route-card,\s*\n\.df2-result-route \{[\s\S]{0,200}background: #fff;/);
    assert.match(studio, /\.df2-result-stat-card,\s*\n\.df2-result-metric \{[\s\S]{0,200}--df-surface/);
    assert.doesNotMatch(studio, /\.df2-result-stat-card,\s*\n\.df2-result-metric \{[\s\S]{0,200}background: #fff;/);
    assert.match(studio, /\.df2-result-metric\.is-warn \{ background: var\(--df-warning-bg\)/);
    assert.match(studio, /\.df2-result-metric\.is-ok \{ background: var\(--df-success-bg\)/);
    assert.match(studio, /\.df2-result-section \{[\s\S]{0,160}--df-surface/);
    assert.doesNotMatch(studio, /\.df2-result-section \{[\s\S]{0,160}background: #fff;/);
    assert.match(studio, /\.df2-result-section\.error \{[\s\S]{0,80}--df-danger-bg/);
    assert.doesNotMatch(studio, /\.df2-result-section\.error \{[\s\S]{0,80}#fef2f2/);

    assert.match(ui, /\.df2-result-route \{[\s\S]{0,420}--df-surface/);
    assert.doesNotMatch(ui, /\.df2-result-route \{[\s\S]{0,420}background: #fff;/);
    assert.match(ui, /\.df2-result-metrics \{[\s\S]{0,420}--df-surface/);
    assert.doesNotMatch(ui, /\.df2-result-metrics \{[\s\S]{0,420}background: #fff;/);
    assert.match(ui, /\.df2-result-metric \{[\s\S]{0,220}--df-surface/);
    assert.doesNotMatch(ui, /\.df2-result-metric \{[\s\S]{0,220}background: #fff;/);
    assert.match(ui, /\.df2-result-metric\.is-ok \{[\s\S]{0,80}--df-success-bg/);
  });

  it("Sidebar chrome, toolbar, and jobs detail hosts do not flatten to #fff", () => {
    const polish = sheet("shell-polish.css");
    const ui = sheet("enterprise-ui.css");
    const app = sheet("app-styles.css");

    assert.match(polish, /\.df2-user-actions button \{[\s\S]{0,220}--df-surface/);
    assert.doesNotMatch(polish, /\.df2-user-actions button \{[\s\S]{0,220}background: #fff;/);
    assert.match(polish, /\.df2-sidebar-expand-topbar \{[\s\S]{0,200}--df-surface/);
    assert.doesNotMatch(polish, /\.df2-sidebar-expand-topbar \{[\s\S]{0,200}background: #fff;/);
    assert.match(polish, /\.df2-sidebar-collapse-btn \{[\s\S]{0,220}--df-surface/);
    assert.doesNotMatch(polish, /\.df2-sidebar-collapse-btn \{[\s\S]{0,220}background: #fff;/);
    assert.match(polish, /\.df2-security-row \{[\s\S]{0,200}--df-surface/);
    assert.doesNotMatch(polish, /\.df2-security-row \{[\s\S]{0,200}background: #fff;/);

    assert.match(ui, /\.df2-toolbar \{[\s\S]{0,420}--df-surface/);
    assert.doesNotMatch(ui, /\.df2-toolbar \{[\s\S]{0,420}background: #fff;/);
    assert.match(ui, /\.df2-toolbar-search \{[\s\S]{0,420}--df-surface/);
    assert.doesNotMatch(ui, /\.df2-toolbar-search \{[\s\S]{0,420}background: #fff;/);
    assert.match(ui, /\.df2-toolbar-search:focus-within \{[\s\S]{0,120}--df-surface/);
    assert.doesNotMatch(ui, /\.df2-toolbar-search:focus-within \{[\s\S]{0,120}background: #fff !important;/);
    assert.match(ui, /\.df2-toolbar-gitops-toggle \{[\s\S]{0,280}--df-surface/);
    assert.match(ui, /\.df2-jobs-detail-tab\.is-active \{[\s\S]{0,80}--df-surface/);
    assert.doesNotMatch(ui, /\.df2-jobs-detail-tab\.is-active \{[\s\S]{0,80}background: #fff;/);
    assert.match(ui, /\.df2-jobs-detail-panel \{[\s\S]{0,160}--df-surface/);
    assert.doesNotMatch(ui, /\.df2-jobs-detail-panel \{[\s\S]{0,160}background: #fff;/);
    assert.match(ui, /\.df2-transfer-step-viewport \{[\s\S]{0,80}--df-surface/);
    assert.doesNotMatch(ui, /\.df2-transfer-step-viewport \{[\s\S]{0,80}background: #fff;/);

    assert.match(app, /\.df2-app \.df2-context-bar \{[\s\S]{0,120}--df-surface/);
    assert.doesNotMatch(app, /\.df2-app \.df2-context-bar \{[\s\S]{0,160}#fff 70%/);
  });

  it("Source aside, multistream, and structure preview do not flatten to #fff", () => {
    const studio = sheet("transfer-studio.css");
    const polish = sheet("shell-polish.css");

    assert.match(studio, /\.df2-multistream-preview \{[\s\S]{0,200}--df-surface/);
    assert.doesNotMatch(studio, /\.df2-multistream-preview \{[\s\S]{0,200}background: #fff;/);
    assert.match(studio, /\.df2-multistream-tabs \{[\s\S]{0,160}--df-surface-muted/);
    assert.match(studio, /\.df2-multistream-tab:hover \{[\s\S]{0,80}--df-surface/);
    assert.match(studio, /\.df2-multistream-tab\.is-active \{[\s\S]{0,80}--df-surface/);
    assert.match(studio, /\.df2-multistream-tab\.is-error\.is-active \{[\s\S]{0,80}--df-danger-bg/);
    assert.match(studio, /\.df2-source-aside-connector \{[\s\S]{0,160}--df-surface/);
    assert.doesNotMatch(studio, /\.df2-source-aside-connector \{[\s\S]{0,160}background: #fff;/);
    assert.match(studio, /\.df2-source-aside-connector-row \{[\s\S]{0,160}--df-surface/);
    assert.doesNotMatch(studio, /\.df2-source-aside-connector-row \{[\s\S]{0,160}background: #fff;/);
    assert.match(studio, /\.df2-source-aside-path \{[\s\S]{0,160}--df-surface-muted/);
    assert.match(studio, /\.df2-structure-field-more \{[\s\S]{0,80}--df-surface/);
    assert.doesNotMatch(studio, /\.df2-structure-field-more \{[\s\S]{0,80}background: #fff;/);
    assert.match(studio, /\.df2-structure-field-chip \{[\s\S]{0,200}--df-surface-muted/);

    assert.match(polish, /\.df2-structure-view-toggle button\.active \{[\s\S]{0,80}--df-surface/);
    assert.doesNotMatch(polish, /\.df2-structure-view-toggle button\.active \{[\s\S]{0,80}background: #fff;/);
    assert.match(polish, /\.df2-structure-json-doc \{[\s\S]{0,160}--df-surface/);
    assert.doesNotMatch(polish, /\.df2-structure-json-doc \{[\s\S]{0,160}background: #fff;/);
    assert.match(polish, /\.df2-structure-field-chip \{[\s\S]{0,160}--df-surface/);
    assert.doesNotMatch(polish, /\.df2-structure-field-chip \{[\s\S]{0,160}background: #fff;/);
    assert.match(polish, /\.df2-structure-table-wrap \{[\s\S]{0,120}--df-surface/);
    assert.doesNotMatch(polish, /\.df2-structure-table-wrap \{[\s\S]{0,120}background: #fff;/);
  });

  it("Validate repair, run-id, and compact preflight leftovers do not flatten to #fff", () => {
    const studio = sheet("transfer-studio.css");

    assert.match(studio, /\.df2-repair-issues li \{[\s\S]{0,120}--df-surface/);
    assert.doesNotMatch(studio, /\.df2-repair-issues li \{[\s\S]{0,120}background: #fff;/);
    assert.match(studio, /\.df2-repair-issues li\.is-block \{[\s\S]{0,80}--df-danger-bg/);
    assert.match(studio, /\.df2-repair-issues li\.is-warn \{[\s\S]{0,80}--df-warning-bg/);
    assert.match(studio, /\.df2-repair-actions li \{[\s\S]{0,120}--df-surface-muted/);
    assert.match(studio, /\.df2-vd-assist-chevron \{[\s\S]{0,200}--df-surface/);
    assert.doesNotMatch(studio, /\.df2-vd-assist-chevron \{[\s\S]{0,200}background: #fff;/);
    assert.match(studio, /\.df2-vd-run-id \{[\s\S]{0,200}--df-surface-muted/);
    assert.match(studio, /\.df2-vd-run-id code \{[\s\S]{0,160}--df-surface/);
    assert.doesNotMatch(studio, /\.df2-vd-run-id code \{[\s\S]{0,160}background: #fff;/);
    assert.match(studio, /\.df2-vd-run-id-copy \{[\s\S]{0,160}--df-surface/);
    assert.doesNotMatch(studio, /\.df2-vd-run-id-copy \{[\s\S]{0,160}background: #fff;/);
    assert.match(studio, /\.df2-vd-xform-hash \{[\s\S]{0,120}--df-surface/);
    assert.match(studio, /\.df2-map-pk-details-body \{[\s\S]{0,280}--df-surface/);
    assert.match(studio, /\.df2-preflight\.is-compact \.df2-preflight-step \{[\s\S]{0,120}--df-surface/);
    assert.match(studio, /\.df2-preflight-diagnostics-list > li \{[\s\S]{0,80}--df-surface/);
    assert.match(studio, /\.df2-run-launch-stage \{[\s\S]{0,120}--df-surface/);
    assert.match(studio, /\.df2-theater-v2-phase \{[\s\S]{0,280}--df-surface/);
    assert.doesNotMatch(studio, /\.df2-theater-v2-phase \{[\s\S]{0,280}background: #fff;/);
    assert.match(studio, /\.df2-theater-v2-phase\.done \{[\s\S]{0,80}--df-success-bg/);
    assert.match(studio, /\.df2-theater-v2-phase\.active \{[\s\S]{0,80}--df-brand-muted/);
    assert.match(studio, /\.df2-theater-v2-phase\.failed \{[\s\S]{0,80}--df-danger-bg/);
  });

  it("Overview, MCP, and modal leftovers do not flatten to #fff", () => {
    const polish = sheet("shell-polish.css");
    const premium = sheet("premium-theme.css");
    const dataflow = sheet("dataflow-ui.css");
    const pages = sheet("enterprise-pages.css");
    const consistency = sheet("ui-consistency.css");
    const ui = sheet("enterprise-ui.css");

    assert.match(polish, /\.df2-connector-chip \{[\s\S]{0,280}--df-surface/);
    assert.doesNotMatch(polish, /\.df2-connector-chip \{[\s\S]{0,280}background: #fff;/);
    assert.match(polish, /\.df2-job-phase-pill \{[\s\S]{0,280}--df-surface/);
    assert.doesNotMatch(polish, /\.df2-job-phase-pill \{[\s\S]{0,280}background: #fff;/);
    assert.match(polish, /\.df2-job-phase-pill\.done \{[\s\S]{0,120}--df-success-bg/);
    assert.match(polish, /\.df2-job-phase-pill\.active \{[\s\S]{0,120}--df-brand-muted/);
    assert.match(polish, /\.df2-job-phase-pill\.failed \{[\s\S]{0,120}--df-danger-bg/);
    assert.match(polish, /\.df2-settings-nav \{[\s\S]{0,280}--df-surface/);
    assert.match(polish, /\.df2-mcp-tile \{[\s\S]{0,280}--df-surface/);
    assert.match(polish, /\.df2-search-dropdown \{[\s\S]{0,280}--df-surface/);
    assert.match(polish, /\.df2-control-metric \{[\s\S]{0,280}--df-surface/);
    assert.match(polish, /\.dt-modal \{[\s\S]{0,320}--df-surface/);
    assert.doesNotMatch(polish, /\.dt-modal \{[\s\S]{0,320}background: #ffffff;/);
    assert.match(polish, /\.dt-modal-body \{[\s\S]{0,200}--df-surface/);
    assert.doesNotMatch(polish, /\.dt-modal-body \{[\s\S]{0,200}background: #fff;/);
    assert.match(polish, /\.dt-modal-body \.df2-input \{[\s\S]{0,280}--df-surface/);
    assert.match(polish, /\.df2-input:disabled,[\s\S]{0,80}--df-surface-muted/);
    assert.match(polish, /\.df2-stream-row \{[\s\S]{0,280}--df-surface/);
    assert.doesNotMatch(polish, /\.df2-stream-row \{[\s\S]{0,280}background: #fff;/);
    assert.match(polish, /\.df2-overview-kpis \.df2-stat \{[\s\S]{0,200}--df-surface/);
    assert.doesNotMatch(polish, /\.df2-overview-kpis \.df2-stat \{[\s\S]{0,200}background: #fff;/);
    assert.match(polish, /\.df2-overview-action \{[\s\S]{0,360}--df-surface/);
    assert.doesNotMatch(polish, /\.df2-overview-action \{[\s\S]{0,360}background: #fff;/);
    assert.match(polish, /\.df2-overview-action:hover \{[\s\S]{0,120}--df-brand-muted/);
    assert.match(polish, /\.df2-stats \.df2-stat,[\s\S]{0,160}background: var\(--df-surface\)/);

    assert.match(premium, /\.df2-dashboard-matrix button \{[\s\S]{0,280}--df-surface/);
    assert.doesNotMatch(premium, /\.df2-dashboard-matrix button \{[\s\S]{0,280}background: #fff;/);
    assert.match(premium, /\.df2-ops-card \{[\s\S]{0,200}--df-surface/);
    assert.doesNotMatch(premium, /\.df2-ops-card \{[\s\S]{0,200}background: #fff;/);
    assert.match(premium, /\.df2-ops-card\.ok \{[\s\S]{0,120}--df-success-bg/);
    assert.match(premium, /\.df2-ops-card\.warn \{[\s\S]{0,120}--df-warning-bg/);
    assert.match(premium, /\.df2-tab\.active \{[\s\S]{0,120}--df-surface/);
    assert.doesNotMatch(premium, /\.df2-tab\.active \{[\s\S]{0,120}background: #fff;/);
    assert.match(premium, /\.df2-connection-workbench \{[\s\S]{0,200}--df-surface/);
    assert.doesNotMatch(premium, /\.df2-connection-workbench \{[\s\S]{0,200}background: #fff;/);
    assert.match(premium, /\.df2-pipeline \{[\s\S]{0,200}--df-surface/);
    assert.doesNotMatch(premium, /\.df2-pipeline \{[\s\S]{0,200}background: #fff;/);
    assert.match(premium, /\.df2-mcp-tile \{[\s\S]{0,200}--df-surface/);
    assert.doesNotMatch(premium, /\.df2-mcp-tile \{[\s\S]{0,200}background: #fff;/);
    assert.match(premium, /\.df2-login-card \{[\s\S]{0,200}background: #fff;/);
    assert.match(premium, /\.df2-pilot-console-strip \{[\s\S]{0,200}--df-surface/);

    assert.match(dataflow, /\.df2-mcp-tile \{[\s\S]{0,200}--df-surface/);
    assert.doesNotMatch(dataflow, /\.df2-mcp-tile \{[\s\S]{0,200}background: #fff;/);
    assert.match(dataflow, /\.df2-ops-card \{[\s\S]{0,200}--df-surface/);
    assert.doesNotMatch(dataflow, /\.df2-ops-card \{[\s\S]{0,200}background: #fff;/);
    assert.match(dataflow, /\.df2-app \.df2-ops-card\.ok \{[\s\S]{0,120}--df-success-bg/);
    assert.match(dataflow, /\.df2-app \.df2-ops-card\.warn \{[\s\S]{0,120}--df-warning-bg/);
    assert.match(dataflow, /\.df2-connection-workbench \{[\s\S]{0,280}--df-surface/);

    assert.match(pages, /\.df2-mcp-tile \{[\s\S]{0,280}--df-surface/);
    assert.doesNotMatch(pages, /\.df2-mcp-tile \{[\s\S]{0,280}background: #fff;/);
    assert.match(consistency, /\.df2-control-metric \{[\s\S]{0,360}--df-surface/);
    assert.doesNotMatch(consistency, /\.df2-control-metric \{[\s\S]{0,360}background: #fff;/);

    assert.match(ui, /\.df2-app \.dt-modal,[\s\S]{0,520}--df-surface/);
    assert.match(ui, /\.df2-app \.df2-ops-card\.ok,[\s\S]{0,120}--df-success-bg/);
    assert.match(ui, /\.df2-app \.df2-job-phase-pill\.active \{[\s\S]{0,120}--df-brand-muted/);
    assert.match(ui, /\.df2-app \.df2-job-phase-pill\.failed \{[\s\S]{0,120}--df-danger-bg/);
  });

  it("Theater, result, and quarantine leftovers do not flatten to #fff", () => {
    const ui = sheet("enterprise-ui.css");

    assert.match(ui, /\.df2-gate8-proof-grid div \{[\s\S]{0,160}--df-surface/);
    assert.doesNotMatch(ui, /\.df2-gate8-proof-grid div \{[\s\S]{0,160}background: #fff;/);
    assert.match(ui, /\.df2-result-cdc-strip dl div \{[\s\S]{0,160}--df-surface/);
    assert.doesNotMatch(ui, /\.df2-result-cdc-strip dl div \{[\s\S]{0,160}background: #fff;/);
    assert.match(ui, /\.df2-cdc-snapshot-list li \{[\s\S]{0,280}--df-surface/);
    assert.doesNotMatch(ui, /\.df2-cdc-snapshot-list li \{[\s\S]{0,280}background: #fff;/);
    assert.match(ui, /\.df2-jobs-evidence-chip \{[\s\S]{0,360}--df-surface/);
    assert.doesNotMatch(ui, /\.df2-jobs-evidence-chip \{[\s\S]{0,360}background: #fff;/);
    assert.match(ui, /\.df2-jobs-quarantine-metric \{[\s\S]{0,160}--df-surface/);
    assert.doesNotMatch(ui, /\.df2-jobs-quarantine-metric \{[\s\S]{0,160}background: #fff;/);
    assert.match(ui, /\.df2-run-ready-actions \{[\s\S]{0,280}--df-surface/);
    assert.doesNotMatch(ui, /\.df2-run-ready-actions \{[\s\S]{0,280}background: #fff;/);
    assert.match(ui, /\.df2-gate8-control-totals \{[\s\S]{0,200}--df-surface/);
    assert.doesNotMatch(ui, /\.df2-gate8-control-totals \{[\s\S]{0,200}background: #fff;/);
    assert.match(ui, /\.df2-gate8-control-totals\.is-proven \{[\s\S]{0,80}--df-success-bg/);
    assert.match(ui, /\.df2-gate8-control-totals\.is-unproven \{[\s\S]{0,80}--df-warning-bg/);
    assert.match(ui, /\.df2-theater-lineage \{[\s\S]{0,160}--df-surface/);
    assert.doesNotMatch(ui, /\.df2-theater-lineage \{[\s\S]{0,160}background: #fff;/);
    assert.match(ui, /\.df2-theater-v3-alert\.success \{[\s\S]{0,80}--df-success-bg/);
    assert.match(ui, /\.df2-theater-v3-alert\.error \{[\s\S]{0,80}--df-danger-bg/);
    assert.match(ui, /\.df2-theater-v3-next \{[\s\S]{0,360}--df-surface/);
    assert.doesNotMatch(ui, /\.df2-theater-v3-next \{[\s\S]{0,360}background: #fff;/);
    assert.match(ui, /\.df2-jobs-v3-failure-message \{[\s\S]{0,200}--df-danger-bg/);
    assert.doesNotMatch(ui, /\.df2-jobs-v3-failure-message \{[\s\S]{0,200}background: #fff;/);
    assert.match(ui, /\.df2-quarantine-inspect-body \{[\s\S]{0,200}--df-surface/);
    assert.doesNotMatch(ui, /\.df2-quarantine-inspect-body \{[\s\S]{0,200}background: #ffffff;/);
    assert.match(ui, /\.df2-quarantine-summary-chip \{[\s\S]{0,160}--df-surface/);
    assert.doesNotMatch(ui, /\.df2-quarantine-summary-chip \{[\s\S]{0,160}background: #fff;/);
    assert.match(ui, /\.df2-result-fidelity-inline > span \{[\s\S]{0,280}--df-surface/);
    assert.doesNotMatch(ui, /\.df2-result-fidelity-inline > span \{[\s\S]{0,280}background: #fff;/);
    assert.match(ui, /\.df2-app \.df2-gate8-control-totals\.is-proven,[\s\S]{0,80}--df-success-bg/);
    assert.match(ui, /\.df2-app \.df2-jobs-v3-failure-message,[\s\S]{0,120}--df-danger-bg/);
  });

  it("Connector setup, overview-v3, and dest-card leftovers do not flatten to #fff", () => {
    const ui = sheet("enterprise-ui.css");
    const studio = sheet("transfer-studio.css");

    assert.match(ui, /\.df2-conn-setup-icon \{[\s\S]{0,280}--df-surface/);
    assert.doesNotMatch(ui, /\.df2-conn-setup-icon \{[\s\S]{0,280}background: #fff;/);
    assert.match(ui, /\.df2-auth-card \{[\s\S]{0,360}--df-surface/);
    assert.doesNotMatch(ui, /\.df2-auth-card \{[\s\S]{0,360}background: #fff;/);
    assert.match(ui, /\.df2-auth-card\.is-active,[\s\S]{0,120}--df-brand-muted/);
    assert.match(ui, /\.df2-conn-help \{[\s\S]{0,360}--df-surface/);
    assert.doesNotMatch(ui, /\.df2-conn-help \{[\s\S]{0,360}background: #fff;/);
    assert.match(ui, /\.df2-overview-v3-ops-chip \{[\s\S]{0,280}--df-surface/);
    assert.doesNotMatch(ui, /\.df2-overview-v3-ops-chip \{[\s\S]{0,280}background: #fff;/);
    assert.match(ui, /\.df2-overview-v3-ops-chip\.is-live \{[\s\S]{0,80}--df-brand-muted/);
    assert.match(ui, /\.df2-overview-v3-ops-chip\.is-fail \{[\s\S]{0,80}--df-danger-bg/);
    assert.match(ui, /\.df2-overview-v3-card-head \{[\s\S]{0,280}--df-surface/);
    assert.doesNotMatch(ui, /\.df2-overview-v3-card-head \{[\s\S]{0,280}background: #fff;/);
    assert.match(ui, /\.lp-mkt-page \.lp-mkt-list li \{[\s\S]{0,160}background: #fff;/);

    assert.match(studio, /\.df2-dest-connector-card \{[\s\S]{0,420}--df-surface/);
    assert.doesNotMatch(studio, /\.df2-dest-connector-card \{[\s\S]{0,420}background: #fff;/);
    assert.match(studio, /\.df2-dest-connector-card\.active \{[\s\S]{0,80}--df-brand-muted/);
    assert.match(studio, /\.df2-page-transfer-studio \.df2-dest-connector-card \{[\s\S]{0,420}--df-surface/);
    assert.match(studio, /\.df2-page-transfer-studio \.df2-dest-connector-card\.active \{[\s\S]{0,80}--df-brand-muted/);

    assert.match(ui, /\.df2-app \.df2-conn-setup-icon,[\s\S]{0,280}--df-surface/);
    assert.match(ui, /\.df2-app \.df2-auth-card\.is-active,[\s\S]{0,160}--df-brand-muted/);
    assert.match(ui, /\.df2-app \.df2-overview-v3-ops-chip\.is-fail \{[\s\S]{0,80}--df-danger-bg/);
  });

  it("Warn, success, and white-ending leftover chips follow tokens", () => {
    const ui = sheet("enterprise-ui.css");
    const app = sheet("app-styles.css");

    assert.match(ui, /\.df2-gate8-proof \{[\s\S]{0,160}--df-success-bg/);
    assert.doesNotMatch(ui, /\.df2-gate8-proof \{[\s\S]{0,160}#fff 70%/);
    assert.match(ui, /\.df2-gate8-proof\.is-fail \{[\s\S]{0,80}--df-danger-bg/);
    assert.match(ui, /\.df2-app \.df2-gate8-proof\.is-fail \{[\s\S]{0,80}--df-danger-bg/);
    assert.match(ui, /\.df2-gate8-proof-mismatches \{[\s\S]{0,160}--df-warning-bg/);
    assert.doesNotMatch(ui, /\.df2-gate8-proof-mismatches \{[\s\S]{0,160}#fff7ed/);
    assert.match(ui, /\.df2-jobs-evidence-chip\.tone-ok \{[\s\S]{0,80}--df-success-bg/);
    assert.match(ui, /\.df2-jobs-evidence-chip\.tone-warn \{[\s\S]{0,80}--df-warning-bg/);
    assert.match(ui, /\.df2-jobs-evidence-chip\.tone-danger \{[\s\S]{0,80}--df-danger-bg/);
    assert.match(ui, /\.df2-jobs-quarantine-meaning \{[\s\S]{0,200}--df-warning-bg/);
    assert.doesNotMatch(ui, /\.df2-jobs-quarantine-meaning \{[\s\S]{0,200}#fffbeb/);
    assert.match(ui, /\.df2-mcp-status-pill\.is-online \{[\s\S]{0,80}--df-success-bg/);
    assert.match(ui, /\.df2-mcp-status-pill\.is-offline \{[\s\S]{0,80}--df-warning-bg/);
    assert.match(ui, /\.df2-validate-stage \{[\s\S]{0,360}--df-surface/);
    assert.doesNotMatch(ui, /\.df2-validate-stage \{[\s\S]{0,360}#ffffff 72%/);
    assert.match(ui, /\.df2-preflight\.is-compact \.df2-preflight-step\.pass \{[\s\S]{0,80}--df-success-bg/);
    assert.match(ui, /\.df2-preflight\.is-compact \.df2-preflight-step\.block \{[\s\S]{0,80}--df-danger-bg/);
    assert.match(ui, /\.df2-preflight-diagnostics\.df2-disclosure \{[\s\S]{0,160}--df-danger-bg/);
    assert.match(ui, /\.df2-run-readiness \{[\s\S]{0,280}--df-success-bg/);
    assert.doesNotMatch(ui, /\.df2-run-readiness \{[\s\S]{0,280}#ffffff 100%/);
    assert.match(ui, /\.df2-run-step > \.df2-card-body\.df2-run-starting \{[\s\S]{0,360}--df-surface/);
    assert.match(ui, /\.df2-theater-v3-progress-block \{[\s\S]{0,200}--df-surface/);
    assert.match(ui, /\.df2-result-proof \{[\s\S]{0,160}--df-success-bg/);
    assert.match(ui, /\.df2-jobs-v3-failure-panel \{[\s\S]{0,280}--df-danger-bg/);
    assert.match(ui, /\.df2-jobs-v3-alert\.error \{[\s\S]{0,80}--df-danger-bg/);
    assert.match(ui, /\.df2-quarantine-inspect \{[\s\S]{0,200}--df-warning-bg/);
    assert.doesNotMatch(ui, /\.df2-quarantine-inspect \{[\s\S]{0,200}#fffbeb/);
    assert.match(ui, /\.df2-quarantine-explainer \{[\s\S]{0,160}--df-warning-bg/);
    assert.match(ui, /\.df2-quarantine-durable-warn \{[\s\S]{0,80}--df-warning-bg/);
    assert.match(ui, /\.df2-quarantine-summary \{[\s\S]{0,200}--df-warning-bg/);
    assert.match(ui, /\.df2-jobs-v3-quarantine,[\s\S]{0,200}--df-warning-bg/);
    assert.match(ui, /\.df2-status-metric-pill\.ok \{[\s\S]{0,80}--df-success-bg/);
    assert.match(ui, /\.df2-status-metric-pill\.warn \{[\s\S]{0,80}--df-danger-bg/);
    assert.match(ui, /\.df2-status-metric-pill\.live \{[\s\S]{0,80}--df-brand-muted/);
    assert.match(ui, /\.df2-conn-probe\.is-ok \{[\s\S]{0,80}--df-success-bg/);
    assert.match(ui, /\.df2-conn-probe\.is-fail \{[\s\S]{0,80}--df-danger-bg/);
    assert.match(ui, /\.df2-connectors-list \.df2-connector-card\.error \{[\s\S]{0,80}--df-danger-bg/);
    assert.match(ui, /\.df2-connectors-list \.df2-connector-card\.selected \{[\s\S]{0,80}--df-brand-muted/);
    assert.match(ui, /\.df2-overview-v3 \.df2-metric-glass-teal \{[\s\S]{0,80}--df-brand-muted/);
    assert.match(ui, /\.df2-overview-v3 \.df2-metric-glass-green \{[\s\S]{0,80}--df-success-bg/);
    assert.match(ui, /\.df2-overview-v3 \.df2-metric-glass-amber \{[\s\S]{0,80}--df-warning-bg/);
    assert.match(app, /\.df2-app \.df2-overview-v3 \.df2-metric-glass-teal \{[\s\S]{0,80}--df-brand-muted/);
    assert.doesNotMatch(app, /\.df2-app \.df2-overview-v3 \.df2-metric-glass-teal \{[\s\S]{0,160}#fff 78%/);
    assert.match(ui, /\.df2-pilot-v2 \.df2-pilot-composer-bar \{[\s\S]{0,80}--df-surface/);
  });

  it("Studio, shell, and badge warn leftovers follow warning tokens", () => {
    const studio = sheet("transfer-studio.css");
    const polish = sheet("shell-polish.css");
    const dataflow = sheet("dataflow-ui.css");
    const platform = sheet("enterprise-platform.css");
    const topology = sheet("pipeline-topology.css");
    const ui = sheet("enterprise-ui.css");

    assert.match(studio, /\.df2-source-aside-stream-warn \{[\s\S]{0,200}--df-warning-bg/);
    assert.doesNotMatch(studio, /\.df2-source-aside-stream-warn \{[\s\S]{0,200}#fffbeb/);
    assert.match(studio, /\.df2-map-band-chip\.is-attention \{[\s\S]{0,80}--df-warning-bg/);
    assert.match(studio, /\.df2-csv-validation-alert \{[\s\S]{0,200}--df-warning-bg/);
    assert.match(studio, /\.df2-dest-target-status\.is-pending \{[\s\S]{0,80}--df-warning-bg/);
    assert.match(studio, /\.df2-result-error-hint \{[\s\S]{0,160}--df-warning-bg/);
    assert.match(studio, /\.df2-validate-rail-review \{[\s\S]{0,160}--df-warning-bg/);
    assert.match(studio, /\.df2-theater-v3-live-pill\.is-quarantine \{[\s\S]{0,80}--df-warning-bg/);
    assert.match(studio, /\.df2-theater-v3-alert\.warn \{[\s\S]{0,80}--df-warning-bg/);
    assert.match(studio, /\.df2-vd-cell-preview \{[\s\S]{0,160}--df-warning-bg/);
    assert.match(studio, /\.df2-vd-iso-group \{[\s\S]{0,160}--df-warning-bg/);
    assert.match(studio, /\.df2-vd-iso-issue \{[\s\S]{0,120}--df-warning-bg/);
    assert.match(studio, /\.df2-vd-append-warn \{[\s\S]{0,200}--df-warning-bg/);
    assert.match(studio, /\.df2-map-identity-banner \{[\s\S]{0,200}--df-warning-bg/);
    assert.match(studio, /\.df2-map-blocker-bar \{[\s\S]{0,80}--df-warning-bg/);
    assert.match(studio, /\.df2-vd-proof-chip\.band-low \{[\s\S]{0,80}--df-warning-bg/);
    assert.match(studio, /\.df2-vd-proof-chip\.band-high \{[\s\S]{0,80}--df-success-bg/);
    assert.match(studio, /\.df2-vd-local-banner \{[\s\S]{0,200}--df-warning-bg/);

    assert.match(polish, /\.df2-type-risk\.warn \{[\s\S]{0,80}--df-warning-bg/);
    assert.match(polish, /\.df2-type-risk\.block \{[\s\S]{0,80}--df-danger-bg/);
    assert.match(polish, /\.df2-structure-empty-rows \{[\s\S]{0,280}--df-warning-bg/);
    assert.match(polish, /\.df2-col-badge-warn \{[\s\S]{0,80}--df-warning-bg/);
    assert.match(polish, /\.df2-overview-health-pill\.warn \{[\s\S]{0,80}--df-warning-bg/);
    assert.match(polish, /\.df2-overview-health-pill\.live \{[\s\S]{0,80}--df-brand-muted/);

    assert.match(dataflow, /\.df2-badge-run \{[\s\S]{0,80}--df-warning-bg/);
    assert.doesNotMatch(dataflow, /\.df2-badge-run \{[\s\S]{0,80}#fffbeb/);
    assert.match(dataflow, /\.dt-badge-warning \{[\s\S]{0,80}--df-warning-bg/);
    assert.match(dataflow, /\.df2-row-warn \{[\s\S]{0,80}--df-warning-bg/);
    assert.match(dataflow, /\.df2-control-metric\.warn \{[\s\S]{0,80}--df-warning-bg/);
    assert.match(dataflow, /\.df2-rail-stage\.warn \{[\s\S]{0,80}--df-warning-bg/);
    assert.match(dataflow, /\.df2-rail-alert \{[\s\S]{0,200}--df-warning-bg/);
    assert.match(dataflow, /\.df2-assurance-chip\.warn \{[\s\S]{0,80}--df-warning-bg/);
    assert.match(dataflow, /\.df2-mapping-col\.pii \{[\s\S]{0,80}--df-warning-bg/);
    assert.match(dataflow, /\.df2-readiness-row\.warn \{[\s\S]{0,80}--df-warning-bg/);

    assert.match(platform, /\.df2-badge-warning \{[\s\S]{0,80}--df-warning-bg/);
    assert.match(platform, /\.df2-badge-beta \{[\s\S]{0,80}--df-warning-bg/);
    assert.match(topology, /\.df2-csv-validation-alert \{[\s\S]{0,200}--df-warning-bg/);
    assert.match(ui, /\.df2-app \.df2-control-metric\.warn,[\s\S]{0,80}--df-warning-bg/);
    assert.match(ui, /\.df2-app \.df2-readiness-row\.warn \{[\s\S]{0,80}--df-warning-bg/);
  });

  it("Column workbench and map-proof leftovers follow tokens", () => {
    const workbench = sheet("column-workbench.css");
    const premium = sheet("premium-theme.css");
    const ui = sheet("enterprise-ui.css");

    assert.match(workbench, /\.df2-map-proof-pair-risks li\.sev-warn \{[\s\S]{0,80}--df-warning-bg/);
    assert.doesNotMatch(workbench, /\.df2-map-proof-pair-risks li\.sev-warn \{[\s\S]{0,80}#fffbeb/);
    assert.match(workbench, /\.df2-map-proof-pair \{[\s\S]{0,200}--df-surface/);
    assert.doesNotMatch(workbench, /\.df2-map-proof-pair \{[\s\S]{0,200}background: #fff;/);
    assert.match(workbench, /\.df2-map-proof-pair\.is-aligned \{[\s\S]{0,80}--df-success-bg/);
    assert.match(workbench, /\.df2-badge\.fidelity-ok \{[\s\S]{0,80}--df-success-bg/);
    assert.match(workbench, /\.df2-badge\.fidelity-warn \{[\s\S]{0,80}--df-warning-bg/);
    assert.match(workbench, /\.df2-map-editor-pane \{[\s\S]{0,200}--df-surface/);
    assert.doesNotMatch(workbench, /\.df2-map-editor-pane \{[\s\S]{0,200}background: #fff;/);
    assert.match(workbench, /\.df2-mapping-intelligence \{[\s\S]{0,200}--df-surface/);
    assert.match(workbench, /\.df2-mapping-attention \{[\s\S]{0,200}--df-warning-bg/);
    assert.doesNotMatch(workbench, /\.df2-mapping-attention \{[\s\S]{0,200}#fffbeb/);
    assert.match(workbench, /\.df2-mapping-intelligence-pair\.tier-warn \{[\s\S]{0,80}--df-warning-bg/);
    assert.match(workbench, /\.df2-mapping-intelligence-pair\.tier-block \{[\s\S]{0,80}--df-danger-bg/);
    assert.match(workbench, /\.df2-mapping-pairs-route \{[\s\S]{0,320}--df-surface/);
    assert.match(workbench, /\.df2-mapping-pairs-bridge \{[\s\S]{0,160}--df-success-bg/);
    assert.match(workbench, /\.df2-mapping-pair-conf-ok \{[\s\S]{0,80}--df-success-bg/);
    assert.match(workbench, /\.df2-mapping-pair-conf-warn \{[\s\S]{0,80}--df-warning-bg/);
    assert.match(workbench, /\.df2-mapping-pair-conf-block \{[\s\S]{0,80}--df-danger-bg/);
    assert.match(workbench, /\.df2-mapping-pair-warn \{[\s\S]{0,80}--df-warning-bg/);
    assert.match(workbench, /\.df2-mapping-pair-block \{[\s\S]{0,80}--df-danger-bg/);

    assert.match(premium, /\.df2-dashboard-next \{[\s\S]{0,200}--df-surface/);
    assert.doesNotMatch(premium, /\.df2-dashboard-next \{[\s\S]{0,200}#ffffff/);
    assert.match(premium, /\.df2-jobs-command \{[\s\S]{0,200}--df-surface/);
    assert.doesNotMatch(premium, /\.df2-jobs-command \{[\s\S]{0,200}#fff;/);
    assert.match(premium, /\.df2-column-review-alert \{[\s\S]{0,160}--df-warning-bg/);
    assert.doesNotMatch(premium, /\.df2-column-review-alert \{[\s\S]{0,160}#fffbeb/);
    assert.match(premium, /\.df2-column-review-table tr\.warn td \{[\s\S]{0,80}--df-warning-bg/);
    assert.match(premium, /\.df2-login-card \{[\s\S]{0,160}background: #fff;/);
    assert.match(premium, /\.df2-pilot-console-strip \{[\s\S]{0,240}--df-surface/);

    assert.match(ui, /\.df2-app \.df2-jobs-command \{[\s\S]{0,80}--df-surface/);
    assert.match(ui, /\.df2-app \.df2-column-review-table tr\.warn td \{[\s\S]{0,80}--df-warning-bg/);
    assert.match(ui, /\.df2-app \.df2-mapping-pair-block \{[\s\S]{0,80}--df-danger-bg/);
  });

  it("Result dashboard and theater-rejected leftovers follow tokens", () => {
    const studio = sheet("transfer-studio.css");
    const topology = sheet("pipeline-topology.css");
    const premium = sheet("premium-theme.css");
    const ui = sheet("enterprise-ui.css");

    assert.match(topology, /\.df2-theater-rejected \{[\s\S]{0,200}--df-warning-bg/);
    assert.doesNotMatch(topology, /\.df2-theater-rejected \{[\s\S]{0,200}#fffbeb/);

    assert.match(
      studio,
      /\.df2-result-fidelity,\s*\n\.df2-data-integrity \{[\s\S]{0,200}--df-warning-bg/,
    );
    assert.doesNotMatch(
      studio,
      /\.df2-result-fidelity,\s*\n\.df2-data-integrity \{[\s\S]{0,200}#fffbeb/,
    );
    assert.doesNotMatch(
      studio,
      /\.df2-result-fidelity,\s*\n\.df2-data-integrity \{[\s\S]{0,200}#ffffff/,
    );

    assert.match(
      ui,
      /\.df2-result-dashboard \{\s*--df-result-pad-x: 16px;[\s\S]{0,80}--df-surface/,
    );
    assert.doesNotMatch(
      ui,
      /\.df2-result-dashboard \{\s*--df-result-pad-x: 16px;[\s\S]{0,160}255, 255, 255/,
    );
    assert.match(
      ui,
      /\.df2-result-dashboard\.is-warn,\s*\n\.df2-result-dashboard\.is-quarantine \{[\s\S]{0,200}--df-warning-bg/,
    );
    assert.doesNotMatch(ui, /\.df2-result-dashboard\.is-warn[\s\S]{0,200}#fffbeb/);
    assert.match(ui, /\.df2-result-dashboard\.is-success \{[\s\S]{0,200}--df-success-bg/);
    assert.match(ui, /\.df2-result-dashboard\.is-error \{[\s\S]{0,200}--df-danger-bg/);
    assert.match(ui, /\.df2-result-head \{[\s\S]{0,320}--df-surface/);
    assert.doesNotMatch(ui, /\.df2-result-head \{[\s\S]{0,320}255, 255, 255/);

    assert.match(premium, /\.df2-login-card \{[\s\S]{0,160}background: #fff;/);
    assert.match(premium, /\.df2-pilot-console-strip \{[\s\S]{0,240}--df-surface/);

    assert.match(ui, /\.df2-app \.df2-result-dashboard \{[\s\S]{0,80}--df-surface/);
    assert.match(ui, /\.df2-app \.df2-theater-rejected \{[\s\S]{0,80}--df-warning-bg/);
    assert.match(ui, /\.df2-app \.df2-result-fidelity,/);
    assert.match(ui, /\.df2-app \.df2-result-dashboard\.is-error \{[\s\S]{0,80}--df-danger-bg/);
  });

  it("Pilot, Proofs, Help, and job-action leftovers follow tokens", () => {
    const tokens = sheet("tokens.css");
    const pilot = sheet("pilot-chat.css");
    const dataflow = sheet("dataflow-ui.css");
    const premium = sheet("premium-theme.css");
    const benches = sheet("benchmarks.css");
    const docs = sheet("docs-page.css");
    const ui = sheet("enterprise-ui.css");

    assert.match(tokens, /--df-text:\s*var\(--df-text-primary\)/);
    assert.match(tokens, /--df-surface-2:\s*var\(--df-surface-muted\)/);

    assert.match(pilot, /\.df2-pilot-workspace\.df2-pilot-v2 \{[\s\S]{0,160}--df-surface/);
    assert.doesNotMatch(pilot, /\.df2-pilot-workspace\.df2-pilot-v2 \{[\s\S]{0,160}background: #fff;/);
    assert.match(pilot, /\.df2-pilot-v2 \.df2-pilot-composer-bar \{[\s\S]{0,200}--df-surface/);
    assert.match(pilot, /\.df2-pilot-v2 \.df2-pilot-msg\.assistant \{[\s\S]{0,160}--df-surface-muted/);
    assert.match(pilot, /\.df2-pilot-v2 \.df2-pilot-send \{[\s\S]{0,200}color: #fff;/);

    assert.match(dataflow, /\.df2-btn \{[\s\S]{0,160}--df-text-primary/);
    assert.doesNotMatch(dataflow, /\.df2-btn \{[\s\S]{0,160}#26312a/);
    assert.match(dataflow, /\.df2-pilot-idea \{[\s\S]{0,240}--df-surface/);
    assert.match(dataflow, /\.df2-pilot-capability \{[\s\S]{0,280}--df-surface/);
    assert.match(dataflow, /\.df2-pilot-quick button \{[\s\S]{0,200}--df-surface-raised/);

    assert.match(premium, /\.df2-pilot-console-strip \{[\s\S]{0,200}--df-surface/);
    assert.match(premium, /\.df2-login-card \{[\s\S]{0,160}background: #fff;/);

    assert.match(benches, /\.df2-page-benchmarks-table \{[\s\S]{0,160}--df-surface/);
    assert.doesNotMatch(benches, /\.df2-page-benchmarks-table \{[\s\S]{0,160}background: #fff;/);

    assert.match(docs, /\.df2-page-docs \.df2-docs-toc nav \{[\s\S]{0,200}--df-surface/);
    assert.doesNotMatch(docs, /\.df2-page-docs \.df2-docs-toc nav \{[\s\S]{0,200}255, 255, 255/);

    assert.match(ui, /\.df2-app \.df2-btn:not\(\.df2-btn-primary\):not\(\.df2-btn-danger\) \{[\s\S]{0,120}--df-text-primary/);
    assert.match(ui, /\.df2-app \.df2-jobs-detail-footer \.df2-btn:not\(\.df2-btn-primary\):not\(\.df2-btn-danger\),/);
    assert.match(ui, /\.df2-app \.df2-pilot-idea \{[\s\S]{0,80}--df-surface/);
    assert.match(ui, /\.df2-app \.df2-page-benchmarks-table th \{[\s\S]{0,80}--df-surface-muted/);
    assert.match(ui, /\.df2-app \.df2-page-docs \.df2-docs-toc nav \{[\s\S]{0,80}--df-surface/);

    const platform = sheet("enterprise-platform.css");
    assert.match(platform, /\.df2-pilot-main,[\s\S]{0,80}--df-surface/);
    assert.doesNotMatch(platform, /\.df2-pilot-main,[\s\S]{0,120}#f8fafc !important/);
  });

  it("Contracts list, portaled drawers, and Jobs detail buttons follow tokens", () => {
    const tokens = sheet("tokens.css");
    const ui = sheet("enterprise-ui.css");
    const connectors = sheet("connectors-page.css");
    const platform = sheet("enterprise-platform.css");
    const premium = sheet("premium-theme.css");

    assert.match(tokens, /--df-surface-subtle:\s*var\(--df-surface-muted\)/);
    assert.match(tokens, /--df-surface-sunken:\s*var\(--df-surface-muted\)/);
    assert.match(tokens, /--df-navy-700:\s*var\(--df-text-secondary\)/);

    assert.match(ui, /\.df2-contract-row \{\s*\n\s*min-height:[\s\S]{0,280}--df-surface/);
    assert.match(ui, /\.df2-contract-row-name \{[\s\S]{0,160}--df-text-primary/);
    assert.doesNotMatch(ui, /\.df2-contract-row-name \{[\s\S]{0,160}color: #0f172a/);
    assert.match(ui, /\.df2-contract-rows-head \{[\s\S]{0,280}--df-text-tertiary/);
    assert.doesNotMatch(ui, /\.df2-contract-rows-head \{[\s\S]{0,280}color: #94a3b8/);
    assert.match(ui, /\.df2-pipeline-row-name \{[\s\S]{0,160}--df-text-primary/);
    assert.doesNotMatch(ui, /\.df2-pipeline-row-name \{[\s\S]{0,160}color: #0f172a/);
    assert.match(ui, /\.df2-drawer-footer \{[\s\S]{0,160}--df-surface-muted/);
    assert.doesNotMatch(ui, /\.df2-drawer-footer \{[\s\S]{0,160}background: #fafbfc/);

    assert.match(connectors, /\.df2-connector-rows-head \{[\s\S]{0,220}--df-surface-muted/);
    assert.match(connectors, /\.df2-drawer-fact strong \{[\s\S]{0,80}--df-text-primary/);
    assert.doesNotMatch(connectors, /\.df2-drawer-fact strong \{[\s\S]{0,80}color: #0f172a/);
    assert.match(connectors, /\.df2-drawer-related-row \{[\s\S]{0,420}--df-surface/);
    assert.doesNotMatch(connectors, /\.df2-drawer-related-row \{[\s\S]{0,420}background: #fff;/);
    assert.match(connectors, /\.df2-drawer-related-main strong \{[\s\S]{0,80}--df-text-primary/);

    assert.match(platform, /\.df2-btn \{[\s\S]{0,80}--df-text-primary/);
    assert.doesNotMatch(platform, /\.df2-btn \{[\s\S]{0,80}--df-navy-700/);
    assert.match(platform, /\.df2-cell-title \{[\s\S]{0,80}--df-text-primary/);
    assert.doesNotMatch(platform, /\.df2-cell-title \{[\s\S]{0,80}#0f172a/);

    assert.match(premium, /\.df2-login-card \{[\s\S]{0,160}background: #fff;/);

    assert.match(
      ui,
      /\.df2-app \.df2-jobs-detail-card \.df2-btn:not\(\.df2-btn-primary\):not\(\.df2-btn-danger\),/,
    );
    assert.match(ui, /html\[data-theme="dark"\]:has\(\.df2-app\) \.df2-drawer,/);
    assert.match(ui, /html\[data-theme="dark"\]:has\(\.df2-app\) \.df2-drawer-footer,/);
    assert.match(
      ui,
      /html\[data-theme="dark"\]:has\(\.df2-app\) \.df2-drawer \.df2-btn:not\(\.df2-btn-primary\):not\(\.df2-btn-danger\),/,
    );
  });

  it("Modals, Schedules editor, empty-state, and brand chrome follow tokens", () => {
    const ui = sheet("enterprise-ui.css");
    const platform = sheet("enterprise-platform.css");
    const dataflow = sheet("dataflow-ui.css");
    const premium = sheet("premium-theme.css");

    assert.match(ui, /\.df2-modal-footer \{[\s\S]{0,200}--df-surface-muted/);
    assert.doesNotMatch(ui, /\.df2-modal-footer \{[\s\S]{0,200}background: #fafbfc/);
    assert.match(ui, /\.df2-empty-state \{[\s\S]{0,160}--df-surface-muted/);
    assert.doesNotMatch(ui, /\.df2-empty-state \{[\s\S]{0,160}background: #fafbfc/);
    assert.match(ui, /\.df2-jobs-v3-phase-pill \{[\s\S]{0,240}--df-surface-muted/);
    assert.doesNotMatch(ui, /\.df2-jobs-v3-phase-pill \{[\s\S]{0,240}background: #fafbfc/);
    assert.match(ui, /\.df2-jobs-v3-alert \{[\s\S]{0,240}--df-surface-muted/);
    assert.match(ui, /\.df2-sched-panel-head strong \{[\s\S]{0,80}--df-text-primary/);
    assert.doesNotMatch(ui, /\.df2-sched-panel-head strong \{[\s\S]{0,80}color: #0f172a/);
    assert.match(ui, /\.df2-sched-nextrun strong \{[\s\S]{0,80}--df-text-primary/);
    assert.match(ui, /\.df2-sched-switch-row strong \{[\s\S]{0,80}--df-text-primary/);
    assert.match(ui, /\.df2-sched-run-error p\.df2-sched-run-fix \{[\s\S]{0,80}--df-text-primary/);

    assert.match(platform, /\.df2-brand-name,[\s\S]{0,80}--df-text-primary/);
    assert.doesNotMatch(platform, /\.df2-brand-name,[\s\S]{0,80}#0f172a/);
    assert.match(dataflow, /\.df2-brand-name \{[\s\S]{0,120}--df-text-primary/);

    assert.match(premium, /\.df2-login-card \{[\s\S]{0,160}background: #fff;/);

    assert.match(ui, /html\[data-theme="dark"\]:has\(\.df2-app\) \.df2-modal,/);
    assert.match(ui, /html\[data-theme="dark"\]:has\(\.df2-app\) \.df2-modal-footer,/);
    assert.match(ui, /\.df2-app \.df2-sched-panel-head strong,/);
    assert.match(ui, /\.df2-app \.df2-empty-state,/);
  });

  it("Settings, Query hover, and tab leftovers follow tokens", () => {
    const ui = sheet("enterprise-ui.css");
    const dataflow = sheet("dataflow-ui.css");
    const settings = sheet("settings-enterprise.css");
    const workspace = sheet("settings-workspace.css");
    const query = sheet("query-playground.css");
    const premium = sheet("premium-theme.css");

    assert.match(dataflow, /\.df2-tab:hover \{[\s\S]{0,80}--df-text-primary/);
    assert.doesNotMatch(dataflow, /\.df2-tab:hover \{[\s\S]{0,80}#0f172a/);
    assert.match(dataflow, /\.df2-settings-nav button:hover \{[\s\S]{0,120}--df-surface-muted/);
    assert.doesNotMatch(dataflow, /\.df2-settings-nav button:hover \{[\s\S]{0,120}#f1f5f9/);
    assert.match(dataflow, /\.df2-catalog-nav button:hover \{[\s\S]{0,120}--df-surface-muted/);

    assert.match(settings, /\.df2-settings-summary-item strong \{[\s\S]{0,80}--df-text-primary/);
    assert.doesNotMatch(settings, /\.df2-settings-summary-item strong \{[\s\S]{0,80}color: #0f172a/);
    assert.match(settings, /\.df2-settings-section-footer \{[\s\S]{0,200}--df-surface-muted/);
    assert.doesNotMatch(settings, /\.df2-settings-section-footer \{[\s\S]{0,200}background: #fafbfc/);
    assert.match(settings, /\.df2-settings-policy-row \{[\s\S]{0,220}--df-surface-muted/);
    assert.match(settings, /\.df2-settings-policy-row h3 \{[\s\S]{0,80}--df-text-primary/);
    assert.match(settings, /\.df2-settings-sso-card \{[\s\S]{0,160}--df-surface-muted/);
    assert.match(settings, /\.df2-settings-field label \{[\s\S]{0,160}--df-text-secondary/);

    assert.match(workspace, /\.df2-page-settings \.df2-security-row \{[\s\S]{0,160}--df-surface-muted/);
    assert.match(workspace, /\.df2-settings-model-title \{[\s\S]{0,120}--df-text-primary/);
    assert.doesNotMatch(workspace, /\.df2-settings-model-title \{[\s\S]{0,120}color: #0f172a/);

    assert.match(query, /\.df2-query-editor-action:hover:not\(:disabled\) \{[\s\S]{0,160}--df-surface-muted/);
    assert.doesNotMatch(query, /\.df2-query-editor-action:hover:not\(:disabled\) \{[\s\S]{0,160}#f1f5f9/);

    assert.match(premium, /\.df2-login-card \{[\s\S]{0,160}background: #fff;/);

    assert.match(ui, /\.df2-app \.df2-settings-section-footer,/);
    assert.match(ui, /\.df2-app \.df2-settings-nav button:hover,/);
    assert.match(ui, /\.df2-app \.df2-settings-nav-item\.active,/);

    const polish = sheet("shell-polish.css");
    assert.match(polish, /\.df2-settings-nav button\.active \{[\s\S]{0,120}--df-brand-muted/);
    assert.doesNotMatch(polish, /\.df2-settings-nav button\.active \{[\s\S]{0,120}#f0fdfa !important/);
  });

  it("MCP titles, Query leftover mint chips, and Validate launch follow tokens", () => {
    const mcp = sheet("mcp-enterprise.css");
    const query = sheet("query-playground.css");
    const studio = sheet("transfer-studio.css");
    const ui = sheet("enterprise-ui.css");
    const premium = sheet("premium-theme.css");

    assert.match(mcp, /\.df2-mcp-hero-copy h2 \{[\s\S]{0,120}--df-text-primary/);
    assert.doesNotMatch(mcp, /\.df2-mcp-hero-copy h2 \{[\s\S]{0,120}color: #0f172a/);
    assert.match(mcp, /\.df2-mcp-hero-status strong \{[\s\S]{0,80}--df-text-primary/);
    assert.doesNotMatch(mcp, /\.df2-mcp-hero-status strong \{[\s\S]{0,80}color: #0f172a/);
    assert.match(mcp, /\.df2-mcp-logs-table th,[\s\S]{0,160}--df-border/);
    assert.doesNotMatch(mcp, /\.df2-mcp-logs-table th,[\s\S]{0,160}#f1f5f9/);
    assert.match(mcp, /\.df2-mcp-log-status--ok \{[\s\S]{0,80}--df-success/);
    assert.doesNotMatch(mcp, /\.df2-mcp-log-status--ok \{[\s\S]{0,80}#047857/);
    assert.match(mcp, /\.df2-mcp-log-status--err \{[\s\S]{0,80}--df-danger/);
    assert.match(mcp, /\.df2-mcp-tile code \{[\s\S]{0,160}--df-brand-strong/);
    assert.doesNotMatch(mcp, /\.df2-mcp-tile code \{[\s\S]{0,160}color: #0f766e/);
    assert.match(mcp, /\.df2-mcp-panel-head h2 \{[\s\S]{0,120}--df-text-primary/);

    assert.match(ui, /\.df2-mcp-status-pill\.is-online \{[\s\S]{0,120}--df-success/);
    assert.doesNotMatch(ui, /\.df2-mcp-status-pill\.is-online \{[\s\S]{0,120}#047857/);
    assert.doesNotMatch(ui, /\.df2-mcp-status-pill\.is-online \{[\s\S]{0,120}#a7f3d0/);
    assert.match(ui, /\.df2-mcp-status-pill\.is-offline \{[\s\S]{0,120}--df-warning/);

    assert.match(query, /\.df2-query-editor-dialect-pill \{[\s\S]{0,200}--df-brand-muted/);
    assert.doesNotMatch(query, /\.df2-query-editor-dialect-pill \{[\s\S]{0,200}#f0fdfa/);
    assert.match(query, /\.df2-qw-th-type\[data-tone="text"\] \{[\s\S]{0,80}--df-brand-muted/);
    assert.doesNotMatch(query, /\.df2-qw-th-type\[data-tone="text"\] \{[\s\S]{0,80}#f0fdfa/);

    assert.match(studio, /\.df2-validate-launch \{[\s\S]{0,160}--df-brand-muted/);
    assert.doesNotMatch(studio, /\.df2-validate-launch \{[\s\S]{0,160}#f0fdfa !important/);

    assert.match(premium, /\.df2-login-card \{[\s\S]{0,160}background: #fff;/);

    assert.match(ui, /\.df2-app \.df2-mcp-hero-copy h2,/);
    assert.match(ui, /\.df2-app \.df2-query-editor-dialect-pill,/);
    assert.match(ui, /\.df2-app \.df2-mcp-tile code \{[\s\S]{0,80}--df-brand-strong/);
    assert.match(ui, /\.df2-app \.df2-validate-launch \{[\s\S]{0,120}--df-brand-muted/);
    assert.match(ui, /\.df2-filter-bar \{[\s\S]{0,360}--df-seg-track/);
    assert.doesNotMatch(ui, /\.df2-filter-bar \{[\s\S]{0,360}background: #f1f5f9/);
    assert.match(ui, /\.df2-app \.df2-filter-bar \{[\s\S]{0,80}--df-seg-track/);
  });

  it("Query leftover pastel type chips follow tokens", () => {
    const query = sheet("query-playground.css");
    const ui = sheet("enterprise-ui.css");
    const premium = sheet("premium-theme.css");

    assert.match(query, /\.df2-qw-th-type\[data-tone="number"\] \{[\s\S]{0,80}--df-info-bg/);
    assert.doesNotMatch(query, /\.df2-qw-th-type\[data-tone="number"\] \{[\s\S]{0,80}#eff6ff/);
    assert.match(query, /\.df2-qw-th-type\[data-tone="time"\] \{[\s\S]{0,80}--df-brand-muted/);
    assert.doesNotMatch(query, /\.df2-qw-th-type\[data-tone="time"\] \{[\s\S]{0,80}#f5f3ff/);
    assert.match(query, /\.df2-qw-th-type\[data-tone="bool"\] \{[\s\S]{0,80}--df-warning-bg/);
    assert.doesNotMatch(query, /\.df2-qw-th-type\[data-tone="bool"\] \{[\s\S]{0,80}#fefce8/);
    assert.match(query, /\.df2-qw-th-type\[data-tone="struct"\] \{[\s\S]{0,80}--df-surface-muted/);
    assert.doesNotMatch(query, /\.df2-qw-th-type\[data-tone="struct"\] \{[\s\S]{0,80}#fdf4ff/);
    assert.match(
      query,
      /\.df2-page-query \.df2-query-results tbody tr:hover td \{[\s\S]{0,160}--df-surface/,
    );
    assert.doesNotMatch(
      query,
      /\.df2-page-query \.df2-query-results tbody tr:hover td \{[\s\S]{0,160}#fff/,
    );

    assert.match(premium, /\.df2-login-card \{[\s\S]{0,160}background: #fff;/);

    assert.match(ui, /\.df2-app \.df2-qw-col-type\[data-tone="number"\],/);
    assert.match(ui, /\.df2-app \.df2-qw-col-type\[data-tone="bool"\],/);
    assert.match(ui, /\.df2-app \.df2-page-query \.df2-query-results tbody tr:hover td \{[\s\S]{0,160}--df-surface/);
  });

  it("Help / docs leftover navy and mint follow tokens", () => {
    const docs = sheet("docs-page.css");
    const ui = sheet("enterprise-ui.css");
    const premium = sheet("premium-theme.css");

    assert.match(docs, /\.df2-docs-arch-title \{[\s\S]{0,80}--df-text-primary/);
    assert.doesNotMatch(docs, /\.df2-docs-arch-title \{[\s\S]{0,80}fill: #0f172a/);
    assert.match(docs, /\.df2-docs-walkthrough-steps \{[\s\S]{0,80}--df-text-secondary/);
    assert.doesNotMatch(docs, /\.df2-docs-walkthrough-steps \{[\s\S]{0,80}color: #334155/);
    assert.match(docs, /\.df2-docs-shot img \{[\s\S]{0,200}--df-surface-muted/);
    assert.doesNotMatch(docs, /\.df2-docs-shot img \{[\s\S]{0,200}background: #f8fafc/);
    assert.match(docs, /\.df2-docs-shot figcaption strong \{[\s\S]{0,80}--df-text-primary/);
    assert.doesNotMatch(docs, /\.df2-docs-shot figcaption strong \{[\s\S]{0,80}color: #0f172a/);

    assert.match(docs, /\.docs-sidebar-home \{[\s\S]{0,200}--df-brand-muted/);
    assert.doesNotMatch(docs, /\.docs-sidebar-home \{[\s\S]{0,200}background: #f0fdfa/);
    assert.match(docs, /\.docs-sidebar-group button \{[\s\S]{0,200}--df-text-secondary/);
    assert.doesNotMatch(docs, /\.docs-sidebar-group button \{[\s\S]{0,200}color: #334155/);
    assert.match(docs, /\.docs-sidebar-group button\.is-active \{[\s\S]{0,80}--df-brand-muted/);
    assert.doesNotMatch(docs, /\.docs-sidebar-group button\.is-active \{[\s\S]{0,80}#ecfdf5/);
    assert.match(docs, /\.docs-sidebar-brand strong \{[\s\S]{0,80}--df-text-primary/);
    assert.doesNotMatch(docs, /\.docs-sidebar-brand strong \{[\s\S]{0,80}color: #0f172a/);
    assert.match(docs, /\.docs-space-page-head h1 \{[\s\S]{0,200}--df-text-primary/);
    assert.doesNotMatch(docs, /\.docs-space-page-head h1 \{[\s\S]{0,200}color: #0f172a/);
    assert.match(docs, /\.docs-space-tree-group \{[\s\S]{0,160}--df-surface-muted/);
    assert.doesNotMatch(docs, /\.docs-space-tree-group \{[\s\S]{0,160}background: #fafbfc/);
    assert.match(docs, /\.docs-space-algorithm-steps strong \{[\s\S]{0,80}--df-text-primary/);
    assert.match(docs, /\.docs-callout \{[\s\S]{0,160}--df-brand-muted/);
    assert.doesNotMatch(docs, /\.docs-callout \{[\s\S]{0,160}background: #f0fdfa/);
    assert.match(docs, /\.docs-card-icon \{[\s\S]{0,200}--df-brand-muted/);
    assert.doesNotMatch(docs, /\.docs-card-icon \{[\s\S]{0,200}background: #f0fdfa/);

    assert.match(premium, /\.df2-login-card \{[\s\S]{0,160}background: #fff;/);

    assert.match(ui, /\.df2-app \.df2-docs-shot figcaption strong,/);
    assert.match(ui, /\.df2-app \.docs-space-page-head h1,/);
    assert.match(ui, /\.df2-app \.docs-sidebar-home,/);
    assert.match(ui, /\.df2-app \.docs-callout \{[\s\S]{0,80}--df-brand-muted/);
  });

  it("Studio leftover navy on Gate8, Validate, and Theater follows tokens", () => {
    const ui = sheet("enterprise-ui.css");
    const premium = sheet("premium-theme.css");

    assert.match(ui, /\.df2-gate8-proof-head h3 \{[\s\S]{0,80}--df-text-primary/);
    assert.doesNotMatch(ui, /\.df2-gate8-proof-head h3 \{[\s\S]{0,80}color: #0f172a/);
    assert.match(ui, /\.df2-gate8-proof-grid dd \{[\s\S]{0,160}--df-text-primary/);
    assert.doesNotMatch(ui, /\.df2-gate8-proof-grid dd \{[\s\S]{0,160}color: #0f172a/);
    assert.match(ui, /\.df2-gate8-proof-badge\.is-ok \{[\s\S]{0,80}--df-success-bg/);
    assert.doesNotMatch(ui, /\.df2-gate8-proof-badge\.is-ok \{[\s\S]{0,80}#d1fae5/);
    assert.match(ui, /\.df2-gate8-proof-badge\.is-bad \{[\s\S]{0,80}--df-danger-bg/);
    assert.match(ui, /\.df2-gate8-proof-badge\.is-pending \{[\s\S]{0,80}--df-warning-bg/);
    assert.match(ui, /\.df2-validate-stage-core h3 \{[\s\S]{0,80}--df-text-primary/);
    assert.doesNotMatch(ui, /\.df2-validate-stage-core h3 \{[\s\S]{0,80}color: #0f172a/);
    assert.match(ui, /\.df2-preflight\.is-compact \.df2-preflight-step-title \{[\s\S]{0,80}--df-text-primary/);
    assert.doesNotMatch(ui, /\.df2-preflight\.is-compact \.df2-preflight-step-title \{[\s\S]{0,80}color: #0f172a/);
    assert.match(ui, /\.df2-preflight\.is-compact \.df2-preflight-step\.running \{[\s\S]{0,80}--df-info-bg/);
    assert.doesNotMatch(ui, /\.df2-preflight\.is-compact \.df2-preflight-step\.running \{[\s\S]{0,80}#f0f9ff/);
    assert.match(ui, /\.df2-toolbar-gitops-toggle \{[\s\S]{0,280}--df-text-secondary/);
    assert.doesNotMatch(ui, /\.df2-toolbar-gitops-toggle \{[\s\S]{0,280}color: #475569/);
    assert.match(ui, /\.df2-toolbar-gitops-toggle:hover \{[\s\S]{0,80}--df-text-primary/);
    assert.doesNotMatch(ui, /\.df2-toolbar-gitops-toggle:hover \{[\s\S]{0,80}color: #0f172a/);
    assert.match(ui, /\.df2-theater-v3-progress-copy h3 \{[\s\S]{0,80}--df-text-primary/);
    assert.doesNotMatch(ui, /\.df2-theater-v3-progress-copy h3 \{[\s\S]{0,80}color: #0f172a/);
    assert.match(ui, /\.df2-run-readiness-route \{[\s\S]{0,160}--df-text-primary/);

    assert.match(premium, /\.df2-login-card \{[\s\S]{0,160}background: #fff;/);

    assert.match(ui, /\.df2-app \.df2-gate8-proof-head h3,/);
    assert.match(ui, /\.df2-app \.df2-validate-stage-core h3,/);
    assert.match(ui, /\.df2-app \.df2-toolbar-gitops-toggle:hover \{[\s\S]{0,40}--df-text-primary/);
  });

  it("Pilot leftover paper, navy, and mint follow tokens", () => {
    const ui = sheet("enterprise-ui.css");
    const app = sheet("app-styles.css");
    const responsive = sheet("responsive-shell-fixes.css");
    const polish = sheet("shell-polish.css");
    const premium = sheet("premium-theme.css");

    assert.match(ui, /\.df2-pilot-v2 \.df2-pilot-aside \{[\s\S]{0,280}--df-surface-muted/);
    assert.doesNotMatch(ui, /\.df2-pilot-v2 \.df2-pilot-aside \{[\s\S]{0,280}background: #fafbfc/);
    assert.match(ui, /\.df2-pilot-v2 \.df2-pilot-aside\.is-collapsed \{[\s\S]{0,80}--df-surface/);
    assert.doesNotMatch(ui, /\.df2-pilot-v2 \.df2-pilot-aside\.is-collapsed \{[\s\S]{0,80}background: #fff/);
    assert.match(ui, /\.df2-pilot-v2 \.df2-pilot-session-row\.active \.df2-pilot-session \{[\s\S]{0,80}--df-brand-strong/);
    assert.doesNotMatch(
      ui,
      /\.df2-pilot-v2 \.df2-pilot-session-row\.active \.df2-pilot-session \{[\s\S]{0,80}color: #0f766e/,
    );
    assert.match(ui, /\.df2-pilot-session-count \{[\s\S]{0,80}--df-surface-muted/);
    assert.doesNotMatch(ui, /\.df2-pilot-session-count \{[\s\S]{0,80}background: #f1f5f9/);
    assert.match(ui, /\.df2-pilot-pending-row \{[\s\S]{0,200}--df-surface-muted/);
    assert.doesNotMatch(ui, /\.df2-pilot-pending-row \{[\s\S]{0,200}background: #f8fafc/);
    assert.match(ui, /\.df2-pilot-pending-label \{[\s\S]{0,80}--df-text-primary/);
    assert.doesNotMatch(ui, /\.df2-pilot-pending-label \{[\s\S]{0,80}color: #0f172a/);
    assert.match(ui, /\.df2-pilot-followup \{[\s\S]{0,160}--df-surface/);
    assert.doesNotMatch(ui, /\.df2-pilot-followup \{[\s\S]{0,160}background: #fff;/);
    assert.match(ui, /\.df2-pilot-followup:hover \{[\s\S]{0,80}--df-brand-muted/);
    assert.doesNotMatch(ui, /\.df2-pilot-followup:hover \{[\s\S]{0,80}background: #f0fdfa/);
    assert.match(ui, /\.df2-pilot-v2 \.df2-pilot-tool-log \{[\s\S]{0,160}--df-surface-muted/);
    assert.doesNotMatch(ui, /\.df2-pilot-v2 \.df2-pilot-tool-log \{[\s\S]{0,160}background: #f8fafc/);

    assert.match(premium, /\.df2-login-card \{[\s\S]{0,160}background: #fff;/);

    assert.match(ui, /\.df2-app \.df2-pilot-pending-label \{/);
    assert.match(ui, /\.df2-app \.df2-pilot-followup \{/);
    assert.match(ui, /\.df2-app \.df2-pilot-followup:hover \{[\s\S]{0,80}--df-brand-muted/);
    assert.match(ui, /\.df2-app \.df2-pilot-pending-row,/);

    assert.match(app, /\.df2-app \.df2-pilot-aside\.is-collapsed \{[\s\S]{0,80}--df-surface/);
    assert.match(
      app,
      /\.df2-app \.df2-pilot-v2 \.df2-pilot-session-row\.active \.df2-pilot-session \{[\s\S]{0,80}--df-brand-strong/,
    );
    assert.match(polish, /\.df2-pilot-session\.active \{[\s\S]{0,160}--df-brand-muted/);
    assert.doesNotMatch(polish, /\.df2-pilot-session\.active \{[\s\S]{0,160}#f0fdfa/);
    assert.match(responsive, /\.df2-pilot-workspace\.df2-pilot-v2 \.df2-pilot-aside \{[\s\S]{0,280}--df-surface-muted/);
    assert.doesNotMatch(
      responsive,
      /\.df2-pilot-workspace\.df2-pilot-v2 \.df2-pilot-aside \{[\s\S]{0,280}background: #fafbfc/,
    );
    assert.match(polish, /\.df2-pilot-aside \{[\s\S]{0,160}--df-surface-muted/);
    assert.doesNotMatch(polish, /\.df2-pilot-aside \{[\s\S]{0,160}linear-gradient\(180deg, #fff/);
  });

  it("Jobs leftover navy, paper, and mint follow tokens", () => {
    const ui = sheet("enterprise-ui.css");
    const premium = sheet("premium-theme.css");

    assert.match(ui, /\.df2-jobs-evidence-chip \{[\s\S]{0,280}--df-text-primary/);
    assert.doesNotMatch(ui, /\.df2-jobs-evidence-chip \{[\s\S]{0,280}color: #0f172a/);
    assert.match(ui, /\.df2-jobs-evidence-chip:hover \{[\s\S]{0,80}--df-surface-muted/);
    assert.doesNotMatch(ui, /\.df2-jobs-evidence-chip:hover \{[\s\S]{0,80}background: #f8fafc/);
    assert.match(ui, /\.df2-jobs-explanation-prose \{[\s\S]{0,160}--df-text-primary/);
    assert.doesNotMatch(ui, /\.df2-jobs-explanation-prose \{[\s\S]{0,160}color: #0f172a/);
    assert.match(ui, /\.df2-adv-section-title \{[\s\S]{0,80}--df-text-primary/);
    assert.doesNotMatch(ui, /\.df2-adv-section-title \{[\s\S]{0,80}color: #0f172a/);
    assert.match(ui, /\.df2-adv-behavior-callout \{[\s\S]{0,160}--df-surface-muted/);
    assert.doesNotMatch(ui, /\.df2-adv-behavior-callout \{[\s\S]{0,160}background: #f8fafc/);
    assert.match(ui, /\.df2-adv-suggest-chip \{[\s\S]{0,160}--df-brand-muted/);
    assert.doesNotMatch(ui, /\.df2-adv-suggest-chip \{[\s\S]{0,160}background: #f0fdfa/);
    assert.doesNotMatch(ui, /\.df2-adv-suggest-chip \{[\s\S]{0,160}color: #0f766e/);

    assert.match(premium, /\.df2-login-card \{[\s\S]{0,160}background: #fff;/);

    assert.match(ui, /\.df2-app \.df2-jobs-evidence-chip \{[\s\S]{0,80}--df-text-primary/);
    assert.match(ui, /\.df2-app \.df2-jobs-explanation-prose,/);
    assert.match(ui, /\.df2-app \.df2-adv-suggest-chip,/);
    assert.match(ui, /\.df2-app \.df2-adv-behavior-callout \{[\s\S]{0,80}--df-surface-muted/);
  });

  it("Theater leftover navy, paper, and mint follow tokens", () => {
    const ui = sheet("enterprise-ui.css");
    const premium = sheet("premium-theme.css");

    assert.match(ui, /\.df2-theater-v3-endpoint strong \{[\s\S]{0,80}--df-text-primary/);
    assert.doesNotMatch(ui, /\.df2-theater-v3-endpoint strong \{[\s\S]{0,80}color: #0f172a/);
    assert.match(ui, /\.df2-theater-v3-arrow \{[\s\S]{0,40}--df-brand-strong/);
    assert.doesNotMatch(ui, /\.df2-theater-v3-arrow \{[\s\S]{0,40}color: #0f766e/);
    assert.match(ui, /\.df2-theater-v3-metric strong \{[\s\S]{0,80}--df-text-primary/);
    assert.doesNotMatch(ui, /\.df2-theater-v3-metric strong \{[\s\S]{0,80}color: #0f172a/);
    assert.match(ui, /\.df2-theater-lineage summary \{[\s\S]{0,80}--df-text-primary/);
    assert.doesNotMatch(ui, /\.df2-theater-lineage summary \{[\s\S]{0,80}color: #0f172a/);
    assert.match(ui, /\.df2-theater-v3-stream \{[\s\S]{0,200}--df-surface-muted/);
    assert.doesNotMatch(ui, /\.df2-theater-v3-stream \{[\s\S]{0,200}background: #f8fafc/);
    assert.match(ui, /\.df2-theater-v3-stream strong \{[\s\S]{0,40}--df-text-primary/);
    assert.match(ui, /\.df2-theater-pop-strip \{[\s\S]{0,200}--df-surface-muted/);
    assert.doesNotMatch(ui, /\.df2-theater-pop-strip \{[\s\S]{0,200}background: #f8fafc/);
    assert.match(ui, /\.df2-theater-v3-phase\.active \{[\s\S]{0,80}--df-brand-muted/);
    assert.doesNotMatch(ui, /\.df2-theater-v3-phase\.active \{[\s\S]{0,80}background: #ccfbf1/);
    assert.match(ui, /\.df2-theater-cdc-chip \{[\s\S]{0,160}--df-surface-muted/);
    assert.doesNotMatch(ui, /\.df2-theater-cdc-chip \{[\s\S]{0,160}background: #f1f5f9/);
    assert.match(ui, /\.df2-theater-v3-next-copy strong \{[\s\S]{0,40}--df-text-primary/);
    assert.doesNotMatch(ui, /\.df2-theater-v3-next-copy strong \{[\s\S]{0,40}color: #0f172a/);

    assert.match(premium, /\.df2-login-card \{[\s\S]{0,160}background: #fff;/);

    assert.match(ui, /\.df2-app \.df2-theater-v3-endpoint strong,/);
    assert.match(ui, /\.df2-app \.df2-theater-v3-arrow \{[\s\S]{0,40}--df-brand-strong/);
    assert.match(ui, /\.df2-app \.df2-theater-v3-phase\.active \{[\s\S]{0,80}--df-brand-muted/);
    assert.match(ui, /\.df2-app \.df2-theater-cdc-chip,/);
  });

  it("Result leftover navy, paper, and mint follow tokens", () => {
    const ui = sheet("enterprise-ui.css");
    const studio = sheet("transfer-studio.css");
    const premium = sheet("premium-theme.css");

    assert.match(ui, /\.df2-result-title \{[\s\S]{0,120}--df-text-primary/);
    assert.doesNotMatch(ui, /\.df2-result-title \{[\s\S]{0,120}color: #0f172a/);
    assert.match(studio, /\.df2-result-title \{[\s\S]{0,80}--df-text-primary/);
    assert.doesNotMatch(studio, /\.df2-result-title \{[\s\S]{0,80}color: #0f172a/);
    assert.match(ui, /\.df2-result-metric strong \{[\s\S]{0,120}--df-text-primary/);
    assert.doesNotMatch(ui, /\.df2-result-metric strong \{[\s\S]{0,120}color: #0f172a/);
    assert.match(ui, /\.df2-result-meta-chip strong \{[\s\S]{0,80}--df-text-primary/);
    assert.doesNotMatch(ui, /\.df2-result-meta-chip strong \{[\s\S]{0,80}color: #0f172a/);
    assert.match(ui, /\.df2-result-proof-dl dd \{[\s\S]{0,80}--df-text-primary/);
    assert.doesNotMatch(ui, /\.df2-result-proof-dl dd \{[\s\S]{0,80}color: #0f172a/);
    assert.match(ui, /\.df2-quarantine-inspect-body \{[\s\S]{0,200}--df-text-primary/);
    assert.doesNotMatch(ui, /\.df2-quarantine-inspect-body \{[\s\S]{0,200}color: #0f172a/);
    assert.match(ui, /\.df2-quarantine-next \{[\s\S]{0,200}--df-surface-muted/);
    assert.doesNotMatch(ui, /\.df2-quarantine-next \{[\s\S]{0,200}background: #f8fafc/);
    assert.match(ui, /\.df2-quarantine-next-copy strong \{[\s\S]{0,160}--df-text-secondary/);
    assert.doesNotMatch(ui, /\.df2-quarantine-next-copy strong \{[\s\S]{0,160}color: #334155/);
    assert.match(ui, /\.df2-result-more > summary \{[\s\S]{0,160}--df-text-secondary/);
    assert.doesNotMatch(ui, /\.df2-result-more > summary \{[\s\S]{0,160}color: #334155/);
    assert.match(ui, /\.df2-result-more\[open\] > summary \{[\s\S]{0,80}--df-surface-muted/);
    assert.doesNotMatch(ui, /\.df2-result-more\[open\] > summary \{[\s\S]{0,80}background: #f8fafc/);
    assert.match(ui, /\.df2-quarantine-apply-suggested \{[\s\S]{0,160}--df-text-secondary/);
    assert.doesNotMatch(ui, /\.df2-quarantine-apply-suggested \{[\s\S]{0,160}color: #475569/);
    assert.match(ui, /\.df2-quarantine-apply-suggested code \{[\s\S]{0,80}--df-brand-muted/);
    assert.doesNotMatch(ui, /\.df2-quarantine-apply-suggested code \{[\s\S]{0,80}background: #ecfdf5/);

    assert.match(premium, /\.df2-login-card \{[\s\S]{0,160}background: #fff;/);

    assert.match(ui, /\.df2-app \.df2-result-title,/);
    assert.match(ui, /\.df2-app \.df2-result-metric strong,/);
    assert.match(ui, /\.df2-app \.df2-quarantine-next,/);
    assert.match(ui, /\.df2-app \.df2-quarantine-apply-suggested code \{[\s\S]{0,80}--df-brand-muted/);
  });

  it("Job-row leftover mint and status pastels follow tokens", () => {
    const ui = sheet("enterprise-ui.css");
    const premium = sheet("premium-theme.css");

    assert.match(ui, /\.df2-job-row:hover \{[\s\S]{0,80}--df-list-row-hover/);
    assert.doesNotMatch(ui, /\.df2-job-row:hover \{[\s\S]{0,80}background: #f0fdfa/);
    assert.match(ui, /\.df2-job-row\.is-active,\n\.df2-job-row\.is-active:hover \{[\s\S]{0,80}--df-brand-soft/);
    assert.doesNotMatch(ui, /\.df2-job-row\.is-active,\n\.df2-job-row\.is-active:hover \{[\s\S]{0,80}background: #ccfbf1/);
    assert.match(ui, /\.df2-job-row-status \{[\s\S]{0,160}--df-surface-muted/);
    assert.doesNotMatch(ui, /\.df2-job-row-status \{[\s\S]{0,160}background: #f1f5f9/);
    assert.match(ui, /\.df2-job-row-status\.is-completed \{[\s\S]{0,80}--df-success-bg/);
    assert.doesNotMatch(ui, /\.df2-job-row-status\.is-completed \{[\s\S]{0,80}#dcfce7/);
    assert.match(ui, /\.df2-job-row-status\.is-failed \{[\s\S]{0,80}--df-danger-bg/);
    assert.match(ui, /\.df2-job-row-status\.is-running,\n\.df2-job-row-status\.is-pending \{[\s\S]{0,80}--df-brand-muted/);
    assert.doesNotMatch(
      ui,
      /\.df2-job-row-status\.is-running,\n\.df2-job-row-status\.is-pending \{[\s\S]{0,80}background: #ccfbf1/,
    );
    assert.match(ui, /\.df2-job-row-bar > i \{[\s\S]{0,80}--df-brand/);
    assert.doesNotMatch(ui, /\.df2-job-row-bar > i \{[\s\S]{0,80}linear-gradient\(90deg, #0f766e/);

    assert.match(premium, /\.df2-login-card \{[\s\S]{0,160}background: #fff;/);

    assert.match(ui, /\.df2-app \.df2-job-row:hover:not\(\.is-active\) \{[\s\S]{0,80}--df-list-row-hover/);
    assert.match(ui, /\.df2-app \.df2-job-row-status\.is-pending \{[\s\S]{0,80}--df-brand-muted/);
    assert.match(ui, /\.df2-app \.df2-job-row-bar > i \{[\s\S]{0,40}--df-brand/);
  });

  it("Validate leftover navy vd titles follow tokens", () => {
    const studio = sheet("transfer-studio.css");
    const ui = sheet("enterprise-ui.css");
    const premium = sheet("premium-theme.css");

    assert.match(studio, /\.df2-vd-remediation-log strong \{[\s\S]{0,80}--df-text-primary/);
    assert.doesNotMatch(studio, /\.df2-vd-remediation-log strong \{[\s\S]{0,80}color: #0f172a/);
    assert.match(studio, /\.df2-vd-remediation-log li \{[\s\S]{0,160}--df-text-secondary/);
    assert.doesNotMatch(studio, /\.df2-vd-remediation-log li \{[\s\S]{0,160}color: #334155/);
    assert.match(studio, /\.df2-vd-count strong \{[\s\S]{0,80}--df-text-primary/);
    assert.doesNotMatch(studio, /\.df2-vd-count strong \{[\s\S]{0,80}color: #0f172a/);
    assert.match(studio, /\.df2-vd-count\.ok strong \{[\s\S]{0,40}--df-success/);
    assert.match(studio, /\.df2-vd-count\.block strong \{[\s\S]{0,40}--df-danger/);
    assert.match(studio, /\.df2-vd-rules-head strong \{[\s\S]{0,80}--df-text-primary/);
    assert.doesNotMatch(studio, /\.df2-vd-rules-head strong \{[\s\S]{0,80}color: #0f172a/);
    assert.match(studio, /\.df2-vd-rule-label \{[\s\S]{0,80}--df-text-primary/);
    assert.doesNotMatch(studio, /\.df2-vd-rule-label \{[\s\S]{0,80}color: #0f172a/);
    assert.match(studio, /\.df2-vd-blockers li strong \{[\s\S]{0,80}--df-text-primary/);
    assert.match(studio, /\.df2-vd-assist-summary \{[\s\S]{0,80}--df-text-primary/);
    assert.match(studio, /\.df2-vd-coerce-head strong \{[\s\S]{0,80}--df-text-primary/);
    assert.match(studio, /\.df2-vd-coerce-col strong \{[\s\S]{0,80}--df-text-primary/);
    assert.match(studio, /\.df2-vd-explain-issues strong \{[\s\S]{0,80}--df-text-primary/);
    assert.doesNotMatch(studio, /\.df2-vd-explain-issues strong \{[\s\S]{0,80}color: #0f172a/);
    assert.match(studio, /\.df2-vd-explain-issues p \{[\s\S]{0,80}--df-text-secondary/);
    assert.doesNotMatch(studio, /\.df2-vd-explain-issues p \{[\s\S]{0,80}color: #334155/);

    assert.match(premium, /\.df2-login-card \{[\s\S]{0,160}background: #fff;/);

    assert.match(ui, /\.df2-app \.df2-vd-remediation-log strong,/);
    assert.match(ui, /\.df2-app \.df2-vd-rule-label,/);
    assert.match(ui, /\.df2-app \.df2-vd-explain-issues strong \{[\s\S]{0,80}--df-text-primary/);
    assert.match(ui, /\.df2-app \.df2-vd-count\.ok strong \{[\s\S]{0,40}--df-success/);
    assert.match(ui, /\.df2-app \.df2-vd-count\.block strong \{[\s\S]{0,40}--df-danger/);
  });

  it("Job-timeline leftover mint and status pastels follow tokens", () => {
    const ui = sheet("enterprise-ui.css");
    const premium = sheet("premium-theme.css");

    assert.match(ui, /\.df2-job-timeline-dot \{[\s\S]{0,240}--df-surface-muted/);
    assert.doesNotMatch(ui, /\.df2-job-timeline-dot \{[\s\S]{0,240}background: #f1f5f9/);
    assert.match(ui, /\.df2-job-timeline-item\.is-done \.df2-job-timeline-dot \{[\s\S]{0,80}--df-success-bg/);
    assert.doesNotMatch(ui, /\.df2-job-timeline-item\.is-done \.df2-job-timeline-dot \{[\s\S]{0,80}#dcfce7/);
    assert.match(ui, /\.df2-job-timeline-item\.is-failed \.df2-job-timeline-dot \{[\s\S]{0,80}--df-danger-bg/);
    assert.match(ui, /\.df2-job-timeline-item\.is-active \.df2-job-timeline-dot \{[\s\S]{0,80}--df-brand-muted/);
    assert.doesNotMatch(ui, /\.df2-job-timeline-item\.is-active \.df2-job-timeline-dot \{[\s\S]{0,80}#ccfbf1/);
    assert.match(ui, /\.df2-job-timeline-item\.is-skipped \.df2-job-timeline-dot \{[\s\S]{0,80}--df-warning-bg/);
    assert.match(ui, /\.df2-job-timeline-item\.is-warning \.df2-job-timeline-dot \{[\s\S]{0,80}--df-warning-bg/);
    assert.match(ui, /\.df2-job-timeline-item\.is-warning strong \{[\s\S]{0,40}--df-warning/);

    assert.match(premium, /\.df2-login-card \{[\s\S]{0,160}background: #fff;/);

    assert.match(ui, /\.df2-app \.df2-job-timeline-dot \{[\s\S]{0,80}--df-surface-muted/);
    assert.match(ui, /\.df2-app \.df2-job-timeline-item\.is-active \.df2-job-timeline-dot \{[\s\S]{0,80}--df-brand-muted/);
    assert.match(ui, /\.df2-app \.df2-job-timeline-item\.is-warning strong \{[\s\S]{0,40}--df-warning/);
  });

  it("Validate leftover paper vd-count chips follow tokens", () => {
    const studio = sheet("transfer-studio.css");
    const ui = sheet("enterprise-ui.css");
    const premium = sheet("premium-theme.css");

    assert.match(studio, /\.df2-vd-count \{[\s\S]{0,200}--df-surface-muted/);
    assert.doesNotMatch(studio, /\.df2-vd-count \{[\s\S]{0,200}background: #f1f5f9/);
    assert.match(studio, /\.df2-vd-count \{[\s\S]{0,200}--df-text-secondary/);
    assert.doesNotMatch(studio, /\.df2-vd-count \{[\s\S]{0,200}color: #475569/);
    assert.match(studio, /\.df2-vd-count\.ok \{[\s\S]{0,80}--df-success-bg/);
    assert.doesNotMatch(studio, /\.df2-vd-count\.ok \{[\s\S]{0,80}#f0fdf4/);
    assert.match(studio, /\.df2-vd-count\.block \{[\s\S]{0,80}--df-danger-bg/);
    assert.doesNotMatch(studio, /\.df2-vd-count\.block \{[\s\S]{0,80}#fef2f2/);
    assert.match(studio, /\.df2-vd-count\.skip \{[\s\S]{0,80}--df-surface-muted/);
    assert.doesNotMatch(studio, /\.df2-vd-count\.skip \{[\s\S]{0,80}#f8fafc/);

    assert.match(premium, /\.df2-login-card \{[\s\S]{0,160}background: #fff;/);

    assert.match(ui, /\.df2-app \.df2-vd-count \{[\s\S]{0,80}--df-surface-muted/);
    assert.match(ui, /\.df2-app \.df2-vd-count\.ok \{[\s\S]{0,40}--df-success-bg/);
    assert.match(ui, /\.df2-app \.df2-vd-count\.block \{[\s\S]{0,40}--df-danger-bg/);
    assert.match(ui, /\.df2-app \.df2-vd-count\.skip \{[\s\S]{0,40}--df-surface-muted/);
  });

  it("Pipeline-row leftover mint open control follows tokens", () => {
    const ui = sheet("enterprise-ui.css");
    const premium = sheet("premium-theme.css");

    assert.match(
      ui,
      /\.df2-pipeline-row:hover \.df2-pipeline-row-open,\n\.df2-pipeline-row\.selected \.df2-pipeline-row-open \{[\s\S]{0,80}--df-brand-strong/,
    );
    assert.match(
      ui,
      /\.df2-pipeline-row:hover \.df2-pipeline-row-open,\n\.df2-pipeline-row\.selected \.df2-pipeline-row-open \{[\s\S]{0,80}--df-brand-muted/,
    );
    assert.doesNotMatch(
      ui,
      /\.df2-pipeline-row:hover \.df2-pipeline-row-open,\n\.df2-pipeline-row\.selected \.df2-pipeline-row-open \{[\s\S]{0,80}#f0fdfa/,
    );
    assert.doesNotMatch(
      ui,
      /\.df2-pipeline-row:hover \.df2-pipeline-row-open,\n\.df2-pipeline-row\.selected \.df2-pipeline-row-open \{[\s\S]{0,80}#0f766e/,
    );

    assert.match(premium, /\.df2-login-card \{[\s\S]{0,160}background: #fff;/);

    assert.match(ui, /\.df2-app \.df2-pipeline-row:hover \.df2-pipeline-row-open,/);
    assert.match(
      ui,
      /\.df2-app \.df2-pipeline-row\.selected \.df2-pipeline-row-open \{[\s\S]{0,80}--df-brand-muted/,
    );
  });

  it("Dest leftover navy and paper flatteners follow tokens", () => {
    const studio = sheet("transfer-studio.css");
    const ui = sheet("enterprise-ui.css");
    const premium = sheet("premium-theme.css");

    assert.match(studio, /\.df2-dest-connector-card-name \{[\s\S]{0,200}--df-text-primary/);
    assert.doesNotMatch(studio, /\.df2-dest-connector-card-name \{[\s\S]{0,200}color: #0f172a/);
    assert.match(
      studio,
      /\.df2-page-transfer-studio \.df2-dest-connector-card-name \{[\s\S]{0,120}--df-text-primary/,
    );
    assert.doesNotMatch(
      studio,
      /\.df2-page-transfer-studio \.df2-dest-connector-card-name \{[\s\S]{0,120}color: #0f172a/,
    );
    assert.match(
      studio,
      /\.df2-page-transfer-studio \.df2-dest-engine-panel \{[\s\S]{0,480}--df-surface-muted/,
    );
    assert.doesNotMatch(
      studio,
      /\.df2-page-transfer-studio \.df2-dest-engine-panel \{[\s\S]{0,480}background: #f8fafc/,
    );
    assert.match(
      studio,
      /\.df2-page-transfer-studio \.df2-dest-engine-search input \{[\s\S]{0,360}--df-text-primary/,
    );
    assert.doesNotMatch(
      studio,
      /\.df2-page-transfer-studio \.df2-dest-engine-search input \{[\s\S]{0,360}color: #0f172a/,
    );
    assert.match(
      studio,
      /\.df2-page-transfer-studio \.df2-dest-engine-search-clear:hover \{[\s\S]{0,80}--df-surface-muted/,
    );
    assert.doesNotMatch(
      studio,
      /\.df2-page-transfer-studio \.df2-dest-engine-search-clear:hover \{[\s\S]{0,80}#f1f5f9/,
    );
    assert.match(
      studio,
      /\.df2-page-transfer-studio \.df2-dest-right-empty \{[\s\S]{0,200}--df-surface-muted/,
    );
    assert.doesNotMatch(
      studio,
      /\.df2-page-transfer-studio \.df2-dest-right-empty \{[\s\S]{0,200}background: #f8fafc/,
    );

    assert.match(premium, /\.df2-login-card \{[\s\S]{0,160}background: #fff;/);

    assert.match(ui, /\.df2-app \.df2-page-transfer-studio \.df2-dest-connector-card-name,/);
    assert.match(ui, /\.df2-app \.df2-page-transfer-studio \.df2-dest-engine-panel,/);
    assert.match(
      ui,
      /\.df2-app \.df2-page-transfer-studio \.df2-dest-right-empty \{[\s\S]{0,80}--df-text-secondary/,
    );
  });

  it("Pipe-card and toolbar leftover mint follow tokens", () => {
    const ui = sheet("enterprise-ui.css");
    const premium = sheet("premium-theme.css");

    assert.match(ui, /\.df2-pipe-card-arrow \{[\s\S]{0,200}--df-brand-muted/);
    assert.doesNotMatch(ui, /\.df2-pipe-card-arrow \{[\s\S]{0,200}background: #f0fdfa/);
    assert.match(ui, /\.df2-pipe-card-arrow \{[\s\S]{0,200}--df-brand-strong/);
    assert.match(ui, /\.df2-toolbar-status \{[\s\S]{0,240}--df-brand-muted/);
    assert.doesNotMatch(ui, /\.df2-toolbar-status \{[\s\S]{0,240}background: #f0fdfa/);
    assert.match(ui, /\.df2-toolbar-status \{[\s\S]{0,240}--df-brand-strong/);
    assert.doesNotMatch(ui, /\.df2-toolbar-status \{[\s\S]{0,240}color: #0f766e/);
    assert.match(ui, /\.df2-toolbar-status::before \{[\s\S]{0,160}--df-brand/);
    assert.doesNotMatch(ui, /\.df2-toolbar-status::before \{[\s\S]{0,160}#14b8a6/);

    assert.match(premium, /\.df2-login-card \{[\s\S]{0,160}background: #fff;/);

    assert.match(ui, /\.df2-app \.df2-pipe-card-arrow \{[\s\S]{0,80}--df-brand-muted/);
    assert.match(ui, /\.df2-app \.df2-toolbar-status \{[\s\S]{0,80}--df-brand-muted/);
    assert.match(ui, /\.df2-app \.df2-toolbar-status::before \{[\s\S]{0,40}--df-brand/);
  });

  it("Pilot leftover mint active category tabs follow tokens", () => {
    const ui = sheet("enterprise-ui.css");
    const premium = sheet("premium-theme.css");

    assert.match(
      ui,
      /\.df2-pilot-v2 \.df2-pilot-categories \.df2-tab\.is-active,[\s\S]{0,160}--df-brand-muted/,
    );
    assert.doesNotMatch(
      ui,
      /\.df2-pilot-v2 \.df2-pilot-categories \.df2-tab\.is-active,[\s\S]{0,160}#f0fdfa/,
    );
    assert.match(
      ui,
      /\.df2-pilot-v2 \.df2-pilot-categories button\.is-active \{[\s\S]{0,80}--df-brand-strong/,
    );
    assert.doesNotMatch(
      ui,
      /\.df2-pilot-v2 \.df2-pilot-categories button\.is-active \{[\s\S]{0,80}#0f766e/,
    );

    assert.match(premium, /\.df2-login-card \{[\s\S]{0,160}background: #fff;/);

    assert.match(ui, /\.df2-app \.df2-pilot-v2 \.df2-pilot-categories \.df2-tab\.is-active,/);
    assert.match(
      ui,
      /\.df2-app \.df2-pilot-v2 \.df2-pilot-categories button\.is-active \{[\s\S]{0,80}--df-brand-muted/,
    );
  });
});
