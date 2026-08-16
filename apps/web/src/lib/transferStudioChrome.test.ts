/**
 * Transfer Studio chrome contracts (no browser).
 * Run: npx --yes tsx --test apps/web/src/lib/transferStudioChrome.test.ts
 *
 * Playwright is not wired in apps/web yet — these guard the operator-facing
 * remediations and label contracts that caused Validate false-green + stepper clutter.
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import {
  findDuplicateKeyRoot,
  isDuplicateIdentitySignal,
} from "./validateIssueGrouping.js";
import { totalPages } from "./columnWorkbench.js";
import type { PreflightResult } from "./types.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const webRoot = join(__dirname, "..");

function basePreflight(over: Partial<PreflightResult> = {}): PreflightResult {
  return {
    passed: false,
    passed_count: 10,
    total_gates: 13,
    readiness_score: 76.9,
    gates: [],
    blockers: [],
    ...over,
  };
}

describe("Transfer Studio chrome contracts", () => {
  it("WizardSteps exposes shortLabel for density breakpoints", () => {
    const src = readFileSync(join(webRoot, "components/ui/WizardSteps.tsx"), "utf8");
    assert.match(src, /wizard-label-short/);
    assert.match(src, /shortLabel/);
  });

  it("enterprise CSS enables short labels ≤1440px (overrides hide)", () => {
    const css = readFileSync(join(webRoot, "styles/enterprise-ui.css"), "utf8");
    assert.match(css, /@media \(max-width: 1440px\)/);
    assert.match(css, /wizard-label-short \{\s*display: inline !important;/s);
    assert.match(css, /validate-rail-primary-fix[\s\S]*width: 100% !important/);
    assert.match(
      css,
      /validate-rail-actions \{\s*[\s\S]*flex-direction: column !important/,
    );
  });

  it("laptop density uses tokens at 100% browser zoom — never CSS zoom", () => {
    const css = readFileSync(join(webRoot, "styles/tokens.css"), "utf8");
    assert.doesNotMatch(css, /\.df2-app\s*\{[^}]*\bzoom\s*:/);
    assert.match(css, /@media \(max-width: 1512px\)/);
    assert.match(css, /--df-sidebar-width:\s*204px/);
    assert.match(css, /--df-btn-height:\s*32px/);
    assert.match(css, /--df-layout-viewport-h:\s*100dvh/);
  });

  it("shell reserves sidebar with padding so main cannot spill past the right edge", () => {
    const css = readFileSync(join(webRoot, "styles/shell-polish.css"), "utf8");
    assert.match(css, /padding-left:\s*var\(--df-sidebar-width/);
    assert.match(css, /\.df2-app \.df2-main \{[\s\S]*margin-left:\s*0\s*!important/);
  });

  it("transfer-studio stacks chrome via container query + 1280 fallback", () => {
    const css = readFileSync(join(webRoot, "styles/transfer-studio.css"), "utf8");
    assert.match(css, /container-type:\s*inline-size/);
    assert.match(css, /@container studio-chrome \(max-width: 900px\)/);
    assert.match(css, /@media \(max-width: 1280px\)/);
    assert.match(css, /df2-transfer-studio-chrome-row[\s\S]*flex-direction: column/);
    assert.match(css, /validate-rail-primary-fix[\s\S]*width:\s*100%/);
  });

  it("ValidateActionsRail labels review-grade passes as Review-grade, not ready", () => {
    const src = readFileSync(join(webRoot, "components/transfer/ValidateActionsRail.tsx"), "utf8");
    assert.match(src, /reviewGrade/);
    assert.match(src, /statusLabel/);
    assert.match(src, /reviewGrade\s*\n\s*\? "Review-grade"/);
    assert.match(src, /executeDisabled[\s\S]*reviewGrade/);
    assert.match(src, /Execute \(review\)/);
    assert.match(src, /Review-grade \/ local preflight/);
    assert.doesNotMatch(src, /span>\{passed \? "ready" : "blocked"\}/);
  });

  it("ValidateDashboard surfaces review subtitle when passed", () => {
    const src = readFileSync(join(webRoot, "components/transfer/ValidateDashboard.tsx"), "utf8");
    assert.match(src, /decision === "review"[\s\S]*executiveSummary\?\.subtitle/);
  });

  it("Map step keeps Continue in the wizard footer and pages columns when needed", () => {
    const mapStep = readFileSync(join(webRoot, "pages/transfer/TransferMapStep.tsx"), "utf8");
    const review = readFileSync(join(webRoot, "components/ColumnReviewPanel.tsx"), "utf8");
    const studio = readFileSync(join(webRoot, "styles/transfer-studio.css"), "utf8");
    const workbench = readFileSync(join(webRoot, "styles/column-workbench.css"), "utf8");

    assert.match(mapStep, /\{continueToValidate\}/);
    assert.match(mapStep, /Continue to Validate →/);
    assert.doesNotMatch(mapStep, /footerAction=/);
    assert.doesNotMatch(review, /footerAction/);
    assert.match(review, /pages > 1 &&/);
    assert.match(review, /df2-column-review-pager/);
    assert.match(review, /Mapping column pages/);
    assert.doesNotMatch(review, /compact && sampleRows && sampleRows\.length > 0 \? "is-split"/);

    assert.match(
      studio,
      /df2-map-step-panel > \.df2-card-footer\.df2-wizard-footer\.df2-map-footer \{[\s\S]*max-height:\s*none !important/,
    );
    assert.match(studio, /df2-map-step-panel \.df2-card-body,[\s\S]*padding:\s*4px 8px 0 !important/);
    assert.match(studio, /grid-template-rows:\s*minmax\(0, 1fr\) !important/);
    assert.match(
      workbench,
      /df2-map-step-workspace\.is-full-editor \{[\s\S]*grid-template-rows:\s*minmax\(0, 1fr\) !important/,
    );
    assert.doesNotMatch(workbench, /minmax\(0, min\(42vh, 380px\)\)/);
    assert.doesNotMatch(workbench, /header-height, 60px\) - 40px/);
    assert.equal(totalPages(8, 50), 1);
    assert.equal(totalPages(51, 50), 2);
    assert.equal(totalPages(100, 25), 4);
  });

  it("source-probe duplicate signal is recognized for Fix routing", () => {
    assert.equal(
      isDuplicateIdentitySignal("id: duplicate key values from source probe (a×4)"),
      true,
    );
    const pf = basePreflight({
      gates: [
        {
          id: "g9_data_integrity",
          status: "block",
          message: "id: duplicate key values from source probe (a×4)",
          duration_ms: 1,
          details: { primary_key: "id", issue_texts: ["id: duplicate key values from source probe (a×4)"] },
        },
      ],
      blockers: [
        {
          id: "g9_data_integrity",
          message: "Data integrity failed: id: duplicate key values from source probe",
          details: { primary_key: "id" },
        },
      ],
    });
    const root = findDuplicateKeyRoot(pf, "upsert");
    assert.ok(root);
    assert.equal(root!.primaryKey, "id");
    assert.match(root!.fixHint, /Primary key|unique column|dedupe/i);
  });

  it("workspace list rows share compact density — Connectors match Contracts/Schedules", () => {
    const tokens = readFileSync(join(webRoot, "styles/tokens.css"), "utf8");
    const consistency = readFileSync(join(webRoot, "styles/ui-consistency.css"), "utf8");
    const connectors = readFileSync(join(webRoot, "styles/connectors-page.css"), "utf8");
    const enterprise = readFileSync(join(webRoot, "styles/enterprise-ui.css"), "utf8");
    const card = readFileSync(join(webRoot, "components/ui/ConnectorCard.tsx"), "utf8");

    assert.match(tokens, /--df-list-row-min-h:\s*48px/);
    assert.match(tokens, /--df-list-row-pad-y:\s*8px/);
    assert.match(tokens, /--df-list-row-title:\s*13px/);
    assert.match(tokens, /@media \(min-width: 1920px\)/);
    // Row density is keyed on width alone. A `max-height` rule used to shrink
    // rows again on short viewports, on top of the width rules, which is how a
    // 1280x800 laptop ended up with a 38px row. The full ladder — no overlaps,
    // no value below the floor — is asserted in styles/listRowDensity.test.ts.
    assert.doesNotMatch(
      tokens,
      /@media \(max-height:[^)]*\)\s*\{[^}]*--df-list-row-/,
      'list-row density must not depend on viewport height',
    );
    assert.doesNotMatch(consistency, /min-height:\s*196px/);
    assert.match(connectors, /\.df2-connector-row \{[\s\S]*min-height:\s*var\(--df-list-row-min-h/);
    assert.match(enterprise, /\.df2-contract-row \{[\s\S]*min-height:\s*var\(--df-list-row-min-h/);
    assert.match(enterprise, /\.df2-pipeline-row \{[\s\S]*min-height:\s*var\(--df-list-row-min-h/);
    assert.match(enterprise, /\.df2-connector-row,\s*\n\s*\.df2-contract-row,\s*\n\s*\.df2-pipeline-row/);
    assert.match(card, /df2-btn-label/);
    assert.match(card, /size=\{16\}/);
  });
});
