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
});
