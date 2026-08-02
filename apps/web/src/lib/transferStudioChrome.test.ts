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
    assert.match(css, /validate-rail-primary-fix[\s\S]*width: auto !important/);
  });

  it("tokens apply laptop UI scale (~80%) at 100% browser zoom", () => {
    const css = readFileSync(join(webRoot, "styles/tokens.css"), "utf8");
    assert.match(css, /--df-ui-scale:\s*0\.8/);
    assert.match(css, /\.df2-app \{\s*zoom: var\(--df-ui-scale\)/s);
    assert.match(css, /@media \(max-width: 1512px\)/);
  });

  it("transfer-studio stacks chrome via container query + 1280 fallback", () => {
    const css = readFileSync(join(webRoot, "styles/transfer-studio.css"), "utf8");
    assert.match(css, /container-type:\s*inline-size/);
    assert.match(css, /@container studio-chrome \(max-width: 900px\)/);
    assert.match(css, /@media \(max-width: 1280px\)/);
    assert.match(css, /df2-transfer-studio-chrome-row[\s\S]*flex-direction: column/);
    assert.match(css, /validate-rail-primary-fix[\s\S]*width: auto/);
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
