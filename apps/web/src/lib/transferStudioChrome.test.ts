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

  it("the pre-load step is named for the operator, never 'Shape'", () => {
    const constants = readFileSync(join(webRoot, "pages/transfer/studioConstants.ts"), "utf8");
    const inspector = readFileSync(join(webRoot, "components/transfer/TransferStudioInspector.tsx"), "utf8");
    const page = readFileSync(join(webRoot, "pages/TransferPage.tsx"), "utf8");

    assert.match(constants, /label: "Transform", shortLabel: "Xform"/);
    assert.doesNotMatch(constants, /label: "Shape"/);
    assert.match(inspector, /title: "Transform \(pre-load\)"/);
    assert.match(page, /Continue to Transform/);
    assert.doesNotMatch(page, /Continue to Shape/);
  });

  it("Validate is asked about the transformed rows, not the raw source", () => {
    // The run shapes on the read, so a Validate that scores the source refuses
    // values the write never carries (a stripped control character, a rounded
    // decimal). Both calls must carry the same recipe.
    const page = readFileSync(join(webRoot, "pages/TransferPage.tsx"), "utf8");
    const api = readFileSync(join(webRoot, "lib/api.ts"), "utf8");

    const preflightCall = page.slice(page.indexOf("pf = await runPreflight("));
    assert.match(
      preflightCall.slice(0, preflightCall.indexOf("});")),
      /shape_recipe: recipePayload\(shapeSteps\)/,
      "runPreflight must send the approved recipe",
    );
    assert.match(api, /shape_recipe\?: ShapeRecipeWire/);
  });

  it("Validate renders the transformed image it was judged on", () => {
    // The backend already returned `transform_image`; an operator who cannot see
    // it has no way to know which rows the verdict covers, or that the rows the
    // recipe removed are absent by instruction rather than lost.
    const dash = readFileSync(join(webRoot, "components/transfer/ValidateDashboard.tsx"), "utf8");
    const types = readFileSync(join(webRoot, "lib/types.ts"), "utf8");
    const css = readFileSync(join(webRoot, "styles/transfer-studio.css"), "utf8");

    assert.match(types, /transform_image\?: \{/);
    assert.match(dash, /preflight\?\.transform_image \?\? null/);
    assert.match(dash, /Gates judged the transformed rows, not the raw source/);
    assert.match(dash, /recipe \{transformImage\.recipe_hash/);
    assert.match(dash, /removed by\s*\n?\s*transform/);
    assert.match(dash, /diverted by\s*\n?\s*transform/);
    assert.match(dash, /Re-read carrier\(s\) after transform/);
    // Sample counts are not population proof, and the panel must say so.
    assert.match(dash, /never the whole\s*\n?\s*population/);
    assert.ok(css.includes(".df2-vd-xform {"), ".df2-vd-xform has no rule");
  });

  it("Map decides carriers from the transformed image, and the plan keeps source truth", () => {
    // Transform runs before Map by design: a column rounded to whole numbers is
    // no longer a lossy decimal, so mapping it against the raw carrier explains
    // a narrowing the write will never perform. The persisted plan must stay raw
    // — the engine applies the recipe once, on the read.
    const page = readFileSync(join(webRoot, "pages/TransferPage.tsx"), "utf8");
    const step = readFileSync(join(webRoot, "pages/transfer/TransferTransformStep.tsx"), "utf8");
    const router = readFileSync(
      join(webRoot, "../../api/src/routers/shape_router.py"),
      "utf8",
    );

    const mapCalls = page.slice(page.indexOf("const useDirect ="));
    const mapBody = mapCalls.slice(0, mapCalls.indexOf("// Do NOT create an empty draft plan"));
    assert.match(mapBody, /source_columns: mapSourceCols/);
    assert.match(mapBody, /source_schema: mapSourceSchema/);
    // The plan override keeps declared truth so the recipe is not applied twice.
    assert.match(mapBody, /source_schema: declaredSchema/);
    assert.doesNotMatch(mapBody, /source_schema: mapSourceSchema,\s*\n\s*target_columns: mapTargetCols,/);
    // Sampled values shown to the mapping engine follow the transform too.
    assert.match(page, /image\.sampleRows\s*\n?\s*\.slice\(0, 8\)/);
    // The carrier of a transformed column is re-read from the transformed rows.
    assert.match(step, /column_types: sourceSchema/);
    assert.match(router, /out_types, retyped = shaped_column_types\(/);
  });

  it("the cell-level preview scans the transformed values, not the raw cell", () => {
    // `/preflight/run` carried the recipe while `/preflight/preview-cells` did
    // not, so the quarantine table cited `Invalid integer: '22.43'` on a column
    // the approved recipe rounds to 22 — a finding no writer would ever raise.
    const page = readFileSync(join(webRoot, "pages/TransferPage.tsx"), "utf8");
    const api = readFileSync(join(webRoot, "lib/api.ts"), "utf8");
    const router = readFileSync(
      join(webRoot, "../../api/src/routers/preflight_router.py"),
      "utf8",
    );

    const cellCall = page.slice(page.indexOf("previewQuarantineCells({"));
    const cellBody = cellCall.slice(0, cellCall.indexOf("});"));
    assert.match(cellBody, /shape_recipe: recipePayload\(shapeSteps\)/);
    // A changed recipe is different evidence, so the effect must re-run on it.
    const deps = cellCall.slice(cellCall.indexOf("}, ["), cellCall.indexOf("]);") + 3);
    assert.match(deps, /shapeSteps/);
    assert.match(api, /shape_recipe\?: ShapeRecipeWire \| null;/);
    // The backend applies the same pre-load image before it scans a cell.
    assert.match(router, /shape_recipe: dict\[str, Any\] \| None = None/);
    assert.match(router, /image = shaped_preflight_image\(/);
    assert.match(router, /if image\.applied:/);
  });

  it("a refused row is not reported as an accounting defect", () => {
    const step = readFileSync(
      join(webRoot, "pages/transfer/TransferTransformStep.tsx"),
      "utf8",
    );
    const ledger = step.slice(step.indexOf("df2-xform-ledger"));
    const block = ledger.slice(0, ledger.indexOf("</p>"));
    assert.match(block, /preview\?\.refusal/);
    assert.match(block, /the preview stopped at the refused row above/);
    // The defect wording survives, but only for an imbalance with no refusal.
    assert.match(block, /: " · ledger does not balance/);
  });

  it("a required Transform option says so before Add is clicked", () => {
    const builder = readFileSync(
      join(webRoot, "components/transfer/TransformStepBuilder.tsx"),
      "utf8",
    );
    const css = readFileSync(join(webRoot, "styles/transform-prep.css"), "utf8");

    assert.match(builder, /const blankRequired = useMemo\(/);
    assert.match(builder, /is-invalid/);
    assert.match(builder, /role="alert"/);
    // Add cannot append a step the engine would refuse, and says why.
    assert.match(builder, /disabled=\{!canPlan \|\| !operation \|\| Boolean\(missing\)/);
    assert.ok(css.includes(".df2-xform-required {"), ".df2-xform-required has no rule");
    assert.ok(
      css.includes(".df2-field.is-invalid > .df2-input"),
      "invalid required inputs are not marked",
    );
  });

  it("the Transform step ships the stylesheet its own namespace needs", () => {
    const entry = readFileSync(join(webRoot, "styles/app-styles.css"), "utf8");
    const css = readFileSync(join(webRoot, "styles/transform-prep.css"), "utf8");

    // Every class the step used was previously undefined, so the panel laid out
    // in default flow. The stylesheet must be reachable from the one entrypoint.
    assert.match(entry, /@import "\.\/transform-prep\.css";/);
    for (const rule of [".df2-xform-grid", ".df2-xform-card", ".df2-xform-bars", ".df2-xform-scroll"]) {
      assert.ok(css.includes(rule), `${rule} has no rule`);
    }
    assert.match(css, /grid-template-columns: minmax\(0, 5fr\) minmax\(0, 6fr\)/);
    assert.match(css, /@media \(max-width: 1180px\)/);
  });

  it("the Transform step states its own rules on screen", () => {
    const guide = readFileSync(join(webRoot, "components/transfer/TransformGuidePanel.tsx"), "utf8");
    const step = readFileSync(join(webRoot, "pages/transfer/TransferTransformStep.tsx"), "utf8");

    assert.match(guide, /runs on the read, before anything is written/);
    assert.match(guide, /never modified/);
    assert.match(guide, /not as loss/);
    assert.match(guide, /post-load transform/);
    assert.match(guide, /re-checks every row of the[\s\S]{0,40}population/);
    // Identity is what Execute is held to, so it is stated where it is approved.
    assert.match(step, /recipe \{preview\.recipe\.recipe_hash\}/);
    // A refused recipe has no identity — Map must not be reachable behind it.
    assert.match(step, /disabled=\{Boolean\(previewError\) \|\| Boolean\(preview\?\.refusal\)\}/);
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

  it("ValidateActionsRail offers Re-run Validate in every state, not only when blocked", () => {
    const src = readFileSync(join(webRoot, "components/transfer/ValidateActionsRail.tsx"), "utf8");
    assert.match(src, /Re-run Validate/);
    // Gating the control on `blocked` stranded a green verdict: re-validating
    // meant Back ▸ Continue to Validate.
    assert.doesNotMatch(src, /\{\(blocked \|\| \(!preflight && !preflighting\)\) && \(/);
    assert.match(src, /onClick=\{onRunPreflight\}/);
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

  it("Source/Dest studio panes share one 50/50 owner and stack only at 1100px", () => {
    const css = readFileSync(join(webRoot, "styles/transfer-studio.css"), "utf8");
    const lock = css.slice(css.lastIndexOf("Studio source / destination — one owner"));
    assert.match(
      lock,
      /\.df2-app \.df2-page-transfer-studio \.df2-source-step \.df2-transfer-step-split \{[\s\S]*grid-template-columns:\s*minmax\(0, 1fr\) minmax\(0, 1fr\)/,
    );
    assert.match(lock, /@media \(max-width: 1100px\)/);
    assert.doesNotMatch(
      css,
      /@media \(max-width: 1280px\) \{[\s\S]{0,400}?\.df2-source-step \.df2-transfer-step-split \{[\s\S]{0,80}?grid-template-columns:\s*1fr/,
      "1280 laptop must keep the two-pane grid; stack only at 1100",
    );
    assert.match(
      lock,
      /\.df2-page-transfer-studio \.df2-dest-step > \.df2-wizard-footer[\s\S]*max-height:\s*none/,
    );
  });

  it("Destination saved/new lists nest-scroll above the wizard footer", () => {
    const css = readFileSync(join(webRoot, "styles/transfer-studio.css"), "utf8");
    const landing = readFileSync(join(webRoot, "styles/landing.css"), "utf8");
    const destOwner = css.slice(css.lastIndexOf("Destination rail — last owner"));
    assert.match(
      destOwner,
      /\.df2-page-transfer-studio \.df2-dest-step\.df2-transfer-step-viewport \{[\s\S]*overflow:\s*hidden !important/,
    );
    assert.doesNotMatch(
      destOwner,
      /\.df2-page-transfer-studio \.df2-dest-step\.df2-transfer-step-viewport \{[\s\S]*overflow:\s*visible/,
    );
    assert.match(
      destOwner,
      /\.df2-page-transfer-studio \.df2-dest-connector-list \{[\s\S]*overflow-y:\s*auto !important/,
    );
    assert.match(
      destOwner,
      /\.df2-page-transfer-studio \.df2-dest-picker\.is-new-connection \.df2-dest-engine-panel \{[\s\S]*overflow-y:\s*auto !important/,
    );
    assert.match(
      destOwner,
      /\.df2-page-transfer-studio \.df2-dest-step > \.df2-wizard-footer \{[\s\S]*flex:\s*0 0 auto !important/,
    );
    const afterSourceOwner = css.slice(css.lastIndexOf("Studio source / destination — one owner"));
    assert.doesNotMatch(
      afterSourceOwner,
      /\.df2-page-transfer-studio \.df2-dest-step\.df2-transfer-step-viewport \{[\s\S]{0,160}overflow:\s*visible/,
      "Source natural-height must not unlock Destination overflow",
    );
    assert.doesNotMatch(
      destOwner,
      /\.df2-dest-connector-list \{[\s\S]{0,120}overflow:\s*visible/,
      "1100px must not disable dest list scroll after the last dest owner",
    );
    assert.match(landing, /Marketing cards — last owner/);
    assert.match(landing, /overflow-wrap:\s*anywhere/);
  });

  it("every Transfer step owns a visible primary CTA and does not reuse Shape", () => {
    const constants = readFileSync(join(webRoot, "pages/transfer/studioConstants.ts"), "utf8");
    const page = readFileSync(join(webRoot, "pages/TransferPage.tsx"), "utf8");
    const mapStep = readFileSync(join(webRoot, "pages/transfer/TransferMapStep.tsx"), "utf8");
    const xform = readFileSync(join(webRoot, "pages/transfer/TransferTransformStep.tsx"), "utf8");
    const rail = readFileSync(join(webRoot, "components/transfer/ValidateActionsRail.tsx"), "utf8");

    assert.match(constants, /label: "Source".*label: "Destination".*label: "Transform".*label: "Map".*label: "Validate".*label: "Run"/s);
    assert.match(page, /Continue to Destination/);
    assert.match(page, /Analyze Route/);
    assert.match(page, /Continue to Transform/);
    assert.match(page, /← Back to source/);
    assert.match(xform, /Back to Destination/);
    assert.match(xform, /Continue without transforming/);
    assert.match(xform, /Continue with this transform/);
    assert.match(mapStep, /Continue to Validate →/);
    assert.match(mapStep, /← Back/);
    assert.match(rail, /"Execute"/);
    assert.match(page, /Execute Transfer/);
    assert.doesNotMatch(page, /Continue to Shape/);
    // One primary exit per step — dest Analyze is ghost, not a second primary.
    assert.match(page, /className="df2-btn df2-btn-ghost"[\s\S]{0,500}Analyze Route/);
    assert.match(page, /className="df2-btn df2-btn-primary"[\s\S]{0,400}Continue to Transform/);
  });

  it("laptop density does not hide Dest/Transform/Validate primary actions behind 48px", () => {
    const studio = readFileSync(join(webRoot, "styles/transfer-studio.css"), "utf8");
    const xform = readFileSync(join(webRoot, "styles/transform-prep.css"), "utf8");
    const lock = studio.slice(studio.lastIndexOf("Studio source / destination — one owner"));
    assert.match(lock, /max-height:\s*none !important/);
    assert.match(
      studio,
      /df2-map-step-panel > \.df2-card-footer\.df2-wizard-footer\.df2-map-footer \{[\s\S]*max-height:\s*none/,
    );
    assert.match(xform, /\.df2-xform-actions \{/);
    assert.match(xform, /flex-wrap:\s*wrap/);
    // Rank-19 48px lock must not be the last Dest/Validate footer owner.
    const afterRank19 = studio.slice(studio.lastIndexOf("Rank 19: never clip Studio primary actions"));
    assert.match(afterRank19, /df2-dest-step[\s\S]*max-height:\s*none/);
    assert.match(afterRank19, /df2-validate-footer[\s\S]*max-height:\s*none/);
    assert.match(studio, /df2-validate-rail-actions/);
  });

  it("Transform kitchen stays 5fr/6fr until 1180; Map intel hides only at 1280", () => {
    const xform = readFileSync(join(webRoot, "styles/transform-prep.css"), "utf8");
    const studio = readFileSync(join(webRoot, "styles/transfer-studio.css"), "utf8");
    assert.match(xform, /grid-template-columns: minmax\(0, 5fr\) minmax\(0, 6fr\)/);
    assert.match(xform, /@media \(max-width: 1180px\) \{\s*\n\s*\.df2-xform-grid \{ grid-template-columns: minmax\(0, 1fr\)/);
    assert.match(
      studio,
      /@media \(max-width: 1280px\) \{[\s\S]*\.df2-map-intel-aside \{\s*\n\s*display: none !important/,
    );
  });

  it("Validate owns one studio primary — dashboard Map CTAs are not teal", () => {
    const rail = readFileSync(join(webRoot, "components/transfer/ValidateActionsRail.tsx"), "utf8");
    const dash = readFileSync(join(webRoot, "components/transfer/ValidateDashboard.tsx"), "utf8");
    const page = readFileSync(join(webRoot, "pages/TransferPage.tsx"), "utf8");
    const help = readFileSync(join(webRoot, "lib/helpDocs.ts"), "utf8");

    assert.match(rail, /resolveValidateStudioPrimary/);
    assert.match(rail, /data-studio-primary/);
    assert.match(rail, /df2-validate-studio-primary-label/);
    assert.doesNotMatch(rail, /slice\(0,\s*26\)/);
    const studio = readFileSync(join(webRoot, "styles/transfer-studio.css"), "utf8");
    assert.match(
      studio,
      /df2-validate-footer-actions \.df2-btn\[data-studio-primary="true"\][\s\S]*min-width:\s*max-content/,
    );
    assert.match(dash, /dashboardCtaVariant/);
    assert.match(dash, /dashCta\("map_open"\)/);
    assert.doesNotMatch(
      dash,
      /variant="primary"[\s\S]{0,200}Open Map to fix|Open Map to fix[\s\S]{0,80}variant="primary"/,
    );
    assert.match(page, /studioPrimary=\{studioPrimary\}/);
    assert.match(page, /promoteBlockedPrimaryFix/);
    assert.match(
      help,
      /Source → Destination → Transform → Map → Validate → Run/,
    );
    assert.doesNotMatch(
      help,
      /five-step rail: \*\*Src → Dest → Map → Validate → Run\*\*/,
    );
  });

  it("Destination Advanced owns a number locale contract next to date locale", () => {
    const constants = readFileSync(join(webRoot, "lib/transferConstants.ts"), "utf8");
    const drawer = readFileSync(join(webRoot, "components/transfer/DestinationAdvancedDrawer.tsx"), "utf8");
    const page = readFileSync(join(webRoot, "pages/TransferPage.tsx"), "utf8");
    const api = readFileSync(join(webRoot, "lib/api.ts"), "utf8");

    assert.match(constants, /export type NumberLocaleId = "" \| "US" \| "EU"/);
    assert.match(constants, /label: "US \(1,234\.56\)"/);
    assert.match(constants, /label: "EU \(1\.234,56\)"/);
    assert.match(drawer, /Number locale/);
    assert.match(drawer, /onNumberLocaleChange/);
    assert.match(page, /numberLocales=\{NUMBER_LOCALES\}/);
    assert.match(page, /number_locale: numberLocale/);
    assert.match(api, /formData.append\("number_locale"/);
  });

  it("Validate surfaces number locale set_locale with one Advanced CTA", () => {
    const dash = readFileSync(join(webRoot, "components/transfer/ValidateDashboard.tsx"), "utf8");
    const panel = readFileSync(join(webRoot, "components/transfer/NumberLocalePanel.tsx"), "utf8");
    const honesty = readFileSync(join(webRoot, "lib/validateHonestyControls.ts"), "utf8");
    assert.match(honesty, /export function numberLocaleValidateAction/);
    assert.match(dash, /NumberLocalePanel/);
    assert.match(dash, /numberLocaleValidateAction\(preflight\)/);
    assert.match(panel, /Set number locale/);
    assert.match(panel, /onOpenAdvanced/);
    assert.doesNotMatch(panel, /Set number locale[\s\S]*Set number locale/);
  });

  it("Validate surfaces date locale set_locale with one Advanced CTA", () => {
    const dash = readFileSync(join(webRoot, "components/transfer/ValidateDashboard.tsx"), "utf8");
    const panel = readFileSync(join(webRoot, "components/transfer/NumberLocalePanel.tsx"), "utf8");
    const honesty = readFileSync(join(webRoot, "lib/validateHonestyControls.ts"), "utf8");
    const drawer = readFileSync(join(webRoot, "components/transfer/DestinationAdvancedDrawer.tsx"), "utf8");
    const page = readFileSync(join(webRoot, "pages/TransferPage.tsx"), "utf8");
    assert.match(honesty, /export function dateLocaleValidateAction/);
    assert.match(dash, /DateLocalePanel/);
    assert.match(dash, /dateLocaleValidateAction\(preflight\)/);
    assert.match(panel, /Set date locale/);
    assert.doesNotMatch(panel, /Set date locale[\s\S]*Set date locale/);
    assert.match(drawer, /id="df2-adv-date-locale"/);
    assert.match(drawer, /id="df2-adv-number-locale"/);
    assert.match(drawer, /localeFocus/);
    assert.match(drawer, /scrollAdvancedLocaleIntoView/);
    assert.match(dash, /onOpenLocaleSettings/);
    assert.match(page, /openLocaleSettings/);
    assert.match(honesty, /export function scrollAdvancedLocaleIntoView/);
  });

  it("Source, Schedules, and Gate8 pick files through one sr-only input + label", () => {
    const hidden = readFileSync(join(webRoot, "components/ui/HiddenFileInput.tsx"), "utf8");
    const page = readFileSync(join(webRoot, "pages/TransferPage.tsx"), "utf8");
    const schedules = readFileSync(join(webRoot, "pages/SchedulesPage.tsx"), "utf8");
    const gate8 = readFileSync(join(webRoot, "components/transfer/Gate8ProofCard.tsx"), "utf8");
    const css = readFileSync(join(webRoot, "styles/enterprise-ui.css"), "utf8");

    assert.match(hidden, /className="df2-sr-only"/);
    assert.match(hidden, /type="file"/);
    assert.doesNotMatch(hidden, /<input[\s\S]*\bhidden(?:\s|=|>)/);
    const srOnly = css.match(/\.df2-sr-only \{([^}]+)\}/);
    assert.ok(srOnly, ".df2-sr-only rule is missing");
    assert.match(srOnly[1], /clip: rect\(0, 0, 0, 0\)/);
    assert.doesNotMatch(srOnly[1], /display:\s*none/);

    assert.match(page, /id="df2-source-file"/);
    assert.match(page, /htmlFor="df2-source-file"/);
    assert.match(page, /<label\s+htmlFor="df2-source-file"[\s\S]*className=\{`df2-upload/);
    assert.doesNotMatch(page, /<input[\s\S]{0,80}type="file"/);
    assert.doesNotMatch(page, /fileInputRef\.current\?\.click/);

    assert.match(schedules, /id="df2-schedule-import"/);
    assert.match(schedules, /htmlFor="df2-schedule-import"/);
    assert.doesNotMatch(schedules, /importInputRef\.current\?\.click/);

    assert.match(gate8, /id="df2-gate8-verify-proof"/);
    assert.match(gate8, /htmlFor="df2-gate8-verify-proof"/);
    assert.doesNotMatch(gate8, /verifyInputRef\.current\?\.click/);
  });

  it("local preflight and file export use the write-path number locale parser", () => {
    const localPf = readFileSync(join(webRoot, "lib/localPreflight.ts"), "utf8");
    const localEx = readFileSync(join(webRoot, "lib/localFileExport.ts"), "utf8");
    const localTx = readFileSync(join(webRoot, "lib/localTransform.ts"), "utf8");
    const page = readFileSync(join(webRoot, "pages/TransferPage.tsx"), "utf8");
    assert.match(localTx, /parseLocaleNumber/);
    assert.match(localPf, /applyLocalTransform/);
    assert.match(localEx, /applyLocalTransform/);
    assert.match(page, /numberLocale,/);
    assert.match(page, /dateLocale,/);
    assert.match(localPf, /date_locale_report/);
    assert.doesNotMatch(localPf, /replace\(\/,\/g/);
    assert.doesNotMatch(localEx, /replace\(\/,\/g/);
    assert.doesNotMatch(localTx, /replace\(\/,\/g/);
  });

  it("Validate Remap reads kernel findings; population-fit honesty names the dest widen", () => {
    const dash = readFileSync(join(webRoot, "components/transfer/ValidateDashboard.tsx"), "utf8");
    const types = readFileSync(join(webRoot, "lib/types.ts"), "utf8");
    const fit = readFileSync(join(webRoot, "lib/populationFit.ts"), "utf8");
    assert.match(dash, /Prefer Decision Kernel validation_findings/);
    assert.match(dash, /suggestedTargetType/);
    assert.match(dash, /widen to \$\{populationFit\.offenders\[0\]\.suggestedTargetType\}/);
    assert.match(fit, /suggestedTargetType: String\(f\.suggested_target_type/);
    assert.match(types, /suggested_target_type\?: string;/);
    assert.match(types, /suggested_fix\?: string;/);
    // Existing Remap CTA is the only primary — do not add a second teal for fit.
    assert.doesNotMatch(dash, /dashCta\("population/);
    assert.doesNotMatch(dash, /variant="primary"[\s\S]{0,120}widen to NUMBER/);
  });
});
