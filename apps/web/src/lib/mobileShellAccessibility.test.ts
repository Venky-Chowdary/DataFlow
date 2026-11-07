import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, it } from "node:test";

const webRoot = join(dirname(fileURLToPath(import.meta.url)), "..");
const app = readFileSync(join(webRoot, "DataTransferApp.tsx"), "utf8");
const shellCss = readFileSync(join(webRoot, "styles/app-styles.css"), "utf8");
const stepperCss = readFileSync(join(webRoot, "styles/enterprise-ui.css"), "utf8");
const transferStudioCss = readFileSync(join(webRoot, "styles/transfer-studio.css"), "utf8");
const tokensCss = readFileSync(join(webRoot, "styles/tokens.css"), "utf8");

describe("responsive app shell accessibility", () => {
  it("names the header actions and exposes the mobile navigation state", () => {
    assert.match(app, /aria-label="New transfer"/);
    assert.match(app, /aria-label="Toggle Datawrap Pilot"/);
    assert.match(app, /aria-label="Open navigation"/);
    assert.match(app, /aria-hidden=\{mobileShell \? !mobileNavOpen : undefined\}/);
    assert.match(app, /inert=\{mobileShell && !mobileNavOpen\}/);
    assert.match(app, /event\.key !== "Escape"/);
    assert.match(app, /mobileNavButtonRef\.current\?\.focus\(\)/);
    assert.match(app, /mobileNavRef\.current\?\.querySelector/);
  });

  it("keeps compact header actions visible and content-sized", () => {
    assert.match(app, /df2-topbar-btn-text-short/);
    assert.match(shellCss, /@media \(max-width: 900px\)[\s\S]*?df2-topbar-new-transfer[\s\S]*?flex: 0 0 auto/);
    assert.match(shellCss, /df2-topbar-pilot[\s\S]*?justify-content: center/);
    assert.match(shellCss, /df2-topbar-btn-text-short\s*\{\s*display: inline-flex/);
  });

  it("hides the closed mobile navigation after its transition without horizontal bleed", () => {
    assert.match(shellCss, /overflow-x: clip/);
    assert.match(shellCss, /transform: translateX\(calc\(-100% - var\(--df-mobile-nav-offset\)\)\) !important/);
    assert.match(shellCss, /visibility: hidden !important/);
    assert.match(shellCss, /\.df2-app \.df2-sidebar\.open\s*\{[\s\S]*?visibility: visible !important/);
  });

  it("uses a compact six-step row with the route below it", () => {
    assert.match(stepperCss, /grid-template-columns: repeat\(6, minmax\(0, 1fr\)\)/);
    assert.match(transferStudioCss, /\.df2-page-transfer-studio \.df2-transfer-studio-chrome-row\s*\{\s*display: grid;\s*grid-template-columns: minmax\(0, 1fr\)/);
    assert.match(stepperCss, /df2-transfer-studio-chrome-row \.df2-route-bar\s*\{[\s\S]*?order: 2/);
    assert.match(stepperCss, /df2-wizard-label-short\s*\{[\s\S]*?display: inline !important/);
    assert.match(tokensCss, /--df-stepper-mobile-height: 32px/);
    assert.match(tokensCss, /--df-mobile-nav-max-width: 280px/);
  });
});
