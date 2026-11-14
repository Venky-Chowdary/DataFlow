/**
 * UX-25: the flag-gated recipe canvas sat in Settings → General as a plain
 * "Recipe canvas" checkbox any admin could tick. It is labelled Preview — not
 * for production, and turning it on asks first.
 */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, it } from "node:test";
import { CANVAS_PREVIEW_CONFIRM, CANVAS_PREVIEW_HINT, CANVAS_PREVIEW_LABEL } from "./studioFlags.js";

const settings = readFileSync(join(dirname(fileURLToPath(import.meta.url)), "..", "pages", "SettingsPage.tsx"), "utf8");
const field = settings.slice(settings.indexOf('htmlFor="studio-canvas"'), settings.indexOf('id="studio-canvas-hint"') + 200);

describe("recipe canvas setting", () => {
  it("is labelled a preview, not for production", () => {
    assert.equal(CANVAS_PREVIEW_LABEL, "Preview — not for production");
    assert.match(field, /\{CANVAS_PREVIEW_LABEL\}/);
    assert.match(CANVAS_PREVIEW_HINT, /Not supported for production/);
  });

  it("asks before turning it on, and only then", () => {
    assert.match(CANVAS_PREVIEW_CONFIRM.title, /preview recipe canvas/);
    assert.match(field, /if \(!next\) \{[\s\S]*setStudioCanvas\(false\)/);
    assert.match(field, /confirm\(CANVAS_PREVIEW_CONFIRM\)\.then\(\(ok\) => \{\s*if \(!ok\) return;[\s\S]*setStudioCanvas\(true\)/);
  });
});
