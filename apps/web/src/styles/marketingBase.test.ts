/**
 * The public landing sheets are their own file so the workspace bundle can
 * stay under the CSS chunk budget. Later sheets still override this base.
 */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, it } from "node:test";

const STYLES = dirname(fileURLToPath(import.meta.url));
const SRC = dirname(STYLES);
const WEB = dirname(SRC);

describe("marketing base stylesheet", () => {
  it("loads landing and layout ahead of the workspace bundle", () => {
    const base = readFileSync(join(STYLES, "marketing-base.css"), "utf8");
    const app = readFileSync(join(STYLES, "app-styles.css"), "utf8");
    const main = readFileSync(join(SRC, "main.tsx"), "utf8");
    const vite = readFileSync(join(WEB, "vite.config.ts"), "utf8");
    const landingAt = base.indexOf('@import "./landing.css"');
    const layoutAt = base.indexOf('@import "./marketing-layout.css"');
    const markerAt = base.indexOf(".df2-css-split-marketing-base");
    assert.ok(landingAt >= 0 && layoutAt > landingAt && markerAt > layoutAt);
    assert.doesNotMatch(app, /@import "\.\/landing\.css"/);
    assert.doesNotMatch(app, /@import "\.\/marketing-layout\.css"/);
    assert.match(app, /@import "\.\/marketing-editorial\.css"/);
    assert.match(app, /@import "\.\/marketing-hero\.css"/);
    const baseImport = main.indexOf('import "./styles/marketing-base.css"');
    const appImport = main.indexOf('import "./styles/app-styles.css"');
    assert.ok(baseImport >= 0 && appImport > baseImport);
    assert.match(vite, /splitMarketingBaseCss/);
  });
});
