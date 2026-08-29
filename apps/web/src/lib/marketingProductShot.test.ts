/**
 * Run: npx --yes tsx --test apps/web/src/lib/marketingProductShot.test.ts
 */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { describe, it } from "node:test";
import { fileURLToPath } from "node:url";

const webRoot = join(dirname(fileURLToPath(import.meta.url)), "..");

describe("marketing product photography", () => {
  it("home hero frames Job Theater, not an overflowing schematic", () => {
    const home = readFileSync(join(webRoot, "pages/LandingPage.tsx"), "utf8");
    assert.match(home, /ProductShot/);
    assert.match(home, /app-jobs\.png/);
    assert.match(home, /PostgreSQL · public\.orders/);
    assert.match(home, /Snowflake · ANALYTICS\.ORDERS/);
    assert.doesNotMatch(home, /ProofLoopArt/);
  });

  it("typed chips clip to the plate so Snowflake.orders cannot paint past the border", () => {
    const art = readFileSync(join(webRoot, "components/marketing/hero-art/HeroArtFrame.tsx"), "utf8");
    assert.match(art, /clipPath/);
    const css = readFileSync(join(webRoot, "styles/marketing-hero.css"), "utf8");
    assert.match(css, /\.lp-product-shot-route span \{[\s\S]*?text-overflow:\s*ellipsis/);
    assert.match(css, /\.lp-product-shot-viewport \{[\s\S]*?overflow:\s*hidden/);
  });

  it("product surface tabs use current workspace shots", () => {
    const home = readFileSync(join(webRoot, "pages/LandingPage.tsx"), "utf8");
    assert.match(home, /SURFACE_SHOTS/);
    assert.match(home, /app-transfer-validate\.png/);
    assert.match(home, /app-pipelines\.png/);
  });
});
