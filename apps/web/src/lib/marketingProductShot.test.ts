/**
 * Run: npx --yes tsx --test apps/web/src/lib/marketingProductShot.test.ts
 */
import assert from "node:assert/strict";
import { existsSync, readFileSync, readdirSync } from "node:fs";
import { dirname, join } from "node:path";
import { describe, it } from "node:test";
import { fileURLToPath } from "node:url";
import { DEMO_ROUTE, WORKSPACE_SHOT } from "./workspaceShots";
import { listAllHelpDocs } from "./helpDocs";

const webRoot = join(dirname(fileURLToPath(import.meta.url)), "..");
const publicRoot = join(webRoot, "..", "public");
const shotDir = join(publicRoot, "docs", "screenshots");

function helpDocFigureSources(): string[] {
  const out: string[] = [];
  for (const doc of listAllHelpDocs()) {
    for (const section of doc.sections) {
      if (section.figure) out.push(section.figure.src);
      for (const step of section.workflow ?? []) {
        if (step.figure) out.push(step.figure.src);
      }
    }
  }
  return out;
}

describe("marketing product photography", () => {
  it("home hero stages the Studio map on the demo route, and the run screenshots stay off the homepage", () => {
    const home = readFileSync(join(webRoot, "pages/LandingPage.tsx"), "utf8");
    assert.match(home, /ProductShot/);
    assert.match(home, /src=\{WORKSPACE_SHOT\.transferMapStage\}/);
    assert.doesNotMatch(home, /transferValidateVerdict|jobTrustCard/);
    assert.doesNotMatch(home, /Postgres to Snowflake, start to finish/);
    assert.match(home, /DEMO_ROUTE\.source/);
    assert.doesNotMatch(home, /ProofLoopArt/);
    assert.doesNotMatch(home, /Trust Report 92/);
    assert.equal(DEMO_ROUTE.source, "PostgreSQL · public.sales_orders");
    assert.equal(DEMO_ROUTE.dest, "Snowflake · DW_DEMO.SALES_ORDERS_REPORTING");
  });

  it("typed chips clip to the plate so Snowflake.orders cannot paint past the border", () => {
    const art = readFileSync(join(webRoot, "components/marketing/hero-art/HeroArtFrame.tsx"), "utf8");
    assert.match(art, /clipPath/);
    const css = readFileSync(join(webRoot, "styles/marketing-hero.css"), "utf8");
    assert.match(css, /\.lp-product-shot-route span \{[\s\S]*?text-overflow:\s*ellipsis/);
    assert.match(css, /\.lp-product-shot-viewport \{[\s\S]*?overflow:\s*hidden/);
    assert.match(css, /\.lp-product-shot-viewport img \{[\s\S]*?object-fit:\s*contain/);
    assert.match(css, /\.lp-sol-v2 \.lp-mkt-body[\s\S]*?1080px/);
  });

  it("home workflow and feature tiles take every image from WORKSPACE_SHOT", () => {
    const home = readFileSync(join(webRoot, "pages/LandingPage.tsx"), "utf8");
    assert.match(home, /shot: WORKSPACE_SHOT\.transferSource/);
    assert.match(home, /shot: WORKSPACE_SHOT\.schedulesCard/);
    assert.doesNotMatch(home, /\/docs\/screenshots\//);
    assert.doesNotMatch(home, /MappingCinema|ProofCinema|ObservabilityInAction/);
  });

  it("product and solution heroes frame live workspace shots, not schematic SVGs", () => {
    const surfaces = readFileSync(join(webRoot, "pages/marketing/ProductSurfaces.tsx"), "utf8");
    assert.match(surfaces, /ProductShot/);
    assert.match(surfaces, /WORKSPACE_SHOT\.jobTrust/);
    assert.match(surfaces, /WORKSPACE_SHOT\.transferMap/);
    assert.match(surfaces, /WORKSPACE_SHOT\.schedules/);
    assert.doesNotMatch(surfaces, /GateCombArt|RunSpineArt|CutoverArt|WarehouseLayersArt|WatermarkArt/);
    assert.doesNotMatch(surfaces, /MappingCinema|ProofCinema|CdcCinema/);
    assert.doesNotMatch(surfaces, /Job Theater/);
  });

  it("docs and product reels share ProductShot chrome so labels cannot overflow", () => {
    const reel = readFileSync(join(webRoot, "components/docs/DocsShotReel.tsx"), "utf8");
    assert.match(reel, /lp-product-shot-chrome/);
    assert.match(reel, /lp-product-shot-surface/);
    const shared = readFileSync(join(webRoot, "pages/marketing/productPageShared.tsx"), "utf8");
    assert.match(shared, /WORKSPACE_SHOT/);
    assert.doesNotMatch(shared, /\/docs\/screenshots\//);
  });

  it("every WORKSPACE_SHOT path is a WebP file that ships in public/", () => {
    for (const [key, src] of Object.entries(WORKSPACE_SHOT)) {
      assert.match(src, /^\/docs\/screenshots\/app-[a-z-]+\.webp$/, key);
      const file = join(publicRoot, src);
      assert.ok(existsSync(file), `${key} → ${src} is missing`);
      const head = readFileSync(file).subarray(0, 12);
      assert.equal(head.subarray(0, 4).toString("ascii"), "RIFF", `${key} is not a RIFF container`);
      assert.equal(head.subarray(8, 12).toString("ascii"), "WEBP", `${key} is not WebP`);
    }
  });

  it("no screenshot ships without a WORKSPACE_SHOT entry", () => {
    const referenced = new Set(Object.values(WORKSPACE_SHOT).map((src) => src.split("/").pop()));
    const orphans = readdirSync(shotDir).filter((name) => !referenced.has(name));
    assert.deepEqual(orphans, []);
  });

  it("help doc figures resolve to shipped screenshots with in-frame markers", () => {
    const known = new Set<string>(Object.values(WORKSPACE_SHOT));
    const sources = helpDocFigureSources();
    assert.ok(sources.length > 30, `expected the guides to carry figures, found ${sources.length}`);
    for (const src of sources) assert.ok(known.has(src), `${src} is not a WORKSPACE_SHOT path`);

    for (const doc of listAllHelpDocs()) {
      for (const section of doc.sections) {
        const figures = [section.figure, ...(section.workflow ?? []).map((s) => s.figure)];
        for (const fig of figures) {
          for (const m of fig?.markers ?? []) {
            for (const v of [m.x, m.y]) {
              const pct = Number.parseFloat(v);
              assert.ok(v.endsWith("%") && pct >= 0 && pct <= 100, `${doc.id} marker ${m.label} at ${v}`);
            }
          }
        }
      }
    }
  });
});
