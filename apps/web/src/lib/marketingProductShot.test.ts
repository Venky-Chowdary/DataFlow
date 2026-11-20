/**
 * Run: npx --yes tsx --test apps/web/src/lib/marketingProductShot.test.ts
 */
import assert from "node:assert/strict";
import { existsSync, readFileSync, readdirSync } from "node:fs";
import { dirname, join } from "node:path";
import { describe, it } from "node:test";
import { fileURLToPath } from "node:url";
import { DEMO_ROUTE, SHOT_PIXELS, WORKSPACE_SHOT } from "./workspaceShots";
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
  it("home renders the product as UI in code, with no raster screenshots", () => {
    const home = readFileSync(join(webRoot, "pages/LandingPage.tsx"), "utf8");
    assert.match(home, /<StatementHero \/>/);
    assert.doesNotMatch(home, /ProductShot|<img/);
    assert.doesNotMatch(home, /transferValidateVerdict|jobTrustCard/);
    assert.doesNotMatch(home, /Postgres to Snowflake, start to finish/);
    assert.doesNotMatch(home, /ProofLoopArt/);
    assert.doesNotMatch(home, /Trust Report 92/);
    assert.equal(DEMO_ROUTE.source, "PostgreSQL · public.sales_orders");
    assert.equal(DEMO_ROUTE.dest, "Snowflake · DW_DEMO.SALES_ORDERS_REPORTING");
  });

  it("captures reserve their size, paint a placeholder, and disappear on error", () => {
    const shot = readFileSync(join(webRoot, "components/marketing/ProductShot.tsx"), "utf8");
    assert.match(shot, /width=\{size\.width\}/);
    assert.match(shot, /height=\{size\.height\}/);
    assert.match(shot, /aspectRatio/);
    assert.match(shot, /onError=/);
    const css = readFileSync(join(webRoot, "styles/marketing-system.css"), "utf8");
    assert.match(css, /\.mk-crop-frame \{[\s\S]*?overflow:\s*hidden[\s\S]*?background:/);
    assert.doesNotMatch(css, /filter:\s*brightness/);
  });

  it("home takes no image from outside WORKSPACE_SHOT", () => {
    const home = readFileSync(join(webRoot, "pages/LandingPage.tsx"), "utf8");
    assert.doesNotMatch(home, /\/docs\/screenshots\//);
    assert.doesNotMatch(home, /MappingCinema|ProofCinema|ObservabilityInAction/);
  });

  it("product and solution pages show only cropped regions of the workspace", () => {
    const surfaces = readFileSync(join(webRoot, "pages/marketing/StoryPages.tsx"), "utf8");
    const template = readFileSync(join(webRoot, "pages/marketing/StoryPage.tsx"), "utf8");
    assert.match(template, /<ProductShot/);
    const used = [...surfaces.matchAll(/WORKSPACE_SHOT\.(\w+)/g)].map((m) => m[1]);
    assert.ok(used.length >= 3, "expected the story pages to carry crops");
    for (const key of used) assert.match(key, /Crop$/, `${key} is a full-window capture`);
    assert.doesNotMatch(surfaces, /GateCombArt|RunSpineArt|CutoverArt|WarehouseLayersArt|WatermarkArt/);
    assert.doesNotMatch(surfaces, /MappingCinema|ProofCinema|CdcCinema/);
    assert.doesNotMatch(surfaces, /Job Theater/);
  });

  it("step labels on each story page follow the five steps, once each", () => {
    const order = ["Plan", "Validate", "Execute", "Verify", "Evidence"];
    const surfaces = readFileSync(join(webRoot, "pages/marketing/StoryPages.tsx"), "utf8");
    for (const page of surfaces.split("export function ").slice(1)) {
      const labels = [...page.matchAll(/\{ step: "(\w+)"/g)].map((m) => m[1]);
      const idx = labels.map((l) => order.indexOf(l));
      assert.ok(idx.every((v) => v >= 0), page.slice(0, 30));
      assert.equal(new Set(labels).size, labels.length, `${page.slice(0, 30)} repeats a step`);
      assert.deepEqual([...idx].sort((a, b) => a - b), idx, `${page.slice(0, 30)} is out of order`);
    }
  });

  it("every screenshot's recorded size matches its WebP header", () => {
    for (const [name, [w, h]] of Object.entries(SHOT_PIXELS)) {
      const buf = readFileSync(join(shotDir, name));
      const chunk = buf.subarray(12, 16).toString("ascii");
      let width = 0;
      let height = 0;
      if (chunk === "VP8X") {
        width = 1 + buf.readUIntLE(24, 3);
        height = 1 + buf.readUIntLE(27, 3);
      } else if (chunk === "VP8 ") {
        width = buf.readUInt16LE(26) & 0x3fff;
        height = buf.readUInt16LE(28) & 0x3fff;
      } else if (chunk === "VP8L") {
        const bits = buf.readUInt32LE(21);
        width = (bits & 0x3fff) + 1;
        height = ((bits >> 14) & 0x3fff) + 1;
      }
      assert.deepEqual([width, height], [w, h], name);
    }
    for (const name of readdirSync(shotDir)) assert.ok(name in SHOT_PIXELS, `${name} has no recorded size`);
  });

  it("docs and product reels share ProductShot chrome so labels cannot overflow", () => {
    const reel = readFileSync(join(webRoot, "components/docs/DocsShotReel.tsx"), "utf8");
    assert.match(reel, /lp-product-shot-chrome/);
    assert.match(reel, /lp-product-shot-surface/);
    const stories = readFileSync(join(webRoot, "pages/marketing/StoryPages.tsx"), "utf8");
    assert.match(stories, /WORKSPACE_SHOT/);
    assert.doesNotMatch(stories, /\/docs\/screenshots\//);
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
