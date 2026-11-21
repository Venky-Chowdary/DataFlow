import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";

// The mark module itself, not the package barrel (the barrel pulls in React).
import { BRAND_COLORS } from "../../../../packages/design-system/src/brand/mark";
import { CATALOG_TOTAL, PROVEN_DRIVERS } from "./provenEvidence";
import { PUBLIC_PAGE_META } from "./publicNavigation";
import {
  DEFAULT_DESCRIPTION,
  OG_IMAGE_PATH,
  OG_IMAGE_SIZE,
  PAGE_META,
  SHARE_CARD,
  SOFTWARE_FEATURES,
} from "./seo";

const WEB = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..", "..");
const read = (rel: string) => readFileSync(path.join(WEB, rel), "utf8");
const INDEX = read("index.html");
const MANIFEST = JSON.parse(read("public/site.webmanifest")) as Record<string, unknown>;
const HOME = PUBLIC_PAGE_META.home;

function metaContent(html: string, attr: "name" | "property", key: string): string {
  const re = new RegExp(`<meta ${attr}="${key.replace(/[.:]/g, "\\$&")}" content="([^"]*)"`);
  const m = re.exec(html);
  assert.ok(m, `index.html has no <meta ${attr}="${key}">`);
  return m[1];
}

/** Claims the site no longer makes, or never could back (P0-1, P1-15). */
const STALE = [
  /eight preflight/i,
  /9 preflight/i,
  /G1\s*[–-]\s*G9/,
  /universal data/i,
  /value[ -]by[ -]value/i,
  /every load verified/i,
  /move any schema anywhere/i,
  /DM\+?\s?Sans/,
  /fonts\.googleapis/,
];

test("the share card says what the site says", () => {
  // Headline: the home hero, word for word.
  assert.match(read("src/pages/LandingPage.tsx"), new RegExp(`<h1[^>]*>${SHARE_CARD.headline}</h1>`));
  // The statement is labelled as an example exactly as the hero labels it.
  assert.ok(read("src/components/marketing/StatementHero.tsx").includes(SHARE_CARD.statementLabel));
  // Gates: the product's own count (Transfer page, helpDocs).
  assert.equal(SHARE_CARD.facts[0], "Nine core gates before any write.");
  assert.match(read("src/lib/helpDocs.ts"), /before any write: nine core gates/);
  // Drivers: counted from the CI evidence, never typed in.
  assert.equal(SHARE_CARD.facts[1], `${PROVEN_DRIVERS} of ${CATALOG_TOTAL} drivers proven in CI.`);
  // The page title carries the same promise as the card.
  const words = (s: string) => s.toLowerCase().replace(/[^a-z ]/g, "");
  assert.ok(words(HOME.title).includes(words(SHARE_CARD.headline)));
});

test("index.html meta matches the home page meta", () => {
  assert.equal(metaContent(INDEX, "name", "description"), HOME.description);
  assert.equal(metaContent(INDEX, "property", "og:title"), `Datawrap — ${HOME.title}`);
  assert.equal(metaContent(INDEX, "property", "og:description"), HOME.description);
  assert.equal(metaContent(INDEX, "name", "twitter:description"), HOME.description);
  assert.ok(INDEX.includes(`<title>Datawrap — ${HOME.title}</title>`));
  for (const key of ["og:image", "twitter:image"] as const) {
    const attr = key === "og:image" ? "property" : "name";
    assert.equal(metaContent(INDEX, attr, key), `https://www.datawrap.io${OG_IMAGE_PATH}`);
    assert.equal(metaContent(INDEX, attr, `${key}:alt`), SHARE_CARD.alt);
  }
  assert.equal(metaContent(INDEX, "property", "og:image:width"), String(OG_IMAGE_SIZE.width));
  assert.equal(metaContent(INDEX, "property", "og:image:height"), String(OG_IMAGE_SIZE.height));
  assert.equal(metaContent(INDEX, "name", "theme-color").toLowerCase(), BRAND_COLORS.tile.toLowerCase());
});

test("the static JSON-LD lists the same features as the live one", () => {
  const m = /<script type="application\/ld\+json" id="datawrap-jsonld">([\s\S]*?)<\/script>/.exec(INDEX);
  assert.ok(m, "index.html JSON-LD needs id=datawrap-jsonld so the app replaces it, not adds a second");
  const graph = (JSON.parse(m[1]) as { "@graph": Array<Record<string, unknown>> })["@graph"];
  const app = graph.find((n) => n["@type"] === "SoftwareApplication");
  assert.deepEqual(app?.featureList, [...SOFTWARE_FEATURES]);
  assert.equal(app?.description, HOME.description);
});

test("the manifest is the home page's name and description, in the tile ink", () => {
  assert.equal(MANIFEST.name, "Datawrap");
  assert.equal(MANIFEST.description, HOME.description);
  assert.equal(String(MANIFEST.theme_color).toLowerCase(), BRAND_COLORS.tile.toLowerCase());
});

test("app-side defaults reuse the home meta", () => {
  assert.equal(DEFAULT_DESCRIPTION, HOME.description);
  assert.equal(PAGE_META.landing.title, HOME.title);
  assert.equal(PAGE_META.landing.description, HOME.description);
});

test("no stale or overclaiming copy in meta, manifest or brand exports", () => {
  const sources = {
    "index.html": INDEX,
    "public/site.webmanifest": read("public/site.webmanifest"),
    "src/lib/seo.ts": read("src/lib/seo.ts"),
    "src/lib/usePageMeta.ts": read("src/lib/usePageMeta.ts"),
    "scripts/export-brand-icons.mjs": read("scripts/export-brand-icons.mjs"),
    "scripts/export-social-brand.mjs": read("scripts/export-social-brand.mjs"),
  };
  for (const [file, text] of Object.entries(sources)) {
    for (const re of STALE) assert.doesNotMatch(text, re, `${file} still says ${re}`);
  }
  for (const feature of SOFTWARE_FEATURES) {
    for (const re of STALE) assert.doesNotMatch(feature, re);
  }
});

test("the share card keeps its size", () => {
  // PNG IHDR: width and height are big-endian at bytes 16..24. The card is
  // captured at 2x, so links keep resolving to the same 1200x630 shape.
  for (const rel of ["public/og-image.png", "public/brand/social/og-1200x630.png"]) {
    const buf = readFileSync(path.join(WEB, rel));
    assert.equal(buf.readUInt32BE(16), OG_IMAGE_SIZE.width * 2, rel);
    assert.equal(buf.readUInt32BE(20), OG_IMAGE_SIZE.height * 2, rel);
  }
});
