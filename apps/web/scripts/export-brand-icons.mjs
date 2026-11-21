/**
 * Render every Datawrap raster icon and the share card from the one canonical
 * mark definition (`@dataflow/design-system` → `brand/mark`), and rewrite
 * `public/favicon.svg` from it so the checked-in vector cannot drift either.
 *
 * Favicons, the manifest icons and the OG card used to be hand-exported, so
 * they drifted onto an older mark while the social exports moved on — the app
 * shipped two different logos at once. Everything here is generated, so the
 * mark can only change in one place.
 *
 * The share card and the lockups are set the way the site sets them:
 * - the wordmark is IBM Plex Sans 600 at -0.01em, at the header's proportions
 *   (28 px mark, 10 px gap, 19.2 px word), loaded from the bundled
 *   @fontsource files, never from a font CDN;
 * - every colour is a `--df-*` token read from `src/styles/tokens.css`;
 * - every line of copy comes from `SHARE_CARD` (`src/lib/seo.ts`) and the
 *   home statement's figures from `EXAMPLE_STATEMENT`, so the card cannot say
 *   something the site does not.
 *
 * This module also exports the helpers `export-social-brand.mjs` uses, so the
 * covers and lockups share one renderer.
 *
 *   npm run brand:icons                      (writes into public/)
 *   BRAND_OUT_DIR=/tmp/x tsx scripts/export-brand-icons.mjs   (preview elsewhere)
 */
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { createRequire } from "node:module";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

import { chromium } from "playwright";

import { brandMarkSvg } from "../../../packages/design-system/src/brand/mark.ts";
import { EXAMPLE_READ, EXAMPLE_STATEMENT } from "../src/components/marketing/StatementHero.tsx";
import { SHARE_CARD } from "../src/lib/seo.ts";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const WEB = path.resolve(__dirname, "..");
export const PUBLIC = process.env.BRAND_OUT_DIR
  ? path.resolve(process.env.BRAND_OUT_DIR)
  : path.join(WEB, "public");
const TOKENS_CSS = path.join(WEB, "src", "styles", "tokens.css");

export { brandMarkSvg, SHARE_CARD };

/**
 * Launch Chromium. PLAYWRIGHT_CHROMIUM_PATH points at a browser build when
 * the installed Playwright and its downloaded browsers disagree on version.
 */
export function launchBrowser() {
  const executablePath = process.env.PLAYWRIGHT_CHROMIUM_PATH || undefined;
  return chromium.launch(executablePath ? { executablePath } : {});
}

/** Square marks: file → edge in CSS px (captured at 2x, then downscaled). */
const SQUARE_ICONS = {
  "favicon-32.png": 32,
  "favicon-64.png": 64,
  "apple-touch-icon.png": 180,
  "datawrap-mark-192.png": 192,
  "datawrap-mark.png": 512,
  "brand/datawrap-mark-256.png": 256,
  "brand/datawrap-mark-512.png": 512,
  "brand/datawrap-mark-1024.png": 1024,
  "brand/datawrap-mark.png": 2048,
};

/** The header lockup's proportions, relative to the mark's edge. */
export const LOCKUP = { gap: 10 / 28, word: 19.2 / 28 };

const fmt = new Intl.NumberFormat("en-US");

export function svgDataUrl(svg) {
  return `data:image/svg+xml;charset=utf-8,${encodeURIComponent(svg)}`;
}

/** Every top-level `:root { … }` block of tokens.css: the --df-* tokens only. */
async function tokenBlocks() {
  const css = await readFile(TOKENS_CSS, "utf8");
  const out = [];
  const re = /^:root\s*\{/gm;
  let m;
  while ((m = re.exec(css))) {
    let depth = 0;
    for (let i = m.index; i < css.length; i++) {
      if (css[i] === "{") depth++;
      else if (css[i] === "}" && --depth === 0) {
        out.push(css.slice(m.index, i + 1));
        break;
      }
    }
  }
  if (!out.length) throw new Error(`no :root tokens found in ${TOKENS_CSS}`);
  return out.join("\n");
}

/** IBM Plex Sans 400/500/600 (latin), embedded from the app's own dependency. */
async function plexFaces() {
  const require = createRequire(path.join(WEB, "package.json"));
  const dir = path.join(path.dirname(require.resolve("@fontsource/ibm-plex-sans/400.css")), "files");
  const faces = [];
  for (const weight of [400, 500, 600]) {
    const buf = await readFile(path.join(dir, `ibm-plex-sans-latin-${weight}-normal.woff2`));
    faces.push(
      `@font-face{font-family:"IBM Plex Sans";font-style:normal;font-weight:${weight};` +
        `src:url(data:font/woff2;base64,${buf.toString("base64")}) format("woff2")}`,
    );
  }
  return faces.join("\n");
}

let headCache;
/**
 * Shared <head> styles: fonts, tokens, and the card palette — the marketing
 * site's own colour roles (`.mk` in marketing-statement.css), built from the
 * same --df-* tokens. No colour literal is introduced here.
 */
export async function brandHead() {
  headCache ??= (async () => `<style>
${await plexFaces()}
${await tokenBlocks()}
:root{
  --c-paper: var(--df-paper);
  --c-ink: var(--df-intaglio);
  --c-ink-2: color-mix(in srgb, var(--df-intaglio) 80%, var(--df-paper));
  --c-ink-3: color-mix(in srgb, var(--df-intaglio) 74%, var(--df-paper));
  --c-rule-strong: color-mix(in srgb, var(--df-intaglio) 30%, var(--df-paper));
  --c-sheet: color-mix(in srgb, white 82%, var(--df-paper));
  --c-landed: var(--df-landed);
  --c-serial: var(--df-serial);
  --c-graphite: color-mix(in srgb, var(--df-intaglio) 62%, var(--df-paper));
  --c-night: var(--df-paper-night);
  --c-on-night: var(--df-intaglio-light);
  --c-tile: var(--df-intaglio);
  --c-on-tile: var(--df-paper);
  --c-on-tile-2: color-mix(in srgb, var(--df-intaglio-light) 82%, var(--df-intaglio));
}
html,body{margin:0;padding:0;overflow:hidden}
body{font-family:"IBM Plex Sans",sans-serif;font-synthesis:none;-webkit-font-smoothing:antialiased;
  font-variant-numeric:tabular-nums lining-nums}
.lockup{display:inline-flex;align-items:center;flex:none}
.lockup img{display:block;flex:none}
.lockup span{font-weight:600;letter-spacing:-0.01em;line-height:1;white-space:nowrap}
</style>`)();
  return headCache;
}

/** One lockup, at the header's proportions, with the mark at `mark` px. */
export function lockupHtml(svg, mark, color) {
  return `<span class="lockup" style="gap:${(mark * LOCKUP.gap).toFixed(2)}px">` +
    `<img src="${svgDataUrl(svg)}" width="${mark}" height="${mark}" alt="" />` +
    `<span style="font-size:${(mark * LOCKUP.word).toFixed(2)}px;color:${color}">Datawrap</span></span>`;
}

/**
 * Screenshot an HTML page that sets text. Fails when IBM Plex Sans did not
 * load rather than shipping a fallback face (how the old cards ended up in
 * a different typeface from the site).
 */
export async function shoot(browser, { width, height, html, scale = 2, transparent = false }) {
  const page = await browser.newPage({
    viewport: { width, height },
    deviceScaleFactor: scale,
  });
  await page.setContent(html, { waitUntil: "load" });
  await page.evaluate(() =>
    Promise.all([400, 500, 600].map((w) => document.fonts.load(`${w} 20px "IBM Plex Sans"`))),
  );
  const missing = await page.evaluate(() =>
    [400, 500, 600].filter((w) => !document.fonts.check(`${w} 20px "IBM Plex Sans"`)),
  );
  if (missing.length) throw new Error(`IBM Plex Sans did not load (weights ${missing.join(", ")})`);
  // Shrink any [data-fit] line until it fits its box (long copy on narrow covers).
  await page.evaluate(() => {
    for (const el of document.querySelectorAll("[data-fit]")) {
      let size = parseFloat(getComputedStyle(el).fontSize);
      while (el.scrollWidth > el.clientWidth && size > 8) {
        size -= 1;
        el.style.fontSize = `${size}px`;
      }
    }
  });
  const buf = await page.screenshot({ type: "png", omitBackground: transparent });
  await page.close();
  return buf;
}

/** Screenshot with no text (the square marks): no font check needed. */
export async function shootMark(browser, { width, height, html, scale }) {
  const page = await browser.newPage({ viewport: { width, height }, deviceScaleFactor: scale });
  await page.setContent(html, { waitUntil: "load" });
  const buf = await page.screenshot({ type: "png", omitBackground: true });
  await page.close();
  return buf;
}

/** Render at 2x then resample down, so small favicons stay crisp. */
async function renderSquare(browser, svg, size) {
  const src = svgDataUrl(svg);
  const css = Math.max(size, 64);
  const scale = Math.max(1, Math.round((size * 2) / css));
  const buf = await shootMark(browser, {
    width: css,
    height: css,
    scale,
    html: `<!doctype html><html><head><style>
      html,body{margin:0;width:${css}px;height:${css}px;overflow:hidden;background:transparent}
      img{width:${css}px;height:${css}px;display:block}
    </style></head><body><img src="${src}" alt="" /></body></html>`,
  });
  if (css * scale === size) return buf;
  return resample(browser, buf, size);
}

/** Downscale a PNG buffer through the browser's own image pipeline. */
async function resample(browser, buf, size) {
  const page = await browser.newPage({ viewport: { width: size, height: size } });
  await page.setContent("<!doctype html><html><body></body></html>");
  const dataUrl = `data:image/png;base64,${Buffer.from(buf).toString("base64")}`;
  const out = await page.evaluate(
    async ([url, edge]) => {
      const img = new Image();
      img.src = url;
      await img.decode();
      const canvas = document.createElement("canvas");
      canvas.width = edge;
      canvas.height = edge;
      const ctx = canvas.getContext("2d");
      ctx.imageSmoothingEnabled = true;
      ctx.imageSmoothingQuality = "high";
      ctx.drawImage(img, 0, 0, edge, edge);
      return canvas.toDataURL("image/png").split(",")[1];
    },
    [dataUrl, size],
  );
  await page.close();
  return Buffer.from(out, "base64");
}

/**
 * The share card (1200x630, captured at 2x): the lockup, the home headline,
 * and the home page's example statement, labelled as an example exactly as
 * the hero labels it. The equation balances: read = landed + held + skipped.
 */
export async function renderShareCard(browser, svg) {
  const s = EXAMPLE_STATEMENT;
  const term = (cls, figure, label) =>
    `<div class="term"><b class="${cls}">${figure}</b><span>${label}</span></div>`;
  const html = `<!doctype html><html><head>${await brandHead()}<style>
    html,body{width:1200px;height:630px;background:var(--c-paper);color:var(--c-ink)}
    .card{box-sizing:border-box;height:630px;padding:56px 64px 60px;display:flex;flex-direction:column}
    .top{display:flex;align-items:center;justify-content:space-between}
    .url{font-size:22px;font-weight:500;color:var(--c-ink-2)}
    h1{margin:auto 0;font-size:96px;font-weight:500;letter-spacing:-0.02em;line-height:1;white-space:nowrap}
    .stmt{margin:0;box-sizing:border-box;border:2px solid var(--c-rule-strong);border-radius:16px;
      background:var(--c-sheet);padding:28px 40px 30px}
    .label{font-size:22px;font-weight:600;margin:0 0 22px}
    .eq{display:flex;align-items:flex-start;justify-content:space-between}
    .term{display:flex;flex-direction:column;gap:10px}
    .term b{font-size:58px;font-weight:600;letter-spacing:-0.03em;line-height:1}
    .term span{font-size:22px;color:var(--c-ink-3)}
    .op{font-size:52px;font-weight:400;line-height:1;color:var(--c-ink-3);padding-top:2px}
    .landed{color:var(--c-landed)} .held{color:var(--c-serial)} .skipped{color:var(--c-graphite)}
  </style></head><body><div class="card">
    <div class="top">${lockupHtml(svg, 52, "var(--c-ink)")}<span class="url">datawrap.io</span></div>
    <h1>${SHARE_CARD.headline}</h1>
    <figure class="stmt">
      <p class="label">${SHARE_CARD.statementLabel}</p>
      <div class="eq">
        ${term("", fmt.format(EXAMPLE_READ), "read")}
        <span class="op">=</span>
        ${term("landed", fmt.format(s.landed), "landed")}
        <span class="op">+</span>
        ${term("held", fmt.format(s.held), "held")}
        <span class="op">+</span>
        ${term("skipped", fmt.format(s.skipped), "skipped")}
      </div>
    </figure>
  </div></body></html>`;
  return shoot(browser, { width: 1200, height: 630, scale: 2, html });
}

async function main() {
  const svg = brandMarkSvg();
  await mkdir(PUBLIC, { recursive: true });
  await writeFile(path.join(PUBLIC, "favicon.svg"), svg);
  console.log("  favicon.svg");
  const browser = await launchBrowser();
  try {
    for (const [rel, size] of Object.entries(SQUARE_ICONS)) {
      const out = path.join(PUBLIC, rel);
      await mkdir(path.dirname(out), { recursive: true });
      await writeFile(out, await renderSquare(browser, svg, size));
      console.log(`  ${rel} (${size}px)`);
    }
    await writeFile(path.join(PUBLIC, "og-image.png"), await renderShareCard(browser, svg));
    console.log("  og-image.png (1200x630 @2x)");
  } finally {
    await browser.close();
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href) {
  main().catch((err) => {
    console.error(err);
    process.exit(1);
  });
}
