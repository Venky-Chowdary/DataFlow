/**
 * Render every Datawrap raster icon from the one canonical mark definition
 * (`@dataflow/design-system` → `brand/mark`), and rewrite `public/favicon.svg`
 * from it so the checked-in vector cannot drift either.
 *
 * Favicons, the manifest icons and the OG card used to be hand-exported, so
 * they drifted onto an older mark while the social exports moved on — the app
 * shipped two different logos at once. Everything here is generated, so the
 * mark can only change in one place.
 *
 *   npm run brand:icons
 */
import { mkdir, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { chromium } from "playwright";

import { brandMarkSvg } from "../../../packages/design-system/src/brand/mark.ts";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const WEB = path.resolve(__dirname, "..");
const PUBLIC = path.join(WEB, "public");
const SVG_PATH = path.join(PUBLIC, "favicon.svg");

const BRAND_BG = "#0A3D3A";
const TEXT = "#F8FAFC";
const ACCENT = "#5EEAD4";

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

const OG_TAGLINE = "Semantic maps · Eight preflight gates · Checksum proof";

function svgDataUrl(svg) {
  return `data:image/svg+xml;charset=utf-8,${encodeURIComponent(svg)}`;
}

async function shoot(browser, { width, height, html, scale = 2, transparent = false }) {
  const page = await browser.newPage({
    viewport: { width, height },
    deviceScaleFactor: scale,
  });
  await page.setContent(html, { waitUntil: "networkidle" });
  await page.waitForTimeout(120);
  const buf = await page.screenshot({ type: "png", omitBackground: transparent });
  await page.close();
  return buf;
}

/** Render at 2x then resample down, so small favicons stay crisp. */
async function renderSquare(browser, svg, size) {
  const src = svgDataUrl(svg);
  const css = Math.max(size, 64);
  const scale = Math.max(1, Math.round((size * 2) / css));
  const buf = await shoot(browser, {
    width: css,
    height: css,
    scale,
    transparent: true,
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

/** Open-graph / Twitter card: mark, wordmark, one honest line of product copy. */
async function renderOgCard(browser, svg) {
  const src = svgDataUrl(svg);
  return shoot(browser, {
    width: 1200,
    height: 630,
    scale: 2,
    html: `<!doctype html><html><head>
      <link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@500;700&display=swap" rel="stylesheet" />
      <style>
        html,body{margin:0;width:1200px;height:630px;overflow:hidden;
          background:linear-gradient(120deg,#062f2d 0%,${BRAND_BG} 55%,#04211f 100%);
          font-family:'DM Sans',system-ui,sans-serif}
        .wrap{position:absolute;inset:0;display:flex;flex-direction:column;justify-content:center;
          gap:28px;padding:0 96px;box-sizing:border-box}
        .row{display:flex;align-items:center;gap:28px}
        img{width:132px;height:132px;display:block;object-fit:contain;flex:none;
          filter:drop-shadow(0 12px 32px rgba(0,0,0,.38))}
        .name{font-weight:700;font-size:92px;letter-spacing:-0.035em;color:${TEXT};line-height:1}
        .headline{font-weight:700;font-size:46px;letter-spacing:-0.025em;color:${TEXT};margin:0;line-height:1.15}
        .headline em{font-style:normal;color:#2DD4BF}
        .sub{font-weight:500;font-size:28px;color:${ACCENT};margin:0;letter-spacing:0.01em}
      </style></head><body>
      <div class="wrap">
        <div class="row"><img src="${src}" width="132" height="132" alt="" /><span class="name">Datawrap</span></div>
        <h1 class="headline">Move any schema anywhere — <em>proven.</em></h1>
        <p class="sub">${OG_TAGLINE}</p>
      </div>
    </body></html>`,
  });
}

async function main() {
  const svg = brandMarkSvg();
  await writeFile(SVG_PATH, svg);
  console.log("  favicon.svg");
  const browser = await chromium.launch();
  try {
    for (const [rel, size] of Object.entries(SQUARE_ICONS)) {
      const out = path.join(PUBLIC, rel);
      await mkdir(path.dirname(out), { recursive: true });
      await writeFile(out, await renderSquare(browser, svg, size));
      console.log(`  ${rel} (${size}px)`);
    }
    await writeFile(path.join(PUBLIC, "og-image.png"), await renderOgCard(browser, svg));
    console.log("  og-image.png (1200x630)");
  } finally {
    await browser.close();
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
