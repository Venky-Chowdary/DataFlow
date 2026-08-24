/**
 * Export LinkedIn + social sizes from the CURRENT Datawrap logo
 * (favicon.svg / DtLogo wrap-lattice).
 *
 * Rules:
 * - One logo per asset (never mark + lockup together)
 * - SVG rendered at final pixel size (no shrink-from-tiny-PNG)
 * - Covers also exported @2x for crisp uploads
 */
import { chromium } from "playwright";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const WEB = path.resolve(__dirname, "..");
const OUT = path.join(WEB, "public", "brand", "social");
const SVG_PATH = path.join(WEB, "public", "favicon.svg");
const ATMOSPHERE = path.join(WEB, "public", "brand", "hero-anywhere-atmosphere.png");
const BG = "#0A3D3A";
const TEXT_DARK = "#0F172A";
const TEXT_LIGHT = "#F8FAFC";

function svgDataUrl(svg) {
  return `data:image/svg+xml;charset=utf-8,${encodeURIComponent(svg)}`;
}

function toDataUrl(buf, mime = "image/png") {
  return `data:${mime};base64,${Buffer.from(buf).toString("base64")}`;
}

async function screenshotHtml(browser, { width, height, html, scale = 2, transparent = false }) {
  const page = await browser.newPage({
    viewport: { width, height },
    deviceScaleFactor: scale,
  });
  await page.setContent(html, { waitUntil: "networkidle" });
  // Wait a tick for webfonts / SVG paint
  await page.waitForTimeout(120);
  const buf = await page.screenshot({
    type: "png",
    omitBackground: transparent,
  });
  await page.close();
  return buf;
}

/** Square mark — SVG at exact size, @2x capture for HD. */
async function renderMark(browser, svg, size) {
  const src = svgDataUrl(svg);
  return screenshotHtml(browser, {
    width: size,
    height: size,
    scale: 2,
    transparent: true,
    html: `<!doctype html><html><head><style>
      html,body{margin:0;width:${size}px;height:${size}px;overflow:hidden;background:transparent}
      img{width:${size}px;height:${size}px;display:block;image-rendering:auto}
    </style></head><body>
      <img src="${src}" width="${size}" height="${size}" alt="" />
    </body></html>`,
  });
}

/** Horizontal lockup: one mark + Datawrap word (matches BrandWordmark). */
async function renderLockup(browser, svg, { mark, pad, dark = false, scale = 2 } = {}) {
  const textColor = dark ? TEXT_LIGHT : TEXT_DARK;
  const bg = dark ? BG : "#FFFFFF";
  const fontSize = Math.round(mark * 0.72);
  const gap = Math.round(mark * 0.28);
  const width = pad * 2 + mark + gap + Math.round(fontSize * 5.55);
  const height = pad * 2 + mark;
  const src = svgDataUrl(svg);
  const buf = await screenshotHtml(browser, {
    width,
    height,
    scale,
    html: `<!doctype html><html><head>
      <link rel="preconnect" href="https://fonts.googleapis.com" />
      <link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@700&display=swap" rel="stylesheet" />
      <style>
        html,body{margin:0;padding:0;background:${bg};width:${width}px;height:${height}px;overflow:hidden}
        .row{display:flex;align-items:center;gap:${gap}px;padding:${pad}px;box-sizing:border-box;
             width:${width}px;height:${height}px}
        img{width:${mark}px;height:${mark}px;display:block;flex:none}
        .w{font-family:'DM Sans',system-ui,sans-serif;font-weight:700;font-size:${fontSize}px;
           letter-spacing:-0.035em;color:${textColor};line-height:1;white-space:nowrap}
      </style>
    </head><body>
      <div class="row">
        <img src="${src}" width="${mark}" height="${mark}" alt="" />
        <span class="w">Datawrap</span>
      </div>
    </body></html>`,
  });
  return { buf, width: width * scale, height: height * scale, cssW: width, cssH: height };
}

/**
 * LinkedIn / social cover — tagline-first.
 * Left is clear for the profile logo overlay; optional small mark on the right only.
 *
 * Brand lines (from landing / docs):
 *   "Move any schema anywhere — proven."
 *   "Semantic maps · Eight preflight gates · Checksum proof"
 */
async function renderLinkedInCover(browser, svg, {
  w = 1584,
  h = 396,
  useAtmosphere = true,
  atmosphereDataUrl = "",
  showMark = false,
  scale = 2,
} = {}) {
  const src = svgDataUrl(svg);
  const markSize = Math.round(h * 0.42);

  let bgCss = `background:${BG}`;
  if (useAtmosphere && atmosphereDataUrl) {
    bgCss = `background:
      linear-gradient(100deg, rgba(8,40,38,.88) 0%, rgba(10,61,58,.72) 42%, rgba(5,24,26,.9) 100%),
      url('${atmosphereDataUrl}') center/cover no-repeat,
      ${BG}`;
  }

  const markBlock = showMark
    ? `<img class="mark" src="${src}" width="${markSize}" height="${markSize}" alt="" />`
    : "";

  const html = `<!doctype html><html><head>
    <link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@500;700&display=swap" rel="stylesheet" />
    <style>
      html,body{margin:0;width:${w}px;height:${h}px;overflow:hidden;background:${BG}}
      .bg{position:absolute;inset:0;${bgCss}}
      .veil{position:absolute;inset:0;background:
        linear-gradient(90deg, rgba(5,24,26,.55) 0%, rgba(5,24,26,.2) 55%, rgba(5,24,26,.45) 100%)}
      /* Keep left ~22% visually open — LinkedIn profile logo sits bottom-left */
      .copy{
        position:absolute;
        left:${Math.round(w * 0.22)}px;
        right:${showMark ? Math.round(w * 0.22) : Math.round(w * 0.08)}px;
        top:50%;
        transform:translateY(-50%);
        display:flex;flex-direction:column;gap:${Math.round(h * 0.06)}px;
      }
      .eyebrow{
        font-family:'DM Sans',system-ui,sans-serif;font-weight:500;
        font-size:${Math.round(h * 0.085)}px;letter-spacing:0.06em;text-transform:uppercase;
        color:#5EEAD4;line-height:1;
      }
      .headline{
        font-family:'DM Sans',system-ui,sans-serif;font-weight:700;
        font-size:${Math.round(h * 0.195)}px;letter-spacing:-0.03em;
        color:#F8FAFC;line-height:1.08;margin:0;
        text-shadow:0 2px 18px rgba(0,0,0,.35);
      }
      .headline em{font-style:normal;color:#2DD4BF}
      .sub{
        font-family:'DM Sans',system-ui,sans-serif;font-weight:500;
        font-size:${Math.round(h * 0.095)}px;letter-spacing:-0.01em;
        color:rgba(248,250,252,.78);line-height:1.25;margin:0;max-width:38em;
      }
      .mark{
        position:absolute;right:${Math.round(w * 0.055)}px;top:50%;
        transform:translateY(-50%);
        width:${markSize}px;height:${markSize}px;
        filter:drop-shadow(0 10px 28px rgba(0,0,0,.4));
      }
    </style>
  </head><body>
    <div class="bg"></div>
    <div class="veil"></div>
    <div class="copy">
      <div class="eyebrow">Universal data movement with proof</div>
      <h1 class="headline">Move any schema<br/>anywhere — <em>proven.</em></h1>
      <p class="sub">Semantic maps · Eight preflight gates · Checksum proof</p>
    </div>
    ${markBlock}
  </body></html>`;

  return screenshotHtml(browser, { width: w, height: h, html, scale });
}

/** Other platforms: same tagline layout, optional right mark. */
async function renderCoverDarkWordmark(browser, svg, opts) {
  return renderLinkedInCover(browser, svg, { showMark: false, ...opts });
}

async function main() {
  await mkdir(OUT, { recursive: true });
  const svg = await readFile(SVG_PATH, "utf8");
  const browser = await chromium.launch();

  let atmosphereDataUrl = "";
  try {
    atmosphereDataUrl = toDataUrl(await readFile(ATMOSPHERE));
  } catch {
    /* optional */
  }

  console.log("HD marks (SVG @2x)…");
  // Prefer uploading larger logos — platforms downscale cleanly; upscaling looks soft
  const markSizes = {
    "logo-300.png": 300,
    "logo-400.png": 400,
    "logo-512.png": 512,
    "logo-800.png": 800,
    "logo-1024.png": 1024,
    "logo-2048.png": 2048,
    "linkedin-logo-300.png": 300,
    "linkedin-logo-800.png": 800, // recommended upload (LinkedIn shrinks)
    "instagram-profile-320.png": 320,
    "facebook-profile-170.png": 170,
    "x-profile-400.png": 400,
    "youtube-profile-800.png": 800,
  };
  for (const [name, size] of Object.entries(markSizes)) {
    const buf = await renderMark(browser, svg, size);
    await writeFile(path.join(OUT, name), buf);
    console.log(`  ${name}  (${size * 2}px @2x → sharp)`);
  }
  // App icons and the OG card belong to export-brand-icons.mjs — two scripts
  // writing the same file is how the shipped favicon drifted off-brand.

  console.log("HD lockups (single mark + word)…");
  const lockupLight = await renderLockup(browser, svg, { mark: 512, pad: 96, dark: false, scale: 2 });
  await writeFile(path.join(OUT, "lockup-light.png"), lockupLight.buf);
  const lockupDark = await renderLockup(browser, svg, { mark: 512, pad: 96, dark: true, scale: 2 });
  await writeFile(path.join(OUT, "lockup-dark.png"), lockupDark.buf);
  console.log("  lockup-light.png / lockup-dark.png");

  console.log("LinkedIn covers — tagline-first, left clear for profile logo…");
  // Primary: no logo on cover (profile logo already sits left)
  const linkedinPrimary = await renderLinkedInCover(browser, svg, {
    useAtmosphere: true, atmosphereDataUrl, showMark: false, scale: 2,
  });
  await writeFile(path.join(OUT, "linkedin-cover-1584x396.png"), linkedinPrimary);
  console.log("  linkedin-cover-1584x396.png  ← use this (tagline only)");

  const linkedinSolid = await renderLinkedInCover(browser, svg, {
    useAtmosphere: false, atmosphereDataUrl: "", showMark: false, scale: 2,
  });
  await writeFile(path.join(OUT, "linkedin-cover-1584x396-solid.png"), linkedinSolid);
  console.log("  linkedin-cover-1584x396-solid.png");

  // Alt: small mark on the RIGHT only
  const linkedinMarkRight = await renderLinkedInCover(browser, svg, {
    useAtmosphere: true, atmosphereDataUrl, showMark: true, scale: 2,
  });
  await writeFile(path.join(OUT, "linkedin-cover-1584x396-mark-right.png"), linkedinMarkRight);
  console.log("  linkedin-cover-1584x396-mark-right.png  (optional)");

  const other = [
    ["x-header-1500x500.png", 1500, 500],
    ["facebook-cover-820x312.png", 820, 312],
    ["facebook-cover-1640x624.png", 1640, 624],
    ["youtube-banner-2560x1440.png", 2560, 1440],
    ["og-1200x630.png", 1200, 630],
  ];
  for (const [name, w, h] of other) {
    const buf = await renderLinkedInCover(browser, svg, {
      w, h, useAtmosphere: true, atmosphereDataUrl, showMark: false, scale: 2,
    });
    await writeFile(path.join(OUT, name), buf);
    console.log(`  ${name}`);
  }

  // Padded profile squares (one mark, generous safe margin for circular crop)
  for (const [name, bg] of [
    ["logo-400-white-pad.png", "#FFFFFF"],
    ["logo-400-brand-pad.png", BG],
  ]) {
    const size = 400;
    const inner = 300;
    const src = svgDataUrl(svg);
    const buf = await screenshotHtml(browser, {
      width: size,
      height: size,
      scale: 2,
      html: `<!doctype html><html><body style="margin:0;width:${size}px;height:${size}px;background:${bg};display:grid;place-items:center">
        <img src="${src}" width="${inner}" height="${inner}" alt="" /></body></html>`,
    });
    await writeFile(path.join(OUT, name), buf);
    console.log(" ", name);
  }

  await browser.close();
  console.log("\nDone. Upload tips:");
  console.log("  LinkedIn logo  → linkedin-logo-800.png");
  console.log("  LinkedIn cover → linkedin-cover-1584x396.png  (tagline only; left clear)");
  console.log("  Optional       → linkedin-cover-1584x396-mark-right.png");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
