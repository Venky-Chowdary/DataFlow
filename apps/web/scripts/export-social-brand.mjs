/**
 * Export the social profile marks, lockups and covers from the canonical
 * Datawrap mark (`@dataflow/design-system` → `brand/mark`), with the same
 * renderer, tokens and IBM Plex Sans wordmark as the share card
 * (`export-brand-icons.mjs`).
 *
 * Rules:
 * - One logo per asset (never mark + lockup together).
 * - The wordmark is set exactly as the site header sets it: IBM Plex Sans
 *   600, -0.01em, at the header's mark : gap : word proportions.
 * - Cover copy is `SHARE_CARD` from `src/lib/seo.ts`: the home headline and
 *   facts the site states (nine core gates; drivers proven in CI, counted
 *   from the CI evidence). No tagline is typed in here.
 * - No photo or gradient behind the copy: the tile ink, flat.
 * - Every asset keeps its file name and pixel size (all captured at 2x).
 *
 *   npm run brand:social
 *   BRAND_OUT_DIR=/tmp/x npm run brand:social   (preview elsewhere)
 */
import { spawnSync } from "node:child_process";
import { mkdir, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const SELF = fileURLToPath(import.meta.url);

/**
 * The shared renderer imports the mark, the home statement and the site copy
 * from TypeScript sources, which plain `node` cannot load. Under node, re-run
 * this script with tsx (the runner the test suite and brand:icons use).
 */
async function loadShared() {
  try {
    return await import("./export-brand-icons.mjs");
  } catch (err) {
    if (process.env.DW_BRAND_UNDER_TSX) throw err;
    const run = spawnSync(process.platform === "win32" ? "tsx.cmd" : "tsx", [SELF, ...process.argv.slice(2)], {
      stdio: "inherit",
      shell: process.platform === "win32",
      env: { ...process.env, DW_BRAND_UNDER_TSX: "1" },
    });
    process.exit(run.status ?? 1);
  }
}

const {
  PUBLIC,
  SHARE_CARD,
  brandHead,
  brandMarkSvg,
  launchBrowser,
  lockupHtml,
  renderShareCard,
  shoot,
  shootMark,
  svgDataUrl,
} = await loadShared();

const OUT = path.join(PUBLIC, "brand", "social");

/** Square mark, transparent, SVG drawn at the final size and captured @2x. */
function renderMark(browser, svg, size) {
  return shootMark(browser, {
    width: size,
    height: size,
    scale: 2,
    html: `<!doctype html><html><head><style>
      html,body{margin:0;width:${size}px;height:${size}px;overflow:hidden;background:transparent}
      img{width:${size}px;height:${size}px;display:block}
    </style></head><body><img src="${svgDataUrl(svg)}" alt="" /></body></html>`,
  });
}

/** Padded square for circular profile crops: one mark on a flat ground. */
async function renderPaddedMark(browser, svg, { size, inner, ground }) {
  return shoot(browser, {
    width: size,
    height: size,
    scale: 2,
    html: `<!doctype html><html><head>${await brandHead()}<style>
      html,body{width:${size}px;height:${size}px;background:${ground}}
      body{display:grid;place-items:center}
    </style></head><body><img src="${svgDataUrl(svg)}" width="${inner}" height="${inner}" alt="" /></body></html>`,
  });
}

/** Mark + wordmark, centred on a canvas of a fixed size. */
async function renderLockup(browser, svg, { width, height, mark, dark, scale = 2 }) {
  const ground = dark ? "var(--c-night)" : "white";
  const ink = dark ? "var(--c-on-night)" : "var(--c-ink)";
  return shoot(browser, {
    width,
    height,
    scale,
    html: `<!doctype html><html><head>${await brandHead()}<style>
      html,body{width:${width}px;height:${height}px;background:${ground}}
      body{display:grid;place-items:center}
    </style></head><body>${lockupHtml(svg, mark, ink)}</body></html>`,
  });
}

/**
 * A cover: the home headline and the site's facts, on the tile ink.
 * `box` is where the copy may sit (platform safe areas; LinkedIn and X put
 * the profile picture over the bottom-left, so copy starts at 22%).
 */
async function renderCover(browser, svg, { w, h, box, showMark = false, center = false, size = 0.2 }) {
  const markSize = Math.round(h * 0.42);
  const copy = box ?? {
    x: Math.round(w * 0.22),
    y: 0,
    w: Math.round(w * (showMark ? 0.56 : 0.7)),
    h,
  };
  const headline = Math.round(copy.h * size);
  const facts = Math.round(headline * 0.36);
  const mark = showMark
    ? `<img class="mark" src="${svgDataUrl(svg)}" width="${markSize}" height="${markSize}" alt="" />`
    : "";
  return shoot(browser, {
    width: w,
    height: h,
    scale: 2,
    html: `<!doctype html><html><head>${await brandHead()}<style>
      html,body{width:${w}px;height:${h}px;background:var(--c-tile)}
      .copy{position:absolute;left:${copy.x}px;top:${copy.y}px;width:${copy.w}px;height:${copy.h}px;
        display:flex;flex-direction:column;justify-content:center;align-items:${center ? "center" : "flex-start"};gap:${Math.round(headline * 0.32)}px}
      h1,p{margin:0;white-space:nowrap;overflow:hidden;max-width:100%}
      h1{font-size:${headline}px;font-weight:500;letter-spacing:-0.02em;line-height:1.05;color:var(--c-on-tile)}
      p{font-size:${facts}px;font-weight:400;line-height:1.3;color:var(--c-on-tile-2)}
      .mark{position:absolute;right:${Math.round(w * 0.055)}px;top:${Math.round((h - markSize) / 2)}px;
        border-radius:22%;outline:2px solid color-mix(in srgb, white 14%, transparent);outline-offset:-1px}
    </style></head><body>
      <div class="copy">
        <h1 data-fit>${SHARE_CARD.headline}</h1>
        <p data-fit>${SHARE_CARD.facts.join(" ")}</p>
      </div>
      ${mark}
    </body></html>`,
  });
}

async function main() {
  await mkdir(OUT, { recursive: true });
  const svg = brandMarkSvg();
  const browser = await launchBrowser();
  const save = async (name, buf, note = "") => {
    await writeFile(path.join(OUT, name), buf);
    console.log(`  ${name}${note ? `  ${note}` : ""}`);
  };

  try {
    console.log("Marks (SVG at final size, @2x)…");
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
      await save(name, await renderMark(browser, svg, size));
    }
    await save("logo-400-white-pad.png", await renderPaddedMark(browser, svg, { size: 400, inner: 300, ground: "white" }));
    await save("logo-400-brand-pad.png", await renderPaddedMark(browser, svg, { size: 400, inner: 300, ground: "var(--c-tile)" }));

    console.log("Lockups (IBM Plex Sans wordmark)…");
    await save("lockup-light.png", await renderLockup(browser, svg, { width: 2895, height: 704, mark: 512, dark: false }));
    await save("lockup-dark.png", await renderLockup(browser, svg, { width: 2895, height: 704, mark: 512, dark: true }));
    await writeFile(
      path.join(PUBLIC, "brand", "datawrap-lockup-4k.png"),
      await renderLockup(browser, svg, { width: 768, height: 512, mark: 132, dark: false }),
    );
    console.log("  ../datawrap-lockup-4k.png");

    console.log("Covers (headline + site facts on the tile ink)…");
    await save("linkedin-cover-1584x396.png", await renderCover(browser, svg, { w: 1584, h: 396 }), "← use this");
    await save("linkedin-cover-1584x396-solid.png", await renderCover(browser, svg, { w: 1584, h: 396 }));
    await save("linkedin-cover-1584x396-mark-right.png", await renderCover(browser, svg, { w: 1584, h: 396, showMark: true }), "(optional)");
    await save("x-header-1500x500.png", await renderCover(browser, svg, { w: 1500, h: 500 }));
    await save("facebook-cover-820x312.png", await renderCover(browser, svg, { w: 820, h: 312 }));
    await save("facebook-cover-1640x624.png", await renderCover(browser, svg, { w: 1640, h: 624 }));
    // YouTube crops the banner per device; copy stays inside the 1546x423
    // area every device shows.
    await save(
      "youtube-banner-2560x1440.png",
      await renderCover(browser, svg, { w: 2560, h: 1440, box: { x: 507, y: 508, w: 1546, h: 423 }, center: true, size: 0.26 }),
    );

    // The share card, same render as public/og-image.png.
    await save("og-1200x630.png", await renderShareCard(browser, svg), "(= og-image.png)");
  } finally {
    await browser.close();
  }

  console.log("\nUpload: LinkedIn logo → linkedin-logo-800.png; cover → linkedin-cover-1584x396.png");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
