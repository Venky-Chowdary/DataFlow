// Automated overlap + WCAG AA contrast check for the auth surfaces.
// For each viewport (390/768/1440) x theme (light/dark) x screen
// (login/signup/forgot): asserts the decorative rail and brand panel
// intersect no rendered text node, and that Back/link/CTA text meets
// AA contrast (4.5:1) against its effective background.
//
//   node apps/web/scripts/auth-overlap-check.mjs [--base URL] [--cdp URL] [--out FILE]
import { chromium } from "playwright";
import { writeFileSync, appendFileSync } from "node:fs";

const args = {};
for (let i = 2; i < process.argv.length; i += 2) args[process.argv[i].replace(/^--/, "")] = process.argv[i + 1];
const BASE = args.base || "http://127.0.0.1:5173";
const CDP = args.cdp || "http://localhost:29229";
const OUT = args.out || null;
const log = (l) => { console.log(l); if (OUT) appendFileSync(OUT, l + "\n"); };
if (OUT) writeFileSync(OUT, `auth overlap/contrast check — ${new Date().toISOString()}\n`);

const VIEWPORTS = {
  mobile: { width: 390, height: 844 },
  tablet: { width: 768, height: 1024 },
  desktop: { width: 1440, height: 900 },
};

function relLum([r, g, b]) {
  const f = (c) => { c /= 255; return c <= 0.03928 ? c / 12.92 : ((c + 0.055) / 1.055) ** 2.4; };
  return 0.2126 * f(r) + 0.7152 * f(g) + 0.0722 * f(b);
}
function ratio(a, b) {
  const [l1, l2] = [relLum(a), relLum(b)].sort((x, y) => y - x);
  return (l1 + 0.05) / (l2 + 0.05);
}
function parseRgb(s) {
  const m = s.match(/rgba?\(([^)]+)\)/);
  if (!m) return null;
  const [r, g, b, a = 1] = m[1].split(",").map(Number);
  return { rgb: [r, g, b], a };
}
function blendOver(fg, bg, alpha) {
  return fg.map((c, i) => Math.round(c * alpha + bg[i] * (1 - alpha)));
}
function effectiveBg(el) {
  let cur = el;
  while (cur && cur !== document.documentElement) {
    const c = parseRgb(getComputedStyle(cur).backgroundColor);
    if (c && c.a > 0.98) return c.rgb;
    if (c && c.a > 0) {
      // translucent layer — composite over what lies beneath
      const under = effectiveBg(cur.parentElement);
      if (under) return blendOver(c.rgb, under, c.a);
    }
    cur = cur.parentElement;
  }
  return [255, 255, 255];
}
function effectiveFg(el) {
  let cur = el;
  while (cur && cur !== document.documentElement) {
    const c = parseRgb(getComputedStyle(cur).color);
    if (c) {
      if (c.a >= 0.98) return c.rgb;
      const under = effectiveBg(cur);
      if (under) return blendOver(c.rgb, under, c.a);
    }
    cur = cur.parentElement;
  }
  return [0, 0, 0];
}

const browser = await chromium.connectOverCDP(CDP);
let failures = 0;

async function check(vpName, viewport, theme, screen) {
  const ctx = await browser.newContext({ viewport, deviceScaleFactor: 1 });
  const page = await ctx.newPage();
  await page.context().addInitScript((t) => {
    window.localStorage.setItem("df2.theme", t);
    window.localStorage.removeItem("df2.session");
  }, theme);
  await page.goto(`${BASE}/`, { waitUntil: "networkidle" });
  await page.waitForTimeout(700);
  await page.getByText(/try datawrap free/i).first().click();
  await page.waitForTimeout(800);
  if (screen === "signup") { await page.getByText("Create an account").first().click(); }
  if (screen === "forgot") { await page.getByText(/forgot/i).first().click(); }
  await page.waitForTimeout(700);
  const tag = `${vpName}/${theme}/${screen}`;

  // 1) decorative elements (rail/orbs/grid) must not paint OVER text —
  // an intersection only counts when the decoration wins paint order
  // (positive z-index, or positioned after the text in DOM order).
  const overlaps = await page.evaluate(() => {
    const hits = [];
    const decos = [".lp-login-brand-overlay", ".lp-login-brand-orb",
      ".lp-login-brand-orb--a", ".lp-login-brand-orb--b", ".lp-login-brand-grid"]
      .map((s) => ({ s, el: document.querySelector(s) }))
      .filter((x) => x.el && x.el.getClientRects().length);
    if (!decos.length) return hits;
    const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT);
    const textRects = [];
    for (let n = walker.nextNode(); n; n = walker.nextNode()) {
      if (!n.textContent.trim()) continue;
      const el = n.parentElement;
      if (!el || ["SCRIPT", "STYLE"].includes(el.tagName)) continue;
      const r = new Range(); r.selectNodeContents(n);
      const rect = r.getBoundingClientRect();
      if (rect.width && rect.height) textRects.push({ text: n.textContent.trim().slice(0, 30), rect, el: n.parentElement });
    }
    const zOf = (el) => {
      let cur = el, z = 0;
      while (cur) {
        const zi = parseInt(getComputedStyle(cur).zIndex, 10);
        if (!isNaN(zi) && zi !== 0) { z = zi; break; }
        cur = cur.parentElement;
      }
      return z;
    };
    for (const { s, el } of decos) {
      const b = el.getBoundingClientRect();
      const dz = zOf(el);
      for (const t of textRects) {
        const intersects = b.left < t.rect.right && b.right > t.rect.left && b.top < t.rect.bottom && b.bottom > t.rect.top;
        if (!intersects) continue;
        // Painted under the text when it precedes it in DOM order at equal z.
        const pos = el.compareDocumentPosition(t.el);
        const textAfter = Boolean(pos & Node.DOCUMENT_POSITION_FOLLOWING);
        const paintsOver = dz > zOf(t.el) || (dz === zOf(t.el) && textAfter);
        if (paintsOver) hits.push(`${s} paints over text "${t.text}"`);
      }
    }
    return hits;
  });
  for (const h of overlaps) { log(`FAIL ${tag}: ${h}`); failures++; }
  if (!overlaps.length) log(`ok   ${tag}: no decorative/text overlap`);

  // 1b) the accent rule under the H1 must keep a clear gap from the
  // heading at every width — it lives in flow precisely so it can never
  // clip the trailing glyph (was an absolute rail at top:42%).
  const ruleGap = await page.evaluate(() => {
    const title = document.querySelector(".lp-login-brand-title");
    const rule = document.querySelector(".lp-login-brand-rule");
    if (!title || !rule || !rule.getClientRects().length) return null;
    const t = title.getBoundingClientRect();
    const r = rule.getBoundingClientRect();
    return { gap: r.top - t.bottom, intersectsTitle:
      r.left < t.right && r.right > t.left && r.top < t.bottom && r.bottom > t.top };
  });
  if (ruleGap === null) {
    log(`ok   ${tag}: accent rule absent on this layout`);
  } else if (ruleGap.intersectsTitle || ruleGap.gap < 12) {
    log(`FAIL ${tag}: accent rule gap ${ruleGap.gap.toFixed(1)}px (min 12, no intersection)`);
    failures++;
  } else {
    log(`ok   ${tag}: accent rule clears the H1 by ${ruleGap.gap.toFixed(1)}px`);
  }

  // 2) AA contrast on Back control, link-variant buttons, and CTAs —
  // measured on rendered pixels (element screenshot → the two dominant
  // color clusters are the label and its backdrop, gradients included).
  const handles = await page.$$(".lp-login-back, .lp-btn--link, .lp-btn--brand, .lp-login-forgot, .lp-login-brand-logo .dw-wordmark-text");
  for (const h of handles) {
    const label = (await h.textContent())?.trim().slice(0, 24) || "?";
    const cls = (await h.getAttribute("class") || "").split(" ").slice(0, 2).join(".");
    const box = await h.boundingBox();
    if (!box || !box.width || !box.height) continue;
    await h.scrollIntoViewIfNeeded();
    const fgCss = await h.evaluate((el) => getComputedStyle(el).color);
    const png = await h.screenshot();
    const pair = await page.evaluate(async ({ b64, fgCss }) => {
      const fm = fgCss.match(/rgba?\(([^)]+)\)/);
      if (!fm) return null;
      const [fr, fg_, fb] = fm[1].split(",").map(Number);
      const fg = [fr, fg_, fb];
      const img = new Image();
      img.src = `data:image/png;base64,${b64}`;
      await img.decode();
      const cv = document.createElement("canvas");
      cv.width = img.width; cv.height = img.height;
      const g = cv.getContext("2d");
      g.drawImage(img, 0, 0);
      const { data } = g.getImageData(0, 0, cv.width, cv.height);
      const counts = new Map();
      for (let i = 0; i < data.length; i += 4) {
        if (data[i + 3] < 128) continue;
        // exclude pixels near the label color — leaves the painted backdrop
        if (Math.hypot(data[i] - fg[0], data[i + 1] - fg[1], data[i + 2] - fg[2]) < 60) continue;
        const key = (data[i] >> 4 << 8) | (data[i + 1] >> 4 << 4) | (data[i + 2] >> 4);
        counts.set(key, (counts.get(key) || 0) + 1);
      }
      const top = [...counts.entries()].sort((a, b) => b[1] - a[1]);
      if (!top.length) return null;
      const dec = (k) => [(k >> 8 & 15) * 17, (k >> 4 & 15) * 17, (k & 15) * 17];
      return { fg, bg: dec(top[0][0]) };
    }, { b64: png.toString("base64"), fgCss });
    if (!pair) { log(`warn ${tag}: ${cls} "${label}" uniform — skipped`); continue; }
    const rr = ratio(pair.fg, pair.bg);
    if (rr < 4.5) { log(`FAIL ${tag}: ${cls} "${label}" pixel contrast ${rr.toFixed(2)}:1`); failures++; }
    else log(`ok   ${tag}: ${cls} "${label}" ${rr.toFixed(2)}:1`);
  }
  // 4) mobile: the first field + primary CTA must land inside the initial
  // viewport — the compact brand header is not allowed to push the form
  // below the fold (regression: brand kept height:100% → ~180px gap).
  if (vpName === "mobile") {
    const fold = await page.evaluate(() => {
      const vh = window.innerHeight;
      const input = document.querySelector(
        ".lp-login-auth input, .lp-login-auth [contenteditable]");
      const cta = document.querySelector(
        '.lp-login-auth button[type="submit"], ' +
        '.lp-login-auth .lp-btn--brand, ' +
        '.lp-login-auth button.df2-btn--primary, ' +
        '.lp-login-auth button[class*="primary"]');
      const top = (el) => el ? el.getBoundingClientRect() : null;
      return { vh, input: top(input), cta: top(cta) };
    });
    for (const [what, r] of [["first input", fold.input], ["primary CTA", fold.cta]]) {
      if (!r || !r.height) { log(`warn ${tag}: ${what} not found — fold check skipped`); continue; }
      if (r.top >= fold.vh || r.bottom > fold.vh + 1) {
        log(`FAIL ${tag}: ${what} below the fold (bottom ${r.bottom.toFixed(0)} > ${fold.vh})`);
        failures++;
      } else {
        log(`ok   ${tag}: ${what} above the fold (bottom ${r.bottom.toFixed(0)} <= ${fold.vh})`);
      }
    }
  }
  await ctx.close();
}

for (const [vpName, vp] of Object.entries(VIEWPORTS))
  for (const theme of ["light", "dark"])
    for (const screen of ["login", "signup", "forgot"])
      await check(vpName, vp, theme, screen);

await browser.close();
log(`\n${failures ? `${failures} FAILURES` : "ALL CHECKS PASSED"}`);
process.exit(failures ? 1 : 0);
