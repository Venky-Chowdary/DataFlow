// Signup-surface screenshot harness (single, parameterized).
// Drives the running stack over CDP (Devin's Chrome) or a bundled Chromium.
//
// Usage:
//   node apps/web/scripts/signup-screenshots.mjs \
//     [--sha <git-sha>] [--theme light|dark|both] [--viewport all|desktop|mobile|tablet] \
//     [--base http://127.0.0.1:5173] [--out <dir>] [--cdp http://localhost:29229]
//
// PNGs land in docs/signup-screenshots/<sha>/ at the repo root (default),
// named <viewport>-<theme>-<NN>-<state>.png. Console mailer only — never
// sends to a real inbox.
import { chromium } from "playwright";
import { mkdirSync } from "node:fs";
import { execSync } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");

const args = {};
for (let i = 2; i < process.argv.length; i += 2) args[process.argv[i].replace(/^--/, "")] = process.argv[i + 1];
const SHA = args.sha || execSync("git rev-parse --short=8 HEAD", { cwd: repoRoot }).toString().trim();
const BASE = args.base || "http://127.0.0.1:5173";
const OUT = path.resolve(args.out || path.join(repoRoot, "docs/signup-screenshots", SHA));
const THEME = args.theme || "both";
const VP = args.viewport || "all";
const CDP = args.cdp || "http://localhost:29229";

const VIEWPORTS = {
  desktop: { width: 1440, height: 900 },
  tablet: { width: 768, height: 1024 },
  mobile: { width: 390, height: 844 },
};
mkdirSync(OUT, { recursive: true });
console.log(`output → ${OUT} (sha ${SHA})`);

const browser = await chromium.connectOverCDP(CDP);

async function shot(page, name) {
  await page.screenshot({ path: path.join(OUT, `${name}.png`), fullPage: true });
  console.log("saved", name);
}

async function gotoLogin(page, theme) {
  const ctx = page.context();
  await ctx.addInitScript((t) => {
    window.localStorage.setItem("df2.theme", t);
    window.localStorage.removeItem("df2.session");
  }, theme);
  await page.goto(`${BASE}/`, { waitUntil: "networkidle" });
  await page.waitForTimeout(800);
  await page.getByText(/try datawrap free/i).first().click();
  await page.waitForTimeout(900);
}

async function run(vpName, viewport, theme) {
  const ctx = await browser.newContext({ viewport, deviceScaleFactor: 1 });
  const page = await ctx.newPage();
  const tag = `${vpName}-${theme}`;
  const uniq = `${vpName}.${theme}`;

  await gotoLogin(page, theme);
  await shot(page, `${tag}-01-login`);

  // Signup — empty state.
  await page.getByText("Create an account").first().click();
  await page.waitForTimeout(700);
  await shot(page, `${tag}-02-signup-empty`);

  // Signup — error state.
  await page.locator("#signup-name").fill("Ada Lovelace");
  await page.locator('input[type="email"]').first().fill("not-an-email");
  await page.locator('input[type="password"]').first().fill("short");
  await page.locator('button[type="submit"]').last().click();
  await page.waitForTimeout(900);
  await shot(page, `${tag}-03-signup-error`);

  // Signup — valid submission → verify step.
  await page.locator('input[type="email"]').first().fill(`verify.${uniq}@example.com`);
  await page.locator('input[type="password"]').first().fill("correct-horse-staple-9");
  await page.locator('button[type="submit"]').last().click();
  await page.waitForSelector("#auth-code", { timeout: 8000 });
  await page.waitForTimeout(400);
  await shot(page, `${tag}-04-signup-verify-empty`);

  // Resend cooldown state.
  if (await page.locator('button:has-text("Resend")').count()) {
    await shot(page, `${tag}-05-verify-resend-state`);
  }

  // Wrong code → error.
  await page.locator("#auth-code").fill("000000");
  await page.locator('button[type="submit"]').last().click();
  await page.waitForTimeout(1100);
  await shot(page, `${tag}-06-verify-error`);

  // Forgot flow.
  await gotoLogin(page, theme);
  await page.getByText(/forgot/i).first().click();
  await page.waitForTimeout(700);
  await shot(page, `${tag}-07-forgot-empty`);
  await page.locator('input[type="email"]').first().fill(`reset.${uniq}@example.com`);
  await page.locator('button[type="submit"]').last().click();
  await page.waitForTimeout(1200);
  await shot(page, `${tag}-08-forgot-submitted`);

  // Reset fields / expired-link state (selector depends on flow mode).
  const resetCode = page.locator("#reset-code");
  const newPass = page.locator('input[autocomplete="new-password"]').first();
  if (await resetCode.count()) {
    await resetCode.fill("000000");
    await newPass.fill("short");
    await page.locator('button[type="submit"]').last().click();
    await page.waitForTimeout(1100);
    await shot(page, `${tag}-09-reset-error`);
  }

  await ctx.close();
}

const vps = VP === "all" ? Object.keys(VIEWPORTS) : [VP];
const themes = THEME === "both" ? ["light", "dark"] : [THEME];
for (const v of vps) {
  for (const t of themes) await run(v, VIEWPORTS[v], t);
}
await browser.close();
console.log("done");
