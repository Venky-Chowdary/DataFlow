// Part 2: verify step (empty + error + resend state) and forgot/reset flow.
import { chromium } from "playwright";
import { mkdirSync } from "node:fs";

const OUT = "/home/ubuntu/signup-shots";
mkdirSync(OUT, { recursive: true });
const BASE = "http://127.0.0.1:5173";
const browser = await chromium.connectOverCDP("http://localhost:29229");

async function shot(page, name) {
  await page.screenshot({ path: `${OUT}/${name}.png`, fullPage: true });
  console.log("saved", name);
}

async function run(vpName, viewport, email) {
  const ctx = await browser.newContext({ viewport, deviceScaleFactor: 1 });
  const page = await ctx.newPage();
  const tag = vpName;

  await page.goto(`${BASE}/`, { waitUntil: "networkidle" });
  await page.getByText(/try datawrap free/i).first().click();
  await page.waitForTimeout(900);
  await page.getByText("Create an account").first().click();
  await page.waitForTimeout(600);

  // Valid submission → verify step.
  await page.locator("#signup-name").fill("Ada Lovelace");
  await page.locator('input[type="email"]').first().fill(email);
  await page.locator('input[type="password"]').first().fill("correct-horse-staple-9");
  await page.locator('button[type="submit"]').last().click();
  await page.waitForSelector("#auth-code", { timeout: 8000 });
  await page.waitForTimeout(400);
  await shot(page, `${tag}-04-signup-verify-empty`);

  // Resend cooldown state (button shows "Resend in Ns").
  const resendBtn = page.locator('button:has-text("Resend")');
  if (await resendBtn.count()) await shot(page, `${tag}-05-verify-resend-state`);

  // Wrong code → error.
  await page.locator("#auth-code").fill("000000");
  await page.locator('button[type="submit"]').last().click();
  await page.waitForTimeout(1100);
  await shot(page, `${tag}-06-verify-error`);

  // Forgot flow: empty → submitted → reset fields.
  await page.goto(`${BASE}/`, { waitUntil: "networkidle" });
  await page.getByText(/try datawrap free/i).first().click();
  await page.waitForTimeout(800);
  await page.getByText(/forgot/i).first().click();
  await page.waitForTimeout(700);
  await shot(page, `${tag}-07-forgot-empty`);
  await page.locator('input[type="email"]').first().fill("ada@example.com");
  await page.locator('button[type="submit"]').last().click();
  await page.waitForSelector("#reset-code", { timeout: 8000 });
  await page.waitForTimeout(400);
  await shot(page, `${tag}-08-forgot-reset-empty`);

  // Weak new password → error state.
  await page.locator("#reset-code").fill("000000");
  await page.locator('input[autocomplete="new-password"]').fill("short");
  await page.locator('button[type="submit"]').last().click();
  await page.waitForTimeout(1100);
  await shot(page, `${tag}-09-reset-error`);

  await ctx.close();
}

await run("desktop", { width: 1440, height: 900 }, "verify.d1@example.com");
await run("mobile", { width: 390, height: 844 }, "verify.m1@example.com");
await browser.close();
console.log("done");
