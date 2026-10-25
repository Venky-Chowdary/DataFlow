// Signup-surface screenshot capture: drives the running stack via CDP.
// Usage: node signup_screenshots.mjs
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

async function fresh(viewport) {
  const ctx = await browser.newContext({ viewport, deviceScaleFactor: 1 });
  const page = await ctx.newPage();
  return { ctx, page };
}

// Real flows that must reach the code screen use a live OTP submit against
// the API (console mailer). Wrong-code + invalid-input states drive the UI.
async function run(vpName, viewport) {
  const { ctx, page } = await fresh(viewport);
  const tag = vpName;

  // Login — shows Create an account + Forgot password links.
  await page.goto(`${BASE}/`, { waitUntil: "networkidle" });
  await page.waitForTimeout(800);
  const getStarted = page.getByText(/try datawrap free/i).first();
  await getStarted.click();
  await page.waitForTimeout(900);
  await shot(page, `${tag}-01-login`);

  // Signup — empty state.
  await page.getByText("Create an account").first().click();
  await page.waitForTimeout(700);
  await shot(page, `${tag}-02-signup-empty`);

  // Signup — error state: bad email + short password.
  const email = page.locator('input[type="email"], input[name="email"]').first();
  const pass = page.locator('input[type="password"]').first();
  if (await email.count()) await email.fill("not-an-email");
  if (await pass.count()) await pass.fill("short");
  const submit = page.locator('button[type="submit"], button:has-text("Continue"), button:has-text("Create")').last();
  await submit.click();
  await page.waitForTimeout(900);
  await shot(page, `${tag}-03-signup-error`);

  // Signup — loading/valid state: real submission → verify step.
  if (await email.count()) await email.fill("screenshot.taken@example.com");
  if (await pass.count()) await pass.fill("correct-horse-staple-9");
  await submit.click();
  await page.waitForTimeout(1200);
  await shot(page, `${tag}-04-signup-verify-empty`);

  // Verify — wrong code error.
  const codeInput = page.locator('input[autocomplete="one-time-code"], input[inputmode="numeric"], input[name="code"]').first();
  if (await codeInput.count()) {
    await codeInput.fill("000000");
    const verify = page.locator('button[type="submit"], button:has-text("Verify")').last();
    await verify.click();
    await page.waitForTimeout(1000);
    await shot(page, `${tag}-05-verify-error`);
  }

  // Forgot — empty state.
  await page.goto(`${BASE}/`, { waitUntil: "networkidle" });
  await page.waitForTimeout(700);
  await page.getByText(/try datawrap free/i).first().click();
  await page.waitForTimeout(800);
  const forgot = page.getByText(/forgot/i).first();
  await forgot.click();
  await page.waitForTimeout(700);
  await shot(page, `${tag}-06-forgot-empty`);

  // Forgot — submitted (uniform 202 → reset step).
  const fEmail = page.locator('input[type="email"], input[name="email"]').first();
  if (await fEmail.count()) await fEmail.fill("ada@example.com");
  const fSubmit = page.locator('button[type="submit"], button:has-text("Send"), button:has-text("Continue")').last();
  await fSubmit.click();
  await page.waitForTimeout(1000);
  await shot(page, `${tag}-07-forgot-submitted`);

  await ctx.close();
}

await run("desktop", { width: 1440, height: 900 });
await run("mobile", { width: 390, height: 844 });
await browser.close();
console.log("done");
