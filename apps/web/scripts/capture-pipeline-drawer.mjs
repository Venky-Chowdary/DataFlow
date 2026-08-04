import { chromium } from "playwright";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const OUT = path.resolve(__dirname, "../public/docs/screenshots");
const EMAIL = process.env.DATAWRAP_EMAIL;
const PASS = process.env.DATAWRAP_PASSWORD;
if (!EMAIL || !PASS) {
  throw new Error("Set DATAWRAP_EMAIL and DATAWRAP_PASSWORD for local screenshot capture.");
}

const browser = await chromium.launch({ headless: true });
const page = await (
  await browser.newContext({ viewport: { width: 1920, height: 1080 }, deviceScaleFactor: 2 })
).newPage();

await page.goto("http://127.0.0.1:5173/#/login", { waitUntil: "domcontentloaded" });
if (!(await page.getByRole("textbox", { name: /work email/i }).count())) {
  await page.getByRole("button", { name: /^log in$/i }).first().click();
}
await page.getByRole("textbox", { name: /work email/i }).fill(EMAIL);
await page.getByRole("textbox", { name: /password/i }).fill(PASS);
await page.getByRole("button", { name: /sign in to workspace/i }).click();
await page.waitForURL(/#\//, { timeout: 30000 });
await page.waitForTimeout(500);

const expand = page.getByRole("button", { name: /expand navigation/i }).first();
if (await expand.count()) {
  try { await expand.click({ timeout: 1500 }); } catch { /* ok */ }
}

await page.getByRole("button", { name: /^pipelines$/i }).first().click();
await page.waitForTimeout(900);

// Prefer visible pipeline cards / list buttons
const candidates = [
  page.getByRole("button", { name: /docs sample|nightly|orders|pipeline/i }).first(),
  page.locator(".df2-pipeline-card, [class*='PipelineCard'], [class*='pipeline-card']").first(),
  page.locator("main button, main [role='button']").filter({ hasText: /active|paused|hourly|daily|weekly|cron/i }).first(),
  page.locator("main tbody tr").locator("visible=true").first(),
];

let opened = false;
for (const c of candidates) {
  if (!(await c.count())) continue;
  try {
    await c.click({ timeout: 4000 });
    await page.waitForTimeout(900);
    if (
      (await page.getByRole("button", { name: /run now/i }).count()) ||
      (await page.getByRole("tab", { name: /overview|history|config|schema/i }).count()) ||
      (await page.locator(".df2-drawer, [role='dialog']").count())
    ) {
      opened = true;
      break;
    }
  } catch {
    /* try next */
  }
}

if (!opened) {
  // create one quickly if form can save
  const newBtn = page.getByRole("button", { name: /new pipeline|create pipeline/i }).first();
  if (await newBtn.count()) {
    await newBtn.click();
    await page.waitForTimeout(500);
    await page.locator("#sched-name").fill("Docs audit pipeline");
    await page.locator("#sched-src-table").fill("orders");
    await page.locator("#sched-dst-table").fill("orders_warehouse");
    const save = page.getByRole("button", { name: /save pipeline/i });
    if (!(await save.isDisabled())) {
      await save.click();
      await page.waitForTimeout(1500);
      const row = page.locator("main").getByText(/docs audit pipeline/i).first();
      if (await row.count()) {
        await row.click();
        await page.waitForTimeout(900);
        opened = true;
      }
    }
  }
}

console.log("drawer open?", opened);
await page.screenshot({ path: path.join(OUT, "app-pipelines-drawer.png"), type: "png" });
console.log("wrote app-pipelines-drawer.png");

if (opened) {
  const history = page.getByRole("tab", { name: /history/i }).or(page.getByRole("button", { name: /^history$/i })).first();
  if (await history.count()) {
    await history.click();
    await page.waitForTimeout(500);
    await page.screenshot({ path: path.join(OUT, "app-pipelines-history.png"), type: "png" });
    console.log("wrote app-pipelines-history.png");
  }
}

await browser.close();
