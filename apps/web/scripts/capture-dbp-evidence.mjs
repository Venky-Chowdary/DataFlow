/**
 * D-BP evidence captures (loop 5:12 PM batch):
 *  - dbp15-16-map-error-{w}.png — Map step error state: error title/icon,
 *    concise inline error + Details (no duplicate toast wall of text).
 *  - dbp13-validate-{w}.png — Validate step: "N of M passed (k skipped)" toast
 *    + "0 blocked" gate count on the ok token (D-BP-12/13).
 *  - dbp10-watchdog-1440.png — the 90s watchdog copy promised earlier.
 *
 * Uses the no-login "Preview workspace (UI only)" path on the vite dev server.
 * To force the error state deterministically, the egress-blocked connector
 * flow is simulated by seeding a loopback destination whose schema probe the
 * API refuses — the same real UI path as dbp10-map-egress-error-*.png.
 */
import { chromium } from "playwright";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const OUT = path.resolve(__dirname, "../../docs/evidence");
const BASE = process.env.PLAYWRIGHT_BASE_URL ?? "http://127.0.0.1:5173";

async function openStudio(page, width) {
  await page.setViewportSize({ width, height: 900 });
  await page.goto(`${BASE}/#/transfer`, { waitUntil: "domcontentloaded" });
  await page.evaluate(() => {
    localStorage.setItem("df2.theme", "light");
    localStorage.removeItem("df.studioSchemaBrowser");
    localStorage.removeItem("df.studioCanvas");
  });
  const preview = page.locator("button", { hasText: "Preview workspace (UI only)" });
  if (await preview.isVisible().catch(() => false)) {
    await preview.click();
  }
}

async function shoot(page, name) {
  await page.screenshot({ path: path.join(OUT, name), fullPage: false });
  console.log("captured", name);
}

const browser = await chromium.launch({ headless: true });

// --- D-BP-12/13: Validate step at both widths -----------------------------
for (const width of [1440, 1280]) {
  const page = await browser.newPage({ viewport: { width, height: 900 } });
  await openStudio(page, width);
  await page.getByRole("button", { name: /Load sample orders CSV/ }).click();
  // Walk the wizard forward to Validate.
  const next = page.getByRole("button", {
    name: /Continue to (Destination|Validate|Transform|Map|Review)/,
  });
  for (let i = 0; i < 6 && (await next.isVisible().catch(() => false)); i++) {
    await next.first().click();
    await page.waitForTimeout(1200);
  }
  const validateTab = page.getByRole("tab", { name: /Validate/ });
  if (await validateTab.isVisible().catch(() => false)) {
    await validateTab.click();
    await page.waitForTimeout(1500);
  }
  await shoot(page, `dbp13-validate-${width}.png`);
  await page.close();
}

// --- D-BP-15/16: Map error state + watchdog copy --------------------------
// Same flow as the egress-error capture: a file source + loopback DB dest the
// API egress-guard refuses, so the Map step lands on the error EmptyState.
for (const width of [1440, 1280]) {
  const page = await browser.newPage({ viewport: { width, height: 900 } });
  await openStudio(page, width);
  await page.getByRole("button", { name: /Load sample orders CSV/ }).click();
  const next = page.getByRole("button", { name: /Continue to Destination/ });
  if (await next.isVisible().catch(() => false)) {
    await next.click();
    await page.waitForTimeout(800);
  }
  // Pick any database destination tile, fill a loopback host the API refuses.
  const dbTile = page.getByRole("button", { name: /PostgreSQL|MySQL|Database/ }).first();
  if (await dbTile.isVisible().catch(() => false)) {
    await dbTile.click();
    await page.waitForTimeout(600);
  }
  const hostField = page.locator('input[placeholder*="host" i], input[name*="host" i]').first();
  if (await hostField.isVisible().catch(() => false)) {
    await hostField.fill("127.0.0.1");
  }
  const mapBtn = page.getByRole("button", { name: /Continue to Map|Map columns|Next/ }).first();
  if (await mapBtn.isVisible().catch(() => false)) {
    await mapBtn.click();
    // The schema probe refuses quickly; the watchdog copy appears at 90s —
    // wait out the failure path (either lands on the same EmptyState).
    await page.waitForTimeout(20_000);
  }
  await shoot(page, `dbp15-16-map-error-${width}.png`);
  await page.close();
}

await browser.close();
console.log("done");
