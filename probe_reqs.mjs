import { chromium } from "playwright";
const browser = await chromium.launch();
const page = await browser.newPage({ viewport: { width: 1440, height: 900 } });
const reqs = [];
page.on("request", (r) => {
  const u = r.url();
  if (u.includes("/api/") || u.includes("8001")) reqs.push(`${r.method()} ${u}`);
});
page.on("response", (r) => {
  const u = r.url();
  if (u.includes("/api/") || u.includes("8001")) console.log("RES", r.status(), u);
});
await page.goto("http://localhost:5173/#/transfer");
const preview = page.locator("button", { hasText: "Preview workspace (UI only)" });
await preview.evaluate((n) => n.click());
await page.getByRole("button", { name: /Load sample orders CSV/ }).waitFor();
await page.getByRole("button", { name: /Load sample orders CSV/ }).click();
await page.waitForTimeout(1500);
const clickIf = async (re, ms = 2200) => {
  const b = page.getByRole("button", { name: re }).first();
  if (await b.isVisible().catch(() => false)) { await b.click().catch(() => {}); await page.waitForTimeout(ms); return true; }
  return false;
};
await clickIf(/Continue to Destination/);
const fileTab = page.getByRole("tab", { name: /File export|File/ }).first();
if (await fileTab.isVisible().catch(() => false)) { await fileTab.click(); await page.waitForTimeout(500); }
await clickIf(/Continue to Review|Continue to Transform/);
await clickIf(/Review result/, 1500);
await clickIf(/Continue without transforming/, 3000);
console.log("=== after map nav ===");
const toVal = page.getByRole("button", { name: /Continue to Validate/ }).first();
const visible = await toVal.isVisible().catch(() => false);
const enabled = visible ? await toVal.isEnabled().catch(() => false) : false;
const title = visible ? await toVal.getAttribute("title") : null;
console.log("toValidate:", { visible, enabled, title });
await page.waitForTimeout(2000);
console.log("REQUESTS:", JSON.stringify([...new Set(reqs)], null, 1));
await browser.close();
