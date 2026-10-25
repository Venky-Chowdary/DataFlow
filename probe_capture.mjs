import { chromium } from "playwright";
import { writeFileSync, mkdirSync } from "fs";
const OUT = "/home/ubuntu/repos/DataFlow/probe_capture";
mkdirSync(OUT, { recursive: true });
const seen = new Set();
const browser = await chromium.launch();
const page = await browser.newPage({ viewport: { width: 1440, height: 900 } });
page.on("response", async (r) => {
  const u = r.url();
  if (!u.includes("/api/")) return;
  const key = `${r.request().method()} ${u.split("/api/v1")[1]?.split("?")[0] ?? u}`;
  try {
    const body = await r.text();
    const safe = key.replace(/[^a-zA-Z0-9]+/g, "_");
    let i = seen.has(key) ? 2 : 1;
    seen.add(key);
    const fname = `${OUT}/${safe}_${i}.json`;
    writeFileSync(fname, body);
    console.log(r.status(), key, "->", fname, `(${body.length}b)`);
  } catch (e) { console.log("ERR", key, String(e).slice(0, 80)); }
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
await clickIf(/Continue without transforming/, 3500);
const toVal = page.getByRole("button", { name: /Continue to Validate/ }).first();
if (await toVal.isVisible().catch(() => false)) {
  console.log("toValidate enabled:", await toVal.isEnabled());
  await toVal.click().catch(() => {});
  await page.waitForTimeout(4000);
}
console.log("URL:", page.url());
await page.screenshot({ path: `${OUT}/final.png` });
await page.waitForTimeout(1500);
await browser.close();
console.log("done");
