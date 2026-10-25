import { chromium } from "playwright";
const browser = await chromium.launch();
const page = await browser.newPage({ viewport: { width: 1024, height: 900 } });
await page.goto("http://localhost:5173/#/transfer");
const preview = page.locator("button", { hasText: "Preview workspace (UI only)" });
await preview.evaluate((n) => n.click());
await page.getByRole("button", { name: /Load sample orders CSV/ }).waitFor();
await page.getByRole("button", { name: /Load sample orders CSV/ }).click();
await page.waitForTimeout(1500);
const clickIf = async (re, ms = 2000) => {
  const b = page.getByRole("button", { name: re }).first();
  if (await b.isVisible().catch(() => false)) { await b.click().catch(() => {}); await page.waitForTimeout(ms); return true; }
  return false;
};
await clickIf(/Continue to Destination/);
const fileTab = page.getByRole('tab', { name: /File export|File/ }).first();
if (await fileTab.isVisible().catch(() => false)) { await fileTab.click(); await page.waitForTimeout(400); }
await clickIf(/Continue to Review|Continue to Transform/);
await page.waitForTimeout(1500);
const dump = await page.evaluate(() => {
  const pick = (sel) => { const el = document.querySelector(sel); if (!el) return null;
    const cs = getComputedStyle(el); const r = el.getBoundingClientRect();
    return { cls: String(el.className).slice(0,70), disp: cs.display, dir: cs.flexDirection,
      x: Math.round(r.x), y: Math.round(r.y), w: Math.round(r.width), h: Math.round(r.height),
      kids: [...el.children].map(c => { const cr = c.getBoundingClientRect();
        return String(c.className).slice(0,45)+'@'+Math.round(cr.x)+','+Math.round(cr.y)+' w'+Math.round(cr.width); }) }; };
  return { chrome: pick('.df2-transfer-studio-chrome'), row: pick('.df2-transfer-studio-chrome-row'),
    wizard: pick('.df2-transfer-studio-chrome .df2-wizard-studio'), track: pick('.df2-transfer-studio-chrome .df2-wizard-track'),
    dock: pick('.df2-toast-dock'), route: pick('.df2-route-bar') };
});
console.log(JSON.stringify(dump, null, 1));
await browser.close();
