import { chromium } from "playwright";

const browser = await chromium.launch();
const page = await browser.newPage({ viewport: { width: 1024, height: 900 } });
await page.goto("http://localhost:5173/#/transfer");
const preview = page.locator("button", { hasText: "Preview workspace (UI only)" });
await preview.evaluate((n) => n.click());
await page.getByRole("button", { name: /Load sample orders CSV/ }).waitFor();
await page.getByRole("button", { name: /Load sample orders CSV/ }).click();
await page.waitForTimeout(1500);

const dump = await page.evaluate(() => {
  const dock = document.querySelector('.df2-toast-dock');
  const chrome = document.querySelector('.df2-transfer-studio-chrome');
  const out = {dock: null, chrome: null, labels: null};
  if (dock) {
    const cs = getComputedStyle(dock); const r = dock.getBoundingClientRect();
    out.dock = {pos: cs.position, x: Math.round(r.x), y: Math.round(r.y), w: Math.round(r.width), toasts: dock.querySelectorAll('.dt-toast').length};
  }
  if (chrome) {
    const r = chrome.getBoundingClientRect();
    out.chrome = {x: Math.round(r.x), y: Math.round(r.y), h: Math.round(r.height)};
  }
  const els = [
    ...document.querySelectorAll(
      ".df2-transfer-studio-chrome .df2-wizard-studio .df2-wizard-label, .df2-transfer-studio-chrome .df2-wizard-studio .df2-wizard-label-short",
    ),
  ];
  out.labels = els.map((el) => {
    const r = el.getBoundingClientRect();
    const cs = getComputedStyle(el);
    return {
      cls: el.className,
      text: el.textContent,
      display: cs.display,
      x: Math.round(r.x), y: Math.round(r.y),
      w: Math.round(r.width), h: Math.round(r.height),
      sw: el.scrollWidth, cw: el.clientWidth,
    };
  });
  return out;
});
console.log(JSON.stringify(dump, null, 1));
await browser.close();
