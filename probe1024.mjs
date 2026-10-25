import { chromium } from "playwright";

const browser = await chromium.launch();
const page = await browser.newPage({ viewport: { width: 1024, height: 900 } });
await page.goto("http://localhost:5173/#/transfer");
const preview = page.locator("button", { hasText: "Preview workspace (UI only)" });
await preview.evaluate((n) => n.click());
await page.getByRole("button", { name: /Load sample orders CSV/ }).waitFor();
await page.getByRole("button", { name: /Load sample orders CSV/ }).click();
await page.waitForTimeout(1200);

// Walk to Validate like the spec
const clickIf = async (re, ms = 2000) => {
  const b = page.getByRole("button", { name: re }).first();
  if (await b.isVisible().catch(() => false)) {
    await b.click().catch(() => {});
    await page.waitForTimeout(ms);
    return true;
  }
  return false;
};
await clickIf(/Continue to Destination/);
const fileTab = page.getByRole("tab", { name: /File export|File/ }).first();
if (await fileTab.isVisible().catch(() => false)) {
  await fileTab.click();
  await page.waitForTimeout(400);
}
await clickIf(/Continue to Review|Continue to Transform/);
await clickIf(/Review result/);
await clickIf(/Continue without transforming/);
const toValidate = page.getByRole("button", { name: /Continue to Validate/ }).first();
try {
  await toValidate.waitFor({ state: "visible", timeout: 15000 });
  await toValidate.click();
  await page.waitForTimeout(2500);
} catch (e) {
  console.log("VALIDATE UNREACHABLE:", String(await page.locator(".df2-transfer-step-viewport").innerText()).slice(0, 300));
}

// Dump header + footer geometry/computed
const dump = await page.evaluate(() => {
  const pick = (sel) => {
    const el = document.querySelector(sel);
    if (!el) return null;
    const cs = getComputedStyle(el);
    const r = el.getBoundingClientRect();
    return {
      sel, cls: el.className,
      wrap: cs.flexWrap, dir: cs.flexDirection,
      x: Math.round(r.x), y: Math.round(r.y), w: Math.round(r.width), h: Math.round(r.height),
      children: [...el.children].map((c) => {
        const cr = c.getBoundingClientRect();
        return `${c.className?.toString?.().slice(0, 40)}@x${Math.round(cr.x)},y${Math.round(cr.y)},w${Math.round(cr.width)}`;
      }),
    };
  };
  return {
    chrome: pick(".df2-transfer-studio-chrome-row"),
    wizard: pick(".df2-wizard-studio .df2-wizard-track"),
    dock: pick(".df2-toast-dock"),
    routeBar: pick(".df2-route-bar, [class*='route-bar']"),
    footer1: pick(".df2-card-footer.df2-wizard-footer"),
    footer2: pick(".df2-validate-footer"),
    footer3: pick(".df2-studio-actions"),
  };
});
console.log(JSON.stringify(dump, null, 1));
await page.screenshot({ path: "/tmp/probe-1024.png" });
await browser.close();
