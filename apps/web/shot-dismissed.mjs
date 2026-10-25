import { chromium } from "playwright";
const b = await chromium.launch();
for (const w of [1440, 1280]) {
  const p = await b.newPage({ viewport: { width: w, height: 900 } });
  await p.goto("http://localhost:5173/#/transfer");
  await p.locator("button", { hasText: "Preview workspace (UI only)" }).evaluate(n => n.click());
  await p.getByRole("button", { name: /Load sample orders CSV/ }).click();
  await p.waitForTimeout(600);
  const clickIf = async (re, ms = 2200) => {
    const x = p.getByRole("button", { name: re }).first();
    if (await x.isVisible().catch(() => false)) { await x.click().catch(() => {}); await p.waitForTimeout(ms); }
  };
  await clickIf(/Continue to Destination/);
  const tab = p.getByRole("tab", { name: /File export|File/ }).first();
  if (await tab.isVisible().catch(() => false)) { await tab.click(); await p.waitForTimeout(400); }
  await clickIf(/Continue to Review|Continue to Transform/);
  await clickIf(/Review result/);
  await clickIf(/Continue without transforming/);
  const toV = p.getByRole("button", { name: /Continue to Validate/ }).first();
  await toV.waitFor({ state: "visible", timeout: 20000 });
  await toV.click();
  await p.waitForTimeout(2500);
  // wait for all toasts to auto-dismiss
  await p.waitForSelector(".dt-toast", { state: "detached", timeout: 30000 }).catch(() => {});
  await p.waitForTimeout(800);
  await p.screenshot({ path: `docs/evidence/dbp19-validate-dismissed-${w}.png` });
  console.log("shot dismissed", w);
  await p.close();
}
await b.close();
