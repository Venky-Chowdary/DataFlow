import { chromium } from "playwright";
import { readFileSync } from "fs";
const OUT = "/home/ubuntu/repos/DataFlow/probe_capture";
const FX = "/home/ubuntu/repos/DataFlow/apps/web/e2e/fixtures/hermetic";
const fx = (n) => readFileSync(`${FX}/${n}`, "utf8");
const browser = await chromium.launch();
const stubs = async (page) => {
  await page.route("**/api/**", (r) => r.fulfill({ status: 404, contentType: "application/json", body: '{"detail":"x"}' }));
  for (const [pat, name, status] of [
    ["**/api/v1/auth/sso/providers","auth_sso_providers.json",200],
    ["**/api/v1/team/workspaces","team_workspaces.json",200],
    ["**/api/v1/workspace/settings","workspace_settings.json",200],
    ["**/api/v1/transfer/capabilities","transfer_capabilities.json",200],
    ["**/api/v1/connectors/upload","connectors_upload.json",200],
    ["**/api/v1/schedules*","schedules.json",200],
    ["**/api/v1/connectors/jobs*","connectors_jobs.json",200],
    ["**/api/v1/connectors/saved*","connectors_saved.json",200],
    ["**/api/v1/transfer/analyze","transfer_analyze.json",200],
    ["**/api/v1/shape/catalog*","shape_catalog.json",200],
    ["**/api/v1/transform-library/*","transform_library.json",200],
    ["**/api/v1/shape/profile","shape_profile.json",200],
    ["**/api/v1/shape/preview","shape_preview.json",200],
    ["**/api/v1/ai/analyze/enhanced","ai_analyze_enhanced.json",200],
    ["**/api/v1/transfer/map","transfer_map.json",200],
    ["**/api/v1/preflight/preview-cells","preflight_preview_cells.json",200],
    ["**/api/v1/contracts*","contracts.json",200],
    ["**/api/v1/transfer/*/proof-pack*","__404__",404],
  ]) {
    await page.route(pat, (r) => r.fulfill({ status, contentType: "application/json", body: name === "__404__" ? "{}" : fx(name) }));
  }
  await page.route("**/api/v1/auth/me", (r) => r.fulfill({ status: 401, contentType: "application/json", body: "{}" }));
  await page.route("**/api/v1/transfer/plans/*/mappings*", (r) => r.fulfill({ status: 200, contentType: "application/json", body: fx("plan_mappings.json") }));
  await page.route("**/api/v1/transfer/plans/*/preflight*", async (r) => {
    const name = r.request().method() === "POST" ? "plan_preflight_post.json" : "plan_preflight_get.json";
    await r.fulfill({ status: r.request().method() === "POST" ? 202 : 200, contentType: "application/json", body: fx(name) });
  });
  await page.route("**/api/v1/transfer/plans/*/approve*", (r) => r.fulfill({ status: 200, contentType: "application/json", body: fx("plan_approve.json") }));
  await page.route("**/api/v1/transfer/plans", (r) => r.fulfill({ status: 200, contentType: "application/json", body: fx("transfer_plans.json") }));
};
for (const w of [1440, 1280]) {
  const page = await browser.newPage({ viewport: { width: w, height: 900 } });
  await stubs(page);
  await page.addInitScript(() => { localStorage.setItem("df2.theme", "light"); });
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
  const fileTab = page.getByRole("tab", { name: /File export|File/ }).first();
  if (await fileTab.isVisible().catch(() => false)) { await fileTab.click(); await page.waitForTimeout(400); }
  await clickIf(/Continue to Review|Continue to Transform/);
  await clickIf(/Review result/, 2500);
  await clickIf(/Continue without transforming/, 3000);
  const toVal = page.getByRole("button", { name: /Continue to Validate/ }).first();
  if (await toVal.isVisible().catch(() => false)) {
    await toVal.click().catch(() => {});
    await page.waitForTimeout(5000);
  }
  // scroll the validate scroller to the bottom to see the contract rail boundary
  await page.locator(".df2-validate-scroll").evaluate((n) => n.scrollTop = n.scrollHeight).catch(() => {});
  await page.waitForTimeout(600);
  const info = await page.evaluate(() => {
    const els = [...document.querySelectorAll("*")].filter(
      (n) => n instanceof HTMLElement && n.scrollHeight > n.clientHeight + 40 && /auto|scroll/.test(getComputedStyle(n).overflowY),
    );
    const vs = document.querySelector(".df2-validate-scroll");
    return {
      scrollers: els.map((n) => ({ cls: String(n.className).slice(0, 90), sh: n.scrollHeight, ch: n.clientHeight })),
      vs: vs ? { mask: getComputedStyle(vs).webkitMaskImage || getComputedStyle(vs).maskImage, oy: getComputedStyle(vs).overflowY, pb: getComputedStyle(vs).paddingBottom, sh: vs.scrollHeight, ch: vs.clientHeight } : null,
      rail: (() => { const n = document.querySelector(".df2-validate-footer-contract"); if (!n) return null; const r = n.getBoundingClientRect(); return { y: r.y, h: r.height }; })(),
    };
  });
  console.log(JSON.stringify(info));
  await page.screenshot({ path: `${OUT}/dbp18-validate-${w}.png` });
  await page.close();
  console.log("done", w);
}
await browser.close();
