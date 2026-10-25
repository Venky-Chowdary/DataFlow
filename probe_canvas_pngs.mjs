import { chromium } from "playwright";
import { readFileSync } from "fs";
const OUT = "/home/ubuntu/repos/DataFlow/probe_capture";
const FX = "/home/ubuntu/repos/DataFlow/apps/web/e2e/fixtures/hermetic";
const fx = (n) => readFileSync(`${FX}/${n}`, "utf8");
const browser = await chromium.launch();
const stubs = async (page, canvas) => {
  await page.route("**/api/**", (r) => r.fulfill({ status: 404, contentType: "application/json", body: '{"detail":"x"}' }));
  await page.route("**/api/v1/auth/sso/providers", (r) => r.fulfill({ status: 200, contentType: "application/json", body: fx("auth_sso_providers.json") }));
  await page.route("**/api/v1/auth/me", (r) => r.fulfill({ status: 401, contentType: "application/json", body: '{"detail":"x"}' }));
  await page.route("**/api/v1/team/workspaces", (r) => r.fulfill({ status: 200, contentType: "application/json", body: fx("team_workspaces.json") }));
  await page.route("**/api/v1/workspace/settings", (r) => r.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ org_name: "Datawrap", timezone: "UTC", retention_days: 90, studio_schema_browser: false, studio_canvas: canvas }) }));
  await page.route("**/api/v1/transfer/capabilities", (r) => r.fulfill({ status: 200, contentType: "application/json", body: fx("transfer_capabilities.json") }));
  await page.route("**/api/v1/connectors/upload", (r) => r.fulfill({ status: 200, contentType: "application/json", body: fx("connectors_upload.json") }));
  await page.route("**/api/v1/schedules*", (r) => r.fulfill({ status: 200, contentType: "application/json", body: fx("schedules.json") }));
  await page.route("**/api/v1/connectors/jobs*", (r) => r.fulfill({ status: 200, contentType: "application/json", body: fx("connectors_jobs.json") }));
  await page.route("**/api/v1/connectors/saved*", (r) => r.fulfill({ status: 200, contentType: "application/json", body: fx("connectors_saved.json") }));
  await page.route("**/api/v1/transfer/analyze", (r) => r.fulfill({ status: 200, contentType: "application/json", body: fx("transfer_analyze.json") }));
  await page.route("**/api/v1/shape/catalog*", (r) => r.fulfill({ status: 200, contentType: "application/json", body: fx("shape_catalog.json") }));
  await page.route("**/api/v1/transform-library/*", (r) => r.fulfill({ status: 200, contentType: "application/json", body: fx("transform_library.json") }));
  await page.route("**/api/v1/shape/profile", (r) => r.fulfill({ status: 200, contentType: "application/json", body: fx("shape_profile.json") }));
  await page.route("**/api/v1/shape/preview", (r) => r.fulfill({ status: 200, contentType: "application/json", body: fx("shape_preview.json") }));
  await page.route("**/api/v1/ai/analyze/enhanced", (r) => r.fulfill({ status: 200, contentType: "application/json", body: fx("ai_analyze_enhanced.json") }));
  await page.route("**/api/v1/transfer/map", (r) => r.fulfill({ status: 200, contentType: "application/json", body: fx("transfer_map.json") }));
  await page.route("**/api/v1/preflight/preview-cells", (r) => r.fulfill({ status: 200, contentType: "application/json", body: fx("preflight_preview_cells.json") }));
  await page.route("**/api/v1/contracts*", (r) => r.fulfill({ status: 200, contentType: "application/json", body: fx("contracts.json") }));
  await page.route("**/api/v1/transfer/*/proof-pack*", (r) => r.fulfill({ status: 404, contentType: "application/json", body: "{}" }));
  await page.route("**/api/v1/transfer/plans/*/mappings*", (r) => r.fulfill({ status: 200, contentType: "application/json", body: fx("plan_mappings.json") }));
  await page.route("**/api/v1/transfer/plans/*/preflight*", async (r) => {
    const name = r.request().method() === "POST" ? "plan_preflight_post.json" : "plan_preflight_get.json";
    await r.fulfill({ status: r.request().method() === "POST" ? 202 : 200, contentType: "application/json", body: fx(name) });
  });
  await page.route("**/api/v1/transfer/plans/*/approve*", (r) => r.fulfill({ status: 200, contentType: "application/json", body: fx("plan_approve.json") }));
  await page.route("**/api/v1/transfer/plans", (r) => r.fulfill({ status: 200, contentType: "application/json", body: fx("transfer_plans.json") }));
};
for (const w of [1440, 1280, 1024, 390]) {
  const page = await browser.newPage({ viewport: { width: w, height: 900 } });
  await stubs(page, true);
  await page.addInitScript(() => {
    localStorage.setItem("df.studioCanvas", "1");
    localStorage.setItem("df2.theme", "light");
  });
  await page.goto("http://localhost:5173/#/transfer");
  const preview = page.locator("button", { hasText: "Preview workspace (UI only)" });
  await preview.evaluate((n) => n.click());
  await page.getByRole("button", { name: /Load sample orders CSV/ }).waitFor();
  await page.getByRole("button", { name: /Load sample orders CSV/ }).click();
  await page.waitForTimeout(1800);
  await page.screenshot({ path: `${OUT}/p13-canvas-connect-${w}.png` });
  // Advance to a later step for a second shot where reachable
  const clickIf = async (re, ms = 2000) => {
    const b = page.getByRole("button", { name: re }).first();
    if (await b.isVisible().catch(() => false)) { await b.click().catch(() => {}); await page.waitForTimeout(ms); return true; }
    return false;
  };
  await clickIf(/Continue to Destination/);
  const fileTab = page.getByRole("tab", { name: /File export|File/ }).first();
  if (await fileTab.isVisible().catch(() => false)) { await fileTab.click(); await page.waitForTimeout(400); }
  await clickIf(/Continue to Review|Continue to Transform/);
  await clickIf(/Continue without transforming/, 3000);
  await page.screenshot({ path: `${OUT}/p13-canvas-review-${w}.png` });
  await page.close();
  console.log("done", w);
}
await browser.close();
