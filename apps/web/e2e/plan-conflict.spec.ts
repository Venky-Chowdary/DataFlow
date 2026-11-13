import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { expect, test, type Page, type Request } from "@playwright/test";
import { installHermeticStubs } from "./hermetic";
import { openStudio } from "./support";

/**
 * Plan conflict, end to end in the browser (PS-26): Validate's mapping save
 * meets a newer revision saved elsewhere (409 plan_version_conflict). The
 * Studio must show both versions side by side and do exactly what the chosen
 * button says. Hermetic: every API call is stubbed (no API running).
 */
const FIXTURES = join(dirname(fileURLToPath(import.meta.url)), "fixtures", "hermetic");
const planFixture = JSON.parse(readFileSync(join(FIXTURES, "plan_mappings.json"), "utf8")) as {
  plan: { id: string; active_version: number; revisions: Array<{ version: number; mappings: Array<{ source: string; target: string }> }> };
};

/** Revision 2, "saved elsewhere": customer_email now feeds the status column. */
function latestPlan() {
  const plan = structuredClone(planFixture.plan);
  const v1 = plan.revisions[0];
  const v2 = {
    ...v1,
    version: 2,
    mappings: v1.mappings.map((m) => (m.target === "status" ? { ...m, source: "customer_email" } : m)),
    validation: { passed: true, issues: [], source: "ui_sync" },
  };
  plan.revisions = [v1, v2];
  plan.active_version = 2;
  return plan;
}

type SaveLog = Array<{ base_version?: number; mappings: Array<{ source: string; target: string }> }>;

async function stubConflict(page: Page): Promise<SaveLog> {
  const saves: SaveLog = [];
  const latest = latestPlan();
  await page.route("**/api/v1/transfer/plans/*/mappings*", async (route) => {
    const body = route.request().postDataJSON() as SaveLog[number];
    saves.push(body);
    if (saves.length === 1) {
      await route.fulfill({
        status: 409,
        contentType: "application/json",
        body: JSON.stringify({
          detail: {
            code: "plan_version_conflict",
            message: "A newer mapping revision exists than the one this edit was based on.",
            base_version: body.base_version ?? 0,
            current_version: 2,
            compared: "head",
          },
        }),
      });
      return;
    }
    const saved = structuredClone(latest);
    saved.revisions.push({ ...saved.revisions[1], version: 3, mappings: body.mappings });
    saved.active_version = 3;
    await route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ plan: saved }) });
  });
  // GET the plan (the comparison); POST /transfer/plans stays the hermetic stub.
  await page.route(/\/api\/v1\/transfer\/plans\/[^/?]+$/, async (route) => {
    if (route.request().method() !== "GET") return route.fallback();
    await route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ plan: latest }) });
  });
  return saves;
}

async function walkToValidate(page: Page) {
  await openStudio(page);
  await page.getByRole("button", { name: /Load sample orders CSV/ }).click();
  const clickIf = async (name: RegExp) => {
    const b = page.getByRole("button", { name }).first();
    if (await b.isVisible().catch(() => false)) await b.click();
  };
  await expect(page.getByRole("button", { name: /Continue to Destination|Continue to Review/ }).first()).toBeVisible({
    timeout: 20_000,
  });
  await clickIf(/Continue to Destination/);
  const fileTab = page.getByRole("tab", { name: /File export|File/ }).first();
  await fileTab.waitFor({ state: "visible", timeout: 15_000 });
  await fileTab.click();
  const next = page.getByRole("button", { name: /Continue to Review|Continue to Transform/ }).first();
  await next.waitFor({ state: "visible", timeout: 15_000 });
  await next.click();
  await clickIf(/Review result/);
  await clickIf(/Continue without transforming/);
  const toValidate = page.getByRole("button", { name: /Continue to Validate/ }).first();
  await toValidate.waitFor({ state: "visible", timeout: 20_000 });
  const approve = page.getByRole("button", { name: /Approve eligible/ }).first();
  if (await approve.isVisible().catch(() => false)) await approve.click();
  await expect(toValidate).toBeEnabled({ timeout: 15_000 });
  await toValidate.click();
}

const dialog = (page: Page) => page.getByRole("dialog", { name: "This plan changed since you opened it" });

test.describe("plan conflict (PS-26)", () => {
  test.beforeEach(async ({ page }) => {
    await installHermeticStubs(page);
    await page.addInitScript(() => {
      localStorage.removeItem("df.studioSchemaBrowser");
      localStorage.removeItem("df.studioCanvas");
      localStorage.setItem("df2.theme", "light");
    });
  });

  test("Validate shows both versions; Escape cancels; Keep mine saves over the latest head", async ({ page }) => {
    const saves = await stubConflict(page);
    await walkToValidate(page);

    const d = dialog(page);
    await expect(d).toBeVisible({ timeout: 20_000 });
    await expect(d).toHaveAttribute("aria-modal", "true");
    await expect.poll(() => page.evaluate(() => Boolean(document.activeElement?.closest('[role="dialog"]')))).toBe(true);
    await expect(d.getByText("Revision 2 was saved after this page loaded the plan")).toBeVisible();
    const statusRow = d.locator('tbody tr[data-differs="true"]', { has: page.getByRole("rowheader", { name: "status" }) });
    await expect(statusRow).toContainText("customer_email");
    await expect(statusRow).toContainText("(different)");
    await expect(d.locator('tbody tr[data-differs="true"]')).toHaveCount(1);
    expect(saves).toHaveLength(1);

    // Escape cancels: nothing saved, Validate panel says why there is no verdict.
    await page.keyboard.press("Escape");
    await expect(d).toBeHidden();
    const note = page.getByTestId("validate-plan-conflict");
    await expect(note).toContainText("Validate did not run");
    expect(saves).toHaveLength(1);

    // Re-open from the panel, then Keep mine: an explicit save over revision 2.
    await note.getByRole("button", { name: "Compare with the latest revision" }).click();
    await expect(d).toBeVisible();
    const save = page.waitForRequest((r: Request) => r.url().includes("/mappings") && r.method() === "POST");
    await d.getByRole("button", { name: "Keep mine (save over revision 2)" }).click();
    const sent = (await save).postDataJSON() as SaveLog[number];
    expect(sent.base_version).toBe(2);
    expect(sent.mappings.find((m) => m.target === "status")?.source).toBe("status");
    await expect(d).toBeHidden();
    await expect(page.locator(".dt-toast", { hasText: "Your mappings were saved" }).first()).toBeVisible();
  });

  test("Use latest puts the latest revision in the editor without saving", async ({ page }) => {
    const saves = await stubConflict(page);
    await walkToValidate(page);
    const d = dialog(page);
    await expect(d).toBeVisible({ timeout: 20_000 });
    await d.getByRole("button", { name: "Use latest (revision 2)" }).click();
    await expect(d).toBeHidden();
    await expect(page.locator(".dt-toast", { hasText: "Showing revision 2" }).first()).toBeVisible();
    expect(saves).toHaveLength(1);
  });
});
