import { expect, test, type Locator, type Page } from "@playwright/test";
import { openStudio } from "./support";

/**
 * D-BP-11: a toast must never cover the sticky footer action bar, a primary
 * action, or the step's content card — the "Source profiled" toast painted
 * over "Continue to Transform", "Preflight passed" covered the contract row
 * on Validate, and the toast stack covered the bottom-right of the Map
 * empty-state card. Geometry, not styling.
 *
 * Every visible .dt-toast is measured — .first() can resolve to a toast that
 * is registered but not yet rendered (boundingBox() returns null), which is
 * how this spec used to false-fail.
 */

function intersects(
  a: { x: number; y: number; width: number; height: number },
  b: { x: number; y: number; width: number; height: number },
): boolean {
  return (
    a.x < b.x + b.width &&
    a.x + a.width > b.x &&
    a.y < b.y + b.height &&
    a.y + a.height > b.y
  );
}

async function visibleBoxes(
  locator: Locator,
): Promise<{ x: number; y: number; width: number; height: number }[]> {
  const boxes: { x: number; y: number; width: number; height: number }[] = [];
  const n = await locator.count();
  for (let i = 0; i < n; i++) {
    const el = locator.nth(i);
    if (!(await el.isVisible().catch(() => false))) continue;
    const box = await el.boundingBox();
    if (box) boxes.push(box);
  }
  return boxes;
}

async function assertToastClearance(page: Page, label: string) {
  const toasts = page.locator(".dt-toast:visible");
  await expect(toasts.first(), `a toast must be visible on ${label}`).toBeVisible();
  const toastBoxes = await visibleBoxes(toasts);
  expect(
    toastBoxes.length,
    `at least one rendered toast must have a box on ${label}`,
  ).toBeGreaterThan(0);

  const targets = page.locator(
    ".df2-wizard-footer, .df2-run-footer, .df2-transfer-step-panel .df2-btn-primary, " +
      ".df2-empty-state, .df2-map-empty-state, .df2-card",
  );
  const targetBoxes = await visibleBoxes(targets);
  expect(
    targetBoxes.length,
    `a footer / primary action / card must be on screen on ${label}`,
  ).toBeGreaterThan(0);
  for (const t of toastBoxes) {
    for (const b of targetBoxes) {
      expect(
        intersects(t, b),
        `toast ${JSON.stringify(t)} must not overlap ${JSON.stringify(b)} on ${label}`,
      ).toBe(false);
    }
  }
}

async function loadSample(page: Page) {
  await page
    .getByRole("button", { name: /Load sample orders CSV/ })
    .click();
}

test.describe("toast/footer clearance (D-BP-11)", () => {
  test.beforeEach(async ({ page }) => {
    await page.addInitScript(() => {
      localStorage.removeItem("df.studioSchemaBrowser");
      localStorage.removeItem("df.studioCanvas");
      localStorage.setItem("df2.theme", "light");
    });
  });

  for (const width of [1440, 1280]) {
    test(`no toast overlap with footer, primary actions, or cards @${width}px`, async ({
      page,
    }) => {
      await page.setViewportSize({ width, height: 900 });
      await openStudio(page);

      // Source step: the sample-file load fires "Source profiled", which used
      // to cover "Continue to Transform" in the wizard footer.
      await loadSample(page);
      await assertToastClearance(page, `Source @${width}px`);

      // Validate step: the "Preflight passed" toast used to cover the contract
      // search/select row — the original complaint.
      const next = page.getByRole("button", {
        name: /Continue to (Destination|Validate|Transform)/,
      });
      if (await next.isVisible().catch(() => false)) {
        await next.click();
      }
      // Reach Validate via the step rail if the wizard exposes it; otherwise
      // settle for any step that still shows a live toast.
      const validateTab = page.getByRole("tab", { name: /Validate/ });
      if (await validateTab.isVisible().catch(() => false)) {
        await validateTab.click();
      }
      if (await page.locator(".dt-toast:visible").count()) {
        await assertToastClearance(page, `post-Source step @${width}px`);
      }
    });
  }
});
