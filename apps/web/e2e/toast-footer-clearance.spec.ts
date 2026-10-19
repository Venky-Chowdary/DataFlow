import { expect, test } from "@playwright/test";
import { openStudio } from "./support";

/**
 * D-BP-11: a toast must never cover the sticky footer action bar or a primary
 * action — the "Source profiled" toast painted over "Continue to Transform",
 * and "Preflight passed" covered the contract row. Geometry, not styling.
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

test.describe("toast/footer clearance (D-BP-11)", () => {
  test.beforeEach(async ({ page }) => {
    await page.addInitScript(() => {
      localStorage.removeItem("df.studioSchemaBrowser");
      localStorage.removeItem("df.studioCanvas");
      localStorage.setItem("df2.theme", "light");
    });
  });

  for (const width of [1440, 1280]) {
    test(`no toast/footer or toast/primary overlap @${width}px`, async ({
      page,
    }) => {
      await page.setViewportSize({ width, height: 900 });
      await openStudio(page);

      // The sample-file load fires the "Source profiled" toast that used to
      // cover "Continue to Transform" (a primary action in the wizard footer).
      await page
        .getByRole("button", { name: /Load sample orders CSV/ })
        .click();

      const toast = page.locator(".dt-toast").first();
      await expect(toast).toBeVisible({ timeout: 10_000 });
      const toastBox = await toast.boundingBox();
      expect(toastBox, "toast must render a box").toBeTruthy();
      if (!toastBox) return;

      const targets = page.locator(
        ".df2-wizard-footer, .df2-run-footer, .df2-transfer-step-panel .df2-btn-primary",
      );
      const n = await targets.count();
      expect(n, "a wizard footer / primary action must be on screen").toBeGreaterThan(0);
      for (let i = 0; i < n; i++) {
        const t = targets.nth(i);
        if (!(await t.isVisible())) continue;
        const box = await t.boundingBox();
        if (!box) continue;
        expect(
          intersects(toastBox, box),
          `toast must not overlap footer/primary action @${width}px`,
        ).toBe(false);
      }
    });
  }
});
