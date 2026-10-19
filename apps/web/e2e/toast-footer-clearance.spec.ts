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

  // D-BP-20: docked toasts must live inside the header toast dock — never
  // over the step content. The content column also keeps the full width:
  // its right edge stays aligned to the stepper's right edge (+/-8px).
  const dock = page.locator(".df2-toast-dock").first();
  const stepPanel = page.locator(".df2-transfer-step-viewport:visible").first();
  const stepper = page.locator(".df2-transfer-studio-chrome").first();
  const [dockBox, panelBox, stepperBox] = await Promise.all([
    dock.boundingBox(),
    stepPanel.boundingBox(),
    stepper.boundingBox(),
  ]);
  if (dockBox && panelBox && stepperBox) {
    for (const t of toastBoxes) {
      expect(
        t.y >= dockBox.y - 4 &&
          t.y + t.height <= dockBox.y + dockBox.height + 4,
        `docked toast ${JSON.stringify(t)} must stay within the dock row on ${label}`,
      ).toBe(true);
    }
    const edgeDelta = Math.abs(panelBox.x + panelBox.width - (stepperBox.x + stepperBox.width));
    expect(
      edgeDelta,
      `content right edge must align to the stepper right edge (+/-8px) on ${label} (delta=${edgeDelta.toFixed(1)})`,
    ).toBeLessThanOrEqual(8);
  }

  const targets = page.locator(
    ".df2-wizard-footer, .df2-run-footer, .df2-transfer-step-panel .df2-btn-primary, " +
      ".df2-empty-state, .df2-map-empty-state, .df2-card, " +
      ".df2-contract-bind, .df2-vd-metric",
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

  for (const width of [1440, 1280, 1024]) {
    test(`no toast overlap with footer, primary actions, or cards @${width}px`, async ({
      page,
    }) => {
      await page.setViewportSize({ width, height: 900 });
      await openStudio(page);

      // Source step: the sample-file load fires "Source profiled", which used
      // to cover "Continue to Transform" in the wizard footer.
      await loadSample(page);
      await assertToastClearance(page, `Source @${width}px`);

      // Validate step — HARD assertion: the spec must actually reach the
      // Validate panel (the "Preflight passed" toast covered the contract
      // search/select and a metric card — the original complaint). Any
      // conditional path that skips this leg is a spec bug.
      const clickIf = async (re: RegExp, ms = 2500) => {
        const b = page.getByRole("button", { name: re }).first();
        if (await b.isVisible().catch(() => false)) {
          await b.click().catch(() => {});
          await page.waitForTimeout(ms);
        }
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
      const toValidate = page
        .getByRole("button", { name: /Continue to Validate/ })
        .first();
      await toValidate.waitFor({ state: "visible", timeout: 15_000 });
      await toValidate.click();
      // Validate panel + the contract bind field + at least one metric card
      // must be on screen, and the Validate toasts must be live.
      await expect(page.locator(".df2-contract-bind").first()).toBeVisible({
        timeout: 15_000,
      });
      await expect(page.locator(".df2-vd-metric").first()).toBeVisible();
      await assertToastClearance(page, `Validate step @${width}px`);
    });
  }
});
