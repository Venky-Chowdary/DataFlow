import { test, type Page } from "@playwright/test";
import { openStudio } from "./support";
import path from "node:path";
import { fileURLToPath } from "node:url";

/**
 * Evidence capture for the loop 5:12 PM defect batch — walks the real wizard
 * to Validate (file→file preview path) and to the Map error state, shooting
 * each into docs/evidence.
 */

const OUT = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)),
  "../../../docs/evidence",
);

async function clickIf(page: Page, name: RegExp, wait = 900) {
  const btn = page.getByRole("button", { name }).first();
  if (await btn.isVisible().catch(() => false)) {
    if (await btn.isEnabled().catch(() => false)) {
      await btn.click();
      await page.waitForTimeout(wait);
      return true;
    }
  }
  return false;
}

test.describe("D-BP evidence capture", () => {
  test.beforeEach(async ({ page }) => {
    await page.addInitScript(() => {
      localStorage.removeItem("df.studioSchemaBrowser");
      localStorage.removeItem("df.studioCanvas");
      localStorage.setItem("df2.theme", "light");
    });
  });

  for (const width of [1440, 1280]) {
    test(`Validate + map error states @${width}`, async ({ page }) => {
      await page.setViewportSize({ width, height: 900 });
      await openStudio(page);
      await page.getByRole("button", { name: /Load sample orders CSV/ }).click();
      await page.waitForTimeout(800);

      // Destination step — pick file export (the preview-mode path).
      await clickIf(page, /Continue to Destination/);
      await page.getByRole("tab", { name: /File export/ }).click().catch(() => {});
      await page.waitForTimeout(500);
      await clickIf(page, /Continue to Review|Continue to Transform/, 1200);

      // Transform → Result → Map.
      await clickIf(page, /Review result/, 1500);
      await clickIf(page, /Continue without transforming/, 1500);

      // Map — continue to Validate; the button enables once the route resolves.
      const toValidate = page
        .getByRole("button", { name: /Continue to Validate/ })
        .first();
      await toValidate.waitFor({ state: "visible", timeout: 15_000 }).catch(() => {});
      if (await toValidate.isVisible().catch(() => false)) {
        await toValidate.click({ timeout: 10_000 }).catch(() => {});
        await page.waitForTimeout(2500);
      }

      await page.screenshot({ path: path.join(OUT, `dbp13-validate-${width}.png`) });
      console.log(`shot validate @${width}`);
    });

    test(`Map error state @${width}`, async ({ page }) => {
      await page.setViewportSize({ width, height: 900 });
      await openStudio(page);
      await page.getByRole("button", { name: /Load sample orders CSV/ }).click();
      await page.waitForTimeout(800);

      await clickIf(page, /Continue to Destination/);
      // Pick the live local Postgres connector — its probe passes against the
      // running API, then the stub below fails the dest-schema introspect so
      // the Map step lands on the error EmptyState (D-BP-15/16).
      await page
        .getByRole("radio", { name: /QA Postgres Local/ })
        .first()
        .click()
        .catch(() => {});
      await page.waitForTimeout(1500);
      // A target table is required to continue — type a new name.
      const tableBox = page.getByPlaceholder(/Pick table or type new name/).first();
      if (await tableBox.isVisible().catch(() => false)) {
        await tableBox.click().catch(() => {});
        await page.keyboard.type("qa_dbp_map_error", { delay: 15 });
        await page.keyboard.press("Enter").catch(() => {});
        await page.waitForTimeout(400);
      }
      await clickIf(page, /Continue to Review|Continue to Transform/, 2000);
      await clickIf(page, /Review result/, 1500);
      await page.route("**/api/**", (route) =>
        route.fulfill({
          status: 503,
          contentType: "application/json",
          body: JSON.stringify({ detail: "schema probe refused for capture" }),
        }),
      );
      await clickIf(page, /Continue without transforming/, 1500);
      // goToMapping introspect fails → error EmptyState + short pointer toast.
      await page.waitForTimeout(8000);
      await page.unroute("**/api/**");
      await page.screenshot({ path: path.join(OUT, `dbp15-16-map-error-${width}.png`) });
      console.log(`shot map-error @${width}`);
    });

    test(`Map watchdog copy @${width}`, async ({ page }) => {
      await page.setViewportSize({ width, height: 900 });
      await openStudio(page);
      await page.getByRole("button", { name: /Load sample orders CSV/ }).click();
      await page.waitForTimeout(800);
      await clickIf(page, /Continue to Destination/);
      await page
        .getByRole("radio", { name: /QA Postgres Local/ })
        .first()
        .click()
        .catch(() => {});
      await page.waitForTimeout(1500);
      const tableBox = page.getByPlaceholder(/Pick table or type new name/).first();
      if (await tableBox.isVisible().catch(() => false)) {
        await tableBox.click().catch(() => {});
        await page.keyboard.type("qa_dbp_watchdog", { delay: 15 });
        await page.keyboard.press("Enter").catch(() => {});
        await page.waitForTimeout(400);
      }
      await clickIf(page, /Continue to Review|Continue to Transform/, 2000);
      await clickIf(page, /Review result/, 1500);
      // Hang the introspect so the MAPPING_TIMEOUT_MS watchdog fires —
      // captured with the timeout temporarily shortened on the dev server.
      await page.route("**/api/**", () => new Promise(() => {}));
      await clickIf(page, /Continue without transforming/, 1500);
      await page.waitForTimeout(12_000);
      await page.screenshot({ path: path.join(OUT, `dbp10-watchdog-${width}.png`) });
      console.log(`shot watchdog @${width}`);
    });
  }
});
